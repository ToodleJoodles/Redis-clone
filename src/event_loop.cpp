#include "event_loop.hpp"
#include <cstring>
#include <algorithm>
#include <iostream>

namespace redis {

EventLoop::EventLoop() {
    init();
}

EventLoop::~EventLoop() {
    stop();
    if (epoll_fd_ >= 0) {
        close_epoll(epoll_fd_);
        epoll_fd_ = -1;
    }
}

bool EventLoop::init() {
    if (epoll_fd_ < 0) {
        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    }
    return epoll_fd_ >= 0;
}

bool EventLoop::add_fd(socket_t fd, uint32_t events, EventCallback read_cb, EventCallback write_cb) {
    if (epoll_fd_ < 0) return false;

    struct epoll_event ev{};
    ev.events = events;
    ev.data.fd = static_cast<int>(fd);

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        return false;
    }

    handlers_[fd] = FdHandler{fd, events, std::move(read_cb), std::move(write_cb)};
    return true;
}

bool EventLoop::modify_fd(socket_t fd, uint32_t events) {
    if (epoll_fd_ < 0) return false;

    auto it = handlers_.find(fd);
    if (it == handlers_.end()) return false;

    struct epoll_event ev{};
    ev.events = events;
    ev.data.fd = static_cast<int>(fd);

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        return false;
    }

    it->second.events = events;
    return true;
}

bool EventLoop::remove_fd(socket_t fd) {
    if (epoll_fd_ < 0) return false;

    handlers_.erase(fd);
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    return true;
}

int EventLoop::add_timer(int interval_ms, TimerCallback cb, bool periodic) {
    int id = next_timer_id_++;
    auto now = std::chrono::steady_clock::now();
    TimerTask task{
        id,
        std::chrono::milliseconds(interval_ms),
        now + std::chrono::milliseconds(interval_ms),
        periodic,
        std::move(cb)
    };
    timers_.push_back(std::move(task));
    return id;
}

void EventLoop::remove_timer(int timer_id) {
    timers_.erase(
        std::remove_if(timers_.begin(), timers_.end(),
                       [timer_id](const TimerTask& t) { return t.id == timer_id; }),
        timers_.end()
    );
}

int EventLoop::get_next_timeout_ms() const {
    if (timers_.empty()) {
        return 100; // default 100ms idle timeout
    }

    auto now = std::chrono::steady_clock::now();
    int min_wait_ms = 100;
    for (const auto& t : timers_) {
        if (t.next_run <= now) {
            return 0;
        }
        auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(t.next_run - now).count();
        if (diff < min_wait_ms) {
            min_wait_ms = static_cast<int>(diff);
        }
    }
    return std::max(0, min_wait_ms);
}

void EventLoop::process_timers() {
    auto now = std::chrono::steady_clock::now();
    std::vector<TimerCallback> to_run;

    for (auto it = timers_.begin(); it != timers_.end();) {
        if (it->next_run <= now) {
            to_run.push_back(it->callback);
            if (it->periodic) {
                it->next_run = now + it->interval;
                ++it;
            } else {
                it = timers_.erase(it);
            }
        } else {
            ++it;
        }
    }

    for (const auto& cb : to_run) {
        if (cb) {
            cb();
        }
    }
}

void EventLoop::run() {
    running_ = true;
    constexpr int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];

    while (running_) {
        int timeout = get_next_timeout_ms();
        int nfds = epoll_wait(epoll_fd_, events, MAX_EVENTS, timeout);

        for (int i = 0; i < nfds; ++i) {
            socket_t fd = static_cast<socket_t>(events[i].data.fd);
            uint32_t ev = events[i].events;

            auto it = handlers_.find(fd);
            if (it == handlers_.end()) continue;

            FdHandler handler = it->second;

            if ((ev & (EPOLLIN | EPOLLERR | EPOLLHUP)) && handler.read_callback) {
                handler.read_callback(fd);
            }

            // Handler might have been removed during read_callback
            if (handlers_.find(fd) != handlers_.end() && (ev & EPOLLOUT) && handler.write_callback) {
                handler.write_callback(fd);
            }
        }

        process_timers();
    }
}

void EventLoop::stop() {
    running_ = false;
}

// ---------------- PosixSocket ----------------

socket_t PosixSocket::create_server_socket(const std::string& host, int port, int backlog) {
    net_init();

    socket_t sfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sfd == INVALID_SOCKET_FD) {
        return INVALID_SOCKET_FD;
    }

    int opt = 1;
#if defined(_WIN32)
    ::setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));
#else
    ::setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif

    if (!set_nonblocking(sfd)) {
        close_fd(sfd);
        return INVALID_SOCKET_FD;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (host.empty() || host == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
            close_fd(sfd);
            return INVALID_SOCKET_FD;
        }
    }

    if (::bind(sfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close_fd(sfd);
        return INVALID_SOCKET_FD;
    }

    if (::listen(sfd, backlog) != 0) {
        close_fd(sfd);
        return INVALID_SOCKET_FD;
    }

    return sfd;
}

bool PosixSocket::set_nonblocking(socket_t fd) {
    return set_socket_nonblocking(fd);
}

void PosixSocket::close_fd(socket_t fd) {
    close_socket(fd);
}

PosixSocket::ReadResult PosixSocket::read_available(socket_t fd, std::string& buffer) {
    char buf[4096];
    while (true) {
#if defined(_WIN32)
        int n = ::recv(fd, buf, sizeof(buf), 0);
#else
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
#endif
        if (n > 0) {
            buffer.append(buf, static_cast<size_t>(n));
        } else if (n == 0) {
            return ReadResult::Closed;
        } else {
            if (is_socket_would_block()) {
                return ReadResult::Success;
            }
            return ReadResult::Error;
        }
    }
}

ssize_t PosixSocket::write_buffer(socket_t fd, const char* data, size_t len) {
#if defined(_WIN32)
    int n = ::send(fd, data, static_cast<int>(len), 0);
    if (n < 0) {
        if (is_socket_would_block()) {
            return 0;
        }
        return -1;
    }
    return n;
#else
    ssize_t n = ::send(fd, data, len, 0);
    if (n < 0) {
        if (is_socket_would_block()) {
            return 0;
        }
        return -1;
    }
    return n;
#endif
}

} // namespace redis
