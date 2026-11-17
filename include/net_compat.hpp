#pragma once

#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

using socket_t = int;
constexpr socket_t INVALID_SOCKET_FD = -1;

inline void net_init() {}
inline void net_cleanup() {}

inline bool set_socket_nonblocking(socket_t fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

inline void close_socket(socket_t fd) {
    if (fd >= 0) {
        close(fd);
    }
}

inline void close_epoll(int epfd) {
    if (epfd >= 0) {
        close(epfd);
    }
}

inline bool is_socket_would_block() {
    return errno == EAGAIN || errno == EWOULDBLOCK;
}

#elif defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <cstdint>

using socket_t = SOCKET;
constexpr socket_t INVALID_SOCKET_FD = INVALID_SOCKET;

inline void net_init() {
    static bool initialized = false;
    if (!initialized) {
        WSADATA wsa;
        WSAStartup(MAKEWORD(2, 2), &wsa);
        initialized = true;
    }
}

inline void net_cleanup() {
    WSACleanup();
}

inline bool set_socket_nonblocking(socket_t fd) {
    u_long mode = 1;
    return ioctlsocket(fd, FIONBIO, &mode) == 0;
}

inline void close_socket(socket_t fd) {
    if (fd != INVALID_SOCKET) {
        closesocket(fd);
    }
}

inline bool is_socket_would_block() {
    int err = WSAGetLastError();
    return err == WSAEWOULDBLOCK;
}

// POSIX epoll constants
#define EPOLLIN      0x001
#define EPOLLPRI     0x002
#define EPOLLOUT     0x004
#define EPOLLERR     0x008
#define EPOLLHUP     0x010
#define EPOLLRDNORM  0x040
#define EPOLLRDBAND  0x080
#define EPOLLWRNORM  0x100
#define EPOLLWRBAND  0x200
#define EPOLLMSG     0x400
#define EPOLLRDHUP   0x2000
#define EPOLLET      (1u << 31)
#define EPOLLONESHOT (1u << 30)

#define EPOLL_CTL_ADD 1
#define EPOLL_CTL_DEL 2
#define EPOLL_CTL_MOD 3
#define EPOLL_CLOEXEC 02000000

typedef union epoll_data {
    void* ptr;
    int fd;
    uint32_t u32;
    uint64_t u64;
} epoll_data_t;

struct epoll_event {
    uint32_t events;
    epoll_data_t data;
};

struct WinEpollContext {
    std::unordered_map<socket_t, epoll_event> registered;
    std::mutex mtx;
};

inline std::unordered_map<int, WinEpollContext*>& get_epoll_map() {
    static std::unordered_map<int, WinEpollContext*> epoll_map;
    return epoll_map;
}

inline std::mutex& get_epoll_map_mutex() {
    static std::mutex epoll_map_mtx;
    return epoll_map_mtx;
}

inline int epoll_create1(int /*flags*/) {
    net_init();
    std::lock_guard<std::mutex> lock(get_epoll_map_mutex());
    static int next_id = 100;
    int id = ++next_id;
    get_epoll_map()[id] = new WinEpollContext();
    return id;
}

inline int epoll_ctl(int epfd, int op, socket_t fd, struct epoll_event* event) {
    std::lock_guard<std::mutex> lock(get_epoll_map_mutex());
    auto it = get_epoll_map().find(epfd);
    if (it == get_epoll_map().end()) return -1;
    WinEpollContext* ctx = it->second;
    std::lock_guard<std::mutex> ctx_lock(ctx->mtx);

    if (op == EPOLL_CTL_ADD || op == EPOLL_CTL_MOD) {
        if (!event) return -1;
        ctx->registered[fd] = *event;
        return 0;
    } else if (op == EPOLL_CTL_DEL) {
        ctx->registered.erase(fd);
        return 0;
    }
    return -1;
}

inline int epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout) {
    WinEpollContext* ctx = nullptr;
    {
        std::lock_guard<std::mutex> lock(get_epoll_map_mutex());
        auto it = get_epoll_map().find(epfd);
        if (it == get_epoll_map().end()) return -1;
        ctx = it->second;
    }

    std::vector<WSAPOLLFD> poll_fds;
    std::vector<socket_t> sock_list;
    {
        std::lock_guard<std::mutex> ctx_lock(ctx->mtx);
        poll_fds.reserve(ctx->registered.size());
        sock_list.reserve(ctx->registered.size());
        for (const auto& [sock, ev] : ctx->registered) {
            WSAPOLLFD pfd{};
            pfd.fd = sock;
            pfd.events = 0;
            if (ev.events & EPOLLIN) pfd.events |= (POLLIN | POLLRDNORM);
            if (ev.events & EPOLLOUT) pfd.events |= (POLLOUT | POLLWRNORM);
            pfd.revents = 0;
            poll_fds.push_back(pfd);
            sock_list.push_back(sock);
        }
    }

    if (poll_fds.empty()) {
        if (timeout > 0) {
            Sleep(static_cast<DWORD>(timeout));
        }
        return 0;
    }

    int res = WSAPoll(poll_fds.data(), static_cast<ULONG>(poll_fds.size()), timeout);
    if (res <= 0) {
        return res;
    }

    std::lock_guard<std::mutex> ctx_lock(ctx->mtx);
    int count = 0;
    for (size_t i = 0; i < poll_fds.size() && count < maxevents; ++i) {
        if (poll_fds[i].revents == 0) continue;
        auto reg_it = ctx->registered.find(sock_list[i]);
        if (reg_it == ctx->registered.end()) continue;

        uint32_t out_events = 0;
        if (poll_fds[i].revents & (POLLIN | POLLRDNORM)) out_events |= EPOLLIN;
        if (poll_fds[i].revents & (POLLOUT | POLLWRNORM)) out_events |= EPOLLOUT;
        if (poll_fds[i].revents & POLLERR) out_events |= EPOLLERR;
        if (poll_fds[i].revents & POLLHUP) out_events |= EPOLLHUP;

        events[count].events = out_events;
        events[count].data = reg_it->second.data;
        count++;
    }
    return count;
}

inline void close_epoll(int epfd) {
    std::lock_guard<std::mutex> lock(get_epoll_map_mutex());
    auto it = get_epoll_map().find(epfd);
    if (it != get_epoll_map().end()) {
        delete it->second;
        get_epoll_map().erase(it);
    }
}

#endif
