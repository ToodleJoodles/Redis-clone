#pragma once

#include "net_compat.hpp"
#include <functional>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <memory>

namespace redis {

using EventCallback = std::function<void(socket_t fd)>;
using TimerCallback = std::function<void()>;

struct FdHandler {
    socket_t fd{INVALID_SOCKET_FD};
    uint32_t events{0};
    EventCallback read_callback;
    EventCallback write_callback;
};

struct TimerTask {
    int id{0};
    std::chrono::milliseconds interval{0};
    std::chrono::steady_clock::time_point next_run;
    bool periodic{true};
    TimerCallback callback;
};

class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    bool init();
    bool add_fd(socket_t fd, uint32_t events, EventCallback read_cb, EventCallback write_cb = nullptr);
    bool modify_fd(socket_t fd, uint32_t events);
    bool remove_fd(socket_t fd);

    int add_timer(int interval_ms, TimerCallback cb, bool periodic = true);
    void remove_timer(int timer_id);

    void run();
    void stop();
    bool is_running() const { return running_; }

private:
    void process_timers();
    int get_next_timeout_ms() const;

    int epoll_fd_{-1};
    bool running_{false};
    std::unordered_map<socket_t, FdHandler> handlers_;
    std::vector<TimerTask> timers_;
    int next_timer_id_{1};
};

class PosixSocket {
public:
    enum class ReadResult {
        Success,
        Closed,
        Error
    };

    static socket_t create_server_socket(const std::string& host, int port, int backlog = 1024);
    static bool set_nonblocking(socket_t fd);
    static void close_fd(socket_t fd);
    static ReadResult read_available(socket_t fd, std::string& buffer);
    static ssize_t write_buffer(socket_t fd, const char* data, size_t len);
};

} // namespace redis
