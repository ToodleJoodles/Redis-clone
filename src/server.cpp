#include "server.hpp"
#include "resp_parser.hpp"
#include "common.hpp"
#include <iostream>
#include <sstream>

namespace redis {

RedisServer::RedisServer() : RedisServer(ServerConfig{}) {}

RedisServer::RedisServer(ServerConfig config) : config_(std::move(config)) {
    // Hook up key mutation notification for transactions
    storage_.set_key_change_callback([this](const std::string& key) {
        tx_manager_.on_key_modified(key);
    });

    if (config_.aof_enabled) {
        aof_ = std::make_unique<AofEngine>(config_.aof_path, config_.aof_fsync);
    }
}

RedisServer::~RedisServer() {
    stop();
}

bool RedisServer::start() {
    if (!loop_.init()) {
        std::cerr << "Failed to initialize EventLoop\n";
        return false;
    }

    // Try loading persistence files on startup
    if (config_.aof_enabled) {
        std::cout << "Checking AOF file: " << config_.aof_path << std::endl;
        AofEngine::replay(config_.aof_path, storage_);
        if (!aof_->open()) {
            std::cerr << "Failed to open AOF file for appending: " << config_.aof_path << std::endl;
        }
    } else {
        std::cout << "Checking RDB snapshot: " << config_.rdb_path << std::endl;
        RdbParser::load(config_.rdb_path, storage_);
    }

    listen_fd_ = PosixSocket::create_server_socket(config_.host, config_.port);
    if (listen_fd_ == INVALID_SOCKET_FD) {
        std::cerr << "Failed to bind to " << config_.host << ":" << config_.port << "\n";
        return false;
    }

    std::cout << "Redis server started on " << config_.host << ":" << config_.port << std::endl;

    // Register listening socket
    loop_.add_fd(listen_fd_, EPOLLIN, [this](socket_t sfd) {
        on_accept(sfd);
    });

    // Active key eviction periodic timer (every 100ms)
    loop_.add_timer(100, [this]() {
        storage_.active_eviction_step(20, 0.25);
    }, true);

    loop_.run();
    return true;
}

void RedisServer::stop() {
    loop_.stop();

    if (listen_fd_ != INVALID_SOCKET_FD) {
        loop_.remove_fd(listen_fd_);
        PosixSocket::close_fd(listen_fd_);
        listen_fd_ = INVALID_SOCKET_FD;
    }

    std::vector<socket_t> fds;
    fds.reserve(clients_.size());
    for (const auto& [fd, _] : clients_) {
        fds.push_back(fd);
    }
    for (socket_t fd : fds) {
        close_client(fd);
    }

    if (aof_) {
        aof_->close();
    }
}

void RedisServer::on_accept(socket_t server_fd) {
    while (true) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
#if defined(_WIN32)
        socket_t cfd = ::accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
#else
        socket_t cfd = ::accept4(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd < 0 && (errno == ENOSYS || errno == EINVAL)) {
            cfd = ::accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
            if (cfd >= 0) {
                PosixSocket::set_nonblocking(cfd);
            }
        }
#endif
        if (cfd == INVALID_SOCKET_FD) {
            break;
        }

        PosixSocket::set_nonblocking(cfd);
        ClientConnection client{};
        client.fd = cfd;
        clients_[cfd] = std::move(client);

        loop_.add_fd(cfd, EPOLLIN,
            [this](socket_t fd) { on_client_read(fd); },
            [this](socket_t fd) { on_client_write(fd); }
        );
    }
}

void RedisServer::on_client_read(socket_t client_fd) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) return;

    ClientConnection& client = it->second;
    auto res = PosixSocket::read_available(client.fd, client.read_buf);

    if (res == PosixSocket::ReadResult::Closed || res == PosixSocket::ReadResult::Error) {
        close_client(client_fd);
        return;
    }

    // Process all pipelined commands in read_buf
    while (!client.read_buf.empty()) {
        RespValue cmd_val;
        size_t consumed = 0;
        ParseStatus st = RespParser::parse(client.read_buf, cmd_val, consumed);

        if (st == ParseStatus::Incomplete) {
            break; // wait for more bytes
        } else if (st == ParseStatus::Error) {
            client.write_buf += RespSerializer::error("Protocol error: unbalance or invalid format");
            size_t next_line = client.read_buf.find("\r\n");
            if (next_line != std::string::npos) {
                client.read_buf.erase(0, next_line + 2);
            } else {
                client.read_buf.clear();
            }
            break;
        }

        // Successfully parsed a complete command
        client.read_buf.erase(0, consumed);

        std::vector<std::string> args;
        if (cmd_val.type == RespType::Array) {
            for (const auto& elem : cmd_val.elements) {
                args.push_back(elem.str_val);
            }
        } else if (cmd_val.type == RespType::BulkString || cmd_val.type == RespType::SimpleString) {
            args.push_back(cmd_val.str_val);
        }

        if (args.empty()) {
            continue;
        }

        std::string response = execute_command(client_fd, args);
        client.write_buf += response;
    }

    if (!client.write_buf.empty()) {
        flush_client_write(client);
    }
}

void RedisServer::on_client_write(socket_t client_fd) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) return;

    flush_client_write(it->second);
}

void RedisServer::flush_client_write(ClientConnection& client) {
    if (client.write_buf.empty()) {
        loop_.modify_fd(client.fd, EPOLLIN);
        if (client.close_after_write) {
            close_client(client.fd);
        }
        return;
    }

    ssize_t written = PosixSocket::write_buffer(client.fd, client.write_buf.data(), client.write_buf.size());
    if (written < 0) {
        close_client(client.fd);
        return;
    }

    client.write_buf.erase(0, static_cast<size_t>(written));

    if (client.write_buf.empty()) {
        loop_.modify_fd(client.fd, EPOLLIN);
        if (client.close_after_write) {
            close_client(client.fd);
        }
    } else {
        loop_.modify_fd(client.fd, EPOLLIN | EPOLLOUT);
    }
}

void RedisServer::close_client(socket_t client_fd) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) return;

    pubsub_.client_disconnected(client_fd);
    tx_manager_.client_disconnected(client_fd);

    loop_.remove_fd(client_fd);
    PosixSocket::close_fd(client_fd);
    clients_.erase(it);
}

std::string RedisServer::execute_command(socket_t client_fd, const std::vector<std::string>& args) {
    if (args.empty()) {
        return RespSerializer::error("empty command");
    }

    std::string cmd = to_upper(args[0]);

    // Check PubSub restrictions
    if (pubsub_.is_subscribed(client_fd)) {
        if (cmd != "SUBSCRIBE" && cmd != "UNSUBSCRIBE" && cmd != "PSUBSCRIBE" &&
            cmd != "PUNSUBSCRIBE" && cmd != "PING" && cmd != "QUIT") {
            return RespSerializer::custom_error("ERR", "only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context");
        }
    }

    // Check Transaction mode: queue commands if in MULTI, unless EXEC, DISCARD, MULTI, WATCH
    if (tx_manager_.is_in_transaction(client_fd)) {
        if (cmd != "EXEC" && cmd != "DISCARD" && cmd != "MULTI" && cmd != "WATCH") {
            tx_manager_.queue_command(client_fd, args);
            return RespSerializer::queued();
        }
    }

    return dispatch_command(client_fd, args);
}

std::string RedisServer::dispatch_command(socket_t client_fd, const std::vector<std::string>& args) {
    std::string cmd = to_upper(args[0]);

    // Core Commands
    if (cmd == "PING") {
        if (args.size() == 1) {
            return RespSerializer::pong();
        } else if (args.size() == 2) {
            return RespSerializer::bulk_string(args[1]);
        }
        return RespSerializer::error("wrong number of arguments for 'ping' command");
    }

    if (cmd == "ECHO") {
        if (args.size() != 2) {
            return RespSerializer::error("wrong number of arguments for 'echo' command");
        }
        return RespSerializer::bulk_string(args[1]);
    }

    if (cmd == "GET") {
        if (args.size() != 2) {
            return RespSerializer::error("wrong number of arguments for 'get' command");
        }
        std::string val;
        if (storage_.get(args[1], val)) {
            return RespSerializer::bulk_string(val);
        }
        if (storage_.type(args[1]) != "none" && storage_.type(args[1]) != "string") {
            return RespSerializer::custom_error("WRONGTYPE", "Operation against a key holding the wrong kind of value");
        }
        return RespSerializer::null_bulk_string();
    }

    if (cmd == "SET") {
        if (args.size() < 3) {
            return RespSerializer::error("wrong number of arguments for 'set' command");
        }
        int64_t ttl_ms = -1;
        for (size_t i = 3; i < args.size(); ++i) {
            std::string opt = to_upper(args[i]);
            if (opt == "EX" && i + 1 < args.size()) {
                try {
                    ttl_ms = std::stoll(args[++i]) * 1000;
                } catch (...) {
                    return RespSerializer::error("value is not an integer or out of range");
                }
            } else if (opt == "PX" && i + 1 < args.size()) {
                try {
                    ttl_ms = std::stoll(args[++i]);
                } catch (...) {
                    return RespSerializer::error("value is not an integer or out of range");
                }
            }
        }
        storage_.set(args[1], args[2], ttl_ms);
        if (aof_) aof_->append_command(args);
        return RespSerializer::ok();
    }

    if (cmd == "DEL") {
        if (args.size() < 2) {
            return RespSerializer::error("wrong number of arguments for 'del' command");
        }
        std::vector<std::string> keys(args.begin() + 1, args.end());
        size_t count = storage_.del(keys);
        if (count > 0 && aof_) aof_->append_command(args);
        return RespSerializer::integer(static_cast<int64_t>(count));
    }

    if (cmd == "EXISTS") {
        if (args.size() < 2) {
            return RespSerializer::error("wrong number of arguments for 'exists' command");
        }
        std::vector<std::string> keys(args.begin() + 1, args.end());
        size_t count = storage_.exists(keys);
        return RespSerializer::integer(static_cast<int64_t>(count));
    }

    if (cmd == "INCR" || cmd == "DECR") {
        if (args.size() != 2) {
            return RespSerializer::error("wrong number of arguments for '" + args[0] + "' command");
        }
        int64_t delta = (cmd == "INCR") ? 1 : -1;
        int64_t new_val = 0;
        std::string err;
        if (storage_.incr_by(args[1], delta, new_val, err)) {
            if (aof_) aof_->append_command(args);
            return RespSerializer::integer(new_val);
        }
        return "-" + err + "\r\n";
    }

    if (cmd == "INCRBY" || cmd == "DECRBY") {
        if (args.size() != 3) {
            return RespSerializer::error("wrong number of arguments for '" + args[0] + "' command");
        }
        int64_t delta = 0;
        try {
            delta = std::stoll(args[2]);
        } catch (...) {
            return RespSerializer::error("value is not an integer or out of range");
        }
        if (cmd == "DECRBY") delta = -delta;
        int64_t new_val = 0;
        std::string err;
        if (storage_.incr_by(args[1], delta, new_val, err)) {
            if (aof_) aof_->append_command(args);
            return RespSerializer::integer(new_val);
        }
        return "-" + err + "\r\n";
    }

    if (cmd == "TTL" || cmd == "PTTL") {
        if (args.size() != 2) {
            return RespSerializer::error("wrong number of arguments for '" + args[0] + "' command");
        }
        int64_t rem = storage_.ttl(args[1], (cmd == "PTTL"));
        return RespSerializer::integer(rem);
    }

    if (cmd == "EXPIRE" || cmd == "PEXPIRE") {
        if (args.size() != 3) {
            return RespSerializer::error("wrong number of arguments for '" + args[0] + "' command");
        }
        int64_t time_val = 0;
        try {
            time_val = std::stoll(args[2]);
        } catch (...) {
            return RespSerializer::error("value is not an integer or out of range");
        }
        if (cmd == "EXPIRE") time_val *= 1000;
        bool ok = storage_.expire(args[1], time_val);
        if (ok && aof_) aof_->append_command(args);
        return RespSerializer::integer(ok ? 1 : 0);
    }

    if (cmd == "PERSIST") {
        if (args.size() != 2) {
            return RespSerializer::error("wrong number of arguments for 'persist' command");
        }
        bool ok = storage_.persist(args[1]);
        if (ok && aof_) aof_->append_command(args);
        return RespSerializer::integer(ok ? 1 : 0);
    }

    if (cmd == "TYPE") {
        if (args.size() != 2) {
            return RespSerializer::error("wrong number of arguments for 'type' command");
        }
        return RespSerializer::simple_string(storage_.type(args[1]));
    }

    // Sorted Set Commands
    if (cmd == "ZADD") {
        if (args.size() < 4 || (args.size() % 2 != 0)) {
            return RespSerializer::error("wrong number of arguments for 'zadd' command");
        }
        std::vector<std::pair<double, std::string>> members;
        for (size_t i = 2; i + 1 < args.size(); i += 2) {
            try {
                members.emplace_back(std::stod(args[i]), args[i + 1]);
            } catch (...) {
                return RespSerializer::error("value is not a valid float");
            }
        }
        size_t added = 0;
        std::string err;
        if (storage_.zadd(args[1], members, added, err)) {
            if (aof_) aof_->append_command(args);
            return RespSerializer::integer(static_cast<int64_t>(added));
        }
        return "-" + err + "\r\n";
    }

    if (cmd == "ZSCORE") {
        if (args.size() != 3) {
            return RespSerializer::error("wrong number of arguments for 'zscore' command");
        }
        double score = 0;
        bool not_found = false;
        std::string err;
        if (storage_.zscore(args[1], args[2], score, not_found, err)) {
            if (not_found) {
                return RespSerializer::null_bulk_string();
            }
            return RespSerializer::bulk_string(double_to_string(score));
        }
        return "-" + err + "\r\n";
    }

    if (cmd == "ZREM") {
        if (args.size() < 3) {
            return RespSerializer::error("wrong number of arguments for 'zrem' command");
        }
        std::vector<std::string> members(args.begin() + 2, args.end());
        size_t removed = 0;
        std::string err;
        if (storage_.zrem(args[1], members, removed, err)) {
            if (removed > 0 && aof_) aof_->append_command(args);
            return RespSerializer::integer(static_cast<int64_t>(removed));
        }
        return "-" + err + "\r\n";
    }

    if (cmd == "ZRANGE") {
        if (args.size() < 4 || args.size() > 5) {
            return RespSerializer::error("wrong number of arguments for 'zrange' command");
        }
        long start = 0, stop = 0;
        try {
            start = std::stol(args[2]);
            stop = std::stol(args[3]);
        } catch (...) {
            return RespSerializer::error("value is not an integer or out of range");
        }
        bool with_scores = (args.size() == 5 && string_equals_ci(args[4], "WITHSCORES"));
        std::vector<std::pair<std::string, double>> results;
        std::string err;
        if (storage_.zrange(args[1], start, stop, with_scores, results, err)) {
            std::vector<RespValue> elements;
            for (const auto& [member, score] : results) {
                elements.push_back(RespValue::make_bulk_string(member));
                if (with_scores) {
                    elements.push_back(RespValue::make_bulk_string(double_to_string(score)));
                }
            }
            return RespSerializer::array(elements);
        }
        return "-" + err + "\r\n";
    }

    if (cmd == "ZCARD") {
        if (args.size() != 2) {
            return RespSerializer::error("wrong number of arguments for 'zcard' command");
        }
        size_t card = 0;
        std::string err;
        if (storage_.zcard(args[1], card, err)) {
            return RespSerializer::integer(static_cast<int64_t>(card));
        }
        return "-" + err + "\r\n";
    }

    if (cmd == "ZRANK") {
        if (args.size() != 3) {
            return RespSerializer::error("wrong number of arguments for 'zrank' command");
        }
        long rank = 0;
        bool not_found = true;
        std::string err;
        if (storage_.zrank(args[1], args[2], rank, not_found, err)) {
            if (not_found) {
                return RespSerializer::null_bulk_string();
            }
            return RespSerializer::integer(rank);
        }
        return "-" + err + "\r\n";
    }

    // Pub/Sub Commands
    if (cmd == "SUBSCRIBE") {
        if (args.size() < 2) {
            return RespSerializer::error("wrong number of arguments for 'subscribe' command");
        }
        std::string resp;
        for (size_t i = 1; i < args.size(); ++i) {
            size_t count = pubsub_.subscribe(client_fd, args[i]);
            resp += RespSerializer::array({
                RespValue::make_bulk_string("subscribe"),
                RespValue::make_bulk_string(args[i]),
                RespValue::make_integer(static_cast<int64_t>(count))
            });
        }
        return resp;
    }

    if (cmd == "UNSUBSCRIBE") {
        std::string resp;
        if (args.size() == 1) {
            auto unsubs = pubsub_.unsubscribe_all(client_fd);
            if (unsubs.empty()) {
                return RespSerializer::array({
                    RespValue::make_bulk_string("unsubscribe"),
                    RespValue::make_null_bulk(),
                    RespValue::make_integer(0)
                });
            }
            for (const auto& [channel, rem] : unsubs) {
                resp += RespSerializer::array({
                    RespValue::make_bulk_string("unsubscribe"),
                    RespValue::make_bulk_string(channel),
                    RespValue::make_integer(static_cast<int64_t>(rem))
                });
            }
        } else {
            for (size_t i = 1; i < args.size(); ++i) {
                size_t rem = pubsub_.unsubscribe(client_fd, args[i]);
                resp += RespSerializer::array({
                    RespValue::make_bulk_string("unsubscribe"),
                    RespValue::make_bulk_string(args[i]),
                    RespValue::make_integer(static_cast<int64_t>(rem))
                });
            }
        }
        return resp;
    }

    if (cmd == "PUBLISH") {
        if (args.size() != 3) {
            return RespSerializer::error("wrong number of arguments for 'publish' command");
        }
        size_t count = pubsub_.publish(args[1], args[2], [this](socket_t fd, const std::string& msg) {
            auto it = clients_.find(fd);
            if (it != clients_.end()) {
                it->second.write_buf += msg;
                flush_client_write(it->second);
            }
        });
        return RespSerializer::integer(static_cast<int64_t>(count));
    }

    // Transactions Commands
    if (cmd == "MULTI") {
        if (args.size() != 1) {
            return RespSerializer::error("wrong number of arguments for 'multi' command");
        }
        std::string err;
        if (tx_manager_.multi(client_fd, err)) {
            return RespSerializer::ok();
        }
        return "-" + err + "\r\n";
    }

    if (cmd == "DISCARD") {
        if (args.size() != 1) {
            return RespSerializer::error("wrong number of arguments for 'discard' command");
        }
        std::string err;
        if (tx_manager_.discard(client_fd, err)) {
            return RespSerializer::ok();
        }
        return "-" + err + "\r\n";
    }

    if (cmd == "WATCH") {
        if (args.size() < 2) {
            return RespSerializer::error("wrong number of arguments for 'watch' command");
        }
        std::vector<std::string> keys(args.begin() + 1, args.end());
        std::string err;
        if (tx_manager_.watch(client_fd, keys, err)) {
            return RespSerializer::ok();
        }
        return "-" + err + "\r\n";
    }

    if (cmd == "UNWATCH") {
        if (args.size() != 1) {
            return RespSerializer::error("wrong number of arguments for 'unwatch' command");
        }
        tx_manager_.unwatch(client_fd);
        return RespSerializer::ok();
    }

    if (cmd == "EXEC") {
        if (args.size() != 1) {
            return RespSerializer::error("wrong number of arguments for 'exec' command");
        }
        std::vector<std::vector<std::string>> queue;
        bool aborted = false;
        std::string err;
        if (!tx_manager_.exec(client_fd, queue, aborted, err)) {
            return "-" + err + "\r\n";
        }
        if (aborted) {
            return RespSerializer::null_array();
        }
        std::string res = "*" + std::to_string(queue.size()) + "\r\n";
        for (const auto& queued_cmd : queue) {
            res += dispatch_command(client_fd, queued_cmd);
        }
        return res;
    }

    // Persistence & Server Administration
    if (cmd == "SAVE" || cmd == "BGSAVE") {
        if (!RdbParser::save(config_.rdb_path, storage_)) {
            return RespSerializer::error("Failed to save RDB snapshot");
        }
        return RespSerializer::ok();
    }

    if (cmd == "COMMAND") {
        return "*0\r\n";
    }

    if (cmd == "INFO") {
        std::string info = "# Server\r\nredis_version:7.0.0\r\narch_bits:64\r\nrole:master\r\nconnected_clients:" +
                           std::to_string(clients_.size()) + "\r\n";
        return RespSerializer::bulk_string(info);
    }

    if (cmd == "QUIT") {
        auto it = clients_.find(client_fd);
        if (it != clients_.end()) {
            it->second.close_after_write = true;
        }
        return RespSerializer::ok();
    }

    return RespSerializer::custom_error("ERR", "unknown command '" + args[0] + "'");
}

} // namespace redis
