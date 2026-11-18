#pragma once

#include "event_loop.hpp"
#include "storage_engine.hpp"
#include "pubsub.hpp"
#include "transaction.hpp"
#include "aof.hpp"
#include "rdb.hpp"
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace redis {

struct ClientConnection {
    socket_t fd{INVALID_SOCKET_FD};
    std::string read_buf;
    std::string write_buf;
    bool close_after_write{false};
};

struct ServerConfig {
    std::string host{"127.0.0.1"};
    int port{6379};
    bool aof_enabled{false};
    std::string aof_path{"appendonly.aof"};
    AofFsyncPolicy aof_fsync{AofFsyncPolicy::EverySec};
    std::string rdb_path{"dump.rdb"};
};

class RedisServer {
public:
    using Config = ServerConfig;

    RedisServer();
    explicit RedisServer(ServerConfig config);
    ~RedisServer();

    bool start();
    void stop();

    StorageEngine& get_storage() { return storage_; }
    PubSubManager& get_pubsub() { return pubsub_; }
    TransactionManager& get_transaction_manager() { return tx_manager_; }

    // Direct command execution for testing or server invocation
    std::string execute_command(socket_t client_fd, const std::vector<std::string>& args);

private:
    void on_accept(socket_t server_fd);
    void on_client_read(socket_t client_fd);
    void on_client_write(socket_t client_fd);
    void close_client(socket_t client_fd);
    void flush_client_write(ClientConnection& client);

    std::string dispatch_command(socket_t client_fd, const std::vector<std::string>& args);

    Config config_;
    EventLoop loop_;
    socket_t listen_fd_{INVALID_SOCKET_FD};

    StorageEngine storage_;
    PubSubManager pubsub_;
    TransactionManager tx_manager_;
    std::unique_ptr<AofEngine> aof_;

    std::unordered_map<socket_t, ClientConnection> clients_;
};

} // namespace redis
