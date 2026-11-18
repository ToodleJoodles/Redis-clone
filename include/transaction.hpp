#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "net_compat.hpp"

namespace redis {

struct ClientTxState {
    bool in_transaction{false};
    bool dirty{false};
    std::vector<std::vector<std::string>> queue;
    std::unordered_set<std::string> watched_keys;
};

class TransactionManager {
public:
    TransactionManager();

    bool multi(socket_t client_fd, std::string& err);
    bool is_in_transaction(socket_t client_fd) const;
    void queue_command(socket_t client_fd, std::vector<std::string> cmd);

    bool watch(socket_t client_fd, const std::vector<std::string>& keys, std::string& err);
    void unwatch(socket_t client_fd);

    bool discard(socket_t client_fd, std::string& err);
    bool exec(socket_t client_fd, std::vector<std::vector<std::string>>& out_queue, bool& aborted, std::string& err);

    void on_key_modified(const std::string& key);
    void client_disconnected(socket_t client_fd);

    void clear();

private:
    std::unordered_map<socket_t, ClientTxState> client_states_;
    std::unordered_map<std::string, std::unordered_set<socket_t>> key_to_watchers_;
};

} // namespace redis
