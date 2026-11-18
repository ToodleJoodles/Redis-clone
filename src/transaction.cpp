#include "transaction.hpp"

namespace redis {

TransactionManager::TransactionManager() = default;

bool TransactionManager::multi(socket_t client_fd, std::string& err) {
    auto& state = client_states_[client_fd];
    if (state.in_transaction) {
        err = "ERR MULTI calls can not be nested";
        return false;
    }
    state.in_transaction = true;
    state.queue.clear();
    return true;
}

bool TransactionManager::is_in_transaction(socket_t client_fd) const {
    auto it = client_states_.find(client_fd);
    return it != client_states_.end() && it->second.in_transaction;
}

void TransactionManager::queue_command(socket_t client_fd, std::vector<std::string> cmd) {
    client_states_[client_fd].queue.push_back(std::move(cmd));
}

bool TransactionManager::watch(socket_t client_fd, const std::vector<std::string>& keys, std::string& err) {
    auto& state = client_states_[client_fd];
    if (state.in_transaction) {
        err = "ERR WATCH inside MULTI is not allowed";
        return false;
    }

    for (const auto& key : keys) {
        state.watched_keys.insert(key);
        key_to_watchers_[key].insert(client_fd);
    }
    return true;
}

void TransactionManager::unwatch(socket_t client_fd) {
    auto it = client_states_.find(client_fd);
    if (it == client_states_.end()) return;

    for (const auto& key : it->second.watched_keys) {
        auto w_it = key_to_watchers_.find(key);
        if (w_it != key_to_watchers_.end()) {
            w_it->second.erase(client_fd);
            if (w_it->second.empty()) {
                key_to_watchers_.erase(w_it);
            }
        }
    }
    it->second.watched_keys.clear();
    it->second.dirty = false;
}

bool TransactionManager::discard(socket_t client_fd, std::string& err) {
    auto it = client_states_.find(client_fd);
    if (it == client_states_.end() || !it->second.in_transaction) {
        err = "ERR DISCARD without MULTI";
        return false;
    }

    unwatch(client_fd);
    it->second.queue.clear();
    it->second.in_transaction = false;
    it->second.dirty = false;
    return true;
}

bool TransactionManager::exec(socket_t client_fd, std::vector<std::vector<std::string>>& out_queue, bool& aborted, std::string& err) {
    auto it = client_states_.find(client_fd);
    if (it == client_states_.end() || !it->second.in_transaction) {
        err = "ERR EXEC without MULTI";
        return false;
    }

    auto& state = it->second;
    if (state.dirty) {
        aborted = true;
        out_queue.clear();
    } else {
        aborted = false;
        out_queue = std::move(state.queue);
    }

    unwatch(client_fd);
    state.in_transaction = false;
    state.queue.clear();
    state.dirty = false;
    return true;
}

void TransactionManager::on_key_modified(const std::string& key) {
    auto it = key_to_watchers_.find(key);
    if (it == key_to_watchers_.end()) return;

    for (socket_t client_fd : it->second) {
        auto s_it = client_states_.find(client_fd);
        if (s_it != client_states_.end()) {
            s_it->second.dirty = true;
        }
    }
}

void TransactionManager::client_disconnected(socket_t client_fd) {
    unwatch(client_fd);
    client_states_.erase(client_fd);
}

void TransactionManager::clear() {
    client_states_.clear();
    key_to_watchers_.clear();
}

} // namespace redis
