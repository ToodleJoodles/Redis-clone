#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <functional>
#include "net_compat.hpp"

namespace redis {

class PubSubManager {
public:
    using MessageSender = std::function<void(socket_t client_fd, const std::string& serialized_msg)>;

    PubSubManager();

    // Returns new total channel count for this client
    size_t subscribe(socket_t client_fd, const std::string& channel);

    // Returns new total channel count for this client. Returns 0 if client was not subscribed.
    size_t unsubscribe(socket_t client_fd, const std::string& channel);

    // Unsubscribes from all channels. Returns list of {channel, remaining_count}
    std::vector<std::pair<std::string, size_t>> unsubscribe_all(socket_t client_fd);

    // Publishes message to all clients in channel, using send_fn. Returns number of subscribers reached.
    size_t publish(const std::string& channel, const std::string& message, const MessageSender& send_fn);

    bool is_subscribed(socket_t client_fd) const;
    size_t get_client_subscription_count(socket_t client_fd) const;
    void client_disconnected(socket_t client_fd);

    void clear();

private:
    std::unordered_map<std::string, std::unordered_set<socket_t>> channel_to_clients_;
    std::unordered_map<socket_t, std::unordered_set<std::string>> client_to_channels_;
};

} // namespace redis
