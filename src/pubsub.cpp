#include "pubsub.hpp"
#include "resp_parser.hpp"

namespace redis {

PubSubManager::PubSubManager() = default;

size_t PubSubManager::subscribe(socket_t client_fd, const std::string& channel) {
    channel_to_clients_[channel].insert(client_fd);
    auto& channels = client_to_channels_[client_fd];
    channels.insert(channel);
    return channels.size();
}

size_t PubSubManager::unsubscribe(socket_t client_fd, const std::string& channel) {
    auto client_it = client_to_channels_.find(client_fd);
    if (client_it == client_to_channels_.end()) {
        return 0;
    }

    client_it->second.erase(channel);
    size_t remaining = client_it->second.size();
    if (client_it->second.empty()) {
        client_to_channels_.erase(client_it);
    }

    auto chan_it = channel_to_clients_.find(channel);
    if (chan_it != channel_to_clients_.end()) {
        chan_it->second.erase(client_fd);
        if (chan_it->second.empty()) {
            channel_to_clients_.erase(chan_it);
        }
    }

    return remaining;
}

std::vector<std::pair<std::string, size_t>> PubSubManager::unsubscribe_all(socket_t client_fd) {
    std::vector<std::pair<std::string, size_t>> result;
    auto client_it = client_to_channels_.find(client_fd);
    if (client_it == client_to_channels_.end()) {
        return result;
    }

    std::vector<std::string> channels_to_remove(client_it->second.begin(), client_it->second.end());
    for (const auto& ch : channels_to_remove) {
        size_t rem = unsubscribe(client_fd, ch);
        result.emplace_back(ch, rem);
    }

    return result;
}

size_t PubSubManager::publish(const std::string& channel, const std::string& message, const MessageSender& send_fn) {
    auto chan_it = channel_to_clients_.find(channel);
    if (chan_it == channel_to_clients_.end() || chan_it->second.empty()) {
        return 0;
    }

    std::string serialized = RespSerializer::string_array({"message", channel, message});
    size_t count = 0;

    for (socket_t subscriber : chan_it->second) {
        if (send_fn) {
            send_fn(subscriber, serialized);
            count++;
        }
    }

    return count;
}

bool PubSubManager::is_subscribed(socket_t client_fd) const {
    auto it = client_to_channels_.find(client_fd);
    return it != client_to_channels_.end() && !it->second.empty();
}

size_t PubSubManager::get_client_subscription_count(socket_t client_fd) const {
    auto it = client_to_channels_.find(client_fd);
    return (it != client_to_channels_.end()) ? it->second.size() : 0;
}

void PubSubManager::client_disconnected(socket_t client_fd) {
    auto it = client_to_channels_.find(client_fd);
    if (it == client_to_channels_.end()) return;

    for (const auto& channel : it->second) {
        auto chan_it = channel_to_clients_.find(channel);
        if (chan_it != channel_to_clients_.end()) {
            chan_it->second.erase(client_fd);
            if (chan_it->second.empty()) {
                channel_to_clients_.erase(chan_it);
            }
        }
    }

    client_to_channels_.erase(it);
}

void PubSubManager::clear() {
    channel_to_clients_.clear();
    client_to_channels_.clear();
}

} // namespace redis
