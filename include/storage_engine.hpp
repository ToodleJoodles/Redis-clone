#pragma once

#include "skip_list.hpp"
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>
#include <optional>
#include <cstdint>

namespace redis {

enum class DataType {
    String,
    SortedSet
};

struct SortedSetData {
    SkipList sl;
    std::unordered_map<std::string, double> dict;
};

struct Entry {
    DataType type{DataType::String};
    std::string string_val;
    SortedSetData zset_val;
    int64_t expire_at_ms{-1}; // -1 means no TTL
};

class StorageEngine {
public:
    using KeyChangeCallback = std::function<void(const std::string& key)>;

    StorageEngine();

    void set_key_change_callback(KeyChangeCallback cb) {
        key_change_callback_ = std::move(cb);
    }

    // Expiry
    bool is_expired(const std::string& key) const;
    bool check_and_evict_passive(const std::string& key);
    size_t active_eviction_step(size_t sample_size = 20, double threshold_pct = 0.25);

    // String commands
    bool get(const std::string& key, std::string& value);
    void set(const std::string& key, const std::string& value, int64_t ttl_ms = -1);
    size_t del(const std::vector<std::string>& keys);
    size_t exists(const std::vector<std::string>& keys);
    bool incr_by(const std::string& key, int64_t delta, int64_t& new_val, std::string& err);

    // TTL commands
    int64_t ttl(const std::string& key, bool in_millis = false);
    bool expire(const std::string& key, int64_t ttl_ms);
    bool persist(const std::string& key);
    std::string type(const std::string& key);

    // Sorted Set commands
    bool zadd(const std::string& key, const std::vector<std::pair<double, std::string>>& members, size_t& added_count, std::string& err);
    bool zscore(const std::string& key, const std::string& member, double& score, bool& key_or_member_not_found, std::string& err);
    bool zrem(const std::string& key, const std::vector<std::string>& members, size_t& removed_count, std::string& err);
    bool zrange(const std::string& key, long start, long stop, bool with_scores, std::vector<std::pair<std::string, double>>& results, std::string& err);
    bool zcard(const std::string& key, size_t& card, std::string& err);
    bool zrank(const std::string& key, const std::string& member, long& rank, bool& not_found, std::string& err);

    // Persistence support & inspection
    const std::unordered_map<std::string, Entry>& get_all_entries() const { return db_; }
    void clear();
    size_t size() const { return db_.size(); }

private:
    void notify_key_changed(const std::string& key);

    std::unordered_map<std::string, Entry> db_;
    KeyChangeCallback key_change_callback_;
};

} // namespace redis
