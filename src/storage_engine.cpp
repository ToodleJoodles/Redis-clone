#include "storage_engine.hpp"
#include "common.hpp"
#include <random>

namespace redis {

StorageEngine::StorageEngine() = default;

void StorageEngine::notify_key_changed(const std::string& key) {
    if (key_change_callback_) {
        key_change_callback_(key);
    }
}

bool StorageEngine::is_expired(const std::string& key) const {
    auto it = db_.find(key);
    if (it == db_.end()) return false;
    if (it->second.expire_at_ms == -1) return false;
    return it->second.expire_at_ms <= current_time_ms();
}

bool StorageEngine::check_and_evict_passive(const std::string& key) {
    auto it = db_.find(key);
    if (it == db_.end()) return false;
    if (it->second.expire_at_ms != -1 && it->second.expire_at_ms <= current_time_ms()) {
        db_.erase(it);
        notify_key_changed(key);
        return true;
    }
    return false;
}

size_t StorageEngine::active_eviction_step(size_t sample_size, double threshold_pct) {
    size_t total_evicted = 0;
    std::mt19937 rng(std::random_device{}());

    for (int iter = 0; iter < 10; ++iter) {
        std::vector<std::string> expiring_keys;
        for (const auto& [k, v] : db_) {
            if (v.expire_at_ms != -1) {
                expiring_keys.push_back(k);
            }
        }

        if (expiring_keys.empty()) break;

        std::shuffle(expiring_keys.begin(), expiring_keys.end(), rng);
        size_t current_sample_size = std::min(sample_size, expiring_keys.size());

        size_t expired_count = 0;
        int64_t now = current_time_ms();

        for (size_t i = 0; i < current_sample_size; ++i) {
            const std::string& key = expiring_keys[i];
            auto it = db_.find(key);
            if (it != db_.end() && it->second.expire_at_ms != -1 && it->second.expire_at_ms <= now) {
                db_.erase(it);
                notify_key_changed(key);
                expired_count++;
                total_evicted++;
            }
        }

        if (expired_count < static_cast<size_t>(static_cast<double>(current_sample_size) * threshold_pct)) {
            break;
        }
    }

    return total_evicted;
}

bool StorageEngine::get(const std::string& key, std::string& value) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    if (it == db_.end() || it->second.type != DataType::String) {
        return false;
    }
    value = it->second.string_val;
    return true;
}

void StorageEngine::set(const std::string& key, const std::string& value, int64_t ttl_ms) {
    auto& entry = db_[key];
    entry.type = DataType::String;
    entry.string_val = value;
    entry.zset_val.sl.clear();
    entry.zset_val.dict.clear();
    if (ttl_ms > 0) {
        entry.expire_at_ms = current_time_ms() + ttl_ms;
    } else {
        entry.expire_at_ms = -1;
    }
    notify_key_changed(key);
}

size_t StorageEngine::del(const std::vector<std::string>& keys) {
    size_t count = 0;
    for (const auto& key : keys) {
        check_and_evict_passive(key);
        auto it = db_.find(key);
        if (it != db_.end()) {
            db_.erase(it);
            notify_key_changed(key);
            count++;
        }
    }
    return count;
}

size_t StorageEngine::exists(const std::vector<std::string>& keys) {
    size_t count = 0;
    for (const auto& key : keys) {
        check_and_evict_passive(key);
        if (db_.find(key) != db_.end()) {
            count++;
        }
    }
    return count;
}

bool StorageEngine::incr_by(const std::string& key, int64_t delta, int64_t& new_val, std::string& err) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    if (it == db_.end()) {
        set(key, std::to_string(delta));
        new_val = delta;
        return true;
    }

    if (it->second.type != DataType::String) {
        err = "WRONGTYPE Operation against a key holding the wrong kind of value";
        return false;
    }

    int64_t val = 0;
    try {
        size_t idx = 0;
        val = std::stoll(it->second.string_val, &idx);
        if (idx != it->second.string_val.size()) {
            err = "ERR value is not an integer or out of range";
            return false;
        }
    } catch (...) {
        err = "ERR value is not an integer or out of range";
        return false;
    }

    val += delta;
    it->second.string_val = std::to_string(val);
    new_val = val;
    notify_key_changed(key);
    return true;
}

int64_t StorageEngine::ttl(const std::string& key, bool in_millis) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    if (it == db_.end()) {
        return -2;
    }
    if (it->second.expire_at_ms == -1) {
        return -1;
    }
    int64_t diff = it->second.expire_at_ms - current_time_ms();
    if (diff < 0) {
        db_.erase(it);
        notify_key_changed(key);
        return -2;
    }
    return in_millis ? diff : ((diff + 999) / 1000);
}

bool StorageEngine::expire(const std::string& key, int64_t ttl_ms) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    if (it == db_.end()) {
        return false;
    }
    it->second.expire_at_ms = current_time_ms() + ttl_ms;
    notify_key_changed(key);
    return true;
}

bool StorageEngine::persist(const std::string& key) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    if (it == db_.end() || it->second.expire_at_ms == -1) {
        return false;
    }
    it->second.expire_at_ms = -1;
    notify_key_changed(key);
    return true;
}

std::string StorageEngine::type(const std::string& key) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    if (it == db_.end()) {
        return "none";
    }
    return (it->second.type == DataType::String) ? "string" : "zset";
}

bool StorageEngine::zadd(const std::string& key, const std::vector<std::pair<double, std::string>>& members, size_t& added_count, std::string& err) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    if (it != db_.end() && it->second.type != DataType::SortedSet) {
        err = "WRONGTYPE Operation against a key holding the wrong kind of value";
        return false;
    }

    if (it == db_.end()) {
        auto [inserted_it, _] = db_.emplace(key, Entry{});
        inserted_it->second.type = DataType::SortedSet;
        it = inserted_it;
    }

    added_count = 0;
    auto& zset = it->second.zset_val;

    for (const auto& [score, member] : members) {
        auto dict_it = zset.dict.find(member);
        if (dict_it != zset.dict.end()) {
            double old_score = dict_it->second;
            if (old_score != score) {
                zset.sl.update_score(old_score, member, score);
                dict_it->second = score;
            }
        } else {
            zset.sl.insert(score, member);
            zset.dict[member] = score;
            added_count++;
        }
    }

    notify_key_changed(key);
    return true;
}

bool StorageEngine::zscore(const std::string& key, const std::string& member, double& score, bool& key_or_member_not_found, std::string& err) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    if (it == db_.end()) {
        key_or_member_not_found = true;
        return true;
    }
    if (it->second.type != DataType::SortedSet) {
        err = "WRONGTYPE Operation against a key holding the wrong kind of value";
        return false;
    }

    const auto& dict = it->second.zset_val.dict;
    auto dict_it = dict.find(member);
    if (dict_it == dict.end()) {
        key_or_member_not_found = true;
        return true;
    }

    score = dict_it->second;
    key_or_member_not_found = false;
    return true;
}

bool StorageEngine::zrem(const std::string& key, const std::vector<std::string>& members, size_t& removed_count, std::string& err) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    removed_count = 0;
    if (it == db_.end()) {
        return true;
    }
    if (it->second.type != DataType::SortedSet) {
        err = "WRONGTYPE Operation against a key holding the wrong kind of value";
        return false;
    }

    auto& zset = it->second.zset_val;
    for (const auto& member : members) {
        auto dict_it = zset.dict.find(member);
        if (dict_it != zset.dict.end()) {
            double cur_score = dict_it->second;
            zset.sl.remove(cur_score, member);
            zset.dict.erase(dict_it);
            removed_count++;
        }
    }

    if (zset.dict.empty()) {
        db_.erase(it);
    }

    if (removed_count > 0) {
        notify_key_changed(key);
    }
    return true;
}

bool StorageEngine::zrange(const std::string& key, long start, long stop, bool with_scores, std::vector<std::pair<std::string, double>>& results, std::string& err) {
    results.clear();
    check_and_evict_passive(key);
    auto it = db_.find(key);
    if (it == db_.end()) {
        return true;
    }
    if (it->second.type != DataType::SortedSet) {
        err = "WRONGTYPE Operation against a key holding the wrong kind of value";
        return false;
    }

    results = it->second.zset_val.sl.get_range_by_rank(start, stop);
    (void)with_scores; // caller uses results based on with_scores
    return true;
}

bool StorageEngine::zcard(const std::string& key, size_t& card, std::string& err) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    card = 0;
    if (it == db_.end()) {
        return true;
    }
    if (it->second.type != DataType::SortedSet) {
        err = "WRONGTYPE Operation against a key holding the wrong kind of value";
        return false;
    }

    card = it->second.zset_val.dict.size();
    return true;
}

bool StorageEngine::zrank(const std::string& key, const std::string& member, long& rank, bool& not_found, std::string& err) {
    check_and_evict_passive(key);
    auto it = db_.find(key);
    not_found = true;
    if (it == db_.end()) {
        return true;
    }
    if (it->second.type != DataType::SortedSet) {
        err = "WRONGTYPE Operation against a key holding the wrong kind of value";
        return false;
    }

    const auto& zset = it->second.zset_val;
    auto dict_it = zset.dict.find(member);
    if (dict_it == zset.dict.end()) {
        return true;
    }

    unsigned long r = zset.sl.get_rank(dict_it->second, member);
    if (r == 0) {
        return true;
    }

    rank = static_cast<long>(r - 1); // 0-based rank
    not_found = false;
    return true;
}

void StorageEngine::clear() {
    db_.clear();
}

} // namespace redis
