#pragma once

#include "storage_engine.hpp"
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cstdio>

namespace redis {

enum class AofFsyncPolicy {
    Always,
    EverySec,
    No
};

class AofEngine {
public:
    explicit AofEngine(std::string filepath, AofFsyncPolicy policy = AofFsyncPolicy::EverySec);
    ~AofEngine();

    bool open();
    void close();

    void append_command(const std::vector<std::string>& cmd);
    void flush_and_fsync();

    // Replays commands from an AOF file into the provided StorageEngine
    static bool replay(const std::string& filepath, StorageEngine& engine);

    const std::string& get_filepath() const { return filepath_; }
    AofFsyncPolicy get_policy() const { return policy_; }

private:
    void background_fsync_worker();
    void do_fsync();

    std::string filepath_;
    AofFsyncPolicy policy_;
    FILE* file_{nullptr};

    std::mutex mtx_;
    std::condition_variable cv_;
    std::atomic<bool> running_{false};
    std::thread fsync_thread_;
    bool pending_writes_{false};
};

} // namespace redis
