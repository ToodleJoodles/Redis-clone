#include "aof.hpp"
#include "resp_parser.hpp"
#include "common.hpp"
#include <fstream>
#include <iostream>

#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

namespace redis {

AofEngine::AofEngine(std::string filepath, AofFsyncPolicy policy)
    : filepath_(std::move(filepath)), policy_(policy) {}

AofEngine::~AofEngine() {
    close();
}

bool AofEngine::open() {
    std::lock_guard<std::mutex> lock(mtx_);
    file_ = std::fopen(filepath_.c_str(), "a+b");
    if (!file_) {
        return false;
    }

    if (policy_ == AofFsyncPolicy::EverySec) {
        running_ = true;
        fsync_thread_ = std::thread(&AofEngine::background_fsync_worker, this);
    }
    return true;
}

void AofEngine::close() {
    if (running_) {
        running_ = false;
        cv_.notify_all();
        if (fsync_thread_.joinable()) {
            fsync_thread_.join();
        }
    }

    std::lock_guard<std::mutex> lock(mtx_);
    if (file_) {
        do_fsync();
        std::fclose(file_);
        file_ = nullptr;
    }
}

void AofEngine::do_fsync() {
    if (!file_) return;
    std::fflush(file_);
#if defined(_WIN32)
    _commit(_fileno(file_));
#else
    fdatasync(fileno(file_));
#endif
}

void AofEngine::append_command(const std::vector<std::string>& cmd) {
    if (cmd.empty()) return;
    std::string serialized = RespSerializer::command(cmd);

    std::lock_guard<std::mutex> lock(mtx_);
    if (!file_) return;

    std::fwrite(serialized.data(), 1, serialized.size(), file_);
    pending_writes_ = true;

    if (policy_ == AofFsyncPolicy::Always) {
        do_fsync();
        pending_writes_ = false;
    }
}

void AofEngine::flush_and_fsync() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (file_) {
        do_fsync();
        pending_writes_ = false;
    }
}

void AofEngine::background_fsync_worker() {
    while (running_) {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait_for(lock, std::chrono::seconds(1), [this] {
            return !running_;
        });

        if (file_ && pending_writes_) {
            do_fsync();
            pending_writes_ = false;
        }
    }
}

bool AofEngine::replay(const std::string& filepath, StorageEngine& engine) {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs.is_open()) {
        return false;
    }

    std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    ifs.close();

    std::string_view input = content;
    while (!input.empty()) {
        RespValue val;
        size_t consumed = 0;
        ParseStatus st = RespParser::parse(input, val, consumed);
        if (st != ParseStatus::Success || consumed == 0) {
            break;
        }
        input.remove_prefix(consumed);

        if (val.type != RespType::Array || val.elements.empty()) {
            continue;
        }

        std::vector<std::string> args;
        args.reserve(val.elements.size());
        for (const auto& elem : val.elements) {
            args.push_back(elem.str_val);
        }

        if (args.empty()) continue;
        std::string cmd = to_upper(args[0]);

        if (cmd == "SET" && args.size() >= 3) {
            int64_t ttl_ms = -1;
            for (size_t i = 3; i < args.size(); ++i) {
                std::string opt = to_upper(args[i]);
                if (opt == "EX" && i + 1 < args.size()) {
                    ttl_ms = std::stoll(args[++i]) * 1000;
                } else if (opt == "PX" && i + 1 < args.size()) {
                    ttl_ms = std::stoll(args[++i]);
                }
            }
            engine.set(args[1], args[2], ttl_ms);
        } else if (cmd == "DEL" && args.size() >= 2) {
            std::vector<std::string> keys(args.begin() + 1, args.end());
            engine.del(keys);
        } else if (cmd == "INCR" && args.size() == 2) {
            int64_t nv = 0;
            std::string err;
            engine.incr_by(args[1], 1, nv, err);
        } else if (cmd == "DECR" && args.size() == 2) {
            int64_t nv = 0;
            std::string err;
            engine.incr_by(args[1], -1, nv, err);
        } else if (cmd == "INCRBY" && args.size() == 3) {
            int64_t nv = 0;
            std::string err;
            engine.incr_by(args[1], std::stoll(args[2]), nv, err);
        } else if (cmd == "DECRBY" && args.size() == 3) {
            int64_t nv = 0;
            std::string err;
            engine.incr_by(args[1], -std::stoll(args[2]), nv, err);
        } else if (cmd == "EXPIRE" && args.size() == 3) {
            engine.expire(args[1], std::stoll(args[2]) * 1000);
        } else if (cmd == "PEXPIRE" && args.size() == 3) {
            engine.expire(args[1], std::stoll(args[2]));
        } else if (cmd == "PERSIST" && args.size() == 2) {
            engine.persist(args[1]);
        } else if (cmd == "ZADD" && args.size() >= 4) {
            std::vector<std::pair<double, std::string>> members;
            for (size_t i = 2; i + 1 < args.size(); i += 2) {
                members.emplace_back(std::stod(args[i]), args[i + 1]);
            }
            size_t added = 0;
            std::string err;
            engine.zadd(args[1], members, added, err);
        } else if (cmd == "ZREM" && args.size() >= 3) {
            std::vector<std::string> members(args.begin() + 2, args.end());
            size_t removed = 0;
            std::string err;
            engine.zrem(args[1], members, removed, err);
        }
    }

    return true;
}

} // namespace redis
