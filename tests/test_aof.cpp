#include "test_framework.hpp"
#include "aof.hpp"
#include "storage_engine.hpp"
#include <cstdio>

using namespace redis;

void test_aof_write_and_replay() {
    const std::string aof_file = "test_data.aof";
    std::remove(aof_file.c_str());

    {
        AofEngine aof(aof_file, AofFsyncPolicy::Always);
        ASSERT_TRUE(aof.open());

        aof.append_command({"SET", "name", "antigravity"});
        aof.append_command({"SET", "counter", "10"});
        aof.append_command({"INCR", "counter"});
        aof.append_command({"ZADD", "scores", "99.5", "alice", "100.0", "bob"});
        aof.append_command({"DEL", "name"});
        aof.close();
    }

    // Replay into a fresh StorageEngine
    StorageEngine restored_engine;
    ASSERT_TRUE(AofEngine::replay(aof_file, restored_engine));

    std::string val;
    ASSERT_FALSE(restored_engine.get("name", val)); // DEL was logged

    ASSERT_TRUE(restored_engine.get("counter", val));
    ASSERT_EQ(val, "11"); // 10 + 1

    size_t card = 0;
    std::string err;
    ASSERT_TRUE(restored_engine.zcard("scores", card, err));
    ASSERT_EQ(card, 2);

    double score = 0;
    bool not_found = false;
    ASSERT_TRUE(restored_engine.zscore("scores", "bob", score, not_found, err));
    ASSERT_FALSE(not_found);
    ASSERT_EQ(score, 100.0);

    std::remove(aof_file.c_str());
}

void test_aof_everysec_fsync() {
    const std::string aof_file = "test_everysec.aof";
    std::remove(aof_file.c_str());

    {
        AofEngine aof(aof_file, AofFsyncPolicy::EverySec);
        ASSERT_TRUE(aof.open());
        aof.append_command({"SET", "msg", "world"});
        aof.flush_and_fsync();
        aof.close();
    }

    StorageEngine engine;
    ASSERT_TRUE(AofEngine::replay(aof_file, engine));
    std::string val;
    ASSERT_TRUE(engine.get("msg", val));
    ASSERT_EQ(val, "world");

    std::remove(aof_file.c_str());
}

int main() {
    test::run_test("AOF Write and Replay", test_aof_write_and_replay);
    test::run_test("AOF EverySec Fsync", test_aof_everysec_fsync);
    return test::summarize();
}
