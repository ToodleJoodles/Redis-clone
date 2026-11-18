#include "test_framework.hpp"
#include "rdb.hpp"
#include "storage_engine.hpp"
#include <cstdio>
#include <fstream>

using namespace redis;

void test_rdb_save_and_load() {
    const std::string rdb_file = "test_dump.rdb";
    std::remove(rdb_file.c_str());

    StorageEngine src_engine;
    src_engine.set("str_key", "hello redis");
    src_engine.set("exp_key", "will survive", 60000); // 60s TTL

    size_t added = 0;
    std::string err;
    src_engine.zadd("z_key", {{10.5, "alpha"}, {20.0, "beta"}}, added, err);

    ASSERT_TRUE(RdbParser::save(rdb_file, src_engine));

    // Load into fresh engine
    StorageEngine dest_engine;
    ASSERT_TRUE(RdbParser::load(rdb_file, dest_engine));

    std::string val;
    ASSERT_TRUE(dest_engine.get("str_key", val));
    ASSERT_EQ(val, "hello redis");

    ASSERT_TRUE(dest_engine.get("exp_key", val));
    ASSERT_EQ(val, "will survive");
    ASSERT_TRUE(dest_engine.ttl("exp_key") > 0);

    size_t card = 0;
    ASSERT_TRUE(dest_engine.zcard("z_key", card, err));
    ASSERT_EQ(card, 2);

    double score = 0;
    bool not_found = false;
    ASSERT_TRUE(dest_engine.zscore("z_key", "alpha", score, not_found, err));
    ASSERT_FALSE(not_found);
    ASSERT_EQ(score, 10.5);

    std::remove(rdb_file.c_str());
}

void test_rdb_corrupted() {
    const std::string bad_file = "bad.rdb";
    std::ofstream ofs(bad_file, std::ios::binary);
    ofs << "NOT_REDIS_MAGIC_HEADER";
    ofs.close();

    StorageEngine engine;
    ASSERT_FALSE(RdbParser::load(bad_file, engine));

    std::remove(bad_file.c_str());
}

int main() {
    test::run_test("RDB Save and Load", test_rdb_save_and_load);
    test::run_test("RDB Corrupted Header", test_rdb_corrupted);
    return test::summarize();
}
