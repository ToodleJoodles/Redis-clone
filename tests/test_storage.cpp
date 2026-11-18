#include "test_framework.hpp"
#include "storage_engine.hpp"
#include <thread>
#include <chrono>

using namespace redis;

void test_basic_kv() {
    StorageEngine engine;
    std::string val;
    ASSERT_FALSE(engine.get("k1", val));

    engine.set("k1", "v1");
    ASSERT_TRUE(engine.get("k1", val));
    ASSERT_EQ(val, "v1");

    ASSERT_EQ(engine.exists({"k1", "nonexistent"}), 1);

    ASSERT_EQ(engine.del({"k1"}), 1);
    ASSERT_FALSE(engine.get("k1", val));
    ASSERT_EQ(engine.del({"k1"}), 0);
}

void test_incr_decr() {
    StorageEngine engine;
    int64_t new_val = 0;
    std::string err;

    // INCR creates key if not exists
    ASSERT_TRUE(engine.incr_by("counter", 1, new_val, err));
    ASSERT_EQ(new_val, 1);

    ASSERT_TRUE(engine.incr_by("counter", 1, new_val, err));
    ASSERT_EQ(new_val, 2);

    // DECR
    ASSERT_TRUE(engine.incr_by("counter", -1, new_val, err));
    ASSERT_EQ(new_val, 1);

    // INCRBY
    ASSERT_TRUE(engine.incr_by("counter", 10, new_val, err));
    ASSERT_EQ(new_val, 11);

    // Non-integer string returns error
    engine.set("str_key", "hello");
    ASSERT_FALSE(engine.incr_by("str_key", 1, new_val, err));
    ASSERT_FALSE(err.empty());
}

void test_ttl_passive_eviction() {
    StorageEngine engine;
    // Set with 50ms TTL
    engine.set("temp_key", "val", 50);

    std::string val;
    ASSERT_TRUE(engine.get("temp_key", val));
    ASSERT_EQ(val, "val");

    int64_t ttl_val = engine.ttl("temp_key", true);
    ASSERT_TRUE(ttl_val > 0 && ttl_val <= 50);

    // Wait 70ms for key to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(70));

    // Passive eviction occurs upon GET or EXISTS
    ASSERT_FALSE(engine.get("temp_key", val));
    ASSERT_EQ(engine.exists({"temp_key"}), 0);
    ASSERT_EQ(engine.ttl("temp_key"), -2);
}

void test_ttl_commands() {
    StorageEngine engine;
    engine.set("k2", "v2");
    ASSERT_EQ(engine.ttl("k2"), -1); // no expiry

    // EXPIRE
    ASSERT_TRUE(engine.expire("k2", 5000));
    ASSERT_TRUE(engine.ttl("k2") > 0);

    // PERSIST
    ASSERT_TRUE(engine.persist("k2"));
    ASSERT_EQ(engine.ttl("k2"), -1);
}

void test_active_eviction() {
    StorageEngine engine;
    // Insert 50 keys with 20ms TTL
    for (int i = 0; i < 50; ++i) {
        engine.set("key_" + std::to_string(i), "v", 20);
    }

    // Insert 20 persistent keys
    for (int i = 0; i < 20; ++i) {
        engine.set("perm_" + std::to_string(i), "v");
    }

    // Sleep past expiration
    std::this_thread::sleep_for(std::chrono::milliseconds(40));

    size_t evicted = engine.active_eviction_step(30, 0.25);
    ASSERT_TRUE(evicted > 0);

    // Permanent keys must still exist
    for (int i = 0; i < 20; ++i) {
        std::string val;
        ASSERT_TRUE(engine.get("perm_" + std::to_string(i), val));
    }
}

int main() {
    test::run_test("Storage KV", test_basic_kv);
    test::run_test("Storage INCR/DECR", test_incr_decr);
    test::run_test("Storage Passive Expiry", test_ttl_passive_eviction);
    test::run_test("Storage TTL Commands", test_ttl_commands);
    test::run_test("Storage Active Eviction", test_active_eviction);
    return test::summarize();
}
