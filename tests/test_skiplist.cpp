#include "test_framework.hpp"
#include "skip_list.hpp"
#include "storage_engine.hpp"

using namespace redis;

void test_skiplist_ordering() {
    SkipList sl;
    sl.insert(10.0, "alice");
    sl.insert(5.0, "bob");
    sl.insert(20.0, "charlie");
    sl.insert(10.0, "aaron"); // same score as alice, but "aaron" < "alice"

    ASSERT_EQ(sl.size(), 4);

    // Range [0, -1] should return in ascending order:
    // bob (5.0), aaron (10.0), alice (10.0), charlie (20.0)
    auto range = sl.get_range_by_rank(0, -1);
    ASSERT_EQ(range.size(), 4);
    ASSERT_EQ(range[0].first, "bob");
    ASSERT_EQ(range[1].first, "aaron");
    ASSERT_EQ(range[2].first, "alice");
    ASSERT_EQ(range[3].first, "charlie");
}

void test_skiplist_rank() {
    SkipList sl;
    sl.insert(1.0, "one");
    sl.insert(2.0, "two");
    sl.insert(3.0, "three");
    sl.insert(4.0, "four");

    ASSERT_EQ(sl.get_rank(1.0, "one"), 1);
    ASSERT_EQ(sl.get_rank(3.0, "three"), 3);
    ASSERT_EQ(sl.get_rank(4.0, "four"), 4);
    ASSERT_EQ(sl.get_rank(99.0, "nonexistent"), 0);

    SkipListNode* n = sl.get_element_by_rank(2);
    ASSERT_TRUE(n != nullptr);
    ASSERT_EQ(n->member, "two");
}

void test_skiplist_delete_and_update() {
    SkipList sl;
    sl.insert(100.0, "hero");
    sl.insert(200.0, "boss");

    ASSERT_TRUE(sl.update_score(100.0, "hero", 300.0));
    auto range = sl.get_range_by_rank(0, -1);
    ASSERT_EQ(range[0].first, "boss");
    ASSERT_EQ(range[1].first, "hero");

    ASSERT_TRUE(sl.remove(300.0, "hero"));
    ASSERT_EQ(sl.size(), 1);
    ASSERT_FALSE(sl.remove(300.0, "hero"));
}

void test_zset_storage_commands() {
    StorageEngine engine;
    size_t added = 0;
    std::string err;

    // ZADD
    ASSERT_TRUE(engine.zadd("leaderboard", {{100, "player1"}, {200, "player2"}, {150, "player3"}}, added, err));
    ASSERT_EQ(added, 3);

    // ZCARD
    size_t card = 0;
    ASSERT_TRUE(engine.zcard("leaderboard", card, err));
    ASSERT_EQ(card, 3);

    // ZSCORE
    double score = 0;
    bool not_found = false;
    ASSERT_TRUE(engine.zscore("leaderboard", "player3", score, not_found, err));
    ASSERT_FALSE(not_found);
    ASSERT_EQ(score, 150.0);

    // ZRANK (0-based)
    long rank = 0;
    ASSERT_TRUE(engine.zrank("leaderboard", "player1", rank, not_found, err));
    ASSERT_EQ(rank, 0); // lowest score is rank 0
    ASSERT_TRUE(engine.zrank("leaderboard", "player3", rank, not_found, err));
    ASSERT_EQ(rank, 1);
    ASSERT_TRUE(engine.zrank("leaderboard", "player2", rank, not_found, err));
    ASSERT_EQ(rank, 2);

    // ZRANGE
    std::vector<std::pair<std::string, double>> results;
    ASSERT_TRUE(engine.zrange("leaderboard", 0, -1, true, results, err));
    ASSERT_EQ(results.size(), 3);
    ASSERT_EQ(results[0].first, "player1");
    ASSERT_EQ(results[1].first, "player3");
    ASSERT_EQ(results[2].first, "player2");

    // ZREM
    size_t removed = 0;
    ASSERT_TRUE(engine.zrem("leaderboard", {"player1", "nonexistent"}, removed, err));
    ASSERT_EQ(removed, 1);
    ASSERT_TRUE(engine.zcard("leaderboard", card, err));
    ASSERT_EQ(card, 2);
}

int main() {
    test::run_test("SkipList Ordering", test_skiplist_ordering);
    test::run_test("SkipList Rank", test_skiplist_rank);
    test::run_test("SkipList Delete and Update", test_skiplist_delete_and_update);
    test::run_test("Sorted Set Storage Commands", test_zset_storage_commands);
    return test::summarize();
}
