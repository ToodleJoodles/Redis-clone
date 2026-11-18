#include "test_framework.hpp"
#include "transaction.hpp"

using namespace redis;

void test_tx_basic() {
    TransactionManager tm;
    socket_t client = 1;
    std::string err;

    ASSERT_TRUE(tm.multi(client, err));
    ASSERT_TRUE(tm.is_in_transaction(client));

    // Nested multi fails
    ASSERT_FALSE(tm.multi(client, err));
    ASSERT_EQ(err, "ERR MULTI calls can not be nested");

    tm.queue_command(client, {"SET", "a", "1"});
    tm.queue_command(client, {"GET", "a"});

    std::vector<std::vector<std::string>> queue;
    bool aborted = false;
    ASSERT_TRUE(tm.exec(client, queue, aborted, err));
    ASSERT_FALSE(aborted);
    ASSERT_EQ(queue.size(), 2);
    ASSERT_EQ(queue[0][0], "SET");
    ASSERT_EQ(queue[1][0], "GET");

    ASSERT_FALSE(tm.is_in_transaction(client));
}

void test_tx_discard() {
    TransactionManager tm;
    socket_t client = 2;
    std::string err;

    // Discard without multi fails
    ASSERT_FALSE(tm.discard(client, err));

    ASSERT_TRUE(tm.multi(client, err));
    tm.queue_command(client, {"SET", "b", "2"});
    ASSERT_TRUE(tm.discard(client, err));
    ASSERT_FALSE(tm.is_in_transaction(client));

    // Exec after discard fails
    std::vector<std::vector<std::string>> queue;
    bool aborted = false;
    ASSERT_FALSE(tm.exec(client, queue, aborted, err));
}

void test_tx_watch_modified() {
    TransactionManager tm;
    socket_t client1 = 10;
    std::string err;

    // Watch key "account"
    ASSERT_TRUE(tm.watch(client1, {"account"}, err));

    // Key is modified by another client
    tm.on_key_modified("account");

    // Client starts multi and exec
    ASSERT_TRUE(tm.multi(client1, err));
    tm.queue_command(client1, {"SET", "account", "100"});

    std::vector<std::vector<std::string>> queue;
    bool aborted = false;
    ASSERT_TRUE(tm.exec(client1, queue, aborted, err));
    ASSERT_TRUE(aborted); // Optimistic concurrency check failed!
    ASSERT_TRUE(queue.empty());
}

void test_tx_watch_unmodified() {
    TransactionManager tm;
    socket_t client = 20;
    std::string err;

    ASSERT_TRUE(tm.watch(client, {"safe_key"}, err));

    // Different key is modified
    tm.on_key_modified("other_key");

    ASSERT_TRUE(tm.multi(client, err));
    tm.queue_command(client, {"SET", "safe_key", "ok"});

    std::vector<std::vector<std::string>> queue;
    bool aborted = false;
    ASSERT_TRUE(tm.exec(client, queue, aborted, err));
    ASSERT_FALSE(aborted);
    ASSERT_EQ(queue.size(), 1);
}

int main() {
    test::run_test("Transaction Basic Multi/Exec", test_tx_basic);
    test::run_test("Transaction Discard", test_tx_discard);
    test::run_test("Transaction Watch Abort", test_tx_watch_modified);
    test::run_test("Transaction Watch Commit", test_tx_watch_unmodified);
    return test::summarize();
}
