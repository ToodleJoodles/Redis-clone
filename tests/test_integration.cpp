#include "test_framework.hpp"
#include "server.hpp"
#include "resp_parser.hpp"

using namespace redis;

void test_server_core_commands() {
    RedisServer server;
    socket_t client = 1;

    // PING
    ASSERT_EQ(server.execute_command(client, {"PING"}), "+PONG\r\n");
    ASSERT_EQ(server.execute_command(client, {"PING", "hello"}), "$5\r\nhello\r\n");

    // ECHO
    ASSERT_EQ(server.execute_command(client, {"ECHO", "test echo"}), "$9\r\ntest echo\r\n");

    // SET and GET
    ASSERT_EQ(server.execute_command(client, {"SET", "mykey", "myval"}), "+OK\r\n");
    ASSERT_EQ(server.execute_command(client, {"GET", "mykey"}), "$5\r\nmyval\r\n");
    ASSERT_EQ(server.execute_command(client, {"GET", "nonexistent"}), "$-1\r\n");

    // EXISTS
    ASSERT_EQ(server.execute_command(client, {"EXISTS", "mykey", "nonexistent"}), ":1\r\n");

    // INCR and DECR
    ASSERT_EQ(server.execute_command(client, {"INCR", "num"}), ":1\r\n");
    ASSERT_EQ(server.execute_command(client, {"INCRBY", "num", "10"}), ":11\r\n");
    ASSERT_EQ(server.execute_command(client, {"DECR", "num"}), ":10\r\n");
    ASSERT_EQ(server.execute_command(client, {"DECRBY", "num", "5"}), ":5\r\n");

    // TYPE
    ASSERT_EQ(server.execute_command(client, {"TYPE", "mykey"}), "+string\r\n");
    ASSERT_EQ(server.execute_command(client, {"TYPE", "nonexistent"}), "+none\r\n");

    // DEL
    ASSERT_EQ(server.execute_command(client, {"DEL", "mykey", "num"}), ":2\r\n");
    ASSERT_EQ(server.execute_command(client, {"EXISTS", "mykey"}), ":0\r\n");
}

void test_server_zset_commands() {
    RedisServer server;
    socket_t client = 2;

    // ZADD
    ASSERT_EQ(server.execute_command(client, {"ZADD", "myzset", "1.5", "one", "2.5", "two"}), ":2\r\n");
    ASSERT_EQ(server.execute_command(client, {"TYPE", "myzset"}), "+zset\r\n");

    // ZCARD
    ASSERT_EQ(server.execute_command(client, {"ZCARD", "myzset"}), ":2\r\n");

    // ZSCORE
    ASSERT_EQ(server.execute_command(client, {"ZSCORE", "myzset", "two"}), "$3\r\n2.5\r\n");
    ASSERT_EQ(server.execute_command(client, {"ZSCORE", "myzset", "three"}), "$-1\r\n");

    // ZRANK
    ASSERT_EQ(server.execute_command(client, {"ZRANK", "myzset", "one"}), ":0\r\n");
    ASSERT_EQ(server.execute_command(client, {"ZRANK", "myzset", "two"}), ":1\r\n");

    // ZRANGE
    std::string range_res = server.execute_command(client, {"ZRANGE", "myzset", "0", "-1"});
    ASSERT_EQ(range_res, "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n");

    // ZRANGE WITHSCORES
    std::string range_scores = server.execute_command(client, {"ZRANGE", "myzset", "0", "-1", "WITHSCORES"});
    ASSERT_EQ(range_scores, "*4\r\n$3\r\none\r\n$3\r\n1.5\r\n$3\r\ntwo\r\n$3\r\n2.5\r\n");

    // ZREM
    ASSERT_EQ(server.execute_command(client, {"ZREM", "myzset", "one"}), ":1\r\n");
    ASSERT_EQ(server.execute_command(client, {"ZCARD", "myzset"}), ":1\r\n");
}

void test_server_transactions() {
    RedisServer server;
    socket_t client1 = 3;
    socket_t client2 = 4;

    // WATCH and abort on modification
    ASSERT_EQ(server.execute_command(client1, {"WATCH", "balance"}), "+OK\r\n");
    ASSERT_EQ(server.execute_command(client1, {"MULTI"}), "+OK\r\n");
    ASSERT_EQ(server.execute_command(client1, {"SET", "balance", "100"}), "+QUEUED\r\n");

    // Client2 modifies the watched key
    ASSERT_EQ(server.execute_command(client2, {"SET", "balance", "50"}), "+OK\r\n");

    // Client1 EXEC should abort and return null array
    ASSERT_EQ(server.execute_command(client1, {"EXEC"}), "*-1\r\n");

    // Verify balance was untouched by client1
    ASSERT_EQ(server.execute_command(client1, {"GET", "balance"}), "$2\r\n50\r\n");

    // Successful transaction
    ASSERT_EQ(server.execute_command(client1, {"MULTI"}), "+OK\r\n");
    ASSERT_EQ(server.execute_command(client1, {"SET", "k1", "v1"}), "+QUEUED\r\n");
    ASSERT_EQ(server.execute_command(client1, {"GET", "k1"}), "+QUEUED\r\n");
    std::string exec_res = server.execute_command(client1, {"EXEC"});
    ASSERT_EQ(exec_res, "*2\r\n+OK\r\n$2\r\nv1\r\n");
}

void test_server_pubsub() {
    RedisServer server;
    socket_t sub_client = 5;
    socket_t pub_client = 6;

    // Client subscribes
    std::string sub_res = server.execute_command(sub_client, {"SUBSCRIBE", "chat"});
    ASSERT_EQ(sub_res, "*3\r\n$9\r\nsubscribe\r\n$4\r\nchat\r\n:1\r\n");

    // Subscribed client is restricted
    std::string err_res = server.execute_command(sub_client, {"SET", "foo", "bar"});
    ASSERT_TRUE(err_res.rfind("-ERR", 0) == 0);

    // Publishing to channel
    ASSERT_EQ(server.execute_command(pub_client, {"PUBLISH", "chat", "hi everyone"}), ":1\r\n");
    ASSERT_EQ(server.execute_command(pub_client, {"PUBLISH", "empty_chan", "msg"}), ":0\r\n");

    // Unsubscribe
    std::string unsub_res = server.execute_command(sub_client, {"UNSUBSCRIBE", "chat"});
    ASSERT_EQ(unsub_res, "*3\r\n$11\r\nunsubscribe\r\n$4\r\nchat\r\n:0\r\n");

    // Now sub_client can execute normal commands again
    ASSERT_EQ(server.execute_command(sub_client, {"PING"}), "+PONG\r\n");
}

int main() {
    test::run_test("Integration Core Commands", test_server_core_commands);
    test::run_test("Integration Sorted Sets", test_server_zset_commands);
    test::run_test("Integration Transactions", test_server_transactions);
    test::run_test("Integration PubSub", test_server_pubsub);
    return test::summarize();
}
