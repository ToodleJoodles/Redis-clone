#include "test_framework.hpp"
#include "pubsub.hpp"

using namespace redis;

void test_pubsub_basic() {
    PubSubManager pm;
    socket_t client1 = 101;
    socket_t client2 = 102;

    ASSERT_EQ(pm.subscribe(client1, "news"), 1);
    ASSERT_EQ(pm.subscribe(client1, "sports"), 2);
    ASSERT_EQ(pm.subscribe(client2, "news"), 1);

    ASSERT_TRUE(pm.is_subscribed(client1));
    ASSERT_TRUE(pm.is_subscribed(client2));

    std::vector<std::pair<socket_t, std::string>> delivered;
    auto send_fn = [&](socket_t fd, const std::string& msg) {
        delivered.emplace_back(fd, msg);
    };

    // Publish to news: client1 and client2 receive
    size_t reached = pm.publish("news", "Breaking news!", send_fn);
    ASSERT_EQ(reached, 2);
    ASSERT_EQ(delivered.size(), 2);

    // Publish to sports: only client1 receives
    delivered.clear();
    reached = pm.publish("sports", "Goal scored!", send_fn);
    ASSERT_EQ(reached, 1);
    ASSERT_EQ(delivered.size(), 1);
    ASSERT_EQ(delivered[0].first, client1);

    // Publish to weather: 0 subscribers
    delivered.clear();
    reached = pm.publish("weather", "Sunny", send_fn);
    ASSERT_EQ(reached, 0);
    ASSERT_TRUE(delivered.empty());
}

void test_pubsub_unsubscribe() {
    PubSubManager pm;
    socket_t client = 201;

    pm.subscribe(client, "ch1");
    pm.subscribe(client, "ch2");
    pm.subscribe(client, "ch3");

    ASSERT_EQ(pm.unsubscribe(client, "ch2"), 2);

    auto remaining = pm.unsubscribe_all(client);
    ASSERT_EQ(remaining.size(), 2);
    ASSERT_FALSE(pm.is_subscribed(client));
}

void test_pubsub_disconnect() {
    PubSubManager pm;
    socket_t client = 301;

    pm.subscribe(client, "ch1");
    pm.client_disconnected(client);
    ASSERT_FALSE(pm.is_subscribed(client));

    size_t reached = pm.publish("ch1", "hello", [](socket_t, const std::string&) {});
    ASSERT_EQ(reached, 0);
}

int main() {
    test::run_test("PubSub Basic", test_pubsub_basic);
    test::run_test("PubSub Unsubscribe", test_pubsub_unsubscribe);
    test::run_test("PubSub Disconnect", test_pubsub_disconnect);
    return test::summarize();
}
