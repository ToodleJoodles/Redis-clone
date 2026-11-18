#include "test_framework.hpp"
#include "resp_parser.hpp"

using namespace redis;

void test_simple_string() {
    std::string_view input = "+OK\r\n";
    RespValue val;
    size_t consumed = 0;
    ParseStatus st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(consumed, 5);
    ASSERT_EQ(val.type, RespType::SimpleString);
    ASSERT_EQ(val.str_val, "OK");
    ASSERT_EQ(RespSerializer::serialize(val), "+OK\r\n");
}

void test_error() {
    std::string_view input = "-ERR unknown command\r\n";
    RespValue val;
    size_t consumed = 0;
    ParseStatus st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::Error);
    ASSERT_EQ(val.str_val, "ERR unknown command");
    ASSERT_EQ(RespSerializer::serialize(val), "-ERR unknown command\r\n");
}

void test_integer() {
    std::string_view input = ":1024\r\n:-55\r\n";
    RespValue val;
    size_t consumed = 0;
    ParseStatus st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::Integer);
    ASSERT_EQ(val.int_val, 1024);
    ASSERT_EQ(consumed, 7);

    input.remove_prefix(consumed);
    st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::Integer);
    ASSERT_EQ(val.int_val, -55);
}

void test_bulk_string() {
    // Normal bulk string
    std::string_view input = "$6\r\nfoobar\r\n";
    RespValue val;
    size_t consumed = 0;
    ParseStatus st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::BulkString);
    ASSERT_EQ(val.str_val, "foobar");
    ASSERT_EQ(consumed, 12);

    // Empty bulk string
    input = "$0\r\n\r\n";
    st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::BulkString);
    ASSERT_EQ(val.str_val, "");
    ASSERT_EQ(consumed, 6);

    // Null bulk string
    input = "$-1\r\n";
    st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::NullBulkString);
    ASSERT_TRUE(val.is_null());
    ASSERT_EQ(consumed, 5);
}

void test_array() {
    // Array with bulk strings
    std::string_view input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    RespValue val;
    size_t consumed = 0;
    ParseStatus st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::Array);
    ASSERT_EQ(val.elements.size(), 2);
    ASSERT_EQ(val.elements[0].str_val, "foo");
    ASSERT_EQ(val.elements[1].str_val, "bar");

    // Null array
    input = "*-1\r\n";
    st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::NullArray);
    ASSERT_TRUE(val.is_null());

    // Empty array
    input = "*0\r\n";
    st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::Array);
    ASSERT_EQ(val.elements.size(), 0);
}

void test_pipelining() {
    std::string buffer = "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n:123\r\n";
    bool error = false;
    std::vector<RespValue> pipeline = RespParser::parse_pipeline(buffer, error);

    ASSERT_FALSE(error);
    ASSERT_EQ(pipeline.size(), 3);
    ASSERT_TRUE(buffer.empty());

    ASSERT_EQ(pipeline[0].type, RespType::Array);
    ASSERT_EQ(pipeline[0].elements[0].str_val, "PING");

    ASSERT_EQ(pipeline[1].type, RespType::Array);
    ASSERT_EQ(pipeline[1].elements[0].str_val, "ECHO");
    ASSERT_EQ(pipeline[1].elements[1].str_val, "hello");

    ASSERT_EQ(pipeline[2].type, RespType::Integer);
    ASSERT_EQ(pipeline[2].int_val, 123);
}

void test_incomplete_chunks() {
    std::string buffer = "*2\r\n$3\r\nfoo\r\n";
    bool error = false;
    // Missing second element
    auto pipeline = RespParser::parse_pipeline(buffer, error);
    ASSERT_FALSE(error);
    ASSERT_EQ(pipeline.size(), 0);
    ASSERT_EQ(buffer, "*2\r\n$3\r\nfoo\r\n");

    // Append second element
    buffer += "$3\r\nbar\r\n";
    pipeline = RespParser::parse_pipeline(buffer, error);
    ASSERT_FALSE(error);
    ASSERT_EQ(pipeline.size(), 1);
    ASSERT_TRUE(buffer.empty());
    ASSERT_EQ(pipeline[0].elements.size(), 2);
}

void test_inline_commands() {
    std::string_view input = "PING\r\n";
    RespValue val;
    size_t consumed = 0;
    ParseStatus st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::Array);
    ASSERT_EQ(val.elements.size(), 1);
    ASSERT_EQ(val.elements[0].str_val, "PING");

    input = "SET foo \"hello world\"\r\n";
    st = RespParser::parse(input, val, consumed);
    ASSERT_EQ(st, ParseStatus::Success);
    ASSERT_EQ(val.type, RespType::Array);
    ASSERT_EQ(val.elements.size(), 3);
    ASSERT_EQ(val.elements[0].str_val, "SET");
    ASSERT_EQ(val.elements[1].str_val, "foo");
    ASSERT_EQ(val.elements[2].str_val, "hello world");
}

int main() {
    test::run_test("RESP2 Simple String", test_simple_string);
    test::run_test("RESP2 Error", test_error);
    test::run_test("RESP2 Integer", test_integer);
    test::run_test("RESP2 Bulk String", test_bulk_string);
    test::run_test("RESP2 Array", test_array);
    test::run_test("RESP2 Pipelining", test_pipelining);
    test::run_test("RESP2 Incomplete Chunks", test_incomplete_chunks);
    test::run_test("RESP2 Inline Commands", test_inline_commands);
    return test::summarize();
}
