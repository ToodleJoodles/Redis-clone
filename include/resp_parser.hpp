#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <cstdint>
#include <iostream>

namespace redis {

enum class RespType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    NullBulkString,
    Array,
    NullArray
};

enum class ParseStatus {
    Success,
    Incomplete,
    Error
};

inline std::ostream& operator<<(std::ostream& os, ParseStatus st) {
    switch (st) {
        case ParseStatus::Success: return os << "ParseStatus::Success";
        case ParseStatus::Incomplete: return os << "ParseStatus::Incomplete";
        case ParseStatus::Error: return os << "ParseStatus::Error";
    }
    return os << "ParseStatus::Unknown";
}

inline std::ostream& operator<<(std::ostream& os, RespType t) {
    switch (t) {
        case RespType::SimpleString: return os << "RespType::SimpleString";
        case RespType::Error: return os << "RespType::Error";
        case RespType::Integer: return os << "RespType::Integer";
        case RespType::BulkString: return os << "RespType::BulkString";
        case RespType::NullBulkString: return os << "RespType::NullBulkString";
        case RespType::Array: return os << "RespType::Array";
        case RespType::NullArray: return os << "RespType::NullArray";
    }
    return os << "RespType::Unknown";
}

struct RespValue {
    RespType type{RespType::NullBulkString};
    std::string str_val;
    int64_t int_val{0};
    std::vector<RespValue> elements;

    static RespValue make_simple_string(std::string s) {
        RespValue v;
        v.type = RespType::SimpleString;
        v.str_val = std::move(s);
        return v;
    }

    static RespValue make_error(std::string err) {
        RespValue v;
        v.type = RespType::Error;
        v.str_val = std::move(err);
        return v;
    }

    static RespValue make_integer(int64_t val) {
        RespValue v;
        v.type = RespType::Integer;
        v.int_val = val;
        return v;
    }

    static RespValue make_bulk_string(std::string s) {
        RespValue v;
        v.type = RespType::BulkString;
        v.str_val = std::move(s);
        return v;
    }

    static RespValue make_null_bulk() {
        RespValue v;
        v.type = RespType::NullBulkString;
        return v;
    }

    static RespValue make_array(std::vector<RespValue> elems) {
        RespValue v;
        v.type = RespType::Array;
        v.elements = std::move(elems);
        return v;
    }

    static RespValue make_null_array() {
        RespValue v;
        v.type = RespType::NullArray;
        return v;
    }

    bool is_null() const {
        return type == RespType::NullBulkString || type == RespType::NullArray;
    }

    bool operator==(const RespValue& other) const;
    bool operator!=(const RespValue& other) const { return !(*this == other); }
};

class RespParser {
public:
    static ParseStatus parse(std::string_view input, RespValue& out, size_t& consumed);
    static std::vector<RespValue> parse_pipeline(std::string& buffer, bool& had_error);
};

class RespSerializer {
public:
    static std::string serialize(const RespValue& value);
    static std::string ok();
    static std::string pong();
    static std::string simple_string(std::string_view s);
    static std::string queued();
    static std::string error(std::string_view err);
    static std::string custom_error(std::string_view prefix, std::string_view msg);
    static std::string integer(int64_t val);
    static std::string bulk_string(std::string_view s);
    static std::string null_bulk_string();
    static std::string null_array();
    static std::string array(const std::vector<RespValue>& items);
    static std::string string_array(const std::vector<std::string>& items);
    static std::string command(const std::vector<std::string>& args);
};

} // namespace redis
