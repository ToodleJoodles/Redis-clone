#include "resp_parser.hpp"
#include <charconv>
#include <sstream>

namespace redis {

bool RespValue::operator==(const RespValue& other) const {
    if (type != other.type) return false;
    switch (type) {
        case RespType::SimpleString:
        case RespType::Error:
        case RespType::BulkString:
            return str_val == other.str_val;
        case RespType::Integer:
            return int_val == other.int_val;
        case RespType::NullBulkString:
        case RespType::NullArray:
            return true;
        case RespType::Array:
            if (elements.size() != other.elements.size()) return false;
            for (size_t i = 0; i < elements.size(); ++i) {
                if (elements[i] != other.elements[i]) return false;
            }
            return true;
    }
    return false;
}

namespace {

bool find_crlf(std::string_view sv, size_t& pos) {
    pos = sv.find("\r\n");
    return pos != std::string_view::npos;
}

bool parse_int64(std::string_view sv, int64_t& out) {
    if (sv.empty()) return false;
    size_t start = 0;
    bool neg = false;
    if (sv[0] == '-') {
        neg = true;
        start = 1;
    } else if (sv[0] == '+') {
        start = 1;
    }
    if (start >= sv.size()) return false;

    int64_t val = 0;
    for (size_t i = start; i < sv.size(); ++i) {
        if (sv[i] < '0' || sv[i] > '9') return false;
        val = val * 10 + (sv[i] - '0');
    }
    out = neg ? -val : val;
    return true;
}

ParseStatus parse_inline(std::string_view input, RespValue& out, size_t& consumed) {
    size_t line_end = input.find("\r\n");
    size_t delim_len = 2;
    if (line_end == std::string_view::npos) {
        line_end = input.find('\n');
        delim_len = 1;
        if (line_end == std::string_view::npos) {
            return ParseStatus::Incomplete;
        }
    }

    std::string_view line = input.substr(0, line_end);
    consumed = line_end + delim_len;

    std::vector<RespValue> words;
    size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) {
            ++i;
        }
        if (i >= line.size()) break;

        size_t start = i;
        if (line[i] == '"' || line[i] == '\'') {
            char quote = line[i++];
            start = i;
            while (i < line.size() && line[i] != quote) {
                ++i;
            }
            words.push_back(RespValue::make_bulk_string(std::string(line.substr(start, i - start))));
            if (i < line.size() && line[i] == quote) {
                ++i;
            }
        } else {
            while (i < line.size() && line[i] != ' ' && line[i] != '\t') {
                ++i;
            }
            words.push_back(RespValue::make_bulk_string(std::string(line.substr(start, i - start))));
        }
    }

    if (words.empty()) {
        // empty line, return empty array
        out = RespValue::make_array({});
    } else {
        out = RespValue::make_array(std::move(words));
    }
    return ParseStatus::Success;
}

} // namespace

ParseStatus RespParser::parse(std::string_view input, RespValue& out, size_t& consumed) {
    consumed = 0;
    if (input.empty()) {
        return ParseStatus::Incomplete;
    }

    char prefix = input[0];

    // Inline command handling if not starting with RESP markers
    if (prefix != '+' && prefix != '-' && prefix != ':' && prefix != '$' && prefix != '*') {
        return parse_inline(input, out, consumed);
    }

    size_t crlf_pos = 0;
    if (!find_crlf(input, crlf_pos)) {
        return ParseStatus::Incomplete;
    }

    std::string_view first_line = input.substr(1, crlf_pos - 1);

    switch (prefix) {
        case '+': {
            out = RespValue::make_simple_string(std::string(first_line));
            consumed = crlf_pos + 2;
            return ParseStatus::Success;
        }
        case '-': {
            out = RespValue::make_error(std::string(first_line));
            consumed = crlf_pos + 2;
            return ParseStatus::Success;
        }
        case ':': {
            int64_t val = 0;
            if (!parse_int64(first_line, val)) {
                return ParseStatus::Error;
            }
            out = RespValue::make_integer(val);
            consumed = crlf_pos + 2;
            return ParseStatus::Success;
        }
        case '$': {
            int64_t length = 0;
            if (!parse_int64(first_line, length)) {
                return ParseStatus::Error;
            }

            if (length == -1) {
                out = RespValue::make_null_bulk();
                consumed = crlf_pos + 2;
                return ParseStatus::Success;
            }

            if (length < -1) {
                return ParseStatus::Error;
            }

            size_t total_needed = crlf_pos + 2 + static_cast<size_t>(length) + 2;
            if (input.size() < total_needed) {
                return ParseStatus::Incomplete;
            }

            size_t data_start = crlf_pos + 2;
            if (input[data_start + static_cast<size_t>(length)] != '\r' ||
                input[data_start + static_cast<size_t>(length) + 1] != '\n') {
                return ParseStatus::Error;
            }

            out = RespValue::make_bulk_string(std::string(input.substr(data_start, length)));
            consumed = total_needed;
            return ParseStatus::Success;
        }
        case '*': {
            int64_t count = 0;
            if (!parse_int64(first_line, count)) {
                return ParseStatus::Error;
            }

            if (count == -1) {
                out = RespValue::make_null_array();
                consumed = crlf_pos + 2;
                return ParseStatus::Success;
            }

            if (count < -1) {
                return ParseStatus::Error;
            }

            size_t current_pos = crlf_pos + 2;
            std::vector<RespValue> elements;
            elements.reserve(static_cast<size_t>(count));

            for (int64_t i = 0; i < count; ++i) {
                if (current_pos >= input.size()) {
                    return ParseStatus::Incomplete;
                }
                RespValue elem;
                size_t elem_consumed = 0;
                ParseStatus st = parse(input.substr(current_pos), elem, elem_consumed);
                if (st != ParseStatus::Success) {
                    return st;
                }
                elements.push_back(std::move(elem));
                current_pos += elem_consumed;
            }

            out = RespValue::make_array(std::move(elements));
            consumed = current_pos;
            return ParseStatus::Success;
        }
        default:
            return ParseStatus::Error;
    }
}

std::vector<RespValue> RespParser::parse_pipeline(std::string& buffer, bool& had_error) {
    had_error = false;
    std::vector<RespValue> results;

    while (!buffer.empty()) {
        RespValue val;
        size_t consumed = 0;
        ParseStatus st = parse(buffer, val, consumed);

        if (st == ParseStatus::Success) {
            results.push_back(std::move(val));
            buffer.erase(0, consumed);
        } else if (st == ParseStatus::Incomplete) {
            break;
        } else {
            had_error = true;
            size_t next_line = buffer.find("\r\n");
            if (next_line != std::string::npos) {
                buffer.erase(0, next_line + 2);
            } else {
                buffer.clear();
            }
            break;
        }
    }
    return results;
}

// ---------------- RespSerializer ----------------

std::string RespSerializer::serialize(const RespValue& value) {
    switch (value.type) {
        case RespType::SimpleString:
            return simple_string(value.str_val);
        case RespType::Error:
            return "-" + value.str_val + "\r\n";
        case RespType::Integer:
            return integer(value.int_val);
        case RespType::BulkString:
            return bulk_string(value.str_val);
        case RespType::NullBulkString:
            return null_bulk_string();
        case RespType::Array:
            return array(value.elements);
        case RespType::NullArray:
            return null_array();
    }
    return null_bulk_string();
}

std::string RespSerializer::ok() {
    return "+OK\r\n";
}

std::string RespSerializer::pong() {
    return "+PONG\r\n";
}

std::string RespSerializer::simple_string(std::string_view s) {
    std::string res;
    res.reserve(s.size() + 3);
    res.append("+");
    res.append(s);
    res.append("\r\n");
    return res;
}

std::string RespSerializer::queued() {
    return "+QUEUED\r\n";
}

std::string RespSerializer::error(std::string_view err) {
    std::string s;
    s.reserve(err.size() + 8);
    s.append("-ERR ");
    s.append(err);
    s.append("\r\n");
    return s;
}

std::string RespSerializer::custom_error(std::string_view prefix, std::string_view msg) {
    std::string s;
    s.reserve(prefix.size() + msg.size() + 4);
    s.append("-");
    s.append(prefix);
    if (!msg.empty()) {
        s.append(" ");
        s.append(msg);
    }
    s.append("\r\n");
    return s;
}

std::string RespSerializer::integer(int64_t val) {
    std::string s = ":" + std::to_string(val) + "\r\n";
    return s;
}

std::string RespSerializer::bulk_string(std::string_view s) {
    std::string res;
    std::string len_str = std::to_string(s.size());
    res.reserve(1 + len_str.size() + 2 + s.size() + 2);
    res.append("$");
    res.append(len_str);
    res.append("\r\n");
    res.append(s);
    res.append("\r\n");
    return res;
}

std::string RespSerializer::null_bulk_string() {
    return "$-1\r\n";
}

std::string RespSerializer::null_array() {
    return "*-1\r\n";
}

std::string RespSerializer::array(const std::vector<RespValue>& items) {
    std::string res = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& item : items) {
        res.append(serialize(item));
    }
    return res;
}

std::string RespSerializer::string_array(const std::vector<std::string>& items) {
    std::string res = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& item : items) {
        res.append(bulk_string(item));
    }
    return res;
}

std::string RespSerializer::command(const std::vector<std::string>& args) {
    return string_array(args);
}

} // namespace redis
