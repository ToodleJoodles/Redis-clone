#include "rdb.hpp"
#include "common.hpp"
#include <fstream>
#include <cstring>
#include <iostream>

namespace redis {

namespace {

bool decompress_lzf(const uint8_t* in, size_t in_len, uint8_t* out, size_t out_len) {
    const uint8_t* in_end = in + in_len;
    uint8_t* out_end = out + out_len;
    uint8_t* op = out;
    const uint8_t* ip = in;

    while (ip < in_end) {
        unsigned int ctrl = *ip++;
        if (ctrl < 32) {
            ctrl++;
            if (op + ctrl > out_end || ip + ctrl > in_end) return false;
            std::memcpy(op, ip, ctrl);
            op += ctrl;
            ip += ctrl;
        } else {
            unsigned int len = ctrl >> 5;
            int ref = static_cast<int>(op - out) - static_cast<int>((ctrl & 0x1f) << 8) - 1;
            if (len == 7) {
                if (ip >= in_end) return false;
                len += *ip++;
            }
            if (ip >= in_end) return false;
            ref -= *ip++;
            len += 2;

            if (op + len > out_end || ref < 0) return false;
            const uint8_t* ref_ptr = out + ref;
            while (len--) {
                *op++ = *ref_ptr++;
            }
        }
    }
    return op == out_end;
}

} // namespace

uint64_t RdbParser::crc64(uint64_t crc, const uint8_t* data, size_t len) {
    // Simple 64-bit poly checksum
    static const uint64_t poly = 0xad93d23594c935a9ULL;
    for (size_t i = 0; i < len; ++i) {
        crc ^= static_cast<uint64_t>(data[i]);
        for (int b = 0; b < 8; ++b) {
            if (crc & 1) {
                crc = (crc >> 1) ^ poly;
            } else {
                crc >>= 1;
            }
        }
    }
    return crc;
}

bool RdbParser::read_length(const uint8_t*& ptr, const uint8_t* end, uint64_t& len, bool& is_special) {
    if (ptr >= end) return false;
    uint8_t b = *ptr++;
    int type = (b & 0xC0) >> 6;
    is_special = false;

    if (type == 0) { // 00: 6-bit length
        len = b & 0x3F;
        return true;
    } else if (type == 1) { // 01: 14-bit length
        if (ptr >= end) return false;
        len = ((b & 0x3F) << 8) | (*ptr++);
        return true;
    } else if (type == 2) { // 10: 32-bit length
        if (ptr + 4 > end) return false;
        len = (static_cast<uint64_t>(ptr[0]) << 24) |
              (static_cast<uint64_t>(ptr[1]) << 16) |
              (static_cast<uint64_t>(ptr[2]) << 8) |
              static_cast<uint64_t>(ptr[3]);
        ptr += 4;
        return true;
    } else { // 11: special encoding
        is_special = true;
        len = b & 0x3F;
        return true;
    }
}

bool RdbParser::read_string(const uint8_t*& ptr, const uint8_t* end, std::string& out) {
    uint64_t len = 0;
    bool is_special = false;
    if (!read_length(ptr, end, len, is_special)) return false;

    if (!is_special) {
        if (ptr + len > end) return false;
        out.assign(reinterpret_cast<const char*>(ptr), static_cast<size_t>(len));
        ptr += len;
        return true;
    }

    if (len == 0) { // 8-bit integer
        if (ptr >= end) return false;
        int8_t val = static_cast<int8_t>(*ptr++);
        out = std::to_string(val);
        return true;
    } else if (len == 1) { // 16-bit integer
        if (ptr + 2 > end) return false;
        int16_t val = static_cast<int16_t>(ptr[0] | (ptr[1] << 8));
        ptr += 2;
        out = std::to_string(val);
        return true;
    } else if (len == 2) { // 32-bit integer
        if (ptr + 4 > end) return false;
        int32_t val = static_cast<int32_t>(ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24));
        ptr += 4;
        out = std::to_string(val);
        return true;
    } else if (len == 3) { // LZF compressed string
        uint64_t clen = 0, ulen = 0;
        bool sp = false;
        if (!read_length(ptr, end, clen, sp) || sp) return false;
        if (!read_length(ptr, end, ulen, sp) || sp) return false;
        if (ptr + clen > end) return false;

        std::vector<uint8_t> decompressed(ulen);
        if (!decompress_lzf(ptr, clen, decompressed.data(), ulen)) {
            return false;
        }
        ptr += clen;
        out.assign(reinterpret_cast<const char*>(decompressed.data()), decompressed.size());
        return true;
    }
    return false;
}

bool RdbParser::load(const std::string& filepath, StorageEngine& engine) {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs.is_open()) {
        return false;
    }

    std::vector<uint8_t> buffer((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    ifs.close();

    if (buffer.size() < 9) { // At least "REDIS0001"
        return false;
    }

    const uint8_t* ptr = buffer.data();
    const uint8_t* end = buffer.data() + buffer.size();

    // Check magic "REDIS"
    if (std::memcmp(ptr, "REDIS", 5) != 0) {
        return false;
    }
    ptr += 5;

    // Check version (4 bytes ASCII)
    int version = (ptr[0] - '0') * 1000 + (ptr[1] - '0') * 100 + (ptr[2] - '0') * 10 + (ptr[3] - '0');
    if (version < 1 || version > 9999) {
        return false;
    }
    ptr += 4;

    int64_t expire_time_ms = -1;

    while (ptr < end) {
        uint8_t op = *ptr++;

        if (op == RDB_OPCODE_EOF) {
            break;
        }

        if (op == RDB_OPCODE_SELECTDB) {
            uint64_t db_num = 0;
            bool sp = false;
            if (!read_length(ptr, end, db_num, sp)) return false;
            continue;
        }

        if (op == RDB_OPCODE_RESIZEDB) {
            uint64_t db_size = 0, exp_size = 0;
            bool sp = false;
            if (!read_length(ptr, end, db_size, sp)) return false;
            if (!read_length(ptr, end, exp_size, sp)) return false;
            continue;
        }

        if (op == RDB_OPCODE_AUX) {
            std::string aux_key, aux_val;
            if (!read_string(ptr, end, aux_key)) return false;
            if (!read_string(ptr, end, aux_val)) return false;
            continue;
        }

        if (op == RDB_OPCODE_EXPIRETIME) {
            if (ptr + 4 > end) return false;
            uint32_t sec = static_cast<uint32_t>(ptr[0]) |
                           (static_cast<uint32_t>(ptr[1]) << 8) |
                           (static_cast<uint32_t>(ptr[2]) << 16) |
                           (static_cast<uint32_t>(ptr[3]) << 24);
            ptr += 4;
            expire_time_ms = static_cast<int64_t>(sec) * 1000;
            if (ptr >= end) return false;
            op = *ptr++; // next is value type
        } else if (op == RDB_OPCODE_EXPIRETIME_MS) {
            if (ptr + 8 > end) return false;
            uint64_t ms = 0;
            for (int i = 0; i < 8; ++i) {
                ms |= (static_cast<uint64_t>(ptr[i]) << (i * 8));
            }
            ptr += 8;
            expire_time_ms = static_cast<int64_t>(ms);
            if (ptr >= end) return false;
            op = *ptr++; // next is value type
        }

        // Value parsing
        std::string key;
        if (!read_string(ptr, end, key)) return false;

        int64_t remaining_ttl = -1;
        if (expire_time_ms != -1) {
            remaining_ttl = expire_time_ms - current_time_ms();
            if (remaining_ttl <= 0) {
                // Key already expired
                expire_time_ms = -1;
                continue;
            }
        }

        if (op == RDB_TYPE_STRING) {
            std::string val;
            if (!read_string(ptr, end, val)) return false;
            engine.set(key, val, remaining_ttl);
        } else if (op == RDB_TYPE_ZSET || op == RDB_TYPE_ZSET_2) {
            uint64_t count = 0;
            bool sp = false;
            if (!read_length(ptr, end, count, sp)) return false;

            std::vector<std::pair<double, std::string>> members;
            members.reserve(static_cast<size_t>(count));

            for (uint64_t i = 0; i < count; ++i) {
                std::string member;
                if (!read_string(ptr, end, member)) return false;
                double score = 0.0;
                if (op == RDB_TYPE_ZSET) {
                    std::string score_str;
                    if (!read_string(ptr, end, score_str)) return false;
                    score = std::stod(score_str);
                } else {
                    if (ptr + 8 > end) return false;
                    std::memcpy(&score, ptr, 8);
                    ptr += 8;
                }
                members.emplace_back(score, member);
            }

            size_t added = 0;
            std::string err;
            engine.zadd(key, members, added, err);
            if (remaining_ttl > 0) {
                engine.expire(key, remaining_ttl);
            }
        } else {
            // Unsupported type in RDB, stop or skip
            return false;
        }

        expire_time_ms = -1;
    }

    return true;
}

bool RdbParser::write_length(std::vector<uint8_t>& buf, uint64_t len) {
    if (len < (1 << 6)) {
        buf.push_back(static_cast<uint8_t>(len));
    } else if (len < (1 << 14)) {
        buf.push_back(static_cast<uint8_t>((len >> 8) | 0x40));
        buf.push_back(static_cast<uint8_t>(len & 0xFF));
    } else {
        buf.push_back(0x80);
        buf.push_back(static_cast<uint8_t>((len >> 24) & 0xFF));
        buf.push_back(static_cast<uint8_t>((len >> 16) & 0xFF));
        buf.push_back(static_cast<uint8_t>((len >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(len & 0xFF));
    }
    return true;
}

bool RdbParser::write_string(std::vector<uint8_t>& buf, const std::string& s) {
    write_length(buf, s.size());
    buf.insert(buf.end(), s.begin(), s.end());
    return true;
}

bool RdbParser::save(const std::string& filepath, const StorageEngine& engine) {
    std::vector<uint8_t> buf;
    // Magic and version "REDIS0009"
    const char* magic = "REDIS0009";
    buf.insert(buf.end(), magic, magic + 9);

    // SELECTDB 0
    buf.push_back(RDB_OPCODE_SELECTDB);
    write_length(buf, 0);

    // RESIZEDB
    buf.push_back(RDB_OPCODE_RESIZEDB);
    write_length(buf, engine.size());
    write_length(buf, 0);

    const auto& entries = engine.get_all_entries();
    for (const auto& [key, entry] : entries) {
        if (entry.expire_at_ms != -1) {
            buf.push_back(RDB_OPCODE_EXPIRETIME_MS);
            uint64_t ms = static_cast<uint64_t>(entry.expire_at_ms);
            for (int i = 0; i < 8; ++i) {
                buf.push_back(static_cast<uint8_t>((ms >> (i * 8)) & 0xFF));
            }
        }

        if (entry.type == DataType::String) {
            buf.push_back(RDB_TYPE_STRING);
            write_string(buf, key);
            write_string(buf, entry.string_val);
        } else if (entry.type == DataType::SortedSet) {
            buf.push_back(RDB_TYPE_ZSET);
            write_string(buf, key);
            const auto& dict = entry.zset_val.dict;
            write_length(buf, dict.size());
            for (const auto& [member, score] : dict) {
                write_string(buf, member);
                write_string(buf, double_to_string(score));
            }
        }
    }

    // EOF
    buf.push_back(RDB_OPCODE_EOF);

    // 8-byte CRC64
    uint64_t crc = crc64(0, buf.data(), buf.size());
    for (int i = 0; i < 8; ++i) {
        buf.push_back(static_cast<uint8_t>((crc >> (i * 8)) & 0xFF));
    }

    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs.is_open()) return false;
    ofs.write(reinterpret_cast<const char*>(buf.data()), buf.size());
    return ofs.good();
}

} // namespace redis
