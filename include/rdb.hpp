#pragma once

#include "storage_engine.hpp"
#include <string>
#include <vector>
#include <cstdint>

namespace redis {

constexpr uint8_t RDB_OPCODE_AUX = 0xFA;
constexpr uint8_t RDB_OPCODE_RESIZEDB = 0xFB;
constexpr uint8_t RDB_OPCODE_EXPIRETIME_MS = 0xFC;
constexpr uint8_t RDB_OPCODE_EXPIRETIME = 0xFD;
constexpr uint8_t RDB_OPCODE_SELECTDB = 0xFE;
constexpr uint8_t RDB_OPCODE_EOF = 0xFF;

constexpr uint8_t RDB_TYPE_STRING = 0;
constexpr uint8_t RDB_TYPE_ZSET = 3;
constexpr uint8_t RDB_TYPE_ZSET_2 = 5;

class RdbParser {
public:
    static bool load(const std::string& filepath, StorageEngine& engine);
    static bool save(const std::string& filepath, const StorageEngine& engine);

private:
    static bool read_length(const uint8_t*& ptr, const uint8_t* end, uint64_t& len, bool& is_special);
    static bool read_string(const uint8_t*& ptr, const uint8_t* end, std::string& out);
    static bool write_length(std::vector<uint8_t>& buf, uint64_t len);
    static bool write_string(std::vector<uint8_t>& buf, const std::string& s);
    static uint64_t crc64(uint64_t crc, const uint8_t* data, size_t len);
};

} // namespace redis
