# Project Guidelines: Redis Clone (C++17)

## Architectural Constraints
- Language Standard: C++17
- Target Environment: Linux POSIX sockets with an epoll-based single-threaded event loop
- Build System: Modern CMake (CMakeLists.txt)
- Code Quality: Clean modular separation into distinct headers/sources (server, resp_parser, storage_engine, aof, rdb)
- Avoid monolithic single-file implementations.

## Resume Deliverables
1. Single-threaded epoll event loop with non-blocking sockets and pipelining
2. RESP2 protocol parser (Simple strings, Errors, Integers, Bulk strings, Arrays)
3. Core Commands: GET, SET, DEL, EXISTS, INCR, DECR
4. Key Expiry (passive and active TTL eviction)
5. Sorted Sets (ZADD, ZRANGE, ZSCORE, ZREM) using skip list / hash map
6. Pub/Sub engine (SUBSCRIBE, PUBLISH, UNSUBSCRIBE)
7. Transactions (MULTI, EXEC, DISCARD, WATCH) with optimistic concurrency
8. Persistence:
   - AOF (Append Only File) with configurable 'everysec' fsync and replay on startup
   - Binary RDB parser capable of loading snapshot files
9. Test Suite: Comprehensive unit tests and integration tests verifying RESP2 compliance
