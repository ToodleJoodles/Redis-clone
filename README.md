# Redis Clone (C++17)

A modular, high-performance Redis clone implemented in modern C++17 adhering to the RESP2 protocol specification.

## Architectural Overview

- **Single-Threaded Epoll Event Loop**: Non-blocking sockets using `epoll` (`epoll_create1`, `epoll_ctl`, `epoll_wait`) with support for I/O pipelining and periodic timers.
- **RESP2 Protocol Engine**: Complete parser and serializer for Simple Strings, Errors, Integers, Bulk Strings (including nulls), Arrays (including nulls/empty), and inline text commands.
- **Storage Engine**:
  - Key-value store (`GET`, `SET` with `EX`/`PX`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `DECRBY`).
  - Key expiry with passive eviction on access and active periodic randomized sampling eviction.
  - Sorted Sets (`ZADD`, `ZRANGE`, `ZSCORE`, `ZREM`, `ZCARD`, `ZRANK`) implemented using a Skip List with rank span pointers ($O(\log N)$ rank calculation and range retrieval) combined with an $O(1)$ hash map.
- **Pub/Sub Engine**: Channel subscription management (`SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`) with real-time broadcast delivery and connection lifecycle tracking.
- **Transaction Manager**: Multi-command atomic execution (`MULTI`, `EXEC`, `DISCARD`) with optimistic concurrency control (`WATCH`, `UNWATCH`) aborting execution if watched keys are modified before `EXEC`.
- **Persistence**:
  - **AOF (Append Only File)**: Logs mutating commands with a background `everysec` / `always` `fsync` worker thread and startup command replay loader.
  - **Binary RDB Parser**: Snapshot loader and serializer compatible with standard Redis binary `dump.rdb` format (magic header `REDIS0009`, opcodes, length encoding, integer string compression, sorted set records, and CRC64 checksum verification).

## Project Layout

```
├── CMakeLists.txt              # Modern CMake configured for C++17 and pthread
├── AGENTS.md                   # Project architectural guidelines & deliverables
├── include/
│   ├── common.hpp              # Utility helpers, time, string conversion
│   ├── net_compat.hpp          # POSIX epoll & sockets abstraction / Windows compatibility
│   ├── event_loop.hpp          # Epoll EventLoop and PosixSocket wrapper
│   ├── resp_parser.hpp         # RESP2 streaming parser and serializer
│   ├── skip_list.hpp           # SkipList with forward/backward pointers and level spans
│   ├── storage_engine.hpp      # In-memory storage, active & passive TTL eviction, Sorted Sets
│   ├── pubsub.hpp              # Pub/Sub channel and subscriber registry
│   ├── transaction.hpp         # MULTI/EXEC/DISCARD/WATCH manager
│   ├── aof.hpp                 # Append Only File engine and replay loader
│   ├── rdb.hpp                 # Binary RDB snapshot loader and saver
│   └── server.hpp              # Server coordinator, client session state, command dispatcher
├── src/
│   ├── event_loop.cpp
│   ├── resp_parser.cpp
│   ├── skip_list.cpp
│   ├── storage_engine.cpp
│   ├── pubsub.cpp
│   ├── transaction.cpp
│   ├── aof.cpp
│   ├── rdb.cpp
│   ├── server.cpp
│   └── main.cpp                # Server entrypoint (default 127.0.0.1:6379)
└── tests/
    ├── test_framework.hpp      # Lightweight assertion and test harness
    ├── test_resp.cpp           # RESP2 parsing, serialization, pipelining, and inline tests
    ├── test_storage.cpp        # KV, TTL, passive & active eviction tests
    ├── test_skiplist.cpp       # SkipList ordering, spans, ranks, ranges, and ZSet tests
    ├── test_pubsub.cpp         # Channel subscription, broadcast, and cleanup tests
    ├── test_transaction.cpp    # Transaction queuing, discard, and WATCH abort tests
    ├── test_aof.cpp            # AOF logging, background fsync, and replay tests
    ├── test_rdb.cpp            # Binary RDB snapshot serialization and parsing tests
    └── test_integration.cpp    # End-to-end command execution and client interaction tests
```

## Building and Testing

### Prerequisites
- CMake 3.16+
- C++17 compliant compiler (GCC 9+, Clang 10+, or MSVC 2019+)
- POSIX threads (`pthread`)

### Build
```bash
cmake -B build
cmake --build build
```

### Run Tests
```bash
ctest --test-dir build --output-on-failure
```

### Run Server
```bash
./build/redis_server --host 127.0.0.1 --port 6379
```

Available options:
- `--host <ip>`: Bind address (default: `127.0.0.1`)
- `--port <port>`: Port number (default: `6379`)
- `--aof [path]`: Enable AOF persistence (default: `appendonly.aof`)
- `--rdb <path>`: RDB snapshot path (default: `dump.rdb`)
