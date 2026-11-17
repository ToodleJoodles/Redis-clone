cmake_minimum_required(VERSION 3.16)
project(redis_clone VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable compiler warnings
if(MSVC)
    add_compile_options(/W4 /EHsc)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Threading support (pthread on POSIX)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Core library
set(REDIS_CORE_SOURCES
    src/event_loop.cpp
    src/resp_parser.cpp
    src/skip_list.cpp
    src/storage_engine.cpp
    src/pubsub.cpp
    src/transaction.cpp
    src/aof.cpp
    src/rdb.cpp
    src/server.cpp
)

add_library(redis_core STATIC ${REDIS_CORE_SOURCES})
target_include_directories(redis_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(redis_core PUBLIC Threads::Threads)

if(WIN32)
    target_link_libraries(redis_core PUBLIC ws2_32)
endif()

# Main server executable
add_executable(redis_server src/main.cpp)
target_link_libraries(redis_server PRIVATE redis_core)

# Tests
enable_testing()

set(TEST_TARGETS
    test_resp
    test_storage
    test_skiplist
    test_pubsub
    test_transaction
    test_aof
    test_rdb
    test_integration
)

foreach(test_name ${TEST_TARGETS})
    add_executable(${test_name} tests/${test_name}.cpp)
    target_link_libraries(${test_name} PRIVATE redis_core)
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()
