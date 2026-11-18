#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <functional>
#include <sstream>

namespace test {

inline int g_total_asserts = 0;
inline int g_failed_asserts = 0;
inline int g_total_tests = 0;
inline int g_failed_tests = 0;

inline void reset_stats() {
    g_total_asserts = 0;
    g_failed_asserts = 0;
    g_total_tests = 0;
    g_failed_tests = 0;
}

#define ASSERT_TRUE(cond) do { \
    test::g_total_asserts++; \
    if (!(cond)) { \
        test::g_failed_asserts++; \
        std::cerr << "  [FAIL] " << __FILE__ << ":" << __LINE__ \
                  << " Assertion failed: " << #cond << std::endl; \
    } \
} while(0)

#define ASSERT_FALSE(cond) ASSERT_TRUE(!(cond))

#define ASSERT_EQ(actual, expected) do { \
    test::g_total_asserts++; \
    if ((actual) != (expected)) { \
        test::g_failed_asserts++; \
        std::cerr << "  [FAIL] " << __FILE__ << ":" << __LINE__ \
                  << "\n    Expected: " << (expected) \
                  << "\n    Actual:   " << (actual) << std::endl; \
    } \
} while(0)

#define ASSERT_NE(actual, expected) do { \
    test::g_total_asserts++; \
    if ((actual) == (expected)) { \
        test::g_failed_asserts++; \
        std::cerr << "  [FAIL] " << __FILE__ << ":" << __LINE__ \
                  << " Expected not equal: " << #actual << " == " << #expected << std::endl; \
    } \
} while(0)

inline void run_test(const std::string& name, const std::function<void()>& fn) {
    g_total_tests++;
    int before_failed = g_failed_asserts;
    std::cout << "[RUN ] " << name << "..." << std::endl;
    try {
        fn();
    } catch (const std::exception& e) {
        g_failed_asserts++;
        std::cerr << "  [EXCEPTION] Caught: " << e.what() << std::endl;
    } catch (...) {
        g_failed_asserts++;
        std::cerr << "  [EXCEPTION] Unknown exception caught!" << std::endl;
    }

    if (g_failed_asserts > before_failed) {
        g_failed_tests++;
        std::cout << "[FAIL] " << name << std::endl;
    } else {
        std::cout << "[PASS] " << name << std::endl;
    }
}

inline int summarize() {
    std::cout << "\n========================================\n";
    std::cout << "Test Summary: "
              << (g_total_tests - g_failed_tests) << "/" << g_total_tests << " tests passed ("
              << (g_total_asserts - g_failed_asserts) << "/" << g_total_asserts << " assertions passed)\n";
    std::cout << "========================================\n";
    return g_failed_tests > 0 ? 1 : 0;
}

} // namespace test
