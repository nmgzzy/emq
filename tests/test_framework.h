#pragma once
// 轻量级单元测试框架（无第三方依赖）
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

struct TestCase {
    std::string name;
    std::function<void()> fn;
};

static std::vector<TestCase>& getTests() {
    static std::vector<TestCase> tests;
    return tests;
}

static int g_passed = 0;
static int g_failed = 0;
static const char* g_currentTest = nullptr;

#define TEST(name) \
    static void test_##name(); \
    static struct Reg_##name { \
        Reg_##name() { getTests().push_back({#name, test_##name}); } \
    } reg_##name; \
    static void test_##name()

#define CHECK(expr) \
    do { \
        if (!(expr)) { \
            std::fprintf(stderr, "  FAIL [%s:%d] %s\n", \
                         __FILE__, __LINE__, #expr); \
            g_failed++; \
        } else { \
            g_passed++; \
        } \
    } while(0)

#define CHECK_EQ(a, b) CHECK((a) == (b))
#define CHECK_NE(a, b) CHECK((a) != (b))
#define CHECK_GT(a, b) CHECK((a) >  (b))
#define CHECK_GE(a, b) CHECK((a) >= (b))
#define CHECK_LT(a, b) CHECK((a) <  (b))
#define CHECK_LE(a, b) CHECK((a) <= (b))
#define CHECK_STR_EQ(a, b) CHECK(std::string(a) == std::string(b))
#define CHECK_TRUE(expr)  CHECK(!!(expr))
#define CHECK_FALSE(expr) CHECK(!(expr))

inline int RUN_ALL_TESTS() {
    int total = 0;
    for (auto& tc : getTests()) {
        std::printf("[TEST] %s\n", tc.name.c_str());
        g_currentTest = tc.name.c_str();
        int beforeFail = g_failed;
        tc.fn();
        if (g_failed == beforeFail) {
            std::printf("  PASS\n");
        }
        total++;
    }
    std::printf("\n--- Results: %d passed, %d failed (total %d) ---\n",
                g_passed, g_failed, total);
    return g_failed > 0 ? 1 : 0;
}
