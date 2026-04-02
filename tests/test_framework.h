#pragma once
// 轻量级单元测试框架（无第三方依赖）
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <vector>
#include <set>

struct TestCase {
    std::string module;
    std::string name;
    std::function<void()> fn;
};

inline std::vector<TestCase>& getTests() {
    static std::vector<TestCase> tests;
    return tests;
}

inline int& testPassed() { static int v = 0; return v; }
inline int& testFailed() { static int v = 0; return v; }
inline const char*& currentTestName() { static const char* v = nullptr; return v; }

#ifndef EMQ_TEST_MODULE
#define EMQ_TEST_MODULE "default"
#endif

#define TEST(name) \
    static void test_##name(); \
    static struct Reg_##name { \
        Reg_##name() { getTests().push_back({EMQ_TEST_MODULE, #name, test_##name}); } \
    } reg_##name; \
    static void test_##name()

#define CHECK(expr) \
    do { \
        if (!(expr)) { \
            std::fprintf(stderr, "  FAIL [%s:%d] %s\n", \
                         __FILE__, __LINE__, #expr); \
            testFailed()++; \
        } else { \
            testPassed()++; \
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

inline int RUN_ALL_TESTS(int argc = 0, char** argv = nullptr) {
    std::set<std::string> modules;
    bool listOnly = false;

    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "--list") == 0 || std::strcmp(argv[i], "-l") == 0) {
            listOnly = true;
        } else if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            std::printf("Usage: %s [options] [module...]\n\n", argv[0]);
            std::printf("Options:\n");
            std::printf("  --list, -l    List available test modules\n");
            std::printf("  --help, -h    Show this help message\n");
            std::printf("\nRun without arguments to execute all tests.\n");
            std::printf("Specify one or more module names to run only those modules.\n");
            return 0;
        } else {
            modules.insert(argv[i]);
        }
    }

    if (listOnly) {
        std::set<std::string> allModules;
        for (auto& tc : getTests()) allModules.insert(tc.module);
        std::printf("Available test modules:\n");
        for (auto& m : allModules) {
            int cnt = 0;
            for (auto& tc : getTests()) if (tc.module == m) cnt++;
            std::printf("  %-20s (%d tests)\n", m.c_str(), cnt);
        }
        return 0;
    }

    int total = 0, skipped = 0;
    std::string lastModule;

    for (auto& tc : getTests()) {
        if (!modules.empty() && modules.find(tc.module) == modules.end()) {
            skipped++;
            continue;
        }
        if (tc.module != lastModule) {
            std::printf("\n===== Module: %s =====\n", tc.module.c_str());
            lastModule = tc.module;
        }
        std::printf("[TEST] %s\n", tc.name.c_str());
        currentTestName() = tc.name.c_str();
        int beforeFail = testFailed();
        tc.fn();
        if (testFailed() == beforeFail) {
            std::printf("  PASS\n");
        }
        total++;
    }

    std::printf("\n--- Results: %d passed, %d failed, %d tests run",
                testPassed(), testFailed(), total);
    if (skipped > 0) std::printf(", %d skipped", skipped);
    std::printf(" ---\n");
    return testFailed() > 0 ? 1 : 0;
}
