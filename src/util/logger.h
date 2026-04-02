#pragma once
#include <cstdio>
#include <cstdarg>
#include <ctime>
#include <mutex>
#include <string>
#include <atomic>

namespace embedmq {
namespace util {

enum class LogLevel : int {
    Debug   = 0,
    Info    = 1,
    Warning = 2,
    Error   = 3,
    None    = 4,
};

class Logger {
public:
    static Logger& instance() {
        static Logger inst;
        return inst;
    }

    void setLevel(LogLevel level) { level_ = level; }
    LogLevel level() const { return level_; }

    void log(LogLevel level, const char* tag, const char* fmt, ...) {
        if (level < level_) return;

        char timebuf[32];
        std::time_t now = std::time(nullptr);
        struct tm tmInfo;
#ifdef _MSC_VER
        localtime_s(&tmInfo, &now);
#else
        localtime_r(&now, &tmInfo);
#endif
        std::strftime(timebuf, sizeof(timebuf), "%H:%M:%S", &tmInfo);

        const char* levelStr = "?";
        switch (level) {
            case LogLevel::Debug:   levelStr = "D"; break;
            case LogLevel::Info:    levelStr = "I"; break;
            case LogLevel::Warning: levelStr = "W"; break;
            case LogLevel::Error:   levelStr = "E"; break;
            default: break;
        }

        char msgbuf[1024];
        va_list args;
        va_start(args, fmt);
        std::vsnprintf(msgbuf, sizeof(msgbuf), fmt, args);
        va_end(args);

        std::lock_guard<std::mutex> lock(mutex_);
        std::fprintf(stderr, "[%s][%s][%s] %s\n", timebuf, levelStr, tag, msgbuf);
    }

private:
    Logger() : level_(LogLevel::Info) {}
    std::atomic<LogLevel> level_;
    std::mutex            mutex_;
};

} // namespace util
} // namespace embedmq

#define EMQ_LOG_D(tag, fmt, ...) \
    embedmq::util::Logger::instance().log(embedmq::util::LogLevel::Debug,   tag, fmt, ##__VA_ARGS__)
#define EMQ_LOG_I(tag, fmt, ...) \
    embedmq::util::Logger::instance().log(embedmq::util::LogLevel::Info,    tag, fmt, ##__VA_ARGS__)
#define EMQ_LOG_W(tag, fmt, ...) \
    embedmq::util::Logger::instance().log(embedmq::util::LogLevel::Warning, tag, fmt, ##__VA_ARGS__)
#define EMQ_LOG_E(tag, fmt, ...) \
    embedmq::util::Logger::instance().log(embedmq::util::LogLevel::Error,   tag, fmt, ##__VA_ARGS__)
