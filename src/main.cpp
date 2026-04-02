/**
 * EmbedMQ — 跨平台轻量级通信中间件
 *
 * 这是 EmbedMQ 库的入口占位文件。
 * 实际用法请参考：
 *   examples/pub_sub/main.cpp  —— 发布订阅示例
 *   examples/req_rep/main.cpp  —— 请求响应示例
 *
 * 构建库：  xmake build embedmq
 * 运行测试：xmake run emq_tests              （全部测试）
 *           xmake run emq_tests pal           （指定模块）
 *           xmake run emq_tests --list        （列出所有模块）
 */
#include <cstdio>

int main() {
    std::printf("EmbedMQ v0.2.0 — Phase 1+2 Implemented\n");
    std::printf("Run 'xmake run example_pub_sub' or 'xmake run example_req_rep'\n");
    std::printf("Run 'xmake run emq_tests' to execute all unit tests\n");
    return 0;
}
