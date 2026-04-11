/**
 * EmbedMQ 发布-订阅示例
 *
 * 演示同进程内的 Pub/Sub 通信：
 *   - 一个发布者每 200ms 发布传感器数据
 *   - 两个订阅者（精确匹配 + 通配符匹配）
 *
 * 运行: xmake run example_pub_sub
 */

#include "embedmq/embedmq.h"
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

int main() {
    std::printf("=== EmbedMQ Pub/Sub Example ===\n");

    embedmq::ParticipantConfig cfg;
    cfg.nodeName = "sensor_node";
    // 仅演示同进程通信，禁用网络以快速启动
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;

    auto participant = embedmq::Participant::create(cfg);
    std::printf("[Node] Started: %s (id=%u)\n",
                participant->name().c_str(), participant->id());

    // 订阅者 1：精确匹配
    auto sub1 = participant->createSubscriber(
        "sensor/temperature",
        [](const embedmq::ReceivedMessage& msg) {
            std::printf("[Sub1] sensor/temperature = %s\n",
                        std::string(msg.payload.asText()).c_str());
        });

    // 订阅者 2：通配符匹配所有传感器数据
    auto sub2 = participant->createSubscriber(
        "sensor/#",
        [](const embedmq::ReceivedMessage& msg) {
            std::printf("[Sub2] %s = %s\n",
                        msg.topic.c_str(),
                        std::string(msg.payload.asText()).c_str());
        });

    // 发布者
    auto pubTemp = participant->createPublisher("sensor/temperature");
    auto pubHumid = participant->createPublisher("sensor/humidity");

    // 发布 5 次数据
    for (int i = 1; i <= 5; i++) {
        std::string temp   = std::to_string(20 + i) + ".0°C";
        std::string humid  = std::to_string(50 + i) + "%";

        pubTemp->publish(temp);
        pubHumid->publish(humid);

        std::printf("\n--- Tick %d ---\n", i);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::printf("\n[Sub1] Total received: %llu\n",
                static_cast<unsigned long long>(sub1->messageCount()));
    std::printf("[Sub2] Total received: %llu\n",
                static_cast<unsigned long long>(sub2->messageCount()));

    participant->shutdown();
    std::printf("\n=== Done ===\n");
    return 0;
}
