#pragma once
#include <atomic>
#include <optional>
#include <utility>

namespace embedmq {
namespace util {

/// 无锁多生产者单消费者队列（Vyukov 侵入式无界队列）
///
/// - 多个线程可并发 push；
/// - 仅单个线程可 pop（消费者）。
///
/// 实现要点：维护一个带哨兵节点的单向链表，生产者用一次 exchange
/// 原子地接入新节点；消费者推进 tail，旧哨兵被回收，被消费节点成为新哨兵。
template<typename T>
class MpscQueue {
public:
    MpscQueue() {
        Node* stub = new Node();
        head_.store(stub, std::memory_order_relaxed);
        tail_ = stub;
    }

    ~MpscQueue() {
        while (pop().has_value()) {}
        delete tail_; // 最后的哨兵节点
    }

    MpscQueue(const MpscQueue&)            = delete;
    MpscQueue& operator=(const MpscQueue&) = delete;

    /// 多生产者安全
    void push(T value) {
        Node* n    = new Node(std::move(value));
        Node* prev = head_.exchange(n, std::memory_order_acq_rel);
        prev->next.store(n, std::memory_order_release);
    }

    /// 仅消费者线程调用
    std::optional<T> pop() {
        Node* tail = tail_;
        Node* next = tail->next.load(std::memory_order_acquire);
        if (!next) return std::nullopt;
        // next 携带真正的数据，移动出来；旧哨兵 tail 被回收，next 成为新哨兵
        std::optional<T> value = std::move(next->value);
        next->value.reset();
        tail_ = next;
        delete tail;
        return value;
    }

    /// 近似判空（生产者链接尚未完成时可能短暂误报非空/空）
    bool empty() const {
        Node* tail = tail_;
        return tail->next.load(std::memory_order_acquire) == nullptr;
    }

private:
    struct Node {
        std::atomic<Node*> next{nullptr};
        std::optional<T>   value;
        Node() = default;
        explicit Node(T v) : value(std::move(v)) {}
    };

    alignas(64) std::atomic<Node*> head_; // 生产者端
    alignas(64) Node*              tail_; // 消费者端（仅消费者访问）
};

} // namespace util
} // namespace embedmq
