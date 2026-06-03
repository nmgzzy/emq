#!/usr/bin/env python3
"""
EmbedMQ Python 压力测试 / 稳定性测试（Phase 5 测试补强）

通过 Python 绑定（ctypes → C ABI）对核心库施加高负载、并发、生命周期 churn 与
长时间 soak，验证：
  - 正确性：本地投递为同步内联，计数精确无丢失；
  - 线程安全：多线程并发发布、并发增删订阅者不崩溃/不死锁；
  - 稳定性：churn / soak 下进程存活、计数单调增长、RSS 不持续膨胀；
  - 性能：报告吞吐(msg/s)与请求往返延迟。

所有场景走进程内通信（关闭 UDP/多播），不依赖网络环境。

注意：Python 回调经由 ctypes 在持有 GIL 的情况下被调用，吞吐显著低于 C++，
因此默认条数较小；可用命令行参数放大。

用法（需先构建 C ABI 共享库 `xmake build embedmq_c`）::

    python3 bindings/python/stress.py all
    python3 bindings/python/stress.py throughput -n 100000
    python3 bindings/python/stress.py concurrent -t 4 -s 4 -d 2
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time

import embedmq


# ---- 工具 ----

def _new_local(name: str) -> embedmq.Participant:
    """进程内参与者：关闭网络与多播，本地投递确定可计数。"""
    return embedmq.Participant(name, enable_udp=False, enable_multicast=False)


def _rss_kb() -> int:
    """读取当前进程 RSS(KB)，仅 Linux 有效，否则返回 0。"""
    try:
        with open("/proc/self/statm") as f:
            resident = int(f.read().split()[1])
        return resident * (os.sysconf("SC_PAGE_SIZE") // 1024)
    except Exception:
        return 0


class Counter:
    """GIL 下的简单计数器（回调里 += 受 GIL 保护，足够安全）。"""
    __slots__ = ("n",)

    def __init__(self):
        self.n = 0

    def bump(self, _msg=None):
        self.n += 1


class Result:
    def __init__(self, name: str, passed: bool, detail: str):
        self.name = name
        self.passed = passed
        self.detail = detail

    def report(self):
        print("[%s] %-12s %s" % ("PASS" if self.passed else "FAIL",
                                 self.name, self.detail))


# ---- 场景 ----

def scenario_throughput(args) -> Result:
    n = min(args.messages, 200000)
    payload = b"x" * args.payload
    with _new_local("py_tp") as p:
        c = Counter()
        sub = p.create_subscriber("bench/topic", c.bump)
        pub = p.create_publisher("bench/topic")
        t0 = time.perf_counter()
        for _ in range(n):
            pub.publish(payload)
        sec = time.perf_counter() - t0
        passed = (c.n == n)
        detail = ("sent=%d recv=%d in %.3fs => %.1f Kmsg/s (payload=%dB)%s"
                  % (n, c.n, sec, n / sec / 1000 if sec else 0, args.payload,
                     "" if passed else "  [LOSS]"))
    return Result("throughput", passed, detail)


def scenario_fanout(args) -> Result:
    n = min(args.messages, 50000)
    m = max(1, args.subs)
    payload = b"x" * args.payload
    with _new_local("py_fo") as p:
        counters = [Counter() for _ in range(m)]
        subs = [p.create_subscriber("fan/#", counters[i].bump) for i in range(m)]
        pub = p.create_publisher("fan/data")
        t0 = time.perf_counter()
        for _ in range(n):
            pub.publish(payload)
        sec = time.perf_counter() - t0
        passed = all(c.n == n for c in counters)
        detail = ("%d subs x %d msgs = %d deliveries in %.3fs => %.1f Kdeliv/s%s"
                  % (m, n, m * n, sec, (m * n) / sec / 1000 if sec else 0,
                     "" if passed else "  [MISMATCH]"))
    return Result("fanout", passed, detail)


def scenario_concurrent(args) -> Result:
    t = max(1, args.threads)
    k = max(1, args.subs)
    payload = b"x" * args.payload
    with _new_local("py_cc") as p:
        recv = Counter()
        subs = [p.create_subscriber("cc/#", recv.bump) for _ in range(k)]
        stop = threading.Event()
        sent_lock = threading.Lock()
        sent_total = [0]

        def producer(idx):
            pub = p.create_publisher("cc/p%d" % idx)
            local = 0
            while not stop.is_set():
                for _ in range(128):
                    pub.publish(payload)
                local += 128
            with sent_lock:
                sent_total[0] += local

        threads = [threading.Thread(target=producer, args=(i,)) for i in range(t)]
        t0 = time.perf_counter()
        for th in threads:
            th.start()
        time.sleep(args.duration)
        stop.set()
        for th in threads:
            th.join()
        sec = time.perf_counter() - t0
        expect = sent_total[0] * k
        passed = (recv.n == expect)
        detail = ("%d producers x %d subs, sent=%d recv=%d (expect=%d) in %.2fs "
                  "=> %.1f Kmsg/s%s"
                  % (t, k, sent_total[0], recv.n, expect, sec,
                     sent_total[0] / sec / 1000 if sec else 0,
                     "" if passed else "  [MISMATCH]"))
    return Result("concurrent", passed, detail)


def scenario_reqrep(args) -> Result:
    r = max(1, args.threads)
    per = max(1, min(args.messages, 5000) // r)
    payload = b"x" * args.payload
    with _new_local("py_rr") as p:
        served = Counter()

        def handler(msg):
            served.bump()
            return msg.payload

        rep = p.create_replier("echo", handler)
        ok = [0] * r
        fail = [0] * r
        lat_ns = [0] * r
        max_ns = [0] * r

        def worker(idx):
            req = p.create_requester("echo")
            for _ in range(per):
                a = time.perf_counter_ns()
                res = req.request(payload, timeout_ms=2000)
                b = time.perf_counter_ns()
                if res is not None and len(res) == len(payload):
                    ok[idx] += 1
                    d = b - a
                    lat_ns[idx] += d
                    if d > max_ns[idx]:
                        max_ns[idx] = d
                else:
                    fail[idx] += 1

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(r)]
        t0 = time.perf_counter()
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        sec = time.perf_counter() - t0
        total = per * r
        tok, tfail = sum(ok), sum(fail)
        avg_us = (sum(lat_ns) / 1000.0 / tok) if tok else 0.0
        passed = (tfail == 0 and tok == total)
        detail = ("%d threads x %d req = %d, ok=%d fail=%d served=%d in %.2fs => "
                  "%.1f Kreq/s, avg=%.1fus max=%.1fus%s"
                  % (r, per, total, tok, tfail, served.n, sec,
                     total / sec / 1000 if sec else 0, avg_us,
                     max(max_ns) / 1000.0 if max_ns else 0,
                     "" if passed else "  [LOST]"))
    return Result("reqrep", passed, detail)


def scenario_churn(args) -> Result:
    iters = max(1, min(args.messages, 500))
    payload = b"x" * args.payload
    rss0 = _rss_kb()
    delivered = Counter()
    ok = True
    t0 = time.perf_counter()
    for i in range(iters):
        p = _new_local("py_churn_%d" % i)
        c = Counter()
        sub = p.create_subscriber("c/#", lambda m, cc=c: (cc.bump(), delivered.bump()))
        pub = p.create_publisher("c/x")
        for _ in range(8):
            pub.publish(payload)
        if c.n != 8:
            ok = False
        # 显式释放句柄，触发 C 侧析构
        sub.close()
        pub.close()
        p.close()
        if not ok:
            break
    sec = time.perf_counter() - t0
    rss1 = _rss_kb()
    detail = ("%d create/destroy cycles in %.2fs (%.0f cyc/s), delivered=%d, "
              "RSS %dKB->%dKB (Δ%+dKB)%s"
              % (iters, sec, iters / sec if sec else 0, delivered.n,
                 rss0, rss1, rss1 - rss0, "" if ok else "  [LIFECYCLE FAIL]"))
    return Result("churn", ok, detail)


def scenario_soak(args) -> Result:
    payload = b"x" * args.payload
    with _new_local("py_soak") as p:
        recv = Counter()
        stable = p.create_subscriber("soak/#", recv.bump)
        stop = threading.Event()
        sent = Counter()

        def pub_thread():
            pub = p.create_publisher("soak/data")
            while not stop.is_set():
                for _ in range(64):
                    pub.publish(payload)
                    sent.bump()
                time.sleep(0.0005)

        def churn_thread():
            while not stop.is_set():
                s = p.create_subscriber("soak/#", recv.bump)
                time.sleep(0.01)
                s.close()

        pt = threading.Thread(target=pub_thread)
        ct = threading.Thread(target=churn_thread)
        rss0 = _rss_kb()
        t0 = time.perf_counter()
        pt.start()
        ct.start()
        time.sleep(args.duration / 2.0)
        mid = recv.n
        time.sleep(args.duration / 2.0)
        stop.set()
        pt.join()
        ct.join()
        sec = time.perf_counter() - t0
        rss1 = _rss_kb()
        grew = recv.n > mid and mid > 0
        alive = p.running
        passed = grew and alive
        detail = ("%.1fs mixed load: sent=%d recv=%d (mid=%d), running=%d, "
                  "RSS %dKB->%dKB (Δ%+dKB)%s"
                  % (sec, sent.n, recv.n, mid, 1 if alive else 0, rss0, rss1,
                     rss1 - rss0, "" if passed else "  [SOAK FAIL]"))
    return Result("soak", passed, detail)


SCENARIOS = {
    "throughput": scenario_throughput,
    "fanout": scenario_fanout,
    "concurrent": scenario_concurrent,
    "reqrep": scenario_reqrep,
    "churn": scenario_churn,
    "soak": scenario_soak,
}


def main() -> int:
    ap = argparse.ArgumentParser(description="EmbedMQ Python 压力/稳定性测试")
    ap.add_argument("scenario", choices=list(SCENARIOS) + ["all"])
    ap.add_argument("-d", "--duration", type=int, default=2, help="持续型场景时长(秒)")
    ap.add_argument("-n", "--messages", type=int, default=50000, help="消息条数")
    ap.add_argument("-t", "--threads", type=int, default=4, help="生产者/请求者线程数")
    ap.add_argument("-s", "--subs", type=int, default=4, help="订阅者数")
    ap.add_argument("-p", "--payload", type=int, default=16, help="载荷字节数")
    args = ap.parse_args()
    if args.duration < 1:
        args.duration = 1

    print("=== emq_stress.py (lib %s) duration=%ds messages=%d threads=%d subs=%d payload=%dB ==="
          % (embedmq.version(), args.duration, args.messages, args.threads,
             args.subs, args.payload))

    if args.scenario == "all":
        names = ["throughput", "fanout", "concurrent", "reqrep", "churn", "soak"]
    else:
        names = [args.scenario]

    results = []
    for name in names:
        r = SCENARIOS[name](args)
        r.report()
        results.append(r)

    failed = sum(0 if r.passed else 1 for r in results)
    print("\n=== %d scenario(s): %d passed, %d failed ==="
          % (len(results), len(results) - failed, failed))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
