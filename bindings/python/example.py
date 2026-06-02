#!/usr/bin/env python3
"""
EmbedMQ Python 绑定示例 / 自测。

演示同进程内的 Pub/Sub 与 Req/Rep，并对结果做断言。
退出码 0 表示全部通过。

运行（需先构建 C ABI 共享库）::

    xmake build embedmq_c
    python3 bindings/python/example.py
"""

import sys
import time

import embedmq


def test_pub_sub() -> None:
    print("== Pub/Sub ==")
    received = []
    # 仅同进程：禁用网络以快速启动、避免外部干扰
    with embedmq.Participant("py_node", enable_udp=False,
                             enable_multicast=False) as p:
        print("  node=%s id=%d running=%s" % (p.name, p.id, p.running))

        sub = p.create_subscriber("sensor/#", lambda m: received.append(m))
        pub = p.create_publisher("sensor/temperature")

        for i in range(5):
            pub.publish("temp=%d" % (20 + i))
            time.sleep(0.02)

        # 给回调线程一点时间投递
        time.sleep(0.2)

        print("  收到 %d 条, subscriber.message_count=%d"
              % (len(received), sub.message_count))
        assert len(received) == 5, "应收到 5 条消息, 实际 %d" % len(received)
        assert received[0].topic == "sensor/temperature"
        assert received[0].text == "temp=20"
        print("  [OK] pub/sub")


def test_req_rep() -> None:
    print("== Req/Rep ==")
    with embedmq.Participant("py_calc", enable_udp=False,
                             enable_multicast=False) as p:
        def handler(msg: embedmq.Message):
            a, b = msg.text.split()
            return str(int(a) * int(b))

        rep = p.create_replier("multiply", handler)
        req = p.create_requester("multiply")

        resp = req.request("6 7", timeout_ms=2000)
        print("  6 * 7 -> %r" % resp)
        assert resp == b"42", "期望 b'42', 实际 %r" % resp

        resp2 = req.request("12 12", timeout_ms=2000)
        assert resp2 == b"144"
        print("  replier.request_count=%d" % rep.request_count)
        assert rep.request_count == 2
        print("  [OK] req/rep")


def test_binary() -> None:
    print("== 二进制载荷 ==")
    got = []
    with embedmq.Participant("py_bin", enable_udp=False,
                             enable_multicast=False) as p:
        sub = p.create_subscriber("raw", lambda m: got.append(m.payload))
        pub = p.create_publisher("raw")
        blob = bytes(range(256))
        pub.publish(blob)
        time.sleep(0.15)
        assert len(got) == 1 and got[0] == blob, "二进制载荷往返不一致"
        print("  [OK] 256 字节二进制往返")


def main() -> int:
    print("EmbedMQ Python binding self-test, lib version =", embedmq.version())
    test_pub_sub()
    test_req_rep()
    test_binary()
    print("\n全部通过 ✓")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as e:
        print("断言失败:", e, file=sys.stderr)
        sys.exit(1)
