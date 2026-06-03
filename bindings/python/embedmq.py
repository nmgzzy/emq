"""
EmbedMQ Python 绑定（Phase 5）

基于 ctypes 封装 EmbedMQ 的 C ABI 共享库（libembedmq_c.so / .dylib / embedmq_c.dll），
提供 Pythonic 的发布-订阅 / 请求-响应接口，零额外依赖（仅标准库）。

用法概览::

    import embedmq

    p = embedmq.Participant("py_node")

    def on_msg(msg):
        print(msg.topic, msg.text)

    sub = p.create_subscriber("sensor/#", on_msg)
    pub = p.create_publisher("sensor/temp")
    pub.publish("25.6")
    ...
    p.shutdown()

库定位：先按环境变量 ``EMBEDMQ_LIB`` 指定的绝对路径加载；否则在常见构建目录与
系统库路径中搜索。可用 :func:`load_library` 显式指定。
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import sys
import weakref
from ctypes import (
    CFUNCTYPE, POINTER, Structure,
    c_char, c_char_p, c_int, c_size_t, c_uint8, c_uint16, c_uint32,
    c_uint64, c_void_p,
)
from typing import Callable, List, Optional

__all__ = [
    "EmbedMQError", "QoS", "Message", "Participant",
    "Publisher", "Subscriber", "Requester", "Replier",
    "load_library", "version",
]


# ===================== 库加载 =====================

_lib: Optional[ctypes.CDLL] = None


def _candidate_names() -> List[str]:
    if sys.platform.startswith("win"):
        return ["embedmq_c.dll"]
    if sys.platform == "darwin":
        return ["libembedmq_c.dylib"]
    return ["libembedmq_c.so"]


def _candidate_dirs() -> List[str]:
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.abspath(os.path.join(here, "..", ".."))
    dirs = [
        os.path.join(repo, "build", "linux", "x86_64", "release"),
        os.path.join(repo, "build", "linux", "x86_64", "debug"),
        os.path.join(repo, "build", "macosx", "x86_64", "release"),
        os.path.join(repo, "build", "macosx", "arm64", "release"),
        os.path.join(repo, "build", "windows", "x64", "release"),
        os.path.join(repo, "build"),
        here,
    ]
    # 同时扫描 build 下任意架构目录
    build_root = os.path.join(repo, "build")
    if os.path.isdir(build_root):
        for root, _subdirs, files in os.walk(build_root):
            if any(n in files for n in _candidate_names()):
                dirs.append(root)
    return dirs


def load_library(path: Optional[str] = None) -> ctypes.CDLL:
    """加载 C ABI 共享库并完成函数签名绑定。可显式传入库路径。"""
    global _lib
    if _lib is not None and path is None:
        return _lib

    candidates: List[str] = []
    if path:
        candidates.append(path)
    env = os.environ.get("EMBEDMQ_LIB")
    if env:
        candidates.append(env)
    for d in _candidate_dirs():
        for n in _candidate_names():
            candidates.append(os.path.join(d, n))
    # 退回系统查找
    found = ctypes.util.find_library("embedmq_c")
    if found:
        candidates.append(found)

    last_err = None
    for c in candidates:
        if not c:
            continue
        try:
            lib = ctypes.CDLL(c)
            _bind(lib)
            _lib = lib
            return lib
        except OSError as e:  # noqa: PERF203
            last_err = e
            continue
    raise EmbedMQError(
        "无法加载 EmbedMQ C ABI 共享库。请先构建 (xmake build embedmq_c) "
        "或设置环境变量 EMBEDMQ_LIB 指向库文件。最后错误: %s" % last_err
    )


# ===================== C 类型 =====================

class _CMessage(Structure):
    _fields_ = [
        ("topic", c_char_p),
        ("payload", POINTER(c_uint8)),
        ("payload_len", c_size_t),
        ("timestamp", c_uint64),
        ("source_id", c_uint16),
        ("sequence_id", c_uint32),
        ("correlation_id", c_uint32),
    ]


_SUBSCRIBE_CB = CFUNCTYPE(None, POINTER(_CMessage), c_void_p)
_REQUEST_CB = CFUNCTYPE(None, POINTER(_CMessage), POINTER(POINTER(c_uint8)),
                        POINTER(c_size_t), c_void_p)
_PEER_EVENT_CB = CFUNCTYPE(None, c_uint16, c_char_p, c_int, c_void_p)


def _bind(lib: ctypes.CDLL) -> None:
    """声明所有导出函数的参数/返回类型。"""
    lib.emq_version.restype = c_char_p
    lib.emq_status_str.argtypes = [c_int]
    lib.emq_status_str.restype = c_char_p
    lib.emq_alloc.argtypes = [c_size_t]
    lib.emq_alloc.restype = POINTER(c_uint8)
    lib.emq_free.argtypes = [POINTER(c_uint8)]

    lib.emq_participant_create.argtypes = [c_char_p]
    lib.emq_participant_create.restype = c_void_p
    lib.emq_participant_create_ex.argtypes = [c_char_p, c_uint8, c_int, c_int, c_int]
    lib.emq_participant_create_ex.restype = c_void_p
    lib.emq_participant_destroy.argtypes = [c_void_p]
    lib.emq_participant_id.argtypes = [c_void_p]
    lib.emq_participant_id.restype = c_uint16
    lib.emq_participant_name.argtypes = [c_void_p]
    lib.emq_participant_name.restype = c_char_p
    lib.emq_participant_is_running.argtypes = [c_void_p]
    lib.emq_participant_is_running.restype = c_int
    lib.emq_participant_peer_count.argtypes = [c_void_p]
    lib.emq_participant_peer_count.restype = c_int
    lib.emq_participant_peer_name.argtypes = [c_void_p, c_int, c_char_p, c_size_t]
    lib.emq_participant_peer_name.restype = c_int
    lib.emq_participant_on_peer_event.argtypes = [c_void_p, _PEER_EVENT_CB, c_void_p]
    lib.emq_participant_on_peer_event.restype = c_int
    lib.emq_participant_shutdown.argtypes = [c_void_p]

    lib.emq_publisher_create.argtypes = [c_void_p, c_char_p, c_int]
    lib.emq_publisher_create.restype = c_void_p
    lib.emq_publisher_destroy.argtypes = [c_void_p]
    lib.emq_publisher_publish.argtypes = [c_void_p, c_void_p, c_size_t]
    lib.emq_publisher_publish.restype = c_int
    lib.emq_publisher_publish_str.argtypes = [c_void_p, c_char_p]
    lib.emq_publisher_publish_str.restype = c_int
    lib.emq_publisher_subscriber_count.argtypes = [c_void_p]
    lib.emq_publisher_subscriber_count.restype = c_int

    lib.emq_subscriber_create.argtypes = [c_void_p, c_char_p, c_int, _SUBSCRIBE_CB, c_void_p]
    lib.emq_subscriber_create.restype = c_void_p
    lib.emq_subscriber_destroy.argtypes = [c_void_p]
    lib.emq_subscriber_pause.argtypes = [c_void_p]
    lib.emq_subscriber_pause.restype = c_int
    lib.emq_subscriber_resume.argtypes = [c_void_p]
    lib.emq_subscriber_resume.restype = c_int
    lib.emq_subscriber_message_count.argtypes = [c_void_p]
    lib.emq_subscriber_message_count.restype = c_uint64

    lib.emq_requester_create.argtypes = [c_void_p, c_char_p, c_int]
    lib.emq_requester_create.restype = c_void_p
    lib.emq_requester_destroy.argtypes = [c_void_p]
    lib.emq_requester_request.argtypes = [
        c_void_p, c_void_p, c_size_t, c_uint32,
        POINTER(POINTER(c_uint8)), POINTER(c_size_t),
    ]
    lib.emq_requester_request.restype = c_int

    lib.emq_replier_create.argtypes = [c_void_p, c_char_p, c_int, _REQUEST_CB, c_void_p]
    lib.emq_replier_create.restype = c_void_p
    lib.emq_replier_destroy.argtypes = [c_void_p]
    lib.emq_replier_request_count.argtypes = [c_void_p]
    lib.emq_replier_request_count.restype = c_uint64


# ===================== 错误码 =====================

EMQ_OK = 0
EMQ_ERR_TIMEOUT = -4


class EmbedMQError(Exception):
    """EmbedMQ 操作失败时抛出。"""


class QoS:
    """QoS 级别常量。"""
    BEST_EFFORT = 0
    RELIABLE = 1
    EXACTLY_ONCE = 2


# ===================== 消息 =====================

class Message:
    """订阅/请求回调收到的消息（已从 C 视图拷贝为 Python 对象）。"""

    __slots__ = ("topic", "payload", "timestamp", "source_id",
                 "sequence_id", "correlation_id")

    def __init__(self, c_msg: _CMessage):
        self.topic: str = c_msg.topic.decode("utf-8", "replace") if c_msg.topic else ""
        n = int(c_msg.payload_len)
        if c_msg.payload and n:
            self.payload: bytes = bytes(bytearray(c_msg.payload[i] for i in range(n)))
        else:
            self.payload = b""
        self.timestamp = int(c_msg.timestamp)
        self.source_id = int(c_msg.source_id)
        self.sequence_id = int(c_msg.sequence_id)
        self.correlation_id = int(c_msg.correlation_id)

    @property
    def text(self) -> str:
        """以 UTF-8 文本解码载荷。"""
        return self.payload.decode("utf-8", "replace")

    def __repr__(self) -> str:
        return ("Message(topic=%r, src=%d, seq=%d, payload=%d bytes)"
                % (self.topic, self.source_id, self.sequence_id, len(self.payload)))


def _to_bytes(data) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8")
    raise TypeError("载荷必须是 str / bytes / bytearray")


# ===================== 句柄包装 =====================

class Publisher:
    def __init__(self, handle: int, lib: ctypes.CDLL, participant=None):
        self._h = handle
        self._lib = lib
        # 强引用 participant：保证底层 MessageBus 在本对象析构前一直有效，
        # 杜绝「participant 先于 child 释放」导致的悬垂指针 / double free。
        self._participant = participant

    def publish(self, data) -> None:
        """发布载荷（str/bytes）。失败抛出 EmbedMQError。"""
        if not self._h:
            raise EmbedMQError("publisher 已销毁")
        b = _to_bytes(data)
        rc = self._lib.emq_publisher_publish(self._h, b, len(b))
        if rc != EMQ_OK:
            raise EmbedMQError("publish 失败: %s"
                               % self._lib.emq_status_str(rc).decode())

    @property
    def subscriber_count(self) -> int:
        return max(0, int(self._lib.emq_publisher_subscriber_count(self._h)))

    def close(self) -> None:
        if self._h:
            self._lib.emq_publisher_destroy(self._h)
            self._h = 0
            self._participant = None

    def __del__(self):
        self.close()


class Subscriber:
    def __init__(self, handle: int, lib: ctypes.CDLL, cb_holder, participant=None):
        self._h = handle
        self._lib = lib
        # 持有 ctypes 回调对象，防止被 GC（否则 C 端回调会野指针）
        self._cb_holder = cb_holder
        self._participant = participant

    def pause(self) -> None:
        self._lib.emq_subscriber_pause(self._h)

    def resume(self) -> None:
        self._lib.emq_subscriber_resume(self._h)

    @property
    def message_count(self) -> int:
        return int(self._lib.emq_subscriber_message_count(self._h))

    def close(self) -> None:
        if self._h:
            self._lib.emq_subscriber_destroy(self._h)
            self._h = 0
            self._cb_holder = None
            self._participant = None

    def __del__(self):
        self.close()


class Requester:
    def __init__(self, handle: int, lib: ctypes.CDLL, participant=None):
        self._h = handle
        self._lib = lib
        self._participant = participant

    def request(self, data, timeout_ms: int = 5000) -> Optional[bytes]:
        """同步请求。超时返回 None，否则返回响应 bytes。"""
        if not self._h:
            raise EmbedMQError("requester 已销毁")
        b = _to_bytes(data)
        out_ptr = POINTER(c_uint8)()
        out_len = c_size_t(0)
        rc = self._lib.emq_requester_request(
            self._h, b, len(b), int(timeout_ms),
            ctypes.byref(out_ptr), ctypes.byref(out_len))
        if rc == EMQ_ERR_TIMEOUT:
            return None
        if rc != EMQ_OK:
            raise EmbedMQError("request 失败: %s"
                               % self._lib.emq_status_str(rc).decode())
        n = int(out_len.value)
        if out_ptr and n:
            result = bytes(bytearray(out_ptr[i] for i in range(n)))
        else:
            result = b""
        if out_ptr:
            self._lib.emq_free(out_ptr)
        return result

    def close(self) -> None:
        if self._h:
            self._lib.emq_requester_destroy(self._h)
            self._h = 0
            self._participant = None

    def __del__(self):
        self.close()


class Replier:
    def __init__(self, handle: int, lib: ctypes.CDLL, cb_holder, participant=None):
        self._h = handle
        self._lib = lib
        self._cb_holder = cb_holder
        self._participant = participant

    @property
    def request_count(self) -> int:
        return int(self._lib.emq_replier_request_count(self._h))

    def close(self) -> None:
        if self._h:
            self._lib.emq_replier_destroy(self._h)
            self._h = 0
            self._cb_holder = None
            self._participant = None

    def __del__(self):
        self.close()


# ===================== Participant =====================

class Participant:
    """EmbedMQ 节点。封装一个 C ABI participant 句柄。"""

    def __init__(self, name: str = "", domain: int = 0,
                 enable_udp: bool = True, enable_shm: bool = False,
                 enable_multicast: bool = True, lib: Optional[ctypes.CDLL] = None):
        self._lib = lib or load_library()
        cname = name.encode("utf-8") if name else None
        self._h = self._lib.emq_participant_create_ex(
            cname, int(domain) & 0xFF,
            1 if enable_udp else 0,
            1 if enable_shm else 0,
            1 if enable_multicast else 0)
        if not self._h:
            raise EmbedMQError("创建 Participant 失败")
        # 持有所有回调 trampoline，避免被 GC
        self._peer_cb = None
        # 弱引用跟踪所有子对象（pub/sub/req/rep）：close() 时先于自身释放它们，
        # 避免底层 MessageBus 先被销毁而子对象析构时悬垂访问。
        self._children = weakref.WeakSet()

    def _track(self, child):
        self._children.add(child)
        return child

    # ---- 属性 ----
    @property
    def id(self) -> int:
        return int(self._lib.emq_participant_id(self._h))

    @property
    def name(self) -> str:
        return self._lib.emq_participant_name(self._h).decode("utf-8", "replace")

    @property
    def running(self) -> bool:
        return bool(self._lib.emq_participant_is_running(self._h))

    def peers(self) -> List[str]:
        """返回当前发现的对端名称列表。"""
        count = int(self._lib.emq_participant_peer_count(self._h))
        out: List[str] = []
        buf = ctypes.create_string_buffer(256)
        for i in range(max(0, count)):
            if self._lib.emq_participant_peer_name(self._h, i, buf, 256) == EMQ_OK:
                out.append(buf.value.decode("utf-8", "replace"))
        return out

    def on_peer_event(self, callback: Callable[[int, str, bool], None]) -> None:
        """注册对端上下线回调: callback(peer_id, peer_name, connected)。"""
        def trampoline(pid, pname, connected, _ud):
            try:
                callback(int(pid), pname.decode("utf-8", "replace") if pname else "",
                         bool(connected))
            except Exception:  # noqa: BLE001  回调异常不得跨越 ABI
                pass
        self._peer_cb = _PEER_EVENT_CB(trampoline)
        self._lib.emq_participant_on_peer_event(self._h, self._peer_cb, None)

    # ---- 工厂 ----
    def create_publisher(self, topic: str, qos: int = QoS.BEST_EFFORT) -> Publisher:
        h = self._lib.emq_publisher_create(self._h, topic.encode("utf-8"), int(qos))
        if not h:
            raise EmbedMQError("创建 Publisher 失败: %s" % topic)
        return self._track(Publisher(h, self._lib, self))

    def create_subscriber(self, topic: str,
                          callback: Callable[[Message], None],
                          qos: int = QoS.BEST_EFFORT) -> Subscriber:
        def trampoline(c_msg_ptr, _ud):
            try:
                callback(Message(c_msg_ptr.contents))
            except Exception:  # noqa: BLE001
                pass
        cb = _SUBSCRIBE_CB(trampoline)
        h = self._lib.emq_subscriber_create(
            self._h, topic.encode("utf-8"), int(qos), cb, None)
        if not h:
            raise EmbedMQError("创建 Subscriber 失败: %s" % topic)
        return self._track(Subscriber(h, self._lib, cb, self))

    def create_requester(self, service: str, qos: int = QoS.RELIABLE) -> Requester:
        h = self._lib.emq_requester_create(self._h, service.encode("utf-8"), int(qos))
        if not h:
            raise EmbedMQError("创建 Requester 失败: %s" % service)
        return self._track(Requester(h, self._lib, self))

    def create_replier(self, service: str,
                       handler: Callable[[Message], object],
                       qos: int = QoS.RELIABLE) -> Replier:
        """注册响应服务。handler(msg) 返回 str/bytes 作为响应（None 视为空）。"""
        lib = self._lib

        def trampoline(c_msg_ptr, out_pp, out_len, _ud):
            try:
                resp = handler(Message(c_msg_ptr.contents))
            except Exception:  # noqa: BLE001
                resp = None
            if resp is None:
                out_pp[0] = ctypes.cast(0, POINTER(c_uint8))
                out_len[0] = 0
                return
            data = _to_bytes(resp)
            n = len(data)
            # 用库分配器分配，交由库释放（与 C ABI 约定一致）
            buf = lib.emq_alloc(n)
            if n:
                ctypes.memmove(buf, data, n)
            out_pp[0] = buf
            out_len[0] = n

        cb = _REQUEST_CB(trampoline)
        h = lib.emq_replier_create(self._h, service.encode("utf-8"), int(qos), cb, None)
        if not h:
            raise EmbedMQError("创建 Replier 失败: %s" % service)
        return self._track(Replier(h, lib, cb, self))

    # ---- 生命周期 ----
    def shutdown(self) -> None:
        if self._h:
            self._lib.emq_participant_shutdown(self._h)

    def close(self) -> None:
        if self._h:
            # 先释放所有仍存活的子对象（pub/sub/req/rep），再销毁参与者，
            # 保证底层 MessageBus 仍有效，避免子对象悬垂析构。
            for child in list(self._children):
                try:
                    child.close()
                except Exception:  # noqa: BLE001
                    pass
            self._children.clear()
            self._lib.emq_participant_destroy(self._h)
            self._h = 0

    def __enter__(self) -> "Participant":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self):
        self.close()


def version() -> str:
    """返回底层 C 库版本字符串。"""
    lib = load_library()
    return lib.emq_version().decode("utf-8")
