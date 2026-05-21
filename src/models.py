"""数据模型定义 —— Message, Consumer, MessageStatus"""

from dataclasses import dataclass, field
from enum import Enum
import time
import threading


class MessageStatus(Enum):
    """消息状态枚举"""
    PENDING = "PENDING"        # 待投递（已持久化，等待消费者拉取）
    DELIVERED = "DELIVERED"    # 已投递（消费者已拉取，等待 ACK）
    ACKED = "ACKED"            # 已确认（消费者已 ACK，可清理）
    DLQ = "DLQ"                # 死信（超过最大重试次数）
    EXPIRED = "EXPIRED"        # 已过期（超过 TTL）


@dataclass
class Message:
    """消息核心数据结构"""
    msg_id: str                # 全局唯一 ID
    target: str                # Topic 或 Queue 名称
    body: str                  # 消息体（JSON 字符串）
    timestamp: int             # Unix 时间戳（秒）
    ttl: int = 0               # TTL 秒数，0=服务器默认，-1=永不过期
    ack_timeout: int = 0       # ACK 超时秒数，0=服务器默认

    # ---- 运行时状态（不持久化） ----
    status: MessageStatus = MessageStatus.PENDING
    retry_count: int = 0
    delivered_to: str = ""
    deliver_time: float = 0.0
    original_target: str = ""  # 移入 DLQ 前的原始 target

    # ---- 类级别 ID 生成器 ----
    _id_counter: int = field(default=0, repr=False, compare=False)
    _id_lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    @classmethod
    def generate_id(cls) -> str:
        """生成全局唯一消息 ID: M{timestamp}{seq}"""
        with cls._id_lock:
            cls._id_counter += 1
        ts = int(time.time() * 1000)
        return f"M{ts}{cls._id_counter:06d}"


@dataclass
class Consumer:
    """消费者会话"""
    consumer_id: str           # 格式 "C{conn_id}"
    subscriptions: set = field(default_factory=set)    # 已订阅的 Topic 集合
    groups: set = field(default_factory=set)            # 所属消费者组集合
    last_heartbeat: float = 0.0
