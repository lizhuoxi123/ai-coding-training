"""Acknowledgment Manager —— ACK/NACK/超时重投/DLQ/TTL"""

import threading
import time
import logging

from src.models import Message, MessageStatus

logger = logging.getLogger("MQServer")

# 默认配置
DEFAULT_ACK_TIMEOUT = 30       # 秒
DEFAULT_MAX_RETRIES = 5
DEFAULT_TTL = 86400            # 24 小时
MAX_TTL = 604800               # 7 天


class AckManager:
    """管理消息确认、超时重投、DLQ 和 TTL"""

    def __init__(
        self,
        default_ack_timeout: float = DEFAULT_ACK_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        default_ttl: int = DEFAULT_TTL,
    ):
        self.default_ack_timeout = default_ack_timeout
        self.max_retries = max_retries
        self.default_ttl = default_ttl

        # DLQ 消息存储: msg_id -> Message
        self._dlq: dict[str, Message] = {}
        self._lock = threading.Lock()

    # ----------------------------------------------------------------
    # ACK / NACK
    # ----------------------------------------------------------------

    def ack(self, msg: Message, consumer_id: str) -> None:
        """确认消息

        Raises:
            ValueError: 消息不属于该消费者
        """
        if msg.status != MessageStatus.DELIVERED:
            # 幂等：已确认的消息再次 ACK 无影响
            if msg.status == MessageStatus.ACKED:
                return
            raise ValueError(
                f"Message '{msg.msg_id}' is not in DELIVERED state (current: {msg.status.value})"
            )

        if msg.delivered_to != consumer_id:
            raise ValueError(
                f"Message '{msg.msg_id}' is not delivered to '{consumer_id}'"
            )

        msg.status = MessageStatus.ACKED
        logger.info(f"Message {msg.msg_id} acknowledged by '{consumer_id}'")

    def nack(self, msg: Message, consumer_id: str) -> None:
        """否定确认：立即重投

        Raises:
            ValueError
        """
        if msg.status != MessageStatus.DELIVERED:
            raise ValueError("Only DELIVERED messages can be NACKed")

        if msg.delivered_to != consumer_id:
            raise ValueError(
                f"Message '{msg.msg_id}' is not delivered to '{consumer_id}'"
            )

        msg.retry_count += 1
        if msg.retry_count > self.max_retries:
            self._move_to_dlq(msg, "Max retries exceeded after NACK")
        else:
            msg.status = MessageStatus.PENDING
            msg.delivered_to = ""
            msg.deliver_time = 0.0
            logger.info(
                f"Message {msg.msg_id} NACKed by '{consumer_id}', "
                f"retry {msg.retry_count}/{self.max_retries}"
            )

    # ----------------------------------------------------------------
    # 超时检查
    # ----------------------------------------------------------------

    def check_timeouts(self, messages: dict[str, Message]) -> list[str]:
        """检查超时未确认的消息，返回需要重投的 msg_id 列表"""
        now = time.time()
        expired = []

        with self._lock:
            for msg_id, msg in messages.items():
                if msg.status != MessageStatus.DELIVERED:
                    continue
                if msg.deliver_time <= 0:
                    continue

                timeout = msg.ack_timeout if msg.ack_timeout > 0 else self.default_ack_timeout
                if now - msg.deliver_time > timeout:
                    msg.retry_count += 1
                    if msg.retry_count > self.max_retries:
                        self._move_to_dlq(msg, "Max retries exceeded after timeout")
                    else:
                        msg.status = MessageStatus.PENDING
                        msg.delivered_to = ""
                        msg.deliver_time = 0.0
                        expired.append(msg_id)
                        logger.info(
                            f"Message {msg_id} timed out, "
                            f"retry {msg.retry_count}/{self.max_retries}"
                        )

        return expired

    # ----------------------------------------------------------------
    # TTL 检查
    # ----------------------------------------------------------------

    def check_ttl(self, messages: dict[str, Message]) -> list[str]:
        """检查 TTL 过期的消息，返回已过期的 msg_id 列表"""
        now = time.time()
        expired = []

        for msg_id, msg in messages.items():
            if msg.status not in (MessageStatus.PENDING, MessageStatus.DELIVERED):
                continue

            ttl = msg.ttl if msg.ttl > 0 else self.default_ttl
            if ttl < 0:  # -1 表示永不过期
                continue

            msg_age = now - msg.timestamp
            if msg_age > ttl:
                msg.status = MessageStatus.EXPIRED
                self._move_to_dlq(msg, f"TTL expired (age={msg_age:.0f}s, ttl={ttl}s)")
                expired.append(msg_id)
                logger.info(f"Message {msg_id} expired (TTL={ttl}s)")

        return expired

    # ----------------------------------------------------------------
    # DLQ 死信队列
    # ----------------------------------------------------------------

    def _move_to_dlq(self, msg: Message, reason: str) -> None:
        """将消息移入 DLQ"""
        msg.original_target = msg.target
        msg.target = "DLQ"
        msg.status = MessageStatus.DLQ
        with self._lock:
            self._dlq[msg.msg_id] = msg
        logger.warning(f"Message {msg.msg_id} moved to DLQ: {reason}")

    def get_dlq_messages(self) -> list[Message]:
        """获取所有 DLQ 消息"""
        with self._lock:
            return list(self._dlq.values())

    def get_dlq_message(self, msg_id: str) -> Message | None:
        with self._lock:
            return self._dlq.get(msg_id)

    def replay_from_dlq(self, msg_id: str) -> Message | None:
        """从 DLQ 重放一条消息，返回重放的消息或 None"""
        with self._lock:
            msg = self._dlq.pop(msg_id, None)
        if msg is None:
            return None

        msg.target = msg.original_target
        msg.original_target = ""
        msg.status = MessageStatus.PENDING
        msg.retry_count = 0
        msg.delivered_to = ""
        msg.deliver_time = 0.0
        logger.info(f"Message {msg_id} replayed from DLQ to '{msg.target}'")
        return msg

    def replay_all_from_dlq(self, target: str) -> list[Message]:
        """从 DLQ 重放所有属于 target 的消息"""
        replayed = []
        with self._lock:
            to_replay = [
                mid for mid, msg in self._dlq.items()
                if msg.original_target == target
            ]
        for mid in to_replay:
            msg = self.replay_from_dlq(mid)
            if msg:
                replayed.append(msg)
        return replayed

    # ----------------------------------------------------------------
    # 工具方法
    # ----------------------------------------------------------------

    @staticmethod
    def validate_ttl(ttl: int) -> None:
        """验证 TTL 值

        Raises:
            ValueError
        """
        if ttl == 0:
            raise ValueError("TTL must be greater than 0, or use -1 for no expiry")
        if ttl > MAX_TTL:
            raise ValueError(f"TTL exceeds maximum ({MAX_TTL}s)")
