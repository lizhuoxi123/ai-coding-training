"""Queue Manager —— Queue 创建/删除 + FIFO 入队/出队"""

import re
import threading
import logging
from collections import deque

logger = logging.getLogger("MQServer")

# Queue 名称验证: 与 Topic 相同规则
QUEUE_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,256}$")

# 默认最大 Queue 容量 & 消息大小
DEFAULT_MAX_QUEUE_SIZE = 10000
DEFAULT_MAX_MESSAGE_BYTES = 1 * 1024 * 1024  # 1 MB


class QueueError(Exception):
    """Queue 相关错误基类"""
    pass


class QueueExistsError(QueueError):
    """Queue 已存在"""
    pass


class QueueNotFoundError(QueueError):
    """Queue 不存在"""
    pass


class QueueNotEmptyError(QueueError):
    """Queue 中有未消费消息"""
    pass


class QueueFullError(QueueError):
    """Queue 已满"""
    pass


class InvalidQueueNameError(QueueError):
    """Queue 名称不合法"""
    pass


class MessageTooLargeError(QueueError):
    """消息体过大"""
    pass


class EmptyMessageError(QueueError):
    """消息体为空"""
    pass


class QueueManager:
    """管理 Queue 的生命周期和消息入队/出队"""

    def __init__(
        self,
        default_max_size: int = DEFAULT_MAX_QUEUE_SIZE,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    ):
        self._default_max_size = default_max_size
        self._max_message_bytes = max_message_bytes
        # queue_name -> (deque of (msg_id, body), max_size)
        self._queues: dict[str, tuple[deque, int]] = {}
        self._lock = threading.Lock()

    # ----------------------------------------------------------------
    # Queue CRUD
    # ----------------------------------------------------------------

    def create_queue(self, name: str, max_size: int | None = None) -> None:
        """创建 Queue

        Args:
            name: Queue 名称
            max_size: 最大容量，None 则使用默认值

        Raises:
            InvalidQueueNameError, QueueExistsError
        """
        self._validate_name(name)

        if max_size is None:
            max_size = self._default_max_size
        if max_size <= 0:
            max_size = self._default_max_size

        with self._lock:
            if name in self._queues:
                raise QueueExistsError(f"Queue '{name}' already exists")
            self._queues[name] = (deque(), max_size)
            logger.info(f"Queue '{name}' created (max_size={max_size})")

    def delete_queue(self, name: str) -> None:
        """删除 Queue

        Raises:
            QueueNotFoundError, QueueNotEmptyError
        """
        with self._lock:
            if name not in self._queues:
                raise QueueNotFoundError(f"Queue '{name}' not found")
            q, _ = self._queues[name]
            if q:
                raise QueueNotEmptyError(
                    f"Queue '{name}' has {len(q)} pending message(s)"
                )
            del self._queues[name]
            logger.info(f"Queue '{name}' deleted")

    def list_queues(self) -> list[str]:
        """列出所有 Queue 名称"""
        with self._lock:
            return sorted(self._queues.keys())

    def queue_exists(self, name: str) -> bool:
        """检查 Queue 是否存在"""
        with self._lock:
            return name in self._queues

    # ----------------------------------------------------------------
    # 消息入队 / 出队
    # ----------------------------------------------------------------

    def enqueue(self, queue: str, msg_id: str, body: str) -> None:
        """消息入队（FIFO）

        Raises:
            QueueNotFoundError, QueueFullError,
            EmptyMessageError, MessageTooLargeError
        """
        self._validate_message(body)

        with self._lock:
            if queue not in self._queues:
                raise QueueNotFoundError(f"Queue '{queue}' not found")
            q, max_size = self._queues[queue]
            if len(q) >= max_size:
                raise QueueFullError(
                    f"Queue '{queue}' is full ({len(q)}/{max_size})"
                )
            q.append((msg_id, body))
            logger.debug(f"Message {msg_id} enqueued to '{queue}'")

    def dequeue(self, queue: str) -> tuple[str, str] | None:
        """消息出队（FIFO），返回 (msg_id, body) 或 None

        Raises:
            QueueNotFoundError
        """
        with self._lock:
            if queue not in self._queues:
                raise QueueNotFoundError(f"Queue '{queue}' not found")
            q, _ = self._queues[queue]
            if not q:
                return None
            return q.popleft()

    def get_queue_depth(self, queue: str) -> int:
        """获取 Queue 中待消费消息数"""
        with self._lock:
            if queue not in self._queues:
                return 0
            q, _ = self._queues[queue]
            return len(q)

    # ----------------------------------------------------------------
    # 验证
    # ----------------------------------------------------------------

    @staticmethod
    def _validate_name(name: str) -> None:
        """验证 Queue 名称"""
        if not name:
            raise InvalidQueueNameError("Queue name must not be empty")
        if not QUEUE_NAME_RE.match(name):
            raise InvalidQueueNameError(
                "Queue name must match [a-zA-Z0-9_-]+, length 1-256"
            )

    def _validate_message(self, body: str) -> None:
        """验证消息体"""
        if not body:
            raise EmptyMessageError("Message body must not be empty")
        if len(body.encode("utf-8")) > self._max_message_bytes:
            raise MessageTooLargeError(
                f"Message body exceeds {self._max_message_bytes} bytes"
            )
