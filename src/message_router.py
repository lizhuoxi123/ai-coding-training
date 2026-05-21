"""Message Router —— 统一消息路由、状态管理、Long Polling 支持"""

import threading
import time
import logging

from src.models import Message, MessageStatus
from src.topic_manager import TopicManager, TopicNotFoundError
from src.queue_manager import QueueManager, QueueNotFoundError, QueueFullError

logger = logging.getLogger("MQServer")


class MessageRouter:
    """统一消息路由层，管理消息生命周期"""

    def __init__(self, topic_manager: TopicManager, queue_manager: QueueManager):
        self.topic_mgr = topic_manager
        self.queue_mgr = queue_manager

        # 消息注册表: msg_id -> Message（运行时状态）
        self._messages: dict[str, Message] = {}
        self._msg_lock = threading.Lock()

        # 每个消费者在 Queue 上的 offset: (queue, consumer_id) -> offset
        self._queue_offsets: dict[str, int] = {}
        self._offset_lock = threading.Lock()

        # 消费者在 Topic 上的 offset: (topic, consumer_id) -> offset
        self._topic_offsets: dict[str, int] = {}
        self._topic_offset_lock = threading.Lock()

    # ----------------------------------------------------------------
    # 生产消息
    # ----------------------------------------------------------------

    def route_to_topic(self, topic: str, body: str, ttl: int = 0) -> str:
        """向 Topic 发布消息，返回 msg_id"""
        if not self.topic_mgr.topic_exists(topic):
            raise TopicNotFoundError(f"Topic '{topic}' not found")

        msg = self._create_message(target=topic, body=body, ttl=ttl)
        with self._msg_lock:
            self._messages[msg.msg_id] = msg
        logger.info(f"Message {msg.msg_id} published to topic '{topic}'")
        return msg.msg_id

    def route_to_queue(
        self, queue: str, body: str, ttl: int = 0
    ) -> str:
        """向 Queue 发送消息，返回 msg_id"""
        msg = self._create_message(target=queue, body=body, ttl=ttl)

        # 先标记为 PENDING，再入队
        with self._msg_lock:
            self._messages[msg.msg_id] = msg

        try:
            self.queue_mgr.enqueue(queue, msg.msg_id, body)
        except QueueFullError:
            with self._msg_lock:
                del self._messages[msg.msg_id]
            raise

        logger.info(f"Message {msg.msg_id} sent to queue '{queue}'")
        return msg.msg_id

    # ----------------------------------------------------------------
    # 消费消息
    # ----------------------------------------------------------------

    def consume_from_queue(
        self, queue: str, consumer_id: str, timeout: float = 0
    ) -> Message | None:
        """从 Queue 消费消息，支持 Long Polling

        Args:
            queue: Queue 名称
            consumer_id: 消费者 ID
            timeout: 阻塞等待超时（秒），0 表示非阻塞

        Returns:
            Message 或 None（无消息或超时）
        """
        deadline = time.time() + timeout if timeout > 0 else 0

        while True:
            result = self.queue_mgr.dequeue(queue)
            if result is not None:
                msg_id, body = result
                with self._msg_lock:
                    msg = self._messages.get(msg_id)
                    if msg is None:
                        continue  # 消息已被清理，跳过
                    msg.status = MessageStatus.DELIVERED
                    msg.delivered_to = consumer_id
                    msg.deliver_time = time.time()
                logger.info(
                    f"Consumer '{consumer_id}' consumed {msg_id} from queue '{queue}'"
                )
                return msg

            # 非阻塞模式：直接返回 None
            if timeout <= 0:
                return None

            # 阻塞模式：检查超时
            if time.time() >= deadline:
                return None

            # 等待一小段时间再检查（避免繁忙等待）
            time.sleep(0.1)

    def consume_from_topic(
        self, topic: str, consumer_id: str
    ) -> Message | None:
        """从 Topic 消费消息（基于 offset）

        从订阅者的 offset 开始，返回下一条未消费的消息。
        如果所有消息都已消费，返回 None。

        Returns:
            Message 或 None
        """
        # TODO: Phase 3 持久化后，消息来自磁盘而非内存
        # 当前简化：扫描所有 topic 消息找到 offset 对应的
        offset_key = f"{topic}:{consumer_id}"
        with self._topic_offset_lock:
            offset = self._topic_offsets.get(offset_key, 0)

        # 收集该 topic 的所有消息（按时间戳排序）
        topic_msgs = []
        with self._msg_lock:
            for msg in self._messages.values():
                if msg.target == topic:
                    topic_msgs.append(msg)

        topic_msgs.sort(key=lambda m: m.msg_id)

        if offset < len(topic_msgs):
            msg = topic_msgs[offset]
            # 更新 offset
            with self._topic_offset_lock:
                self._topic_offsets[offset_key] = offset + 1
            logger.info(
                f"Consumer '{consumer_id}' consumed {msg.msg_id} "
                f"from topic '{topic}' (offset={offset})"
            )
            return msg

        return None

    # ----------------------------------------------------------------
    # 消息确认（基础，Phase 4 会增强）
    # ----------------------------------------------------------------

    def ack_message(self, msg_id: str, consumer_id: str) -> None:
        """确认消息

        Raises:
            KeyError: 消息不存在
            ValueError: 消息不属于该消费者
        """
        with self._msg_lock:
            msg = self._messages.get(msg_id)
            if msg is None:
                raise KeyError(f"Message '{msg_id}' not found")
            if msg.delivered_to != consumer_id:
                raise ValueError(
                    f"Message '{msg_id}' is not delivered to '{consumer_id}'"
                )
            msg.status = MessageStatus.ACKED
            logger.info(f"Message {msg_id} acknowledged by '{consumer_id}'")

    # ----------------------------------------------------------------
    # Offset 管理
    # ----------------------------------------------------------------

    def set_offset(self, topic: str, consumer_id: str, offset: int) -> None:
        """设置消费者在 Topic 上的消费 offset"""
        if offset < 0:
            raise ValueError("Offset must not be negative")
        offset_key = f"{topic}:{consumer_id}"
        with self._topic_offset_lock:
            self._topic_offsets[offset_key] = offset
        logger.info(
            f"Consumer '{consumer_id}' offset for '{topic}' set to {offset}"
        )

    def get_offset(self, topic: str, consumer_id: str) -> int:
        """获取消费者在 Topic 上的消费 offset"""
        offset_key = f"{topic}:{consumer_id}"
        with self._topic_offset_lock:
            return self._topic_offsets.get(offset_key, 0)

    # ----------------------------------------------------------------
    # 内部方法
    # ----------------------------------------------------------------

    def _create_message(
        self, target: str, body: str, ttl: int = 0
    ) -> Message:
        """创建 Message 对象"""
        msg_id = Message.generate_id()
        return Message(
            msg_id=msg_id,
            target=target,
            body=body,
            timestamp=int(time.time()),
            ttl=ttl,
            status=MessageStatus.PENDING,
        )

    def get_message(self, msg_id: str) -> Message | None:
        """通过 msg_id 获取消息"""
        with self._msg_lock:
            return self._messages.get(msg_id)

    def restore_message(self, msg: Message) -> None:
        """从持久化恢复消息到内存（启动时调用）"""
        with self._msg_lock:
            if msg.msg_id not in self._messages:
                self._messages[msg.msg_id] = msg
                # 恢复的消息标记为 PENDING（如果未确认）
                if msg.status != MessageStatus.ACKED:
                    msg.status = MessageStatus.PENDING

    def get_pending_messages(self) -> list[Message]:
        """获取所有待投递的消息"""
        with self._msg_lock:
            return [
                m for m in self._messages.values()
                if m.status == MessageStatus.PENDING
            ]
