"""Topic Manager —— Topic 创建/删除/订阅管理"""

import re
import threading
import logging

logger = logging.getLogger("MQServer")

# Topic 名称验证: 字母、数字、下划线、连字符，长度 1-256
TOPIC_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,256}$")

# 默认最大 Topic 数量
DEFAULT_MAX_TOPICS = 1000


class TopicError(Exception):
    """Topic 相关错误基类"""
    pass


class TopicExistsError(TopicError):
    """Topic 已存在"""
    pass


class TopicNotFoundError(TopicError):
    """Topic 不存在"""
    pass


class TopicHasSubscribersError(TopicError):
    """Topic 有活跃订阅者"""
    pass


class MaxTopicsReachedError(TopicError):
    """Topic 数量达到上限"""
    pass


class InvalidTopicNameError(TopicError):
    """Topic 名称不合法"""
    pass


class TopicManager:
    """管理 Topic 的生命周期和订阅者"""

    def __init__(self, max_topics: int = DEFAULT_MAX_TOPICS):
        self._max_topics = max_topics
        # topic_name -> set of consumer_id
        self._topics: dict[str, set[str]] = {}
        self._lock = threading.Lock()

    # ----------------------------------------------------------------
    # Topic CRUD
    # ----------------------------------------------------------------

    def create_topic(self, name: str) -> None:
        """创建 Topic

        Raises:
            InvalidTopicNameError: 名称不合法
            TopicExistsError: Topic 已存在
            MaxTopicsReachedError: 超过最大 Topic 数
        """
        self._validate_name(name)

        with self._lock:
            if name in self._topics:
                raise TopicExistsError(f"Topic '{name}' already exists")
            if len(self._topics) >= self._max_topics:
                raise MaxTopicsReachedError(
                    f"Max topics ({self._max_topics}) reached"
                )
            self._topics[name] = set()
            logger.info(f"Topic '{name}' created")

    def delete_topic(self, name: str) -> None:
        """删除 Topic

        Raises:
            TopicNotFoundError: Topic 不存在
            TopicHasSubscribersError: Topic 有活跃订阅者
        """
        with self._lock:
            if name not in self._topics:
                raise TopicNotFoundError(f"Topic '{name}' not found")
            if self._topics[name]:
                raise TopicHasSubscribersError(
                    f"Topic '{name}' has {len(self._topics[name])} active subscriber(s)"
                )
            del self._topics[name]
            logger.info(f"Topic '{name}' deleted")

    def list_topics(self) -> list[str]:
        """列出所有 Topic 名称"""
        with self._lock:
            return sorted(self._topics.keys())

    def topic_exists(self, name: str) -> bool:
        """检查 Topic 是否存在"""
        with self._lock:
            return name in self._topics

    # ----------------------------------------------------------------
    # 订阅管理
    # ----------------------------------------------------------------

    def subscribe(self, topic: str, consumer_id: str) -> None:
        """消费者订阅 Topic（幂等操作）

        Raises:
            TopicNotFoundError: Topic 不存在
        """
        with self._lock:
            if topic not in self._topics:
                raise TopicNotFoundError(f"Topic '{topic}' not found")
            self._topics[topic].add(consumer_id)
            logger.info(f"Consumer '{consumer_id}' subscribed to '{topic}'")

    def unsubscribe(self, topic: str, consumer_id: str) -> None:
        """消费者取消订阅 Topic

        Raises:
            TopicNotFoundError: Topic 不存在
        """
        with self._lock:
            if topic not in self._topics:
                raise TopicNotFoundError(f"Topic '{topic}' not found")
            self._topics[topic].discard(consumer_id)
            logger.info(f"Consumer '{consumer_id}' unsubscribed from '{topic}'")

    def get_subscribers(self, topic: str) -> set[str]:
        """获取 Topic 的所有订阅者（返回副本）"""
        with self._lock:
            if topic not in self._topics:
                return set()
            return set(self._topics[topic])

    def remove_consumer_from_all(self, consumer_id: str) -> None:
        """从所有 Topic 中移除某消费者（消费者断线时调用）"""
        with self._lock:
            for topic, subscribers in self._topics.items():
                if consumer_id in subscribers:
                    subscribers.discard(consumer_id)
                    logger.info(
                        f"Consumer '{consumer_id}' removed from topic '{topic}'"
                    )

    # ----------------------------------------------------------------
    # 工具方法
    # ----------------------------------------------------------------

    @staticmethod
    def _validate_name(name: str) -> None:
        """验证 Topic 名称合法性

        Raises:
            InvalidTopicNameError
        """
        if not name:
            raise InvalidTopicNameError("Topic name must not be empty")
        if not TOPIC_NAME_RE.match(name):
            raise InvalidTopicNameError(
                "Topic name must match [a-zA-Z0-9_-]+, length 1-256"
            )

    @property
    def topic_count(self) -> int:
        with self._lock:
            return len(self._topics)
