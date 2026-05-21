"""Consumer Group Manager —— 消费者组生命周期 + Round-Robin + 心跳"""

import threading
import time
import logging

logger = logging.getLogger("MQServer")

# 心跳超时配置
DEFAULT_HEARTBEAT_INTERVAL = 10    # 客户端每 10s 应发送 PING
DEFAULT_HEARTBEAT_TIMEOUT = 30     # 30s 无心跳则视为离线


class GroupError(Exception):
    """消费者组错误基类"""
    pass


class GroupExistsError(GroupError):
    pass


class GroupNotFoundError(GroupError):
    pass


class GroupHasConsumersError(GroupError):
    pass


class ConsumerGroupManager:
    """管理消费者组的生命周期和 Round-Robin 负载均衡"""

    def __init__(self, heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT):
        self._heartbeat_timeout = heartbeat_timeout

        # group_name -> set of consumer_id
        self._groups: dict[str, set[str]] = {}
        # group_name -> target (queue/topic name)
        self._group_targets: dict[str, str] = {}
        # group_name -> round-robin index
        self._next_index: dict[str, int] = {}
        # consumer_id -> last heartbeat timestamp
        self._heartbeats: dict[str, float] = {}

        self._lock = threading.Lock()

    # ----------------------------------------------------------------
    # 组 CRUD
    # ----------------------------------------------------------------

    def create_group(self, target: str, group_name: str) -> None:
        """创建消费者组，绑定到 target（Queue 或 Topic）

        Raises:
            GroupExistsError
        """
        with self._lock:
            if group_name in self._groups:
                raise GroupExistsError(f"Group '{group_name}' already exists")
            self._groups[group_name] = set()
            self._group_targets[group_name] = target
            self._next_index[group_name] = 0
            logger.info(f"Group '{group_name}' created (target='{target}')")

    def delete_group(self, group_name: str) -> None:
        """删除消费者组

        Raises:
            GroupNotFoundError, GroupHasConsumersError
        """
        with self._lock:
            if group_name not in self._groups:
                raise GroupNotFoundError(f"Group '{group_name}' not found")
            if self._groups[group_name]:
                raise GroupHasConsumersError(
                    f"Group '{group_name}' has {len(self._groups[group_name])} active consumer(s)"
                )
            del self._groups[group_name]
            del self._group_targets[group_name]
            self._next_index.pop(group_name, None)
            logger.info(f"Group '{group_name}' deleted")

    def group_exists(self, group_name: str) -> bool:
        with self._lock:
            return group_name in self._groups

    def get_group_target(self, group_name: str) -> str | None:
        with self._lock:
            return self._group_targets.get(group_name)

    # ----------------------------------------------------------------
    # 成员管理
    # ----------------------------------------------------------------

    def join_group(self, group_name: str, consumer_id: str) -> None:
        """消费者加入组

        Raises:
            GroupNotFoundError
        """
        with self._lock:
            if group_name not in self._groups:
                raise GroupNotFoundError(f"Group '{group_name}' not found")
            self._groups[group_name].add(consumer_id)
            self._heartbeats[consumer_id] = time.time()
            logger.info(f"Consumer '{consumer_id}' joined group '{group_name}'")

    def leave_group(self, group_name: str, consumer_id: str) -> None:
        """消费者离开组（优雅退出）

        Raises:
            GroupNotFoundError
        """
        with self._lock:
            if group_name not in self._groups:
                raise GroupNotFoundError(f"Group '{group_name}' not found")
            self._groups[group_name].discard(consumer_id)
            logger.info(f"Consumer '{consumer_id}' left group '{group_name}'")

    def remove_consumer_from_all_groups(self, consumer_id: str) -> list[str]:
        """从所有组中移除消费者，返回被移除的组名列表"""
        removed = []
        with self._lock:
            self._heartbeats.pop(consumer_id, None)
            for group_name, members in self._groups.items():
                if consumer_id in members:
                    members.discard(consumer_id)
                    removed.append(group_name)
                    logger.info(
                        f"Consumer '{consumer_id}' removed from group '{group_name}'"
                    )
        return removed

    # ----------------------------------------------------------------
    # Round-Robin
    # ----------------------------------------------------------------

    def next_consumer(self, group_name: str) -> str | None:
        """Round-Robin 选择下一个消费者

        Returns:
            consumer_id 或 None（组中没有消费者）
        """
        with self._lock:
            if group_name not in self._groups:
                return None
            members = self._groups[group_name]
            if not members:
                return None

            idx = self._next_index.get(group_name, 0)
            member_list = sorted(members)  # 排序保证确定性
            if not member_list:
                return None
            consumer = member_list[idx % len(member_list)]
            self._next_index[group_name] = (idx + 1) % len(member_list)
            return consumer

    def get_group_members(self, group_name: str) -> set[str]:
        """获取组内所有成员"""
        with self._lock:
            if group_name not in self._groups:
                return set()
            return set(self._groups[group_name])

    # ----------------------------------------------------------------
    # 心跳
    # ----------------------------------------------------------------

    def heartbeat(self, consumer_id: str) -> None:
        """记录心跳"""
        with self._lock:
            self._heartbeats[consumer_id] = time.time()

    def check_timeouts(self) -> list[str]:
        """检查心跳超时的消费者，返回已超时的 consumer_id 列表"""
        now = time.time()
        timed_out = []
        with self._lock:
            for cid, last_hb in list(self._heartbeats.items()):
                if now - last_hb > self._heartbeat_timeout:
                    timed_out.append(cid)
                    self._heartbeats.pop(cid, None)
                    # 从所有组中移除
                    for group_name, members in self._groups.items():
                        if cid in members:
                            members.discard(cid)
                            logger.info(
                                f"Consumer '{cid}' timed out, removed from group '{group_name}'"
                            )
        return timed_out

    @property
    def heartbeat_timeout(self) -> float:
        return self._heartbeat_timeout
