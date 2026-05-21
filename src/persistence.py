"""持久化存储层 —— 消息序列化、分段日志、索引管理、崩溃恢复

文件格式 (二进制):
    [4B 总长度(不含自身)] [2B ID长度] [N B ID] [2B Target长度] [N B Target]
    [4B Body长度] [N B Body] [8B 时间戳] [4B TTL] [4B ack_timeout]

文件布局:
    data/
    ├── mq-0001.log    # 日志段文件 (append-only, ≤64MB)
    ├── mq-0001.idx    # 索引文件 (msg_id -> offset 映射)
    ├── mq-0002.log
    ├── mq-0002.idx
    └── consumer_offsets.json
"""

import os
import struct
import threading
import time
import json
import logging
import glob
import re

from src.models import Message, MessageStatus

logger = logging.getLogger("MQServer")

# ----------------------------------------------------------------
# 常量
# ----------------------------------------------------------------

DEFAULT_DATA_DIR = "data"
DEFAULT_MAX_SEGMENT_SIZE = 64 * 1024 * 1024  # 64 MB
DEFAULT_MAX_SEGMENTS = 100
MIN_FREE_DISK_MB = 10
DEFAULT_FSYNC_INTERVAL = 1.0  # 秒
SEGMENT_FILE_PATTERN = re.compile(r"^mq-(\d{4})\.log$")

# ----------------------------------------------------------------
# 序列化 / 反序列化
# ----------------------------------------------------------------

def serialize(msg: Message) -> bytes:
    """将 Message 序列化为二进制格式
    
    格式:
        [4B total_len] [2B id_len] [id_bytes] [2B target_len] [target_bytes]
        [4B body_len] [body_bytes] [8B timestamp] [4B ttl] [4B ack_timeout]
    """
    id_bytes = msg.msg_id.encode("utf-8")
    target_bytes = msg.target.encode("utf-8")
    body_bytes = msg.body.encode("utf-8")

    # 计算总长度 (不含 total_len 自身)
    total_len = (
        2 + len(id_bytes) +
        2 + len(target_bytes) +
        4 + len(body_bytes) +
        8 + 4 + 4
    )

    fmt = f">I H{len(id_bytes)}s H{len(target_bytes)}s I{len(body_bytes)}s q i i"
    packed = struct.pack(
        fmt,
        total_len,
        len(id_bytes), id_bytes,
        len(target_bytes), target_bytes,
        len(body_bytes), body_bytes,
        msg.timestamp,
        msg.ttl,
        msg.ack_timeout,
    )
    return packed


def deserialize(data: bytes) -> Message:
    """从二进制格式反序列化为 Message 对象"""
    # 读取总长度
    total_len = struct.unpack_from(">I", data, 0)[0]
    offset = 4

    # 读 ID
    id_len = struct.unpack_from(">H", data, offset)[0]
    offset += 2
    id_bytes = struct.unpack_from(f">{id_len}s", data, offset)[0]
    msg_id = id_bytes.decode("utf-8")
    offset += id_len

    # 读 Target
    target_len = struct.unpack_from(">H", data, offset)[0]
    offset += 2
    target_bytes = struct.unpack_from(f">{target_len}s", data, offset)[0]
    target = target_bytes.decode("utf-8")
    offset += target_len

    # 读 Body
    body_len = struct.unpack_from(">I", data, offset)[0]
    offset += 4
    body_bytes = struct.unpack_from(f">{body_len}s", data, offset)[0]
    body = body_bytes.decode("utf-8")
    offset += body_len

    # 读时间戳
    timestamp = struct.unpack_from(">q", data, offset)[0]
    offset += 8

    # 读 TTL
    ttl = struct.unpack_from(">i", data, offset)[0]
    offset += 4

    # 读 ack_timeout
    ack_timeout = struct.unpack_from(">i", data, offset)[0]

    return Message(
        msg_id=msg_id,
        target=target,
        body=body,
        timestamp=timestamp,
        ttl=ttl,
        ack_timeout=ack_timeout,
        status=MessageStatus.PENDING,
    )


# ----------------------------------------------------------------
# Segment Manager
# ----------------------------------------------------------------

class SegmentManager:
    """管理日志段文件"""

    def __init__(
        self,
        data_dir: str = DEFAULT_DATA_DIR,
        max_segment_size: int = DEFAULT_MAX_SEGMENT_SIZE,
        max_segments: int = DEFAULT_MAX_SEGMENTS,
    ):
        self.data_dir = data_dir
        self.max_segment_size = max_segment_size
        self.max_segments = max_segments
        self._active_segment: str | None = None
        self._active_size: int = 0
        self._segments: list[str] = []  # 按创建顺序排列
        self._lock = threading.Lock()

        os.makedirs(data_dir, exist_ok=True)
        self._scan_existing_segments()

    def get_active_segment(self) -> str:
        """获取当前活跃段文件路径，必要时创建新段"""
        with self._lock:
            if self._active_segment is None or self._active_size >= self.max_segment_size:
                self._roll_segment()
            return self._active_segment

    def get_active_size(self) -> int:
        with self._lock:
            return self._active_size

    def add_bytes(self, n: int) -> None:
        """记录写入字节数（用于判断是否需要分段）"""
        with self._lock:
            self._active_size += n

    def list_segments(self) -> list[str]:
        """返回所有段文件路径（按创建顺序）"""
        with self._lock:
            return list(self._segments)

    def delete_segment(self, filename: str) -> None:
        """删除段文件及其索引文件"""
        log_path = os.path.join(self.data_dir, filename)
        idx_path = log_path.replace(".log", ".idx")
        with self._lock:
            for path in (log_path, idx_path):
                if os.path.exists(path):
                    os.remove(path)
                    logger.info(f"Deleted segment file: {path}")
            if filename in self._segments:
                self._segments.remove(filename)

    def _roll_segment(self) -> None:
        """创建新的日志段"""
        # 检查段文件数量上限
        active_log_count = len(self._segments)
        if active_log_count >= self.max_segments:
            raise RuntimeError(
                f"Too many segments ({active_log_count}). "
                "Wait for old segments to be cleaned up."
            )

        # 生成新段文件名
        seq = active_log_count + 1
        filename = f"mq-{seq:04d}.log"
        path = os.path.join(self.data_dir, filename)

        # 创建空文件
        with open(path, "ab") as _:
            pass

        self._active_segment = filename
        self._active_size = 0
        self._segments.append(filename)
        logger.info(f"Created new segment: {filename}")

    def _scan_existing_segments(self) -> None:
        """扫描 data 目录中已有的段文件"""
        pattern = os.path.join(self.data_dir, "mq-*.log")
        existing = sorted(glob.glob(pattern))
        self._segments = [os.path.basename(p) for p in existing]

        if self._segments:
            # 使用最后一个段作为活跃段
            self._active_segment = self._segments[-1]
            last_path = os.path.join(self.data_dir, self._active_segment)
            self._active_size = os.path.getsize(last_path)
            logger.info(
                f"Resuming from existing segment: {self._active_segment} "
                f"({self._active_size} bytes)"
            )
        else:
            # 无已有段文件，创建第一个
            self._roll_segment()


# ----------------------------------------------------------------
# Index Manager
# ----------------------------------------------------------------

class IndexManager:
    """管理消息 ID 到 (segment_file, offset) 的映射"""

    def __init__(self, data_dir: str = DEFAULT_DATA_DIR):
        self.data_dir = data_dir
        self._index: dict[str, tuple[str, int]] = {}  # msg_id -> (segment, offset)
        self._lock = threading.Lock()

    def put(self, msg_id: str, segment: str, offset: int) -> None:
        with self._lock:
            self._index[msg_id] = (segment, offset)

    def get(self, msg_id: str) -> tuple[str, int] | None:
        with self._lock:
            return self._index.get(msg_id)

    def remove(self, msg_id: str) -> None:
        with self._lock:
            self._index.pop(msg_id, None)

    def persist(self) -> None:
        """将索引刷到磁盘"""
        idx_path = os.path.join(self.data_dir, "index.json")
        with self._lock:
            data = {k: list(v) for k, v in self._index.items()}
        tmp_path = idx_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp_path, idx_path)

    def recover(self) -> None:
        """从磁盘恢复索引"""
        idx_path = os.path.join(self.data_dir, "index.json")
        if not os.path.exists(idx_path):
            logger.info("No index file found, will rebuild from log segments")
            return

        try:
            with open(idx_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            with self._lock:
                self._index = {k: tuple(v) for k, v in data.items()}
            logger.info(f"Recovered {len(self._index)} index entries from disk")
        except (json.JSONDecodeError, KeyError, OSError) as e:
            logger.warning(f"Index file corrupted: {e}, will rebuild from log")
            self._index = {}

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._index)

    def get_all_entries(self) -> list[tuple[str, str, int]]:
        """返回所有 (msg_id, segment, offset) 条目"""
        with self._lock:
            return [(mid, seg, off) for mid, (seg, off) in self._index.items()]


# ----------------------------------------------------------------
# Persistence Layer (统一入口)
# ----------------------------------------------------------------

class PersistenceLayer:
    """持久化层统一接口"""

    def __init__(
        self,
        data_dir: str = DEFAULT_DATA_DIR,
        max_segment_size: int = DEFAULT_MAX_SEGMENT_SIZE,
        fsync_on_write: bool = False,
        fsync_interval: float = DEFAULT_FSYNC_INTERVAL,
    ):
        self.data_dir = data_dir
        self.fsync_on_write = fsync_on_write
        self.fsync_interval = fsync_interval
        self._last_fsync = time.time()

        self.segment_mgr = SegmentManager(data_dir, max_segment_size)
        self.index_mgr = IndexManager(data_dir)
        self._write_lock = threading.Lock()

        # 文件句柄缓存
        self._file_handles: dict[str, object] = {}

    # ------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------

    def append(self, msg: Message) -> int:
        """追加一条消息到日志，返回写入偏移量

        Raises:
            OSError: 磁盘空间不足
        """
        data = serialize(msg)
        segment = self.segment_mgr.get_active_segment()

        # 检查磁盘空间
        self._check_disk_space()

        with self._write_lock:
            f = self._get_file(segment)
            offset = f.tell()
            f.write(data)
            f.flush()

            if self.fsync_on_write:
                os.fsync(f.fileno())

            self.segment_mgr.add_bytes(len(data))
            self.index_mgr.put(msg.msg_id, segment, offset)

            # 检查是否需要定时 fsync
            now = time.time()
            if not self.fsync_on_write and (now - self._last_fsync) >= self.fsync_interval:
                os.fsync(f.fileno())
                self._last_fsync = now

        logger.debug(f"Appended {msg.msg_id} to {segment} at offset {offset}")
        return offset

    def fsync(self) -> None:
        """强制刷盘所有打开的文件"""
        with self._write_lock:
            for f in self._file_handles.values():
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except OSError:
                    pass
            self._last_fsync = time.time()

    # ------------------------------------------------------------
    # 读取
    # ------------------------------------------------------------

    def read(self, msg_id: str) -> Message | None:
        """根据消息 ID 读取消息"""
        loc = self.index_mgr.get(msg_id)
        if loc is None:
            return None

        segment, offset = loc
        path = os.path.join(self.data_dir, segment)
        if not os.path.exists(path):
            return None

        try:
            with open(path, "rb") as f:
                f.seek(offset)
                # 读取总长度
                header = f.read(4)
                if len(header) < 4:
                    return None
                total_len = struct.unpack(">I", header)[0]
                f.seek(offset)
                raw = f.read(4 + total_len)
                return deserialize(raw)
        except (OSError, struct.error) as e:
            logger.warning(f"Failed to read message {msg_id}: {e}")
            return None

    def read_at(self, segment: str, offset: int) -> Message | None:
        """从指定段文件的指定偏移量读取消息"""
        path = os.path.join(self.data_dir, segment)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "rb") as f:
                f.seek(offset)
                header = f.read(4)
                if len(header) < 4:
                    return None
                total_len = struct.unpack(">I", header)[0]
                f.seek(offset)
                raw = f.read(4 + total_len)
                return deserialize(raw)
        except (OSError, struct.error):
            return None

    # ------------------------------------------------------------
    # 崩溃恢复
    # ------------------------------------------------------------

    def recover(self) -> list[Message]:
        """从日志段文件恢复所有消息

        步骤:
        1. 尝试从索引文件恢复
        2. 如果索引不可用，扫描所有日志段重建
        3. 返回所有未确认的消息列表
        """
        self.index_mgr.recover()

        if self.index_mgr.size > 0:
            # 索引恢复成功
            messages = []
            for msg_id, segment, offset in self.index_mgr.get_all_entries():
                msg = self.read_at(segment, offset)
                if msg is not None:
                    messages.append(msg)
            logger.info(f"Recovered {len(messages)} messages from index")
            return messages

        # 索引不可用，扫描所有段文件重建
        logger.info("Rebuilding index from log segments...")
        messages = []
        for segment in self.segment_mgr.list_segments():
            path = os.path.join(self.data_dir, segment)
            try:
                with open(path, "rb") as f:
                    offset = 0
                    while True:
                        header = f.read(4)
                        if len(header) < 4:
                            break
                        total_len = struct.unpack(">I", header)[0]
                        f.seek(offset)
                        raw = f.read(4 + total_len)
                        if len(raw) < 4:
                            # 最后一条消息不完整（崩溃导致）
                            logger.warning(
                                f"Truncated message at {segment}:{offset}"
                            )
                            break
                        try:
                            msg = deserialize(raw)
                            messages.append(msg)
                            self.index_mgr.put(msg.msg_id, segment, offset)
                            offset += len(raw)
                        except (struct.error, UnicodeDecodeError) as e:
                            logger.warning(
                                f"Corrupted message at {segment}:{offset}: {e}"
                            )
                            break
            except OSError as e:
                logger.warning(f"Failed to read segment {segment}: {e}")

        logger.info(
            f"Recovered {len(messages)} messages from {len(self.segment_mgr.list_segments())} segments"
        )
        return messages

    # ------------------------------------------------------------
    # 维护
    # ------------------------------------------------------------

    def cleanup_segments(self, acked_msg_ids: set[str]) -> int:
        """清理已全部确认的日志段文件，返回清理的段数量"""
        # 获取每个段中所有消息的确认状态
        segment_msgs: dict[str, list[str]] = {}
        for msg_id, (segment, _) in self.index_mgr.get_all_entries():
            segment_msgs.setdefault(segment, []).append(msg_id)

        cleaned = 0
        for segment, msg_ids in segment_msgs.items():
            # 如果段中所有消息都已确认，且不是当前活跃段
            if segment == self.segment_mgr.get_active_segment():
                continue
            if all(mid in acked_msg_ids for mid in msg_ids):
                # 从索引中移除这些消息
                for mid in msg_ids:
                    self.index_mgr.remove(mid)
                self.segment_mgr.delete_segment(segment)
                cleaned += 1

        return cleaned

    def close(self) -> None:
        """关闭所有文件句柄并保存索引"""
        self.fsync()
        self.index_mgr.persist()
        for f in self._file_handles.values():
            try:
                f.close()
            except OSError:
                pass
        self._file_handles.clear()
        logger.info("Persistence layer closed")

    # ------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------

    def _get_file(self, segment: str):
        """获取段文件的文件句柄（缓存）"""
        if segment not in self._file_handles:
            path = os.path.join(self.data_dir, segment)
            f = open(path, "ab")
            self._file_handles[segment] = f
        return self._file_handles[segment]

    def _check_disk_space(self) -> None:
        """检查磁盘可用空间"""
        try:
            stat = os.statvfs(self.data_dir)
            free_mb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
            if free_mb < MIN_FREE_DISK_MB:
                raise OSError(
                    f"Disk space low: {free_mb:.1f}MB free, "
                    f"minimum {MIN_FREE_DISK_MB}MB required"
                )
        except AttributeError:
            # Windows 无 statvfs，用简单替代
            import shutil
            usage = shutil.disk_usage(self.data_dir)
            free_mb = usage.free / (1024 * 1024)
            if free_mb < MIN_FREE_DISK_MB:
                raise OSError(
                    f"Disk space low: {free_mb:.1f}MB free, "
                    f"minimum {MIN_FREE_DISK_MB}MB required"
                )
