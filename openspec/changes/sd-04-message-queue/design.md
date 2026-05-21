# Design: 轻量级消息队列实现 (SD-04)

## 架构概览

```
                        ┌──────────────────────────────────┐
                        │         TCP Server (端口 9876)      │
                        │   socket + threading 多线程模型     │
                        └──────────────┬───────────────────┘
                                       │
                        ┌──────────────▼───────────────────┐
                        │        Protocol Parser            │
                        │   文本协议解析 (类 RESP 风格)       │
                        └──────────────┬───────────────────┘
                                       │
          ┌────────────────────────────┼────────────────────────────┐
          │                            │                            │
┌─────────▼─────────┐    ┌────────────▼───────────┐    ┌───────────▼───────────┐
│   Topic Manager   │    │     Queue Manager       │    │  Consumer Group Mgr   │
│  · 创建/删除 Topic │    │  · 创建/删除 Queue      │    │  · 组创建/删除         │
│  · 订阅者管理      │    │  · FIFO 入队/出队        │    │  · 成员管理           │
│  · 广播投递       │    │  · 容量控制             │    │  · Round-Robin 分配   │
└─────────┬─────────┘    └────────────┬───────────┘    │  · 心跳检测           │
          │                            │                └───────────┬───────────┘
          │                            │                            │
          └────────────────────────────┼────────────────────────────┘
                                       │
                        ┌──────────────▼───────────────────┐
                        │       Message Router             │
                        │   统一消息路由 & 状态管理          │
                        └──────────────┬───────────────────┘
                                       │
          ┌────────────────────────────┼────────────────────────────┐
          │                            │                            │
┌─────────▼─────────┐    ┌────────────▼───────────┐    ┌───────────▼───────────┐
│ Persistence Layer │    │  Acknowledgment Mgr    │    │     Index Manager      │
│  · Append-only Log│    │  · ACK / NACK 处理     │    │  · 消息ID → 物理位置   │
│  · Segment 管理   │    │  · 超时重投            │    │  · 内存 LRU 缓存       │
│  · WAL + fsync    │    │  · DLQ 死信队列        │    │  · 磁盘索引持久化       │
│  · 崩溃恢复       │    │  · TTL 过期清理        │    │  · 索引重建            │
└─────────┬─────────┘    └────────────────────────┘    └───────────────────────┘
          │
┌─────────▼─────────┐
│   File System     │
│  data/            │
│  ├── mq-0001.log  │
│  ├── mq-0001.idx  │
│  ├── mq-0002.log  │
│  └── ...          │
└───────────────────┘
```

---

## 模块划分

### Module 1: TCP Server (`server.py`)

**职责**: 接受客户端 TCP 连接，为每个连接创建独立线程，读取原始数据并交给 Protocol Parser。

**关键接口**:
```python
class MQServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 9876)
    def start(self) -> None          # 启动监听
    def stop(self) -> None           # 优雅关闭
    def _handle_client(self, conn: socket.socket, addr: tuple) -> None
```

**设计要点**:
- 使用 `threading.Thread` 为每个客户端连接创建独立处理线程
- 主线程负责 `accept()`，子线程负责读写
- 关闭时等待所有子线程完成（设置超时 5s）

---

### Module 2: Protocol Parser (`protocol.py`)

**职责**: 解析客户端发送的文本命令，返回结构化的命令对象。

**协议格式**（类 Redis RESP 文本协议）:
```
命令格式: <COMMAND> [arg1] [arg2] [...] [body]\r\n
- 命令和参数以空格分隔
- 消息体放在最后，可含空格
- 每条命令以 \r\n 结尾
```

**支持的命令清单**:
```
# Topic / Queue 管理
CREATE_TOPIC <name>
DELETE_TOPIC <name>
LIST_TOPICS
CREATE_QUEUE <name> [max_size]
DELETE_QUEUE <name>
LIST_QUEUES

# 消息生产
PUBLISH <topic> <body>
SEND <queue> <body>

# 消息消费
CONSUME <queue> [TIMEOUT <ms>]
SUBSCRIBE <topic>
UNSUBSCRIBE <topic>
CONSUME_GROUP <queue> <group>
SET_OFFSET <topic> <offset>

# 消息确认
ACK <msg_id>
NACK <msg_id>
ACK_BATCH <msg_id1> [msg_id2] [...]

# 消费者组
CREATE_GROUP <target> <group_name>
DELETE_GROUP <group_name>
LEAVE_GROUP <group_name>

# 心跳
PING

# DLQ
CONSUME DLQ
REPLAY DLQ <msg_id>
REPLAY_ALL DLQ <target>

# 连接
QUIT
```

**关键接口**:
```python
class Command:
    name: str           # 命令名, e.g. "PUBLISH"
    args: list[str]     # 参数列表, e.g. ["orders", '{"key":"value"}']

def parse_command(raw: str) -> Command

# 错误响应编码
def ok_response(data: str = "") -> str       # "+OK ...\r\n"
def err_response(code: str, msg: str) -> str  # "-ERR_CODE msg\r\n"
def null_response() -> str                    # "$-1\r\n"
def data_response(data: str) -> str           # "$len\r\n data\r\n"
```

---

### Module 3: Topic Manager (`topic_manager.py`)

**职责**: 管理 Topic 的创建、删除、查询，维护订阅者列表，广播消息。

**关键接口**:
```python
class TopicManager:
    def create_topic(self, name: str) -> None        # raises TopicExistsError
    def delete_topic(self, name: str) -> None         # raises TopicHasSubscribersError
    def list_topics(self) -> list[str]
    def subscribe(self, topic: str, consumer_id: str) -> None
    def unsubscribe(self, topic: str, consumer_id: str) -> None
    def get_subscribers(self, topic: str) -> list[str]
    def remove_consumer(self, consumer_id: str) -> None  # 从所有Topic移除
```

**数据结构**:
```python
# 内存结构
_topics: dict[str, set[str]]  # topic_name -> {consumer_id, ...}
```

**验证规则**:
- Topic 名称: `^[a-zA-Z0-9_-]{1,256}$`
- 最大 Topic 数: 1000（可配置）

---

### Module 4: Queue Manager (`queue_manager.py`)

**职责**: 管理 Queue 的创建、删除、查询，消息的 FIFO 入队出队。

**关键接口**:
```python
class QueueManager:
    def create_queue(self, name: str, max_size: int = 10000) -> None
    def delete_queue(self, name: str) -> None
    def list_queues(self) -> list[str]
    def enqueue(self, queue: str, message: Message) -> str  # 返回 msg_id
    def dequeue(self, queue: str, consumer_id: str) -> Message | None
    def get_queue_depth(self, queue: str) -> int
```

**数据结构**:
```python
# 每个 Queue 的消息链表
_queues: dict[str, collections.deque]  # queue_name -> deque of Message
_queue_max_sizes: dict[str, int]
```

**验证规则**:
- Queue 名称: 同 Topic 规则
- 最大容量: 默认 10000，可配置
- 消息体大小: ≤ 1MB

---

### Module 5: Message Router (`message_router.py`)

**职责**: 统一管理消息的流转——持久化、路由到 Topic/Queue、状态跟踪。

**关键接口**:
```python
class MessageRouter:
    def route_to_topic(self, topic: str, body: str, ttl: int = 0) -> str
    def route_to_queue(self, queue: str, body: str, ttl: int = 0) -> str
    def consume_from_queue(self, queue: str, consumer_id: str, 
                           group: str = None, timeout: int = 0) -> Message | None
    def consume_from_topic(self, topic: str, consumer_id: str) -> Message | None
```

**消息状态机**:
```
PENDING  →  DELIVERED  →  ACKED
                ↓
          (超时/NACK)
                ↓
           PENDING (重试)
                ↓ (超 max_retries)
              DLQ
```

---

### Module 6: Persistence Layer (`persistence.py`)

**职责**: 消息的磁盘持久化存储，包括追加写入、分段管理、索引维护、崩溃恢复。

**关键接口**:
```python
class PersistenceLayer:
    def __init__(self, data_dir: str = "data", max_segment_size: int = 64*1024*1024)
    def append(self, message: Message) -> int           # 返回写入偏移量
    def read(self, segment: str, offset: int) -> Message
    def fsync(self) -> None
    def recover(self) -> list[Message]                  # 恢复所有未确认消息
    def cleanup_segments(self) -> int                   # 清理已消费段
    def close(self) -> None

class SegmentManager:
    def create_segment(self) -> str                     # 返回段文件名
    def get_active_segment(self) -> str
    def list_segments(self) -> list[str]
    def delete_segment(self, filename: str) -> None

class IndexManager:
    def put(self, msg_id: str, segment: str, offset: int) -> None
    def get(self, msg_id: str) -> tuple[str, int] | None
    def persist(self) -> None                           # 刷索引到磁盘
    def recover(self) -> None                           # 从磁盘恢复索引
```

**文件布局**:
```
data/
├── mq-0001.log    # 日志段文件 (append-only, ≤64MB)
├── mq-0001.idx    # 索引文件 (msg_id -> offset映射)
├── mq-0002.log
├── mq-0002.idx
└── consumer_offsets.json  # 消费者 offset 持久化
```

**消息磁盘格式** (二进制):
```
[4B 总长度] [2B ID长度] [N B ID] [2B Target长度] [N B Target] [4B Body长度] [N B Body] [8B 时间戳]
```

**刷盘策略**:
- 默认 `fsync_interval=1000ms`：每秒刷一次
- 可配置 `fsync_on_write=true`：每条消息立即刷盘（牺牲吞吐换持久性）

---

### Module 7: Consumer Group Manager (`consumer_group.py`)

**职责**: 管理消费者组的生命周期、Round-Robin 负载均衡、心跳检测。

**关键接口**:
```python
class ConsumerGroupManager:
    def create_group(self, target: str, group_name: str) -> None
    def delete_group(self, group_name: str) -> None
    def join_group(self, group_name: str, consumer_id: str) -> None
    def leave_group(self, group_name: str, consumer_id: str) -> None
    def heartbeat(self, consumer_id: str) -> None
    def next_consumer(self, group_name: str) -> str     # Round-Robin 选择
    def get_group_offset(self, group_name: str) -> int
    def advance_offset(self, group_name: str) -> None
    def handle_failures(self) -> None                   # 超时检测
```

**Round-Robin 算法**:
```python
_next_index: dict[str, int]  # group_name -> 下次分配的消费者索引

def next_consumer(self, group_name: str) -> str:
    members = self._groups[group_name]
    idx = self._next_index.get(group_name, 0)
    consumer = list(members)[idx % len(members)]
    self._next_index[group_name] = (idx + 1) % len(members)
    return consumer
```

**心跳机制**:
- 客户端每 10s 发送 `PING`
- 服务端 30s 未收到心跳则标记消费者离线
- 离线消费者的未确认消息重新分配

---

### Module 8: Acknowledgment Manager (`ack_manager.py`)

**职责**: 处理 ACK/NACK、超时重投、DLQ 死信队列、TTL 过期清理。

**关键接口**:
```python
class AckManager:
    def ack(self, msg_id: str, consumer_id: str) -> None
    def nack(self, msg_id: str, consumer_id: str) -> None
    def ack_batch(self, msg_ids: list[str], consumer_id: str) -> list[str]  # 返回失败的
    def check_timeouts(self) -> None                     # 扫描超时消息并重投
    def check_ttl(self) -> None                          # 扫描过期消息
    def move_to_dlq(self, msg: Message, reason: str) -> None
    def replay_from_dlq(self, msg_id: str) -> None
    def replay_all_dlq(self, target: str) -> None
```

**重试策略**:
- 默认 `max_retries=5`
- 重试间隔: 立即重投（无退避），简化实现
- 达到上限后移入 DLQ

**超时检测**: 后台线程每 5s 扫描一次所有 "待确认" 消息，超时的重新入队。

---

### Module 9: CLI Client (`client.py`)

**职责**: 命令行客户端工具，用于与 MQ Server 交互。

**使用示例**:
```bash
# 连接到服务器
python client.py connect localhost 9876

# 创建 Topic
python client.py create_topic orders

# 发布消息
python client.py publish orders '{"order_id": 123}'

# 消费消息
python client.py consume tasks

# 订阅 Topic
python client.py subscribe orders
```

---

## 数据模型

### Message（核心数据结构）
```python
@dataclass
class Message:
    msg_id: str           # 全局唯一 ID，格式: M{timestamp}{seq}
    target: str           # Topic 或 Queue 名称
    body: str             # 消息体 (JSON 字符串)
    timestamp: int        # Unix 时间戳 (秒)
    ttl: int              # TTL 秒数，0=服务器默认，-1=永不过期
    ack_timeout: int      # ACK 超时秒数，0=服务器默认
    
    # 运行时状态（不持久化）
    status: str           # PENDING | DELIVERED | ACKED | DLQ | EXPIRED
    retry_count: int      # 当前重试次数
    delivered_to: str     # 当前分配给的消费者 ID
    deliver_time: float   # 分配时间（用于超时判断）
    original_target: str  # 移入 DLQ 前的原始 target
```

### Consumer（消费者会话）
```python
@dataclass
class Consumer:
    consumer_id: str      # 格式: "C{conn_id}"
    conn: socket.socket   # TCP 连接
    subscriptions: set[str]  # 已订阅的 Topic 集合
    groups: set[str]      # 所属消费者组集合
    last_heartbeat: float # 最后心跳时间
```

---

## 技术选型说明

| 决策 | 选择 | 理由 |
|------|------|------|
| 语言 | Python 3.9+ | 培训要求，快速开发，标准库丰富 |
| 网络模型 | 多线程 (threading) | 简化实现，每个连接一个线程，无需 async |
| 协议格式 | 文本协议（类 RESP） | 可读性强，调试方便，CLI 可以直接 telnet 测试 |
| 存储格式 | 二进制 | 紧凑、支持特殊字符和二进制数据 |
| 消息体格式 | JSON 字符串 | 通用、可读、与语言无关 |
| 索引结构 | 内存 dict + 磁盘备份 | 简单高效，满足单机场景 |
| 依赖 | 仅 Python 标准库 | 零外部依赖，降低环境复杂度 |

---

## 并发模型

```
Main Thread (accept loop)
    │
    ├── ClientHandler Thread 1 (recv/send loop)
    ├── ClientHandler Thread 2
    ├── ...
    ├── ClientHandler Thread N
    │
    ├── TimeoutChecker Thread (每 5s 扫描超时 ACK + TTL)
    └── FsyncThread (每 1s 执行 fsync)

共享资源保护:
- _topics, _queues, _groups: threading.Lock()
- Log file write: threading.Lock() (文件级锁)
- Index: threading.Lock()
```

---

## 错误码规范

| 错误码 | 含义 |
|--------|------|
| `ERR_TOPIC_NOT_FOUND` | Topic 不存在 |
| `ERR_TOPIC_EXISTS` | Topic 已存在 |
| `ERR_TOPIC_HAS_SUBSCRIBERS` | Topic 有活跃订阅者 |
| `ERR_MAX_TOPICS_REACHED` | Topic 数量达到上限 |
| `ERR_QUEUE_NOT_FOUND` | Queue 不存在 |
| `ERR_QUEUE_EXISTS` | Queue 已存在 |
| `ERR_QUEUE_NOT_EMPTY` | Queue 中有未消费消息 |
| `ERR_QUEUE_FULL` | Queue 已满 |
| `ERR_GROUP_EXISTS` | 消费者组已存在 |
| `ERR_GROUP_NOT_FOUND` | 消费者组不存在 |
| `ERR_GROUP_HAS_CONSUMERS` | 组中有活跃消费者 |
| `ERR_INVALID_NAME` | 名称不合法 |
| `ERR_NAME_TOO_LONG` | 名称过长 |
| `ERR_EMPTY_MESSAGE` | 消息体为空 |
| `ERR_MESSAGE_TOO_LARGE` | 消息体过大 |
| `ERR_MESSAGE_NOT_FOUND` | 消息不存在 |
| `ERR_NOT_YOUR_MESSAGE` | 消息不属于当前消费者 |
| `ERR_NOT_SUBSCRIBED` | 未订阅该 Topic |
| `ERR_INVALID_OFFSET` | 无效的 Offset |
| `ERR_INVALID_TTL` | 无效的 TTL |
| `ERR_TTL_TOO_LARGE` | TTL 超出上限 |
| `ERR_DISK_FULL` | 磁盘空间不足 |
| `ERR_TOO_MANY_SEGMENTS` | 段文件数量超限 |
| `ERR_INVALID_COMMAND` | 无法识别的命令 |
