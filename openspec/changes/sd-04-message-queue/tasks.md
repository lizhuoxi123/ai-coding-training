# Tasks: 轻量级消息队列实现 (SD-04)

> 按阶段拆解的原子任务，每个任务可独立提交。遵循 Conventional Commits 规范。

---

## Phase 1: 基础架构搭建

- [ ] **Task 1.1**: 创建项目骨架目录结构
  - 创建 `src/` 目录，`__init__.py`，`main.py` 入口
  - 创建 `data/` 数据目录（`.gitkeep`）
  - 创建 `tests/` 测试目录
  - 创建 `README.md`（项目说明）
  - **产出**: 可运行的空白项目

- [ ] **Task 1.2**: 实现数据模型 `Message` 和 `Consumer`
  - 在 `src/models.py` 中定义 `Message` dataclass（含所有字段）
  - 定义 `Consumer` dataclass
  - 定义消息状态枚举: `PENDING`, `DELIVERED`, `ACKED`, `DLQ`, `EXPIRED`
  - 实现 `Message.generate_id()` 生成全局唯一 ID
  - **产出**: `src/models.py` + 单元测试

- [ ] **Task 1.3**: 实现协议解析器 `Protocol Parser`
  - 在 `src/protocol.py` 中实现 `parse_command(raw: str) -> Command`
  - 支持所有命令的解析（20+ 命令）
  - 实现响应编码函数: `ok_response()`, `err_response()`, `null_response()`, `data_response()`
  - 处理边界情况：空行、只有空格、不完整命令
  - **产出**: `src/protocol.py` + 单元测试（至少覆盖 10 种命令）

- [ ] **Task 1.4**: 实现 TCP Server 骨架
  - 在 `src/server.py` 中实现 `MQServer` 类
  - 实现 `start()` 监听端口 9876
  - 实现 `_handle_client()` 多线程处理连接
  - 实现 `stop()` 优雅关闭（等待活跃线程完成）
  - 实现命令分发骨架（stub handler，返回 "not implemented"）
  - **产出**: `src/server.py`，可启动监听

---

## Phase 2: 核心消息模型（Topic + Queue）

- [ ] **Task 2.1**: 实现 Topic Manager
  - 在 `src/topic_manager.py` 中实现 `TopicManager` 类
  - 实现 `create_topic()`, `delete_topic()`, `list_topics()`
  - 实现名称验证（正则 `^[a-zA-Z0-9_-]{1,256}$`）
  - 实现 Topic 数量上限检查（默认 1000）
  - 实现订阅者管理: `subscribe()`, `unsubscribe()`, `get_subscribers()`
  - 线程安全（使用 `threading.Lock()`）
  - **产出**: `src/topic_manager.py` + 单元测试（覆盖正常+异常场景）

- [ ] **Task 2.2**: 实现 Queue Manager
  - 在 `src/queue_manager.py` 中实现 `QueueManager` 类
  - 实现 `create_queue()`, `delete_queue()`, `list_queues()`
  - 实现 `enqueue()` 和 `dequeue()`（FIFO，使用 `collections.deque`）
  - 实现容量控制和满队列检查
  - 实现消息体大小校验（≤ 1MB）
  - 线程安全
  - **产出**: `src/queue_manager.py` + 单元测试

- [ ] **Task 2.3**: 实现 Message Router（统一路由层）
  - 在 `src/message_router.py` 中实现 `MessageRouter` 类
  - 实现 `route_to_topic()` 和 `route_to_queue()`
  - 实现消息状态管理（PENDING → DELIVERED → ACKED）
  - 集成 TopicManager 和 QueueManager
  - 实现 Long Polling 阻塞等待（`consume_from_queue` 带 timeout）
  - **产出**: `src/message_router.py` + 集成测试

- [ ] **Task 2.4**: 集成命令处理（Topic/Queue CRUD + 生产消费）
  - 在 `src/server.py` 中实现所有 Topic/Queue 管理命令的 handler
  - 实现 `PUBLISH` 和 `SEND` 命令处理
  - 实现 `CONSUME` 命令处理（含 `TIMEOUT` 参数）
  - 实现 `SUBSCRIBE` 和 `UNSUBSCRIBE` 命令处理
  - 端到端可运行：创建 Topic → 发布 → 订阅 → 消费
  - **产出**: 完整的命令处理逻辑，可用 telnet 手动测试

---

## Phase 3: 持久化存储

- [ ] **Task 3.1**: 实现消息序列化/反序列化
  - 在 `src/persistence.py` 中实现二进制序列化格式
  - `serialize(message: Message) -> bytes`
  - `deserialize(data: bytes) -> Message`
  - 处理空 body、特殊字符、二进制数据
  - **产出**: 序列化/反序列化函数 + 单元测试

- [ ] **Task 3.2**: 实现 Segment Manager（分段日志）
  - 实现 `SegmentManager` 类
  - 段文件命名规范: `mq-0001.log` 格式
  - 自动分段（段大小 64MB）
  - 段文件数量上限检查（默认 100）
  - 旧段清理（已全部确认的段可删除）
  - **产出**: `SegmentManager` + 单元测试

- [ ] **Task 3.3**: 实现 Append-Only Log 写入 + fsync
  - 实现日志追加写入（`append()`）
  - 实现 `fsync()` 刷盘
  - 支持 `fsync_interval`（定时刷盘）和 `fsync_on_write`（立即刷盘）两种模式
  - 并发写入串行化（文件锁）
  - 磁盘空间检查（< 10MB 阈值拒绝写入）
  - **产出**: 持久化写入功能 + 单元测试

- [ ] **Task 3.4**: 实现 Index Manager
  - 在 `src/persistence.py` 中实现 `IndexManager` 类
  - 内存索引: `dict[str, tuple[str, int]]`（msg_id → (segment, offset)）
  - 索引持久化到 `.idx` 文件
  - 索引恢复（从磁盘加载）
  - 内存 LRU 淘汰（可选，简化版可省略）
  - **产出**: 索引管理 + 单元测试

- [ ] **Task 3.5**: 实现崩溃恢复
  - 实现 `recover()` 扫描所有日志段，重建索引
  - 处理空数据目录首次启动
  - 处理损坏日志段（截断最后不完整消息）
  - 处理损坏索引文件（从日志段重建）
  - 恢复未确认消息为 PENDING 状态
  - **产出**: 恢复逻辑 + 模拟崩溃测试

- [ ] **Task 3.6**: 集成持久化到 MessageRouter
  - 所有消息写入前先持久化
  - 读取消息时通过索引定位
  - 消息确认后标记为可清理
  - **产出**: 端到端持久化验证

---

## Phase 4: 消费者组与 ACK

- [ ] **Task 4.1**: 实现 Consumer Group Manager
  - 在 `src/consumer_group.py` 中实现 `ConsumerGroupManager` 类
  - 实现组创建/删除: `create_group()`, `delete_group()`
  - 实现成员管理: `join_group()`, `leave_group()`
  - 实现 Round-Robin 消费者选择
  - 处理边界：单消费者、消息少于消费者数
  - **产出**: `src/consumer_group.py` + 单元测试

- [ ] **Task 4.2**: 实现心跳协议
  - 服务端: 处理 `PING` → 返回 `PONG`，刷新心跳时间
  - 后台线程每 5s 扫描心跳超时（30s 无心跳）
  - 超时消费者自动从组中移除，消息重分配
  - 消费者优雅退出: `LEAVE_GROUP` 命令
  - **产出**: 心跳机制 + 故障检测测试

- [ ] **Task 4.3**: 实现 ACK / NACK 管理
  - 在 `src/ack_manager.py` 中实现 `AckManager` 类
  - 实现 `ack()`, `nack()`, `ack_batch()`
  - ACK 幂等性（重复 ACK 安全）
  - NACK 立即重投，重试次数 +1
  - 所有权校验（只能 ACK 自己的消息）
  - **产出**: `src/ack_manager.py` + 单元测试

- [ ] **Task 4.4**: 实现超时重投 + 最大重试
  - 后台线程每 5s 扫描超时消息（默认 30s 超时）
  - 超时消息重新标记为 PENDING
  - 达到 `max_retries`（默认 5）的消息移入 DLQ
  - 支持按消息级别配置 `ack_timeout`
  - **产出**: 超时重投逻辑 + 单元测试

- [ ] **Task 4.5**: 实现 DLQ 死信队列 + TTL
  - DLQ 作为特殊 Queue（名称固定为 "DLQ"）
  - 移入 DLQ 时保留原始 target、重试次数、失败原因
  - 实现 `REPLAY DLQ <msg_id>` 单条重放
  - 实现 `REPLAY_ALL DLQ <target>` 批量重放
  - 实现 TTL 过期检查（后台线程每 5s 扫描）
  - TTL 边界处理：TTL=0 拒绝，超大值限制
  - **产出**: DLQ + TTL 完整功能 + 测试

---

## Phase 5: CLI 客户端 + 端到端测试

- [ ] **Task 5.1**: 实现 CLI 客户端
  - 在 `client.py` 中实现命令行客户端
  - 支持交互模式: `python client.py`（REPL 风格）
  - 支持单命令模式: `python client.py publish orders '{...}'`
  - 实现连接管理、命令发送、响应解析
  - **产出**: `client.py` 可独立运行

- [ ] **Task 5.2**: 编写集成测试（端到端场景）
  - 测试 1: 创建 Topic → 发布 → 订阅 → 消费 → ACK
  - 测试 2: Queue 的 FIFO 顺序验证
  - 测试 3: 消费者组 Round-Robin 分配
  - 测试 4: 消费者断线 → 消息重分配
  - 测试 5: 消息超时重投 → DLQ
  - 测试 6: 服务器重启 → 消息恢复
  - **产出**: `tests/test_integration.py`

- [ ] **Task 5.3**: 编写单元测试（覆盖核心模块）
  - `tests/test_protocol.py` - 协议解析
  - `tests/test_topic_manager.py` - Topic 管理
  - `tests/test_queue_manager.py` - Queue 管理
  - `tests/test_persistence.py` - 持久化
  - `tests/test_consumer_group.py` - 消费者组
  - `tests/test_ack_manager.py` - ACK 管理
  - 目标: 测试通过率 ≥ 80%
  - **产出**: 完整测试套件

---

## Phase 6: 文档与收尾

- [ ] **Task 6.1**: 完善 README.md
  - 项目简介、技术栈
  - 快速开始指南（安装、启动、基本操作）
  - 命令参考手册
  - 架构简要说明
  - 测试运行方式
  - **产出**: 完整的 README.md

- [ ] **Task 6.2**: 代码审查与清理
  - 统一代码风格（PEP 8）
  - 移除调试代码和 print 语句
  - 确保错误处理覆盖所有路径
  - 验证所有 Specs 场景可复现
  - **产出**: 整洁的最终代码

- [ ] **Task 6.3**: 准备 Demo 演示
  - 准备 3 分钟项目介绍
  - 准备 5 分钟功能演示脚本
  - 准备 2 分钟 SDD 过程回顾（AI 使用心得、挑战）
  - 截图测试结果
  - **产出**: 演示材料和运行截图

---

## 任务统计

| Phase | 任务数 | 预计工时 |
|-------|--------|---------|
| Phase 1: 基础架构 | 4 | 3-4h |
| Phase 2: 核心消息模型 | 4 | 4-5h |
| Phase 3: 持久化存储 | 6 | 5-6h |
| Phase 4: 消费者组与 ACK | 5 | 4-5h |
| Phase 5: CLI + 测试 | 3 | 3-4h |
| Phase 6: 文档与收尾 | 3 | 2h |
| **合计** | **25** | **21-26h** |
