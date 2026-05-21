# Spec: Topic/Queue 消息模型

## Overview

本规格定义消息队列中 Topic（发布订阅）和 Queue（点对点）两种消息模型的核心行为，包括 Topic/Queue 的创建、消息路由和基本生产消费流程。

---

### Requirement: Topic 创建与管理
The system SHALL 支持创建、查询和删除 Topic

#### Scenario: 成功创建 Topic
- GIVEN 消息队列服务器已启动
- WHEN 客户端发送创建 Topic 命令: `CREATE_TOPIC orders`
- THEN 服务器返回成功响应: `OK`
- AND Topic "orders" 被创建，可接受消息发布

#### Scenario: 重复创建同名 Topic
- GIVEN Topic "orders" 已存在
- WHEN 客户端再次发送 `CREATE_TOPIC orders`
- THEN 服务器返回错误: `ERR_TOPIC_EXISTS`

#### Scenario: 查询 Topic 列表
- GIVEN 系统中存在 Topic "orders" 和 "logs"
- WHEN 客户端发送 `LIST_TOPICS`
- THEN 服务器返回包含 "orders" 和 "logs" 的列表

#### Scenario: 删除 Topic
- GIVEN Topic "orders" 已创建且无订阅者
- WHEN 客户端发送 `DELETE_TOPIC orders`
- THEN 服务器返回 `OK`
- AND Topic "orders" 被删除

#### Scenario: 删除有订阅者的 Topic
- GIVEN Topic "orders" 有活跃订阅者 C1
- WHEN 客户端发送 `DELETE_TOPIC orders`
- THEN 服务器返回错误: `ERR_TOPIC_HAS_SUBSCRIBERS`
- AND Topic "orders" 保留

#### Scenario: 非法 Topic 名称（空名称）
- GIVEN 消息队列服务器已启动
- WHEN 客户端发送 `CREATE_TOPIC ""`
- THEN 服务器返回错误: `ERR_INVALID_NAME`
- AND 提示: "Topic name must not be empty"

#### Scenario: 非法 Topic 名称（特殊字符）
- GIVEN 消息队列服务器已启动
- WHEN 客户端发送 `CREATE_TOPIC "order$test#123"` 包含特殊字符
- THEN 服务器返回错误: `ERR_INVALID_NAME`
- AND 提示: "Topic name must match [a-zA-Z0-9_-]+"

#### Scenario: Topic 名称超长
- GIVEN 消息队列服务器已启动
- WHEN 客户端发送 `CREATE_TOPIC` 及一个超过 256 字符的名称
- THEN 服务器返回错误: `ERR_NAME_TOO_LONG`
- AND 提示最大允许长度

#### Scenario: 系统 Topic 数量上限
- GIVEN 服务器配置最大 Topic 数量为 1000
- AND 已创建 1000 个 Topic
- WHEN 客户端尝试创建第 1001 个 Topic
- THEN 服务器返回错误: `ERR_MAX_TOPICS_REACHED`

---

### Requirement: Queue 创建与管理
The system SHALL 支持创建、查询和删除 Queue，并验证名称合法性

#### Scenario: 成功创建 Queue
- GIVEN 消息队列服务器已启动
- WHEN 客户端发送创建 Queue 命令: `CREATE_QUEUE tasks`
- THEN 服务器返回成功响应: `OK`
- AND Queue "tasks" 被创建

#### Scenario: 重复创建同名 Queue
- GIVEN Queue "tasks" 已存在
- WHEN 客户端再次发送 `CREATE_QUEUE tasks`
- THEN 服务器返回错误: `ERR_QUEUE_EXISTS`

#### Scenario: 查询 Queue 列表
- GIVEN 系统中存在 Queue "tasks" 和 "emails"
- WHEN 客户端发送 `LIST_QUEUES`
- THEN 服务器返回包含 "tasks" 和 "emails" 的列表

#### Scenario: 成功删除空 Queue
- GIVEN Queue "tasks" 已创建且无待消费消息
- WHEN 客户端发送 `DELETE_QUEUE tasks`
- THEN 服务器返回 `OK`
- AND Queue "tasks" 被删除

#### Scenario: 删除有未消费消息的 Queue
- GIVEN Queue "tasks" 中有 5 条未消费消息
- WHEN 客户端发送 `DELETE_QUEUE tasks`
- THEN 服务器返回错误: `ERR_QUEUE_NOT_EMPTY`
- AND 提示未消费消息数量

#### Scenario: 删除不存在的 Queue
- GIVEN Queue "tasks" 不存在
- WHEN 客户端发送 `DELETE_QUEUE tasks`
- THEN 服务器返回错误: `ERR_QUEUE_NOT_FOUND`

#### Scenario: 非法 Queue 名称
- GIVEN 消息队列服务器已启动
- WHEN 客户端发送 `CREATE_QUEUE ""` 或含特殊字符的名称
- THEN 服务器返回错误: `ERR_INVALID_NAME`
- AND Queue 名称规则与 Topic 相同: `[a-zA-Z0-9_-]+`，长度 1-256

---

### Requirement: 消息发布到 Topic
The system SHALL 支持向 Topic 发布消息，并验证消息合法性

#### Scenario: 成功发布消息
- GIVEN Topic "orders" 已创建
- WHEN 生产者发送 `PUBLISH orders {"order_id": 123, "amount": 99.9}`
- THEN 服务器返回 `OK` 及消息 ID
- AND 消息被持久化存储

#### Scenario: 向不存在的 Topic 发布消息
- GIVEN Topic "nonexistent" 不存在
- WHEN 生产者发送 `PUBLISH nonexistent {"data": "test"}`
- THEN 服务器返回错误: `ERR_TOPIC_NOT_FOUND`

#### Scenario: 发布空消息体
- GIVEN Topic "orders" 已创建
- WHEN 生产者发送 `PUBLISH orders ""`（空消息体）
- THEN 服务器返回错误: `ERR_EMPTY_MESSAGE`
- AND 提示: "Message body must not be empty"

#### Scenario: 发布超大消息
- GIVEN 服务器配置最大消息大小为 1MB
- WHEN 生产者发送消息体超过 1MB
- THEN 服务器返回错误: `ERR_MESSAGE_TOO_LARGE`
- AND 提示: "Maximum message size is 1048576 bytes"

#### Scenario: 并发发布消息
- GIVEN Topic "orders" 已创建
- AND 生产者 P1 和 P2 同时分别发布消息 MA 和 MB
- WHEN 两条 PUBLISH 命令几乎同时到达
- THEN 两条消息都成功写入
- AND 消息 ID 唯一且不同
- AND 消息顺序与写入日志的顺序一致

---

### Requirement: 消息发送到 Queue
The system SHALL 支持向 Queue 发送消息（点对点模式），并验证消息合法性

#### Scenario: 成功发送消息到 Queue
- GIVEN Queue "tasks" 已创建
- WHEN 生产者发送 `SEND tasks {"task": "process_image"}`
- THEN 服务器返回 `OK` 及消息 ID
- AND 消息被添加到 Queue "tasks"

#### Scenario: 向不存在的 Queue 发送消息
- GIVEN Queue "ghost" 不存在
- WHEN 生产者发送 `SEND ghost {"task": "test"}`
- THEN 服务器返回错误: `ERR_QUEUE_NOT_FOUND`

#### Scenario: 向已满的 Queue 发送消息
- GIVEN Queue "tasks" 已配置最大容量为 10000
- AND Queue 中已有 10000 条未消费消息
- WHEN 生产者发送 `SEND tasks {"task": "overflow"}`
- THEN 服务器返回错误: `ERR_QUEUE_FULL`

#### Scenario: 发送空消息体到 Queue
- GIVEN Queue "tasks" 已创建
- WHEN 生产者发送 `SEND tasks ""`（空消息体）
- THEN 服务器返回错误: `ERR_EMPTY_MESSAGE`

#### Scenario: 发送超大消息到 Queue
- GIVEN 服务器配置最大消息大小为 1MB
- WHEN 生产者发送消息体超过 1MB
- THEN 服务器返回错误: `ERR_MESSAGE_TOO_LARGE`

---

### Requirement: 从 Queue 消费消息
The system SHALL 支持从 Queue 拉取消息（FIFO 顺序），支持阻塞等待模式

#### Scenario: 成功消费消息（FIFO）
- GIVEN Queue "tasks" 中有消息 M1: `{"task": "A"}`（先入）和 M2: `{"task": "B"}`（后入）
- WHEN 消费者发送 `CONSUME tasks`
- THEN 服务器返回消息 M1 及其消息 ID（先入先出）
- AND M1 从 Queue 中标记为"待确认"状态

#### Scenario: 连续消费验证 FIFO
- GIVEN Queue "tasks" 中有消息 M1, M2, M3（按此顺序入队）
- WHEN 消费者连续 3 次执行 `CONSUME tasks`
- THEN 依次返回 M1, M2, M3

#### Scenario: 消费空 Queue（非阻塞模式）
- GIVEN Queue "tasks" 为空
- WHEN 消费者发送 `CONSUME tasks`
- THEN 服务器返回 `NULL`（表示无消息）

#### Scenario: 阻塞等待消费（Long Polling）
- GIVEN Queue "tasks" 当前为空
- WHEN 消费者发送 `CONSUME tasks TIMEOUT 5000`（超时 5 秒）
- AND 在 3 秒后生产者向 "tasks" 发送消息 M1
- THEN 消费者在 3 秒后收到 M1
- AND 不等待完整的 5 秒超时

#### Scenario: 阻塞等待超时
- GIVEN Queue "tasks" 当前为空
- WHEN 消费者发送 `CONSUME tasks TIMEOUT 2000`（超时 2 秒）
- AND 2 秒内无消息到达
- THEN 服务器返回 `NULL`（超时）
- AND 连接保持正常

#### Scenario: 消费不存在的 Queue
- GIVEN Queue "ghost" 不存在
- WHEN 消费者发送 `CONSUME ghost`
- THEN 服务器返回错误: `ERR_QUEUE_NOT_FOUND`
