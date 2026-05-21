# Spec: 消费者组（负载均衡）

## Overview

消费者组机制允许将多个消费者归入同一组内，组内以轮询（Round-Robin）方式负载均衡消费消息；不同消费者组之间独立消费，互不影响。

---

### Requirement: 消费者组创建与管理
The system SHALL 支持创建消费者组并将其绑定到 Queue 或 Topic，并处理绑定异常

#### Scenario: 创建消费者组
- GIVEN Queue "tasks" 已创建
- WHEN 客户端发送 `CREATE_GROUP tasks workers`
- THEN 服务器返回 `OK`
- AND 消费者组 "workers" 绑定到 Queue "tasks"

#### Scenario: 创建同名消费者组
- GIVEN 消费者组 "workers" 已存在
- WHEN 客户端发送 `CREATE_GROUP tasks workers`
- THEN 服务器返回错误: `ERR_GROUP_EXISTS`

#### Scenario: 绑定到不存在的 Queue/Topic
- GIVEN Queue 或 Topic "ghost" 不存在
- WHEN 客户端发送 `CREATE_GROUP ghost workers`
- THEN 服务器返回错误: `ERR_QUEUE_NOT_FOUND` 或 `ERR_TOPIC_NOT_FOUND`

#### Scenario: 非法的消费者组名称
- GIVEN 消息队列服务器已启动
- WHEN 客户端发送 `CREATE_GROUP tasks ""` 或含特殊字符的组名
- THEN 服务器返回错误: `ERR_INVALID_NAME`
- AND 组名规则与 Topic/Queue 相同

#### Scenario: 删除消费者组
- GIVEN 消费者组 "workers" 存在且无活跃消费者
- WHEN 客户端发送 `DELETE_GROUP workers`
- THEN 服务器返回 `OK`
- AND 消费者组 "workers" 被删除

#### Scenario: 删除有活跃消费者的组
- GIVEN 消费者组 "workers" 有活跃消费者 C1
- WHEN 客户端发送 `DELETE_GROUP workers`
- THEN 服务器返回错误: `ERR_GROUP_HAS_CONSUMERS`
- AND 消费者组 "workers" 保留

---

### Requirement: 消费者加入组
The system SHALL 允许消费者以消费者组名义消费消息，支持加入多个组

#### Scenario: 消费者以组名消费
- GIVEN 消费者组 "workers" 绑定到 Queue "tasks"
- AND 消费者 C1 已连接
- WHEN C1 发送 `CONSUME_GROUP tasks workers`
- THEN C1 被注册为 "workers" 组的成员
- AND 返回一条 Queue 中的消息

#### Scenario: 加入不存在的消费者组
- GIVEN 消费者组 "ghost" 不存在
- WHEN C1 发送 `CONSUME_GROUP tasks ghost`
- THEN 服务器返回错误: `ERR_GROUP_NOT_FOUND`

#### Scenario: 同一消费者加入多个组
- GIVEN 消费者 C1 已是 "group-A" 的成员
- WHEN C1 发送 `CONSUME_GROUP logs group-B`
- THEN 服务器返回 `OK`
- AND C1 同时是 "group-A" 和 "group-B" 的成员
- AND 两个组的消费进度独立维护

#### Scenario: 消费者离开组
- GIVEN 消费者 C1 是 "workers" 组成员
- WHEN C1 断开连接
- THEN C1 从 "workers" 组中移除
- AND C1 未确认的消息重新分配给组内其他消费者

---

### Requirement: Round-Robin 负载均衡
The system SHALL 在消费者组内以轮询方式分配消息，处理各种消费者数量场景

#### Scenario: 轮询分配消息
- GIVEN Queue "tasks" 有 6 条消息 M1-M6
- AND 消费者组 "workers" 有成员 C1, C2, C3
- WHEN C1, C2, C3 各自连续消费 2 次
- THEN C1 收到 M1, M4
- AND C2 收到 M2, M5
- AND C3 收到 M3, M6

#### Scenario: 单消费者无负载均衡
- GIVEN 消费者组 "workers" 仅有 C1 一个成员
- AND Queue 中有消息 M1-M5
- WHEN C1 连续消费
- THEN C1 收到所有 5 条消息 M1-M5
- AND 分配行为等同于直接消费

#### Scenario: 消息少于消费者数量
- GIVEN 消费者组 "workers" 有 C1, C2, C3 三个成员
- AND Queue 中仅有 2 条消息 M1, M2
- WHEN C1, C2, C3 各消费一次
- THEN C1 收到 M1
- AND C2 收到 M2
- AND C3 收到 `NULL`（无消息）

#### Scenario: 新消费者加入后重新平衡
- GIVEN 消费者组 "workers" 当前仅 C1 在消费
- AND Queue 中有消息 M10-M15
- WHEN 新消费者 C2 加入 "workers" 组并开始消费
- THEN C1 和 C2 按轮询分配后续消息
- AND C1 已收到但未确认的消息不受影响

---

### Requirement: 不同消费者组独立消费
The system SHALL 确保不同消费者组之间消息消费互不影响

#### Scenario: 两组独立消费同一 Queue
- GIVEN Queue "tasks" 有消息 M1-M5
- AND 消费者组 "group-A" 的 C1 已消费 M1-M3
- AND 消费者组 "group-B" 的 C2 新加入
- WHEN C2 开始消费
- THEN C2 从 M1 开始消费（"group-B" 的 offset 从 0 开始）
- AND "group-A" 的消费进度不受影响

---

### Requirement: 组内消费者故障处理
The system SHALL 通过心跳机制检测消费者故障，并将未确认消息重新分配

#### Scenario: 消费者故障后消息重分配
- GIVEN 消费者组 "workers" 有 C1 和 C2
- AND C1 已收到但未确认消息 M5
- WHEN C1 超时未响应（心跳超时 30 秒）
- THEN C1 从 "workers" 组中移除
- AND M5 被重新标记为可投递
- AND C2 的下一次消费可能收到 M5

#### Scenario: 组内最后一个消费者故障
- GIVEN 消费者组 "workers" 仅 C1 在线
- AND C1 已收到但未确认消息 M10
- WHEN C1 断开连接
- THEN M10 被重新标记为可投递
- AND "workers" 组保留，等待新消费者加入

#### Scenario: 心跳协议
- GIVEN 消费者 C1 是 "workers" 组成员
- WHEN C1 每 10 秒发送 `PING`
- THEN 服务器返回 `PONG`
- AND C1 的活跃状态被刷新
- AND C1 不会因心跳超时被移除

#### Scenario: 消费者优雅退出
- GIVEN 消费者 C1 是 "workers" 组成员
- AND C1 持有未确认消息 M5
- WHEN C1 发送 `LEAVE_GROUP workers` 优雅退出
- THEN C1 从组中移除
- AND M5 立即重新分配给组内其他消费者
- AND 无需等待心跳超时
