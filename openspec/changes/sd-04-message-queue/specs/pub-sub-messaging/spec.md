# Spec: 发布-订阅模式

## Overview

本规格定义基于 Topic 的发布-订阅（Pub/Sub）模式，支持多个生产者向同一 Topic 发布消息，多个消费者订阅同一 Topic 并各自独立接收消息。

---

### Requirement: Topic 订阅
The system SHALL 允许消费者订阅 Topic 以接收消息，并处理各种边界情况

#### Scenario: 成功订阅 Topic
- GIVEN Topic "orders" 已创建
- AND 消费者 C1 已连接
- WHEN C1 发送 `SUBSCRIBE orders`
- THEN 服务器返回 `OK`
- AND C1 被加入 Topic "orders" 的订阅者列表

#### Scenario: 订阅不存在的 Topic
- GIVEN Topic "nonexistent" 不存在
- WHEN 消费者发送 `SUBSCRIBE nonexistent`
- THEN 服务器返回错误: `ERR_TOPIC_NOT_FOUND`

#### Scenario: 重复订阅同一 Topic
- GIVEN 消费者 C1 已订阅 Topic "orders"
- WHEN C1 再次发送 `SUBSCRIBE orders`
- THEN 服务器返回 `OK`（幂等操作）
- AND 订阅者列表中 C1 仅出现一次

#### Scenario: 取消订阅
- GIVEN 消费者 C1 已订阅 Topic "orders"
- WHEN C1 发送 `UNSUBSCRIBE orders`
- THEN 服务器返回 `OK`
- AND C1 从 Topic "orders" 的订阅者列表中移除

#### Scenario: 取消未订阅的 Topic
- GIVEN 消费者 C1 未订阅 Topic "orders"
- WHEN C1 发送 `UNSUBSCRIBE orders`
- THEN 服务器返回错误: `ERR_NOT_SUBSCRIBED`

#### Scenario: 消费者断线自动取消订阅
- GIVEN 消费者 C1 订阅了 Topic "orders"
- WHEN C1 的 TCP 连接断开（无显式 UNSUBSCRIBE）
- THEN 服务器自动将 C1 从 "orders" 的订阅者列表移除
- AND C1 的消费位置保留，重连后可继续

---

### Requirement: 消息广播
The system SHALL 将发布到 Topic 的消息广播给所有当前订阅者，慢消费者不影响其他消费者

#### Scenario: 广播给多个消费者
- GIVEN Topic "orders" 有订阅者 C1, C2, C3
- WHEN 生产者向 "orders" 发布消息 M1: `{"order": "new"}`
- THEN C1, C2, C3 各自收到消息 M1 的副本
- AND 每个消费者的消费进度独立追踪

#### Scenario: 慢消费者不影响快消费者
- GIVEN Topic "orders" 有订阅者 C1（快）和 C2（慢）
- AND C2 处理消息很慢，积压大量消息
- WHEN 生产者发布新消息 M10
- THEN C1 立即收到 M10
- AND C2 的消费速度不影响 C1 的消息投递

#### Scenario: 无订阅者时发布消息
- GIVEN Topic "orders" 当前无订阅者
- WHEN 生产者向 "orders" 发布消息 M1
- THEN 消息 M1 被持久化存储
- AND 当新订阅者上线后，可消费历史消息（取决于消费位置）

#### Scenario: 消费者离线后消息不丢失
- GIVEN 消费者 C1 订阅了 Topic "orders"
- AND C1 断开连接
- WHEN 生产者向 "orders" 发布消息 M5, M6, M7
- THEN 消息被持久化
- AND C1 重新连接并订阅后，可从上次消费位置继续消费 M5, M6, M7

#### Scenario: 广播投递部分失败
- GIVEN Topic "orders" 有订阅者 C1, C2
- AND C1 的网络不稳定
- WHEN 生产者发布消息 M1
- THEN M1 被持久化存储
- AND C2 成功收到 M1
- AND C1 的投递失败不影响 C2
- AND C1 恢复后可继续从上次 offset 消费

---

### Requirement: 消费位置管理
The system SHALL 为每个订阅者独立维护消费进度（offset），并持久化 offset 以防丢失

#### Scenario: 独立消费位置
- GIVEN Topic "orders" 有 10 条消息 (offset 0-9)
- AND 消费者 C1 已消费到 offset 5
- AND 消费者 C2 已消费到 offset 2
- WHEN C1 消费下一条
- THEN C1 收到 offset 6 的消息
- AND C2 的 offset 保持为 2，不受影响

#### Scenario: 重置消费位置
- GIVEN 消费者 C1 订阅 Topic "orders"，当前 offset=50
- WHEN C1 发送 `SET_OFFSET orders 0`
- THEN 服务器返回 `OK`
- AND C1 的消费位置重置为 0（可从最早消息重新消费）

#### Scenario: Offset 设为负数
- GIVEN 消费者 C1 订阅 Topic "orders"，当前 offset=10
- WHEN C1 发送 `SET_OFFSET orders -1`
- THEN 服务器返回错误: `ERR_INVALID_OFFSET`
- AND 提示: "Offset must not be negative"

#### Scenario: Offset 超出范围
- GIVEN Topic "orders" 当前最大 offset 为 99
- WHEN C1 发送 `SET_OFFSET orders 150`
- THEN 服务器返回 `OK`
- AND C1 的 offset 设为 150
- AND 下次消费时，C1 只收到 offset >= 150 的新消息（如有）

#### Scenario: Offset 持久化
- GIVEN 消费者 C1 的当前 offset=42
- AND 服务器正常关闭后重启
- WHEN C1 重新连接并订阅 Topic "orders"
- THEN C1 的 offset 从 42 恢复
- AND 继续从 offset 43 开始消费
