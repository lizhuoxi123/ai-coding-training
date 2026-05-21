# Spec: 消息确认与重试机制

## Overview

本规格定义消息确认（ACK）机制，消费者显式确认后才标记消息为已消费，未确认消息在超时后重新投递，确保消息可靠投递。

---

### Requirement: 消息确认（ACK）
The system SHALL 支持消费者对已消费消息进行显式确认（ACK）和否定确认（NACK）

#### Scenario: 成功确认消息
- GIVEN 消费者 C1 已收到消息 M1（状态为"待确认"）
- WHEN C1 发送 `ACK M1`
- THEN 服务器返回 `OK`
- AND 消息 M1 标记为"已确认"
- AND M1 不再投递给任何消费者

#### Scenario: 重复确认同一消息
- GIVEN 消息 M1 已被 C1 确认
- WHEN C1 再次发送 `ACK M1`
- THEN 服务器返回 `OK`（幂等操作）
- AND 消息状态保持"已确认"

#### Scenario: 否定确认（NACK）
- GIVEN 消费者 C1 收到消息 M1 但处理失败（业务逻辑错误）
- WHEN C1 发送 `NACK M1`（表示无法处理）
- THEN 服务器返回 `OK`
- AND M1 立即被重新标记为"可投递"
- AND M1 的重试次数 +1
- AND M1 可由其他消费者重新消费

#### Scenario: 确认不属于自己的消息
- GIVEN 消息 M1 被分配给消费者 C1（待确认状态）
- WHEN 消费者 C2 发送 `ACK M1`
- THEN 服务器返回错误: `ERR_NOT_YOUR_MESSAGE`
- AND M1 状态不变

#### Scenario: 确认不存在的消息
- GIVEN 消息 ID "M99999" 不存在
- WHEN 消费者发送 `ACK M99999`
- THEN 服务器返回错误: `ERR_MESSAGE_NOT_FOUND`

#### Scenario: 批量确认
- GIVEN 消费者 C1 收到消息 M1, M2, M3
- WHEN C1 发送 `ACK_BATCH M1 M2 M3`
- THEN 服务器返回 `OK`
- AND M1, M2, M3 全部标记为"已确认"
- AND 如果其中某条消息不属于 C1，仅该消息确认失败，不影响其余消息

---

### Requirement: 消息超时重投
The system SHALL 在消息未确认超时后自动重新投递，支持可配置的超时时间

#### Scenario: 超时重投
- GIVEN 消费者 C1 收到消息 M5，超时时间设为 30 秒
- AND C1 在 30 秒内未发送 ACK
- WHEN 30 秒超时到达
- THEN M5 被重新标记为"可投递"
- AND 该消息可被其他消费者（或 C1）重新消费

#### Scenario: ACK 在超时前到达
- GIVEN 消费者 C1 收到消息 M5
- WHEN C1 在 25 秒时发送 `ACK M5`（未超时）
- THEN M5 被正常确认
- AND 不触发重投

#### Scenario: 按消息级别配置超时
- GIVEN 生产者发送消息 M1 时指定 `ack_timeout=60` 秒
- AND 默认超时为 30 秒
- WHEN 消费者拉取 M1
- THEN M1 的超时时间为 60 秒
- AND 其余消息使用默认 30 秒超时

---

### Requirement: 最大重投次数
The system SHALL 限制消息的最大重投次数，防止无限循环

#### Scenario: 未超过最大重投次数
- GIVEN 消息 M5 的重投次数为 2，最大重投次数为 5
- WHEN M5 超时重投
- THEN M5 重新标记为"可投递"
- AND 重投次数递增为 3

#### Scenario: 达到最大重投次数
- GIVEN 消息 M5 的重投次数为 5，最大重投次数为 5
- WHEN M5 再次超时未确认
- THEN M5 被移入死信队列（Dead Letter Queue, DLQ）
- AND 服务器日志记录: "Message M5 moved to DLQ after 5 retries"

---

### Requirement: 死信队列（DLQ）
The system SHALL 将无法成功消费的消息移入死信队列，并支持 DLQ 消息的重放

#### Scenario: 消息进入 DLQ
- GIVEN 消息 M5 超过最大重投次数
- WHEN M5 被移入 DLQ
- THEN M5 存储在特殊 Queue "DLQ" 中
- AND 保留原始消息内容、原始 Queue/Topic、失败原因

#### Scenario: 查看 DLQ 消息
- GIVEN DLQ 中有消息 M5
- WHEN 管理员发送 `CONSUME DLQ`
- THEN 返回 M5 的完整信息（含原始 Queue、重试次数、时间戳）

#### Scenario: DLQ 消息重放
- GIVEN DLQ 中有消息 M5
- WHEN 管理员发送 `REPLAY DLQ M5`
- THEN M5 被重新投递到其原始 Queue/Topic
- AND 重试次数重置为 0
- AND M5 从 DLQ 中移除

#### Scenario: DLQ 批量重放
- GIVEN DLQ 中有消息 M1, M2, M3，原始 Queue 均为 "tasks"
- WHEN 管理员发送 `REPLAY_ALL DLQ tasks`
- THEN M1, M2, M3 全部重新投递到 "tasks"
- AND DLQ 被清空

---

### Requirement: 消息过期
The system SHALL 支持消息级别的 TTL（Time To Live），过期消息自动清理，处理边界 TTL 值

#### Scenario: 消息在 TTL 内被消费
- GIVEN 生产者发送消息 M1 时指定 TTL=3600 秒
- WHEN M1 在 1800 秒时被消费并确认
- THEN 正常流程，消息被确认

#### Scenario: 消息超过 TTL
- GIVEN 消息 M2 的 TTL=60 秒
- AND M2 在 60 秒内未被任何消费者拉取
- WHEN 60 秒到达
- THEN M2 被自动标记为过期
- AND 从可投递队列中移除
- AND 可配置为移入 DLQ 或直接丢弃

#### Scenario: TTL=0（立即过期）
- GIVEN 生产者发送消息时指定 TTL=0
- WHEN 消息被写入
- THEN 服务器返回错误: `ERR_INVALID_TTL`
- AND 提示: "TTL must be greater than 0"

#### Scenario: 无 TTL（永不过期）
- GIVEN 生产者发送消息时未指定 TTL
- WHEN 消息写入成功
- THEN 消息默认 TTL 为服务器配置值（如 86400 秒=24小时）
- AND 可按服务器默认 TTL 处理过期

#### Scenario: 超大 TTL 值
- GIVEN 服务器限制最大 TTL 为 604800 秒（7天）
- WHEN 生产者发送消息指定 TTL=9999999 秒
- THEN 服务器返回错误: `ERR_TTL_TOO_LARGE`
- AND 提示最大允许的 TTL 值
