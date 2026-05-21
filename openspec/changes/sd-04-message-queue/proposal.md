# Proposal: 轻量级消息队列实现

## Why

现代分布式系统中，服务间异步通信是核心需求。现有成熟消息队列（如Kafka、RabbitMQ）虽然功能强大，但部署运维复杂，学习曲线陡峭。本课题通过从零实现一个轻量级消息队列，深入理解消息队列的核心原理——包括Topic/Queue模型、持久化存储、发布订阅、消费者组和消息确认机制，为后续使用和调优生产级消息中间件打下坚实基础。

## What Changes

- 实现基于 TCP 的自定义协议消息队列服务器
- 支持 Topic（发布订阅）和 Queue（点对点）两种消息模型
- 实现消息的磁盘持久化存储，支持崩溃恢复
- 实现发布-订阅模式，支持多个消费者订阅同一 Topic
- 实现消费者组机制，同一组内消费者负载均衡消费
- 实现消息确认（ACK）机制，确保消息可靠投递
- 提供 CLI 客户端工具用于生产/消费消息

## Capabilities

### New Capabilities

- `topic-queue-model`: Topic 发布订阅与 Queue 点对点两种消息模型的核心实现，包括消息路由、Topic/Queue 的创建与管理
- `message-persistence`: 消息的磁盘持久化存储，包括顺序写入、分段日志、索引管理和崩溃恢复（WAL 日志）
- `pub-sub-messaging`: 基于 Topic 的发布-订阅模式，支持多生产者多消费者，消息广播给所有订阅者
- `consumer-group`: 消费者组机制，同一组内消费者以轮询方式负载均衡消费，不同组独立消费
- `message-acknowledgment`: 消息确认与重试机制，消费者显式确认后标记消息为已消费，未确认消息支持超时重投

### Modified Capabilities

<!-- 首次实现，无已有能力变更 -->
无

## Impact

- **代码**: 全新项目，Python 实现，预计 1500-2000 行代码
- **依赖**: Python 标准库（socket, threading, json, os），无需外部依赖
- **协议**: 自定义 TCP 文本协议（类 Redis RESP 协议风格）
- **存储**: 本地文件系统（append-only log + index files）
- **运行**: 单机部署，纯软件实现，无需 Docker 或外部服务
