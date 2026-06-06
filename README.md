# 轻量级消息队列 (Lightweight Message Queue)

基于 Python 实现的轻量级消息队列系统。

## 技术栈
- Python 3.9+
- 纯标准库实现，零外部依赖

## 项目结构
```
src/
├── server.py          # TCP 服务器
├── protocol.py        # 协议解析
├── models.py          # 数据模型
├── topic_manager.py   # Topic 管理
├── queue_manager.py   # Queue 管理
├── message_router.py  # 消息路由
├── persistence.py     # 持久化存储
├── consumer_group.py  # 消费者组
├── ack_manager.py     # ACK 管理
client.py              # CLI 客户端
tests/                 # 测试
data/                  # 数据目录
```

## 快速开始

### 启动服务器
```bash
python src/main.py
```

### 使用 CLI 客户端
```bash
# 交互模式
python client.py

# 单命令模式
python client.py CREATE_TOPIC orders
python client.py PUBLISH orders '{"order_id": 123}'
python client.py SUBSCRIBE orders
```

## 命令参考
详见 `design.md` 中的协议规范。

## 运行测试
```bash
# 协议单元测试（纯函数，无需服务器）
python tests/test_protocol.py

# 集成测试（自动启动/关闭服务器，TCP 通信）
python tests/test_integration.py
```
