# Spec: 消息持久化存储

## Overview

本规格定义消息的磁盘持久化存储机制，包括顺序写入（append-only log）、分段日志管理、索引构建和崩溃恢复（WAL），确保消息在服务器重启后不丢失。

---

### Requirement: 消息持久化写入
The system SHALL 将每条消息以追加方式写入磁盘日志文件，并在适当时机刷盘以保证持久性

#### Scenario: 消息成功持久化
- GIVEN 持久化模块已初始化，日志文件路径为 `data/mq.log`
- WHEN 收到一条消息 `{"order_id": 456}`
- THEN 消息被追加写入 `data/mq.log` 文件末尾
- AND 写入包含：消息 ID、Topic/Queue 名称、消息体、时间戳
- AND 返回写入成功

#### Scenario: 磁盘空间不足
- GIVEN 磁盘可用空间小于 10MB 的阈值
- WHEN 尝试写入新消息
- THEN 返回错误: `ERR_DISK_FULL`
- AND 已写入的数据不丢失

#### Scenario: 刷盘策略（fsync）
- GIVEN 持久化模块配置为 `fsync_interval=1000ms`（每秒刷盘）
- WHEN 消息被写入 OS 缓冲区但尚未 fsync
- AND 服务器意外断电
- THEN 最近 1 秒内未 fsync 的消息最多丢失 1 秒的数据
- AND 已 fsync 的消息在重启后完整恢复

#### Scenario: 立即刷盘模式
- GIVEN 持久化模块配置为 `fsync_on_write=true`（每条消息立即刷盘）
- WHEN 消息写入日志文件
- THEN 立即调用 fsync()
- AND 即使立即断电，消息也不丢失
- AND 写入吞吐量低于异步刷盘模式

#### Scenario: 并发写入串行化
- GIVEN 持久化模块接收两条并发写入请求（消息 MA 和 MB）
- WHEN 两个线程同时调用 write()
- THEN 使用文件锁确保串行写入
- AND 两条消息都完整写入，无交错数据

---

### Requirement: 分段日志管理
The system SHALL 将日志文件按大小分段（segment），每段默认最大 64MB，使用统一命名规范

#### Scenario: 日志自动分段
- GIVEN 当前日志段 `mq-0001.log` 已接近 64MB
- WHEN 写入下一条消息导致超过 64MB
- THEN 当前段被关闭，创建新段 `mq-0002.log`
- AND 新消息写入新段

#### Scenario: 段文件命名规范
- GIVEN 存在多个段文件
- WHEN 查看数据目录
- THEN 段文件命名为 `mq-0001.log`, `mq-0002.log`, ... 格式
- AND 编号从 0001 开始，按创建顺序递增
- AND 索引文件对应命名为 `mq-0001.idx`, `mq-0002.idx`

#### Scenario: 旧段清理
- GIVEN 存在多个日志段文件
- WHEN 某段中所有消息均已被确认消费
- THEN 该段文件可被安全删除（或归档）
- AND 保留未完全消费的段文件

#### Scenario: 段文件数量上限
- GIVEN 服务器配置最大段文件数为 100
- AND 当前已存在 100 个未完全消费的段文件
- WHEN 需要创建第 101 个段文件
- THEN 返回错误: `ERR_TOO_MANY_SEGMENTS`
- AND 提示需要等待旧段被清理

---

### Requirement: 消息索引
The system SHALL 维护消息 ID 到物理位置的索引，并将索引持久化到磁盘

#### Scenario: 通过消息 ID 定位
- GIVEN 消息 M100 存储在日志段 `mq-0001.log` 的偏移量 4096 处
- AND 索引已更新
- WHEN 查询消息 M100 的位置
- THEN 返回: 段文件 `mq-0001.log`，偏移量 4096

#### Scenario: 索引持久化
- GIVEN 索引中有 1000 条消息的位置映射
- AND 服务器正常关闭
- WHEN 服务器重新启动
- THEN 从磁盘加载索引文件，恢复所有 1000 条映射
- AND 无需重新扫描日志段文件

#### Scenario: 查询不存在的消息
- GIVEN 索引中不存在消息 ID "M99999"
- WHEN 查询消息 M99999 的位置
- THEN 返回: `NULL`

#### Scenario: 内存索引上限
- GIVEN 服务器配置最大内存索引条目数为 1,000,000
- WHEN 索引条目达到上限
- THEN 最旧的索引条目按 LRU 策略淘汰到磁盘
- AND 查询被淘汰的消息时需要扫描日志段文件

---

### Requirement: 崩溃恢复
The system SHALL 在服务器异常重启后恢复未丢失的消息数据，并处理多种恢复场景

#### Scenario: 正常恢复
- GIVEN 服务器在持久化了 500 条消息后异常崩溃
- AND 重启服务器
- WHEN 持久化模块启动
- THEN 扫描所有日志段文件，重建索引
- AND 恢复所有 500 条消息
- AND 未确认的消息被重新标记为可投递

#### Scenario: 空数据目录首次启动
- GIVEN 数据目录 `data/` 为空（首次启动）
- WHEN 持久化模块启动
- THEN 创建初始日志段 `mq-0001.log`
- AND 创建初始索引文件 `mq-0001.idx`
- AND 无任何错误

#### Scenario: 损坏的日志段恢复
- GIVEN 日志段 `mq-0003.log` 尾部因崩溃而损坏（最后一条消息不完整）
- WHEN 服务器重启恢复
- THEN 丢弃最后一条不完整的消息记录
- AND 其余消息正常恢复
- AND 日志中记录警告: "Truncated message at segment mq-0003.log"

#### Scenario: 索引文件损坏恢复
- GIVEN 索引文件 `mq-0001.idx` 因磁盘故障损坏
- AND 对应的日志段 `mq-0001.log` 完整
- WHEN 服务器重启恢复
- THEN 检测到索引损坏，自动从日志段重建索引
- AND 日志记录: "Index file mq-0001.idx corrupted, rebuilding from log"

---

### Requirement: 消息格式规范
The system SHALL 使用统一的二进制格式存储消息，保证跨平台一致性和特殊字符安全

#### Scenario: 消息序列化格式
- GIVEN 一条消息包含: ID="M001", Target="orders", Body='{"a":1}', Timestamp=1711234567
- WHEN 消息被序列化写入日志
- THEN 格式为: `[4字节总长度][2字节ID长度][消息ID][2字节Target长度][Target][4字节Body长度][Body][8字节时间戳]`
- AND 反序列化后能还原所有字段

#### Scenario: 消息体为空字符串
- GIVEN 一条消息 Body 为空字符串 `""`
- WHEN 消息被序列化
- THEN Body 长度字段为 0
- AND 反序列化后 Body 为空字符串 `""`

#### Scenario: 消息体含特殊字符
- GIVEN 消息 Body 为 `{"text": "hello\nworld\t!"}`（含换行符、制表符）
- WHEN 消息被序列化再反序列化
- THEN Body 完整还原，特殊字符不变
- AND 二进制长度字段确保特殊字符不会破坏格式解析

#### Scenario: 消息体含二进制数据
- GIVEN 消息 Body 为二进制字节序列 `\x00\x01\x02\xFF`
- WHEN 消息被序列化再反序列化
- THEN Body 完整还原所有字节
- AND 格式解析不受 `\x00` 等特殊字节影响
