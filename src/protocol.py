"""协议解析器 —— 文本协议（类 RESP 风格）的命令解析与响应编码

协议格式:
    请求: <COMMAND> [arg1] [arg2] [...] [body]\r\n
    响应:
        成功: +OK [data]\r\n
        错误: -ERR_CODE message\r\n
        空值: $-1\r\n
        数据: $<len>\r\n<data>\r\n
"""

from dataclasses import dataclass
import re

# ----------------------------------------------------------------
# 命令定义
# ----------------------------------------------------------------

@dataclass
class Command:
    """解析后的命令对象"""
    name: str          # 命令名（大写）
    args: list         # 参数列表，含 body

# 所有支持的命令名集合
VALID_COMMANDS = {
    # Topic / Queue 管理
    "CREATE_TOPIC", "DELETE_TOPIC", "LIST_TOPICS",
    "CREATE_QUEUE", "DELETE_QUEUE", "LIST_QUEUES",
    # 消息生产
    "PUBLISH", "SEND",
    # 消息消费
    "CONSUME", "SUBSCRIBE", "UNSUBSCRIBE",
    "CONSUME_GROUP", "SET_OFFSET",
    # 消息确认
    "ACK", "NACK", "ACK_BATCH",
    # 消费者组
    "CREATE_GROUP", "DELETE_GROUP", "LEAVE_GROUP",
    # 心跳
    "PING",
    # DLQ
    "REPLAY", "REPLAY_ALL",
    # 连接
    "QUIT",
}

# 需要至少 1 个参数的命令（参数数量不含命令名本身）
COMMAND_MIN_ARGS = {
    "CREATE_TOPIC": 1,
    "DELETE_TOPIC": 1,
    "LIST_TOPICS": 0,
    "CREATE_QUEUE": 1,
    "DELETE_QUEUE": 1,
    "LIST_QUEUES": 0,
    "PUBLISH": 2,          # <topic> <body>
    "SEND": 2,             # <queue> <body>
    "CONSUME": 1,          # <queue> [TIMEOUT <ms>]
    "SUBSCRIBE": 1,
    "UNSUBSCRIBE": 1,
    "CONSUME_GROUP": 2,    # <queue> <group>
    "SET_OFFSET": 2,       # <topic> <offset>
    "ACK": 1,
    "NACK": 1,
    "ACK_BATCH": 1,
    "CREATE_GROUP": 2,     # <target> <group_name>
    "DELETE_GROUP": 1,
    "LEAVE_GROUP": 1,
    "PING": 0,
    "REPLAY": 2,           # DLQ <msg_id>
    "REPLAY_ALL": 2,       # DLQ <target>
    "QUIT": 0,
}


# ----------------------------------------------------------------
# 解析函数
# ----------------------------------------------------------------

def parse_command(raw: str) -> Command:
    """解析原始文本行，返回 Command 对象。
    
    处理规则：
    - 去除首尾空白和 \\r\\n
    - 按空格切分，保留消息体中的空格
    - 空行返回 None（用特殊命令名 "" 表示）

    Raises:
        ValueError: 命令格式无效
    """
    raw = raw.strip()
    if not raw:
        raise ValueError("Empty command")

    # 按空格分割
    parts = raw.split(" ")
    cmd_name = parts[0].upper()

    if cmd_name not in VALID_COMMANDS:
        raise ValueError(f"Unknown command: {cmd_name}")

    min_args = COMMAND_MIN_ARGS.get(cmd_name, 0)
    args = parts[1:]

    # 对于 PUBLISH / SEND，body 从第 2 个参数开始拼接（因为 body 可能含空格）
    if cmd_name in ("PUBLISH", "SEND"):
        if len(args) < 2:
            raise ValueError(f"{cmd_name} requires <target> and <body>")
        target = args[0]
        body = " ".join(args[1:])
        return Command(name=cmd_name, args=[target, body])

    # 普通命令：直接使用切割后的 args
    if len(args) < min_args:
        raise ValueError(
            f"{cmd_name} requires at least {min_args} arg(s), got {len(args)}"
        )

    return Command(name=cmd_name, args=args)


# ----------------------------------------------------------------
# 响应编码
# ----------------------------------------------------------------

def ok_response(data: str = "") -> str:
    """成功响应: +OK [data]\r\n"""
    if data:
        return f"+OK {data}\r\n"
    return "+OK\r\n"

def err_response(code: str, msg: str = "") -> str:
    """错误响应: -ERR_CODE message\r\n"""
    if msg:
        return f"-{code} {msg}\r\n"
    return f"-{code}\r\n"

def null_response() -> str:
    """空响应（队列无消息等）: $-1\r\n"""
    return "$-1\r\n"

def data_response(data: str) -> str:
    """数据响应: $<len>\r\n<data>\r\n"""
    return f"${len(data)}\r\n{data}\r\n"

def message_response(msg_id: str, body: str) -> str:
    """消息响应（CONSUME 专用）: $<len>\r\n<msg_id> <body>\r\n"""
    payload = f"{msg_id} {body}"
    return f"${len(payload)}\r\n{payload}\r\n"
