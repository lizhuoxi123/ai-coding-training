#!/usr/bin/env python3
"""MQ 命令行客户端 —— 交互式 REPL 和单命令模式

使用方式:
    # 交互模式（默认连接 localhost:9876）
    python client.py

    # 指定连接地址
    python client.py connect localhost 9876

    # 单命令模式
    python client.py create_topic orders
    python client.py publish orders '{"order_id": 123}'
    python client.py consume tasks
"""

import socket
import sys
import readline  # 启用行编辑和历史（Unix）；Windows 用 pyreadline


class MQClient:
    """轻量级消息队列 CLI 客户端"""

    def __init__(self, host: str = "localhost", port: int = 9876):
        self.host = host
        self.port = port
        self._sock: socket.socket | None = None

    def connect(self) -> None:
        """连接到 MQ 服务器"""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(10)
        self._sock.connect((self.host, self.port))
        # 读取欢迎消息
        welcome = self._recv()
        print(welcome)

    def disconnect(self) -> None:
        """断开连接"""
        if self._sock:
            try:
                self._sock.sendall(b"QUIT\r\n")
            except OSError:
                pass
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def send(self, command: str) -> str:
        """发送命令并返回响应"""
        if not self._sock:
            self.connect()
        raw = command.strip() + "\r\n"
        self._sock.sendall(raw.encode("utf-8"))
        return self._recv()

    def _recv(self) -> str:
        """接收一行响应"""
        data = b""
        while b"\r\n" not in data:
            chunk = self._sock.recv(4096)
            if not chunk:
                break
            data += chunk
        return data.decode("utf-8", errors="replace").strip()

    def repl(self) -> None:
        """交互式 REPL 循环"""
        print("MQ Client REPL — type 'help' for commands, 'quit' to exit.")
        print(f"Connected to {self.host}:{self.port}")

        while True:
            try:
                line = input("mq> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nBye!")
                break

            if not line:
                continue

            if line.lower() in ("quit", "exit", "q"):
                break

            if line.lower() == "help":
                self._print_help()
                continue

            try:
                response = self.send(line)
                print(response)
            except (ConnectionRefusedError, ConnectionResetError, OSError) as e:
                print(f"Connection error: {e}")
                break

        self.disconnect()

    @staticmethod
    def _print_help() -> None:
        print("""
=== MQ Commands ===
  Topic/Queue 管理:
    CREATE_TOPIC <name>        创建 Topic
    DELETE_TOPIC <name>        删除 Topic
    LIST_TOPICS                列出所有 Topic
    CREATE_QUEUE <name> [max]  创建 Queue
    DELETE_QUEUE <name>        删除 Queue
    LIST_QUEUES                列出所有 Queue

  消息生产:
    PUBLISH <topic> <body>     向 Topic 发布消息
    SEND <queue> <body>        向 Queue 发送消息

  消息消费:
    CONSUME <queue> [TIMEOUT <ms>]  从 Queue 消费（支持 Long Polling）
    SUBSCRIBE <topic>          订阅 Topic
    UNSUBSCRIBE <topic>        取消订阅

  消费者组:
    CREATE_GROUP <target> <group>  创建消费者组
    DELETE_GROUP <group>       删除消费者组
    CONSUME_GROUP <queue> <group>  以组模式消费
    LEAVE_GROUP <group>        离开消费者组

  消息确认:
    ACK <msg_id>               确认消息
    NACK <msg_id>              否定确认
    ACK_BATCH <id1> [id2...]  批量确认

  DLQ:
    REPLAY DLQ <msg_id>        重放死信消息
    REPLAY_ALL DLQ <target>    批量重放

  Offset:
    SET_OFFSET <topic> <offset>  设置消费位置

  其他:
    PING                       心跳
    QUIT / exit / q            退出
""")


# ================================================================
# 入口
# ================================================================

def main():
    client = MQClient()

    # 单命令模式
    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd == "connect" and len(sys.argv) >= 4:
            client.host = sys.argv[2]
            client.port = int(sys.argv[3])
            client.connect()
            client.repl()
            return

        command = " ".join(sys.argv[1:])
        try:
            response = client.send(command)
            print(response)
        except (ConnectionRefusedError, OSError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            client.disconnect()
        return

    # 交互模式
    try:
        client.connect()
    except (ConnectionRefusedError, OSError) as e:
        print(f"Cannot connect to {client.host}:{client.port}: {e}")
        print("Start the server first: python -m src.main")
        sys.exit(1)

    client.repl()


if __name__ == "__main__":
    main()
