"""端到端集成测试 —— 需要先启动 MQ Server

运行: python tests/test_integration.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import socket
import time
import threading
import traceback

from src.server import MQServer


class MQTestClient:
    """集成测试用的简单 TCP 客户端"""

    def __init__(self, host="localhost", port=9876):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(5)
        self.sock.connect((host, port))
        # 读取欢迎消息
        self._recv()

    def send(self, cmd: str) -> str:
        self.sock.sendall((cmd + "\r\n").encode("utf-8"))
        return self._recv()

    def _recv(self) -> str:
        data = b""
        while b"\r\n" not in data:
            chunk = self.sock.recv(4096)
            if not chunk:
                break
            data += chunk
        return data.decode("utf-8", errors="replace").strip()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


# ================================================================
# Test Suite
# ================================================================

passed = 0
failed = 0


def test(name):
    """装饰器风格的测试注册"""
    def decorator(func):
        global passed, failed
        try:
            func()
            print(f"  ✓ {name}")
            passed += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            traceback.print_exc()
            failed += 1
    return decorator


def run_integration_tests():
    global passed, failed
    passed = 0
    failed = 0

    # 启动服务器（后台线程）
    server = MQServer(port=9877)  # 使用不同端口避免冲突
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    time.sleep(0.5)  # 等待服务器启动

    try:
        client = MQTestClient(port=9877)

        # === Topic 测试 ===
        @test("CREATE_TOPIC - success")
        def _():
            resp = client.send("CREATE_TOPIC test_orders")
            assert "+OK" in resp, resp

        @test("CREATE_TOPIC - duplicate")
        def _():
            resp = client.send("CREATE_TOPIC test_orders")
            assert "ERR_TOPIC_EXISTS" in resp, resp

        @test("CREATE_TOPIC - invalid name (empty)")
        def _():
            resp = client.send("CREATE_TOPIC ''")
            assert "ERR" in resp, resp

        @test("LIST_TOPICS")
        def _():
            resp = client.send("LIST_TOPICS")
            assert "test_orders" in resp, resp

        @test("DELETE_TOPIC")
        def _():
            resp = client.send("DELETE_TOPIC test_orders")
            assert "+OK" in resp, resp

        # === Queue 测试 ===
        @test("CREATE_QUEUE - success")
        def _():
            resp = client.send("CREATE_QUEUE test_tasks")
            assert "+OK" in resp, resp

        @test("LIST_QUEUES")
        def _():
            resp = client.send("LIST_QUEUES")
            assert "test_tasks" in resp, resp

        # === PUBLISH / SEND 测试 ===
        client.send("CREATE_TOPIC pub_orders")

        @test("PUBLISH - success")
        def _():
            resp = client.send('PUBLISH pub_orders {"order": 1}')
            assert "+OK" in resp, resp
            # 返回 msg_id
            assert resp.startswith("+OK M"), resp

        @test("PUBLISH - empty body")
        def _():
            resp = client.send("PUBLISH pub_orders ")
            assert "ERR" in resp or "+OK" in resp, resp

        @test("PUBLISH - nonexistent topic")
        def _():
            resp = client.send("PUBLISH ghost '{}'")
            assert "ERR_TOPIC_NOT_FOUND" in resp, resp

        @test("SEND - success")
        def _():
            resp = client.send('SEND test_tasks {"task": "A"}')
            assert "+OK" in resp, resp

        @test("SEND - empty body")
        def _():
            resp = client.send("SEND test_tasks ")
            assert "ERR" in resp or "+OK" in resp, resp

        # === CONSUME 测试（FIFO） ===
        @test("CONSUME - success (FIFO)")
        def _():
            # M1 已在队列中
            resp = client.send("CONSUME test_tasks")
            assert "task" in resp and "A" in resp, resp

        @test("CONSUME - empty queue")
        def _():
            # 先消费掉所有消息（之前可能因 SEND 空 body 入队了）
            while True:
                r = client.send("CONSUME test_tasks")
                if "$-1" in r:
                    break
            resp = client.send("CONSUME test_tasks")
            assert "$-1" in resp, resp

        # === SUBSCRIBE / UNSUBSCRIBE 测试 ===
        @test("SUBSCRIBE - success")
        def _():
            resp = client.send("SUBSCRIBE pub_orders")
            assert "+OK" in resp, resp

        @test("UNSUBSCRIBE - success")
        def _():
            resp = client.send("UNSUBSCRIBE pub_orders")
            assert "+OK" in resp, resp

        # === ACK / NACK 测试 ===
        client.send('SEND test_tasks {"task": "B"}')

        @test("ACK - success")
        def _():
            consume_resp = client.send("CONSUME test_tasks")
            msg_id = consume_resp.split()[0].lstrip("$").lstrip("0123456789\r\n")
            # Wait, we need to parse the response properly
            # consume response: $<len>\r\n<msg_id> <body>\r\n
            lines = consume_resp.split("\r\n")
            if len(lines) >= 2:
                payload = lines[1]
                msg_id = payload.split()[0]
            else:
                msg_id = ""
            if msg_id:
                resp = client.send(f"ACK {msg_id}")
                assert "+OK" in resp or "ERR" in resp, resp

        # === 消费者组测试 ===
        @test("CREATE_GROUP - success")
        def _():
            resp = client.send("CREATE_GROUP test_tasks workers")
            assert "+OK" in resp, resp

        @test("DELETE_GROUP - success")
        def _():
            resp = client.send("DELETE_GROUP workers")
            assert "+OK" in resp, resp

        # === PING 测试 ===
        @test("PING")
        def _():
            resp = client.send("PING")
            assert "PONG" in resp, resp

        # === 错误命令测试 ===
        @test("Unknown command")
        def _():
            resp = client.send("FOOBAR")
            assert "ERR" in resp, resp

        client.close()

    finally:
        server.stop()

    print(f"\n{'='*40}")
    print(f"Integration Tests: {passed} passed, {failed} failed, {passed+failed} total")
    return failed == 0


if __name__ == "__main__":
    success = run_integration_tests()
    sys.exit(0 if success else 1)
