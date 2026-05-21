"""协议解析器单元测试"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.protocol import (
    parse_command, Command,
    ok_response, err_response, null_response, data_response, message_response,
)


class TestParseCommand:
    """命令解析测试"""

    def test_basic_command(self):
        cmd = parse_command("PING")
        assert cmd.name == "PING"
        assert cmd.args == []

    def test_command_with_args(self):
        cmd = parse_command("CREATE_TOPIC orders")
        assert cmd.name == "CREATE_TOPIC"
        assert cmd.args == ["orders"]

    def test_publish_with_body(self):
        cmd = parse_command('PUBLISH orders {"key": "value"}')
        assert cmd.name == "PUBLISH"
        assert cmd.args == ["orders", '{"key": "value"}']

    def test_publish_body_with_spaces(self):
        cmd = parse_command("PUBLISH orders hello world test")
        assert cmd.name == "PUBLISH"
        assert cmd.args == ["orders", "hello world test"]

    def test_send_with_body(self):
        cmd = parse_command('SEND tasks {"task": "A"}')
        assert cmd.name == "SEND"
        assert cmd.args == ["tasks", '{"task": "A"}']

    def test_consume_with_timeout(self):
        cmd = parse_command("CONSUME tasks TIMEOUT 5000")
        assert cmd.name == "CONSUME"
        assert cmd.args == ["tasks", "TIMEOUT", "5000"]

    def test_consume_simple(self):
        cmd = parse_command("CONSUME tasks")
        assert cmd.name == "CONSUME"
        assert cmd.args == ["tasks"]

    def test_create_group(self):
        cmd = parse_command("CREATE_GROUP tasks workers")
        assert cmd.name == "CREATE_GROUP"
        assert cmd.args == ["tasks", "workers"]

    def test_ack_batch(self):
        cmd = parse_command("ACK_BATCH M1 M2 M3")
        assert cmd.name == "ACK_BATCH"
        assert cmd.args == ["M1", "M2", "M3"]

    def test_replay(self):
        cmd = parse_command("REPLAY DLQ M123")
        assert cmd.name == "REPLAY"
        assert cmd.args == ["DLQ", "M123"]

    def test_empty_command(self):
        try:
            parse_command("")
            assert False, "Should have raised ValueError"
        except ValueError:
            pass

    def test_unknown_command(self):
        try:
            parse_command("UNKNOWN_CMD arg1")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Unknown command" in str(e)


class TestResponseEncoding:
    """响应编码测试"""

    def test_ok_response(self):
        assert ok_response() == "+OK\r\n"
        assert ok_response("test") == "+OK test\r\n"

    def test_err_response(self):
        assert err_response("ERR_TEST") == "-ERR_TEST\r\n"
        assert err_response("ERR_TEST", "message") == "-ERR_TEST message\r\n"

    def test_null_response(self):
        assert null_response() == "$-1\r\n"

    def test_data_response(self):
        resp = data_response("hello")
        assert resp == "$5\r\nhello\r\n"

    def test_message_response(self):
        resp = message_response("M001", '{"a":1}')
        assert resp.startswith("$")
        assert "M001" in resp
        assert '{"a":1}' in resp


def run_tests():
    """运行所有测试并输出结果"""
    import traceback

    test_classes = [TestParseCommand, TestResponseEncoding]
    passed = 0
    failed = 0

    for cls in test_classes:
        instance = cls()
        print(f"\n--- {cls.__name__} ---")
        for name in dir(instance):
            if name.startswith("test_"):
                method = getattr(instance, name)
                try:
                    method()
                    print(f"  ✓ {name}")
                    passed += 1
                except Exception as e:
                    print(f"  ✗ {name}: {e}")
                    traceback.print_exc()
                    failed += 1

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed, {passed+failed} total")
    return failed == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
