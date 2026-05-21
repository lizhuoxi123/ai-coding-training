"""TCP 服务器 —— 多线程连接管理 + 命令分发"""

import socket
import threading
import logging

from src.protocol import (
    parse_command, Command,
    ok_response, err_response, null_response, data_response, message_response
)
from src.models import Consumer

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("MQServer")


class MQServer:
    """轻量级消息队列 TCP 服务器"""

    def __init__(self, host: str = "0.0.0.0", port: int = 9876):
        self.host = host
        self.port = port
        self._server_socket: socket.socket | None = None
        self._running = False
        self._client_threads: list[threading.Thread] = []

        # 消费者注册表: consumer_id -> Consumer
        self._consumers: dict[str, Consumer] = {}
        self._consumers_lock = threading.Lock()
        self._next_consumer_id = 0

    # ================================================================
    # 生命周期
    # ================================================================

    def start(self) -> None:
        """启动服务器，开始监听"""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(128)
        self._server_socket.settimeout(1.0)  # 每秒检查一次是否要退出
        self._running = True

        logger.info(f"MQ Server listening on {self.host}:{self.port}")

        while self._running:
            try:
                conn, addr = self._server_socket.accept()
                logger.info(f"New connection from {addr}")
                t = threading.Thread(
                    target=self._handle_client,
                    args=(conn, addr),
                    daemon=True,
                )
                t.start()
                self._client_threads.append(t)
            except socket.timeout:
                continue  # 正常超时，检查 _running 后继续
            except OSError:
                if self._running:
                    logger.exception("Socket error during accept")
                break

    def stop(self) -> None:
        """优雅关闭服务器"""
        logger.info("Shutting down server...")
        self._running = False

        # 关闭监听 socket
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass

        # 等待所有客户端线程退出（最多 5 秒）
        for t in self._client_threads:
            t.join(timeout=5.0)

        logger.info("Server stopped.")

    # ================================================================
    # 客户端处理
    # ================================================================

    def _handle_client(self, conn: socket.socket, addr: tuple) -> None:
        """处理单个客户端连接（运行在独立线程中）"""
        consumer = self._register_consumer(conn)
        logger.info(f"Consumer {consumer.consumer_id} registered ({addr})")

        try:
            # 发送欢迎消息
            conn.sendall(ok_response(f"Welcome! Your ID: {consumer.consumer_id}").encode())

            buf = ""
            while self._running:
                try:
                    data = conn.recv(4096)
                    if not data:
                        logger.info(f"Consumer {consumer.consumer_id} disconnected")
                        break

                    buf += data.decode("utf-8", errors="replace")

                    # 按 \r\n 分割处理完整命令
                    while "\r\n" in buf:
                        line, buf = buf.split("\r\n", 1)
                        if not line.strip():
                            continue
                        response = self._dispatch(line.strip(), consumer)
                        conn.sendall(response.encode())

                except ConnectionResetError:
                    break
                except Exception:
                    logger.exception(f"Error handling consumer {consumer.consumer_id}")
                    try:
                        conn.sendall(err_response("ERR_INTERNAL", "Internal server error").encode())
                    except OSError:
                        break
        finally:
            self._unregister_consumer(consumer)
            try:
                conn.close()
            except OSError:
                pass

    def _register_consumer(self, conn: socket.socket) -> Consumer:
        """注册新消费者"""
        with self._consumers_lock:
            self._next_consumer_id += 1
            consumer_id = f"C{self._next_consumer_id}"
            consumer = Consumer(consumer_id=consumer_id)
            self._consumers[consumer_id] = consumer
            return consumer

    def _unregister_consumer(self, consumer: Consumer) -> None:
        """注销消费者"""
        with self._consumers_lock:
            self._consumers.pop(consumer.consumer_id, None)
        logger.info(f"Consumer {consumer.consumer_id} unregistered")

    # ================================================================
    # 命令分发
    # ================================================================

    def _dispatch(self, raw: str, consumer: Consumer) -> str:
        """解析并分发命令到对应的 handler"""
        try:
            cmd = parse_command(raw)
        except ValueError as e:
            return err_response("ERR_INVALID_COMMAND", str(e))

        handler = getattr(self, f"_cmd_{cmd.name.lower()}", None)
        if handler is None:
            return err_response("ERR_NOT_IMPLEMENTED", f"Command {cmd.name} not implemented yet")

        try:
            return handler(cmd, consumer)
        except Exception as e:
            logger.exception(f"Error executing {cmd.name}")
            return err_response("ERR_INTERNAL", str(e))

    # ================================================================
    # 命令 Handler Stubs（后续 Phase 实现）
    # ================================================================

    def _cmd_ping(self, cmd: Command, consumer: Consumer) -> str:
        import time
        consumer.last_heartbeat = time.time()
        return ok_response("PONG")

    def _cmd_quit(self, cmd: Command, consumer: Consumer) -> str:
        return ok_response("BYE")

    # --- Topic / Queue 管理（stub） ---
    def _cmd_create_topic(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    def _cmd_delete_topic(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    def _cmd_list_topics(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    def _cmd_create_queue(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    def _cmd_delete_queue(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    def _cmd_list_queues(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    # --- 消息生产 ---
    def _cmd_publish(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    def _cmd_send(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    # --- 消息消费 ---
    def _cmd_consume(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    def _cmd_subscribe(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    def _cmd_unsubscribe(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    def _cmd_consume_group(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 4")

    def _cmd_set_offset(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 2")

    # --- 消息确认 ---
    def _cmd_ack(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 4")

    def _cmd_nack(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 4")

    def _cmd_ack_batch(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 4")

    # --- 消费者组 ---
    def _cmd_create_group(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 4")

    def _cmd_delete_group(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 4")

    def _cmd_leave_group(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 4")

    # --- DLQ ---
    def _cmd_replay(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 4")

    def _cmd_replay_all(self, cmd: Command, consumer: Consumer) -> str:
        return err_response("ERR_NOT_IMPLEMENTED", "Coming in Phase 4")
