"""TCP 服务器 —— 多线程连接管理 + 命令分发"""

import socket
import threading
import logging

from src.protocol import (
    parse_command, Command,
    ok_response, err_response, null_response, data_response, message_response
)
from src.models import Consumer
from src.topic_manager import (
    TopicManager, TopicExistsError, TopicNotFoundError,
    TopicHasSubscribersError, MaxTopicsReachedError, InvalidTopicNameError,
)
from src.queue_manager import (
    QueueManager, QueueExistsError, QueueNotFoundError as QueueNotFoundErr,
    QueueNotEmptyError, QueueFullError, InvalidQueueNameError,
    EmptyMessageError, MessageTooLargeError,
)
from src.message_router import MessageRouter

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

        # 核心模块
        self.topic_mgr = TopicManager()
        self.queue_mgr = QueueManager()
        self.router = MessageRouter(self.topic_mgr, self.queue_mgr)

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
        # 从所有 Topic 中移除
        self.topic_mgr.remove_consumer_from_all(consumer.consumer_id)
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

    # --- Topic 管理 ---
    def _cmd_create_topic(self, cmd: Command, consumer: Consumer) -> str:
        try:
            name = cmd.args[0]
            self.topic_mgr.create_topic(name)
            return ok_response(f"Topic '{name}' created")
        except InvalidTopicNameError as e:
            return err_response("ERR_INVALID_NAME", str(e))
        except TopicExistsError:
            return err_response("ERR_TOPIC_EXISTS", f"Topic '{cmd.args[0]}' already exists")
        except MaxTopicsReachedError:
            return err_response("ERR_MAX_TOPICS_REACHED")

    def _cmd_delete_topic(self, cmd: Command, consumer: Consumer) -> str:
        try:
            name = cmd.args[0]
            self.topic_mgr.delete_topic(name)
            return ok_response(f"Topic '{name}' deleted")
        except TopicNotFoundError:
            return err_response("ERR_TOPIC_NOT_FOUND")
        except TopicHasSubscribersError:
            return err_response("ERR_TOPIC_HAS_SUBSCRIBERS")

    def _cmd_list_topics(self, cmd: Command, consumer: Consumer) -> str:
        topics = self.topic_mgr.list_topics()
        return data_response("\n".join(topics) if topics else "(empty)")

    # --- Queue 管理 ---
    def _cmd_create_queue(self, cmd: Command, consumer: Consumer) -> str:
        try:
            name = cmd.args[0]
            max_size = int(cmd.args[1]) if len(cmd.args) > 1 else None
            self.queue_mgr.create_queue(name, max_size)
            return ok_response(f"Queue '{name}' created")
        except InvalidQueueNameError as e:
            return err_response("ERR_INVALID_NAME", str(e))
        except QueueExistsError:
            return err_response("ERR_QUEUE_EXISTS")

    def _cmd_delete_queue(self, cmd: Command, consumer: Consumer) -> str:
        try:
            name = cmd.args[0]
            self.queue_mgr.delete_queue(name)
            return ok_response(f"Queue '{name}' deleted")
        except QueueNotFoundErr:
            return err_response("ERR_QUEUE_NOT_FOUND")
        except QueueNotEmptyError:
            return err_response("ERR_QUEUE_NOT_EMPTY")

    def _cmd_list_queues(self, cmd: Command, consumer: Consumer) -> str:
        queues = self.queue_mgr.list_queues()
        return data_response("\n".join(queues) if queues else "(empty)")

    # --- 消息生产 ---
    def _cmd_publish(self, cmd: Command, consumer: Consumer) -> str:
        try:
            topic, body = cmd.args[0], cmd.args[1]
            msg_id = self.router.route_to_topic(topic, body)
            return ok_response(msg_id)
        except TopicNotFoundError:
            return err_response("ERR_TOPIC_NOT_FOUND")
        except EmptyMessageError:
            return err_response("ERR_EMPTY_MESSAGE")
        except MessageTooLargeError:
            return err_response("ERR_MESSAGE_TOO_LARGE")

    def _cmd_send(self, cmd: Command, consumer: Consumer) -> str:
        try:
            queue, body = cmd.args[0], cmd.args[1]
            msg_id = self.router.route_to_queue(queue, body)
            return ok_response(msg_id)
        except QueueNotFoundErr:
            return err_response("ERR_QUEUE_NOT_FOUND")
        except QueueFullError:
            return err_response("ERR_QUEUE_FULL")
        except EmptyMessageError:
            return err_response("ERR_EMPTY_MESSAGE")
        except MessageTooLargeError:
            return err_response("ERR_MESSAGE_TOO_LARGE")

    # --- 消息消费 ---
    def _cmd_consume(self, cmd: Command, consumer: Consumer) -> str:
        queue = cmd.args[0]
        timeout = 0

        # 解析 TIMEOUT <ms>
        if len(cmd.args) >= 3 and cmd.args[1].upper() == "TIMEOUT":
            try:
                timeout = int(cmd.args[2]) / 1000.0  # ms → s
            except ValueError:
                return err_response("ERR_INVALID_TIMEOUT", "Timeout must be an integer in milliseconds")

        try:
            msg = self.router.consume_from_queue(
                queue, consumer.consumer_id, timeout
            )
            if msg is None:
                return null_response()
            return message_response(msg.msg_id, msg.body)
        except QueueNotFoundErr:
            return err_response("ERR_QUEUE_NOT_FOUND")

    def _cmd_subscribe(self, cmd: Command, consumer: Consumer) -> str:
        try:
            topic = cmd.args[0]
            self.topic_mgr.subscribe(topic, consumer.consumer_id)
            consumer.subscriptions.add(topic)
            return ok_response(f"Subscribed to '{topic}'")
        except TopicNotFoundError:
            return err_response("ERR_TOPIC_NOT_FOUND")

    def _cmd_unsubscribe(self, cmd: Command, consumer: Consumer) -> str:
        try:
            topic = cmd.args[0]
            self.topic_mgr.unsubscribe(topic, consumer.consumer_id)
            consumer.subscriptions.discard(topic)
            return ok_response(f"Unsubscribed from '{topic}'")
        except TopicNotFoundError:
            return err_response("ERR_TOPIC_NOT_FOUND")

    def _cmd_set_offset(self, cmd: Command, consumer: Consumer) -> str:
        try:
            topic, offset = cmd.args[0], int(cmd.args[1])
            self.router.set_offset(topic, consumer.consumer_id, offset)
            return ok_response(f"Offset set to {offset}")
        except ValueError as e:
            return err_response("ERR_INVALID_OFFSET", str(e))

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
