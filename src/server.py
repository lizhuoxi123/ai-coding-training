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
from src.persistence import PersistenceLayer
from src.consumer_group import (
    ConsumerGroupManager, GroupExistsError, GroupNotFoundError, GroupHasConsumersError,
)
from src.ack_manager import AckManager

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
        self.persistence = PersistenceLayer(data_dir="data")
        self.group_mgr = ConsumerGroupManager()
        self.ack_mgr = AckManager()

        # 后台线程
        self._checker_thread: threading.Thread | None = None

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
        self._server_socket.settimeout(1.0)
        self._running = True

        # 恢复持久化的消息
        recovered = self.persistence.recover()
        if recovered:
            logger.info(f"Recovered {len(recovered)} messages from disk")
            for msg in recovered:
                self.router.restore_message(msg)

        # 启动后台检查线程（超时 ACK、TTL、心跳）
        self._checker_thread = threading.Thread(
            target=self._checker_loop, daemon=True
        )
        self._checker_thread.start()

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

        # 关闭持久化层
        self.persistence.close()

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
        # 从所有消费者组中移除
        removed_groups = self.group_mgr.remove_consumer_from_all_groups(consumer.consumer_id)
        with self._consumers_lock:
            self._consumers.pop(consumer.consumer_id, None)
        logger.info(f"Consumer {consumer.consumer_id} unregistered")
        # 重分配未确认消息
        if removed_groups:
            self._redeliver_pending_messages(consumer.consumer_id)

    # ================================================================
    # 后台任务
    # ================================================================

    def _checker_loop(self) -> None:
        """后台检查线程：超时 ACK、TTL、心跳"""
        while self._running:
            time.sleep(5)  # 每 5 秒检查一次
            if not self._running:
                break

            try:
                # 检查 ACK 超时
                timed_out = self.ack_mgr.check_timeouts(self.router._messages)
                for msg_id in timed_out:
                    msg = self.router.get_message(msg_id)
                    if msg and msg.status == MessageStatus.PENDING:
                        # 重投到原始 queue
                        if self.queue_mgr.queue_exists(msg.target):
                            self.queue_mgr.enqueue(msg.target, msg.msg_id, msg.body)

                # 检查 TTL
                self.ack_mgr.check_ttl(self.router._messages)

                # 检查心跳超时
                offline = self.group_mgr.check_timeouts()
                for cid in offline:
                    self._redeliver_pending_messages(cid)

            except Exception:
                logger.exception("Error in checker loop")

    def _redeliver_pending_messages(self, consumer_id: str) -> None:
        """将消费者的未确认消息重新入队"""
        with self.router._msg_lock:
            for msg_id, msg in self.router._messages.items():
                if msg.delivered_to == consumer_id and msg.status == MessageStatus.DELIVERED:
                    msg.status = MessageStatus.PENDING
                    msg.delivered_to = ""
                    msg.deliver_time = 0.0
                    if self.queue_mgr.queue_exists(msg.target):
                        self.queue_mgr.enqueue(msg.target, msg.msg_id, msg.body)
                    logger.info(f"Message {msg_id} redelivered from offline consumer '{consumer_id}'")

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
            # 持久化消息
            msg = self.router.get_message(msg_id)
            if msg:
                self.persistence.append(msg)
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
            # 持久化消息
            msg = self.router.get_message(msg_id)
            if msg:
                self.persistence.append(msg)
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
        msg_id = cmd.args[0]
        msg = self.router.get_message(msg_id)
        if msg is None:
            return err_response("ERR_MESSAGE_NOT_FOUND")
        try:
            self.ack_mgr.ack(msg, consumer.consumer_id)
            return ok_response()
        except ValueError as e:
            if "not in DELIVERED state" in str(e):
                return ok_response()  # 幂等
            return err_response("ERR_NOT_YOUR_MESSAGE", str(e))

    def _cmd_nack(self, cmd: Command, consumer: Consumer) -> str:
        msg_id = cmd.args[0]
        msg = self.router.get_message(msg_id)
        if msg is None:
            return err_response("ERR_MESSAGE_NOT_FOUND")
        try:
            self.ack_mgr.nack(msg, consumer.consumer_id)
            # 如果 NACK 后变为 PENDING，重新入队
            if msg.status == MessageStatus.PENDING and self.queue_mgr.queue_exists(msg.target):
                self.queue_mgr.enqueue(msg.target, msg.msg_id, msg.body)
            return ok_response()
        except ValueError:
            return err_response("ERR_NOT_YOUR_MESSAGE")

    def _cmd_ack_batch(self, cmd: Command, consumer: Consumer) -> str:
        msg_ids = cmd.args
        failed = []
        for mid in msg_ids:
            msg = self.router.get_message(mid)
            if msg is None:
                failed.append(mid)
                continue
            try:
                self.ack_mgr.ack(msg, consumer.consumer_id)
            except ValueError:
                failed.append(mid)
        if failed:
            return err_response("ERR_PARTIAL_ACK", f"Failed: {', '.join(failed)}")
        return ok_response(f"ACKed {len(msg_ids)} messages")

    # --- 消费者组 ---
    def _cmd_create_group(self, cmd: Command, consumer: Consumer) -> str:
        try:
            target, group = cmd.args[0], cmd.args[1]
            self.group_mgr.create_group(target, group)
            return ok_response(f"Group '{group}' created (target='{target}')")
        except GroupExistsError:
            return err_response("ERR_GROUP_EXISTS")

    def _cmd_consume_group(self, cmd: Command, consumer: Consumer) -> str:
        try:
            queue, group = cmd.args[0], cmd.args[1]

            # 如果组不存在，自动创建
            if not self.group_mgr.group_exists(group):
                if not self.queue_mgr.queue_exists(queue):
                    return err_response("ERR_QUEUE_NOT_FOUND")
                self.group_mgr.create_group(queue, group)

            self.group_mgr.join_group(group, consumer.consumer_id)
            consumer.groups.add(group)

            # Round-Robin：从组中选消费者
            selected = self.group_mgr.next_consumer(group)
            if selected != consumer.consumer_id:
                return null_response()  # 未轮到当前消费者

            msg = self.router.consume_from_queue(queue, consumer.consumer_id)
            if msg is None:
                return null_response()
            return message_response(msg.msg_id, msg.body)

        except QueueNotFoundErr:
            return err_response("ERR_QUEUE_NOT_FOUND")

    def _cmd_delete_group(self, cmd: Command, consumer: Consumer) -> str:
        try:
            group = cmd.args[0]
            self.group_mgr.delete_group(group)
            return ok_response(f"Group '{group}' deleted")
        except GroupNotFoundError:
            return err_response("ERR_GROUP_NOT_FOUND")
        except GroupHasConsumersError:
            return err_response("ERR_GROUP_HAS_CONSUMERS")

    def _cmd_leave_group(self, cmd: Command, consumer: Consumer) -> str:
        try:
            group = cmd.args[0]
            self.group_mgr.leave_group(group, consumer.consumer_id)
            consumer.groups.discard(group)
            return ok_response(f"Left group '{group}'")
        except GroupNotFoundError:
            return err_response("ERR_GROUP_NOT_FOUND")

    # --- DLQ ---
    def _cmd_replay(self, cmd: Command, consumer: Consumer) -> str:
        _dlq_marker, msg_id = cmd.args[0], cmd.args[1]
        msg = self.ack_mgr.replay_from_dlq(msg_id)
        if msg is None:
            return err_response("ERR_MESSAGE_NOT_FOUND")
        self.router.restore_message(msg)
        if self.queue_mgr.queue_exists(msg.target):
            self.queue_mgr.enqueue(msg.target, msg.msg_id, msg.body)
        return ok_response(f"Message {msg_id} replayed")

    def _cmd_replay_all(self, cmd: Command, consumer: Consumer) -> str:
        _dlq_marker, target = cmd.args[0], cmd.args[1]
        replayed = self.ack_mgr.replay_all_from_dlq(target)
        for msg in replayed:
            self.router.restore_message(msg)
            if self.queue_mgr.queue_exists(msg.target):
                self.queue_mgr.enqueue(msg.target, msg.msg_id, msg.body)
        return ok_response(f"Replayed {len(replayed)} messages to '{target}'")
