"""消息队列服务器入口

运行方式: python -m src.main  或  python src/main.py
"""

import sys
import os

# 确保项目根目录在 path 中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.server import MQServer


def main():
    server = MQServer(host="0.0.0.0", port=9876)
    try:
        print("[MQ] Starting server on 0.0.0.0:9876 ...")
        server.start()
    except KeyboardInterrupt:
        print("\n[MQ] Received shutdown signal.")
    finally:
        server.stop()
        print("[MQ] Server stopped.")


if __name__ == "__main__":
    main()
