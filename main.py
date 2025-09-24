import logging
import sys
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

from core.startup_manager import StartupManager


def setup_logging() -> None:
    # 确保日志目录存在
    logs_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logs_dir, exist_ok=True)

    date_str = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(logs_dir, f'app_{date_str}.log')

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 清理已存在处理器，避免重复添加
    if root_logger.handlers:
        for h in list(root_logger.handlers):
            root_logger.removeHandler(h)

    # 统一格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # 文件大小轮转处理器
    file_handler = RotatingFileHandler(
        log_file, maxBytes=50 * 1024 * 1024, backupCount=10, encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def main():
    setup_logging()

    # 创建并运行启动管理器
    startup_manager = StartupManager()
    startup_manager.run()


if __name__ == "__main__":
    main()
