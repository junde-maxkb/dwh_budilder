import logging
import sys

from core.startup_manager import StartupManager


def setup_logging() -> None:
    """配置日志系统"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('app.log', encoding='utf-8')
        ]
    )


def main():
    setup_logging()

    # 创建并运行启动管理器
    startup_manager = StartupManager()
    startup_manager.run()


if __name__ == "__main__":
    main()
