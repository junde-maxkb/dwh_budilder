import logging
import sys

from common.config import ConfigManager
from core.data_processor import DataProcessor
from core.system_manager import SystemManager
from core.task_manager import TaskManager
from core.monitor_service import MonitorService
from database.database_manager import DataBaseManager

logger = logging.getLogger(__name__)


class StartupManager:
    """启动管理器 - 负责系统初始化和启动流程"""

    def __init__(self):
        self.config_manager = None
        self.system_manager = None
        self.db_manager = None
        self.data_processor = None
        self.task_manager = None
        self.monitor_service = None

    def initialize_system(self) -> bool:
        """
        初始化系统所有组件

        Returns:
            bool: 初始化是否成功
        """
        try:
            logger.info("正在启动数据处理系统...")

            self.config_manager = ConfigManager()

            self.system_manager = SystemManager(max_workers=5, health_check_interval=60)
            self.db_manager = DataBaseManager()

            api_config = {
                'base_url': self.config_manager.get('api.base_url', 'http://10.134.188.79:8080'),
                'app_key': self.config_manager.get('api.app_key', '38318de66662a1cc8fc5c745e221081b'),
                'app_secret': self.config_manager.get('api.app_secret', '5ad01a9a76d79ca19a806690050c9d7e')
            }

            auto_report_config = {
                'username': self.config_manager.get('financial_api.username', 'lijin5'),
                'password': self.config_manager.get('financial_api.password', 'Qaz.123456789.')
            }

            self.data_processor = DataProcessor(api_config, self.db_manager, auto_report_config)

            self.task_manager = TaskManager(self.data_processor, self.system_manager, self.db_manager)

            self.monitor_service = MonitorService(
                self.system_manager,
                self.data_processor,
                self.task_manager,
                self.config_manager
            )

            logger.info("系统组件初始化完成")
            return True

        except Exception as e:
            logger.error(f"系统初始化失败: {e}", exc_info=True)
            return False

    def start_system(self) -> bool:
        """
        启动系统

        Returns:
            bool: 启动是否成功
        """
        try:
            # 启动系统管理器
            self.system_manager.start()
            logger.info("系统管理器已启动")

            # 创建初始任务
            financial_success, traditional_success, crawler_success = self.task_manager.create_initial_tasks()

            if financial_success:
                logger.info("财务报表任务添加成功")
            else:
                logger.warning("财务报表任务添加失败，但继续执行其他任务")

            if not traditional_success:
                logger.error("传统数据任务添加失败")
                return False

            if crawler_success:
                logger.info("爬虫任务（组织架构、资金流水、报账单）添加成功")
            else:
                logger.warning("爬虫任务添加失败，但继续执行其他任务")

            return True

        except Exception as e:
            logger.error(f"系统启动失败: {e}", exc_info=True)
            return False

    def start_monitoring(self):
        """启动监控服务"""
        try:
            self.monitor_service.start_continuous_monitoring()
        except KeyboardInterrupt:
            logger.info("接收到中断信号，正在关闭系统...")
            raise

    def shutdown_system(self):
        """优雅关闭系统"""
        try:
            logger.info("正在关闭数据处理系统...")

            if self.system_manager:
                self.system_manager.stop(timeout=60)

            if self.data_processor:
                self.data_processor.close()

            logger.info("数据处理系统已关闭")

        except Exception as e:
            logger.error(f"关闭系统时发生错误: {e}")

    def run(self):
        try:
            if not self.initialize_system():
                logger.error("系统初始化失败，退出程序")
                sys.exit(1)

            if not self.start_system():
                logger.error("系统启动失败，退出程序")
                sys.exit(1)

            self.start_monitoring()

        except KeyboardInterrupt:
            logger.info("接收到中断信号，正在关闭系统...")
        except Exception as e:
            logger.error(f"程序执行失败: {e}", exc_info=True)
            sys.exit(1)
        finally:
            # 4. 关闭系统
            self.shutdown_system()
