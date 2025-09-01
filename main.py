import logging
import sys
import time

from common.config import ConfigManager
from core.data_processor import DataProcessor, create_batch_processing_tasks
from core.system_manager import SystemManager
from database.database_manager import DataBaseManager


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


setup_logging()
logger = logging.getLogger(__name__)


def main():
    """主函数"""
    try:
        config_manager = ConfigManager()

        logger.info("正在启动数据处理系统...")

        # 创建系统管理器
        system_manager = SystemManager(max_workers=5, health_check_interval=60)

        # 创建数据库管理器
        db_manager = DataBaseManager()

        # API配置
        api_config = {
            'base_url': config_manager.get('api.base_url', 'http://10.134.188.79:8080'),
            'app_key': config_manager.get('api.app_key', '38318de66662a1cc8fc5c745e221081b'),
            'app_secret': config_manager.get('api.app_secret', '5ad01a9a76d79ca19a806690050c9d7e')
        }

        # 创建数据处理器
        data_processor = DataProcessor(api_config, db_manager)

        # 启动系统管理器
        system_manager.start()
        logger.info("系统管理器已启动")

        try:
            company_codes = ['001']
            data_types = [
                'account_structure',  # 会计科目结构
                'subject_dimension',  # 科目辅助核算关系
                'customer_vendor',  # 客商字典
                'voucher_list',  # 凭证目录
                'voucher_detail',  # 凭证明细
                'balance',  # 科目余额
                'aux_balance'  # 辅助余额
            ]

            # 创建批量处理任务
            tasks_config = create_batch_processing_tasks(
                company_codes=company_codes,
                data_types=data_types,
                year='2024',
                period_code='202412'
            )

            logger.info(f"创建了 {len(tasks_config)} 个数据处理任务")

            # 将任务添加到系统管理器的队列中
            success = data_processor.add_processing_tasks_to_system(
                system_manager,
                tasks_config
            )

            if success:
                logger.info("所有数据处理任务已成功添加到队列")
            else:
                logger.error("添加任务到队列时发生错误")
                return

            # 监控系统运行状态
            while True:
                # 获取系统状态
                system_status = system_manager.get_system_status()
                processing_stats = data_processor.get_processing_statistics()

                logger.info(f"系统状态: {system_status}")
                logger.info(f"处理统计: {processing_stats}")

                # 检查是否所有任务都已完成
                if (system_status["tasks"]["pending"] == 0 and
                        system_status["tasks"]["running"] == 0 and
                        system_status["tasks"]["retry"] == 0):

                    completed_tasks = system_status["tasks"]["completed"]
                    failed_tasks = system_status["tasks"]["failed"]

                    logger.info(f"所有任务处理完成! 成功: {completed_tasks}, 失败: {failed_tasks}")

                    # 如果需要，可以在这里添加新的任务或者退出
                    if failed_tasks > 0:
                        logger.warning("存在失败的任务，请检查日志")

                    # 清理完成的任务（保留最近24小时的）
                    cleared_count = system_manager.clear_completed_tasks(older_than_hours=24)
                    if cleared_count > 0:
                        logger.info(f"清理了 {cleared_count} 个历史任务")

                    break

                # 每30秒检查一次状态
                time.sleep(30)

        except KeyboardInterrupt:
            logger.info("接收到中断信号，正在关闭系统...")

        finally:
            # 优雅关闭系统
            logger.info("正在关闭数据处理系统...")
            system_manager.stop(timeout=60)
            data_processor.close()
            logger.info("数据处理系统已关闭")

    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
