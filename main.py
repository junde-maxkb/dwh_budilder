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


def process_financial_reports_tasks(data_processor: DataProcessor, system_manager: SystemManager) -> bool:
    """处理财务报表任务并添加到系统队列"""
    logger.info("=== 开始添加财务报表任务到队列 ===")

    try:
        # 定义需要处理的任务筛选条件
        task_filters = [
            "月报",
            "季报",
        ]

        financial_tasks_added = 0

        for i, task_filter in enumerate(task_filters):
            logger.info(f"添加财务报表任务 - 筛选条件: {task_filter or '全部任务'}")

            # 将财务报表任务添加到系统队列
            success = data_processor.add_financial_report_task_to_system(
                system_manager=system_manager,
                task_name_filter=task_filter,
                priority=10 + i
            )

            if success:
                financial_tasks_added += 1
                logger.info(f"✅ 财务报表任务已添加到队列 - 筛选条件: {task_filter or '全部任务'}")
            else:
                logger.error(f"❌ 添加财务报表任务失败 - 筛选条件: {task_filter or '全部任务'}")

        logger.info(f"财务报表任务添加完成，成功添加 {financial_tasks_added} 个任务")
        return financial_tasks_added > 0

    except Exception as e:
        logger.error(f"添加财务报表任务到队列时发生错误: {e}", exc_info=True)
        return False


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

        # 财务报表API配置
        auto_report_config = {
            'username': config_manager.get('financial_api.username', 'lijin5'),
            'password': config_manager.get('financial_api.password', 'Qaz.123456789.')
        }

        # 创建数据处理器（集成财务报表API功能）
        data_processor = DataProcessor(api_config, db_manager, auto_report_config)

        # 启动系统管理器
        system_manager.start()
        logger.info("系统管理器已启动")

        try:
            # === 1. 添加财务报表任务到队列 ===
            logger.info("步骤1: 添加财务报表任务到队列")
            financial_success = process_financial_reports_tasks(data_processor, system_manager)

            if financial_success:
                logger.info("财务报表任务添加成功")
            else:
                logger.warning("财务报表任务添加失败，但继续执行其他任务")

            # === 2. 添加传统财务数据任务到队列 ===
            logger.info("步骤2: 添加传统财务数据任务到队列")

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

            logger.info(f"创建了 {len(tasks_config)} 个传统数据处理任务")

            # 将任务添加到系统管理器的队列中
            success = data_processor.add_processing_tasks_to_system(
                system_manager,
                tasks_config
            )

            if success:
                logger.info("所有传统数据处理任务已成功添加到队列")
            else:
                logger.error("添加传统数据任务到队列时发生错误")
                return

            # === 3. 监控系统运行状态 ===
            logger.info("步骤3: 开始监控系统运行状态")
            logger.info("=" * 60)

            monitor_count = 0
            while True:
                monitor_count += 1

                # 获取系统状态
                system_status = system_manager.get_system_status()
                processing_stats = data_processor.get_processing_statistics()

                # 每5次监控输出一次详细状态
                if monitor_count % 5 == 1:
                    logger.info(f"系统状态详情: {system_status}")
                    logger.info(f"处理统计信息: {processing_stats}")
                else:
                    # 简化输出
                    tasks = system_status["tasks"]
                    logger.info(f"任务状态 - 待处理:{tasks['pending']}, 运行中:{tasks['running']}, "
                                f"重试:{tasks['retry']}, 已完成:{tasks['completed']}, 失败:{tasks['failed']}")

                # 检查是否所有任务都已完成
                if (system_status["tasks"]["pending"] == 0 and
                        system_status["tasks"]["running"] == 0 and
                        system_status["tasks"]["retry"] == 0):

                    completed_tasks = system_status["tasks"]["completed"]
                    failed_tasks = system_status["tasks"]["failed"]

                    logger.info("=" * 60)
                    logger.info(f"🎉 所有任务处理完成!")
                    logger.info(f"📊 任务统计: 成功 {completed_tasks} 个, 失败 {failed_tasks} 个")

                    # 如果需要，可以在这里添加新的任务或者退出
                    if failed_tasks > 0:
                        logger.warning("⚠️ 存在失败的任务，请检查日志获取详细信息")

                    # 清理完成的任务（保留最近24小时的）
                    cleared_count = system_manager.clear_completed_tasks(older_than_hours=24)
                    if cleared_count > 0:
                        logger.info(f"🧹 清理了 {cleared_count} 个历史任务")

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
