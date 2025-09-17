import logging
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class MonitorService:
    """监控服务 - 负责系统状态监控和定时检测"""

    def __init__(self, system_manager, data_processor, task_manager, config_manager):
        self.system_manager = system_manager
        self.data_processor = data_processor
        self.task_manager = task_manager
        self.config_manager = config_manager

        # 配置参数
        self.check_interval_minutes = config_manager.get('monitor.check_interval_minutes', 30)
        self.monitor_interval_seconds = config_manager.get('monitor.monitor_interval_seconds', 30)

        logger.info(f"⚙️ 定时检测配置: 每 {self.check_interval_minutes} 分钟检查新数据，"
                    f"每 {self.monitor_interval_seconds} 秒监控系统状态")

    def start_continuous_monitoring(self):
        """启动持续监控和定时检测系统"""
        logger.info("步骤3: 启动持续监控和定时检测系统")
        logger.info("🔄 系统将保持持续运行状态，定时检测新数据...")
        logger.info("=" * 80)

        monitor_count = 0
        last_check_time = datetime.now()
        next_check_time = last_check_time + timedelta(minutes=self.check_interval_minutes)

        logger.info(f"⏰ 下次新数据检测时间: {next_check_time.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            while True:
                monitor_count += 1
                current_time = datetime.now()

                # 监控系统状态
                self._monitor_system_status(monitor_count)

                # 检查是否到了定时检测新数据的时间
                if current_time >= next_check_time:
                    next_check_time = self._handle_scheduled_check(current_time)

                # 处理任务完成状态
                self._handle_completed_tasks(monitor_count)

                # 显示下次检测倒计时
                self._show_countdown(monitor_count, current_time, next_check_time)

                # 等待下次监控
                time.sleep(self.monitor_interval_seconds)

        except KeyboardInterrupt:
            logger.info("接收到中断信号，正在关闭系统...")
            raise

    def _monitor_system_status(self, monitor_count: int):
        """监控系统状态"""
        system_status = self.system_manager.get_system_status()
        processing_stats = self.data_processor.get_processing_statistics()

        if monitor_count % 5 == 1:
            logger.info(f"🖥️ 系统状态详情: {system_status}")
            logger.info(f"📊 处理统计信息: {processing_stats}")
        else:
            tasks = system_status["tasks"]
            logger.info(f"📋 任务状态 - 待处理:{tasks['pending']}, 运行中:{tasks['running']}, "
                        f"重试:{tasks['retry']}, 已完成:{tasks['completed']}, 失败:{tasks['failed']}")

    def _handle_scheduled_check(self, current_time: datetime) -> datetime:
        """处理定时检测"""
        logger.info("=" * 80)
        logger.info(f"⏰ 定时检测时间到达: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # 执行新数据检测
        has_new_data = self.task_manager.check_and_add_new_data_tasks()

        # 更新下次检测时间
        new_next_check_time = current_time + timedelta(minutes=self.check_interval_minutes)

        if has_new_data:
            logger.info("🆕 发现新数据并已添加到处理队列")
        else:
            logger.info("😴 暂无新数据")

        logger.info(f"⏰ 下次检测时间: {new_next_check_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)

        return new_next_check_time

    def _handle_completed_tasks(self, monitor_count: int):
        """处理已完成的任务"""
        system_status = self.system_manager.get_system_status()

        all_tasks_completed = (
                system_status["tasks"]["pending"] == 0 and
                system_status["tasks"]["running"] == 0 and
                system_status["tasks"]["retry"] == 0
        )

        if all_tasks_completed:
            completed_tasks = system_status["tasks"]["completed"]
            failed_tasks = system_status["tasks"]["failed"]

            if monitor_count % 10 == 1:  # 每10次监控提醒一次任务完成状态
                logger.info("💤 所有任务处理完成，系统保持运行状态等待新数据...")
                logger.info(f"📊 累计统计: 成功 {completed_tasks} 个, 失败 {failed_tasks} 个")

                if failed_tasks > 0:
                    logger.warning("⚠️ 存在失败的任务，请检查日志获取详细信息")

                # 清理完成的任务（保留最近24小时的）
                if monitor_count % 50 == 1:  # 每50次监控清理一次历史任务
                    cleared_count = self.system_manager.clear_completed_tasks(older_than_hours=24)
                    if cleared_count > 0:
                        logger.info(f"🧹 清理了 {cleared_count} 个历史任务")

    def _show_countdown(self, monitor_count: int, current_time: datetime, next_check_time: datetime):
        """显示下次检测倒计时"""
        if monitor_count % 20 == 0:  # 每20次监控显示一次倒计时
            time_until_next_check = next_check_time - current_time
            minutes_left = int(time_until_next_check.total_seconds() / 60)
            logger.info(f"⏳ 距离下次新数据检测还有 {minutes_left} 分钟")
