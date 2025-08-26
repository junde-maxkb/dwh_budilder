import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from queue import PriorityQueue, Empty
from threading import Lock, Event
from typing import Dict, Optional, Any, Callable

from common.config import ConfigManager
from common.decorators import log_execution


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"
    CANCELLED = "cancelled"


@dataclass
class TaskResult:
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)  # 修正：使用 field(default_factory)
    execution_time: Optional[float] = None


class Task:
    def __init__(self, name: str, func: Callable, args: tuple = (), kwargs: dict = None,
                 max_retries: int = 3, priority: int = 0):
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs or {}
        self.status = TaskStatus.PENDING
        self.result: Optional[TaskResult] = None
        self.max_retries = max_retries
        self.retry_count = 0
        self.priority = priority
        self.created_at = datetime.now()
        self.last_run: Optional[datetime] = None
        self.next_retry: Optional[datetime] = None
        self.lock = Lock()
        self.task_id = id(self)  # 添加唯一标识符用于排序

    def __lt__(self, other):
        """用于优先级队列排序 - 修正：添加稳定排序"""
        if self.priority != other.priority:
            return self.priority > other.priority  # 高优先级在前
        return self.task_id < other.task_id  # 相同优先级按创建顺序


class PriorityTaskWrapper:
    """包装类用于优先级队列"""

    def __init__(self, task: Task, schedule_time: datetime = None):
        self.task = task
        self.schedule_time = schedule_time or datetime.now()

    def __lt__(self, other):
        # 首先按调度时间排序
        if self.schedule_time != other.schedule_time:
            return self.schedule_time < other.schedule_time
        # 然后按任务优先级排序
        return self.task < other.task


class SystemManager:
    def __init__(self, max_workers: int = 5, health_check_interval: int = 60):
        self.config = ConfigManager()
        self.logger = logging.getLogger(__name__)
        self.tasks: Dict[str, Task] = {}
        self.task_queue: PriorityQueue = PriorityQueue()  # 修正：使用优先级队列
        self.results: Dict[str, TaskResult] = {}
        self.lock = Lock()
        self.max_workers = max_workers  # 修正：保存参数避免访问私有属性
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = False
        self.shutdown_event = Event()
        self.health_check_interval = health_check_interval
        self.health_status = {
            "status": "healthy",
            "last_check": datetime.now(),
            "startup_time": datetime.now()
        }
        self.active_tasks_count = 0  # 修正：手动跟踪活跃任务数

    def add_task(self, name: str, func: Callable, args: tuple = (),
                 kwargs: dict = None, max_retries: int = 3, priority: int = 0) -> bool:
        """添加新任务到系统"""
        try:
            with self.lock:
                if name in self.tasks:
                    self.logger.warning(f"Task '{name}' already exists, skipping")
                    return False

                task = Task(name, func, args, kwargs, max_retries, priority)
                self.tasks[name] = task

                # 修正：使用包装类添加到优先级队列
                wrapper = PriorityTaskWrapper(task)
                self.task_queue.put(wrapper)

                self.logger.info(f"Task '{name}' added to queue with priority {priority}")
                return True
        except Exception as e:
            self.logger.error(f"Failed to add task '{name}': {str(e)}")
            return False

    @log_execution(include_args=True)
    def execute_task(self, task: Task) -> TaskResult:
        """执行单个任务"""
        start_time = time.time()

        with task.lock:
            task.status = TaskStatus.RUNNING
            task.last_run = datetime.now()

        # 增加活跃任务计数
        with self.lock:
            self.active_tasks_count += 1

        try:
            result_data = task.func(*task.args, **task.kwargs)
            execution_time = time.time() - start_time

            with task.lock:
                task.status = TaskStatus.COMPLETED

            self.logger.info(f"Task '{task.name}' completed successfully in {execution_time:.2f}s")
            return TaskResult(
                success=True,
                data=result_data,
                execution_time=execution_time
            )

        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = str(e)

            self.logger.error(f"Task '{task.name}' failed after {execution_time:.2f}s: {error_msg}")

            with task.lock:
                task.status = TaskStatus.FAILED

            return TaskResult(
                success=False,
                error=error_msg,
                execution_time=execution_time
            )
        finally:
            # 减少活跃任务计数
            with self.lock:
                self.active_tasks_count -= 1

    def handle_failed_task(self, task: Task) -> None:
        """处理失败的任务，使用指数退避重试"""
        with task.lock:
            if task.retry_count < task.max_retries and not self.shutdown_event.is_set():
                task.retry_count += 1
                task.status = TaskStatus.RETRY

                # 指数退避：2^retry_count 秒，最大300秒
                delay_seconds = min(2 ** task.retry_count, 300)
                retry_time = datetime.now() + timedelta(seconds=delay_seconds)
                task.next_retry = retry_time

                # 修正：直接放入队列，避免使用executor调度
                wrapper = PriorityTaskWrapper(task, retry_time)

                try:
                    if not self.shutdown_event.is_set():
                        self.task_queue.put(wrapper)
                        self.logger.info(
                            f"Task '{task.name}' scheduled for retry {task.retry_count}/{task.max_retries} "
                            f"at {retry_time.strftime('%H:%M:%S')}"
                        )
                    else:
                        # 系统正在关闭，标记任务为失败
                        task.status = TaskStatus.FAILED
                        self.logger.warning(
                            f"Task '{task.name}' retry cancelled due to system shutdown"
                        )
                except Exception as e:
                    task.status = TaskStatus.FAILED
                    self.logger.error(f"Failed to schedule retry for task '{task.name}': {str(e)}")
            else:
                task.status = TaskStatus.FAILED  # 确保状态正确
                if self.shutdown_event.is_set():
                    self.logger.warning(
                        f"Task '{task.name}' failed - no retry due to system shutdown"
                    )
                else:
                    self.logger.error(
                        f"Task '{task.name}' failed permanently after {task.max_retries} retries"
                    )

    def process_tasks(self) -> None:
        """处理任务队列"""
        self.logger.info("Task processor started")

        while self.running and not self.shutdown_event.is_set():
            try:
                # 修正：从优先级队列获取包装对象
                wrapper = self.task_queue.get(timeout=1)
                task = wrapper.task

                # 检查任务是否已被取消
                if task.status == TaskStatus.CANCELLED:
                    self.task_queue.task_done()
                    continue

                # 修正：检查重试任务是否到时间
                current_time = datetime.now()
                if (task.status == TaskStatus.RETRY and
                        wrapper.schedule_time and wrapper.schedule_time > current_time):
                    # 重新放回队列等待
                    self.task_queue.put(wrapper)
                    self.task_queue.task_done()
                    time.sleep(0.1)  # 短暂休息避免忙等待
                    continue

                # 执行任务
                result: TaskResult = self.execute_task(task)

                # 保存结果
                with self.lock:
                    self.results[task.name] = result

                # 处理失败任务
                if not result.success:
                    self.handle_failed_task(task)

                self.task_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error processing task queue: {str(e)}")
                continue

        self.logger.info("Task processor stopped")

    def start(self) -> None:
        """启动系统"""
        if self.running:
            self.logger.warning("System manager is already running")
            return

        self.logger.info("Starting system manager...")
        self.running = True
        self.shutdown_event.clear()

        # 启动工作线程
        self.executor.submit(self.process_tasks)
        self.executor.submit(self.health_check_loop)

        self.logger.info("System manager started successfully")

    def stop(self, timeout: int = 30) -> None:
        """优雅停止系统"""
        if not self.running:
            self.logger.warning("System manager is not running")
            return

        self.logger.info("Stopping system manager...")
        self.running = False
        self.shutdown_event.set()

        # 等待当前正在执行的任务完成
        start_time = time.time()
        while self.active_tasks_count > 0 and time.time() - start_time < timeout / 2:
            time.sleep(0.1)

        # 等待任务队列处理完成
        try:
            queue_wait_time = timeout / 2
            start_time = time.time()
            while not self.task_queue.empty() and time.time() - start_time < queue_wait_time:
                time.sleep(0.1)
        except Exception as e:
            self.logger.warning(f"Error waiting for task queue: {str(e)}")

        # 关闭线程池
        try:
            self.executor.shutdown(wait=True)
            self.logger.info("System manager stopped gracefully")
        except Exception as e:
            self.logger.error(f"Error during executor shutdown: {str(e)}")
            self.logger.info("System manager stopped with errors")

    def cancel_task(self, task_name: str) -> bool:
        """取消任务"""
        with self.lock:
            task = self.tasks.get(task_name)
            if not task:
                return False

            with task.lock:
                if task.status in [TaskStatus.PENDING, TaskStatus.RETRY]:
                    task.status = TaskStatus.CANCELLED
                    self.logger.info(f"Task '{task_name}' cancelled")
                    return True
                else:
                    self.logger.warning(f"Cannot cancel task '{task_name}' in status {task.status.value}")
                    return False

    def get_task_status(self, task_name: str) -> Dict[str, Any]:
        """获取任务状态"""
        task = self.tasks.get(task_name)
        if not task:
            return {"error": "Task not found"}

        with task.lock:
            result_data = None
            if task_name in self.results:
                result = self.results[task_name]
                result_data = {
                    "success": result.success,
                    "data": result.data,
                    "error": result.error,
                    "timestamp": result.timestamp.isoformat(),
                    "execution_time": result.execution_time
                }

            return {
                "name": task.name,
                "status": task.status.value,
                "retry_count": task.retry_count,
                "max_retries": task.max_retries,
                "priority": task.priority,
                "created_at": task.created_at.isoformat(),
                "last_run": task.last_run.isoformat() if task.last_run else None,
                "next_retry": task.next_retry.isoformat() if task.next_retry else None,
                "result": result_data
            }

    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        with self.lock:
            task_stats = {
                "total": len(self.tasks),
                "pending": 0,
                "running": 0,
                "completed": 0,
                "failed": 0,
                "retry": 0,
                "cancelled": 0
            }

            for task in self.tasks.values():
                task_stats[task.status.value] += 1

        return {
            "running": self.running,
            "health": self.health_status,
            "tasks": task_stats,
            "queue_size": self.task_queue.qsize(),
            "executor_status": {
                "max_workers": self.max_workers,  # 修正：使用保存的值
                "active_tasks": self.active_tasks_count  # 修正：使用手动跟踪的值
            }
        }

    def health_check_loop(self) -> None:
        """健康检查循环"""
        self.logger.info("Health check loop started")

        while self.running and not self.shutdown_event.is_set():
            try:
                # 检查系统健康状态
                queue_size = self.task_queue.qsize()

                with self.lock:
                    active_tasks = self.active_tasks_count

                # 检查是否有长时间运行的任务
                long_running_tasks = []
                current_time = datetime.now()
                with self.lock:
                    for task in self.tasks.values():
                        with task.lock:
                            if (task.status == TaskStatus.RUNNING and task.last_run and
                                    (current_time - task.last_run).total_seconds() > 300):  # 5分钟
                                long_running_tasks.append(task.name)

                status = "healthy"
                if queue_size > 100:  # 队列积压过多
                    status = "warning"
                if long_running_tasks:
                    status = "warning"

                self.health_status = {
                    "status": status,
                    "last_check": current_time,
                    "startup_time": self.health_status["startup_time"],
                    "queue_size": queue_size,
                    "active_tasks": active_tasks,
                    "long_running_tasks": long_running_tasks
                }

                if status == "warning":
                    self.logger.warning(f"System health warning: {self.health_status}")

            except Exception as e:
                self.logger.error(f"Health check failed: {str(e)}")
                self.health_status = {
                    "status": "unhealthy",
                    "last_check": datetime.now(),
                    "startup_time": self.health_status.get("startup_time", datetime.now()),
                    "error": str(e)
                }

            # 使用shutdown_event来实现可中断的睡眠
            if self.shutdown_event.wait(self.health_check_interval):
                break

        self.logger.info("Health check loop stopped")

    def clear_completed_tasks(self, older_than_hours: int = 24) -> int:
        """清理完成的任务"""
        cutoff_time = datetime.now() - timedelta(hours=older_than_hours)
        cleared_count = 0

        with self.lock:
            tasks_to_remove = []
            for name, task in self.tasks.items():
                with task.lock:
                    if (task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]
                            and task.last_run and task.last_run < cutoff_time):
                        tasks_to_remove.append(name)

            for name in tasks_to_remove:
                del self.tasks[name]
                if name in self.results:
                    del self.results[name]
                cleared_count += 1

        if cleared_count > 0:
            self.logger.info(f"Cleared {cleared_count} completed tasks older than {older_than_hours} hours")

        return cleared_count


if __name__ == '__main__':
    # 定义一个示例任务
    def example_task(x, y):
        time.sleep(1)  # 模拟耗时
        return x + y


    def failing_task():
        time.sleep(1)
        raise ValueError("模拟任务失败")


    manager = SystemManager(max_workers=3, health_check_interval=10)

    # 启动系统
    manager.start()

    # 添加任务
    manager.add_task("task1", example_task, args=(1, 2), priority=5)
    manager.add_task("task2", example_task, args=(10, 20), priority=1)
    manager.add_task("task_fail", failing_task, priority=3)

    # 等待一会儿让任务执行
    time.sleep(5)

    # 获取任务状态
    print("task1 状态:", manager.get_task_status("task1"))
    print("task_fail 状态:", manager.get_task_status("task_fail"))

    # 查看系统状态
    print("系统状态:", manager.get_system_status())

    # 清理24小时前完成的任务
    cleared = manager.clear_completed_tasks(older_than_hours=1)
    print(f"清理掉 {cleared} 个历史任务")

    # 停止系统
    manager.stop()
