import logging
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
from queue import Queue
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from enum import Enum

from common.config import ConfigManager
from common.decorators import log_execution

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"

@dataclass
class TaskResult:
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    retry_count: int = 0
    timestamp: datetime = datetime.now()

class Task:
    def __init__(self, name: str, func: callable, args: tuple = (), kwargs: dict = None):
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs or {}
        self.status = TaskStatus.PENDING
        self.result: Optional[TaskResult] = None
        self.max_retries = 3
        self.retry_count = 0
        self.last_run = None
        self.next_retry = None

class SystemManager:
    def __init__(self):
        self.config = ConfigManager()
        self.logger = logging.getLogger(__name__)
        self.tasks: Dict[str, Task] = {}
        self.task_queue: Queue = Queue()
        self.results: Dict[str, TaskResult] = {}
        self.lock = Lock()
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.running = True
        self.health_status = {"status": "healthy", "last_check": datetime.now()}

    def add_task(self, name: str, func: callable, args: tuple = (), kwargs: dict = None) -> None:
        """添加新任务到系统"""
        with self.lock:
            task = Task(name, func, args, kwargs)
            self.tasks[name] = task
            self.task_queue.put(task)
            self.logger.info(f"Task '{name}' added to queue")

    @log_execution
    def execute_task(self, task: Task) -> TaskResult:
        """执行单个任务"""
        task.status = TaskStatus.RUNNING
        task.last_run = datetime.now()

        try:
            result = task.func(*task.args, **(task.kwargs or {}))
            task.status = TaskStatus.COMPLETED
            return TaskResult(success=True, data=result)
        except Exception as e:
            self.logger.error(f"Task '{task.name}' failed: {str(e)}")
            task.status = TaskStatus.FAILED
            return TaskResult(success=False, error=str(e))

    def handle_failed_task(self, task: Task, result: TaskResult) -> None:
        """处理失败的任务"""
        if task.retry_count < task.max_retries:
            task.retry_count += 1
            task.status = TaskStatus.RETRY
            task.next_retry = datetime.now()  # 可以添加延迟时间
            self.task_queue.put(task)
            self.logger.info(f"Task '{task.name}' scheduled for retry {task.retry_count}/{task.max_retries}")
        else:
            self.logger.error(f"Task '{task.name}' failed permanently after {task.max_retries} retries")

    def process_tasks(self) -> None:
        """处理任务队列"""
        while self.running:
            try:
                task = self.task_queue.get(timeout=1)  # 1秒超时
                result = self.execute_task(task)
                
                with self.lock:
                    self.results[task.name] = result
                    
                if not result.success:
                    self.handle_failed_task(task, result)
                    
                self.task_queue.task_done()
                
            except Queue.Empty:
                continue  # 队列为空，继续等待
            except Exception as e:
                self.logger.error(f"Error processing task queue: {str(e)}")
                continue  # 继续处理其他任务

    def start(self) -> None:
        """启动系统"""
        self.logger.info("Starting system manager...")
        self.running = True
        self.executor.submit(self.process_tasks)
        self.executor.submit(self.health_check_loop)

    def stop(self) -> None:
        """停止系统"""
        self.logger.info("Stopping system manager...")
        self.running = False
        self.executor.shutdown(wait=True)

    def get_task_status(self, task_name: str) -> Dict[str, Any]:
        """获取任务状态"""
        task = self.tasks.get(task_name)
        if not task:
            return {"error": "Task not found"}
            
        return {
            "name": task.name,
            "status": task.status.value,
            "retry_count": task.retry_count,
            "last_run": task.last_run,
            "next_retry": task.next_retry,
            "result": self.results.get(task_name)
        }

    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        return {
            "health": self.health_status,
            "tasks": {
                "total": len(self.tasks),
                "pending": sum(1 for t in self.tasks.values() if t.status == TaskStatus.PENDING),
                "running": sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING),
                "completed": sum(1 for t in self.tasks.values() if t.status == TaskStatus.COMPLETED),
                "failed": sum(1 for t in self.tasks.values() if t.status == TaskStatus.FAILED),
            },
            "queue_size": self.task_queue.qsize()
        }

    def health_check_loop(self) -> None:
        """健康检查循环"""
        while self.running:
            try:
                # 执行健康检查
                self.health_status = {
                    "status": "healthy",
                    "last_check": datetime.now(),
                    "queue_size": self.task_queue.qsize(),
                    "active_tasks": len([t for t in self.tasks.values() if t.status == TaskStatus.RUNNING])
                }
                time.sleep(60)  # 每分钟检查一次
            except Exception as e:
                self.logger.error(f"Health check failed: {str(e)}")
                self.health_status = {
                    "status": "unhealthy",
                    "last_check": datetime.now(),
                    "error": str(e)
                }







