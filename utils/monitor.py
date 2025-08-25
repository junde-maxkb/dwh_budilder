import functools
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Optional
from loguru import logger


class ExecutionStatus(Enum):
    """执行状态枚举类"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"


@dataclass
class ExecutionMetrics:
    """执行指标数据类，记录函数执行的各种指标"""
    function_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    status: ExecutionStatus = ExecutionStatus.PENDING
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    input_size: Optional[int] = None
    output_size: Optional[int] = None
    memory_usage: Optional[float] = None
    extra_data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式便于序列化和存储"""
        return {
            'function_name': self.function_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration': self.duration,
            'status': self.status.value,
            'error_message': self.error_message,
            'error_traceback': self.error_traceback,
            'input_size': self.input_size,
            'output_size': self.output_size,
            'memory_usage': self.memory_usage,
            'extra_data': self.extra_data
        }


class ProcessMonitor:
    """监控器单例类"""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """初始化监控器"""
        if not hasattr(self, 'initialized'):
            self.execution_history: Dict[str, ExecutionMetrics] = {}
            self.current_executions: Dict[str, ExecutionMetrics] = {}
            self.statistics = {
                'total_executions': 0,
                'successful_executions': 0,
                'failed_executions': 0,
                'total_duration': 0.0,
                'average_duration': 0.0
            }
            self.initialized = True

    def start_execution(self, function_name: str, input_data: Any = None) -> str:
        """
        开始执行监控
        Args:
            function_name: 函数名称
            input_data: 输入数据，用于计算数据大小
        Returns:
            execution_id: 执行ID，用于后续跟踪
        """
        execution_id = f"{function_name}_{int(time.time() * 1000)}"

        metrics = ExecutionMetrics(
            function_name=function_name,
            start_time=datetime.now(),
            status=ExecutionStatus.RUNNING
        )

        if input_data is not None:
            try:
                if hasattr(input_data, '__len__'):
                    metrics.input_size = len(input_data)
                elif isinstance(input_data, (str, bytes)):
                    metrics.input_size = len(input_data)
            except Exception as e:
                logger.warning(f"计算输入数据大小时出错: {str(e)}")
                pass

        self.current_executions[execution_id] = metrics
        logger.info(f"开始监控执行: {function_name} [ID: {execution_id}]")
        return execution_id

    def end_execution(self, execution_id: str, result: Any = None, error: Exception = None):
        """
        结束执行监控
        Args:
            execution_id: 执行ID
            result: 执行结果
            error: 执行错误（如果有）
        """
        if execution_id not in self.current_executions:
            logger.warning(f"执行ID {execution_id} 不存在于当前执行中")
            return

        metrics = self.current_executions[execution_id]
        metrics.end_time = datetime.now()

        metrics.duration = (metrics.end_time - metrics.start_time).total_seconds()

        if result is not None:
            try:
                if hasattr(result, '__len__'):
                    metrics.output_size = len(result)
                elif isinstance(result, (str, bytes)):
                    metrics.output_size = len(result)
            except Exception as e:
                logger.warning(f"计算输出数据大小时出错: {str(e)}")
                pass

        if error:
            metrics.status = ExecutionStatus.FAILED
            metrics.error_message = str(error)
            metrics.error_traceback = traceback.format_exc()
            logger.error(f"执行失败: {metrics.function_name} [ID: {execution_id}] - {error}")
        else:
            metrics.status = ExecutionStatus.SUCCESS
            logger.info(f"执行成功: {metrics.function_name} [ID: {execution_id}] - 耗时: {metrics.duration:.2f}s")

        self.execution_history[execution_id] = metrics
        del self.current_executions[execution_id]

        self._update_statistics(metrics)

    def _update_statistics(self, metrics: ExecutionMetrics):
        """
        更新统计信息
        Args:
            metrics: 执行指标
        """
        self.statistics['total_executions'] += 1
        self.statistics['total_duration'] += metrics.duration or 0

        if metrics.status == ExecutionStatus.SUCCESS:
            self.statistics['successful_executions'] += 1
        elif metrics.status == ExecutionStatus.FAILED:
            self.statistics['failed_executions'] += 1

        if self.statistics['total_executions'] > 0:
            self.statistics['average_duration'] = (
                    self.statistics['total_duration'] / self.statistics['total_executions']
            )

    def get_statistics(self) -> Dict[str, Any]:
        """
        获取统计信息
        Returns:
            包含各种统计指标的字典
        """
        return {
            **self.statistics,
            'current_running': len(self.current_executions),
            'success_rate': (
                    self.statistics['successful_executions'] / max(self.statistics['total_executions'], 1) * 100
            )
        }

    def get_execution_history(self, limit: int = 50) -> list[Dict[str, Any]]:
        """
        获取执行历史记录
        Args:
            limit: 返回记录的最大数量
        Returns:
            执行历史记录列表（按时间倒序）
        """
        history = list(self.execution_history.values())

        history.sort(key=lambda x: x.start_time, reverse=True)
        return [metrics.to_dict() for metrics in history[:limit]]

    def clear_history(self):

        self.execution_history.clear()
        logger.info("执行历史记录已清空")


monitor = ProcessMonitor()


def execution_monitor(
        stage: str = "unknown",
        timeout: Optional[float] = None,
        track_memory: bool = False,
        extra_data: Optional[Dict[str, Any]] = None
):
    """
    执行监控装饰器 - 自动监控被装饰函数的执行情况
    Args:
        stage: 执行阶段名称（如：data_fetch, data_clean, data_store）
        timeout: 超时时间（秒），超过此时间将抛出超时异常
        track_memory: 是否追踪内存使用情况
        extra_data: 额外的监控数据
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            function_name = f"{stage}:{func.__name__}"
            execution_id = monitor.start_execution(function_name, args[0] if args else None)

            if extra_data:
                monitor.current_executions[execution_id].extra_data.update(extra_data)

            result = None
            error = None
            timer = None

            try:
                # 内存监控初始化
                if track_memory:
                    import psutil
                    process = psutil.Process()
                    initial_memory = process.memory_info().rss / 1024 / 1024  # 转换为MB

                # 超时控制
                if timeout:
                    import platform

                    if platform.system() == "Windows":
                        timeout_flag = threading.Event()

                        def timeout_handler():
                            timeout_flag.set()

                        timer = threading.Timer(timeout, timeout_handler)
                        timer.start()

                        result = func(*args, **kwargs)

                        if timeout_flag.is_set():
                            raise TimeoutError(f"函数 {function_name} 执行超时 ({timeout}s)")
                    else:
                        # Linux系统使用信号机制
                        import signal

                        def timeout_handler(signum, frame):
                            raise TimeoutError(f"函数 {function_name} 执行超时 ({timeout}s)")

                        signal.signal(signal.SIGALRM, timeout_handler)
                        signal.alarm(int(timeout))
                        result = func(*args, **kwargs)
                        signal.alarm(0)
                else:
                    result = func(*args, **kwargs)

                if track_memory:
                    final_memory = process.memory_info().rss / 1024 / 1024  # MB
                    monitor.current_executions[execution_id].memory_usage = final_memory - initial_memory

            except Exception as e:
                error = e

                if timeout:
                    if timer:
                        timer.cancel()
                    elif platform.system() != "Windows":
                        import signal
                        signal.alarm(0)
            finally:

                if timer:
                    timer.cancel()

                monitor.end_execution(execution_id, result, error)

            if error:
                raise error
            return result

        return wrapper

    return decorator


def pipeline_monitor(pipeline_name: str):
    """
    执行流程监控装饰器 - 用于监控整个数据处理整个流程的执行情况
    Args:
        pipeline_name: 流水线名称
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"开始执行数据流水线: {pipeline_name}")
            pipeline_start = time.time()

            try:
                # 执行流程函数
                result = func(*args, **kwargs)
                pipeline_duration = time.time() - pipeline_start

                # 获取执行统计信息
                stats = monitor.get_statistics()

                logger.info(
                    f"数据流程 {pipeline_name} 执行完成！\n"
                    f"总耗时: {pipeline_duration:.2f}s\n"
                    f"执行步骤: {stats['total_executions']}\n"
                    f"成功率: {stats['success_rate']:.1f}%\n"
                    f"平均步骤耗时: {stats['average_duration']:.2f}s"
                )

                return result

            except Exception as e:
                # 记录失败日志
                pipeline_duration = time.time() - pipeline_start
                logger.error(
                    f"数据流水线 {pipeline_name} 执行失败！\n"
                    f"耗时: {pipeline_duration:.2f}s\n"
                    f"错误: {str(e)}"
                )
                raise

        return wrapper

    return decorator


def retry_with_monitor(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    带监控的重试装饰器 - 自动重试失败的函数执行
    Args:
        max_retries: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff: 退避因子，每次重试延迟时间乘以此因子
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        wait_time = delay * (backoff ** (attempt - 1))  # 指数退避
                        logger.warning(f"重试 {func.__name__} (第{attempt}次)，等待 {wait_time:.1f}s")
                        time.sleep(wait_time)

                    return func(*args, **kwargs)

                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"{func.__name__} 执行失败，准备重试: {str(e)}")
                    else:
                        logger.error(f"{func.__name__} 重试{max_retries}次后仍然失败: {str(e)}")

            # 重试次数用完后抛出最后一个异常
            raise last_exception

        return wrapper

    return decorator
