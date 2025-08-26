import functools
import logging
import time
from typing import Any, Callable, TypeVar, ParamSpec

# 类型参数定义
P = ParamSpec('P')
R = TypeVar('R')

def setup_logger():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

logger = setup_logger()

def retry(max_attempts: int = 3, delay: float = 1.0):
    """
    重试装饰器
    :param max_attempts: 最大重试次数
    :param delay: 重试间隔（秒）
    """
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        logger.error(f"Function {func.__name__} failed after {max_attempts} attempts. Error: {str(e)}")
                        raise
                    logger.warning(f"Attempt {attempts} failed. Retrying in {delay} seconds...")
                    time.sleep(delay)
            return None  # type: ignore
        return wrapper
    return decorator

def log_execution(func: Callable[P, R]) -> Callable[P, R]:
    """
    日志记录装饰器
    记录函数的执行时间和结果
    """
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"Function {func.__name__} executed successfully in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Function {func.__name__} failed after {execution_time:.2f} seconds. Error: {str(e)}")
            raise
    return wrapper

def validate_input(validator: Callable[[Any], bool]):
    """
    输入验证装饰器
    :param validator: 验证函数
    """
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if not validator(*args, **kwargs):
                raise ValueError("Input validation failed")
            return func(*args, **kwargs)
        return wrapper
    return decorator







