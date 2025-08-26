import functools
import logging
import time
from typing import Any, Callable, TypeVar, ParamSpec, Optional, Union

# 类型参数定义
P = ParamSpec('P')
R = TypeVar('R')


def setup_logger(name: Optional[str] = None) -> logging.Logger:
    """设置日志配置，避免全局副作用"""
    logger = logging.getLogger(name or __name__)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


logger = setup_logger()


def retry(max_attempts: int = 3, delay: float = 1.0,
          exceptions: tuple[type[Exception], ...] = (Exception,)):
    """
    重试装饰器
    :param max_attempts: 最大重试次数
    :param delay: 重试间隔（秒）
    :param exceptions: 需要重试的异常类型
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt + 1 == max_attempts:
                        logger.error(f"Function {func.__name__} failed after {max_attempts} attempts. Error: {e}")
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds...")
                    time.sleep(delay)
            # 这行实际上不会执行，但满足类型检查
            raise last_exception or Exception("Unexpected error")

        return wrapper

    return decorator


def log_execution(include_args: bool = False) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    日志记录装饰器
    :param include_args: 是否记录函数参数
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            start_time = time.time()
            args_info = f" with args={args}, kwargs={kwargs}" if include_args else ""

            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(
                    f"Function {func.__name__} executed successfully in {execution_time:.2f} seconds{args_info}")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(
                    f"Function {func.__name__} failed after {execution_time:.2f} seconds{args_info}. Error: {e}")
                raise

        return wrapper

    return decorator


def validate_input(
        validator: Union[Callable[..., bool], dict[str, Callable[[Any], bool]]],
        error_message: str = "Input validation failed"
):
    """
    输入验证装饰器
    :param validator: 验证函数或参数验证字典
    :param error_message: 自定义错误消息
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if callable(validator):
                if not validator(*args, **kwargs):
                    raise ValueError(error_message)
            elif isinstance(validator, dict):
                # 按参数名验证
                import inspect
                sig = inspect.signature(func)
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()

                for param_name, param_validator in validator.items():
                    if param_name in bound_args.arguments:
                        if not param_validator(bound_args.arguments[param_name]):
                            raise ValueError(f"{error_message}: {param_name}")

            return func(*args, **kwargs)

        return wrapper

    return decorator
