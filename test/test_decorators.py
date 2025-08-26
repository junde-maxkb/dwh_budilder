import logging
import time
from unittest.mock import patch

import pytest

from common.decorators import retry, log_execution, validate_input, setup_logger


class TestRetryDecorator:
    """测试重试装饰器"""

    def test_retry_success_first_attempt(self):
        """测试第一次尝试就成功的情况"""

        @retry(max_attempts=3, delay=0.1)
        def successful_function():
            return "success"

        result = successful_function()
        assert result == "success"

    def test_retry_success_after_failures(self):
        """测试多次失败后成功的情况"""
        call_count = 0

        @retry(max_attempts=3, delay=0.1)
        def eventually_successful_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"

        result = eventually_successful_function()
        assert result == "success"
        assert call_count == 3

    def test_retry_max_attempts_exceeded(self):
        """测试超过最大重试次数的情况"""

        @retry(max_attempts=2, delay=0.1)
        def always_failing_function():
            raise ValueError("Always fails")

        with pytest.raises(ValueError, match="Always fails"):
            always_failing_function()

    def test_retry_with_specific_exceptions(self):
        """测试指定异常类型的重试"""
        call_count = 0

        @retry(max_attempts=3, delay=0.1, exceptions=(ConnectionError, TimeoutError))
        def network_function():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("Connection failed")
            elif call_count == 2:
                raise TimeoutError("Timeout")
            return "success"

        result = network_function()
        assert result == "success"
        assert call_count == 3

    def test_retry_with_non_retryable_exception(self):
        """测试不在重试异常列表中的异常"""

        @retry(max_attempts=3, delay=0.1, exceptions=(ConnectionError,))
        def function_with_value_error():
            raise ValueError("Not retryable")

        # ValueError不在重试异常列表中，应该立即抛出
        with pytest.raises(ValueError, match="Not retryable"):
            function_with_value_error()

    @patch('common.decorators.time.sleep')
    def test_retry_delay_timing(self, mock_sleep):
        """测试重试延迟时间"""

        @retry(max_attempts=3, delay=2.0)
        def failing_function():
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            failing_function()

        # 验证sleep被调用了2次（第1次和第2次失败后）
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(2.0)


class TestLogExecutionDecorator:
    """测试日志记录装饰器"""

    @patch('common.decorators.logger')
    def test_log_execution_success_without_args(self, mock_logger):
        """测试成功执行的日志记录（不包含参数）"""

        @log_execution()
        def successful_function():
            time.sleep(0.1)
            return "result"

        result = successful_function()
        assert result == "result"

        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        assert "successful_function executed successfully" in call_args
        assert "seconds" in call_args
        assert "args=" not in call_args  # 不应该包含参数信息

    @patch('common.decorators.logger')
    def test_log_execution_success_with_args(self, mock_logger):
        """测试成功执行的日志记录（包含参数）"""

        @log_execution(include_args=True)
        def function_with_params(a, b, c=None):
            return a + b + (c or 0)

        result = function_with_params(1, 2, c=3)
        assert result == 6

        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        assert "function_with_params executed successfully" in call_args
        assert "args=(1, 2)" in call_args
        assert "kwargs={'c': 3}" in call_args

    @patch('common.decorators.logger')
    def test_log_execution_failure(self, mock_logger):
        """测试执行失败的日志记录"""

        @log_execution(include_args=True)
        def failing_function(x):
            time.sleep(0.1)
            raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            failing_function(42)

        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args[0][0]
        assert "failing_function failed after" in call_args
        assert "Test error" in call_args
        assert "args=(42,)" in call_args


class TestValidateInputDecorator:
    """测试输入验证装饰器"""

    def test_validate_input_function_validator_success(self):
        """测试函数验证器成功的情况"""

        def positive_number_validator(*args, **kwargs):
            return all(isinstance(arg, (int, float)) and arg > 0 for arg in args)

        @validate_input(positive_number_validator)
        def add_positive_numbers(a, b):
            return a + b

        result = add_positive_numbers(5, 3)
        assert result == 8

    def test_validate_input_function_validator_failure(self):
        """测试函数验证器失败的情况"""

        def positive_number_validator(*args, **kwargs):
            return all(isinstance(arg, (int, float)) and arg > 0 for arg in args)

        @validate_input(positive_number_validator, "Numbers must be positive")
        def add_positive_numbers(a, b):
            return a + b

        with pytest.raises(ValueError, match="Numbers must be positive"):
            add_positive_numbers(-1, 5)

    def test_validate_input_dict_validator_success(self):
        """测试字典验证器成功的情况"""

        validators = {
            'name': lambda x: isinstance(x, str) and len(x) > 0,
            'age': lambda x: isinstance(x, int) and x >= 0
        }

        @validate_input(validators)
        def create_user(name, age):
            return f"User: {name}, Age: {age}"

        result = create_user("Alice", 30)
        assert result == "User: Alice, Age: 30"

    def test_validate_input_dict_validator_failure(self):
        """测试字典验证器失败的情况"""

        validators = {
            'name': lambda x: isinstance(x, str) and len(x) > 0,
            'age': lambda x: isinstance(x, int) and x >= 0
        }

        @validate_input(validators, "Invalid user data")
        def create_user(name, age):
            return f"User: {name}, Age: {age}"

        with pytest.raises(ValueError, match="Invalid user data: name"):
            create_user("", 30)

        with pytest.raises(ValueError, match="Invalid user data: age"):
            create_user("Alice", -5)

    def test_validate_input_dict_validator_with_kwargs(self):
        """测试字典验证器处理关键字参数"""

        validators = {
            'email': lambda x: isinstance(x, str) and '@' in x,
            'message': lambda x: isinstance(x, str) and len(x) > 0
        }

        @validate_input(validators)
        def send_email(email, message="Hello"):
            return f"Sent '{message}' to {email}"

        result = send_email("test@example.com", message="Custom message")
        assert result == "Sent 'Custom message' to test@example.com"

        # 测试默认参数
        result = send_email("test@example.com")
        assert result == "Sent 'Hello' to test@example.com"

    def test_validate_input_dict_validator_partial_validation(self):
        """测试字典验证器只验证指定参数"""

        validators = {
            'x': lambda x: x > 0
        }

        @validate_input(validators)
        def calculate(x, y):  # y不会被验证
            return x + y

        result = calculate(5, -3)  # y是负数但不会被验证
        assert result == 2

        with pytest.raises(ValueError):
            calculate(-1, 3)  # x是负数，会验证失败


class TestSetupLogger:
    """测试日志设置函数"""

    def test_setup_logger_default_name(self):
        """测试默认名称的日志器设置"""
        logger = setup_logger()
        assert logger.name == 'common.decorators'
        assert logger.level == logging.INFO

    def test_setup_logger_custom_name(self):
        """测试自定义名称的日志器设置"""
        logger = setup_logger('test_logger')
        assert logger.name == 'test_logger'

    def test_setup_logger_no_duplicate_handlers(self):
        """测试不会重复添加处理器"""
        logger1 = setup_logger('unique_logger')
        handler_count_1 = len(logger1.handlers)

        logger2 = setup_logger('unique_logger')
        handler_count_2 = len(logger2.handlers)

        assert handler_count_1 == handler_count_2
        assert logger1 is logger2


class TestDecoratorCombinations:
    """测试装饰器组合使用"""

    @patch('common.decorators.logger')
    def test_retry_and_log_combination(self, mock_logger):
        """测试重试和日志装饰器组合"""
        call_count = 0

        @log_execution()
        @retry(max_attempts=3, delay=0.1)
        def function_with_both_decorators():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Temporary failure")
            return "success"

        result = function_with_both_decorators()
        assert result == "success"
        assert call_count == 2

        # 验证日志被调用（只记录最终成功）
        mock_logger.info.assert_called_once()

    def test_all_decorators_combination(self):
        """测试所有装饰器组合"""

        validators = {'x': lambda x: isinstance(x, (int, float))}
        call_count = 0

        @validate_input(validators)
        @log_execution()
        @retry(max_attempts=2, delay=0.1)
        def complex_function(x):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("First attempt fails")
            return x * 2

        result = complex_function(5)
        assert result == 10
        assert call_count == 2


class TestEdgeCases:
    """测试边界情况"""

    def test_retry_with_zero_delay(self):
        """测试零延迟重试"""
        call_count = 0

        @retry(max_attempts=2, delay=0)
        def quick_retry_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("First attempt fails")
            return "success"

        result = quick_retry_function()
        assert result == "success"
        assert call_count == 2

    def test_retry_with_one_attempt(self):
        """测试只重试一次的情况"""

        @retry(max_attempts=1, delay=0.1)
        def single_attempt_function():
            raise ValueError("Always fails")

        with pytest.raises(ValueError):
            single_attempt_function()

    def test_function_with_no_return_value(self):
        """测试没有返回值的函数"""
        executed = False

        @log_execution()
        def void_function():
            nonlocal executed
            executed = True

        result = void_function()
        assert result is None
        assert executed is True

    def test_empty_validators_dict(self):
        """测试空验证器字典"""

        @validate_input({})
        def any_function(x, y):
            return x + y

        result = any_function(1, 2)
        assert result == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
