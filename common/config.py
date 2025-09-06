import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


class ConfigManager:
    """配置管理类"""

    def __init__(self, config_path: Optional[str] = None, env_path: Optional[str] = None):
        self.config_path = config_path or str(Path(__file__).parent.parent / "config.json")
        self.env_path = env_path or str(Path(__file__).parent.parent / ".env")
        self._config: Dict[str, Any] = {}
        self.load_config()

    def load_config(self) -> None:
        """加载配置文件"""
        # 加载 .env 文件
        if os.path.exists(self.env_path):
            load_dotenv(self.env_path)

        # 用环境变量覆盖配置
        self._override_with_env()

    def _override_with_env(self) -> None:
        """用环境变量覆盖配置"""
        env_mappings = {
            # API配置
            'API_BASE_URL': 'api.base_url',
            'APP_KEY': 'api.app_key',
            'APP_SECRET': 'api.app_secret',

            # 财务报表API配置
            'USERNAME': 'financial_api.username',
            'PASSWORD': 'financial_api.password',

            # 数据库配置
            'DB_HOST': 'database.host',
            'DB_PORT': 'database.port',
            'DB_USERNAME': 'database.username',
            'DB_PASSWORD': 'database.password',
            'DB_TENANT': 'database.tenant',
            'DB_DATABASE': 'database.database',
        }

        for env_key, config_key in env_mappings.items():
            env_value = os.getenv(env_key)
            if env_value is not None:
                # 类型转换
                if config_key in ['database.port', 'server1.port', 'server2.port']:
                    env_value = int(env_value)

                self._set_nested_value(config_key, env_value)

    def _set_nested_value(self, key: str, value: Any) -> None:
        """设置嵌套配置值（不保存到文件）"""
        keys = key.split('.')
        config = self._config

        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]

        config[keys[-1]] = value

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        keys = key.split('.')
        value = self._config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default

        return value


# 创建全局配置管理器实例
config_manager = ConfigManager()

if __name__ == '__main__':
    # 获取API配置
    api_base_url = config_manager.get('api.base_url')
    app_key = config_manager.get('api.app_key')
    app_secret = config_manager.get('api.app_secret')
    db_host = config_manager.get('database.host')
    db_port = config_manager.get('database.port')
    print(f"API Base URL: {api_base_url}")
    print(f"App Key: {app_key}")
    print(f"App Secret: {app_secret}")
    print(f"DB Host: {db_host}")
    print(f"DB Port: {db_port}")
