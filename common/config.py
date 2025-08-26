import json
import os
from pathlib import Path
from typing import Dict, Any, Optional

class ConfigManager:
    """配置管理类"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or str(Path(__file__).parent.parent / "config.json")
        self._config: Dict[str, Any] = {}
        self.load_config()

    def load_config(self) -> None:
        """加载配置文件"""
        if not os.path.exists(self.config_path):
            self._create_default_config()
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self._config = json.load(f)

    def _create_default_config(self) -> None:
        """创建默认配置文件"""
        default_config = {
            "api": {
                "key": "",
                "base_url": "http://api.example.com",
                "timeout": 30
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "data_warehouse",
                "user": "user",
                "password": ""
            },
            "logging": {
                "level": "INFO",
                "file": "app.log"
            },
            "retry": {
                "max_attempts": 3,
                "delay": 1.0
            }
        }
        
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=4)

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

    def set(self, key: str, value: Any) -> None:
        """设置配置值"""
        keys = key.split('.')
        config = self._config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
        self.save()

    def save(self) -> None:
        """保存配置到文件"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(self._config, f, indent=4)

    @property
    def api_key(self) -> str:
        """获取API密钥"""
        return self.get('api.key', '')

    @api_key.setter
    def api_key(self, value: str) -> None:
        """设置API密钥"""
        self.set('api.key', value)

# 创建全局配置管理器实例
config_manager = ConfigManager()







