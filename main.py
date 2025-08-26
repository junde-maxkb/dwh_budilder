import logging
import sys
import time
from typing import Any, Dict

from core.system_manager import SystemManager
from common.config import ConfigManager
from api.api_client import APIClient
from database.database_manager import DatabaseManager
from utils.data_cleaner import DataCleaner

def setup_logging() -> None:
    """配置日志系统"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('app.log')
        ]
    )

class DataTask:
    """数据任务基类"""
    def __init__(self, config: Dict[str, Any]):
        self.api_client = APIClient(config["api_key"])
        self.db_manager = DatabaseManager(config["database"])
        self.data_cleaner = DataCleaner()
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, data_type: str) -> Dict[str, Any]:
        """执行数据处理任务"""
        try:
            # 1. 从API获取数据
            raw_data = self.api_client.fetch_data(data_type)
            
            # 2. 清洗数据
            cleaned_data = self.data_cleaner.clean(raw_data)
            
            # 3. 存储到数据库
            self.db_manager.save_data(cleaned_data, data_type)
            
            return {
                "status": "success",
                "data_type": data_type,
                "records_processed": len(cleaned_data)
            }
            
        except Exception as e:
            self.logger.error(f"Error processing {data_type} data: {str(e)}")
            raise

def main():
    """主函数"""
    try:
        # 初始化配置和日志
        setup_logging()
        config = ConfigManager()
        logger = logging.getLogger(__name__)
        
        # 创建系统管理器
        system = SystemManager()
        
        # 创建数据任务处理器
        data_task = DataTask(config.get("api"))
        
        # 添加数据处理任务
        data_types = ["type1", "type2", "type3"]
        for data_type in data_types:
            system.add_task(
                name=f"process_{data_type}",
                func=data_task.execute,
                args=(data_type,)
            )
        
        # 启动系统
        system.start()
        
        try:
            while True:
                # 定期检查系统状态
                status = system.get_system_status()
                logger.info(f"System status: {status}")
                
                # 如果所有任务都完成了，可以添加新的任务
                if status["tasks"]["pending"] == 0 and status["tasks"]["running"] == 0:
                    # 这里可以添加新的任务
                    pass
                
                time.sleep(60)  # 每分钟检查一次
                
        except KeyboardInterrupt:
            logger.info("Shutting down system...")
            system.stop()
            
    except Exception as e:
        logger.error(f"System error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()