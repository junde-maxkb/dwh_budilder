import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Any, Optional
from api.api_client import FinanceAPIClient
from core.system_manager import SystemManager
from database.database_manager import DataBaseManager
from utils.data_cleaner import DataCleaner
from utils.monitor import execution_monitor


@dataclass
class ProcessingResult:
    """数据处理结果"""
    success: bool
    data_type: str
    original_count: int
    cleaned_count: int
    saved_count: int
    processing_time: float
    error_message: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class DataProcessor:
    """
    数据处理组件 - 整合API调用、数据库存储、数据清洗和任务队列管理

    功能流程：
    1. 通过API客户端获取原始数据
    2. 将原始数据存储到数据库
    3. 使用数据清洗器清洗数据
    4. 将清洗后的数据更新到数据库
    5. 通过系统管理器管理任务队列
    """

    def __init__(self, api_config: Dict[str, str], db_manager: DataBaseManager = None):
        """
        初始化数据处理器

        Args:
            api_config: API配置，包含base_url, app_key, app_secret
            db_manager: 数据库管理器实例，如果为None则创建新实例
        """
        self.logger = logging.getLogger(__name__)

        # 初始化各个组件
        self.api_client = FinanceAPIClient(
            base_url=api_config['base_url'],
            app_key=api_config['app_key'],
            app_secret=api_config['app_secret']
        )

        self.db_manager = db_manager or DataBaseManager()
        self.data_cleaner = DataCleaner()

        # 数据类型与API方法的映射
        self.api_methods = {
            'account_structure': self.api_client.get_account_structure,
            'subject_dimension': self.api_client.get_subject_dimension_relationship,
            'customer_vendor': self.api_client.get_customer_vendor_dict,
            'voucher_list': self.api_client.get_voucher_list,
            'voucher_detail': self.api_client.get_voucher_detail,
            'voucher_dim_detail': self.api_client.get_voucher_dim_detail,
            'balance': self.api_client.get_balance,
            'aux_balance': self.api_client.get_aux_balance
        }

        # 数据清洗方法映射
        self.cleaning_methods = {
            'account_structure': self.data_cleaner.clean_account_structure,
            'subject_dimension': self.data_cleaner.clean_subject_dimension,
            'customer_vendor': self.data_cleaner.clean_customer_vendor,
            'voucher_list': self.data_cleaner.clean_voucher_list,
            'voucher_detail': self.data_cleaner.clean_voucher_detail,
            'voucher_dim_detail': self.data_cleaner.clean_voucher_detail,  # 使用相同的清洗方法
            'balance': lambda data: self.data_cleaner.clean_balance_data(data, 'balance'),
            'aux_balance': lambda data: self.data_cleaner.clean_balance_data(data, 'aux_balance')
        }

    @execution_monitor(stage="data_processing", track_memory=True)
    def process_data(self, data_type: str, company_code: str, **kwargs) -> ProcessingResult:
        """
        处理单个数据类型的完整流程

        Args:
            data_type: 数据类型
            company_code: 公司代码
            **kwargs: 其他参数（如年份、期间等）

        Returns:
            ProcessingResult: 处理结果
        """
        start_time = datetime.now()

        try:
            self.logger.info(f"开始处理 {data_type} 数据，公司代码: {company_code}")

            # 1. 通过API获取原始数据
            raw_data = self._fetch_api_data(data_type, company_code, **kwargs)
            if not raw_data:
                return ProcessingResult(
                    success=False,
                    data_type=data_type,
                    original_count=0,
                    cleaned_count=0,
                    saved_count=0,
                    processing_time=0,
                    error_message="API返回空数据"
                )

            original_count = len(raw_data)
            self.logger.info(f"从API获取到 {original_count} 条原始数据")

            # # 2. 存储原始数据到数据库
            # self._save_raw_data(raw_data, data_type, company_code)
            # self.logger.info(f"原始数据已存储到数据库")
            #
            # # 3. 清洗数据
            # cleaned_data = self._clean_data(raw_data, data_type)
            # cleaned_count = len(cleaned_data) if cleaned_data is not None else 0
            # self.logger.info(f"数据清洗完成，得到 {cleaned_count} 条清洗后数据")
            #
            # # 4. 存储清洗后的数据
            # saved_count = self._save_cleaned_data(cleaned_data, data_type, company_code)
            # self.logger.info(f"清洗后数据已存储到数据库，实际保存 {saved_count} 条")
            #
            # processing_time = (datetime.now() - start_time).total_seconds()
            #
            # return ProcessingResult(
            #     success=True,
            #     data_type=data_type,
            #     original_count=original_count,
            #     cleaned_count=cleaned_count,
            #     saved_count=saved_count,
            #     processing_time=processing_time
            # )

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"处理 {data_type} 数据时发生错误: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            return ProcessingResult(
                success=False,
                data_type=data_type,
                original_count=0,
                cleaned_count=0,
                saved_count=0,
                processing_time=processing_time,
                error_message=error_msg
            )

    def _fetch_api_data(self, data_type: str, company_code: str, **kwargs) -> List[Dict[str, Any]]:
        """从API获取数据"""
        if data_type not in self.api_methods:
            raise ValueError(f"不支持的数据类型: {data_type}")

        api_method = self.api_methods[data_type]

        # 根据不同的API构建参数
        if data_type in ['account_structure', 'subject_dimension']:
            # 需要年份参数
            year = kwargs.get('year', str(datetime.now().year))
            return api_method(year, company_code)
        elif data_type == 'customer_vendor':
            # 只需要公司代码
            return api_method(company_code)
        elif data_type in ['voucher_list', 'voucher_detail', 'voucher_dim_detail', 'balance', 'aux_balance']:
            # 需要期间参数
            period_code = kwargs.get('period_code', f"{datetime.now().year}01")
            return api_method(company_code, period_code)
        else:
            return api_method(company_code)

    def _clean_data(self, raw_data: List[Dict[str, Any]], data_type: str):
        """清洗数据"""
        if data_type not in self.cleaning_methods:
            self.logger.warning(f"数据类型 {data_type} 没有对应的清洗方法，跳过清洗")
            return raw_data

        cleaning_method = self.cleaning_methods[data_type]
        return cleaning_method(raw_data)

    def _save_raw_data(self, data: List[Dict[str, Any]], data_type: str, company_code: str):
        """保存原始数据到数据库"""
        table_name = f"raw_{data_type}"

        # 为每条数据添加元数据
        for record in data:
            record['company_code'] = company_code
            record['created_at'] = datetime.now().isoformat()
            record['data_source'] = 'api'
            record['processing_status'] = 'raw'

        self._save_to_database(data, table_name)

    def _save_cleaned_data(self, data, data_type: str, company_code: str) -> int:
        """保存清洗后的数据到数据库"""
        if data is None or (hasattr(data, 'empty') and data.empty):
            return 0

        table_name = f"cleaned_{data_type}"

        # 如果是DataFrame，转换为字典列表
        if hasattr(data, 'to_dict'):
            data_list = data.to_dict('records')
        else:
            data_list = data

        # 为每条数据添加元数据
        for record in data_list:
            record['company_code'] = company_code
            record['processing_status'] = 'cleaned'

        self._save_to_database(data_list, table_name)
        return len(data_list)

    def _save_to_database(self, data: List[Dict[str, Any]], table_name: str):
        """保存数据到数据库的通用方法"""
        try:
            if not data:
                self.logger.warning(f"数据列表为空，跳过保存到表 {table_name}")
                return

            # 使用数据库管理器的方法保存数据
            success = self.db_manager.save_dict_list_to_table(
                data=data,
                table_name=table_name,
                if_exists='append'
            )

            if success:
                self.logger.info(f"成功保存 {len(data)} 条数据到表 {table_name}")
            else:
                raise Exception(f"保存数据到表 {table_name} 失败")

        except Exception as e:
            self.logger.error(f"保存数据到表 {table_name} 时发生错误: {str(e)}")
            raise

    def add_processing_tasks_to_system(self, system_manager: SystemManager,
                                       tasks_config: List[Dict[str, Any]]) -> bool:
        """
        将数据处理任务添加到系统管理器的队列中

        Args:
            system_manager: 系统管理器实例
            tasks_config: 任务配置列表，每个配置包含data_type, company_code等参数

        Returns:
            bool: 是否成功添加所有任务
        """
        try:
            for i, task_config in enumerate(tasks_config):
                data_type = task_config['data_type']
                company_code = task_config['company_code']
                task_name = f"process_{data_type}_{company_code}_{i}"

                # 提取其他参数
                kwargs = {k: v for k, v in task_config.items()
                          if k not in ['data_type', 'company_code', 'priority']}

                priority = task_config.get('priority', 0)

                # 添加任务到系统管理器
                success = system_manager.add_task(
                    name=task_name,
                    func=self.process_data,
                    args=(data_type, company_code),
                    kwargs=kwargs,
                    priority=priority,
                    max_retries=3
                )

                if success:
                    self.logger.info(f"任务 {task_name} 已添加到队列")
                else:
                    self.logger.error(f"添加任务 {task_name} 失败")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"添加处理任务到系统管理器时发生错误: {str(e)}")
            return False

    def get_processing_statistics(self) -> Dict[str, Any]:
        """获取数据处理统计信息"""
        return {
            'data_cleaner_stats': self.data_cleaner.cleaning_stats,
            'supported_data_types': list(self.api_methods.keys()),
            'processor_info': {
                'api_client': str(type(self.api_client).__name__),
                'db_manager': str(type(self.db_manager).__name__),
                'data_cleaner': str(type(self.data_cleaner).__name__)
            }
        }

    def close(self):
        """关闭数据处理器，释放资源"""
        try:
            if hasattr(self.api_client, 'close'):
                self.api_client.close()
            if hasattr(self.db_manager, 'close_engine'):
                self.db_manager.close_engine()
            self.logger.info("数据处理器已关闭")
        except Exception as e:
            self.logger.error(f"关闭数据处理器时发生错误: {str(e)}")


def create_batch_processing_tasks(company_codes: List[str],
                                  data_types: List[str],
                                  year: str = None,
                                  period_code: str = None) -> List[Dict[str, Any]]:
    """
    创建批量处理任务配置

    Args:
        company_codes: 公司代码列表
        data_types: 数据类型列表
        year: 年份（可选）
        period_code: 期间代码（可选）

    Returns:
        List[Dict[str, Any]]: 任务配置列表
    """
    tasks = []
    current_year = str(datetime.now().year)
    current_period = f"{datetime.now().year}{datetime.now().month:02d}"

    for company_code in company_codes:
        for i, data_type in enumerate(data_types):
            task_config = {
                'data_type': data_type,
                'company_code': company_code,
                'priority': len(data_types) - i  # 根据顺序设置优先级
            }

            # 根据数据类型添加必要的参数
            if data_type in ['account_structure', 'subject_dimension']:
                task_config['year'] = year or current_year
            elif data_type in ['voucher_list', 'voucher_detail', 'voucher_dim_detail', 'balance', 'aux_balance']:
                task_config['period_code'] = period_code or current_period

            tasks.append(task_config)

    return tasks
