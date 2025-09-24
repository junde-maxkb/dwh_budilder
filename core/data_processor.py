import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Any, Optional
from api.api_client import FinanceAPIClient, create_auto_financial_api
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

    def __init__(self, api_config: Dict[str, str], db_manager: DataBaseManager = None,
                 auto_report_config: Dict[str, str] = None,
                 financial_report_storage_mode: str = 'json'):
        """
        初始化数据处理器

        Args:
            api_config: API配置，包含base_url, app_key, app_secret
            db_manager: 数据库管理器实例，如果为None则创建新实例
            auto_report_config: 自动财务报表API配置，包含username, password
            financial_report_storage_mode: 财务报表存储模式，'legacy' 或 'json'
        """
        self.logger = logging.getLogger(__name__)

        # 初始化各个组件
        self.api_client = FinanceAPIClient(
            base_url=api_config['base_url'],
            app_key=api_config['app_key'],
            app_secret=api_config['app_secret']
        )

        # 初始化自动财务报表API客户端
        self.auto_report_api = None
        if auto_report_config:
            try:
                self.auto_report_api = create_auto_financial_api(
                    username=auto_report_config.get('username', 'lijin5'),
                    password=auto_report_config.get('password', 'Qaz.123456789.')
                )
                self.logger.info("自动财务报表API客户端初始化成功")
            except Exception as e:
                self.logger.warning(f"自动财务报表API客户端初始化失败: {e}")

        self.db_manager = db_manager or DataBaseManager()
        self.data_cleaner = DataCleaner()
        self.financial_report_storage_mode = financial_report_storage_mode.lower().strip()
        if self.financial_report_storage_mode not in ('legacy', 'json'):
            self.financial_report_storage_mode = 'json'

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

        # 数据清洗方法映射 (voucher_dim_detail 使用独立的更高容错方法)
        self.cleaning_methods = {
            'account_structure': self.data_cleaner.clean_account_structure,
            'subject_dimension': self.data_cleaner.clean_subject_dimension,
            'customer_vendor': self.data_cleaner.clean_customer_vendor,
            'voucher_list': self.data_cleaner.clean_voucher_list,
            'voucher_detail': self.data_cleaner.clean_voucher_detail,
            'voucher_dim_detail': self.data_cleaner.clean_voucher_dim_detail,
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

            # 检查数据是否已存在
            year = kwargs.get('year')
            period_code = kwargs.get('period_code')

            if self.db_manager.check_traditional_data_exists(data_type, company_code, year, period_code):
                self.logger.info(
                    f"数据已存在，跳过处理 - 数据类型: {data_type}, 公司: {company_code}, 年份: {year}, 期间: {period_code}")
                return ProcessingResult(
                    success=True,
                    data_type=data_type,
                    original_count=0,
                    cleaned_count=0,
                    saved_count=0,
                    processing_time=0,
                    error_message="数据已存在，已跳过"
                )

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

            # 2. 存储原始数据到数据库 (加入 year / period_code 元数据)
            self._save_raw_data(raw_data, data_type, company_code, year=year, period_code=period_code)
            self.logger.info(f"原始数据已保存到数据库")

            # 3. 清洗数据
            cleaned_data = self._clean_data(raw_data, data_type)
            cleaned_count = len(cleaned_data) if cleaned_data is not None else 0
            self.logger.info(f"数据清洗完成，得到 {cleaned_count} 条清洗后数据")

            # 4. 存储清洗后的数据 (加入 year / period_code 元数据)
            saved_count = self._save_cleaned_data(cleaned_data, data_type, company_code, year=year,
                                                  period_code=period_code)
            self.logger.info(f"清洗后数据已保存到数据库，实际保存 {saved_count} 条")

            processing_time = (datetime.now() - start_time).total_seconds()

            return ProcessingResult(
                success=True,
                data_type=data_type,
                original_count=original_count,
                cleaned_count=cleaned_count,
                saved_count=saved_count,
                processing_time=processing_time
            )

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

    def _save_raw_data(self, data: List[Dict[str, Any]], data_type: str, company_code: str, year: str = None,
                       period_code: str = None):
        """保存原始数据到数据库，并附加判重所需元数据(year / period_code)"""
        table_name = f"raw_{data_type}"

        for record in data:
            record['company_code'] = company_code  # 确保存在
            # 始终创建 period_code 列，即使该数据类型不需要期间（如科目结构、科目维度）
            if 'period_code' not in record:
                record['period_code'] = period_code if period_code else None
            if year:
                record['year'] = year
            record['created_at'] = datetime.now().isoformat()
            record['data_source'] = 'api'
            record['processing_status'] = 'raw'

        self._save_to_database(data, table_name)

    def _save_cleaned_data(self, data, data_type: str, company_code: str, year: str = None,
                           period_code: str = None) -> int:
        """保存清洗后的数据存入数据库，并附加 year / period_code"""
        if data is None or (hasattr(data, 'empty') and data.empty):
            return 0

        table_name = f"cleaned_{data_type}"

        if hasattr(data, 'to_dict'):
            data_list = data.to_dict('records')
        else:
            data_list = data

        for record in data_list:
            record['company_code'] = company_code
            if 'period_code' not in record:
                record['period_code'] = period_code if period_code else None
            if year:
                record['year'] = year
            record['processing_status'] = 'cleaned'

        self._save_to_database(data_list, table_name)
        return len(data_list)

    def _save_to_database(self, data: List[Dict[str, Any]], table_name: str):
        """
        保存数据到数据库，增强的错误处理和重试机制
        """
        try:
            if not data:
                self.logger.warning(f"数据列表为空，跳过保存到表 {table_name}")
                return

            # 使用增强的自动创建表并保存数据的方法
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    success = self.db_manager.auto_create_and_save_data(
                        data=data,
                        table_name=table_name,
                        if_exists='append'
                    )

                    if success:
                        self.logger.info(f"成功自动创建表并保存 {len(data)} 条数据到表 {table_name}")
                        return
                    else:
                        if attempt == max_retries - 1:
                            raise Exception(f"自动创建表并保存数据到表 {table_name} 失败")
                        else:
                            self.logger.warning(f"保存数据到表 {table_name} 失败，第 {attempt + 1} 次重试")
                            time.sleep(0.5 * (attempt + 1))  # 递增延迟

                except Exception as inner_e:
                    if attempt == max_retries - 1:
                        raise inner_e
                    else:
                        self.logger.warning(f"保存数据到表 {table_name} 时发生错误，第 {attempt + 1} 次重试: {inner_e}")
                        time.sleep(0.5 * (attempt + 1))  # 递增延迟

        except Exception as e:
            self.logger.error(f"保存数据到表 {table_name} 时发生错误: {str(e)}")
            # 不再抛出异常，而是记录错误并返回，避免整个任务失败
            self.logger.warning(f"数据保存失败，但任务将继续执行后续步骤")

    def add_processing_tasks_to_system(self, system_manager: SystemManager,
                                       tasks_config: List[Dict[str, Any]]) -> bool:
        """
        将数据处理任务添加到系统管理器的队列中

        Args:
            system_manager: 系统管理器实例
            tasks_config: 任务配置列表，每个配置包含data_type, company_code参数

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
                    self.logger.debug(f"任务 {task_name} 已添加到队列")
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
                'data_cleaner': str(type(self.data_cleaner).__name__),
                'auto_report_api': str(type(self.auto_report_api).__name__) if self.auto_report_api else None
            }
        }

    def get_quarterly_monthly_tasks(self) -> List[Dict[str, Any]]:
        """
        获取季报月报任务列表

        Returns:
            List[Dict[str, Any]]: 季报月报任务列表
        """
        if not self.auto_report_api:
            self.logger.error("自动财务报表API客户端未初始化")
            return []

        try:
            return self.auto_report_api.get_quarterly_monthly_tasks()
        except Exception as e:
            self.logger.error(f"获取季报月报任务失败: {e}")
            return []

    @execution_monitor(stage="financial_report_processing", track_memory=True)
    def process_financial_reports(self, task_info: str | Dict[str, Any] = None) -> ProcessingResult:
        """
        处理财务报表数据，支持逐个单位的数据处理和存储

        Args:
            task_info: 任务信息，可以是任务名称字符串或完整的任务字典

        Returns:
            ProcessingResult: 处理结果
        """
        if not self.auto_report_api:
            self.logger.error("自动财务报表API客户端未初始化")
            return ProcessingResult(
                success=False,
                data_type="financial_reports",
                original_count=0,
                cleaned_count=0,
                saved_count=0,
                processing_time=0,
                error_message="自动财务报表API客户端未初始化"
            )

        start_time = datetime.now()

        # 统计信息
        metadata_saved = 0
        report_data_saved = 0
        error_count = 0

        def save_data_callback(data: Dict[str, Any], data_type: str) -> None:
            """回调函数：处理和保存单个单位的数据"""
            nonlocal metadata_saved, report_data_saved, error_count

            try:
                if data_type == "metadata":
                    # 处理基础元数据
                    metadata_count = self._process_metadata(data)
                    metadata_saved += metadata_count
                    self.logger.info(f"基础元数据保存完成，共 {metadata_count} 条记录")

                elif data_type == "report_data":
                    # 处理单个单位的报表数据
                    if self.financial_report_storage_mode == 'json':
                        raw_records, cleaned_records = self._process_financial_report_unit_json(data)
                        if raw_records:
                            self._save_to_database(raw_records, 'raw_financial_reports_json')
                            report_data_saved += len(raw_records)
                        if cleaned_records:
                            self._save_to_database(cleaned_records, 'cleaned_financial_reports_json')
                            report_data_saved += len(cleaned_records)
                    else:
                        unit_saved = self._process_single_unit_report_data(data)
                        report_data_saved += unit_saved
                        self.logger.info(
                            f"单位 {data.get('company_id')} - {data.get('period_name')} 报表数据保存完成，共 {unit_saved} 条记录")

            except Exception as e:
                error_count += 1
                self.logger.error(f"保存数据时发生错误 (类型: {data_type}): {e}")
                raise

        try:
            self.logger.info(f"开始处理财务报表数据，任务信息: {task_info}")

            if task_info and isinstance(task_info, dict) and 'taskName' in task_info:
                task_name = task_info.get('taskName', '')
                self.logger.info(f"使用预先获取的任务信息: {task_name}")

                report_result = self.auto_report_api.get_all_data_by_task(
                    task_name_filter=None,
                    filter_quarterly_monthly=False,
                    tasks_list=[task_info],
                    save_callback=save_data_callback
                )
            else:
                self.logger.info("未提供具体任务信息，将重新获取任务列表")
                report_result = self.auto_report_api.get_all_data_by_task(
                    task_info,
                    save_callback=save_data_callback
                )

            if not report_result:
                return ProcessingResult(
                    success=False,
                    data_type="financial_reports",
                    original_count=0,
                    cleaned_count=0,
                    saved_count=0,
                    processing_time=(datetime.now() - start_time).total_seconds(),
                    error_message="未获取到财务报表数据"
                )

            processing_time = (datetime.now() - start_time).total_seconds()
            total_saved = metadata_saved + report_data_saved

            # 记录处理统计信息
            self.logger.info(f"财务报表数据处理统计:")
            self.logger.info(f"  - 处理单位数: {report_result.get('processed_count', 0)}")
            self.logger.info(f"  - 成功单位数: {report_result.get('success_count', 0)}")
            self.logger.info(f"  - 失败单位数: {report_result.get('error_count', 0)}")
            self.logger.info(f"  - 元数据记录数: {metadata_saved}")
            self.logger.info(f"  - 报表数据记录数: {report_data_saved}")
            self.logger.info(f"  - 总保存记录数: {total_saved}")

            return ProcessingResult(
                success=True,
                data_type="financial_reports",
                original_count=report_result.get('processed_count', 0),
                cleaned_count=report_result.get('success_count', 0),
                saved_count=total_saved,
                processing_time=processing_time
            )

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"处理财务报表数据时发生错误: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            return ProcessingResult(
                success=False,
                data_type="financial_reports",
                original_count=0,
                cleaned_count=0,
                saved_count=0,
                processing_time=processing_time,
                error_message=error_msg
            )

    def _process_metadata(self, data: Dict[str, Any]) -> int:
        """
        处理基础元数据

        Args:
            data: 包含任务、月份、单位等基础元数据的字典

        Returns:
            int: 保存的记录总数
        """
        total_saved = 0

        try:
            # 1. 保存任务信息
            task_info = [data.get('task', {})]
            if task_info[0]:
                task_info[0]['created_at'] = datetime.now().isoformat()
                task_info[0]['data_source'] = 'financial_report_api'
                task_info[0]['processing_status'] = 'raw'
                task_info = self._dedup_records('financial_report_tasks', task_info,
                                                ['taskId', 'taskID', 'taskName', 'task_name'])
                if task_info:
                    self._save_to_database(task_info, 'financial_report_tasks')
                    total_saved += len(task_info)
                    self.logger.info(f"任务信息元数据处理: {len(task_info)} 条 (去重后)")
                else:
                    self.logger.info("任务信息已存在，跳过插入")

            # 2. 保存月份信息元数据
            periods = data.get('periods', [])
            if periods:
                for period in periods:
                    period['created_at'] = datetime.now().isoformat()
                    period['data_source'] = 'financial_report_api'
                    period['processing_status'] = 'raw'
                # 依据 period 可能的字段: id / period_detail_id
                periods = self._dedup_records('financial_report_periods', periods, ['id', 'period_detail_id'])
                if periods:
                    self._save_to_database(periods, 'financial_report_periods')
                    total_saved += len(periods)
                    self.logger.info(f"月份信息元数据处理: {len(periods)} 条 (去重后)")
                else:
                    self.logger.info("月份信息全部已存在，跳过插入")

            # 3. 保存单位信息元数据
            companies = self._flatten_company_tree(data.get('companies', []))
            if companies:
                for company in companies:
                    company['created_at'] = datetime.now().isoformat()
                    company['data_source'] = 'financial_report_api'
                    company['processing_status'] = 'raw'
                companies = self._dedup_records('financial_report_companies', companies,
                                                ['id', 'company_id', 'companyId'])
                if companies:
                    self._save_to_database(companies, 'financial_report_companies')
                    total_saved += len(companies)
                    self.logger.info(f"单位信息元数据处理: {len(companies)} 条 (去重后)")
                else:
                    self.logger.info("单位信息全部已存在，跳过插入")

            return total_saved

        except Exception as e:
            self.logger.error(f"处理基础元数据时发生错误: {e}")
            raise

    def _dedup_records(self, table_name: str, records: List[Dict[str, Any]], key_candidates: List[str]) -> (
            List)[Dict[str, Any]]:
        if not records:
            return records
        # 选择合适的key
        selected_key = None
        total = len(records)
        for key in key_candidates:
            presence = sum(1 for r in records if key in r and r.get(key) not in (None, ''))
            if presence / total >= 0.6:  # 至少60%记录包含
                selected_key = key
                break
        if not selected_key:
            return records
        try:
            existing_values = self.db_manager.get_existing_values(table_name, selected_key)
        except Exception as e:
            self.logger.error(f"从表 {table_name} 获取已存在的主键值时发生错误: {e}")
            existing_values = set()
        new_seen = set()
        filtered = []
        for r in records:
            val = r.get(selected_key)
            if val is None or val == '':
                # 保留无主键值的记录（或可选择跳过）
                filtered.append(r)
                continue
            sval = str(val)
            if sval in existing_values or sval in new_seen:
                continue
            new_seen.add(sval)
            filtered.append(r)
        dropped = len(records) - len(filtered)
        if dropped > 0:
            self.logger.info(f"去重: 表 {table_name} 依据字段 {selected_key} 删除 {dropped} 条重复记录")
        return filtered

    # 恢复缺失的方法
    def _process_single_unit_report_data(self, unit_data: Dict[str, Any]) -> int:
        """处理单个单位的报表数据 (非 JSON 模式)。返回保存的记录数。"""
        try:
            reports_data = [unit_data]
            raw_report_records = self._process_raw_reports_data(reports_data)
            total_saved = 0
            if raw_report_records:
                self._save_to_database(raw_report_records, 'raw_financial_reports')
                total_saved += len(raw_report_records)
            cleaned_report_data = self._clean_financial_reports_data(raw_report_records)
            if cleaned_report_data:
                self._save_to_database(cleaned_report_data, 'cleaned_financial_reports')
                total_saved += len(cleaned_report_data)
            return total_saved
        except Exception as e:
            self.logger.error(f"处理单个单位报表数据时发生错误: {e}")
            raise

    def _process_raw_reports_data(self, reports_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """将原始报表数据转换为标准化记录格式。"""
        formatted_data = []
        try:
            for report_item in reports_data:
                period_name = report_item.get('period_name', '未知月份')
                period_detail_id = report_item.get('period_detail_id', '')
                company_id = report_item.get('company_id', '')
                parent_id = report_item.get('parent_id', '')
                reports = report_item.get('reports', [])
                report_data = report_item.get('report_data', [])
                if report_data and isinstance(report_data, list):
                    for row_index, row in enumerate(report_data):
                        if isinstance(row, list):
                            record = {
                                'period_name': period_name,
                                'period_detail_id': period_detail_id,
                                'company_id': company_id,
                                'parent_id': parent_id,
                                'row_index': row_index,
                                'raw_data': str(row),
                                'data_source': 'financial_report_api',
                                'processing_status': 'raw',
                                'created_at': datetime.now().isoformat()
                            }
                            for col_index, value in enumerate(row):
                                record[f'col_{col_index}'] = value
                            formatted_data.append(record)
                for report in reports:
                    report_record = {
                        'period_name': period_name,
                        'period_detail_id': period_detail_id,
                        'company_id': company_id,
                        'parent_id': parent_id,
                        'report_id': report.get('reportId', ''),
                        'report_name': report.get('reportName', ''),
                        'report_type': 'report_info',
                        'data_source': 'financial_report_api',
                        'processing_status': 'raw',
                        'created_at': datetime.now().isoformat()
                    }
                    formatted_data.append(report_record)
            self.logger.info(f"原始报表数据处理完成，共生成 {len(formatted_data)} 条记录")
            return formatted_data
        except Exception as e:
            self.logger.error(f"处理原始报表数据时发生错误: {e}")
            raise

    def _clean_financial_reports_data(self, raw_report_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """清洗财务报表数据。"""
        cleaned_data = []
        try:
            for record in raw_report_records:
                cleaned_record = record.copy()
                cleaned_record['processing_status'] = 'cleaned'
                cleaned_record['cleaned_at'] = datetime.now().isoformat()
                for key, value in cleaned_record.items():
                    if isinstance(value, str):
                        cleaned_record[key] = value.strip()
                for col_key in list(cleaned_record.keys()):
                    if col_key.startswith('col_'):
                        value = cleaned_record[col_key]
                        if isinstance(value, str):
                            core = value.replace(',', '').replace(' ', '')
                            if core.replace('.', '', 1).replace('-', '', 1).isdigit():
                                try:
                                    cleaned_record[col_key] = float(core)
                                except ValueError:
                                    pass
                if 'company_id' in cleaned_record:
                    cleaned_record['company_id'] = str(cleaned_record['company_id']).strip()
                if 'parent_id' in cleaned_record:
                    cleaned_record['parent_id'] = str(cleaned_record['parent_id']).strip()
                required_fields = ['company_id', 'period_detail_id']
                if all(cleaned_record.get(f) for f in required_fields):
                    cleaned_data.append(cleaned_record)
                else:
                    self.logger.warning(f"记录缺少必要字段，跳过: {cleaned_record}")
            self.logger.info(
                f"财务报表数据清洗完成，从 {len(raw_report_records)} 条原始记录清洗出 {len(cleaned_data)} 条")
            return cleaned_data
        except Exception as e:
            self.logger.error(f"清洗财务报表数据时发生错误: {e}")
            raise

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

    def _flatten_company_tree(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        将公司树结构展平为列表

        Args:
            companies: 公司树结构

        Returns:
            List[Dict[str, Any]]: 展平后的公司列表
        """
        flattened = []

        def flatten_recursive(company_list: List[Dict[str, Any]]):
            for company in company_list:
                # 复制公司信息，排除children字段
                company_info = {k: v for k, v in company.items() if k != 'children'}
                flattened.append(company_info)

                # 递归处理子公司
                children = company.get('children', [])
                if children:
                    flatten_recursive(children)

        flatten_recursive(companies)
        return flattened

    def add_financial_report_task_to_system(self, system_manager: SystemManager, task_info: Dict[str, Any] = None,
                                            priority: int = 0) -> bool:
        """
        将财务报表处理任务添加到系统管理器

        Args:
            system_manager: 系统管理器实例
            task_info: 任务信息字典
            priority: 任务优先级

        Returns:
            bool: 是否成功添加任务
        """
        try:
            task_name = task_info.get("taskName", "unknown_task") if task_info else "all_tasks"
            formatted_task_name = f"process_financial_reports_{task_name}"

            success = system_manager.add_task(
                name=formatted_task_name,
                func=self.process_financial_reports,
                args=(task_info,),
                kwargs={},
                priority=priority,
                max_retries=3
            )

            if success:
                self.logger.info(f"财务报表处理任务 {formatted_task_name} 已添加到队列")
            else:
                self.logger.error(f"添加财务报表处理任务 {formatted_task_name} 失败")

            return success

        except Exception as e:
            self.logger.error(f"添加财务报表处理任务到系统管理器时发生错误: {str(e)}")
            return False

    def _process_financial_report_unit_json(self, unit_data: Dict[str, Any]) -> (
            List[Dict[str, Any]], List[Dict[str, Any]]):
        """将单个单位的报表数据以 JSON 形式存储。
        返回 (raw_records, cleaned_records) 列表，每个列表通常只有一条记录。"""
        try:
            period_name = unit_data.get('period_name', '')
            period_detail_id = unit_data.get('period_detail_id', '')
            company_id = unit_data.get('company_id', '')
            parent_id = unit_data.get('parent_id', '')
            reports = unit_data.get('reports', []) or []
            report_data_matrix = unit_data.get('report_data', []) or []

            # 统计原始矩阵行/列
            row_count = len(report_data_matrix)
            col_count = max((len(r) for r in report_data_matrix), default=0)

            raw_record = {
                'company_id': company_id,
                'parent_id': parent_id,
                'period_name': period_name,
                'period_detail_id': period_detail_id,
                'reports': reports,  # list -> JSON 序列化由 DB 层处理
                'report_data': report_data_matrix,  # list[list]
                'row_count': row_count,
                'col_count': col_count,
                'data_source': 'financial_report_api',
                'processing_status': 'raw',
                'created_at': datetime.now().isoformat()
            }

            # 清洗：使用 DataCleaner 的内部方法清理矩阵值
            cleaner = self.data_cleaner
            cleaned_matrix = []
            numeric = text = empty = nulls = 0
            cleaned_cells_types = []  # 可选：调试用
            for r_idx, row in enumerate(report_data_matrix):
                new_row = []
                if not isinstance(row, list):
                    continue
                for c_idx, value in enumerate(row):
                    cleaned_value = cleaner.clean_single_report_value(value)
                    vtype = cleaner.classify_report_value(value)
                    if vtype == 'numeric':
                        numeric += 1
                    elif vtype == 'text':
                        text += 1
                    elif vtype == 'empty':
                        empty += 1
                    elif vtype == 'null':
                        nulls += 1
                    new_row.append(cleaned_value)
                    cleaned_cells_types.append(vtype)
                cleaned_matrix.append(new_row)

            cleaned_record = {
                'company_id': company_id,
                'parent_id': parent_id,
                'period_name': period_name,
                'period_detail_id': period_detail_id,
                'reports': reports,
                'report_data': report_data_matrix,  # 原始数据保留
                'cleaned_report_data': cleaned_matrix,
                'summary': {
                    'row_count': row_count,
                    'col_count': col_count,
                    'numeric_values': numeric,
                    'text_values': text,
                    'empty_values': empty,
                    'null_values': nulls,
                    'total_cells': numeric + text + empty + nulls
                },
                'data_source': 'financial_report_api',
                'processing_status': 'cleaned',
                'created_at': datetime.now().isoformat(),
                'cleaned_at': datetime.now().isoformat()
            }
            return [raw_record], [cleaned_record]
        except Exception as e:
            self.logger.error(f"JSON 方式处理单元报表数据失败: {e}")
            return [], []
