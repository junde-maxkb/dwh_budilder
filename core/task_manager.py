import json
import logging
import os
from utils.generate_period_code import generate_period_codes
from core.org_crawler import OrgCrawler
from core.flow_crawler import FlowCrawler
from core.boe_crawler import BoeCrawler

logger = logging.getLogger(__name__)


class TaskManager:
    """任务管理器 - 负责检测新数据和创建处理任务"""

    def __init__(self, data_processor, system_manager, db_manager):
        self.data_processor = data_processor
        self.system_manager = system_manager
        self.db_manager = db_manager

        # 数据类型定义
        self.yearly_data_types = [
            'account_structure',  # 会计科目结构
            'subject_dimension',  # 科目辅助核算关系
            'customer_vendor',  # 客商字典
        ]

        self.period_data_types = [
            'voucher_list',  # 凭证目录
            'voucher_detail',  # 凭证明细
            'voucher_dim_detail',  # 凭证辅助明细
            'balance',  # 科目余额
            'aux_balance'  # 辅助余额
        ]

        current_dir = os.path.dirname(os.path.abspath(__file__))
        print(current_dir)
        company_ids_path = os.path.join(current_dir, '..', 'company_ids.json')
        print(company_ids_path)
        with open(company_ids_path, "r", encoding="utf-8") as f:
            self.company_codes = json.load(f)
        print(f"加载了 {len(self.company_codes)} 个公司代码用于任务管理")

    def check_and_add_new_data_tasks(self) -> bool:
        """
        检查并添加新的数据任务

        Returns:
            bool: 是否添加了新任务
        """
        logger.info("🔍 开始检查新数据...")

        try:
            # 1. 检查财务报表新任务
            has_new_financial_tasks = self._check_financial_report_tasks()

            # 2. 检查传统财务数据新任务
            has_new_traditional_tasks = self._check_traditional_data_tasks()

            # 3. 检查组织架构、资金流水、报账单新任务
            has_new_crawler_tasks = self._check_crawler_tasks()

            has_new_tasks = has_new_financial_tasks or has_new_traditional_tasks or has_new_crawler_tasks

            if has_new_tasks:
                logger.info("🎉 发现新数据，已添加相应任务到处理队列")
            else:
                logger.info("😴 暂无新数据需要处理")

            return has_new_tasks

        except Exception as e:
            logger.error(f"检查新数据时发生错误: {e}")
            return False

    def _check_financial_report_tasks(self) -> bool:
        """检查财务报表新任务"""
        try:
            quarterly_monthly_tasks = self.data_processor.get_quarterly_monthly_tasks()

            if not quarterly_monthly_tasks:
                logger.info("📊 财务报表检查完成，无新任务")
                return False

            new_financial_tasks = 0

            for i, task in enumerate(quarterly_monthly_tasks):
                task_name = task.get("taskName", "")

                # 检查任务是否已处理过
                formatted_task_name = f"process_financial_reports_{task_name}"

                # 获取系统状态，检查是否已有同名任务
                system_status = self.system_manager.get_system_status()
                existing_tasks = system_status.get("task_details", {})

                task_exists = any(formatted_task_name in task_name for task_name in existing_tasks.keys())

                if not task_exists:
                    success = self.data_processor.add_financial_report_task_to_system(
                        system_manager=self.system_manager,
                        task_info=task,
                        priority=10 + i
                    )

                    if success:
                        new_financial_tasks += 1
                        logger.info(f"✅ 发现并添加新的财务报表任务: {task_name}")

            if new_financial_tasks > 0:
                logger.info(f"📈 财务报表检查完成，新增 {new_financial_tasks} 个任务")
                return True
            else:
                logger.info("📊 财务报表检查完成，无新任务")
                return False

        except Exception as e:
            logger.warning(f"检查财务报表新任务时发生错误: {e}")
            return False

    def _check_traditional_data_tasks(self) -> bool:
        """检查传统财务数据新任务"""
        try:
            # 生成包含最新月份的期间代码，从2025年开始
            current_period_codes = generate_period_codes(start_year=2025)
            logger.debug(f"生成当前期间代码，最新期间: {current_period_codes[-1] if current_period_codes else 'None'}")

            new_traditional_tasks = []

            # 检查年度数据
            processed_years = set()
            for period_code in current_period_codes:
                year = period_code.split('-')[0]

                if year not in processed_years:
                    for data_type in self.yearly_data_types:
                        for company_code in self.company_codes:
                            if not self.db_manager.check_traditional_data_exists(data_type, company_code, year=year):
                                task_config = {
                                    'data_type': data_type,
                                    'company_code': company_code,
                                    'year': year,
                                    'period_code': f"{year}-01",
                                    'priority': len(self.yearly_data_types) - self.yearly_data_types.index(data_type)
                                }
                                new_traditional_tasks.append(task_config)
                                logger.info(f"✅ 发现新的年度数据需要处理: {data_type} - {company_code} - {year}")

                    processed_years.add(year)

            # 检查期间数据
            for period_code in current_period_codes:
                year = period_code.split('-')[0]

                for data_type in self.period_data_types:
                    for company_code in self.company_codes:
                        if not self.db_manager.check_traditional_data_exists(data_type, company_code,
                                                                             period_code=period_code):
                            task_config = {
                                'data_type': data_type,
                                'company_code': company_code,
                                'year': year,
                                'period_code': period_code,
                                'priority': len(self.period_data_types) - self.period_data_types.index(data_type)
                            }
                            new_traditional_tasks.append(task_config)
                            logger.info(f"✅ 发现新的期间数据需要处理: {data_type} - {company_code} - {period_code}")

            if new_traditional_tasks:
                success = self.data_processor.add_processing_tasks_to_system(
                    self.system_manager,
                    new_traditional_tasks
                )

                if success:
                    logger.info(f"📈 传统数据检查完成，新增 {len(new_traditional_tasks)} 个任务")
                    return True
                else:
                    logger.error("❌ 添加新的传统数据任务失败")
                    return False
            else:
                logger.info("📊 传统数据检查完成，无新任务")
                return False

        except Exception as e:
            logger.warning(f"检查传统数据新任务时发生错误: {e}")
            return False

    def _check_crawler_tasks(self) -> bool:
        """检查组织架构、资金流水、报账单新任务"""
        try:
            has_new_tasks = False

            # 检查组织架构任务
            if not self._check_crawler_task_exists("org_crawler"):
                if self._add_crawler_task("org_crawler", self._run_org_crawler, priority=20):
                    has_new_tasks = True
                    logger.info("✅ 发现并添加新的组织架构任务")

            # 检查资金流水任务
            if not self._check_crawler_task_exists("flow_crawler"):
                if self._add_crawler_task("flow_crawler", self._run_flow_crawler, priority=19):
                    has_new_tasks = True
                    logger.info("✅ 发现并添加新的资金流水任务")

            # 检查报账单任务
            if not self._check_crawler_task_exists("boe_crawler"):
                if self._add_crawler_task("boe_crawler", self._run_boe_crawler, priority=18):
                    has_new_tasks = True
                    logger.info("✅ 发现并添加新的报账单任务")

            if has_new_tasks:
                logger.info(f"📈 爬虫任务检查完成，新增任务")
                return True
            else:
                logger.info("📊 爬虫任务检查完成，无新任务")
                return False

        except Exception as e:
            logger.warning(f"检查爬虫任务时发生错误: {e}")
            return False

    def _check_crawler_task_exists(self, task_type: str) -> bool:
        """检查爬虫任务是否已存在"""
        try:
            system_status = self.system_manager.get_system_status()
            existing_tasks = system_status.get("task_details", {})
            task_name = f"crawler_{task_type}"
            return any(task_name in task_name_key for task_name_key in existing_tasks.keys())
        except Exception as e:
            logger.warning(f"检查爬虫任务是否存在时发生错误: {e}")
            return False

    def _add_crawler_task(self, task_type: str, func, priority: int = 0) -> bool:
        """添加爬虫任务到系统"""
        try:
            task_name = f"crawler_{task_type}"
            success = self.system_manager.add_task(
                name=task_name,
                func=func,
                args=(),
                kwargs={},
                priority=priority,
                max_retries=3
            )
            return success
        except Exception as e:
            logger.error(f"添加爬虫任务 {task_type} 时发生错误: {e}")
            return False

    def _run_org_crawler(self):
        """运行组织架构爬虫任务"""
        try:
            logger.info("开始执行组织架构爬虫任务...")
            crawler = OrgCrawler()
            crawler.run()
            logger.info("组织架构爬虫任务执行完成")
            return {"success": True, "message": "组织架构爬虫任务执行成功"}
        except Exception as e:
            logger.error(f"组织架构爬虫任务执行失败: {e}")
            raise

    def _run_flow_crawler(self):
        """运行资金流水爬虫任务"""
        try:
            logger.info("开始执行资金流水爬虫任务...")
            crawler = FlowCrawler()
            stats = crawler.run()
            logger.info(f"资金流水爬虫任务执行完成，统计: {stats}")
            return {"success": True, "message": "资金流水爬虫任务执行成功", "stats": stats}
        except Exception as e:
            logger.error(f"资金流水爬虫任务执行失败: {e}")
            raise

    def _run_boe_crawler(self):
        """运行报账单爬虫任务"""
        try:
            logger.info("开始执行报账单爬虫任务...")
            crawler = BoeCrawler()
            # 可以根据需要设置日期范围，这里使用默认值（None表示爬取所有数据）
            crawler.run(start_date="", end_date="")
            logger.info("报账单爬虫任务执行完成")
            return {"success": True, "message": "报账单爬虫任务执行成功"}
        except Exception as e:
            logger.error(f"报账单爬虫任务执行失败: {e}")
            raise

    def _create_initial_crawler_tasks(self) -> bool:
        """创建初始爬虫任务（组织架构、资金流水、报账单）"""
        logger.info("步骤3: 初次启动 - 添加爬虫任务到队列（组织架构、资金流水、报账单）")

        try:
            tasks_added = 0

            # 添加组织架构任务
            if not self._check_crawler_task_exists("org_crawler"):
                if self._add_crawler_task("org_crawler", self._run_org_crawler, priority=20):
                    tasks_added += 1
                    logger.info("✅ 组织架构任务已添加到队列")
                else:
                    logger.error("❌ 添加组织架构任务失败")
            else:
                logger.info("组织架构任务已存在，跳过")

            # 添加资金流水任务
            if not self._check_crawler_task_exists("flow_crawler"):
                if self._add_crawler_task("flow_crawler", self._run_flow_crawler, priority=19):
                    tasks_added += 1
                    logger.info("✅ 资金流水任务已添加到队列")
                else:
                    logger.error("❌ 添加资金流水任务失败")
            else:
                logger.info("资金流水任务已存在，跳过")

            # 添加报账单任务
            if not self._check_crawler_task_exists("boe_crawler"):
                if self._add_crawler_task("boe_crawler", self._run_boe_crawler, priority=18):
                    tasks_added += 1
                    logger.info("✅ 报账单任务已添加到队列")
                else:
                    logger.error("❌ 添加报账单任务失败")
            else:
                logger.info("报账单任务已存在，跳过")

            success = tasks_added > 0
            logger.info(f"爬虫任务添加完成，成功添加 {tasks_added} 个任务")
            return success

        except Exception as e:
            logger.error(f"添加爬虫任务到队列时发生错误: {e}", exc_info=True)
            return False

    def create_initial_tasks(self) -> tuple[bool, bool, bool]:
        """
        创建初始启动任务

        Returns:
            tuple: (财务报表任务是否成功, 传统数据任务是否成功, 爬虫任务是否成功)
        """
        try:
            # 创建初始财务报表任务
            financial_success = self._create_initial_financial_tasks()

            # 创建初始传统数据任务
            traditional_success = self._create_initial_traditional_tasks()

            # 创建初始爬虫任务（组织架构、资金流水、报账单）
            crawler_success = self._create_initial_crawler_tasks()

            return financial_success, traditional_success, crawler_success

        except Exception as e:
            logger.error(f"创建初始任务时发生错误: {e}", exc_info=True)
            return False, False, False

    def _create_initial_financial_tasks(self) -> bool:
        """创建初始财务报表任务"""
        logger.info("步骤1: 初次启动 - 添加财务报表任务到队列")

        try:
            quarterly_monthly_tasks = self.data_processor.get_quarterly_monthly_tasks()

            if not quarterly_monthly_tasks:
                logger.warning("未找到季报月报任务")
                return False

            financial_tasks_added = 0

            for i, task in enumerate(quarterly_monthly_tasks):
                task_name = task.get("taskName", "")
                logger.info(f"添加财务报表任务 - 任务名称: {task_name}")

                success = self.data_processor.add_financial_report_task_to_system(
                    system_manager=self.system_manager,
                    task_info=task,
                    priority=10 + i
                )

                if success:
                    financial_tasks_added += 1
                    logger.info(f"✅ 财务报表任务已添加到队列 - 任务名称: {task_name}")
                else:
                    logger.error(f"❌ 添加财务报表任务失败 - 任务名称: {task_name}")

            success = financial_tasks_added > 0
            logger.info(f"财务报表任务添加完成，成功添加 {financial_tasks_added} 个任务")

            return success

        except Exception as e:
            logger.error(f"添加财务报表任务到队列时发生错误: {e}", exc_info=True)
            return False

    def _create_initial_traditional_tasks(self) -> bool:
        """创建初始传统数据任务"""
        logger.info("步骤2: 初次启动 - 添加传统财务数据任务到队列")

        period_codes = generate_period_codes(start_year=2025)
        logger.info(f"生成了 {len(period_codes)} 个期间代码: {period_codes[:5]}...{period_codes[-5:]}")

        # 过滤已存在的数据
        logger.info("开始检查已存在的数据，过滤重复任务...")

        all_tasks_config = []

        # 1. 处理按年份的数据，先过滤已存在的
        processed_years = set()
        skipped_yearly_tasks = 0
        for period_code in period_codes:
            year = period_code.split('-')[0]

            if year not in processed_years:
                for data_type in self.yearly_data_types:
                    for company_code in self.company_codes:
                        if self.db_manager.check_traditional_data_exists(data_type, company_code, year=year):
                            skipped_yearly_tasks += 1
                            logger.debug(
                                f"跳过已存在的年度数据 - 类型: {data_type}, 公司: {company_code}, 年份: {year}")
                            continue

                        task_config = {
                            'data_type': data_type,
                            'company_code': company_code,
                            'year': year,
                            'period_code': f"{year}-01",
                            'priority': len(self.yearly_data_types) - self.yearly_data_types.index(data_type)
                        }
                        all_tasks_config.append(task_config)

                processed_years.add(year)

        logger.info(f"年度数据检查完成，跳过 {skipped_yearly_tasks} 个已存在的任务")

        # 2. 处理按期间的数据，先过滤已存在的
        skipped_period_tasks = 0
        for period_code in period_codes:
            year = period_code.split('-')[0]

            for data_type in self.period_data_types:
                for company_code in self.company_codes:
                    if self.db_manager.check_traditional_data_exists(data_type, company_code, period_code=period_code):
                        skipped_period_tasks += 1
                        logger.debug(
                            f"跳过已存在的期间数据 - 类型: {data_type}, 公司: {company_code}, 期间: {period_code}")
                        continue

                    task_config = {
                        'data_type': data_type,
                        'company_code': company_code,
                        'year': year,
                        'period_code': period_code,
                        'priority': len(self.period_data_types) - self.period_data_types.index(data_type)
                    }
                    all_tasks_config.append(task_config)

        logger.info(f"期间数据检查完成，跳过 {skipped_period_tasks} 个已存在的任务")

        total_skipped = skipped_yearly_tasks + skipped_period_tasks
        logger.info(f"初次启动数据去重完成：")
        logger.info(f"  - 跳过的重复任务数: {total_skipped}")
        logger.info(f"  - 实际需要执行的任务数: {len(all_tasks_config)}")

        if not all_tasks_config:
            logger.info("所有传统数据任务都已存在，无需重复处理")
            return True

        success = self.data_processor.add_processing_tasks_to_system(
            self.system_manager,
            all_tasks_config
        )

        if success:
            logger.info(f"成功添加 {len(all_tasks_config)} 个新的传统数据处理任务到队列")
            return True
        else:
            logger.error("添加传统数据任务到队列时发生错误")
            return False
