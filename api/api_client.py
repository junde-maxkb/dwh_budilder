import json
import re
import time
from typing import Dict, List, Optional, Any, Tuple

import requests
from fake_useragent import UserAgent
from loguru import logger
from pydantic import BaseModel
from requests import RequestException

from core.automate_chrome import get_automation_data


class APIResponse(BaseModel):
    success: bool
    message: str = ""
    code: int
    result: Optional[List[Dict[str, Any]]] = None
    timestamp: Optional[int] = None


class FinanceAPIClient:

    def __init__(self, base_url: str, app_key: str, app_secret: str):
        self.base_url = base_url.rstrip('/')
        self.app_key = app_key
        self.app_secret = app_secret
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': UserAgent().random
        })

    def _normalize_api_response(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "success": raw_data.get("success", raw_data.get("code") == 200),
            "message": raw_data.get("message") or raw_data.get("info", ""),
            "code": raw_data.get("code", -1),
            "result": raw_data.get("result") if "result" in raw_data else raw_data.get("data"),
            "timestamp": raw_data.get("timestamp")
        }

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> APIResponse:
        url = f"{self.base_url}{endpoint}"

        request_data = {
            "appkey": self.app_key,
            "appSecret": self.app_secret,
            **params
        }

        try:
            logger.info(f"请求API: {endpoint}, 参数: {request_data}")
            response = self.session.post(url, json=request_data, timeout=30)
            response.raise_for_status()

            raw_data = response.json()

            mapped_data = self._normalize_api_response(raw_data)

            api_response = APIResponse(**mapped_data)

            if api_response.success:
                logger.info(f"API调用成功: {endpoint}, 返回数据条数: {len(api_response.result or [])}")
            else:
                logger.error(f"API调用失败: {endpoint}, 错误: {api_response.message}")

            return api_response

        except requests.exceptions.RequestException as e:
            logger.error(f"网络请求失败: {endpoint}, 错误: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {endpoint}, 错误: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"未知错误: {endpoint}, 错误: {str(e)}")
            raise

    def get_account_structure(self, year: str, company_code: str) -> List[Dict[str, Any]]:
        """获取某年度会计科目结构"""
        response = self._make_request("/Cw6Api/GetAcc", {
            "year": year,
            "companyCode": company_code
        })
        return response.result or []

    def get_subject_dimension_relationship(self, year: str, company_code: str) -> List[Dict[str, Any]]:
        """获取科目辅助核算对应关系"""
        response = self._make_request("/Cw6Api/Subject_Dimension_Relationship", {
            "year": year,
            "companyCode": company_code
        })
        return response.result or []

    def get_customer_vendor_dict(self, company_code: str) -> List[Dict[str, Any]]:
        """获取客商字典"""
        response = self._make_request("/Cw6Api/Get_PC", {
            "companyCode": company_code
        })
        return response.result or []

    def get_voucher_list(self, company_code: str, period_code: str) -> List[Dict[str, Any]]:
        """获取凭证目录"""
        response = self._make_request("/Cw6Api/Get_Voucher", {
            "companyCode": company_code,
            "periodCode": period_code
        })
        return response.result or []

    def get_voucher_detail(self, company_code: str, period_code: str) -> List[Dict[str, Any]]:
        """获取凭证明细"""
        response = self._make_request("/Cw6Api/Get_Voucher_Detail", {
            "companyCode": company_code,
            "periodCode": period_code
        })
        return response.result or []

    def get_voucher_dim_detail(self, company_code: str, period_code: str) -> List[Dict[str, Any]]:
        """获取凭证辅助明细"""
        response = self._make_request("/Cw6Api/Get_Voucher_Dim_Detail", {
            "companyCode": company_code,
            "periodCode": period_code
        })
        return response.result or []

    def get_balance(self, company_code: str, period_code: str) -> List[Dict[str, Any]]:
        """获取科目余额"""
        response = self._make_request("/Cw6Api/Get_Balance", {
            "companyCode": company_code,
            "periodCode": period_code
        })
        return response.result or []

    def get_aux_balance(self, company_code: str, period_code: str) -> List[Dict[str, Any]]:
        """获取辅助余额"""
        response = self._make_request("/Cw6Api/Get_Aux_Balance", {
            "companyCode": company_code,
            "periodCode": period_code
        })
        return response.result or []

    def close(self):
        """关闭会话"""
        if self.session:
            self.session.close()


class AutoFinancialReportAPI:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "http://10.3.102.141/shj/vue/api/rp/query_output/query_report_new"
        self.report_url = "http://10.3.102.141/shj/vue/api/rp"
        self.session = requests.Session()

        self.access_token = None
        self.token = None
        self.user_agent = None
        self.cookies = None

        self.base_headers = {
            'Connection': 'keep-alive',
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'http://10.3.102.141',
            'Referer': 'http://10.3.102.141/shj/vue/?1756345177235b0a0c0c4bdcfbc872fadd9186e65b64e',
            'Accept-Language': 'zh-CN,zh;q=0.9'
        }

        logger.info(f"初始化自动化财务报表API客户端, 用户: {username}")

    def login_and_get_tokens(self) -> bool:
        logger.info("开始执行自动化登录...")

        try:
            token_data, cookies, user_agent = get_automation_data(self.username, self.password)

            if not token_data or not cookies or not user_agent:
                logger.error("自动化登录失败，未获取到必要的认证信息")
                return False

            if isinstance(token_data, str):
                self.access_token = token_data
                cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
                self.token = cookie_dict.get('token', cookie_dict.get('TOKEN'))
            elif isinstance(token_data, dict):
                for key, value in token_data.items():
                    if 'X-Access-Token' in key:
                        self.access_token = value
                    elif 'token' in key:
                        self.token = value

            self.cookies = cookies
            self.user_agent = user_agent

            self._update_session_config()

            logger.info(f"自动化登录成功，获取到access_token: {self.access_token[:50]}...")
            return True

        except Exception as e:
            logger.error(f"自动化登录过程中出错: {e}")
            return False

    def _update_session_config(self):
        if not self.cookies or not self.user_agent:
            return

        cookie_dict = {cookie['name']: cookie['value'] for cookie in self.cookies}
        self.session.cookies.update(cookie_dict)

        headers = self.base_headers.copy()
        headers['User-Agent'] = self.user_agent

        cookie_str = '; '.join([f"{name}={value}" for name, value in cookie_dict.items()])
        headers['Cookie'] = cookie_str

        self.session.headers.update(headers)

        logger.info("Session配置已更新")

    def _get_request_headers(self) -> Dict[str, str]:
        """获取请求头"""
        if not self.access_token:
            raise ValueError("未获取到access_token，请先执行登录")

        headers = {
            "Connection": "keep-alive",
            "Accept": "application/json, text/plain, */*",
            "X-Access-Token": self.access_token,
            "X-Access-Token-Old": self.token or "",
            "User-Agent": self.user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                             "AppleWebKit/537.36 (KHTML, like Gecko) "
                                             "Chrome/97.0.4692.71 Safari/537.36",
            "Content-Type": "application/json;charset=UTF-8",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }

        if self.cookies:
            if isinstance(self.cookies, list):
                cookie_dict = {cookie['name']: cookie['value'] for cookie in self.cookies}
                cookie_str = '; '.join([f"{name}={value}" for name, value in cookie_dict.items()])
                headers['Cookie'] = cookie_str
            elif isinstance(self.cookies, str):
                headers['Cookie'] = self.cookies
        return headers

    def _make_api_request(self, report_ids: List[str], company_code: str, company_parent_code: str) -> Dict[str, Any]:
        """
        发送API请求获取报表数据
        :param report_ids: 报表的ID列表
        :param company_code: 单位ID
        :param company_parent_code: 单位的父ID，默认值为"2SH0000001"
        :return:
        """
        if not self.access_token:
            raise ValueError("未获取到access_token请先执行登录")

        timestamp = int(time.time() * 1000)

        params = {
            'TIMESTAMP': timestamp,
            'TOKEN': self.token or ""
        }

        headers = self.base_headers.copy()
        headers['X-Access-Token'] = self.access_token
        if self.token:
            headers['X-Access-Token-Old'] = self.token
        if self.user_agent:
            headers['User-Agent'] = self.user_agent

        data = {
            "reportIds": report_ids,
            "companies": [{"companyCode": company_code, "companyParentCode": company_parent_code}]
        }

        try:
            logger.info(f"发送API请求，报表ID: {report_ids}")
            response = self.session.post(
                self.base_url,
                params=params,
                headers=headers,
                json=data,
                verify=False,
                timeout=300
            )

            response.raise_for_status()
            result = response.json()

            logger.info(f"API请求成功，状态码: {response.status_code}")
            return result

        except RequestException as e:
            logger.error(f"API请求失败: {e}")
            raise RequestException(f"API请求失败: {e}")
        except Exception as e:
            logger.error(f"处理API响应时出错: {e}")
            raise Exception(f"处理API响应时出错: {e}")

    def get_tasks(self) -> List[Dict[str, Any]]:
        """获取任务列表"""
        url = f"{self.report_url}/current_task/list"
        headers = self._get_request_headers()

        try:
            logger.info("获取任务列表...")
            resp = self.session.post(url, headers=headers, json={}, verify=False)
            resp.raise_for_status()
            result = resp.json().get("result", [])
            logger.info(f"成功获取 {len(result)} 个任务")
            return result
        except Exception as e:
            logger.error(f"获取任务列表失败: {e}")
            raise

    def get_period_details(self, period_id: str) -> List[Dict[str, Any]]:
        """获取月份列表（需要任务里的 periodId）"""
        url = f"{self.report_url}/period/queryDetail"
        headers = self._get_request_headers()
        params = {"periodId": period_id}

        try:
            logger.info(f"获取月份列表，periodId: {period_id}")
            resp = self.session.get(url, headers=headers, params=params, verify=False)
            resp.raise_for_status()
            result = resp.json()
            if isinstance(result, list):
                logger.info(f"成功获取 {len(result)} 个月份")
                return result
            elif isinstance(result, dict) and "result" in result:
                periods = result["result"]
                logger.info(f"成功获取 {len(periods)} 个月份")
                return periods
            else:
                logger.info(f"成功获取月份数据")
                return result if isinstance(result, list) else [result]
        except Exception as e:
            logger.error(f"获取月份列表失败: {e}")
            raise

    def get_reports(self, company_code: str, period_detail_id: str, task_id: str) -> List[Dict[str, Any]]:
        """获取报表列表"""
        url = f"{self.report_url}/query_output/report_list"
        headers = self._get_request_headers()
        params = {
            "companyCode": company_code,
            "companyParentCode": "",
            "groupId": "",
            "periodDetailId": period_detail_id,
            "taskId": task_id,
        }

        try:
            # logger.info(f"获取报表列表，公司: {company_code}, 月份: {period_detail_id}, 任务: {task_id}")
            resp = self.session.get(url, headers=headers, params=params, verify=False)
            resp.raise_for_status()
            result = resp.json()
            if isinstance(result, list):
                # logger.info(f"成功获取 {len(result)} 个报表")
                return result
            elif isinstance(result, dict) and "result" in result:
                reports = result["result"]
                # logger.info(f"成功获取 {len(reports)} 个报表")
                return reports
            else:
                # logger.info(f"成功获取报表数据")
                return result if isinstance(result, list) else [result]
        except Exception as e:
            logger.error(f"获取报表列表失败: {e}")
            raise

    def get_companies(self, task_id: str, period_detail_id: str) -> List[Dict[str, Any]]:
        """获取单位树结构"""
        url = f"{self.report_url}/company/all_for_parent_tree"
        headers = self._get_request_headers()
        params = {
            "TIMESTAMP": int(time.time() * 1000),
            "TOKEN": self.token or "",
            "groupId": "",
            "taskId": task_id,
            "periodDetailId": period_detail_id,
        }

        try:
            logger.info(f"获取单位树结构，任务: {task_id}, 月份: {period_detail_id}")
            resp = self.session.get(url, headers=headers, params=params, verify=False)
            resp.raise_for_status()
            result = resp.json()
            if isinstance(result, dict) and "result" in result:
                companies = [result["result"][0]]
                logger.info(f"成功获取单位树结构，包含 {len(companies)} 个顶级单位")
                return companies
            elif isinstance(result, list):
                logger.info(f"成功获取单位树结构，包含 {len(result)} 个单位")
                return result
            else:
                logger.info(f"成功获取单位数据")
                return [result] if result else []
        except Exception as e:
            logger.error(f"获取单位树结构失败: {e}")
            raise

    def _extract_all_companies(self, companies: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
        """从树结构中提取所有公司的ID和父ID"""
        result = []

        def extract_recursive(company_list: List[Dict[str, Any]]):
            for company in company_list:
                company_id = company.get("id") or company.get("SCOMPANY_CODE")
                parent_id = company.get("parentId") or company.get("SPARENT_CODE")

                if company_id and parent_id:
                    result.append((company_id, parent_id))

                children = company.get("children", [])
                if children:
                    extract_recursive(children)

        extract_recursive(companies)
        logger.info(f"从单位树中提取出 {len(result)} 个公司信息")
        return result

    def parse_table_data(self, api_response: Dict[str, Any]) -> List[List[str]]:
        try:
            result = api_response.get("result", [])
            if not result:
                logger.warning("API响应中未找到result数据")
                return []

            all_rows = []

            for item in result:
                data = item.get("formatData", {}).get("data", {})
                data_table = data.get("dataTable", {})

                if not data_table:
                    continue

                rows = [data_table[key] for key in sorted(data_table.keys(), key=int)]

                for row in rows:
                    cols = [str(row[col_key]["value"]) for col_key in sorted(row.keys(), key=int)]
                    all_rows.append(cols)

            logger.info(f"成功解析表格数据，共{len(all_rows)}行")
            return all_rows

        except Exception as e:
            logger.error(f"解析表格数据时出错: {e}")
            return []

    def get_all_data_by_task(self, task_name_filter: str = None, filter_quarterly_monthly: bool = True,
                             tasks_list: List[Dict[str, Any]] = None,
                             save_callback: callable = None) -> Dict[str, Any]:
        """
        根据任务名称获取所有相关数据，支持逐个单位处理和存储
        :param task_name_filter: 任务名称筛选条件，如果为None则使用第一个任务
        :param filter_quarterly_monthly: 是否筛选季报月报任务，默认为True
        :param tasks_list: 预先获取的任务列表，如果提供则不会重复获取
        :param save_callback: 获取到单位数据后的回调函数，用于立即处理和存储数据
        :return: 包含处理统计信息的字典
        """
        try:
            logger.info("开始获取所有数据...")

            if tasks_list is not None:
                logger.info(f"使用预先提供的任务列表，共 {len(tasks_list)} 个任务")
                tasks = tasks_list
            else:
                tasks = self.get_tasks()
                logger.info(f"获取到 {len(tasks)} 个任务")

            if not tasks:
                raise ValueError("未找到任何任务")

            selected_tasks = []

            if filter_quarterly_monthly and tasks_list is None:
                pattern = re.compile(r'.*[季月]报.*', re.IGNORECASE)
                logger.info("应用季报月报筛选条件...")

                for task in tasks:
                    task_name = task.get("taskName", "")
                    if pattern.match(task_name):
                        selected_tasks.append(task)
                        logger.info(f"找到匹配的季报/月报任务: {task_name}")

                if not selected_tasks:
                    logger.warning("未找到包含季报或月报的任务")
                    if not task_name_filter:
                        # 如果没有找到季报月报任务且没有指定特定任务名，返回空结果
                        return {
                            "message": "未找到季报或月报任务",
                            "tasks_found": [],
                            "selected_task": None,
                            "periods": [],
                            "companies": [],
                            "company_pairs": [],
                            "processed_count": 0,
                            "success_count": 0,
                            "error_count": 0
                        }
            else:
                selected_tasks = tasks

            final_task = None
            if task_name_filter:
                logger.info(f"应用任务名称筛选条件: {task_name_filter}")
                for task in selected_tasks:
                    if task_name_filter in task.get("taskName", ""):
                        final_task = task
                        break
                if not final_task:
                    logger.warning(f"未找到包含'{task_name_filter}'的任务，使用第一个匹配的任务")
                    final_task = selected_tasks[0] if selected_tasks else None
            else:
                final_task = selected_tasks[0] if selected_tasks else None

            if not final_task:
                raise ValueError("未找到符合条件的任务")

            logger.info(f"最终选择任务: {final_task.get('taskName', '未知任务')}")

            task_id = final_task["id"]
            period_id = final_task["periodId"]
            group_id = final_task.get("groupId", "")
            logger.info(f"任务ID: {task_id}, 月份ID: {period_id}, 组ID: {group_id}")

            logger.info("开始获取月份列表...")
            # 获取月份列表
            periods = self.get_period_details(period_id)
            if not periods:
                raise ValueError("未找到任何月份数据")
            logger.info(f"获取到 {len(periods)} 个月份")

            logger.info("开始获取单位树结构...")
            # 获取单位树结构
            period_detail_id = periods[0]["id"]
            companies = self.get_companies(task_id, period_detail_id)
            if not companies:
                raise ValueError("未找到任何单位数据")
            logger.info(f"获取到 {len(companies)} 个顶级单位")

            # 提取所有公司信息
            company_pairs = self._extract_all_companies(companies)
            logger.info(f"提取到 {len(company_pairs)} 个公司信息")

            # 统计信息
            processed_count = 0
            success_count = 0
            error_count = 0

            if save_callback:
                base_data = {
                    "task": final_task,
                    "tasks_found": selected_tasks,
                    "periods": periods,
                    "companies": companies,
                    "company_pairs": company_pairs
                }

                try:
                    save_callback(base_data, data_type="metadata")
                    logger.info("基础元数据已保存")
                except Exception as e:
                    logger.error(f"保存基础元数据失败: {e}")

            # 为每个月份和每个公司获取报表数据并立即处理
            logger.info("开始逐个获取和处理报表数据...")
            for period in periods:
                period_detail_id = period["id"]
                period_name = period.get("periodDetailName", "未知月份")

                for company_id, parent_id in company_pairs:
                    processed_count += 1

                    try:
                        if save_callback:
                            try:
                                from database.database_manager import DataBaseManager
                                db_manager = DataBaseManager()

                                if db_manager.check_financial_report_data_exists(company_id, period_detail_id,
                                                                                 "raw_financial_reports"):
                                    logger.info(
                                        f"财务报表数据已存在，跳过处理 - 单位: {company_id}, 期间: {period_name}")
                                    success_count += 1
                                    continue
                            except Exception as check_error:
                                logger.warning(f"检查数据存在性时发生错误: {check_error}，继续处理")

                        reports = self.get_reports(company_id, period_detail_id, task_id)

                        if reports:
                            report_ids = [report.get("reportId") for report in reports if report.get("reportId")]

                            if report_ids:
                                report_data = self._make_api_request(report_ids, company_id, parent_id)
                                report_result = self.parse_table_data(report_data)

                                # 构建单个单位的报表数据
                                single_unit_data = {
                                    "period_name": period_name,
                                    "period_detail_id": period_detail_id,
                                    "company_id": company_id,
                                    "parent_id": parent_id,
                                    "reports": reports,
                                    "report_data": report_result
                                }

                                if save_callback:
                                    try:
                                        save_callback(single_unit_data, data_type="report_data")
                                        success_count += 1
                                        logger.info(f"成功处理并保存 {period_name} - {company_id} 的报表数据")
                                    except Exception as callback_error:
                                        error_count += 1
                                        logger.error(
                                            f"回调函数处理 {period_name} - {company_id} 数据失败: {callback_error}")
                                else:
                                    success_count += 1
                                    logger.info(f"成功获取 {period_name} - {company_id} 的报表数据")

                    except Exception as e:
                        error_count += 1
                        logger.warning(f"获取 {period_name} - {company_id} 的报表数据失败: {e}")
                        continue

            result = {
                "task": final_task,
                "tasks_found": selected_tasks,
                "periods": periods,
                "companies": companies,
                "company_pairs": company_pairs,
                "processed_count": processed_count,
                "success_count": success_count,
                "error_count": error_count,
                "message": f"数据处理完成，共处理 {processed_count} 个单位，成功 {success_count} 个，失败 {error_count} 个"
            }

            logger.info(
                f"完成所有数据获取和处理，共处理 {processed_count} 个单位，成功 {success_count} 个，失败 {error_count} 个")
            return result

        except Exception as e:
            logger.error(f"获取所有数据失败: {e}")
            raise Exception(f"获取所有数据失败: {e}")

    def get_quarterly_monthly_tasks(self) -> List[Dict[str, Any]]:
        """
        专门获取季报月报任务列表
        :return: 季报月报任务列表
        """
        if not self.access_token:
            logger.info("未登录，开始自动登录...")
        if not self.login_and_get_tokens():
            raise ValueError("自动登录失败")
        try:
            logger.info("开始获取季报月报任务...")
            tasks = self.get_tasks()

            if not tasks:
                logger.warning("未获取到任何任务")
                return []

            # 使用正则匹配筛选包含季报或月报的任务
            pattern = re.compile(r'.*(?:202[4-9]|20[3-9]\d|2[1-9]\d{2}|\d{5,}).*[季月]报.*', re.IGNORECASE)
            matched_tasks = []

            for task in tasks:
                task_name = task.get("taskName", "")
                if pattern.match(task_name):
                    matched_tasks.append(task)
                    logger.info(f"找到匹配任务: {task_name}")

            logger.info(f"共找到 {len(matched_tasks)} 个季报月报任务")
            return matched_tasks

        except Exception as e:
            logger.error(f"获取季报月报任务失败: {e}")
            return []


def create_auto_financial_api(username: str, password: str) -> AutoFinancialReportAPI:
    return AutoFinancialReportAPI(username, password)


# ==================== 统一的登录客户端 ====================
class UnifiedLoginClient:
    """统一的登录客户端，用于组织架构、资金流水、报账单等系统的登录"""

    def __init__(self, base_url: str = "http://10.3.102.173", login_key: str = "10030",
                 password: str = "PhhLGbYaZgsvNKJ2YjHF3A=="):
        self.base_url = base_url.rstrip('/')
        self.login_key = login_key
        self.password = password
        self.session = requests.Session()
        self.authkey: Optional[str] = None

        # 设置基础请求头
        self.headers = {
            "Connection": "keep-alive",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/97.0.4692.71 Safari/537.36",
            "Referer": f"{self.base_url}/",
            "Content-Type": "application/x-www-form-urlencoded"
        }

    def login(self) -> bool:
        """
        登录系统获取认证信息
        
        Returns:
            bool: 登录成功返回True，失败返回False
        """
        login_url = f"{self.base_url}/sys/auth/login"

        login_data = {
            "loginKey": self.login_key,
            "password": self.password
        }

        try:
            logger.info(f"正在尝试登录，用户名: {self.login_key}")
            response = self.session.post(login_url, headers=self.headers, data=login_data, verify=False, timeout=30)
            response.raise_for_status()

            result = response.json()
            if result.get("code") == 0:
                # 登录成功，设置默认的管理员认证信息
                self.authkey = "PiK0iSYIrs559DnX1Wcr3UOaWIKk0hNd5yhIaknVf7DGSVuPJUJpFZ9tRsS0ZUdB"

                # 更新请求头中的Cookie
                from requests.utils import dict_from_cookiejar
                cookies = dict_from_cookiejar(self.session.cookies)
                cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
                self.headers["Cookie"] = cookie_str

                logger.info("登录成功")
                return True
            else:
                logger.error(f"登录失败: {result.get('msg', '未知错误')}")
                return False

        except requests.exceptions.RequestException as e:
            logger.error(f"登录请求失败: {e}")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"解析登录响应JSON失败: {e}")
            return False
        except Exception as e:
            logger.error(f"登录过程中发生未知错误: {e}")
            return False

    def is_logged_in(self) -> bool:
        """检查是否已登录"""
        return self.authkey is not None

    def get_authkey(self) -> Optional[str]:
        """获取认证密钥"""
        return self.authkey

    def get_session(self) -> requests.Session:
        """获取会话对象"""
        return self.session

    def get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        return self.headers.copy()


# ==================== 组织架构API客户端 ====================
class OrgAPIClient:
    """组织架构API客户端"""

    def __init__(self, login_client: UnifiedLoginClient):
        self.login_client = login_client
        self.base_url = login_client.base_url

    def get_children_by_id(self, dept_id: str) -> Optional[Dict]:
        """
        获取指定部门ID下的子部门
        
        Args:
            dept_id: 部门ID，0表示根部门
            
        Returns:
            包含子部门信息的字典，失败返回None
        """
        if not self.login_client.is_logged_in():
            raise ValueError("未登录，请先调用login_client.login()")

        url = f"{self.base_url}/sys/common/dept/findChildrenById/{dept_id}"
        params = {"time": int(time.time())}

        headers = self.login_client.get_headers()
        headers["authkey"] = self.login_client.get_authkey()

        try:
            response = self.login_client.get_session().get(url, headers=headers, params=params, verify=False,
                                                           timeout=30)
            response.raise_for_status()

            data = response.json()
            if data.get("code") == 0:
                return data
            else:
                logger.error(f"获取部门 {dept_id} 失败: {data.get('msg', '未知错误')}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"请求部门 {dept_id} 时发生错误: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"解析部门 {dept_id} 响应JSON失败: {e}")
            return None

    def get_dept_page(self, parent_id: str, page: int = 1, limit: int = 100) -> Optional[Dict]:
        """
        分页获取部门信息（包含人员）
        
        Args:
            parent_id: 父部门ID
            page: 页码
            limit: 每页数量
            
        Returns:
            包含分页数据的字典，失败返回None
        """
        if not self.login_client.is_logged_in():
            raise ValueError("未登录，请先调用login_client.login()")

        url = f"{self.base_url}/sys/common/dept/page"

        headers = self.login_client.get_headers()
        headers.update({
            "Content-Type": "application/x-www-form-urlencoded",
            "authkey": self.login_client.get_authkey()
        })

        data = {
            "page": page,
            "limit": limit,
            "orderField": "",
            "order": "",
            "pid": parent_id
        }

        try:
            response = self.login_client.get_session().post(url, headers=headers, data=data, verify=False, timeout=30)
            response.raise_for_status()

            result = response.json()
            if result.get("code") == 0:
                return result
            else:
                logger.error(f"获取部门 {parent_id} 分页数据失败: {result.get('msg', '未知错误')}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"请求部门 {parent_id} 分页数据时发生错误: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"解析部门 {parent_id} 分页响应JSON失败: {e}")
            return None


# ==================== 资金流水API客户端 ====================
class FlowAPIClient:
    """资金流水API客户端"""

    def __init__(self, login_client: UnifiedLoginClient, page_size: int = 100, request_timeout: int = 30):
        self.login_client = login_client
        self.base_url = login_client.base_url
        self.page_size = page_size
        self.request_timeout = request_timeout

    def fetch_page(self, page_index: int) -> Tuple[int, int, List[Dict]]:
        """
        拉取单页数据
        
        Args:
            page_index: 页码
            
        Returns:
            (currPage, totalPage, list)
        """
        if not self.login_client.is_logged_in():
            raise ValueError("未登录，请先调用login_client.login()")

        url = f"{self.base_url}/sys/claim/claimcapitalflow/page"
        headers = self.login_client.get_headers()
        headers["authkey"] = self.login_client.get_authkey()

        form = {
            "page": str(page_index),
            "limit": str(self.page_size),
            "orderField": "",
            "order": "",
            "menuType": "0",
        }

        try:
            resp = self.login_client.get_session().post(
                url,
                headers=headers,
                data=form,
                timeout=self.request_timeout,
                verify=False,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 0:
                raise RuntimeError(f"接口返回异常: {data}")

            page_info = data.get("page", {})
            curr = int(page_info.get("currPage", 1))
            total = int(page_info.get("totalPage", 1))
            items = page_info.get("list", []) or []
            return curr, total, items
        except Exception as e:
            logger.error(f"获取资金流水第 {page_index} 页数据失败: {e}")
            raise


# ==================== 报账单API客户端 ====================
class BoeAPIClient:
    """报账单API客户端"""

    def __init__(self, login_client: UnifiedLoginClient):
        self.login_client = login_client
        self.base_url = login_client.base_url

    def get_query_fields(self, report_code: str = "zfsboestatementvPage.ureport.xml") -> Optional[Dict]:
        """
        获取报表查询字段信息
        
        Args:
            report_code: 报表代码
            
        Returns:
            包含查询字段信息的字典，失败返回None
        """
        if not self.login_client.is_logged_in():
            raise ValueError("未登录，请先调用login_client.login()")

        url = f"{self.base_url}/sys/report/reportmanager/findReportWithQueryFieldByCode"
        params = {"reportCode": report_code}

        headers = self.login_client.get_headers()
        headers.update({
            "pageUri": f"#/reportDetail?reportCode={report_code}",
            "authkey": self.login_client.get_authkey(),
            "x-frame-options": "allow-from http://10.3.92.33/finebi/decision/link/CCga",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/report/"
        })

        try:
            logger.info(f"正在获取报表查询字段信息: {report_code}")
            response = self.login_client.get_session().post(url, headers=headers, params=params, verify=False,
                                                            timeout=30)
            response.raise_for_status()

            data = response.json()
            if data.get("code") == 0:
                logger.info(f"成功获取查询字段信息")
                return data
            else:
                logger.error(f"获取查询字段失败: {data.get('msg', '未知错误')}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"请求查询字段时发生错误: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"解析查询字段响应JSON失败: {e}")
            return None

    def get_report_data(self, start_date: str = None, end_date: str = None, page_size: int = 100, page: int = 1,
                        report_code: str = "zfsboestatementvPage.ureport.xml") -> Optional[Dict]:
        """
        获取报表数据
        
        Args:
            start_date: 开始日期，格式：2025-7-19 00:00:00
            end_date: 结束日期，格式：2025-10-16 23:59:59
            page_size: 每页数量
            page: 页码
            report_code: 报表代码
            
        Returns:
            包含报表数据的字典，失败返回None
        """
        if not self.login_client.is_logged_in():
            raise ValueError("未登录，请先调用login_client.login()")

        from datetime import datetime, timedelta
        from bs4 import BeautifulSoup

        # 如果没有提供日期，默认查询最近3个月的数据
        if not start_date or not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S")

        url = f"{self.base_url}/sys/report/ureport/zfsCustomReport/previewReport"

        # 构建查询参数
        params = {
            "reportName": "单据查询报表",
            "reportCode": report_code,
            "_u": f"database:{report_code}",
            "startBoeDate": start_date,
            "endBoeDate": end_date,
            "st": int(time.time() * 1000),
            "_i": page,
            "zfsReportPageSize_": page_size,
            "zfs_rptReportType_": "XY_REPORT_001002_TYPE_003"
        }

        headers = self.login_client.get_headers()
        headers.update({
            "Upgrade-Insecure-Requests": "1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
                      "*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Referer": f"{self.base_url}/report/"
        })

        try:
            logger.info(f"正在获取第 {page} 页报表数据，日期范围: {start_date} 到 {end_date}")
            response = self.login_client.get_session().get(url, headers=headers, params=params, verify=False,
                                                           timeout=30)
            response.raise_for_status()

            # 解析HTML响应
            soup = BeautifulSoup(response.text, 'html.parser')

            # 提取分页信息
            page_info = self.extract_page_info(soup)

            # 提取表格数据
            table_data = self.extract_table_data(soup)

            result = {
                "page_info": page_info,
                "data": table_data,
                "current_page": page,
                "page_size": page_size,
                "query_params": params
            }

            logger.info(f"成功获取第 {page} 页数据，共 {len(table_data)} 条记录")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"请求报表数据时发生错误: {e}")
            return None
        except Exception as e:
            logger.error(f"解析报表数据时发生错误: {e}")
            return None

    def get_full_report_by_boeno(self, boe_no: str, report_code: str = "zfsboestatementvPage.ureport.xml") -> (
            Optional)[Dict]:
        """
        通过 boeNo 调用全量单据接口并解析页面，返回包含 boeHeaderId 的单条数据
        
        Args:
            boe_no: 单据编号
            report_code: 报表代码
            
        Returns:
            包含boeHeaderId的单条数据，失败返回None
        """
        if not self.login_client.is_logged_in():
            raise ValueError("未登录，请先调用login_client.login()")

        from bs4 import BeautifulSoup
        from urllib.parse import urlparse, parse_qs

        url = f"{self.base_url}/sys/report/ureport/zfsCustomReport/previewReport"

        params = {
            "boeNo": boe_no,
            "_u": f"database:{report_code}"
        }

        headers = self.login_client.get_headers()
        headers.update({
            "Upgrade-Insecure-Requests": "1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
                      "*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Referer": f"{self.base_url}/report/"
        })

        try:
            logger.info(f"通过 boeNo 获取全量单据: {boe_no}")
            response = self.login_client.get_session().get(url, headers=headers, params=params, verify=False,
                                                           timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # 解析表格
            table = soup.find('table')
            if not table:
                logger.warning(f"boeNo={boe_no} 未找到数据表格")
                return None

            headers_row = table.find('tr')
            headers_list: List[str] = []
            if headers_row:
                for th in headers_row.find_all('td'):
                    headers_list.append(th.get_text(strip=True))

            data_rows = table.find_all('tr')[1:]
            if not data_rows:
                logger.warning(f"boeNo={boe_no} 未返回数据行")
                return None

            first_row = data_rows[0]
            row_cells = first_row.find_all('td')
            row_data: Dict[str, any] = {}
            for i, cell in enumerate(row_cells):
                if i < len(headers_list):
                    link = cell.find('a')
                    if link:
                        row_data[headers_list[i]] = {
                            "text": cell.get_text(strip=True),
                            "link": link.get('href', ''),
                            "link_text": link.get_text(strip=True)
                        }
                    else:
                        row_data[headers_list[i]] = cell.get_text(strip=True)

            # 从 a 链接中提取 boeHeaderId
            boe_header_id: Optional[str] = None
            anchor = first_row.find('a')
            if anchor and anchor.get('href'):
                href = anchor.get('href')
                try:
                    parsed = urlparse(href)
                    boe_header_id = (parse_qs(parsed.query).get('boeHeaderId') or [None])[0]
                except Exception as e:
                    logger.error(f"解析 boeHeaderId 时发生错误: {e}")
                    boe_header_id = None

            if not boe_header_id:
                logger.warning(f"boeNo={boe_no} 未解析到 boeHeaderId")

            result = {
                "boeNo": boe_no,
                "boeHeaderId": boe_header_id,
                "row": row_data
            }

            logger.info(f"全量单据解析成功: boeNo={boe_no}, boeHeaderId={boe_header_id}")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"全量单据请求失败 boeNo={boe_no}: {e}")
            return None
        except Exception as e:
            logger.error(f"全量单据解析失败 boeNo={boe_no}: {e}")
            return None

    def get_boe_detail(self, boe_no: str, boe_header_id: str, key: str = "0MQ/TJHOFRPXdpVU7NxEQN6a+Tjjv0WP") -> \
            Optional[Dict]:
        """
        获取单据详情数据
        
        Args:
            boe_no: 单据编号
            boe_header_id: 单据头ID
            key: 详情接口所需的key
            
        Returns:
            包含单据详情的字典，失败返回None
        """
        if not boe_header_id:
            logger.warning(f"获取详情时缺少 boeHeaderId，boeNo={boe_no}")
            return None

        url = f"{self.base_url}/sys/boe/core/editDraftBoe"
        payload = {
            "boeNo": boe_no,
            "boeHeaderId": boe_header_id,
            "key": key
        }

        try:
            logger.info(f"请求单据详情 boeNo={boe_no}, boeHeaderId={boe_header_id}")
            response = requests.post(url, data=payload, timeout=300)
            response.raise_for_status()

            # 期望返回 JSON
            try:
                data = response.json()
            except json.JSONDecodeError:
                # 有些情况下返回文本里内含 JSON 字符串，尝试兜底
                text = response.text
                if text.strip().startswith("{") and text.strip().endswith("}"):
                    data = json.loads(text)
                else:
                    logger.error("详情接口未返回JSON格式")
                    return None

            result = {
                "boeNo": boe_no,
                "boeHeaderId": boe_header_id,
                "data": data
            }
            logger.info(f"单据详情获取成功 boeNo={boe_no}")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"请求单据详情失败 boeNo={boe_no}: {e}")
            return None
        except Exception as e:
            logger.error(f"解析单据详情失败 boeNo={boe_no}: {e}")
            return None

    def extract_page_info(self, soup) -> Dict:
        """提取分页信息"""

        page_info = {
            "current_page": 1,
            "total_pages": 1,
            "total_count": 0
        }

        try:
            # 查找分页信息
            page_index_elem = soup.find('span', {'id': 'reportPageIndex'})
            total_page_elem = soup.find('span', {'id': 'reportTotalPage'})
            total_count_elem = soup.find('span', {'id': 'reportTotalCount'})

            if page_index_elem:
                page_info["current_page"] = int(page_index_elem.text.strip())
            if total_page_elem:
                page_info["total_pages"] = int(total_page_elem.text.strip())
            if total_count_elem:
                page_info["total_count"] = int(total_count_elem.text.strip())

        except Exception as e:
            logger.warning(f"提取分页信息时发生错误: {e}")

        return page_info

    def extract_table_data(self, soup) -> List[Dict]:
        """提取表格数据"""

        table_data = []

        try:
            # 查找表格
            table = soup.find('table')
            if not table:
                logger.warning("未找到数据表格")
                return table_data

            # 获取表头
            headers = []
            header_row = table.find('tr')
            if header_row:
                for th in header_row.find_all('td'):
                    headers.append(th.get_text(strip=True))

            # 获取数据行
            data_rows = table.find_all('tr')[1:]  # 跳过表头

            for row in data_rows:
                row_data = {}
                cells = row.find_all('td')

                for i, cell in enumerate(cells):
                    if i < len(headers):
                        # 处理链接
                        link = cell.find('a')
                        if link:
                            row_data[headers[i]] = {
                                "text": cell.get_text(strip=True),
                                "link": link.get('href', ''),
                                "link_text": link.get_text(strip=True)
                            }
                        else:
                            row_data[headers[i]] = cell.get_text(strip=True)

                if row_data:  # 只添加非空行
                    table_data.append(row_data)

        except Exception as e:
            logger.error(f"提取表格数据时发生错误: {e}")

        return table_data
