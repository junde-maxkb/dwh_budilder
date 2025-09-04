import json
import requests
import time
from typing import Dict, List, Optional, Any, Tuple
from fake_useragent import UserAgent
from loguru import logger
from pydantic import BaseModel
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

            data = response.json()
            api_response = APIResponse(**data)

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
    def __init__(self, username: str = "lijin5", password: str = "Qaz.123456789."):
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

        self.report_headers = {
            "Connection": "keep-alive",
            "Accept": "application/json, text/plain, */*",
            "X-Access-Token": self.access_token,
            "X-Access-Token-Old": self.token,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/97.0.4692.71 Safari/537.36",
            "Content-Type": "application/json;charset=UTF-8",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cookie": self.cookies,
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
                    if 'X-Access-Token' in key or 'token' in key.lower():
                        self.access_token = value
                        break

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

    def _make_api_request(self, report_ids: List[str], company_code: str = "2SH000303B",
                          company_parent_code: str = "2SH0000001") -> Dict[str, Any]:

        if not self.access_token:
            raise ValueError("未获取到access_token，请先执行登录")

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
                timeout=30
            )

            response.raise_for_status()
            result = response.json()

            logger.info(f"API请求成功，状态码: {response.status_code}")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"API请求失败: {e}")
            raise
        except Exception as e:
            logger.error(f"处理API响应时出错: {e}")
            raise

    def get_financial_status(self, company_code: str = "2SH000303B",
                             company_parent_code: str = "2SH0000001") -> Dict[str, Any]:

        logger.info("获取财务状况数据...")
        return self._make_api_request(["1883402501890777089"], company_code, company_parent_code)

    def get_monthly_report_01(self, company_code: str = "2SH000303B",
                              company_parent_code: str = "2SH0000001") -> Dict[str, Any]:

        logger.info("获取月报01表数据...")
        return self._make_api_request(["1882677349741477890"], company_code, company_parent_code)

    def get_monthly_report_04(self, company_code: str = "2SH000303B",
                              company_parent_code: str = "2SH0000001") -> Dict[str, Any]:
        logger.info("获取月报04表数据...")
        return self._make_api_request(["1882677386643509249"], company_code, company_parent_code)

    def get_all_reports(self, company_code: str = "2SH000303B",
                        company_parent_code: str = "2SH0000001") -> Dict[str, Dict[str, Any]]:

        logger.info("开始获取所有报表数据...")

        results = {}

        try:
            results['financial_status'] = self.get_financial_status(company_code, company_parent_code)
            results['monthly_report_01'] = self.get_monthly_report_01(company_code, company_parent_code)
            results['monthly_report_04'] = self.get_monthly_report_04(company_code, company_parent_code)

            logger.info("所有报表数据获取完成")
            return results

        except Exception as e:
            logger.error(f"获取报表数据时出错: {e}")
            raise

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

    def execute_full_workflow(self, company_code: str = "2SH000303B",
                              company_parent_code: str = "2SH0000001") -> Tuple[bool, Dict[str, Any]]:

        logger.info("开始执行完整的数据获取工作流程...")

        try:
            if not self.login_and_get_tokens():
                return False, {"error": "登录失败"}

            all_reports = self.get_all_reports(company_code, company_parent_code)

            parsed_data = {}
            for report_name, report_data in all_reports.items():
                parsed_data[report_name] = {
                    'raw_data': report_data,
                    'parsed_table': self.parse_table_data(report_data)
                }

            logger.info("完整工作流程执行成功")
            return True, parsed_data
        except Exception as e:
            logger.error(f"执行工作流程时出错: {e}")
