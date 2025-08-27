import json
import requests
from typing import Dict, List, Optional, Any
from fake_useragent import UserAgent
from loguru import logger
from pydantic import BaseModel


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
