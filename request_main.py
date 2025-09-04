from typing import List, Dict, Any

import requests


class ReportAPI:
    def __init__(self, base_url: str, token: str, x_access_token: str, cookies: str, verify_ssl: bool = False):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.x_access_token = x_access_token
        self.cookies = cookies
        self.session = requests.Session()
        self.verify_ssl = verify_ssl

        # 通用请求头
        self.headers = {
            "Connection": "keep-alive",
            "Accept": "application/json, text/plain, */*",
            "X-Access-Token": self.x_access_token,
            "X-Access-Token-Old": self.token,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/97.0.4692.71 Safari/537.36",
            "Content-Type": "application/json;charset=UTF-8",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cookie": self.cookies,
        }

    def get_tasks(self) -> List[Dict[str, Any]]:
        """获取任务列表"""
        url = f"{self.base_url}/current_task/list"
        resp = self.session.post(url, headers=self.headers, json={}, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp.json().get("result", [])

    def get_period_details(self, period_id: str) -> List[Dict[str, Any]]:
        """获取月份列表（需要任务里的 periodId）"""
        url = f"{self.base_url}/period/queryDetail"
        params = {"periodId": period_id}
        resp = self.session.get(url, headers=self.headers, params=params, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp.json()

    def get_reports(self, company_code: str, period_detail_id: str, task_id: str,
                    group_id: str = "") -> List[Dict[str, Any]]:
        """获取报表列表"""
        url = f"{self.base_url}/query_output/report_list"
        params = {
            "companyCode": company_code,
            "companyParentCode": "",
            "groupId": group_id,
            "periodDetailId": period_detail_id,
            "taskId": task_id,
        }
        resp = self.session.get(url, headers=self.headers, params=params, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp.json()

    def get_companies(self, task_id: str, period_detail_id: str, group_id: str = "") -> Dict[str, Any]:
        """获取单位树结构"""
        url = f"{self.base_url}/company/all_for_parent_tree"
        params = {
            "groupId": group_id,
            "taskId": task_id,
            "periodDetailId": period_detail_id,
        }
        resp = self.session.get(url, headers=self.headers, params=params, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp.json().get("result", [])


# 使用示例
if __name__ == "__main__":
    BASE_URL = "http://10.3.102.141/shj/vue/api/rp"
    TOKEN = "b10f6d285eff402083dc2fc9db8abf85"
    X_ACCESS_TOKEN = "你的X-Access-Token"
    COOKIES = "tt=xxx; JSESSIONID=xxx"

    api = ReportAPI(BASE_URL, TOKEN, X_ACCESS_TOKEN, COOKIES, verify_ssl=False)

    # 获取任务
    tasks = api.get_tasks()
    print("任务列表:", tasks)

    if tasks:
        task = tasks[0]
        period_id = task["periodId"]
        task_id = task["id"]

        # 获取月份
        periods = api.get_period_details(period_id)
        print("月份列表:", periods)

        if periods:
            period_detail_id = periods[0]["id"]

            # 获取单位
            companies = api.get_companies(task_id, period_detail_id)
            print("单位:", companies)

            # 获取报表
            company_code = companies[0]["id"] if companies else "2SH0000001"
            reports = api.get_reports(company_code, period_detail_id, task_id)
            print("报表:", reports)
