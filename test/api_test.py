import json
import logging
import os
import subprocess
import sys
import time
import requests
from typing import Dict, List, Optional, Tuple, Union, Any
from requests import RequestException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def setup_logging() -> None:
    """配置日志系统"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('app.log', encoding='utf-8')
        ]
    )


setup_logging()

logger = logging.getLogger(__name__)


def setup_chrome_options():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-web-security')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--ignore-certificate-errors')

    # 开启性能日志
    options.add_argument('--enable-logging')
    options.add_argument('--log-level=0')
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    return options


def check_environment():
    try:
        chromedriver_path = "/usr/local/bin/chromedriver"
        if os.path.exists(chromedriver_path):
            logger.info(f"✅ ChromeDriver 存在: {chromedriver_path}")
        else:
            logger.error(f"❌ ChromeDriver 不存在: {chromedriver_path}")
            return False

        chrome_paths = [
            '/usr/local/bin/google-chrome',
            '/opt/chrome-linux64/chrome',
            '/usr/bin/google-chrome',
            '/usr/bin/chromium-browser',
            '/usr/bin/chromium',
            '/opt/google/chrome/chrome'
        ]

        chrome_path = None
        for path in chrome_paths:
            if os.path.exists(path):
                chrome_path = path
                logger.info(f"✅ 找到 Chrome: {path}")
                break

        if not chrome_path:
            logger.error("❌ 未找到 Chrome/Chromium")
            return False

        result = subprocess.run([chrome_path, '--version'],
                                capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            logger.info(f"✅ Chrome 版本: {result.stdout.strip()}")
        else:
            logger.warning(f"⚠️ Chrome 执行有警告: {result.stderr}")

        logger.info("✅ 环境检查通过")
        return True
    except Exception as e:
        logger.error(f"环境检查失败: {e}")
        return False


def get_all_request_headers(driver):
    logger.info("正在获取所有请求头...")
    logs = driver.get_log("performance")
    for log in logs:
        try:
            message = json.loads(log["message"])
            msg = message.get("message", {})
            if msg.get("method") == "Network.requestWillBeSent":
                request = msg.get("params", {}).get("request", {})
                headers = request.get("headers", {})
                if headers:
                    logger.info(f"获取到请求头: {headers}")
                    return headers
        except Exception as e:
            logger.warning(f"解析网络日志时出错: {e}")
            continue
    return {}


def get_latest_token(driver) -> Optional[Union[str, Dict[str, str]]]:
    logger.info("正在获取最新的 X-Access-Token...")

    token_sources = [
        "sessionStorage.getItem('X-Access-Token')",
        "sessionStorage.getItem('token')",
        "localStorage.getItem('X-Access-Token')",
        "localStorage.getItem('token')",
        "window.token",
        "window.accessToken"
    ]
    token_dict = {}
    for source in token_sources:
        try:
            token = driver.execute_script(f"return {source};")
            if token:
                token_dict[source] = token
        except Exception as e:
            logger.warning(f"执行脚本获取 token 时出错: {e}")
            continue
    print("从 JS 中获取到的 token:", token_dict)
    return token_dict


def get_automation_data(username: str = "lijin5", password: str = "Qaz.123456789.") \
        -> Tuple[Optional[str], Optional[List[Dict]], Optional[str]]:
    logger.info("=" * 60)
    logger.info("开始执行自动化流程获取数据")
    logger.info("=" * 60)

    if not check_environment():
        logger.error("环境检查失败")
        return None, None, None

    chromedriver_path = "/usr/local/bin/chromedriver"
    service = Service(chromedriver_path)
    options = setup_chrome_options()

    driver = None
    try:
        # 初始化驱动
        driver = webdriver.Chrome(service=service, options=options)
        logger.info("WebDriver 初始化成功")

        # 执行登录和导航流程
        wait = WebDriverWait(driver, 10)
        driver.get('https://caikuai.crc.cr/#/login?redirectModule=')
        logger.info("访问登录页面...")
        time.sleep(10)

        # 关闭弹窗
        close_button = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR,
             "body > div.login > div.el-dialog__wrapper.tip-dialog > div > div.el-dialog__header > button")))
        close_button.click()
        time.sleep(5)

        # 输入账号
        account = wait.until(EC.presence_of_element_located((By.ID, "loginKey")))
        account.send_keys(username)

        # 输入密码
        password_field = driver.find_element(By.ID, "password")
        password_field.send_keys(password)

        # 处理验证码
        try:
            captcha = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "body > div > div.login-pad > div > form > div:nth-child(3) > div > div")))
            captcha_text = captcha.text
            logger.info(f"验证码是: {captcha_text}")
        except Exception as e:
            logger.warning(f"未检测到验证码: {e}")
            captcha_text = input("请输入验证码: ")
        driver.find_element(By.ID, "checkcode").send_keys(captcha_text)

        # 点击登录
        login_button = wait.until(EC.element_to_be_clickable((By.ID, "login")))
        login_button.click()
        logger.info("登录请求已发送...")
        time.sleep(5)

        # 点击大数据按钮
        try:
            big_data_button = wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "body > div.platform > div.container > main > div:nth-child(2) > div"))
            )
            big_data_button.click()
            logger.info("已点击大数据按钮。")
            time.sleep(5)
        except Exception as e:
            logger.error(f"未找到大数据按钮，可能是页面结构已更改。{e}")

        # 点击过程管理
        try:
            all_windows_before = driver.window_handles
            process_management_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#master > section > div:nth-child(2) > div > div > div")))
            process_management_button.click()
            logger.info("已点击过程管理按钮")
            wait.until(lambda driver: len(driver.window_handles) > len(all_windows_before))
            all_windows = driver.window_handles
            logger.info(f"所有窗口数量: {len(all_windows)}")
            new_window = None
            for window in all_windows:
                if window not in all_windows_before:
                    new_window = window
                    break
            if new_window:
                driver.switch_to.window(new_window)
                logger.info("已经切换到新标签页")
                time.sleep(5)
            else:
                logger.error("没找到新的标签")
        except Exception as e:
            logger.error(f"未找到过程管理按钮，可能是页面结构已更改。{e}")

        # 点击切换单位
        try:
            switch_unit_span = wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR,
                 "body > app-root > layout > fc-layoutrow > div > div.fc-layoutrowcell.fc-layoutrowcell1 > "
                 "fcnavbar > header > div.fc-navbar-content > div > span:nth-child(6) > span.analyze")))
            switch_unit_span.click()
            logger.info("点击财务共享中心")
            time.sleep(5)

            # /html/body/div/div[3]/div/nz-modal/div/div[2]/div/div/div[2]/fc-companychange/fc-layoutpanel/div/div[3]/div/div/ag-grid-angular/div/div[2]/div[1]/div[3]/div[2]/div/div/div[1]/div[3]
            shanghai_company_span = wait.until(EC.element_to_be_clickable(
                (By.XPATH,
                 "/html/body/div/div[3]/div/nz-modal/div/div[2]/div/div/div[2]/fc-companychange/fc-layoutpanel/div"
                 "/div[3]/div/div/ag-grid-angular/div/div[2]/div[1]/div[3]/div[2]/div/div/div[1]/div[3]"))
            )
            shanghai_company_span.click()
            logger.info("点击上海局集团公司")
            time.sleep(5)

            confirm_button = wait.until(EC.element_to_be_clickable(
                (By.XPATH,
                 "/html/body/div/div[3]/div/nz-modal/div/div[2]/div/div/div[2]/fc-companychange/div/fc-button[1]/button"
                 )))
            confirm_button.click()
            logger.info("更换成功")
            time.sleep(2)
            title = driver.find_element(By.CSS_SELECTOR, "span.analyze").get_attribute("title")
            logger.info(title)
        except Exception as e:
            logger.error("点击失败：", e)

        # 获取数据
        current_url = driver.current_url
        logger.info(f"当前页面URL: {current_url}")

        # 获取token
        token = get_latest_token(driver)
        logger.info(f"获取到的 token: {token}")

        # 获取cookies
        cookies = driver.get_cookies()
        logger.info(f"获取到 {len(cookies)} 个 cookies")

        # 获取useragent
        user_agent = driver.execute_script("return navigator.userAgent;")
        logger.info(f"获取到的 useragent: {user_agent}")

        # 获取所有请求头
        headers_list = get_all_request_headers(driver)
        logger.info(f"获取到 {len(headers_list)} 个请求头")

        logger.info("🎉 自动化流程执行完成")
        return token, cookies, user_agent

    except Exception as e:
        logger.error(f"自动化流程执行出错: {e}", exc_info=True)
        return None, None, None
    finally:
        if driver:
            driver.quit()
            logger.info("WebDriver 已关闭")


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
        logger.info(headers)
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
                timeout=30
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
            logger.info(f"获取报表列表，公司: {company_code}, 月份: {period_detail_id}, 任务: {task_id}")
            resp = self.session.get(url, headers=headers, params=params, verify=False)
            resp.raise_for_status()
            result = resp.json()
            if isinstance(result, list):
                logger.info(f"成功获取 {len(result)} 个报表")
                return result
            elif isinstance(result, dict) and "result" in result:
                reports = result["result"]
                logger.info(f"成功获取 {len(reports)} 个报表")
                return reports
            else:
                logger.info(f"成功获取报表数据")
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
            logger.info(result)
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

    def get_all_data_by_task(self, task_name_filter: str = None) -> Dict[str, Any]:
        """
        根据任务名称获取所有相关数据
        :param task_name_filter: 任务名称筛选条件，如果为None则使用第一个任务
        :return: 包含所有数据的字典
        """
        if not self.access_token:
            logger.info("未登录，开始自动登录...")
            if not self.login_and_get_tokens():
                raise ValueError("自动登录失败")

        try:
            logger.info("开始获取所有数据...")
            # 获取任务列表
            tasks = self.get_tasks()
            logger.info(f"获取到 {len(tasks)} 个任务")
            if not tasks:
                raise ValueError("未找到任何任务")

            logger.info(f"筛选指定任务: {task_name_filter}")
            # 筛选任务
            selected_task = None
            if task_name_filter:
                for task in tasks:
                    if task_name_filter in task.get("taskName", ""):
                        selected_task = task
                        break
                if not selected_task:
                    logger.warning(f"未找到包含'{task_name_filter}'的任务，使用第一个任务")
                    selected_task = tasks[0]
            else:
                selected_task = tasks[0]

            logger.info(f"选择任务: {selected_task.get('taskName', '未知任务')}")

            task_id = selected_task["id"]
            period_id = selected_task["periodId"]
            group_id = selected_task.get("groupId", "")
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

            all_data = {
                "task": selected_task,
                "periods": periods,
                "companies": companies,
                "company_pairs": company_pairs,
                "reports_data": []
            }

            # 为每个月份和每个公司获取报表数据
            logger.info("开始获取报表数据...")
            for period in periods:
                period_detail_id = period["id"]
                period_name = period.get("periodDetailName", "未知月份")

                for company_id, parent_id in company_pairs:
                    try:

                        reports = self.get_reports(company_id, period_detail_id, task_id)

                        if reports:

                            report_ids = [report.get("reportId") for report in reports if report.get("reportId")]

                            if report_ids:
                                report_data = self._make_api_request(report_ids, company_id, parent_id)

                                report_result = self.parse_table_data(report_data)

                                all_data["reports_data"].append({
                                    "period_name": period_name,
                                    "period_detail_id": period_detail_id,
                                    "company_id": company_id,
                                    "parent_id": parent_id,
                                    "reports": reports,
                                    "report_data": report_result
                                })
                                logger.info(f"成功获取 {period_name} - {company_id} 的报表数据，{all_data}")

                    except Exception as e:
                        logger.warning(f"获取 {period_name} - {company_id} 的报表数据失败: {e}")
                        continue

            logger.info(f"完成所有数据获取，共获取 {len(all_data['reports_data'])} 份报表数据")
            return all_data

        except Exception as e:
            logger.error(f"获取所有数据失败: {e}")
            raise Exception(f"获取所有数据失败: {e}")


if __name__ == '__main__':
    auto_motion_api = AutoFinancialReportAPI()
    all_data = auto_motion_api.get_all_data_by_task(task_name_filter="2025年月报")
