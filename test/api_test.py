import json
import logging
import os
import subprocess
import sys
import time
import requests
from typing import Dict, List, Optional, Tuple, Union, Any
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


if __name__ == '__main__':
    auto_motion_api = AutoFinancialReportAPI()
    auto_motion_api.execute_full_workflow()
