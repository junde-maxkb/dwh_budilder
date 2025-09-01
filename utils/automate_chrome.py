import logging
import os
import sys
import time
import json
import requests
import subprocess

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('selenium_project.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


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


# =============================
# 网络日志 & Token 获取
# =============================
def get_all_request_headers(driver):
    logger.info("正在获取所有请求头...")
    headers_list = []
    logs = driver.get_log("performance")
    for log in logs:
        try:
            message = json.loads(log["message"])
            msg = message.get("message", {})
            if msg.get("method") == "Network.requestWillBeSent":
                request = msg.get("params", {}).get("request", {})
                url = request.get("url", "")
                headers = request.get("headers", {})
                if headers:
                    headers_list.append({
                        "url": url,
                        "headers": headers
                    })
        except Exception as e:
            logger.warning(f"解析日志时出错: {e}")
            continue
    return headers_list


def get_latest_token(driver):
    logger.info("正在获取最新的 X-Access-Token...")
    time.sleep(2)
    logs = driver.get_log('performance')
    for log in reversed(logs):
        try:
            message = json.loads(log['message'])
            if message.get('message', {}).get('method') == 'Network.requestWillBeSent':
                headers = message.get('message', {}).get('params', {}).get('request', {}).get('headers', {})
                if 'X-Access-Token' in headers:
                    return headers['X-Access-Token']
        except Exception as e:
            logger.warning(f"解析日志时出错: {e}")
            continue

    # 回退方案：从 JS 中取
    token_sources = [
        "sessionStorage.getItem('X-Access-Token')",
        "sessionStorage.getItem('token')",
        "localStorage.getItem('X-Access-Token')",
        "localStorage.getItem('token')",
        "window.token",
        "window.accessToken"
    ]
    for source in token_sources:
        try:
            token = driver.execute_script(f"return {source};")
            if token:
                return token
        except Exception as e:
            logger.warning(f"执行脚本获取 token 时出错: {e}")
            continue
    return None


class DynamicTokenSession:
    def __init__(self, driver, base_session):
        self.driver = driver
        self.session = base_session

    def get_fresh_token(self):
        return get_latest_token(self.driver)

    def request(self, method, url, **kwargs):
        fresh_token = self.get_fresh_token()
        if fresh_token:
            self.session.headers.update({'X-Access-Token': fresh_token})
            logger.info(f"更新 X-Access-Token: {fresh_token}")
        return self.session.request(method, url, **kwargs)

    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)


def session(driver):
    selenium_cookies = driver.get_cookies()
    cookies_dict = {c['name']: c['value'] for c in selenium_cookies}
    sessions = requests.Session()
    sessions.cookies.update(cookies_dict)

    user_agent = driver.execute_script("return navigator.userAgent;")
    sessions.headers.update({
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": driver.current_url,
    })
    return sessions


# =============================
# 实际业务流程
# =============================
def run_project_flow(driver):
    wait = WebDriverWait(driver, 10)
    driver.get('https://caikuai.crc.cr/#/login?redirectModule=')
    logger.info("访问登录页面...")
    time.sleep(10)

    close_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,
                                                              "body > div.login > div.el-dialog__wrapper.tip-dialog > "
                                                              "div > div.el-dialog__header > button")))
    close_button.click()
    time.sleep(5)

    # 输入账号
    account = wait.until(EC.presence_of_element_located((By.ID, "loginKey")))
    account.send_keys("lijin5")

    # 输入密码
    password = driver.find_element(By.ID, "password")
    password.send_keys("Qaz.123456789.")

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

    # 点击大数据(图片类型) class 为：protal-icon 、selector 为： body > div.platform>div.container > main> div:nth-child(2) >
    # div > div > div:nth-child(2) > div
    try:
        big_data_button = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "body > div.platform > div.container > main > div:nth-child(2) > div"))
        )
        big_data_button.click()
        print("已点击大数据按钮。")
        time.sleep(5)
    except Exception as e:
        print("未找到大数据按钮，可能是页面结构已更改。", e)

    # 点击过程管理 selector 为 #master >section > div:nth-child(2) > div > div > div >div
    try:
        all_windows_befors = driver.window_handles
        process_management_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#master > section > div:nth-child(2) > div > div > div")))
        process_management_button.click()
        print("已点击过程管理按钮。")
        wait.until(lambda driver: len(driver.window_handles) > len(all_windows_befors))
        all_windows = driver.window_handles
        print("所有窗口数量:", len(all_windows))
        new_windows = None
        for windows in all_windows:
            if windows not in all_windows_befors:
                new_windows = windows
                break
        if new_windows:
            driver.switch_to.window(new_windows)
            print("已经切换到新标签页")
            time.sleep(5)

        else:
            print("没找到新的标签")
    except Exception as e:
        print("未找到过程管理按钮，可能是页面结构已更改。", e)

    # 获取当前页面的URL
    current_url = driver.current_url
    print("当前页面URL:", current_url)

    # 获取请求头和 token
    headers = get_all_request_headers(driver)
    logger.info(f"获取到 {len(headers)} 个请求头")
    token = get_latest_token(driver)
    logger.info(f"获取到的 token: {token}")


# =============================
# 主入口
# =============================
def main():
    logger.info("=" * 60)
    logger.info("Selenium Linux 项目测试开始")
    logger.info("=" * 60)

    if not check_environment():
        logger.error("环境检查失败")
        return False

    chromedriver_path = "/usr/local/bin/chromedriver"
    service = Service(chromedriver_path)
    options = setup_chrome_options()

    driver = None
    try:
        driver = webdriver.Chrome(service=service, options=options)
        logger.info("WebDriver 初始化成功")

        run_project_flow(driver)

        logger.info("🎉 项目流程执行完成")
        return True
    except Exception as e:
        logger.error(f"运行出错: {e}", exc_info=True)
        return False
    finally:
        if driver:
            driver.quit()
            logger.info("WebDriver 已关闭")


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
