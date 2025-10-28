import csv
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from requests.utils import dict_from_cookiejar

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('boe_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BoeCrawler:
    """报账单数据爬取器"""

    def __init__(self, base_url: str = "http://10.3.102.173", login_key: str = "10030",
                 password: str = "PhhLGbYaZgsvNKJ2YjHF3A=="):
        self.base_url = base_url
        self.login_key = login_key
        self.password = password
        self.session = requests.Session()
        self.reports = []  # 存储所有报账单数据
        self.query_fields = []  # 存储查询字段信息

        # 设置请求头
        self.headers = {
            "Connection": "keep-alive",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/97.0.4692.71 Safari/537.36",
            "Referer": f"{base_url}/",
            "Cookie": ""
        }

        # 认证信息，将通过登录获取
        self.authkey = None

    def login(self) -> bool:
        """
        登录系统获取认证信息
        
        Returns:
            bool: 登录成功返回True，失败返回False
        """
        login_url = f"{self.base_url}/sys/auth/login"

        # 准备登录数据
        login_data = {
            "loginKey": self.login_key,
            "password": self.password
        }

        # 设置登录请求头
        login_headers = {
            "Connection": "keep-alive",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/97.0.4692.71 Safari/537.36",
            "Referer": f"{self.base_url}/",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        try:
            logger.info(f"正在尝试登录，用户名: {self.login_key}")
            response = self.session.post(login_url, headers=login_headers, data=login_data, verify=False, timeout=30)
            response.raise_for_status()

            result = response.json()

            if result.get("code") == 0:
                # 登录成功，获取认证信息
                data = result.get("data", {})
                self.authkey = data.get("authkey")

                if self.authkey:
                    logger.info("登录成功，已获取认证密钥")

                    # 更新请求头中的Cookie
                    cookies = dict_from_cookiejar(self.session.cookies)
                    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
                    self.headers["Cookie"] = cookie_str

                    return True
                else:
                    logger.error("登录成功但未获取到认证密钥")
                    return False
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

    def update_authkey(self, new_authkey: str):
        """更新认证密钥"""
        self.authkey = new_authkey
        logger.info("认证密钥已更新")

    def is_logged_in(self) -> bool:
        """检查是否已登录"""
        return self.authkey is not None

    def get_query_fields(self, report_code: str = "zfsboestatementvPage.ureport.xml") -> Optional[Dict]:
        """
        获取报表查询字段信息
        
        Args:
            report_code: 报表代码
            
        Returns:
            包含查询字段信息的字典，失败返回None
        """
        url = f"{self.base_url}/sys/report/reportmanager/findReportWithQueryFieldByCode"
        params = {"reportCode": report_code}

        headers = self.headers.copy()
        headers.update({
            "pageUri": f"#/reportDetail?reportCode={report_code}",
            "authkey": self.authkey,
            "x-frame-options": "allow-from http://10.3.92.33/finebi/decision/link/CCga",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/report/"
        })

        try:
            logger.info(f"正在获取报表查询字段信息: {report_code}")
            response = self.session.post(url, headers=headers, params=params, verify=False, timeout=30)
            response.raise_for_status()

            data = response.json()
            if data.get("code") == 0:
                self.query_fields = data.get("queryFields", [])
                logger.info(f"成功获取 {len(self.query_fields)} 个查询字段")
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

    def get_report_data(self,
                        start_date: str = None,
                        end_date: str = None,
                        page_size: int = 100,
                        page: int = 1,
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

        headers = self.headers.copy()
        headers.update({
            "Upgrade-Insecure-Requests": "1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
                      "*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Referer": f"{self.base_url}/report/"
        })

        try:
            logger.info(f"正在获取第 {page} 页报表数据，日期范围: {start_date} 到 {end_date}")
            response = self.session.get(url, headers=headers, params=params, verify=False, timeout=30)
            response.raise_for_status()

            # 解析HTML响应
            soup = BeautifulSoup(response.text, 'html.parser')

            # 提取分页信息
            page_info = self._extract_page_info(soup)

            # 提取表格数据
            table_data = self._extract_table_data(soup)

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

    def _extract_page_info(self, soup: BeautifulSoup) -> Dict:
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

    def _extract_table_data(self, soup: BeautifulSoup) -> List[Dict]:
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

    def crawl_all_data(self,
                       start_date: str = None,
                       end_date: str = None,
                       page_size: int = 100,
                       max_pages: int = None) -> bool:
        """
        爬取所有报表数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            page_size: 每页数量
            max_pages: 最大页数限制，None表示无限制
            
        Returns:
            bool: 爬取成功返回True，失败返回False
        """
        logger.info("开始爬取报账单数据...")

        try:
            # 首先获取查询字段信息
            if not self.get_query_fields():
                logger.error("获取查询字段失败，无法继续")
                return False

            page = 1

            while True:
                # 检查页数限制
                if max_pages and page > max_pages:
                    logger.info(f"已达到最大页数限制 {max_pages}，停止爬取")
                    break

                # 获取当前页数据
                result = self.get_report_data(
                    start_date=start_date,
                    end_date=end_date,
                    page_size=page_size,
                    page=page
                )

                if not result:
                    logger.error(f"获取第 {page} 页数据失败")
                    break

                # 添加数据到总列表
                self.reports.extend(result["data"])

                # 更新分页信息
                page_info = result["page_info"]
                total_pages = page_info["total_pages"]

                logger.info(f"已获取第 {page}/{total_pages} 页数据，当前页 {len(result['data'])} 条记录")

                # 检查是否还有下一页
                if page >= total_pages:
                    break

                page += 1

                # 添加延迟避免请求过快
                time.sleep(1)

            logger.info(f"爬取完成！共获取 {len(self.reports)} 条报账单记录")
            return True

        except Exception as e:
            logger.error(f"爬取过程中发生错误: {e}")
            return False

    def export_to_json(self, filename: str = "boe_data.json"):
        """导出数据到JSON文件"""
        data = {
            "reports": self.reports,
            "query_fields": self.query_fields,
            "summary": {
                "total_reports": len(self.reports),
                "export_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"数据已导出到 {filename}")

    def export_to_csv(self, filename: str = "boe_reports.csv"):
        """导出数据到CSV文件"""
        if not self.reports:
            logger.warning("没有数据可导出")
            return

        # 获取所有字段名
        all_fields = set()
        for report in self.reports:
            all_fields.update(report.keys())

        fieldnames = sorted(list(all_fields))

        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for report in self.reports:
                # 处理嵌套字典（如链接数据）
                row_data = {}
                for field, value in report.items():
                    if isinstance(value, dict):
                        # 如果是字典，提取文本内容
                        row_data[field] = value.get('text', str(value))
                    else:
                        row_data[field] = value

                writer.writerow(row_data)

        logger.info(f"报账单数据已导出到 {filename}")

    def run(self, start_date: str = None, end_date: str = None, page_size: int = 100, max_pages: int = None):
        """运行爬取任务"""
        logger.info("开始报账单数据爬取任务...")

        start_time = time.time()

        try:
            # 首先进行登录
            logger.info("正在登录系统...")
            if not self.login():
                logger.error("登录失败，无法继续爬取")
                return

            # 爬取数据
            if not self.crawl_all_data(start_date, end_date, page_size, max_pages):
                logger.error("数据爬取失败")
                return

            # 导出数据
            self.export_to_json()
            self.export_to_csv()

            end_time = time.time()
            duration = end_time - start_time

            logger.info(f"爬取任务完成！")
            logger.info(f"共获取 {len(self.reports)} 条报账单记录")
            logger.info(f"耗时: {duration:.2f} 秒")

        except KeyboardInterrupt:
            logger.info("用户中断了爬取过程")
        except Exception as e:
            logger.error(f"爬取过程中发生错误: {e}")
            raise


def main():
    """主函数"""
    # 创建爬取器实例
    crawler = BoeCrawler()

    # 测试登录
    logger.info("测试登录功能...")
    if crawler.login():
        logger.info("登录测试成功！")
        logger.info(f"认证密钥: {crawler.authkey[:20]}..." if crawler.authkey else "未获取到认证密钥")
    else:
        logger.error("登录测试失败！")
        return

    # 设置查询参数 - 爬取2025年07-19到2025年10-16的所有数据
    start_date = "2025-07-19 00:00:00"
    end_date = "2025-10-16 23:59:59"

    logger.info(f"开始爬取指定时间段的数据: {start_date} 到 {end_date}")

    # 运行爬取任务 - 不限制页数，爬取所有页
    crawler.run(start_date=start_date, end_date=end_date, page_size=100, max_pages=None)


if __name__ == "__main__":
    main()
