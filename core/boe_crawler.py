import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from requests.utils import dict_from_cookiejar

from database.database_manager import DataBaseManager

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
        self.reports = []
        self.full_reports = []
        self.details = []
        self.query_fields = []
        self.detail_key: Optional[str] = None
        
        # 初始化数据库管理器
        self.db_manager = DataBaseManager()

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
            response = self.session.post(login_url, headers=login_headers, data=login_data, verify=False, timeout=300)
            response.raise_for_status()

            result = response.json()

            if result.get("code") == 0:
                # 登录成功，设置默认的管理员认证信息
                self.authkey = "PiK0iSYIrs559DnX1Wcr3UOaWIKk0hNd5yhIaknVf7DGSVuPJUJpFZ9tRsS0ZUdB"

                # 更新请求头中的Cookie
                cookies = dict_from_cookiejar(self.session.cookies)
                cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
                self.headers["Cookie"] = cookie_str

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

    def update_authkey(self, new_authkey: str):
        """更新认证密钥"""
        self.authkey = new_authkey
        logger.info("认证密钥已更新")

    def update_detail_key(self, detail_key: str):
        """更新单据详情接口所需的 key（如有需要）"""
        self.detail_key = detail_key
        logger.info("单据详情 key 已更新")

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

    def get_full_report_by_boeno(self, boe_no: str, report_code: str = "zfsboestatementvPage.ureport.xml") -> (
            Optional)[Dict]:
        """通过 boeNo 调用全量单据接口并解析页面，返回包含 boeHeaderId 的单条数据。"""
        url = f"{self.base_url}/sys/report/ureport/zfsCustomReport/previewReport"

        params = {
            "boeNo": boe_no,
            "_u": f"database:{report_code}"
        }

        headers = self.headers.copy()
        headers.update({
            "Upgrade-Insecure-Requests": "1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
                      "*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Referer": f"{self.base_url}/report/"
        })

        try:
            logger.info(f"通过 boeNo 获取全量单据: {boe_no}")
            response = self.session.get(url, headers=headers, params=params, verify=False, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # 解析表格（与 _extract_table_data 类似，但只取单条）
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
                # href 形如 /sys/report/ureport/zfsCustomReport/reportBillView?boeHeaderId=xxxx
                try:
                    from urllib.parse import urlparse, parse_qs
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

            logger.info(f"全量单据解析成功: boeNo={boe_no}, boeHeaderId={boe_header_id}, row={row_data}")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"全量单据请求失败 boeNo={boe_no}: {e}")
            return None
        except Exception as e:
            logger.error(f"全量单据解析失败 boeNo={boe_no}: {e}")
            return None

    def get_boe_detail(self, boe_no: str, boe_header_id: str) -> Optional[Dict]:
        """获取单据详情数据（/sys/boe/core/editDraftBoe）。部分环境需要 key。"""
        if not boe_header_id:
            logger.warning(f"获取详情时缺少 boeHeaderId，boeNo={boe_no}")
            return None

        url = f"{self.base_url}/sys/boe/core/editDraftBoe"
        payload = {
            "boeNo": boe_no,
            "boeHeaderId": boe_header_id,
            "key": "0MQ/TJHOFRPXdpVU7NxEQN6a+Tjjv0WP"
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

            # 规范化输出关键信息
            result = {
                "boeNo": boe_no,
                "boeHeaderId": boe_header_id,
                "data": data
            }
            logger.info(f"单据详情获取成功 boeNo={boe_no},row={result}")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"请求单据详情失败 boeNo={boe_no}: {e}")
            return None
        except Exception as e:
            logger.error(f"解析单据详情失败 boeNo={boe_no}: {e}")
            return None

    def process_bills_with_details(self):
        """串联处理：从 self.reports 提取 boeNo -> 全量单据 -> boeHeaderId -> 单据详情，并仅保存两类数据。"""
        if not self.reports:
            logger.warning("没有可处理的报账单记录（列表页数据为空）")
            return

        processed = 0
        for report in self.reports:
            # 提取 boeNo（列名通常为“单据编号”）
            boe_value = report.get("单据编号") or report.get("单据编号(BOE)") or report.get("单据号")
            if not boe_value:
                continue

            boe_no = None
            if isinstance(boe_value, dict):
                boe_no = boe_value.get("text") or boe_value.get("link_text")
            elif isinstance(boe_value, str):
                boe_no = boe_value.strip()

            if not boe_no:
                continue

            # 1) 通过 boeNo 获取全量单据（单条），解析 boeHeaderId
            full_item = self.get_full_report_by_boeno(boe_no)
            if not full_item:
                logger.warning(f"boeNo={boe_no} 全量单据获取失败，跳过详情")
                continue

            self.full_reports.append(full_item)

            boe_header_id = full_item.get("boeHeaderId")
            if not boe_header_id:
                logger.warning(f"boeNo={boe_no} 未取得 boeHeaderId，跳过详情")
                continue

            # 2) 通过 boeHeaderId 获取单据详情
            detail_item = self.get_boe_detail(boe_no, boe_header_id)
            if detail_item:
                self.details.append(detail_item)

            processed += 1
            time.sleep(0.5)
        logger.info(f"串联处理完成，共处理 {processed} 条单据详情")

    def save_full_reports_to_database(self, table_name: str = "row_boe_full_reports") -> bool:
        """
        将全量单据数据写入数据库
        
        Args:
            table_name: 表名，默认为 "boe_full_reports"
            
        Returns:
            bool: 写入成功返回True，失败返回False
        """
        if not self.full_reports:
            logger.warning("没有全量单据数据需要写入数据库")
            return True
        
        try:
            logger.info(f"开始将 {len(self.full_reports)} 条全量单据数据写入数据库表 {table_name}")
            success = self.db_manager.auto_create_and_save_data(
                self.full_reports,
                table_name,
                if_exists='append'
            )
            if success:
                logger.info(f"成功将 {len(self.full_reports)} 条全量单据数据写入数据库表 {table_name}")
            else:
                logger.error(f"写入全量单据数据到数据库表 {table_name} 失败")
            return success
        except Exception as e:
            logger.error(f"保存全量单据数据到数据库时发生错误: {e}")
            return False

    def save_details_to_database(self, table_name: str = "row_boe_details") -> bool:
        """
        将单据详情数据写入数据库，只包含 id 和 report 两个字段
        report 字段存储完整的详情 JSON 数据（文本类型/CLOB）
        
        Args:
            table_name: 表名，默认为 "boe_details"
            
        Returns:
            bool: 写入成功返回True，失败返回False
        """
        if not self.details:
            logger.warning("没有单据详情数据需要写入数据库")
            return True
        
        try:
            # 准备详情数据，只包含 id 和 report 两个字段
            detail_data = []
            for detail_item in self.details:
                boe_header_id = detail_item.get("boeHeaderId")
                if not boe_header_id:
                    logger.warning(f"详情数据缺少 boeHeaderId，跳过: {detail_item.get('boeNo', 'unknown')}")
                    continue
                
                # 将详情数据转换为 JSON 字符串
                detail_json = json.dumps(detail_item.get("data", {}), ensure_ascii=False)
                
                detail_data.append({
                    "id": str(boe_header_id),  # 确保 id 是字符串类型
                    "report": detail_json  # report 字段将自动识别为 CLOB 类型
                })
            
            if not detail_data:
                logger.warning("没有有效的单据详情数据需要写入数据库")
                return True
            
            logger.info(f"开始将 {len(detail_data)} 条单据详情数据写入数据库表 {table_name}")
            
            # 使用数据库管理器写入数据
            # 注意：需要确保 report 字段被识别为 CLOB 类型
            # 在 database_manager.py 中已经有对 REPORTS 字段的特殊处理，但这里是 report（小写）
            # 我们可以通过字段名映射来确保使用 CLOB
            success = self.db_manager.auto_create_and_save_data(
                detail_data,
                table_name,
                if_exists='append'
            )
            
            if success:
                logger.info(f"成功将 {len(detail_data)} 条单据详情数据写入数据库表 {table_name}")
            else:
                logger.error(f"写入单据详情数据到数据库表 {table_name} 失败")
            return success
        except Exception as e:
            logger.error(f"保存单据详情数据到数据库时发生错误: {e}")
            return False

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

            # 串联处理全量单据与详情，仅保存所需数据
            self.process_bills_with_details()

            # 将全量单据数据写入数据库
            logger.info("开始将全量单据数据写入数据库...")
            self.save_full_reports_to_database()

            # 将单据详情数据写入数据库（只包含 id 和 report 两个字段）
            logger.info("开始将单据详情数据写入数据库...")
            self.save_details_to_database()

            end_time = time.time()
            duration = end_time - start_time

            logger.info(f"爬取任务完成！")
            logger.info(f"共获取 {len(self.reports)} 条报账单记录")
            logger.info(f"共获取 {len(self.full_reports)} 条全量单据记录")
            logger.info(f"共获取 {len(self.details)} 条单据详情记录")
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
    end_date = "2025-07-19 23:59:59"

    logger.info(f"开始爬取指定时间段的数据: {start_date} 到 {end_date}")

    # 运行爬取任务 - 不限制页数，爬取所有页
    crawler.run(start_date=start_date, end_date=end_date, page_size=100, max_pages=None)


if __name__ == "__main__":
    main()
