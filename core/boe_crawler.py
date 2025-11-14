import json
import logging
import time
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from api.api_client import UnifiedLoginClient, BoeAPIClient
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
        self.reports = []
        self.full_reports = []
        self.details = []
        self.query_fields = []
        self.detail_key: Optional[str] = None

        # 初始化数据库管理器
        self.db_manager = DataBaseManager()

        # 使用统一的登录客户端和API客户端
        self.login_client = UnifiedLoginClient(base_url, login_key, password)
        self.api_client = BoeAPIClient(self.login_client)

        # 保持向后兼容
        self.session = self.login_client.get_session()
        self.authkey = None  # 将在登录后设置

    def login(self) -> bool:
        """
        登录系统获取认证信息

        Returns:
            bool: 登录成功返回True，失败返回False
        """
        success = self.login_client.login()
        if success:
            self.authkey = self.login_client.get_authkey()
        return success

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
        result = self.api_client.get_query_fields(report_code)
        if result:
            self.query_fields = result.get("queryFields", [])
        return result

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
        return self.api_client.get_report_data(start_date, end_date, page_size, page, report_code)

    def get_full_report_by_boeno(self, boe_no: str, report_code: str = "zfsboestatementvPage.ureport.xml") -> (
            Optional)[Dict]:
        """通过 boeNo 调用全量单据接口并解析页面，返回包含 boeHeaderId 的单条数据。"""
        return self.api_client.get_full_report_by_boeno(boe_no, report_code)

    def get_boe_detail(self, boe_no: str, boe_header_id: str, key: str = "0MQ/TJHOFRPXdpVU7NxEQN6a+Tjjv0WP") -> \
            Optional[Dict]:
        """获取单据详情数据（/sys/boe/core/editDraftBoe）。部分环境需要 key。"""
        return self.api_client.get_boe_detail(boe_no, boe_header_id, key)

    def process_bills_with_details(self):
        """从 self.reports 提取 boeNo -> 全量单据 -> boeHeaderId -> 单据详情，获取一条数据就写入一条。"""
        if not self.reports:
            logger.warning("没有可处理的报账单记录（列表页数据为空）")
            return

        processed = 0
        saved_full_reports = 0
        saved_details = 0
        for report in self.reports:
            # 提取 boeNo（列名通常为"单据编号"）
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

            if self.save_single_full_report_to_database(full_item):
                saved_full_reports += 1
                self.full_reports.append(full_item)
            else:
                logger.warning(f"boeNo={boe_no} 全量单据写入数据库失败")

            boe_header_id = full_item.get("boeHeaderId")
            # 检查 boeHeaderId 是否为有效值（不是 None、空字符串或字符串 "null"）
            if not boe_header_id or boe_header_id == "null" or str(boe_header_id).strip() == "":
                logger.warning(f"boeNo={boe_no} 未取得有效的 boeHeaderId（值为: {boe_header_id}），跳过详情")
                continue

            detail_item = self.get_boe_detail(boe_no, boe_header_id)
            if detail_item:
                if self.save_single_detail_to_database(detail_item):
                    saved_details += 1
                    self.details.append(detail_item)
                else:
                    logger.warning(f"boeNo={boe_no} 单据详情写入数据库失败")

            processed += 1
            time.sleep(0.5)
        logger.info(
            f"串联处理完成，共处理 {processed} 条单据，成功写入 {saved_full_reports} 条全量单据，{saved_details} 条单据详情")

    def _is_empty_full_report(self, full_item: Dict) -> bool:
        """
        检查全量单据数据是否为空
        
        Args:
            full_item: 全量单据数据
            
        Returns:
            bool: 如果数据为空返回True，否则返回False
        """
        row = full_item.get("row", {})
        if not row:
            return True

        # 检查关键字段是否都为空
        key_fields = ["单据编号", "单据类型", "单据日期", "报账人", "报账部门", "单据状态"]
        has_data = False

        for field in key_fields:
            value = row.get(field, "")
            if isinstance(value, dict):
                # 如果是字典类型，检查text字段
                text = value.get("text", "").strip()
                if text:
                    has_data = True
                    break
            elif isinstance(value, str) and value.strip():
                has_data = True
                break

        # 如果所有关键字段都为空，则认为数据为空
        return not has_data

    def save_single_full_report_to_database(self, full_item: Dict, table_name: str = "row_boe_full_reports") -> bool:
        """
        将单条全量单据数据立即写入数据库
        
        Args:
            full_item: 单条全量单据数据
            table_name: 表名，默认为 "row_boe_full_reports"
            
        Returns:
            bool: 写入成功返回True，失败返回False
        """
        try:
            # 过滤空数据
            if self._is_empty_full_report(full_item):
                boe_no = full_item.get('boeNo', 'unknown')
                logger.info(f"跳过空数据：boeNo={boe_no} 全量单据数据为空，不写入数据库")
                return False

            success = self.db_manager.auto_create_and_save_data(
                [full_item],
                table_name,
                if_exists='append'
            )
            if success:
                logger.debug(
                    f"成功写入单条全量单据数据 boeNo={full_item.get('boeNo', 'unknown')} 到数据库表 {table_name}")
            else:
                logger.error(
                    f"写入单条全量单据数据 boeNo={full_item.get('boeNo', 'unknown')} 到数据库表 {table_name} 失败")
            return success
        except Exception as e:
            logger.error(f"保存单条全量单据数据到数据库时发生错误: {e}")
            return False

    def _is_empty_detail(self, detail_item: Dict) -> bool:
        """
        检查单据详情数据是否为空或无效
        
        Args:
            detail_item: 单据详情数据
            
        Returns:
            bool: 如果数据为空或无效返回True，否则返回False
        """
        boe_header_id = detail_item.get("boeHeaderId")
        # 检查 boeHeaderId 是否为有效值（不是 None、空字符串或字符串 "null"）
        if not boe_header_id or boe_header_id == "null" or str(boe_header_id).strip() == "":
            return True

        # 检查详情数据是否包含错误信息
        data = detail_item.get("data", {})
        if isinstance(data, dict):
            # 检查是否返回错误信息
            code = data.get("code")
            msg = data.get("msg", "")
            # 如果 code 存在且不为 0（0 表示成功），或者包含错误信息，则认为数据无效
            if code is not None and code != 0:
                return True
            # 如果 msg 包含错误关键词，也认为数据无效
            if msg and ("未找到" in msg or "错误" in msg or "失败" in msg):
                return True

        return False

    def save_single_detail_to_database(self, detail_item: Dict, table_name: str = "row_boe_details") -> bool:
        """
        将单条单据详情数据立即写入数据库，只包含 id 和 report 两个字段
        
        Args:
            detail_item: 单条单据详情数据
            table_name: 表名，默认为 "row_boe_details"
            
        Returns:
            bool: 写入成功返回True，失败返回False
        """
        try:
            # 过滤空数据或无效数据
            if self._is_empty_detail(detail_item):
                boe_no = detail_item.get('boeNo', 'unknown')
                logger.info(f"跳过空数据：boeNo={boe_no} 单据详情数据为空或无效，不写入数据库")
                return False

            boe_header_id = detail_item.get("boeHeaderId")
            if not boe_header_id:
                logger.warning(f"详情数据缺少 boeHeaderId，跳过: {detail_item.get('boeNo', 'unknown')}")
                return False

            detail_json = json.dumps(detail_item.get("data", {}), ensure_ascii=False)

            detail_data = {
                "id": str(boe_header_id),
                "report": detail_json
            }

            success = self.db_manager.auto_create_and_save_data(
                [detail_data],  # 将单条数据包装成列表
                table_name,
                if_exists='append'
            )

            if success:
                logger.debug(
                    f"成功写入单条单据详情数据 boeNo={detail_item.get('boeNo', 'unknown')} 到数据库表 {table_name}")
            else:
                logger.error(
                    f"写入单条单据详情数据 boeNo={detail_item.get('boeNo', 'unknown')} 到数据库表 {table_name} 失败")
            return success
        except Exception as e:
            logger.error(f"保存单条单据详情数据到数据库时发生错误: {e}")
            return False

    def _extract_page_info(self, soup: BeautifulSoup) -> Dict:
        """提取分页信息"""
        return self.api_client.extract_page_info(soup)

    def _extract_table_data(self, soup: BeautifulSoup) -> List[Dict]:
        """提取表格数据"""
        return self.api_client.extract_table_data(soup)

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
                time.sleep(0.5)

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

            # 串联处理全量单据与详情，获取一条数据就写入一条
            logger.info("开始处理全量单据与详情数据（逐条写入）...")
            self.process_bills_with_details()

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
