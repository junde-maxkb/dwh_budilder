import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from api.api_client import UnifiedLoginClient, FlowAPIClient
from database.database_manager import DataBaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FlowCrawler:
    """资金流水管理爬虫管理器

    能力:
    - 登录获取 authkey / Cookie
    - 分页调用 /sys/claim/claimcapitalflow/page 获取列表
    - 增量去重写入 OceanBase 表 raw_capital_flows
    """

    def __init__(
            self,
            base_url: str = "http://10.3.102.173",
            login_key: str = "10030",
            password: str = "PhhLGbYaZgsvNKJ2YjHF3A==",
            page_size: int = 100,
            request_timeout: int = 30,
    ):
        self.base_url = base_url.rstrip('/')
        self.login_key = login_key
        self.password = password
        self.page_size = page_size
        self.request_timeout = request_timeout

        self.target_table = "raw_capital_flows"
        
        # 初始化数据库管理器
        self.db_manager = DataBaseManager()
        
        # 使用统一的登录客户端和API客户端
        self.login_client = UnifiedLoginClient(base_url, login_key, password)
        self.api_client = FlowAPIClient(self.login_client, page_size, request_timeout)
        
        # 保持向后兼容
        self.session = self.login_client.get_session()
        self.authkey: Optional[str] = None  # 将在登录后设置

    # --------------------- 认证 ---------------------
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

    # --------------------- 拉取分页 ---------------------
    def _fetch_page(self, page_index: int) -> Tuple[int, int, List[Dict]]:
        """拉取单页数据

        返回: (currPage, totalPage, list)
        """
        return self.api_client.fetch_page(page_index)

    def _load_existing_keys(self) -> Tuple[set, str]:
        """优先使用 lsmxId 作为业务唯一键，其次 id。返回(已存在集合, 使用的列名)。"""
        preferred_keys = ["lsmxId", "id"]
        try:
            # 尝试从数据库中加载已存在的键值
            # 注意：数据库中保存的ID字段是 "id"，但原始数据中可能使用 lsmxId 或 id
            existing_keys = self.db_manager.get_existing_values(self.target_table, "id")
            if existing_keys:
                logger.info(f"从数据库表 {self.target_table} 加载到 {len(existing_keys)} 条已存在的ID")
                # 默认使用第一个优先键
                return existing_keys, preferred_keys[0]
        except Exception as e:
            logger.warning(f"加载已存在的去重键失败: {e}")
        # 如果表不存在或没有任何值，默认用 lsmxId
        logger.info(f"数据库表 {self.target_table} 不存在或为空，使用默认键 lsmxId")
        return set[Any](), preferred_keys[0]

    def _dedupe(self, rows: List[Dict], key_field: str, existing: set) -> List[Dict]:
        fresh: List[Dict] = []
        for r in rows:
            key_val = str(r.get(key_field) or "").strip()
            if not key_val:
                # 无法识别的记录，保留以免丢数据
                fresh.append(r)
                continue
            if key_val not in existing:
                existing.add(key_val)
                fresh.append(r)
        return fresh

    def _save_to_database(self, rows: List[Dict], key_field: str) -> int:
        """
        将资金流水数据保存到数据库，只保存 ID 和原始数据两个字段
        
        Args:
            rows: 要保存的数据列表
            key_field: 用作 ID 的字段名（lsmxId 或 id）
            
        Returns:
            int: 成功保存的记录数
        """
        if not rows:
            logger.warning("没有数据需要保存到数据库")
            return 0
        
        try:
            # 准备数据，只包含 id 和原始数据两个字段
            save_data = []
            for row in rows:
                row_id = row.get(key_field)
                if not row_id:
                    # 如果没有ID，尝试使用其他可能的ID字段
                    row_id = row.get("id") or row.get("lsmxId")
                    if not row_id:
                        logger.warning(f"数据缺少ID字段，跳过: {row}")
                        continue
                
                # 将整条记录转换为 JSON 字符串
                raw_data = json.dumps(row, ensure_ascii=False)
                
                save_data.append({
                    "id": str(row_id),  # 确保 id 是字符串类型
                    "raw_data": raw_data  # 原始数据作为 JSON 字符串存储（将自动识别为 CLOB 类型）
                })
            
            if not save_data:
                logger.warning("没有有效的数据需要保存到数据库")
                return 0
            
            logger.info(f"开始将 {len(save_data)} 条资金流水数据写入数据库表 {self.target_table}")
            
            # 使用数据库管理器写入数据
            success = self.db_manager.auto_create_and_save_data(
                save_data,
                self.target_table,
                if_exists='append'
            )
            
            if success:
                logger.info(f"成功将 {len(save_data)} 条资金流水数据写入数据库表 {self.target_table}")
                return len(save_data)
            else:
                logger.error(f"写入资金流水数据到数据库表 {self.target_table} 失败")
                return 0
        except Exception as e:
            logger.error(f"保存资金流水数据到数据库时发生错误: {e}")
            return 0

    # --------------------- 对外主流程 ---------------------
    def run(self, max_pages: Optional[int] = None, sleep_seconds: float = 0.5) -> Dict[str, int]:
        """执行一次全量抓取（支持限制最大页数）。返回统计信息。"""
        stats = {"pages": 0, "fetched": 0, "saved": 0}

        if not self.login():
            return stats

        try:
            existing_keys, key_field = self._load_existing_keys()
            logger.info(f"资金流水 - 使用去重键: {key_field}，当前已存在: {len(existing_keys)} 条")

            page_index = 1
            total_pages = None

            while True:
                if max_pages is not None and page_index > max_pages:
                    logger.info(f"达到最大页数限制 {max_pages}，结束抓取")
                    break

                curr, total, rows = self._fetch_page(page_index)
                total_pages = total
                stats["pages"] += 1
                stats["fetched"] += len(rows)
                logger.info(f"资金流水 - 第 {curr}/{total} 页，获取 {len(rows)} 条")

                # 增量去重
                to_save = self._dedupe(rows, key_field, existing_keys)
                if to_save:
                    # 保存到数据库
                    saved_count = self._save_to_database(to_save, key_field)
                    stats["saved"] += saved_count
                    logger.info(f"资金流水 - 本页新增 {len(to_save)} 条，成功保存 {saved_count} 条")
                else:
                    logger.info("资金流水 - 本页无新增记录")

                if curr >= total:
                    break
                page_index += 1
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

            if total_pages is None:
                logger.warning("资金流水 - 未能解析到总页数")

            logger.info(f"资金流水 - 抓取完成：页数 {stats['pages']}，获取 {stats['fetched']}，入库 {stats['saved']}")
            return stats

        except Exception as e:
            logger.error(f"资金流水 - 抓取异常: {e}")
            return stats


def main():
    crawler = FlowCrawler()
    crawler.run(max_pages=None, sleep_seconds=0.5)


if __name__ == "__main__":
    main()
