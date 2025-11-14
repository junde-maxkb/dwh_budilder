import json
import logging
import time
from typing import Dict, Optional

import requests
from requests.utils import dict_from_cookiejar
from database.database_manager import DataBaseManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('org_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OrgCrawler:
    """组织架构爬取器"""

    def __init__(self, base_url: str = "http://10.3.102.173", login_key: str = "10030",
                 password: str = "PhhLGbYaZgsvNKJ2YjHF3A=="):
        self.base_url = base_url
        self.login_key = login_key
        self.password = password
        self.session = requests.Session()
        self.departments = []  # 存储所有部门信息
        self.employees = []  # 存储所有人员信息
        self.visited_depts = set()  # 避免重复访问
        self.db_manager = DataBaseManager()  # 数据库管理器

        # 设置请求头
        self.headers = {
            "Connection": "keep-alive",
            "Accept": "application/json, text/plain, */*",
            "pageUri": "#/orgManage",
            "Accept-Language": "zh-CN",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/97.0.4692.71 Safari/537.36",
            "Referer": f"{base_url}/",
            "Cookie": ""
        }

        # 认证信息，将通过登录获取
        self.authkey = None

    def _truncate_string(self, text: str, max_chars: int = 400) -> str:
        
        if not text or not isinstance(text, str):
            return text if text is not None else ""
        
        # 如果字符串长度不超过限制，直接返回
        if len(text) <= max_chars:
            return text
        
        # 截断字符串，保留前 max_chars 个字符
        truncated = text[:max_chars]
        
        # 如果原字符串被截断，记录警告日志（仅记录一次，避免日志过多）
        if len(text) > max_chars:
            logger.debug(f"字符串被截断: 原长度={len(text)}, 截断后长度={len(truncated)}, 内容预览={truncated[:50]}...")
        
        return truncated

    def _truncate_employee_fields(self, employee: Dict) -> Dict:
        """
        截断员工数据中可能过长的字符串字段
        
        Args:
            employee: 员工数据字典
        
        Returns:
            处理后的员工数据字典
        """
        # 定义各字段的最大长度限制（字符数）
        field_limits = {
            "dept_name": 400,  # 部门名称可能包含完整路径
            "name": 100,  # 姓名
            "code": 100,  # 编码
            "leader_name": 100,  # 领导姓名
            "primary_post": 100,  # 主岗位ID
            "primary_post_name": 200,  # 主岗位名称
            "travel_level": 100,  # 差旅级别ID
            "travel_level_name": 200,  # 差旅级别名称
            "remark": 500,  # 备注
            "id": 100,  # ID
            "user_id": 100,  # 用户ID
            "dept_id": 100,  # 部门ID
            "parent_id": 100,  # 父部门ID
            "validity_flag": 50,  # 有效性标志
            "enabled_flag": 50,  # 启用标志
            "create_date": 50,  # 创建日期
            "last_update_date": 50,  # 最后更新日期
        }
        
        # 对每个字段进行截断处理
        for field, max_length in field_limits.items():
            if field in employee and employee[field] is not None:
                employee[field] = self._truncate_string(str(employee[field]), max_length)
        
        return employee

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
            print(result)
            if result.get("code") == 0:
                # 登录成功，获取认证信息
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

    def is_logged_in(self) -> bool:
        """检查是否已登录"""
        return self.authkey is not None

    def get_children_by_id(self, dept_id: str) -> Optional[Dict]:
        """
        获取指定部门ID下的子部门
        
        Args:
            dept_id: 部门ID，0表示根部门
            
        Returns:
            包含子部门信息的字典，失败返回None
        """
        url = f"{self.base_url}/sys/common/dept/findChildrenById/{dept_id}"
        params = {"time": int(time.time())}

        headers = self.headers.copy()
        headers["authkey"] = self.authkey

        try:
            response = self.session.get(url, headers=headers, params=params, verify=False, timeout=30)
            response.raise_for_status()

            data = response.json()
            if data.get("code") == 0:
                return data
            else:
                logger.error(f"获取部门 {dept_id} 失败: {data.get('msg', '未知错误')}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"请求部门 {dept_id} 时发生错误: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"解析部门 {dept_id} 响应JSON失败: {e}")
            return None

    def find_shanghai_railway_id(self, root_data: Dict) -> Optional[str]:
        """
        从根部门数据中查找上海铁路的部门ID
        
        Args:
            root_data: 根部门返回的数据
            
        Returns:
            上海铁路的部门ID，未找到返回None
        """
        tree = root_data.get("tree", [])
        for dept in tree:
            dept_info = dept.get("obj", {})
            dept_name = dept_info.get("name", "")
            dept_id = dept_info.get("id")

            # 查找包含"上海铁路"的部门
            if "上海铁路" in dept_name:
                logger.info(f"找到上海铁路部门: {dept_name} (ID: {dept_id})")
                return dept_id

        logger.warning("未找到上海铁路部门")
        return None

    def get_dept_page(self, parent_id: str, page: int = 1, limit: int = 100) -> Optional[Dict]:
        """
        分页获取部门信息（包含人员）
        
        Args:
            parent_id: 父部门ID
            page: 页码
            limit: 每页数量
            
        Returns:
            包含分页数据的字典，失败返回None
        """
        url = f"{self.base_url}/sys/common/dept/page"

        headers = self.headers.copy()
        headers.update({
            "Content-Type": "application/x-www-form-urlencoded",
            "authkey": self.authkey
        })

        data = {
            "page": page,
            "limit": limit,
            "orderField": "",
            "order": "",
            "pid": parent_id
        }

        try:
            response = self.session.post(url, headers=headers, data=data, verify=False, timeout=30)
            response.raise_for_status()

            result = response.json()
            if result.get("code") == 0:
                return result
            else:
                logger.error(f"获取部门 {parent_id} 分页数据失败: {result.get('msg', '未知错误')}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"请求部门 {parent_id} 分页数据时发生错误: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"解析部门 {parent_id} 分页响应JSON失败: {e}")
            return None

    def crawl_departments_recursive(self, dept_id: str = "0", level: int = 0):
        """
        递归爬取所有部门信息
        
        Args:
            dept_id: 部门ID，默认为"0"（根部门）
            level: 当前层级，用于缩进显示
        """
        if dept_id in self.visited_depts:
            return

        self.visited_depts.add(dept_id)
        indent = "  " * level

        logger.info(f"{indent}正在获取部门 {dept_id} 的信息...")

        # 获取子部门
        children_data = self.get_children_by_id(dept_id)
        if not children_data:
            return

        tree = children_data.get("tree", [])
        for dept in tree:
            dept_info = dept.get("obj", {})
            dept_id_current = dept_info.get("id")
            dept_name = dept_info.get("name", "未知部门")
            dept_type = dept_info.get("typeName", "未知类型")

            # 存储部门信息
            department = {
                "id": dept_id_current,
                "parent_id": dept_id,
                "name": dept_name,
                "code": dept_info.get("code", ""),
                "type": dept_info.get("type", ""),
                "type_name": dept_type,
                "level": level,
                "dept_fname": dept_info.get("deptFname", ""),
                "dept_fid": dept_info.get("deptFid", ""),
                "validity_flag": dept_info.get("validityFlag", ""),
                "enabled_flag": dept_info.get("enabledFlag", ""),
                "create_date": dept_info.get("createDate", ""),
                "last_update_date": dept_info.get("lastUpdateDate", ""),
                "leader_name": dept_info.get("leaderName", ""),
                "leader_id": dept_info.get("leaderId", ""),
                "remark": dept_info.get("remark", "")
            }

            self.departments.append(department)
            logger.info(f"{indent}  - {dept_name} ({dept_type})")

            # 如果不是叶子节点，继续递归
            if not dept.get("leaf", True):
                self.crawl_departments_recursive(dept_id_current, level + 1)

            # 获取该部门下的人员信息
            self.crawl_employees_in_dept(dept_id_current, level + 1)

            # 添加延迟避免请求过快
            time.sleep(0.5)

    def crawl_employees_in_dept(self, dept_id: str, level: int = 0):
        """
        获取指定部门下的人员信息
        
        Args:
            dept_id: 部门ID
            level: 当前层级
        """
        indent = "  " * level
        page = 1
        limit = 100

        while True:
            logger.info(f"{indent}正在获取部门 {dept_id} 第 {page} 页人员信息...")

            page_data = self.get_dept_page(dept_id, page, limit)
            if not page_data:
                break

            page_info = page_data.get("page", {})
            dept_list = page_info.get("list", [])

            if not dept_list:
                break

            for item in dept_list:
                # 只处理人员类型的数据
                if item.get("type") == "3" and item.get("typeName") == "人员":
                    employee = {
                        "id": item.get("id", ""),
                        "user_id": item.get("userId", ""),
                        "name": item.get("name", ""),
                        "code": item.get("code", ""),
                        "dept_id": item.get("deptId", ""),
                        "dept_name": item.get("deptFname", "").split(",")[0] if item.get("deptFname") else "",
                        "parent_id": item.get("pid", ""),
                        "primary_post": item.get("primaryPost", ""),
                        "primary_post_name": item.get("primaryPostName", ""),
                        "travel_level": item.get("travelLevel", ""),
                        "travel_level_name": item.get("travelLevelName", ""),
                        "validity_flag": item.get("validityFlag", ""),
                        "enabled_flag": item.get("enabledFlag", ""),
                        "create_date": item.get("createDate", ""),
                        "last_update_date": item.get("lastUpdateDate", ""),
                        "leader_id": item.get("leaderId", ""),
                        "leader_name": item.get("leaderName", ""),
                        "remark": item.get("remark", "")
                    }

                    # 截断可能过长的字段，避免数据库字段长度限制错误
                    employee = self._truncate_employee_fields(employee)
                    
                    self.employees.append(employee)
                    logger.info(f"{indent}  - 人员: {employee['name']} ({employee['code']})")
                    
                    # 立即保存到数据库
                    try:
                        success = self.db_manager.auto_create_and_save_data(
                            [employee],  # 将单条数据包装成列表
                            "raw_organize",
                            if_exists='append'
                        )
                        if success:
                            logger.debug(f"{indent}    ✓ 已保存到数据库")
                        else:
                            logger.warning(f"{indent}    ✗ 保存到数据库失败")
                    except Exception as e:
                        logger.error(f"{indent}    保存人员数据时发生错误: {e}")

            # 检查是否还有下一页
            total_pages = page_info.get("totalPage", 1)
            if page >= total_pages:
                break

            page += 1
            time.sleep(0.5)  # 添加延迟

    def run(self, start_dept_id: str = "0"):
        """运行爬取任务"""
        logger.info("开始爬取组织架构和人员信息...")
        logger.info(f"起始部门ID: {start_dept_id}")

        start_time = time.time()

        try:
            # 首先进行登录
            logger.info("正在登录系统...")
            if not self.login():
                logger.error("登录失败，无法继续爬取")
                return

            # 获取根部门信息，找到上海铁路的ID
            logger.info("正在获取根部门信息...")
            root_data = self.get_children_by_id(start_dept_id)
            if not root_data:
                logger.error("无法获取根部门信息")
                return

            # 查找上海铁路的部门ID
            shanghai_railway_id = self.find_shanghai_railway_id(root_data)
            if shanghai_railway_id:
                logger.info(f"找到上海铁路部门ID: {shanghai_railway_id}")
                # 从上海铁路部门开始递归爬取
                self.crawl_departments_recursive(shanghai_railway_id)
            else:
                logger.warning("未找到上海铁路部门，从根部门开始爬取")
                self.crawl_departments_recursive(start_dept_id)

            # 注意：数据已在获取时逐条保存，此处仅用于统计
            if self.employees:
                logger.info(f"爬取完成，共获取 {len(self.employees)} 条员工数据（已逐条保存到数据库）")
            else:
                logger.warning("没有员工数据需要写入数据库")

            end_time = time.time()
            duration = end_time - start_time

            logger.info(f"爬取完成！")
            logger.info(f"共获取 {len(self.departments)} 个部门")
            logger.info(f"共获取 {len(self.employees)} 个人员")
            logger.info(f"耗时: {duration:.2f} 秒")

        except KeyboardInterrupt:
            logger.info("用户中断了爬取过程")
        except Exception as e:
            logger.error(f"爬取过程中发生错误: {e}")
            raise


def main():
    """主函数"""
    # 创建爬取器实例
    crawler = OrgCrawler()

    # 运行爬取任务
    crawler.run()


if __name__ == "__main__":
    main()
