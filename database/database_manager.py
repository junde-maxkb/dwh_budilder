import logging
import os
import pandas as pd
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
import jaydebeapi

from common.config import config_manager

load_dotenv()

logger = logging.getLogger(__name__)


class DataBaseManager:
    def __init__(self, connection_params: Optional[Dict[str, str]] = None):
        if connection_params:
            self.connection_params = connection_params
        else:
            self.connection_params = self._build_ob_connection_params()
        self.connection = None
        # 修改为正确的jar文件路径
        self.jar_file = os.path.join(os.path.dirname(__file__), '..', 'utils', 'OB', 'oceanbase-client-2.4.1.jar')
        self.driver = 'com.alipay.oceanbase.jdbc.Driver'

    def _build_ob_connection_params(self) -> Dict[str, str]:
        """
        从环境变量构建 OceanBase 连接参数

        Returns:
            Dict[str, str]: 数据库连接参数
        """
        # 从环境变量读取配置
        username = config_manager.get('database.username') or os.getenv('DB_USERNAME', '')
        password = config_manager.get('database.password') or os.getenv('DB_PASSWORD', '')
        tenant = config_manager.get('database.tenant') or os.getenv('DB_TENANT', '')
        host = config_manager.get('database.host') or os.getenv('DB_HOST', '')
        port = config_manager.get('database.port') or os.getenv('DB_PORT', '')
        database = config_manager.get('database.database') or os.getenv('DB_DATABASE', '')

        if not all([username, password, host, port, database]):
            raise ValueError("数据库连接参数不完整，请检查 env 文件变量设置。")

        # 构建 JDBC URL - 修改为正确的格式
        url = f"jdbc:oceanbase://{host}:{port}/{database}"

        # 如果有租户信息，用户名格式为 username@tenant
        if tenant:
            username = f"{username}@{tenant}"

        connection_params = {
            'url': url,
            'user': username,
            'password': password
        }

        logger.info(f"OceanBase 连接参数构建完成: {url}, 用户: {username}")
        return connection_params

    def connect(self):
        """建立数据库连接"""
        try:
            if not os.path.exists(self.jar_file):
                raise FileNotFoundError(f"找不到 OceanBase 驱动文件: {self.jar_file}")

            # 使用正确的连接方式
            self.connection = jaydebeapi.connect(
                self.driver,
                self.connection_params['url'],
                [self.connection_params['user'], self.connection_params['password']],
                self.jar_file
            )
            logger.info("OceanBase 数据库连接成功")
            return self.connection
        except Exception as e:
            logger.error(f"连接到数据库时发生错误: {e}")
            return None

    def close_connection(self):
        """关闭数据库连接"""
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                logger.info("数据库连接已关闭")
            except Exception as e:
                logger.error(f"关闭连接时发生错误: {e}")

    def get_cursor(self):
        """获取数据库游标"""
        if not self.connection:
            self.connect()

        if self.connection:
            return self.connection.cursor()
        return None

    def save_dataframe_to_table(self, df: pd.DataFrame, table_name: str,
                                if_exists: str = 'append') -> bool:
        """
        将DataFrame保存到数据库表

        Args:
            df: 要保存的DataFrame
            table_name: 目标表名
            if_exists: 如果表存在时的行为 ('fail', 'replace', 'append')

        Returns:
            bool: 保存是否成功
        """
        try:
            if df.empty:
                logger.warning(f"DataFrame为空，跳过保存到表 {table_name}")
                return True

            cursor = self.get_cursor()
            if not cursor:
                return False

            # 检查表是否存在
            table_exists = self.table_exists(table_name)

            if if_exists == 'replace' and table_exists:
                # 删除表
                cursor.execute(f"DROP TABLE {table_name}")
                logger.info(f"已删除表 {table_name}")
                table_exists = False
            elif if_exists == 'fail' and table_exists:
                raise ValueError(f"表 {table_name} 已存在")

            # 如果表不存在，创建表
            if not table_exists:
                self._create_table_from_dataframe(df, table_name, cursor)

            # 插入数据
            self._insert_dataframe_data(df, table_name, cursor)

            self.connection.commit()
            logger.info(f"成功保存 {len(df)} 条记录到表 {table_name}")
            return True

        except Exception as e:
            logger.error(f"保存DataFrame到表 {table_name} 时发生错误: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def _create_table_from_dataframe(self, df: pd.DataFrame, table_name: str, cursor):
        """根据DataFrame结构创建表"""
        columns = []
        for col, dtype in df.dtypes.items():
            if dtype == 'object':
                col_type = 'VARCHAR2(4000)'
            elif dtype == 'int64':
                col_type = 'NUMBER'
            elif dtype == 'float64':
                col_type = 'NUMBER'
            elif dtype == 'datetime64[ns]':
                col_type = 'DATE'
            else:
                col_type = 'VARCHAR2(4000)'

            columns.append(f"{col} {col_type}")

        create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
        cursor.execute(create_sql)
        logger.info(f"成功创建表 {table_name}")

    def _insert_dataframe_data(self, df: pd.DataFrame, table_name: str, cursor):
        """将DataFrame数据插入表中"""
        columns = list(df.columns)
        placeholders = ', '.join(['?' for _ in columns])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # 转换数据为适合数据库的格式
        data_rows = []
        for _, row in df.iterrows():
            row_data = []
            for val in row:
                if pd.isna(val):
                    row_data.append(None)
                else:
                    row_data.append(val)
            data_rows.append(row_data)

        # 批量插入
        cursor.executemany(insert_sql, data_rows)

    def save_dict_list_to_table(self, data: List[Dict[str, Any]], table_name: str,
                                if_exists: str = 'append') -> bool:
        """
        将字典列表保存到数据库表

        Args:
            data: 要保存的字典列表
            table_name: 目标表名
            if_exists: 如果表存在时的行为

        Returns:
            bool: 保存是否成功
        """
        try:
            if not data:
                logger.warning(f"数据列表为空，跳过保存到表 {table_name}")
                return True

            # 将字典列表转换为DataFrame
            df = pd.DataFrame(data)
            return self.save_dataframe_to_table(df, table_name, if_exists)

        except Exception as e:
            logger.error(f"保存字典列表到表 {table_name} 时发生错误: {e}")
            return False

    def execute_query(self, query: str, params: List[Any] = None) -> pd.DataFrame:
        """
        执行SQL查询并返回结果

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            pd.DataFrame: 查询结果
        """
        try:
            cursor = self.get_cursor()
            if not cursor:
                return pd.DataFrame()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # 获取列名
            columns = [desc[0] for desc in cursor.description]

            # 获取数据
            rows = cursor.fetchall()

            # 创建DataFrame
            result = pd.DataFrame(rows, columns=columns)

            logger.info(f"查询执行成功，返回 {len(result)} 条记录")
            return result

        except Exception as e:
            logger.error(f"执行查询时发生错误: {e}")
            return pd.DataFrame()

    def execute_sql(self, sql: str, params: List[Any] = None) -> bool:
        """
        执行SQL语句（非查询）

        Args:
            sql: SQL语句
            params: 参数

        Returns:
            bool: 执行是否成功
        """
        try:
            cursor = self.get_cursor()
            if not cursor:
                return False

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            self.connection.commit()
            logger.info("SQL语句执行成功")
            return True

        except Exception as e:
            logger.error(f"执行SQL语句时发生错误: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在

        Args:
            table_name: 表名

        Returns:
            bool: 表是否存在
        """
        try:
            cursor = self.get_cursor()
            if not cursor:
                return False

            # 查询表是否存在
            sql = """
                SELECT COUNT(*) 
                FROM USER_TABLES 
                WHERE TABLE_NAME = UPPER(?)
            """
            cursor.execute(sql, [table_name])
            result = cursor.fetchone()
            exists = result[0] > 0

            return exists

        except Exception as e:
            logger.error(f"检查表 {table_name} 是否存在时发生错误: {e}")
            return False

    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        根据DataFrame结构创建表

        Args:
            df: 参考的DataFrame
            table_name: 要创建的表名

        Returns:
            bool: 创建是否成功
        """
        try:
            if self.table_exists(table_name):
                logger.info(f"表 {table_name} 已存在")
                return True

            cursor = self.get_cursor()
            if not cursor:
                return False

            self._create_table_from_dataframe(df, table_name, cursor)
            self.connection.commit()
            return True

        except Exception as e:
            logger.error(f"创建表 {table_name} 时发生错误: {e}")
            if self.connection:
                self.connection.rollback()
            return False
