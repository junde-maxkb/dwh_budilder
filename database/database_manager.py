import logging
import os
import cx_Oracle
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict, Any
from common.config import config_manager

load_dotenv()

logger = logging.getLogger(__name__)


class DataBaseManager:
    def __init__(self):
        pass

    def _get_ob_connection_params(self) -> tuple:
        """
        获取 OceanBase Oracle 模式连接参数，用 cx_Oracle 连接

        Returns:
            tuple: (username@tenant, password, host:port/database)
        """
        # 从环境变量读取配置
        username = config_manager.get('database.username') or os.getenv('DB_USERNAME', '')
        password = config_manager.get('database.password') or os.getenv('DB_PASSWORD', '')
        tenant = config_manager.get('database.tenant') or os.getenv('DB_TENANT', '')
        host = config_manager.get('database.host') or os.getenv('DB_HOST', '')
        port = config_manager.get('database.port') or os.getenv('DB_PORT', '')
        database = config_manager.get('database.database') or os.getenv('DB_DATABASE', '')

        if not all([username, password, tenant, host, port, database]):
            raise ValueError("数据库连接参数不完整，请检查 env 文件变量设置。")

        # 按照 OceanBase 官方示例格式构建连接参数
        ob_username = f"{username}@{tenant}"
        ob_connection = f"{host}:{port}/{database}"

        return ob_username, password, ob_connection

    def connect(self):
        """
        使用 cx_Oracle 直接连接，按照 OceanBase 官方示例方式

        Returns:
            cx_Oracle.Connection: 数据库连接对象
        """
        try:
            username, password, oracle_connection = self._get_ob_connection_params()
            conn = cx_Oracle.connect(username, password, oracle_connection)
            logger.info("OceanBase 数据库连接成功")
            return conn
        except Exception as e:
            logger.error(f"OceanBase 连接失败: {e}")
            return None

    def close_connection(self, connection):
        """
        关闭数据库连接

        Args:
            connection: cx_Oracle连接对象
        """
        if connection:
            try:
                connection.close()
                logger.info("数据库连接已关闭")
            except Exception as e:
                logger.error(f"关闭连接时发生错误: {e}")

    def save_dataframe_to_table(self, df: pd.DataFrame, table_name: str,
                                if_exists: str = 'append', index: bool = False) -> bool:
        """
        将DataFrame保存到数据库表

        Args:
            df: 要保存的DataFrame
            table_name: 目标表名
            if_exists: 如果表存在时的行为 ('fail', 'replace', 'append')
            index: 是否保存索引

        Returns:
            bool: 保存是否成功
        """
        conn = self.connect()
        try:
            if df.empty:
                logger.warning(f"DataFrame为空，跳过保存到表 {table_name}")
                return True

            if not conn:
                return False

            # 使用pandas的to_sql方法保存数据，cx_Oracle连接对象可以直接使用
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists=if_exists,
                index=index,
                method='multi',  # 批量插入提高性能
                chunksize=1000  # 分批处理大数据集
            )

            logger.info(f"成功保存 {len(df)} 条记录到表 {table_name}")
            self.close_connection(conn)
            return True

        except Exception as e:
            logger.error(f"保存DataFrame到表 {table_name} 时发生错误: {e}")
            if conn:
                self.close_connection(conn)
            return False

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

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        执行SQL查询并返回结果

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            pd.DataFrame: 查询结果
        """
        conn = self.connect()
        try:
            if not conn:
                return pd.DataFrame()

            # 使用 pandas 读取 SQL，cx_Oracle 连接对象可以直接传递给 pandas
            result = pd.read_sql(query, conn, params=params)
            self.close_connection(conn)

            logger.info(f"查询执行成功，返回 {len(result)} 条记录")
            return result

        except Exception as e:
            logger.error(f"执行查询时发生错误: {e}")
            if conn:
                self.close_connection(conn)
            return pd.DataFrame()

    def execute_sql(self, sql: str, params: Dict[str, Any] = None) -> bool:
        """
        执行SQL语句（非查询）

        Args:
            sql: SQL语句
            params: 参数

        Returns:
            bool: 执行是否成功
        """
        conn = self.connect()
        try:
            if not conn:
                return False

            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            conn.commit()
            cursor.close()
            self.close_connection(conn)

            logger.info("SQL语句执行成功")
            return True

        except Exception as e:
            logger.error(f"执行SQL语句时发生错误: {e}")
            if conn:
                self.close_connection(conn)
            return False

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在

        Args:
            table_name: 表名

        Returns:
            bool: 表是否存在
        """
        conn = self.connect()
        try:
            if not conn:
                return False

            # 从文件中读取检查表是否存在的SQL
            sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'common', 'sql', 'check_table.sql')
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql = f.read().strip()

            cursor = conn.cursor()
            cursor.execute(sql, {"table_name": table_name})
            result = cursor.fetchone()
            exists = result[0] > 0 if result else False
            cursor.close()

            self.close_connection(conn)
            return exists

        except Exception as e:
            logger.error(f"检查表 {table_name} 是否存在时发生错误: {e}")
            if conn:
                self.close_connection(conn)
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
        conn = self.connect()
        try:
            if self.table_exists(table_name):
                logger.info(f"表 {table_name} 已存在")
                return True

            if not conn:
                return False

            # 使用pandas的to_sql创建表结构（只创建结构，不插入数据）
            empty_df = df.iloc[0:0].copy()  # 创建空的DataFrame但保留结构
            empty_df.to_sql(
                name=table_name,
                con=conn,
                if_exists='fail',
                index=False
            )

            logger.info(f"成功创建表 {table_name}")
            self.close_connection(conn)
            return True

        except Exception as e:
            logger.error(f"创建表 {table_name} 时发生错误: {e}")
            if conn:
                self.close_connection(conn)
            return False
