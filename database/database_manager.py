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
        ob_username = f"{username}"
        ob_connection = f"{host}:{port}/{database}"

        return ob_username, password, ob_connection

    def connect(self):
        try:
            username, password, oracle_connection = self._get_ob_connection_params()
            conn = cx_Oracle.connect(username, password, oracle_connection)
            logger.info("OceanBase 数据库连接成功")
            return conn
        except Exception as e:
            logger.error(f"OceanBase 连接失败: {e}")
            return None

    def close_connection(self, connection):
        if connection:
            try:
                connection.close()
                logger.info("数据库连接已关闭")
            except Exception as e:
                logger.error(f"关闭连接时发生错误: {e}")

    def _generate_column_definition(self, dtype) -> str:
        """根据 pandas dtype 生成 Oracle 列定义"""
        if pd.api.types.is_integer_dtype(dtype):
            return "NUMBER(18,0)"
        elif pd.api.types.is_float_dtype(dtype):
            return "NUMBER(18,6)"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATE"
        elif pd.api.types.is_bool_dtype(dtype):
            return "NUMBER(1,0)"
        else:
            return "NVARCHAR2(4000)"

    def _create_table_sql(self, df: pd.DataFrame, table_name: str) -> str:
        """生成创建表的 SQL 语句"""
        columns = []
        for column_name, dtype in df.dtypes.items():
            clean_column_name = str(column_name).replace(' ', '_').replace('-', '_')
            column_def = f'"{clean_column_name}" {self._generate_column_definition(dtype)}'
            columns.append(column_def)

        create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
        return create_sql

    def _insert_dataframe_bulk(self, df: pd.DataFrame, table_name: str, conn) -> bool:
        """使用批量插入方式插入 DataFrame 数据"""
        try:
            cursor = conn.cursor()

            # 准备列名
            columns = [f'"{col}"' for col in df.columns]
            placeholders = ', '.join([':' + str(i + 1) for i in range(len(df.columns))])

            insert_sql = f'INSERT INTO "{table_name}" ({", ".join(columns)}) VALUES ({placeholders})'

            # 准备数据
            data_rows = []
            for _, row in df.iterrows():
                row_data = []
                for value in row:
                    if pd.isna(value):
                        row_data.append(None)
                    elif isinstance(value, (pd.Timestamp, pd.DatetimeIndex)):
                        row_data.append(value.to_pydatetime() if hasattr(value, 'to_pydatetime') else value)
                    else:
                        row_data.append(value)
                data_rows.append(tuple(row_data))

            # 批量插入
            cursor.executemany(insert_sql, data_rows)
            conn.commit()
            cursor.close()

            logger.info(f"成功插入 {len(data_rows)} 条记录到表 {table_name}")
            return True

        except Exception as e:
            logger.error(f"批量插入数据到表 {table_name} 时发生错误: {e}")
            conn.rollback()
            return False

    def save_dataframe_to_table(self, df: pd.DataFrame, table_name: str,
                                if_exists: str = 'append') -> bool:
        if df.empty:
            logger.warning(f"DataFrame为空，跳过保存到表 {table_name}")
            return True

        conn = self.connect()
        if not conn:
            return False

        try:
            # 检查表是否存在
            table_exists = self.table_exists(table_name)

            if not table_exists:
                if if_exists == 'fail':
                    logger.error(f"表 {table_name} 不存在且 if_exists='fail'")
                    return False
                else:
                    # 创建表
                    if not self.create_table_from_dataframe(df, table_name):
                        return False
            elif if_exists == 'replace':
                # 删除表并重新创建
                self.execute_sql(f'DROP TABLE "{table_name}"')
                if not self.create_table_from_dataframe(df, table_name):
                    return False
            elif if_exists == 'fail':
                logger.error(f"表 {table_name} 已存在且 if_exists='fail'")
                return False

            # 插入数据
            return self._insert_dataframe_bulk(df, table_name, conn)

        except Exception as e:
            logger.error(f"保存DataFrame到表 {table_name} 时发生错误: {e}")
            return False
        finally:
            self.close_connection(conn)

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        conn = self.connect()
        if not conn:
            return pd.DataFrame()

        try:
            result = pd.read_sql(query, conn, params=params)
            logger.info(f"查询执行成功，返回 {len(result)} 条记录")
            return result
        except Exception as e:
            logger.error(f"执行查询时发生错误: {e}")
            return pd.DataFrame()
        finally:
            self.close_connection(conn)

    def execute_sql(self, sql: str, params: Dict[str, Any] = None) -> bool:
        conn = self.connect()
        if not conn:
            return False

        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            conn.commit()
            cursor.close()
            logger.info("SQL语句执行成功")
            return True
        except Exception as e:
            logger.error(f"执行SQL语句时发生错误: {e}")
            return False
        finally:
            self.close_connection(conn)

    def table_exists(self, table_name: str) -> bool:
        conn = self.connect()
        if not conn:
            return False

        try:
            sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'common', 'sql', 'check_table.sql')
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql = f.read().strip()

            cursor = conn.cursor()
            cursor.execute(sql, {"table_name": table_name})
            result = cursor.fetchone()
            exists = result[0] > 0 if result else False
            cursor.close()
            return exists
        except Exception as e:
            logger.error(f"检查表 {table_name} 是否存在时发生错误: {e}")
            return False
        finally:
            self.close_connection(conn)

    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        if self.table_exists(table_name):
            logger.info(f"表 {table_name} 已存在")
            return True

        conn = self.connect()
        if not conn:
            return False

        try:
            # 使用原生 SQL 创建表
            create_sql = self._create_table_sql(df, table_name)
            cursor = conn.cursor()
            cursor.execute(create_sql)
            conn.commit()
            cursor.close()

            logger.info(f"成功创建表 {table_name}")
            return True
        except Exception as e:
            logger.error(f"创建表 {table_name} 时发生错误: {e}")
            return False
        finally:
            self.close_connection(conn)

    def auto_create_and_save_data(self, data: List[Dict[str, Any]], table_name: str,
                                  if_exists: str = 'append') -> bool:
        if not data:
            logger.warning(f"数据列表为空，跳过表 {table_name} 的创建和保存")
            return True

        try:
            df = pd.DataFrame(data)

            # 检查表是否存在
            if not self.table_exists(table_name):
                logger.info(f"表 {table_name} 不存在，开始自动创建...")
                df_optimized = self._optimize_dataframe_dtypes(df)

                if not self.create_table_from_dataframe(df_optimized, table_name):
                    logger.error(f"创建表 {table_name} 失败")
                    return False
                logger.info(f"成功创建表 {table_name}")

            # 保存数据
            return self.save_dataframe_to_table(df, table_name, if_exists)
        except Exception as e:
            logger.error(f"自动创建表并保存数据时发生错误: {e}")
            return False

    def _optimize_dataframe_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        df_optimized = df.copy()

        for column in df_optimized.columns:
            # 处理字符串类型
            if df_optimized[column].dtype == 'object':
                # 尝试转换为数值类型
                if self._is_numeric_column(df_optimized[column]):
                    df_optimized[column] = pd.to_numeric(df_optimized[column], errors='ignore')
                # 尝试转换为日期类型
                elif self._is_datetime_column(df_optimized[column]):
                    df_optimized[column] = pd.to_datetime(df_optimized[column], errors='ignore')

            # 处理整数类型优化
            elif df_optimized[column].dtype in ['int64', 'int32']:
                min_val = df_optimized[column].min()
                max_val = df_optimized[column].max()

                if min_val >= 0:
                    if max_val < 255:
                        df_optimized[column] = df_optimized[column].astype('uint8')
                    elif max_val < 65535:
                        df_optimized[column] = df_optimized[column].astype('uint16')
                    elif max_val < 4294967295:
                        df_optimized[column] = df_optimized[column].astype('uint32')
                else:
                    if min_val >= -128 and max_val <= 127:
                        df_optimized[column] = df_optimized[column].astype('int8')
                    elif min_val >= -32768 and max_val <= 32767:
                        df_optimized[column] = df_optimized[column].astype('int16')
                    elif min_val >= -2147483648 and max_val <= 2147483647:
                        df_optimized[column] = df_optimized[column].astype('int32')

        return df_optimized

    def _is_numeric_column(self, series: pd.Series) -> bool:
        """检查列是否应该转换为数值类型"""
        sample_values = series.dropna().head(100)
        if len(sample_values) == 0:
            return False

        numeric_count = 0
        for value in sample_values:
            try:
                float(str(value))
                numeric_count += 1
            except Exception as e:
                logger.error(f"检查数值转换时发生错误: {e}")
                pass

        return numeric_count / len(sample_values) > 0.8

    def _is_datetime_column(self, series: pd.Series) -> bool:
        """检查列是否应该转换为日期类型"""
        sample_values = series.dropna().head(50)
        if len(sample_values) == 0:
            return False

        import re
        datetime_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format
        ]

        datetime_count = 0
        for value in sample_values:
            str_value = str(value)
            for pattern in datetime_patterns:
                if re.match(pattern, str_value):
                    datetime_count += 1
                    break

        return datetime_count / len(sample_values) > 0.8

    def create_table_with_schema(self, table_name: str, schema: Dict[str, str]) -> bool:
        if self.table_exists(table_name):
            logger.info(f"表 {table_name} 已存在")
            return True

        conn = self.connect()
        if not conn:
            return False

        try:
            # 构建CREATE TABLE语句
            columns = [f'"{column_name}" {data_type}' for column_name, data_type in schema.items()]
            create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'

            cursor = conn.cursor()
            cursor.execute(create_sql)
            conn.commit()
            cursor.close()

            logger.info(f"成功创建表 {table_name} 使用自定义schema")
            return True
        except Exception as e:
            logger.error(f"创建表 {table_name} 时发生错误: {e}")
            return False
        finally:
            self.close_connection(conn)

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        conn = self.connect()
        if not conn:
            return {}

        try:
            cursor = conn.cursor()

            # 获取表结构
            cursor.execute(f'SELECT * FROM "{table_name}" WHERE ROWNUM <= 0')
            columns_info = []
            for desc in cursor.description:
                columns_info.append({
                    'name': desc[0],
                    'type': desc[1].__name__ if hasattr(desc[1], '__name__') else str(desc[1]),
                    'size': desc[2] if len(desc) > 2 else None
                })

            # 获取行数
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            row_count = cursor.fetchone()[0]
            cursor.close()

            return {
                'table_name': table_name,
                'columns': columns_info,
                'row_count': row_count,
                'exists': True
            }
        except Exception as e:
            logger.error(f"获取表 {table_name} 信息时发生错误: {e}")
            return {'table_name': table_name, 'exists': False, 'error': str(e)}
        finally:
            self.close_connection(conn)
