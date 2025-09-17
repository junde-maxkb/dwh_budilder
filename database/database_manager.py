import logging
import os
import cx_Oracle
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict, Any
import threading
import time
from common.config import config_manager

load_dotenv()

logger = logging.getLogger(__name__)


class DataBaseManager:
    # 类级别的锁，用于防止并发创建表
    _table_creation_locks = {}
    _locks_lock = threading.Lock()
    # 表存在性缓存，避免重复查询
    _table_exists_cache = {}
    _cache_lock = threading.Lock()

    def __init__(self):
        pass

    def _get_table_lock(self, table_name: str) -> threading.Lock:
        """获取指定表的创建锁"""
        with self._locks_lock:
            if table_name not in self._table_creation_locks:
                self._table_creation_locks[table_name] = threading.Lock()
            return self._table_creation_locks[table_name]

    def _update_table_cache(self, table_name: str, exists: bool):
        """更新表存在性缓存"""
        with self._cache_lock:
            self._table_exists_cache[table_name] = exists

    def _get_cached_table_exists(self, table_name: str) -> bool:
        """从缓存获取表存在性，如果缓存中不存在则返回None"""
        with self._cache_lock:
            return self._table_exists_cache.get(table_name)

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
            return conn
        except Exception as e:
            logger.error(f"OceanBase 连接失败: {e}")
            return None

    def _generate_column_definition(self, dtype) -> str:
        if pd.api.types.is_integer_dtype(dtype):
            return "NUMBER(19)"
        elif pd.api.types.is_float_dtype(dtype):
            return "NUMBER(18,6)"
        elif pd.api.types.is_bool_dtype(dtype):
            return "CHAR(1)"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATE"
        elif pd.api.types.is_object_dtype(dtype):
            return "VARCHAR2(255)"
        else:
            return "VARCHAR2(4000)"

    def _create_table_sql(self, df: pd.DataFrame, table_name: str) -> str:
        """生成创建表的 SQL 语句"""
        columns = []
        for column_name, dtype in df.dtypes.items():
            # 清理列名（空格、横杠、特殊字符）
            clean_column_name = (
                str(column_name)
                .strip()
                .replace(" ", "_")
                .replace("-", "_")
                .replace("/", "_")
            )
            column_def = f'"{clean_column_name}" {self._generate_column_definition(dtype)}'
            columns.append(column_def)

        columns_sql = ",\n  ".join(columns)
        create_sql = f'CREATE TABLE "{table_name}" (\n  {columns_sql}\n)'
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

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        :param table_name: 表名
        """
        logger.info(f"检查表: {table_name} 是否存在")
        # 先从缓存中获取表存在性
        cached_exists = self._get_cached_table_exists(table_name)
        if cached_exists is not None:
            logger.info(f"表 {table_name} 存在性缓存命中: {cached_exists}")
            return cached_exists

        conn = self.connect()
        if not conn:
            return False
        try:
            cursor = conn.cursor()
            sql = "SELECT COUNT(*) FROM user_tables WHERE table_name = UPPER(:table_name)"
            cursor.execute(sql, {"table_name": table_name})

            result = cursor.fetchone()
            exists = result and result[0] > 0

            # 更新缓存
            self._update_table_cache(table_name, exists)

            return exists
        except Exception as e:
            logger.error(f"检查表 {table_name} 是否存在时发生错误: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        如果表不存在则根据 DataFrame 创建表
        使用锁机制防止并发创建表的竞态条件
        """
        # 获取表级锁
        table_lock = self._get_table_lock(table_name)

        with table_lock:
            # 在锁内再次检查表是否存在，防止重复创建
            if self.table_exists(table_name):
                logger.info(f"表 {table_name} 已存在，跳过创建")
                return True

            conn = self.connect()
            if not conn:
                return False

            try:
                create_sql = self._create_table_sql(df, table_name)
                cursor = conn.cursor()
                cursor.execute(create_sql)
                conn.commit()
                cursor.close()

                logger.info(f"成功创建表 {table_name}")
                return True
            except cx_Oracle.DatabaseError as e:
                error_code = e.args[0].code if hasattr(e.args[0], 'code') else None
                if error_code == 955:  # ORA-00955: name is already used by an existing object
                    logger.warning(f"表 {table_name} 已存在（并发创建），继续执行")
                    return True
                else:
                    logger.error(f"创建表 {table_name} 时发生数据库错误: {e}")
                    return False
            except Exception as e:
                logger.error(f"创建表 {table_name} 时发生错误: {e}")
                return False
            finally:
                if conn:
                    conn.close()

    def _safe_create_table(self, df: pd.DataFrame, table_name: str, conn) -> bool:
        """
        安全创建表的内部方法，处理并发创建的异常情况
        增强的错误处理和缓存更新
        """
        try:
            # 再次检查表是否存在（双重检查），避免并发创建
            if self.table_exists(table_name):
                logger.info(f"表 {table_name} 已存在，跳过创建")
                return True

            create_sql = self._create_table_sql(df, table_name)
            cursor = conn.cursor()
            cursor.execute(create_sql)
            conn.commit()
            cursor.close()

            # 创建成功后立即更新缓存
            self._update_table_cache(table_name, True)
            logger.info(f"成功创建表 {table_name}")
            return True

        except cx_Oracle.DatabaseError as e:
            error_code = e.args[0].code if hasattr(e.args[0], 'code') else None
            if error_code == 955:  # ORA-00955: name is already used by an existing object
                logger.warning(f"表 {table_name} 已存在（并发创建检测到），更新缓存并继续执行")
                # 更新缓存，表明表已存在
                self._update_table_cache(table_name, True)
                return True
            else:
                logger.error(f"创建表 {table_name} 时发生数据库错误 (代码: {error_code}): {e}")
                return False
        except Exception as e:
            logger.error(f"创建表 {table_name} 时发生未知错误: {e}")
            return False

    def _safe_save_dataframe_to_table(self, df: pd.DataFrame, table_name: str, if_exists: str, conn) -> bool:
        """
        安全的数据保存方法，增强错误处理
        """
        try:
            if df.empty:
                logger.warning(f"DataFrame为空，跳过保存到表 {table_name}")
                return True

            # 检查表是否存在
            table_exists = self.table_exists(table_name)

            if not table_exists:
                if if_exists == 'fail':
                    logger.error(f"表 {table_name} 不存在且 if_exists='fail'")
                    return False
                else:
                    logger.error(f"表 {table_name} 不存在，但应该已经创建")
                    return False
            elif if_exists == 'replace':
                # 删除表并重新创建
                self.execute_sql(f'DROP TABLE "{table_name}"')
                if not self._safe_create_table(df, table_name, conn):
                    return False
                logger.info(f"表 {table_name} 已被替换")
            elif if_exists == 'fail':
                logger.error(f"表 {table_name} 已存在且 if_exists='fail'")
                return False

            logger.info(f"开始插入数据到表 {table_name}，模式: {if_exists}")
            # 插入数据
            return self._insert_dataframe_bulk(df, table_name, conn)

        except Exception as e:
            logger.error(f"保存DataFrame到表 {table_name} 时发生错误: {e}")
            return False

    def save_dataframe_to_table(self, df: pd.DataFrame, table_name: str,
                                if_exists: str = 'append') -> bool:
        """
        保存DataFrame到表，增强版本
        """
        if df.empty:
            logger.warning(f"DataFrame为空，跳过保存到表 {table_name}")
            return True

        conn = self.connect()
        if not conn:
            return False

        try:
            return self._safe_save_dataframe_to_table(df, table_name, if_exists, conn)
        except Exception as e:
            logger.error(f"保存DataFrame到表 {table_name} 时发生错误: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def auto_create_and_save_data(self, data: List[Dict[str, Any]], table_name: str, if_exists: str = 'append') -> bool:
        """
        自动根据数据结构建表并写入数据，避免重复建表，确保数据写入成功
        增强的并发安全版本
        """
        if not data:
            logger.warning(f"数据列表为空，跳过表 {table_name} 的创建和保存")
            return True

        df = pd.DataFrame(data)
        if df.empty:
            logger.warning(f"DataFrame为空，跳过保存到表 {table_name}")
            return True

        # 获取表级锁
        table_lock = self._get_table_lock(table_name)

        with table_lock:
            conn = self.connect()
            if not conn:
                return False

            try:
                # 在锁内检查表是否存在
                table_exists = self.table_exists(table_name)
                if not table_exists:
                    logger.info(f"表 {table_name} 不存在，自动创建...")
                    df_optimized = self._optimize_dataframe_dtypes(df)

                    # 尝试创建表，使用重试机制
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            if self._safe_create_table(df_optimized, table_name, conn):
                                logger.info(f"成功创建表 {table_name}")
                                break
                        except Exception as e:
                            if attempt == max_retries - 1:
                                logger.error(f"创建表 {table_name} 失败，已重试 {max_retries} 次: {e}")
                                return False
                            else:
                                logger.warning(f"创建表 {table_name} 失败，第 {attempt + 1} 次重试: {e}")
                                time.sleep(0.1 * (attempt + 1))  # 递增延迟

                # 数据写入，支持append/replace/fail
                return self._safe_save_dataframe_to_table(df, table_name, if_exists, conn)

            except Exception as e:
                logger.error(f"自动创建表并保存数据时发生错误: {e}")
                return False
            finally:
                if conn:
                    conn.close()

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
            except:
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

    def check_traditional_data_exists(self, data_type: str, company_code: str, year: str = None,
                                      period_code: str = None) -> bool:
        """
        检查传统财务数据是否已存在

        Args:
            data_type: 数据类型
            company_code: 公司代码
            year: 年份（可选）
            period_code: 期间代码（可选）

        Returns:
            bool: 数据是否存在
        """
        table_name = f"raw_{data_type}"

        # 检查表是否存在
        if not self.table_exists(table_name):
            return False

        conditions = {
            'company_code': company_code
        }

        # 根据数据类型添加不同的条件
        if data_type in ['account_structure', 'subject_dimension']:
            # 按年份数据
            if year:
                conditions['year'] = year
        elif data_type in ['voucher_list', 'voucher_detail', 'voucher_dim_detail', 'balance', 'aux_balance']:
            # 按期间数据
            if period_code:
                conditions['period_code'] = period_code
        elif data_type == 'customer_vendor':
            # 客商数据只需要公司代码
            pass

        return self.check_data_exists(table_name, conditions)

    def check_data_exists(self, table_name: str, conditions: Dict[str, Any]) -> bool:
        """
        检查数据是否已存在于数据库中

        Args:
            table_name: 表名
            conditions: 查询条件字典

        Returns:
            bool: 数据是否存在
        """
        if not self.table_exists(table_name):
            return False

        conn = self.connect()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # 构建查询条件
            where_clauses = []
            params = {}

            for key, value in conditions.items():
                where_clauses.append(f'"{key}" = :{key}')
                params[key] = value

            where_clause = " AND ".join(where_clauses)
            sql = f'SELECT COUNT(*) FROM "{table_name}" WHERE {where_clause}'

            cursor.execute(sql, params)
            count = cursor.fetchone()[0]
            cursor.close()

            exists = count > 0
            logger.debug(f"检查数据存在性 - 表: {table_name}, 条件: {conditions}, 存在: {exists}")
            return exists

        except Exception as e:
            logger.error(f"检查数据存在性时发生错误: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def check_financial_report_data_exists(self, company_id: str, period_detail_id: str,
                                           table_name: str = "raw_financial_reports") -> bool:
        """
        检查财务报表数据是否已存在

        Args:
            company_id: 公司ID
            period_detail_id: 期间详细ID
            table_name: 表名

        Returns:
            bool: 数据是否存在
        """
        conditions = {
            'company_id': company_id,
            'period_detail_id': period_detail_id
        }
        return self.check_data_exists(table_name, conditions)

    def close_engine(self):
        """
        关闭数据库引擎（保持兼容性）
        """
        logger.info("数据库管理器已关闭")
