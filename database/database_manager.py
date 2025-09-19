import logging
import os
import cx_Oracle
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict, Any
import threading
import time
from common.config import config_manager
import re

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

    def _generate_column_definition(self, dtype, column_data=None) -> str:
        if pd.api.types.is_integer_dtype(dtype):
            return "NUMBER(19)"
        elif pd.api.types.is_float_dtype(dtype):
            return "NUMBER(18,6)"
        elif pd.api.types.is_bool_dtype(dtype):
            return "CHAR(1)"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATE"
        elif pd.api.types.is_object_dtype(dtype):
            # 动态检测字符串长度
            if column_data is not None:
                max_length = self._calculate_max_string_length(column_data)
                # 根据最大长度选择合适的数据类型
                if max_length <= 255:
                    return "VARCHAR2(255)"
                elif max_length <= 1000:
                    return "VARCHAR2(1000)"
                elif max_length <= 4000:
                    return "VARCHAR2(4000)"
                else:
                    # 超过4000字符使用CLOB
                    return "CLOB"
            else:
                # 默认使用较大的VARCHAR2
                return "VARCHAR2(4000)"
        else:
            return "VARCHAR2(4000)"

    def _calculate_max_string_length(self, column_data) -> int:
        max_length = 0
        try:
            # 处理前100个样本来估算最大长度
            sample_data = column_data.dropna().head(100) if hasattr(column_data, 'dropna') else column_data[:100]

            for value in sample_data:
                if value is not None:
                    # 先通过 _process_data_value 处理数据，获得最终会存储的值
                    processed_value = self._process_data_value(value)
                    if processed_value is not None:
                        # 转换为字符串并计算长度
                        str_value = str(processed_value)
                        # 考虑UTF-8编码，中文字符可能占用更多字节
                        byte_length = len(str_value.encode('utf-8'))
                        max_length = max(max_length, byte_length)

            # 增加50%的缓冲空间，以应对更复杂的数据
            max_length = int(max_length * 1.5)

            # 确保至少有基本长度
            max_length = max(max_length, 255)

        except Exception as e:
            logger.warning(f"计算字符串长度时出错: {e}，使用默认长度")
            max_length = 4000

        return min(max_length, 32767)  # Oracle CLOB最大长度限制

    def _create_table_sql(self, df: pd.DataFrame, table_name: str) -> str:
        """生成创建表的 SQL 语句"""
        columns = []
        for column_name, dtype in df.dtypes.items():
            # 增强的列名清理逻辑，符合Oracle命名规范
            clean_column_name = self._clean_column_name(str(column_name))
            # 传入列数据用于动态长度检测
            column_data = df[column_name] if column_name in df.columns else None
            column_def = f'"{clean_column_name}" {self._generate_column_definition(dtype, column_data)}'
            columns.append(column_def)

        columns_sql = ",\n  ".join(columns)
        create_sql = f'CREATE TABLE "{table_name}" (\n  {columns_sql}\n)'
        return create_sql

    def _clean_column_name(self, column_name: str) -> str:
        clean_name = column_name.strip()
        clean_name = re.sub(r'[^a-zA-Z0-9_$#]', '_', clean_name)
        if clean_name and not clean_name[0].isalpha():
            clean_name = 'COL_' + clean_name
        if len(clean_name) > 30:
            clean_name = clean_name[:26] + '_' + str(hash(column_name) % 1000).zfill(3)
        if not clean_name:
            clean_name = f'COL_{hash(column_name) % 10000}'

        return clean_name.upper()

    def _insert_dataframe_bulk(self, df: pd.DataFrame, table_name: str, conn) -> bool:
        try:
            cursor = conn.cursor()

            table_structure = self._get_table_structure(table_name, conn)

            cleaned_columns = [f'"{self._clean_column_name(str(col))}"' for col in df.columns]
            placeholders = ', '.join([':' + str(i + 1) for i in range(len(df.columns))])

            insert_sql = f'INSERT INTO "{table_name}" ({", ".join(cleaned_columns)}) VALUES ({placeholders})'

            batch_size = min(5000, len(df))
            total_rows = len(df)
            total_inserted = 0

            logger.info(f"开始分批次插入数据到表 {table_name}，总记录数: {total_rows}，批次大小: {batch_size}")

            # 分批处理数据
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]

                # 准备当前批次的数据
                batch_data = []
                for _, row in batch_df.iterrows():
                    row_data = []
                    for i, value in enumerate(row):
                        try:
                            # 增强的数据值处理逻辑
                            processed_value = self._process_data_value(value)

                            # 数据长度验证和截断
                            if table_structure and i < len(table_structure):
                                column_info = table_structure[i]
                                processed_value = self._validate_and_truncate_value(
                                    processed_value, column_info
                                )

                            row_data.append(processed_value)
                        except Exception as e:
                            # 如果无法处理数据值，记录详细错误并使用None
                            # logger.warning(f"无法处理数据值，使用None替代")
                            row_data.append(None)
                    batch_data.append(tuple(row_data))

                # 插入当前批次
                try:
                    cursor.executemany(insert_sql, batch_data)
                    conn.commit()
                    total_inserted += len(batch_data)

                    # 记录进度
                    progress = (total_inserted / total_rows) * 100
                    logger.info(f"已插入 {total_inserted}/{total_rows} 条记录 ({progress:.1f}%)")

                except Exception as batch_error:
                    logger.error(f"插入第 {start_idx}-{end_idx} 批次数据时发生错误: {batch_error}")
                    conn.rollback()
                    cursor.close()
                    return False

            cursor.close()
            logger.info(f"成功完成所有批次插入，总共插入 {total_inserted} 条记录到表 {table_name}")
            return True

        except Exception as e:
            logger.error(f"批量插入数据到表 {table_name} 时发生错误: {e}")
            if 'conn' in locals():
                conn.rollback()
            return False

    def _get_table_structure(self, table_name: str, conn) -> List[Dict]:
        try:
            cursor = conn.cursor()
            sql = """
                SELECT column_name, data_type, data_length, char_length
                FROM user_tab_columns 
                WHERE table_name = UPPER(:table_name)
                ORDER BY column_id
            """
            cursor.execute(sql, {"table_name": table_name})

            structure = []
            for row in cursor.fetchall():
                structure.append({
                    'column_name': row[0],
                    'data_type': row[1],
                    'data_length': row[2],
                    'char_length': row[3]
                })

            cursor.close()
            return structure

        except Exception as e:
            logger.warning(f"获取表结构失败: {e}")
            return []

    def _validate_and_truncate_value(self, value, column_info: Dict):
        if value is None:
            return None

        data_type = column_info.get('data_type', '')
        char_length = column_info.get('char_length', 0)
        data_length = column_info.get('data_length', 0)

        if data_type.startswith('VARCHAR') and isinstance(value, str):
            # 使用char_length（字符长度）而不是data_length（字节长度）
            max_length = char_length if char_length > 0 else data_length

            # 计算字符串的实际字节长度（考虑UTF-8编码）
            try:
                byte_length = len(value.encode('utf-8'))
                char_count = len(value)

                # 如果字节长度超过限制，或者字符数超过限制，都需要截断
                if byte_length > max_length or char_count > max_length:
                    logger.warning(
                        f"数据值长度超过限制 - 字符数: {char_count}, 字节数: {byte_length}, "
                        f"列 {column_info['column_name']} 最大长度: {max_length}，进行截断")

                    # 安全截断，确保不超过字节和字符限制
                    truncated = self._safe_truncate_string_by_bytes(value, max_length)
                    return truncated
            except UnicodeEncodeError as e:
                logger.warning(f"字符编码错误: {e}，使用默认截断方式")
                truncated = self._safe_truncate_string(value, max_length)
                return truncated

        elif data_type == 'CLOB':
            if isinstance(value, str) and len(value) > 32767:
                logger.warning(f"CLOB数据过长 {len(value)}，截断到32767字符")
                return value[:32767]

        return value

    def _safe_truncate_string(self, text: str, max_length: int) -> str:
        if len(text) <= max_length:
            return text

        if max_length > 3:
            truncated = text[:max_length - 3] + "..."
        else:
            truncated = text[:max_length]

        # 确保截断后的字符串可以正确编码
        try:
            truncated.encode('utf-8')
            return truncated
        except UnicodeEncodeError:
            # 如果编码失败，进一步缩短
            return text[:max_length // 2] + "..."

    def _safe_truncate_string_by_bytes(self, text: str, max_bytes: int) -> str:
        if not text:
            return text

        # 如果原文本的字节长度已经符合要求，直接返回
        if len(text.encode('utf-8')) <= max_bytes:
            return text

        # 为"..."预留3个字节
        if max_bytes <= 3:
            return text.encode('utf-8')[:max_bytes].decode('utf-8', errors='ignore')

        target_bytes = max_bytes - 3

        # 二分查找最大可截断的字符位置
        left, right = 0, len(text)
        result_pos = 0

        while left <= right:
            mid = (left + right) // 2
            try:
                substr = text[:mid]
                if len(substr.encode('utf-8')) <= target_bytes:
                    result_pos = mid
                    left = mid + 1
                else:
                    right = mid - 1
            except UnicodeError:
                right = mid - 1

        # 安全截断并添加省略号
        try:
            truncated = text[:result_pos] + "..."
            # 验证截断结果的字节长度
            if len(truncated.encode('utf-8')) <= max_bytes:
                return truncated
            else:
                # 如果还是太长，进一步缩短
                return text[:result_pos - 1] + "..."
        except (UnicodeError, IndexError):
            # 如果出现编码错误，使用更保守的方法
            safe_text = text.encode('utf-8')[:target_bytes].decode('utf-8', errors='ignore')
            return safe_text + "..."

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

    def _safe_create_table(self, df: pd.DataFrame, table_name: str, conn) -> bool:
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
                self.execute_sql(f'DROP TABLE "{table_name}"')
                if not self._safe_create_table(df, table_name, conn):
                    return False
                logger.info(f"表 {table_name} 已被替换")
            elif if_exists == 'fail':
                logger.error(f"表 {table_name} 已存在�� if_exists='fail'")
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
                # 尝试转换为数据类型
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
        """检查列是否应该转为数值类型"""
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
        检查传统财务数据是否存在

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
        if not self.table_exists(table_name):
            return False

        conn = self.connect()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            column_sql = """
                SELECT column_name 
                FROM user_tab_columns 
                WHERE table_name = UPPER(:table_name)
            """
            cursor.execute(column_sql, {"table_name": table_name})
            existing_columns = [row[0].lower() for row in cursor.fetchall()]

            # 构建查询条件，只使用存在的列
            where_clauses = []
            params = {}

            for key, value in conditions.items():
                if key.lower() in existing_columns or key.upper() in [col.upper() for col in existing_columns]:
                    where_clauses.append(f'"{key}" = :{key}')
                    params[key] = value
                else:
                    logger.warning(f"表 {table_name} 中不存在列 {key}，跳过此条件")

            if not where_clauses:
                logger.warning(f"表 {table_name} 中没有匹配的查询条件列，返回False")
                return False

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
        conditions = {
            'company_id': company_id,
            'period_detail_id': period_detail_id
        }
        return self.check_data_exists(table_name, conditions)

    def close_engine(self):
        logger.info("数据库管理器已关闭")

    def _process_date_value(self, value):
        import re
        from datetime import datetime

        if value is None or pd.isna(value):
            return None

        # 如果已经是日期时间对象，直接返回
        if isinstance(value, (datetime, pd.Timestamp)):
            return value.to_pydatetime() if hasattr(value, 'to_pydatetime') else value

        # 如果是字符串且看起来像日期，尝试转换
        if isinstance(value, str):
            value_str = value.strip()

            # 检查无效日期格式
            if self._is_invalid_date_format(value_str):
                # logger.warning(f"检测到无效日期格式: {value_str}，返回None")
                return None

            # 常见的日期格式模式
            date_patterns = [
                (r'^\d{4}-\d{2}-\d{2}$', '%Y-%m-%d'),  # 2024-01-01
                (r'^\d{4}/\d{2}/\d{2}$', '%Y/%m/%d'),  # 2024/01/01
                (r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', '%Y-%m-%d %H:%M:%S'),  # 2024-01-01 12:00:00
                (r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$', '%Y-%m-%dT%H:%M:%S'),  # ISO format
                (r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z?$', None),  # ISO with microseconds
                (r'^\d{2}/\d{2}/\d{4}$', '%m/%d/%Y'),  # 01/01/2024
                (r'^\d{2}-\d{2}-\d{4}$', '%m-%d-%Y'),  # 01-01-2024
            ]

            for pattern, date_format in date_patterns:
                if re.match(pattern, value_str):
                    try:
                        if date_format:
                            parsed_date = datetime.strptime(value_str, date_format)
                            # 验证日期的合理性
                            if self._is_reasonable_date(parsed_date):
                                return parsed_date
                        else:
                            # 对于复杂的ISO格式，使用pandas解析
                            parsed_date = pd.to_datetime(value_str, errors='coerce')
                            if not pd.isna(parsed_date) and self._is_reasonable_date(parsed_date):
                                return parsed_date.to_pydatetime()
                    except (ValueError, TypeError):
                        # 如果解析失败，记录警告但继续���理
                        logger.warning(f"��法解析日期格式: {value_str}")
                        break

        return value

    def _is_reasonable_date(self, date_obj) -> bool:
        if date_obj is None:
            return False

        year = date_obj.year if hasattr(date_obj, 'year') else None
        if year is None:
            return False

        return 1900 <= year <= 2100

    def _process_data_value(self, value):
        # 处理None和NaN值 - 使用更安全的检查方式
        try:
            if value is None:
                return None
            # 安全的NaN检查，避免pandas数组歧义
            if hasattr(pd, 'isna') and pd.isna(value):
                return None
        except (ValueError, TypeError):
            # 如果pandas.isna()对某些类型报错，跳过
            pass

        # 处理列表和数组类型数据 - 使用isinstance而不是真值判断
        if isinstance(value, (list, tuple)):
            try:
                # 使用len()而不是直接判断真值，避免pandas数组歧义
                if len(value) == 0:
                    return None
                # 对于复杂数组数据，先尝试转换为JSON字符串，然后检查长度
                import json
                json_str = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
                # 限制JSON字符串的最大长度，防止超过数据库列限制
                # 进一步减少长度限制，确保安全
                return self._safe_truncate_string_by_bytes(json_str, 2000)  # 更保守的长度限制
            except Exception as e:
                logger.warning(f"JSON序列化失败: {e}，使用截断的字符串转换")
                str_value = str(value)
                return self._safe_truncate_string_by_bytes(str_value, 2000)

        # 处理字典类型数据
        if isinstance(value, dict):
            try:
                import json
                json_str = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
                # 限制JSON字符串的最大长度
                return self._safe_truncate_string_by_bytes(json_str, 2000)
            except Exception as e:
                logger.warning(f"字典JSON序列化失败: {e}，使用截断的字符串转换")
                str_value = str(value)
                return self._safe_truncate_string_by_bytes(str_value, 2000)

        # 处理numpy数组和pandas对象 - 避免直接真值判断
        if hasattr(value, '__array__') or (hasattr(value, '__len__') and not isinstance(value, str)):
            try:
                # 检查是否为pandas Series或DataFrame
                if hasattr(value, 'empty'):
                    # 这是pandas对象
                    if value.empty:
                        return None
                    # 转换为列表后处理
                    try:
                        if hasattr(value, 'tolist'):
                            list_value = value.tolist()
                        else:
                            list_value = list(value)
                        return self._process_data_value(list_value)
                    except Exception as e:
                        logger.warning(f"pandas对象转换失败: {e}，使用截断的字符串转换")
                        str_value = str(value)
                        return self._safe_truncate_string_by_bytes(str_value, 2000)

                # 尝试获取长度
                try:
                    length = len(value)
                except TypeError:
                    # 如果无法获取长度，说明不是序列类型
                    return value

                if length == 0:
                    return None
                elif length == 1:
                    # 单元素数组，提取值
                    try:
                        single_value = value[0] if hasattr(value, '__getitem__') else value
                        return self._process_data_value(single_value)  # 递归处理
                    except (IndexError, TypeError):
                        str_value = str(value)
                        return self._safe_truncate_string_by_bytes(str_value, 2000)

                # 多元素数组，转换为JSON字符串
                try:
                    import json
                    # 转换为列表后序列化
                    if hasattr(value, 'tolist'):
                        json_str = json.dumps(value.tolist(), ensure_ascii=False, separators=(',', ':'))
                    else:
                        json_str = json.dumps(list(value), ensure_ascii=False, separators=(',', ':'))
                    return self._safe_truncate_string_by_bytes(json_str, 2000)
                except Exception as e:
                    logger.warning(f"数组JSON序列化失败: {e}，使用截断的字符串转换")
                    str_value = str(value)
                    return self._safe_truncate_string_by_bytes(str_value, 2000)

            except Exception as e:
                logger.warning(f"处理数组类型数据时出错: {e}，使用截断的字符串转换")
                str_value = str(value)
                return self._safe_truncate_string_by_bytes(str_value, 2000)

        # 处理pandas时间戳
        if isinstance(value, (pd.Timestamp, pd.DatetimeIndex)):
            try:
                return value.to_pydatetime() if hasattr(value, 'to_pydatetime') else value
            except Exception:
                str_value = str(value)
                return self._safe_truncate_string_by_bytes(str_value, 2000)

        # 处理字符串类型的日期
        if isinstance(value, str):
            # 检查是否为无效日期格式
            if self._is_invalid_date_format(value):
                # logger.warning(f"检测到无效日期格式: {value}，使用None替代")
                return None
            # 如果字符串过长，进行截断
            if len(value) > 2000:
                logger.warning(f"字符串过长({len(value)}字符)，截断到2000字符")
                return self._safe_truncate_string_by_bytes(value, 2000)
            # 尝试日期解析
            try:
                date_value = self._process_date_value(value)
                return date_value
            except Exception:
                return value

        # 处理其他类型 - 确保转换为字符串后不会过长
        if value is not None:
            str_value = str(value)
            if len(str_value) > 2000:
                logger.warning(f"值转换为字符串后过长({len(str_value)}字符)，截断到2000字符")
                return self._safe_truncate_string_by_bytes(str_value, 2000)

        return value

    def _is_invalid_date_format(self, date_str: str) -> bool:
        # 无效日期格式模式
        invalid_patterns = [
            r'\d{4}-13-\d{2}',  # 13月
            r'\d{4}-\d{2}-99',  # 99日
            r'\d{4}-99-\d{2}',  # 99月
            r'1601-13-99',  # 特定的无效格式
            r'1602-13-99',
            r'2211-13-99',
            r'2221-99-99'
        ]

        date_str = str(date_str).strip()
        for pattern in invalid_patterns:
            if re.match(pattern, date_str):
                return True

        # 检查月份和日期范围
        date_parts = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str)
        if date_parts:
            year, month, day = map(int, date_parts.groups())
            if month > 12 or month < 1 or day > 31 or day < 1:
                return True

        return False
