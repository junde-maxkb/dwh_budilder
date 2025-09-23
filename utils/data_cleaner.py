import re
import pandas as pd
import warnings
from datetime import datetime
from typing import Dict, List, Any
from loguru import logger

try:
    if pd.__version__ < '2.2.0':
        warnings.warn("pandas 2.2.0 及以下版本存在向下转换问题，建议升级到2.3.0及以上版本以避免潜在问题。", UserWarning)
    pd.set_option('future.no_silent_downcasting', True)
except Exception as e:
    logger.warning(f"检查pandas版本时出错: {str(e)}")


class DataCleaner:
    """
    数据清洗器类 - 负责清洗从API获取的财务数据

    主要功能：
    1. 清洗会计科目结构数据
    2. 清洗科目辅助核算关系数据
    3. 清洗客商字典数据
    4. 清洗凭证相关数据
    5. 清洗余额数据
    6. 提供清洗统计信息
    """

    def __init__(self):
        self.cleaning_stats = {}

    def _ensure_column(self, df: pd.DataFrame, target: str, alternatives: List[str] = None, default: Any = ''):
        """确保 DataFrame 中存在指定列，并合并所有候选列数据到目标列。

        步骤：
        1. 若目标列不存在，创建或用第一个找到的候选列重命名。
        2. 遍历所有候选列，将其非空值填充到目标列的空/空字符串/NaN 位置。
        3. 合并后删除候选列（保留目标列）。
        """
        alts = alternatives or []
        lower_map = {c.lower(): c for c in df.columns}

        # 确保目标列存在
        if target not in df.columns:
            renamed = False
            for alt in alts:
                # 精确匹配
                if alt in df.columns:
                    df.rename(columns={alt: target}, inplace=True)
                    renamed = True
                    break
                # 大小写不一致匹配
                if alt.lower() in lower_map:
                    df.rename(columns={lower_map[alt.lower()]: target}, inplace=True)
                    renamed = True
                    break
            if not renamed:
                # 尝试直接匹配目标列大小写变体
                if target.lower() in lower_map:
                    df.rename(columns={lower_map[target.lower()]: target}, inplace=True)
                else:
                    df[target] = default

        # 现在目标列已存在，开始合并剩余候选列
        mask_missing = df[target].isna() | (df[target].astype(str).str.strip() == '')
        for alt in alts:
            # 找出真实存在的候选列（可能是大小写变体）
            real_alt = None
            if alt in df.columns and alt != target:
                real_alt = alt
            elif alt.lower() in lower_map and lower_map[alt.lower()] != target:
                real_alt = lower_map[alt.lower()]
            if real_alt and real_alt in df.columns:
                try:
                    # 仅填充当前仍为空的位置
                    fill_mask = mask_missing & ~(df[real_alt].isna() | (df[real_alt].astype(str).str.strip() == ''))
                    if fill_mask.any():
                        df.loc[fill_mask, target] = df.loc[fill_mask, real_alt]
                        # 更新剩余缺失掩码
                        mask_missing = df[target].isna() | (df[target].astype(str).str.strip() == '')
                except Exception as e:
                    print(f"合并列 {real_alt} 到 {target} 时出错: {str(e)}")
                # 删除已合并列
                try:
                    if real_alt in df.columns and real_alt != target:
                        df.drop(columns=[real_alt], inplace=True)
                except Exception as e:
                    print(f"删除列 {real_alt} 时出错: {str(e)}")

        # 最终保证不存在的值填默认
        if default != '' and (df[target].isna()).any():
            df[target] = df[target].fillna(default)

    def clean_account_structure(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        清洗会计科目结构数据

        Args:
            raw_data: 从API获取的原始会计科目数据列表

        Returns:
            pd.DataFrame: 清洗后的会计科目结构数据
        """
        if not raw_data:
            return pd.DataFrame()

        # 将原始数据转换为DataFrame
        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df['sacccode'] = df['sacccode'].fillna('').astype(str).str.strip()
        df['saccname'] = df['saccname'].fillna('').astype(str).str.strip()
        df['sacctype'] = df['sacctype'].fillna('').astype(str).str.strip()
        df['saccind'] = df['saccind'].fillna('').astype(str).str.strip()

        df['cleaned_at'] = datetime.now()
        df['data_source'] = 'api_account_structure'

        cleaned_count = len(df)
        self.cleaning_stats['account_structure'] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': 0
        }

        logger.info(f"会计科目结构数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条, 移除0条")
        return df

    def clean_subject_dimension(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        清洗科目辅助核算关系数据

        Args:
            raw_data: 从API获取的原始科目辅助核算关系数据

        Returns:
            pd.DataFrame: 清洗后的科目辅助核算关系数据
        """
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df['sacccode'] = df['sacccode'].fillna('').astype(str).str.strip()
        df['sdimensionCode'] = df['sdimensionCode'].fillna('').astype(str).str.strip()

        df['cleaned_at'] = datetime.now()
        df['data_source'] = 'api_subject_dimension'

        cleaned_count = len(df)
        self.cleaning_stats['subject_dimension'] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': 0
        }

        logger.info(f"科目辅助核算关系数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条, 移除0条")
        return df

    def clean_customer_vendor(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        清洗客商字典数据

        Args:
            raw_data: 从API获取的原始客商数据

        Returns:
            pd.DataFrame: 清洗后的客商字典数据
        """
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df['sbpName'] = df['sbpName'].fillna('').astype(str).str.strip()
        df['screditCode'] = df['screditCode'].fillna('').astype(str).str.strip()
        df['sbptype'] = df['sbptype'].fillna('').astype(str).str.strip()
        df['sshortname'] = df['sshortname'].fillna('').astype(str).str.strip()
        df['sbank'] = df['sbank'].fillna('').astype(str).str.strip()
        df['saccountCode'] = df['saccountCode'].fillna('').astype(str).str.strip()
        df['saccountName'] = df['saccountName'].fillna('').astype(str).str.strip()

        def validate_credit_code(code):
            """
            验证统一社会信用代码格式

            Args:
                code: 信用代码字符串

            Returns:
                str: 验证通过返回原代码，否则返回空字符串
            """
            if not code:
                return ''
            if re.match(r'^[0-9A-Z]{18}$', code):
                return code
            return ''

        df['screditCode'] = df['screditCode'].apply(validate_credit_code)

        df['cleaned_at'] = datetime.now()
        df['data_source'] = 'api_customer_vendor'

        cleaned_count = len(df)
        self.cleaning_stats['customer_vendor'] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': 0
        }

        logger.info(f"客商字典数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条, 移除0条")
        return df

    def clean_voucher_list(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        清洗凭证目录数据

        Args:
            raw_data: 从API获取的原始凭证目录数据

        Returns:
            pd.DataFrame: 清洗后的凭证目录数据
        """
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df['sdocId'] = df['sdocId'].fillna('').astype(str).str.strip()
        df['sdocNo'] = df['sdocNo'].fillna('').astype(str).str.strip()
        df['sdocTypeCode'] = df['sdocTypeCode'].fillna('').astype(str).str.strip()
        df['sentriedby'] = df['sentriedby'].fillna('').astype(str).str.strip()
        df['excerpta'] = df['excerpta'].fillna('').astype(str).str.strip()

        df['sdocDate'] = pd.to_datetime(df['sdocDate'], errors='coerce')

        df['money'] = pd.to_numeric(df['money'], errors='coerce').fillna(0)

        df['isnetbank'] = df['isnetbank'].fillna(False).astype(bool)

        df['cleaned_at'] = datetime.now()
        df['data_source'] = 'api_voucher_list'

        cleaned_count = len(df)
        self.cleaning_stats['voucher_list'] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': 0
        }

        logger.info(f"凭证目录数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条, 移除0条")
        return df

    def clean_voucher_detail(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        清洗凭证明细数据

        Args:
            raw_data: 从API获取的原始凭证明细数据

        Returns:
            pd.DataFrame: 清洗后的凭证明细数据
        """
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        for col, alts in [
            ('sdocId', ['sDocId', 'SDOCID']),
            ('sacccode', ['saccCode', 'SACCCODE', 'accCode']),
            ('bcdtDbt', ['bcdt_dbt', 'BCDTDBT']),
            ('sexcerpta', ['excerpta', 'SEXCERPTA']),
            ('soppAcccode', ['soppacccode', 'SOPPACCCODE']),
            ('screditCode', ['screditcode', 'SCREDITCODE'])
        ]:
            self._ensure_column(df, col, alts, default='')

        df['sdocId'] = df['sdocId'].fillna('').astype(str).str.strip()
        df['sacccode'] = df['sacccode'].fillna('').astype(str).str.strip()
        df['bcdtDbt'] = df['bcdtDbt'].fillna('').astype(str).str.strip()
        df['sexcerpta'] = df['sexcerpta'].fillna('').astype(str).str.strip()
        df['soppAcccode'] = df['soppAcccode'].fillna('').astype(str).str.strip()
        df['screditCode'] = df['screditCode'].fillna('').astype(str).str.strip()

        # 数值与时间字段容错
        for num_col in ['idocLineId', 'ndebit', 'ncredit', 'nexchange']:
            if num_col not in df.columns:
                df[num_col] = 0
        df['idocLineId'] = pd.to_numeric(df['idocLineId'], errors='coerce').fillna(0).astype(int)
        df['ndebit'] = pd.to_numeric(df['ndebit'], errors='coerce').fillna(0)
        df['ncredit'] = pd.to_numeric(df['ncredit'], errors='coerce').fillna(0)
        df['nexchange'] = pd.to_numeric(df['nexchange'], errors='coerce').fillna(0)

        for dt_col in ['createTime', 'updateTime']:
            if dt_col not in df.columns:
                df[dt_col] = pd.NaT
            else:
                df[dt_col] = pd.to_datetime(df[dt_col], errors='coerce')

        df['cleaned_at'] = datetime.now()
        df['data_source'] = 'api_voucher_detail'

        cleaned_count = len(df)
        self.cleaning_stats['voucher_detail'] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': 0
        }

        logger.info(f"凭证明细数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条, 移除0条")
        return df

    def clean_voucher_dim_detail(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """清洗凭证辅助维度明细数据，结构可能与凭证明细不同，需更高容错。"""
        if not raw_data:
            return pd.DataFrame()
        df = pd.DataFrame(raw_data)
        original_count = len(df)

        # 确保关键列存在
        self._ensure_column(df, 'sdocId', ['sDocId', 'SDOCID'], '')
        # sacccode 可能不存在（维度明细可能只有维度编码），因此安全处理
        self._ensure_column(df, 'sacccode', ['saccCode', 'SACCCODE', 'accCode'], '')
        # 常见凭证维度字段（根据经验推测）
        for col, alts in [
            ('dimensionCode', ['dimCode', 'DIMENSIONCODE', 'dim_code']),
            ('dimensionName', ['dimName', 'DIMENSIONNAME', 'dimension']),
            ('dimensionValue', ['dimValue', 'DIMENSIONVALUE', 'dim_value']),
            ('dimensionValueName', ['dimValueName', 'DIMENSIONVALUENAME', 'dim_value_name'])
        ]:
            self._ensure_column(df, col, alts, '')

        # 基础清洗
        text_cols = [c for c in
                     ['sdocId', 'sacccode', 'dimensionCode', 'dimensionName', 'dimensionValue', 'dimensionValueName'] if
                     c in df.columns]
        for c in text_cols:
            df[c] = df[c].fillna('').astype(str).str.strip()

        # 如果存在金额或行号等字段，进行数值转换
        for num_col in ['idocLineId', 'ndebit', 'ncredit', 'nexchange']:
            if num_col in df.columns:
                df[num_col] = pd.to_numeric(df[num_col], errors='coerce').fillna(0)
        if 'idocLineId' in df.columns:
            try:
                df['idocLineId'] = df['idocLineId'].astype(int)
            except Exception as e:
                logger.error("将 idocLineId 转换为整数时出错，尝试填充缺失值后转换。错误信息: " + str(e))
                df['idocLineId'] = df['idocLineId'].fillna(0).astype(int)

        for dt_col in ['createTime', 'updateTime']:
            if dt_col in df.columns:
                df[dt_col] = pd.to_datetime(df[dt_col], errors='coerce')

        df['cleaned_at'] = datetime.now()
        df['data_source'] = 'api_voucher_dim_detail'

        cleaned_count = len(df)
        self.cleaning_stats['voucher_dim_detail'] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': 0
        }
        logger.info(f"凭证辅助维度明细数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条, 移除0条")
        return df

    def clean_balance_data(self, raw_data: List[Dict[str, Any]], data_type: str = 'balance') -> pd.DataFrame:
        """
        清洗余额数据（通用方法，适用于科目余额和辅助余额）

        Args:
            raw_data: 从API获取的原始余额数据
            data_type: 数据类型标识（如：'balance', 'aux_balance'）

        Returns:
            pd.DataFrame: 清洗后的余额数据
        """
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df['sacccode'] = df['sacccode'].fillna('').astype(str).str.strip()

        if 'saccname' in df.columns:
            df['saccname'] = df['saccname'].fillna('').astype(str).str.strip()

        if 'nopen' in df.columns:
            df['nopen'] = pd.to_numeric(df['nopen'], errors='coerce').fillna(0)
        if 'money' in df.columns:
            df['money'] = pd.to_numeric(df['money'], errors='coerce').fillna(0)

        if 'saccind' in df.columns:
            df['saccind'] = df['saccind'].fillna('').astype(str).str.strip()

        df['cleaned_at'] = datetime.now()
        df['data_source'] = f'api_{data_type}'

        cleaned_count = len(df)
        self.cleaning_stats[data_type] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': 0
        }

        logger.info(f"{data_type}数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条, 移除0条")
        return df

    def clean_financial_reports(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        清洗财务报表数据
        """
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df['row_index'] = pd.to_numeric(df['row_index'], errors='coerce').fillna(0).astype(int)
        df['col_index'] = pd.to_numeric(df['col_index'], errors='coerce').fillna(0).astype(int)

        df['original_value'] = df['value']  # 保留原始值用于追溯
        df['cleaned_value'] = df['value'].apply(self._clean_report_value)
        df['value_type'] = df['value'].apply(self._classify_value_type)

        df['is_valid'] = df['cleaned_value'].notna() & (df['cleaned_value'] != '')

        df['has_null_value'] = df['original_value'].isna()
        df['has_empty_string'] = (df['original_value'] == '')
        df['is_numeric'] = df['value_type'] == 'numeric'
        df['is_text'] = df['value_type'] == 'text'

        df['cleaned_at'] = datetime.now()
        df['data_source'] = 'financial_report_api'

        cleaned_count = len(df)
        removed_count = 0

        self.cleaning_stats['financial_reports'] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': removed_count,
            'valid_records': df['is_valid'].sum(),
            'null_values': df['has_null_value'].sum(),
            'empty_strings': df['has_empty_string'].sum(),
            'numeric_values': df['is_numeric'].sum(),
            'text_values': df['is_text'].sum()
        }

        logger.info(f"财务报表数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条, "
                    f"有效数据{df['is_valid'].sum()}条, 数值型{df['is_numeric'].sum()}条, "
                    f"文本型{df['is_text'].sum()}条")

        return df

    def clean_single_report_value(self, value: Any) -> Any:
        """公开接口：清洗报表单元格值。包装内部 _clean_report_value。"""
        return self._clean_report_value(value)

    def classify_report_value(self, value: Any) -> str:
        """公开接口：分类报表单元格值类型。包装内部 _classify_value_type。"""
        return self._classify_value_type(value)

    def _clean_report_value(self, value) -> Any:
        """
        清洗报表中的单个值
        """
        if value is None:
            return None

        str_value = str(value).strip()

        if str_value == '' or str_value.lower() == 'none':
            return ''

        try:
            cleaned_str = re.sub(r'[,，\s]', '', str_value)  # 移除逗号和空格

            if re.match(r'^-?\d+\.?\d*$', cleaned_str):
                if '.' in cleaned_str:
                    return float(cleaned_str)
                else:
                    return int(cleaned_str)
        except (ValueError, TypeError):
            pass

        return str_value

    def _classify_value_type(self, value) -> str:
        """
        分类值的类型
        """
        if value is None:
            return 'null'

        str_value = str(value).strip()
        if str_value == '':
            return 'empty'

        try:
            cleaned_str = re.sub(r'[,，\s]', '', str_value)
            if re.match(r'^-?\d+\.?\d*$', cleaned_str):
                float(cleaned_str)
                return 'numeric'
        except (ValueError, TypeError):
            pass

        return 'text'
