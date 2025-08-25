import re
import pandas as pd
import warnings
from datetime import datetime
from typing import Dict, List, Any
from loguru import logger
from utils.monitor import execution_monitor

try:
    if pd.__version__ <= '2.2.0':
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

    @execution_monitor(stage="data_clean", track_memory=True, extra_data={"data_type": "account_structure"})
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

    @execution_monitor(stage="data_clean", track_memory=True, extra_data={"data_type": "subject_dimension"})
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

    @execution_monitor(stage="data_clean", track_memory=True, extra_data={"data_type": "customer_vendor"})
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

    @execution_monitor(stage="data_clean", track_memory=True, extra_data={"data_type": "voucher_list"})
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

    @execution_monitor(stage="data_clean", track_memory=True, extra_data={"data_type": "voucher_detail"})
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

        df['sdocId'] = df['sdocId'].fillna('').astype(str).str.strip()
        df['sacccode'] = df['sacccode'].fillna('').astype(str).str.strip()
        df['bcdtDbt'] = df['bcdtDbt'].fillna('').astype(str).str.strip()
        df['sexcerpta'] = df['sexcerpta'].fillna('').astype(str).str.strip()
        df['soppAcccode'] = df['soppAcccode'].fillna('').astype(str).str.strip()
        df['screditCode'] = df['screditCode'].fillna('').astype(str).str.strip()

        df['idocLineId'] = pd.to_numeric(df['idocLineId'], errors='coerce').fillna(0).astype(int)
        df['ndebit'] = pd.to_numeric(df['ndebit'], errors='coerce').fillna(0)
        df['ncredit'] = pd.to_numeric(df['ncredit'], errors='coerce').fillna(0)
        df['nexchange'] = pd.to_numeric(df['nexchange'], errors='coerce').fillna(0)

        df['createTime'] = pd.to_datetime(df['createTime'], errors='coerce')
        df['updateTime'] = pd.to_datetime(df['updateTime'], errors='coerce')

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

    @execution_monitor(stage="data_clean", track_memory=True)
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
