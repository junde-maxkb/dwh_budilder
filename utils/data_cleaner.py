import re
from datetime import datetime
from typing import Dict, List, Any

import pandas as pd
from loguru import logger


class DataCleaner:

    def __init__(self):
        self.cleaning_stats = {}

    def clean_account_structure(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df = df.dropna(subset=['sacccode', 'saccname'])
        df['sacccode'] = df['sacccode'].astype(str).str.strip()
        df['saccname'] = df['saccname'].astype(str).str.strip()
        df['sacctype'] = df['sacctype'].fillna('').astype(str)
        df['saccind'] = df['saccind'].fillna('').astype(str)

        df['cleaned_at'] = datetime.now()
        df['data_source'] = 'api_account_structure'

        cleaned_count = len(df)
        self.cleaning_stats['account_structure'] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': original_count - cleaned_count
        }

        logger.info(f"会计科目结构数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条")
        return df

    def clean_subject_dimension(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df = df.dropna(subset=['sacccode', 'sdimensionCode'])
        df['sacccode'] = df['sacccode'].astype(str).str.strip()
        df['sdimensionCode'] = df['sdimensionCode'].astype(str).str.strip()

        df['cleaned_at'] = datetime.now()
        df['data_source'] = 'api_subject_dimension'

        cleaned_count = len(df)
        self.cleaning_stats['subject_dimension'] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': original_count - cleaned_count
        }

        logger.info(f"科目辅助核算关系数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条")
        return df

    def clean_customer_vendor(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df = df.dropna(subset=['sbpName'])
        df['sbpName'] = df['sbpName'].astype(str).str.strip()
        df['screditCode'] = df['screditCode'].fillna('').astype(str).str.strip()
        df['sbptype'] = df['sbptype'].fillna('').astype(str)
        df['sshortname'] = df['sshortname'].fillna('').astype(str).str.strip()
        df['sbank'] = df['sbank'].fillna('').astype(str).str.strip()
        df['saccountCode'] = df['saccountCode'].fillna('').astype(str).str.strip()
        df['saccountName'] = df['saccountName'].fillna('').astype(str).str.strip()

        def validate_credit_code(code):
            if not code:
                return code
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
            'removed': original_count - cleaned_count
        }

        logger.info(f"客商字典数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条")
        return df

    def clean_voucher_list(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df = df.dropna(subset=['sdocId', 'sdocNo'])
        df['sdocId'] = df['sdocId'].astype(str).str.strip()
        df['sdocNo'] = df['sdocNo'].astype(str).str.strip()
        df['sdocTypeCode'] = df['sdocTypeCode'].fillna('').astype(str)
        df['sentriedby'] = df['sentriedby'].fillna('').astype(str).str.strip()
        df['excerpta'] = df['excerpta'].fillna('').astype(str).str.strip()

        df['sdocDate'] = pd.to_datetime(df['sdocDate'], errors='coerce')

        df['money'] = pd.to_numeric(df['money'], errors='coerce').fillna(0)

        df['isnetbank'] = df['isnetbank'].fillna(False).astype(bool)

        df = df.dropna(subset=['sdocDate'])

        df['cleaned_at'] = datetime.now()
        df['data_source'] = 'api_voucher_list'

        cleaned_count = len(df)
        self.cleaning_stats['voucher_list'] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': original_count - cleaned_count
        }

        logger.info(f"凭证目录数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条")
        return df

    def clean_voucher_detail(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df = df.dropna(subset=['sdocId', 'sacccode'])
        df['sdocId'] = df['sdocId'].astype(str).str.strip()
        df['sacccode'] = df['sacccode'].astype(str).str.strip()
        df['idocLineId'] = pd.to_numeric(df['idocLineId'], errors='coerce').fillna(0).astype(int)
        df['bcdtDbt'] = df['bcdtDbt'].fillna('').astype(str)
        df['sexcerpta'] = df['sexcerpta'].fillna('').astype(str).str.strip()
        df['soppAcccode'] = df['soppAcccode'].fillna('').astype(str).str.strip()
        df['screditCode'] = df['screditCode'].fillna('').astype(str).str.strip()

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
            'removed': original_count - cleaned_count
        }

        logger.info(f"凭证明细数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条")
        return df

    def clean_balance_data(self, raw_data: List[Dict[str, Any]], data_type: str = 'balance') -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_count = len(df)

        df = df.dropna(subset=['sacccode'])
        df['sacccode'] = df['sacccode'].astype(str).str.strip()

        if 'saccname' in df.columns:
            df['saccname'] = df['saccname'].fillna('').astype(str).str.strip()

        if 'nopen' in df.columns:
            df['nopen'] = pd.to_numeric(df['nopen'], errors='coerce').fillna(0)
        if 'money' in df.columns:
            df['money'] = pd.to_numeric(df['money'], errors='coerce').fillna(0)

        if 'saccind' in df.columns:
            df['saccind'] = df['saccind'].fillna('').astype(str)

        df['cleaned_at'] = datetime.now()
        df['data_source'] = f'api_{data_type}'

        cleaned_count = len(df)
        self.cleaning_stats[data_type] = {
            'original': original_count,
            'cleaned': cleaned_count,
            'removed': original_count - cleaned_count
        }

        logger.info(f"{data_type}数据清洗完成: 原始{original_count}条, 清洗后{cleaned_count}条")
        return df

    def get_cleaning_summary(self) -> Dict[str, Any]:
        total_original = sum(stats['original'] for stats in self.cleaning_stats.values())
        total_cleaned = sum(stats['cleaned'] for stats in self.cleaning_stats.values())
        total_removed = sum(stats['removed'] for stats in self.cleaning_stats.values())

        return {
            'summary': {
                'total_original': total_original,
                'total_cleaned': total_cleaned,
                'total_removed': total_removed,
                'cleaning_rate': round((total_cleaned / total_original * 100) if total_original > 0 else 0, 2)
            },
            'details': self.cleaning_stats
        }
