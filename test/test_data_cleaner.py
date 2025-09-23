import pandas as pd
import pytest

import numpy as np
from utils.data_cleaner import DataCleaner


@pytest.fixture
def cleaner():
    return DataCleaner()


class TestDataCleaner:

    def test_clean_account_structure(self, cleaner):
        """测试会计科目结构数据清洗"""
        raw_data = [
            {'sacccode': ' 1001 ', 'saccname': '现金 ', 'sacctype': None, 'saccind': ''},
            {'sacccode': '1002', 'saccname': None, 'sacctype': '资产', 'saccind': None},
            {'sacccode': None, 'saccname': '银行存款', 'sacctype': None, 'saccind': None}
        ]

        df = cleaner.clean_account_structure(raw_data)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

        assert df.iloc[0]['sacccode'] == '1001'
        assert df.iloc[0]['saccname'] == '现金'
        assert df.iloc[0]['sacctype'] == ''
        assert df.iloc[1]['saccname'] == ''
        assert df.iloc[2]['sacccode'] == ''

        assert 'cleaned_at' in df.columns
        assert 'data_source' in df.columns
        assert df.iloc[0]['data_source'] == 'api_account_structure'

        stats = cleaner.cleaning_stats['account_structure']
        assert stats['original'] == 3
        assert stats['cleaned'] == 3
        assert stats['removed'] == 0

    def test_clean_subject_dimension(self, cleaner):
        """测试科目辅助核算关系数据清洗"""
        raw_data = [
            {'sacccode': '1001', 'sdimensionCode': 'DIM001'},
            {'sacccode': None, 'sdimensionCode': ' DIM002 '},
            {'sacccode': '1003', 'sdimensionCode': None}
        ]

        df = cleaner.clean_subject_dimension(raw_data)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert df.iloc[1]['sacccode'] == ''
        assert df.iloc[1]['sdimensionCode'] == 'DIM002'
        assert df.iloc[2]['sdimensionCode'] == ''

        stats = cleaner.cleaning_stats['subject_dimension']
        assert stats['removed'] == 0

    def test_clean_customer_vendor(self, cleaner):
        """测试客商字典数据清洗"""
        raw_data = [
            {
                'sbpName': '测试公司A',
                'screditCode': '91110000000000000X',
                'sbptype': None,
                'sshortname': '公司A',
                'sbank': '工商银行',
                'saccountCode': '123456789',
                'saccountName': '账户A'
            },
            {
                'sbpName': None,
                'screditCode': 'invalid_code',
                'sbptype': '客户',
                'sshortname': None,
                'sbank': '',
                'saccountCode': None,
                'saccountName': '账户B'
            }
        ]

        df = cleaner.clean_customer_vendor(raw_data)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

        assert df.iloc[0]['sbpName'] == '测试公司A'
        assert df.iloc[0]['screditCode'] == '91110000000000000X'
        assert df.iloc[0]['sbptype'] == ''
        assert df.iloc[0]['saccountName'] == '账户A'

        assert df.iloc[1]['sbpName'] == ''
        assert df.iloc[1]['screditCode'] == ''
        assert df.iloc[1]['sshortname'] == ''

        stats = cleaner.cleaning_stats['customer_vendor']
        assert stats['removed'] == 0

    def test_clean_voucher_list(self, cleaner):
        """测试凭证目录数据清洗"""
        raw_data = [
            {
                'sdocId': 'DOC001',
                'sdocNo': 'V001',
                'sdocTypeCode': None,
                'sentriedby': '张三',
                'excerpta': '测试凭证',
                'sdocDate': '2024-01-01',
                'money': 1000.50,
                'isnetbank': None
            },
            {
                'sdocId': None,
                'sdocNo': None,
                'sdocTypeCode': 'TYPE001',
                'sentriedby': '李四',
                'excerpta': None,
                'sdocDate': 'invalid_date',
                'money': 'invalid_amount',
                'isnetbank': True
            }
        ]

        df = cleaner.clean_voucher_list(raw_data)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

        assert df.iloc[0]['sdocId'] == 'DOC001'
        assert df.iloc[0]['sdocTypeCode'] == ''
        assert df.iloc[0]['excerpta'] == '测试凭证'
        assert df.iloc[0]['money'] == 1000.50
        assert df.iloc[0]['isnetbank'] == False

        assert df.iloc[1]['sdocId'] == ''
        assert df.iloc[1]['sdocNo'] == ''
        assert pd.isna(df.iloc[1]['sdocDate'])
        assert df.iloc[1]['money'] == 0

        stats = cleaner.cleaning_stats['voucher_list']
        assert stats['removed'] == 0

    def test_clean_voucher_detail(self, cleaner):
        """测试凭证明细数据清洗"""
        raw_data = [
            {
                'sdocId': 'DOC001',
                'sacccode': '1001',
                'bcdtDbt': '借',
                'sexcerpta': '现金收入',
                'soppAcccode': None,
                'screditCode': '91110000000000000X',
                'idocLineId': 1,
                'ndebit': 1000.00,
                'ncredit': None,
                'nexchange': 1.0,
                'createTime': '2024-01-01 10:00:00',
                'updateTime': '2024-01-01 11:00:00'
            },
            {
                'sdocId': None,
                'sacccode': None,
                'bcdtDbt': '贷',
                'sexcerpta': None,
                'soppAcccode': '1002',
                'screditCode': '',
                'idocLineId': 'invalid_id',
                'ndebit': 'invalid_amount',
                'ncredit': 0,
                'nexchange': None,
                'createTime': 'invalid_time',
                'updateTime': None
            }
        ]

        df = cleaner.clean_voucher_detail(raw_data)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

        # 字段清洗验证
        assert df.iloc[0]['sacccode'] == '1001'
        assert df.iloc[0]['sexcerpta'] == '现金收入'
        assert df.iloc[0]['soppAcccode'] == ''
        assert df.iloc[0]['ndebit'] == 1000.00
        assert df.iloc[0]['ncredit'] == 0

        assert df.iloc[1]['sdocId'] == ''
        assert df.iloc[1]['sacccode'] == ''
        assert df.iloc[1]['idocLineId'] == 0
        assert df.iloc[1]['ndebit'] == 0
        assert pd.isna(df.iloc[1]['createTime'])

        stats = cleaner.cleaning_stats['voucher_detail']
        assert stats['removed'] == 0

    def test_clean_balance_data(self, cleaner):
        """测试余额数据清洗"""
        raw_data = [
            {
                'sacccode': '1001',
                'saccname': '现金',
                'nopen': 1000.00,
                'money': None,
                'saccind': '借'
            },
            {
                'sacccode': None,
                'saccname': None,
                'nopen': 'invalid_amount',
                'money': 500.00,
                'saccind': None
            }
        ]

        df = cleaner.clean_balance_data(raw_data, 'test_balance')

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

        assert df.iloc[0]['sacccode'] == '1001'
        assert df.iloc[0]['nopen'] == 1000.00
        assert df.iloc[0]['money'] == 0

        assert df.iloc[1]['sacccode'] == ''
        assert df.iloc[1]['saccname'] == ''
        assert df.iloc[1]['nopen'] == 0
        assert df.iloc[1]['saccind'] == ''

        assert df.iloc[0]['data_source'] == 'api_test_balance'

        stats = cleaner.cleaning_stats['test_balance']
        assert stats['removed'] == 0

    def test_empty_data_handling(self, cleaner):
        df = cleaner.clean_account_structure([])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

        df = cleaner.clean_customer_vendor(None)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_credit_code_validation(self, cleaner):
        raw_data = [
            {'sbpName': '公司A', 'screditCode': '91110000000000000X', 'sbptype': None,
             'sshortname': None, 'sbank': None, 'saccountCode': None, 'saccountName': None},
            {'sbpName': '公司B', 'screditCode': 'invalid123', 'sbptype': None,
             'sshortname': None, 'sbank': None, 'saccountCode': None, 'saccountName': None},
            {'sbpName': '公司C', 'screditCode': '123', 'sbptype': None,
             'sshortname': None, 'sbank': None, 'saccountCode': None, 'saccountName': None},
            {'sbpName': '公司D', 'screditCode': '', 'sbptype': None,
             'sshortname': None, 'sbank': None, 'saccountCode': None, 'saccountName': None}
        ]

        df = cleaner.clean_customer_vendor(raw_data)

        assert df.iloc[0]['screditCode'] == '91110000000000000X'
        assert df.iloc[1]['screditCode'] == ''
        assert df.iloc[2]['screditCode'] == ''
        assert df.iloc[3]['screditCode'] == ''

    def test_data_types_conversion(self, cleaner):
        voucher_data = [
            {
                'sdocId': 123,
                'sdocNo': 456,
                'sdocTypeCode': None,
                'sentriedby': '',
                'excerpta': None,
                'sdocDate': '2024-01-01',
                'money': 1000,
                'isnetbank': 1
            }
        ]

        df = cleaner.clean_voucher_list(voucher_data)

        assert isinstance(df.iloc[0]['sdocId'], str)
        assert isinstance(df.iloc[0]['money'], (int, float, np.integer, np.floating))
        assert isinstance(df.iloc[0]['isnetbank'], (bool, np.bool_))
        assert isinstance(df.iloc[0]['sdocDate'], pd.Timestamp)

    def test_monitor_integration(self, cleaner):
        raw_data = [{'sacccode': '1001', 'saccname': '现金', 'sacctype': None, 'saccind': None}]
        df = cleaner.clean_account_structure(raw_data)

        assert len(df) == 1
        assert df.iloc[0]['sacccode'] == '1001'

        assert 'account_structure' in cleaner.cleaning_stats

    def test_large_dataset_handling(self, cleaner):
        raw_data = []
        for i in range(1000):
            raw_data.append({
                'sacccode': f'100{i:04d}',
                'saccname': f'科目{i}',
                'sacctype': '资产' if i % 2 == 0 else None,
                'saccind': '借' if i % 3 == 0 else None
            })

        df = cleaner.clean_account_structure(raw_data)

        assert len(df) == 1000
        assert cleaner.cleaning_stats['account_structure']['removed'] == 0

    def test_special_characters_handling(self, cleaner):
        raw_data = [
            {
                'sbpName': '  测试公司\n\t  ',
                'screditCode': ' 91110000000000000X ',
                'sbptype': '\r\n客户\t',
                'sshortname': None,
                'sbank': '',
                'saccountCode': '  \n  ',
                'saccountName': '账户A'
            }
        ]

        df = cleaner.clean_customer_vendor(raw_data)

        assert df.iloc[0]['sbpName'] == '测试公司'
        assert df.iloc[0]['screditCode'] == '91110000000000000X'
        assert df.iloc[0]['sbptype'] == '客户'
        assert df.iloc[0]['saccountCode'] == ''

    def test_clean_voucher_dim_detail_missing_sacccode(self, cleaner):
        """测试凭证辅助维度明细在缺少 sacccode 字段时的容错处理"""
        raw_data = [
            {
                'sDocId': 'DOC1001',
                'dimCode': 'KEHU',
                'dimName': '客户',
                'dimValue': 'C001',
                'dimValueName': '客户A',
                'idocLineId': '1'
            },
            {
                'SDOCID': 'DOC1002',
                'DIMENSIONCODE': 'XM',
                'DIMENSIONNAME': '项目',
                'DIMENSIONVALUE': 'P002',
                'DIMENSIONVALUENAME': '项目B',
                'idocLineId': 'invalid'
            }
        ]

        df = cleaner.clean_voucher_dim_detail(raw_data)

        assert isinstance(df, pd.DataFrame)
        assert 'sacccode' in df.columns  # 自动补充
        assert df.iloc[0]['sacccode'] == ''
        assert df.iloc[0]['dimensionCode'] == 'KEHU'
        assert df.iloc[1]['dimensionCode'] == 'XM'
        assert df.iloc[0]['idocLineId'] == 1
        assert df.iloc[1]['idocLineId'] == 0  # 转换失败置0
        assert df.iloc[0]['data_source'] == 'api_voucher_dim_detail'
        stats = cleaner.cleaning_stats['voucher_dim_detail']
        assert stats['original'] == 2
        assert stats['removed'] == 0

