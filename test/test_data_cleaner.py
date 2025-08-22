import pandas as pd
import pytest

from utils.data_cleaner import DataCleaner


@pytest.fixture
def cleaner():
    return DataCleaner()


def test_clean_account_structure(cleaner):
    raw_data = [
        {'sacccode': ' 1001 ', 'saccname': '现金 ', 'sacctype': None, 'saccind': None},
        {'sacccode': None, 'saccname': '银行存款'},
    ]
    df = cleaner.clean_account_structure(raw_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['sacccode'] == '1001'
    assert 'cleaned_at' in df.columns
    assert 'data_source' in df.columns
    assert cleaner.cleaning_stats['account_structure']['removed'] == 1


def test_clean_subject_dimension(cleaner):
    raw_data = [
        {'sacccode': '1001', 'sdimensionCode': '01'},
        {'sacccode': '', 'sdimensionCode': None},
    ]
    df = cleaner.clean_subject_dimension(raw_data)
    assert len(df) == 1
    assert df.iloc[0]['sdimensionCode'] == '01'


def test_clean_customer_vendor(cleaner):
    raw_data = [
        {
            'sbpName': '供应商A',
            'screditCode': '123456789012345678',
            'sbptype': None,
            'sshortname': None,
            'sbank': None,
            'saccountCode': None,
            'saccountName': None
        },
        {
            'sbpName': '供应商B',
            'screditCode': 'invalid_code',
            'sbptype': '',
            'sshortname': '',
            'sbank': '',
            'saccountCode': '',
            'saccountName': ''
        }
    ]
    df = cleaner.clean_customer_vendor(raw_data)
    assert len(df) == 2
    assert df.iloc[0]['screditCode'] == '123456789012345678'
    assert df.iloc[1]['screditCode'] == ''  # invalid_code 被清空


def test_clean_voucher_list(cleaner):
    raw_data = [
        {
            'sdocId': '1',
            'sdocNo': 'A001',
            'sdocDate': '2024-01-01',
            'money': '100',
            'isnetbank': None,
            'sdocTypeCode': None,
            'sentriedby': None,
            'excerpta': None
        },
        {
            'sdocId': None,
            'sdocNo': 'A002',
            'sdocDate': '2024-01-02',
            'money': '200',
            'sdocTypeCode': '',
            'sentriedby': '',
            'excerpta': ''
        }
    ]
    df = cleaner.clean_voucher_list(raw_data)
    assert len(df) == 1
    assert pd.api.types.is_datetime64_any_dtype(df['sdocDate'])
    assert pd.api.types.is_numeric_dtype(df['money'])


def test_cleaning_summary(cleaner):
    cleaner.cleaning_stats = {
        'account_structure': {'original': 10, 'cleaned': 8, 'removed': 2},
        'subject_dimension': {'original': 5, 'cleaned': 5, 'removed': 0},
    }
    summary = cleaner.get_cleaning_summary()
    assert summary['summary']['total_original'] == 15
    assert summary['summary']['total_cleaned'] == 13
    assert summary['summary']['cleaning_rate'] == round(13 / 15 * 100, 2)
