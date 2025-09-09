"""
测试财务报表数据处理功能
"""
import logging
import sys
from datetime import datetime
from utils.data_cleaner import DataCleaner

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


def test_financial_report_cleaning():
    """测试财务报表数据清洗功能"""
    logger.info("开始测试财务报表数据清洗功能")

    # 创建模拟的财务报表数据，按照您提供的格式
    mock_reports_data = [
        ["上海局xxx", None, "", None, None, None, None, None, None, None, None, None, None, None],
        ["收入", 1000000, 2000000, None, "", 0, 3000000, None, None, None, None, None, None, None],
        ["", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["支出", 800000, 1500000, None, "", 0, 2300000, None, None, None, None, None, None, None],
        ["净利润", 200000, 500000, None, "", 0, 700000, None, None, None, None, None, None, None],
    ]

    # 模拟原始记录数据（将表格格式转换为记录格式）
    raw_records = []
    for row_index, row in enumerate(mock_reports_data):
        for col_index, value in enumerate(row):
            record = {
                'row_index': row_index,
                'col_index': col_index,
                'value': value,
                'created_at': datetime.now().isoformat(),
                'data_source': 'financial_report_api',
                'processing_status': 'raw'
            }
            raw_records.append(record)

    logger.info(f"创建了 {len(raw_records)} 条模拟原始记录")

    # 测试数据清洗
    data_cleaner = DataCleaner()

    try:
        cleaned_data = data_cleaner.clean_financial_reports(raw_records)

        if hasattr(cleaned_data, 'to_dict'):
            cleaned_records = cleaned_data.to_dict('records')
        else:
            cleaned_records = cleaned_data

        logger.info(f"数据清洗完成，得到 {len(cleaned_records)} 条清洗后记录")

        # 输出清洗统计
        stats = data_cleaner.cleaning_stats.get('financial_reports', {})
        logger.info(f"清洗统计信息: {stats}")

        # 输出前几条清洗后的数据样例
        logger.info("=" * 80)
        logger.info("清洗后的数据样例 (前5条):")
        for i, record in enumerate(cleaned_records[:5]):
            logger.info(f"记录 {i + 1}: {record}")

        # 分析不同类型的数据
        numeric_count = sum(1 for r in cleaned_records if r.get('is_numeric', False))
        text_count = sum(1 for r in cleaned_records if r.get('is_text', False))
        null_count = sum(1 for r in cleaned_records if r.get('has_null_value', False))
        empty_count = sum(1 for r in cleaned_records if r.get('has_empty_string', False))

        logger.info("=" * 80)
        logger.info("数据类型分析:")
        logger.info(f"数值型数据: {numeric_count} 条")
        logger.info(f"文本型数据: {text_count} 条")
        logger.info(f"空值数据: {null_count} 条")
        logger.info(f"空字符串数据: {empty_count} 条")

        logger.info("=" * 80)
        logger.info("✅ 财务报表数据清洗功能测试成功!")
        return True

    except Exception as e:
        logger.error(f"❌ 财务报表数据清洗测试失败: {e}", exc_info=True)
        return False


def test_data_format_conversion():
    """测试数据格式转换功能"""
    logger.info("开始测试数据格式转换功能")

    # 模拟从API获取的reports_data格式
    reports_data = [
        ["上海局xxx", None, "", None, 100, 200, 300],
        ["北京局yyy", 500, 600, "", None, 700, 800],
        ["", "", "", "", "", "", ""],
        ["合计", 500, 600, "", 100, 900, 1100]
    ]

    logger.info(f"原始报表数据: {len(reports_data)} 行")

    # 转换为标准记录格式
    formatted_data = []
    for row_index, row in enumerate(reports_data):
        if isinstance(row, list):
            for col_index, value in enumerate(row):
                record = {
                    'row_index': row_index,
                    'col_index': col_index,
                    'value': value,
                    'created_at': datetime.now().isoformat(),
                    'data_source': 'financial_report_api',
                    'processing_status': 'raw'
                }
                formatted_data.append(record)

    logger.info(f"转换后记录数: {len(formatted_data)} 条")

    # 输出样例数据
    logger.info("转换后的数据样例 (前10条):")
    for i, record in enumerate(formatted_data[:10]):
        logger.info(f"记录 {i + 1}: {record}")

    logger.info("✅ 数据格式转换测试成功!")
    return True


if __name__ == "__main__":
    logger.info("开始运行财务报表数据处理测试")

    # 测试数据格式转换
    if test_data_format_conversion():
        logger.info("✅ 数据格式转换测试通过")

    # 测试数据清洗
    if test_financial_report_cleaning():
        logger.info("✅ 数据清洗测试通过")

    logger.info("🎉 所有测试完成!")
