import time
import random
from typing import List, Dict, Any
from loguru import logger

# 导入监控装饰器
from utils.monitor import execution_monitor, pipeline_monitor, retry_with_monitor, monitor


class TestDataProcessor:

    def __init__(self):
        self.processed_count = 0

    @execution_monitor(stage="data_fetch", timeout=30, extra_data={"source": "test_api"})
    def fetch_test_data(self, count: int = 10) -> List[Dict[str, Any]]:
        """模拟数据获取过程"""
        logger.info(f"开始获取 {count} 条测试数据")

        # 模拟网络请求时间
        time.sleep(random.uniform(0.5, 2.0))

        # 模拟10%的失败率
        if random.random() < 0.1:
            raise ConnectionError("模拟网络连接失败")

        # 生成测试数据
        test_data = []
        for i in range(count):
            test_data.append({
                "id": i + 1,
                "name": f"测试数据_{i + 1}",
                "value": random.randint(1, 1000),
                "timestamp": time.time(),
                "valid": random.choice([True, False])
            })

        logger.info(f"成功获取 {len(test_data)} 条数据")
        return test_data

    @execution_monitor(stage="data_clean", track_memory=True, extra_data={"operation": "data_validation"})
    def clean_test_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """模拟数据清洗过程"""
        logger.info(f"开始清洗 {len(raw_data)} 条数据")

        # 模拟数据处理时间
        time.sleep(random.uniform(0.3, 1.5))

        # 过滤无效数据
        valid_data = [item for item in raw_data if item.get("valid", False)]

        # 数据标准化
        for item in valid_data:
            item["cleaned"] = True
            item["clean_time"] = time.time()
            # 模拟一些数据转换
            item["normalized_value"] = item["value"] / 1000.0

        logger.info(f"清洗完成，保留 {len(valid_data)} 条有效数据")
        return valid_data

    @execution_monitor(stage="data_store", timeout=20, extra_data={"target": "test_database"})
    @retry_with_monitor(max_retries=2, delay=1.0)
    def store_test_data(self, clean_data: List[Dict[str, Any]], table_name: str = "test_table") -> int:
        """模拟数据存储过程"""
        logger.info(f"开始存储 {len(clean_data)} 条数据到 {table_name}")

        # 模拟数据库写入时间
        time.sleep(random.uniform(0.5, 2.0))

        # 模拟5%的存储失败率
        if random.random() < 0.05:
            raise Exception("模拟数据库写入失败")

        # 模拟成功存储
        self.processed_count += len(clean_data)
        logger.info(f"成功存储 {len(clean_data)} 条数据")
        return len(clean_data)

    @pipeline_monitor("测试数据处理流程")
    def run_test_pipeline(self, data_sources: List[str], batch_size: int = 15):
        """运行完整的测试数据处理流水线"""
        total_processed = 0

        for source in data_sources:
            try:
                logger.info(f"处理数据源: {source}")

                # 1. 数据获取
                raw_data = self.fetch_test_data(batch_size)

                # 2. 数据清洗
                clean_data = self.clean_test_data(raw_data)

                # 3. 数据存储
                stored_count = self.store_test_data(clean_data, f"table_{source}")

                total_processed += stored_count

            except Exception as e:
                logger.error(f"处理数据源 {source} 时发生错误: {str(e)}")
                # 继续处理其他数据源
                continue

        logger.info(f"流水线处理完成，总共处理了 {total_processed} 条数据")
        return total_processed


def test_basic_monitoring():
    """测试基本监控功能"""
    print("\n" + "="*60)
    print("🔍 测试基本监控功能")
    print("="*60)

    processor = TestDataProcessor()

    try:
        # 执行一些基本操作
        data = processor.fetch_test_data(5)
        cleaned = processor.clean_test_data(data)
        stored = processor.store_test_data(cleaned)

        print(f"✅ 基本操作完成: 存储了 {stored} 条数据")

    except Exception as e:
        print(f"❌ 基本操作失败: {str(e)}")

    # 显示监控统计
    stats = monitor.get_statistics()
    print(f"\n📊 当前监控统计:")
    print(f"   总执行次数: {stats['total_executions']}")
    print(f"   成功次数: {stats['successful_executions']}")
    print(f"   失败次数: {stats['failed_executions']}")
    print(f"   成功率: {stats['success_rate']:.1f}%")
    print(f"   平均执行时间: {stats['average_duration']:.2f}秒")


def test_pipeline_monitoring():
    """测试整体流程监控功能"""
    print("\n" + "="*60)
    print("🔄 测试流水线监控功能")
    print("="*60)

    processor = TestDataProcessor()
    data_sources = ["source_A", "source_B", "source_C"]

    try:
        total_processed = processor.run_test_pipeline(data_sources, batch_size=8)
        print(f"✅ 流水线执行完成: 总共处理 {total_processed} 条数据")

    except Exception as e:
        print(f"❌ 流水线执行失败: {str(e)}")


def test_error_handling():
    """测试错误处理和重试机制"""
    print("\n" + "="*60)
    print("⚠️  测试错误处理和重试机制")
    print("="*60)

    processor = TestDataProcessor()

    # 执行多次操作以触发一些错误
    for i in range(5):
        try:
            print(f"🔄 执行第 {i+1} 次测试...")
            data = processor.fetch_test_data(3)
            cleaned = processor.clean_test_data(data)
            stored = processor.store_test_data(cleaned)
            print(f"   ✅ 成功处理 {stored} 条数据")

        except Exception as e:
            print(f"   ❌ 操作失败: {str(e)}")

        time.sleep(0.5)  # 短暂延迟


def show_monitoring_results():
    """显示监控结果"""
    print("\n" + "="*60)
    print("📈 监控结果汇总")
    print("="*60)

    # 获取统计信息
    stats = monitor.get_statistics()
    print(f"📊 执行统计:")
    print(f"   总执行次数: {stats['total_executions']}")
    print(f"   成功次数: {stats['successful_executions']}")
    print(f"   失败次数: {stats['failed_executions']}")
    print(f"   成功率: {stats['success_rate']:.1f}%")
    print(f"   总耗时: {stats['total_duration']:.2f}秒")
    print(f"   平均执行时间: {stats['average_duration']:.2f}秒")

    # 获取执行历史
    history = monitor.get_execution_history(limit=10)
    print(f"\n📝 最近 {len(history)} 次执行记录:")

    for i, record in enumerate(history[:5], 1):  # 只显示前5条
        status_emoji = "✅" if record['status'] == 'success' else "❌"
        print(f"   {i}. {status_emoji} {record['function_name']} - "
              f"{record['duration']:.2f}s - {record['status']}")

    # 显示当前运行的任务
    current_running = len(monitor.current_executions)
    print(f"\n🏃 当前运行任务数: {current_running}")


def main():
    """主测试函数"""
    print("🚀 开始监控功能测试Demo")
    print("这个测试将演示监控装饰器的各种功能")

    # 配置日志输出
    logger.remove()  # 移除默认处理器
    logger.add(lambda msg: print(f"[LOG] {msg}", end=""),
               format="{time:HH:mm:ss} | {level} | {message}",
               level="INFO")

    try:
        # 1. 测试基本监控功能
        test_basic_monitoring()

        # 2. 测试流水线监控
        test_pipeline_monitoring()

        # 3. 测试错误处理
        test_error_handling()

        # 4. 显示监控结果
        show_monitoring_results()

        print("\n" + "="*60)
        print("🎉 监控测试Demo完成!")
        print("="*60)
        print("\n💡 监控功能验证:")
        print("   ✅ 执行时间监控")
        print("   ✅ 成功/失败状态追踪")
        print("   ✅ 错误信息记录")
        print("   ✅ 重试机制")
        print("   ✅ 流水线监控")
        print("   ✅ 统计信息生成")

    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
