#!/usr/bin/env python3
"""
回测脚本
任务：定期执行策略回测，验证策略有效性
执行时机：每周日凌晨 2:00
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from datetime import datetime, timedelta
import logging

# 配置日志
# 确保logs目录存在
import os
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backtest.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """主函数：执行回测"""
    logger.info("="*60)
    logger.info("策略回测任务")
    logger.info("="*60)

    try:
        from code.backtest.backtest_engine_v2 import BacktestEngineV2
        import pickle

        logger.info("初始化回测引擎...")
        engine = BacktestEngineV2()

        # 设置回测参数
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)  # 回测1年

        logger.info(f"回测周期: {start_date.date()} 至 {end_date.date()}")

        # 加载数据
        data_file = 'data/akshare_real_data_fixed.pkl'
        if not os.path.exists(data_file):
            logger.error(f"数据文件不存在: {data_file}")
            return 1

        logger.info(f"加载数据: {data_file}")
        with open(data_file, 'rb') as f:
            stock_data = pickle.load(f)

        logger.info(f"数据记录数: {len(stock_data)}")

        # 定义简单的信号生成函数（示例：选择动量最强的10只股票）
        def simple_signal_func(date, data):
            """
            简单的信号生成函数
            选择动量最强的10只股票
            """
            # 获取当前日期的数据
            current_data = data[data['date'] == date] if 'date' in data.columns else data
            
            if len(current_data) == 0:
                return []
            
            # 按动量排序，选择最强的10只
            if 'momentum_20' in current_data.columns:
                selected = current_data.nlargest(10, 'momentum_20')
                return selected['stock_code'].tolist() if 'stock_code' in current_data.columns else []
            else:
                # 如果没有动量列，随机选择10只
                return current_data.sample(min(10, len(current_data)))['stock_code'].tolist() if 'stock_code' in current_data.columns else []

        # 执行回测
        logger.info("\n执行回测...")
        results = engine.run_backtest(stock_data, simple_signal_func, rebalance_freq='monthly')

        if results:
            logger.info("✓ 回测完成")
            logger.info(f"年化收益率: {results.get('annual_return', 0):.2%}")
            logger.info(f"夏普比率: {results.get('sharpe_ratio', 0):.2f}")
            logger.info(f"最大回撤: {results.get('max_drawdown', 0):.2%}")
        else:
            logger.warning("⚠️ 回测无结果")
            results = {}

        # 保存回测报告
        report_date = datetime.now().strftime('%Y%m%d')
        report_path = f'reports/backtest_report_{report_date}.json'
        
        # 确保reports目录存在
        os.makedirs('reports', exist_ok=True)

        # 处理结果中的DataFrame对象，转换为可序列化的格式
        import json
        from pandas import DataFrame
        
        def convert_dataframes(obj):
            """将DataFrame对象转换为字典格式"""
            if isinstance(obj, dict):
                return {k: convert_dataframes(v) for k, v in obj.items()}
            elif isinstance(obj, DataFrame):
                # 将DataFrame转换为字典列表
                return obj.to_dict('records')
            elif isinstance(obj, list):
                return [convert_dataframes(item) for item in obj]
            else:
                return obj
        
        # 转换结果
        serializable_results = convert_dataframes(results)

        with open(report_path, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'backtest_period': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat()
                },
                'results': serializable_results
            }, f, indent=2)

        logger.info(f"\n✓ 回测报告已保存: {report_path}")

        # 如果有策略表现不佳，发出警告
        if results and isinstance(results, dict):
            # 检查是否包含sharpe_ratio键
            if 'sharpe_ratio' in results:
                if results['sharpe_ratio'] < 1.0:
                    logger.warning("⚠️ 策略夏普比率较低，建议优化")
            else:
                # 可能是多策略结果
                for strategy, result in results.items():
                    if isinstance(result, dict) and 'error' not in result and 'sharpe_ratio' in result:
                        if result['sharpe_ratio'] < 1.0:
                            logger.warning(f"⚠️ 策略 {strategy} 夏普比率较低，建议优化")
        elif results and not isinstance(results, dict):
            # 如果results是单策略结果
            if isinstance(results, dict) and 'sharpe_ratio' in results and results['sharpe_ratio'] < 1.0:
                logger.warning("⚠️ 策略夏普比率较低，建议优化")

        logger.info("="*60)
        logger.info("✅ 回测任务完成")
        logger.info("="*60)

        return 0

    except Exception as e:
        logger.error(f"❌ 回测异常: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
