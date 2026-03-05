#!/usr/bin/env python3
"""
回测系统架构优化
支持多策略对比、多因子评估、参数敏感性分析
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from datetime import datetime, timedelta
import logging
import pickle
import pandas as pd
import numpy as np
import json
from concurrent.futures import ProcessPoolExecutor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backtest_system.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BacktestSystem:
    """回测系统架构"""
    
    def __init__(self, data_file='data/akshare_real_data_fixed.pkl'):
        self.data_file = data_file
        self.data = None
        self.strategies = {}
        self.results = {}
    
    def load_data(self):
        """加载数据"""
        if not os.path.exists(self.data_file):
            logger.error(f"数据文件不存在: {self.data_file}")
            return False
        
        with open(self.data_file, 'rb') as f:
            self.data = pickle.load(f)
        
        logger.info(f"✓ 加载数据: {len(self.data)} 条记录")
        logger.info(f"✓ 股票数量: {self.data['stock_code'].nunique()}")
        return True
    
    def register_strategy(self, name, signal_func, params=None):
        """注册策略"""
        self.strategies[name] = {
            'signal_func': signal_func,
            'params': params or {}
        }
        logger.info(f"✓ 注册策略: {name}")
    
    def run_strategy(self, strategy_name):
        """运行单个策略"""
        if strategy_name not in self.strategies:
            logger.error(f"策略 {strategy_name} 不存在")
            return {}
        
        from code.backtest.backtest_engine_v2 import BacktestEngineV2
        
        strategy = self.strategies[strategy_name]
        engine = BacktestEngineV2()
        
        try:
            result = engine.run_backtest(
                self.data, 
                strategy['signal_func'],
                **strategy['params']
            )
            return result
        except Exception as e:
            logger.error(f"策略 {strategy_name} 运行失败: {e}")
            return {}
    
    def run_all_strategies(self):
        """运行所有策略（并行）"""
        logger.info("\n运行所有策略")
        
        with ProcessPoolExecutor(max_workers=min(4, len(self.strategies))) as executor:
            futures = {executor.submit(self.run_strategy, name): name for name in self.strategies}
            
            for future in futures:
                strategy_name = futures[future]
                try:
                    result = future.result()
                    if result:
                        self.results[strategy_name] = result
                        logger.info(f"  策略 {strategy_name} 运行完成")
                except Exception as e:
                    logger.error(f"  策略 {strategy_name} 运行失败: {e}")
    
    def compare_strategies(self):
        """对比策略表现"""
        if not self.results:
            logger.warning("无策略结果可对比")
            return
        
        logger.info("\n策略表现对比")
        print("\n" + "="*80)
        print(f"{'策略名称':<20} {'总收益率':<10} {'夏普比率':<10} {'最大回撤':<10} {'交易次数':<10}")
        print("="*80)
        
        for strategy_name, result in self.results.items():
            total_return = (result.get('total_value', 0) / 1000000 - 1) * 100
            sharpe = result.get('sharpe_ratio', 0)
            max_drawdown = result.get('max_drawdown', 0) * 100
            trades = result.get('total_trades', 0)
            
            print(f"{strategy_name:<20} {total_return:>9.2f}% {sharpe:>9.2f} {max_drawdown:>9.2f}% {trades:>9}")
        
        print("="*80)
    
    def parameter_sensitivity(self, strategy_name, param_ranges):
        """参数敏感性分析"""
        if strategy_name not in self.strategies:
            logger.error(f"策略 {strategy_name} 不存在")
            return {}
        
        logger.info(f"\n参数敏感性分析: {strategy_name}")
        
        results = {}
        
        # 生成参数组合
        from itertools import product
        param_names = list(param_ranges.keys())
        param_values = list(param_ranges.values())
        
        for params in product(*param_values):
            param_dict = dict(zip(param_names, params))
            param_key = "_" .join([f"{k}={v}" for k, v in param_dict.items()])
            
            # 临时更新策略参数
            original_params = self.strategies[strategy_name]['params'].copy()
            self.strategies[strategy_name]['params'].update(param_dict)
            
            # 运行策略
            result = self.run_strategy(strategy_name)
            if result:
                results[param_key] = {
                    'params': param_dict,
                    'result': result
                }
            
            # 恢复原参数
            self.strategies[strategy_name]['params'] = original_params
        
        # 分析结果
        logger.info("参数敏感性分析结果")
        for param_key, item in results.items():
            result = item['result']
            total_return = (result.get('total_value', 0) / 1000000 - 1) * 100
            sharpe = result.get('sharpe_ratio', 0)
            logger.info(f"  {param_key}: 收益率={total_return:.2f}%, 夏普={sharpe:.2f}")
        
        return results
    
    def save_results(self, report_type):
        """保存回测结果"""
        report_date = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = f'reports/backtest_system_{report_type}_{report_date}.json'
        
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'report_type': report_type,
                'strategies': self.strategies,
                'results': self.results
            }, f, indent=2, default=str)
        
        logger.info(f"\n✓ 回测报告已保存: {report_path}")


def create_default_strategies():
    """创建默认策略"""
    strategies = {
        '动量策略': lambda date, data: {
            stock: 1/10 for stock in data.nlargest(10, 'momentum_20')['stock_code']
        },
        '反转策略': lambda date, data: {
            stock: 1/10 for stock in data.nsmallest(10, 'momentum_20')['stock_code']
        },
        '波动率策略': lambda date, data: {
            stock: 1/10 for stock in data.nsmallest(10, 'volatility_20')['stock_code']
        },
        '量价策略': lambda date, data: {
            stock: 1/10 for stock in data.nlargest(10, 'amount')['stock_code']
        }
    }
    return strategies

def main():
    """主函数"""
    logger.info("="*60)
    logger.info("回测系统架构优化")
    logger.info("="*60)
    
    try:
        system = BacktestSystem()
        
        if not system.load_data():
            return 1
        
        # 注册默认策略
        default_strategies = create_default_strategies()
        for name, signal_func in default_strategies.items():
            system.register_strategy(name, signal_func)
        
        # 运行所有策略
        system.run_all_strategies()
        
        # 对比策略表现
        system.compare_strategies()
        
        # 参数敏感性分析（示例）
        if '动量策略' in system.strategies:
            param_ranges = {
                'rebalance_freq': ['weekly', 'monthly', 'quarterly']
            }
            system.parameter_sensitivity('动量策略', param_ranges)
        
        # 保存结果
        system.save_results('multi_strategy')
        
        logger.info("="*60)
        logger.info("✅ 回测系统架构优化完成")
        logger.info("="*60)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ 回测系统异常: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
