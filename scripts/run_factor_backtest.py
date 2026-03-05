#!/usr/bin/env python3
"""
因子回测脚本
任务：验证新挖掘因子的有效性
支持：单个因子回测、多因子组合回测、股票级别因子分析
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

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/factor_backtest.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FactorBacktester:
    """因子回测器"""
    
    def __init__(self, data_file='data/akshare_real_data_fixed.pkl'):
        self.data_file = data_file
        self.data = None
        self.factors = []
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
        
        # 识别可用的因子列
        self.factors = [col for col in self.data.columns if col.startswith('momentum_') or col.startswith('volatility_') or \
                       col.startswith('price_to_') or col in ['turnover', 'amount', 'change_pct']]
        
        logger.info(f"✓ 识别到 {len(self.factors)} 个因子: {self.factors}")
        
        return True
    
    def backtest_single_factor(self, factor_name):
        """单个因子回测"""
        if factor_name not in self.factors:
            logger.error(f"因子 {factor_name} 不存在")
            return {}
        
        logger.info(f"\n回测因子: {factor_name}")
        
        # 按日期分组
        date_groups = self.data.groupby('date')
        returns = []
        
        for date, group in date_groups:
            if len(group) < 20:
                continue
            
            # 按因子排序，选择前10%和后10%的股票
            sorted_group = group.sort_values(factor_name, ascending=False)
            top_10pct = sorted_group.head(int(len(sorted_group) * 0.1))
            bottom_10pct = sorted_group.tail(int(len(sorted_group) * 0.1))
            
            if len(top_10pct) > 0 and len(bottom_10pct) > 0:
                # 计算下一期收益
                next_date = self.data[self.data['date'] > date]['date'].min()
                if pd.isna(next_date):
                    continue
                
                next_group = self.data[self.data['date'] == next_date]
                
                top_returns = []
                for stock in top_10pct['stock_code']:
                    stock_next = next_group[next_group['stock_code'] == stock]
                    if len(stock_next) > 0:
                        top_returns.append(stock_next['change_pct'].iloc[0])
                
                bottom_returns = []
                for stock in bottom_10pct['stock_code']:
                    stock_next = next_group[next_group['stock_code'] == stock]
                    if len(stock_next) > 0:
                        bottom_returns.append(stock_next['change_pct'].iloc[0])
                
                if top_returns and bottom_returns:
                    long_short_return = np.mean(top_returns) - np.mean(bottom_returns)
                    returns.append(long_short_return)
        
        if returns:
            mean_return = np.mean(returns)
            std_return = np.std(returns)
            sharpe_ratio = mean_return / std_return * np.sqrt(252) if std_return > 0 else 0
            
            result = {
                'factor': factor_name,
                'mean_return': mean_return,
                'std_return': std_return,
                'sharpe_ratio': sharpe_ratio,
                'total_returns': returns,
                'periods': len(returns)
            }
            
            logger.info(f"  平均收益: {mean_return:.2%}")
            logger.info(f"  收益标准差: {std_return:.2%}")
            logger.info(f"  夏普比率: {sharpe_ratio:.2f}")
            logger.info(f"  回测周期: {len(returns)}")
            
            return result
        else:
            logger.warning(f"  因子 {factor_name} 无足够数据进行回测")
            return {}
    
    def backtest_multi_factors(self, factor_weights):
        """多因子组合回测"""
        logger.info("\n回测多因子组合")
        
        # 计算综合因子得分
        date_groups = self.data.groupby('date')
        returns = []
        
        for date, group in date_groups:
            if len(group) < 20:
                continue
            
            # 计算综合得分
            group['composite_score'] = 0
            for factor, weight in factor_weights.items():
                if factor in group.columns:
                    # 标准化因子
                    factor_mean = group[factor].mean()
                    factor_std = group[factor].std()
                    if factor_std > 0:
                        group['composite_score'] += weight * (group[factor] - factor_mean) / factor_std
            
            # 选择得分最高的前10%和最低的后10%
            sorted_group = group.sort_values('composite_score', ascending=False)
            top_10pct = sorted_group.head(int(len(sorted_group) * 0.1))
            bottom_10pct = sorted_group.tail(int(len(sorted_group) * 0.1))
            
            if len(top_10pct) > 0 and len(bottom_10pct) > 0:
                next_date = self.data[self.data['date'] > date]['date'].min()
                if pd.isna(next_date):
                    continue
                
                next_group = self.data[self.data['date'] == next_date]
                
                top_returns = []
                for stock in top_10pct['stock_code']:
                    stock_next = next_group[next_group['stock_code'] == stock]
                    if len(stock_next) > 0:
                        top_returns.append(stock_next['change_pct'].iloc[0])
                
                bottom_returns = []
                for stock in bottom_10pct['stock_code']:
                    stock_next = next_group[next_group['stock_code'] == stock]
                    if len(stock_next) > 0:
                        bottom_returns.append(stock_next['change_pct'].iloc[0])
                
                if top_returns and bottom_returns:
                    long_short_return = np.mean(top_returns) - np.mean(bottom_returns)
                    returns.append(long_short_return)
        
        if returns:
            mean_return = np.mean(returns)
            std_return = np.std(returns)
            sharpe_ratio = mean_return / std_return * np.sqrt(252) if std_return > 0 else 0
            
            result = {
                'factors': factor_weights,
                'mean_return': mean_return,
                'std_return': std_return,
                'sharpe_ratio': sharpe_ratio,
                'total_returns': returns,
                'periods': len(returns)
            }
            
            logger.info(f"  平均收益: {mean_return:.2%}")
            logger.info(f"  收益标准差: {std_return:.2%}")
            logger.info(f"  夏普比率: {sharpe_ratio:.2f}")
            logger.info(f"  回测周期: {len(returns)}")
            
            return result
        else:
            logger.warning("  无足够数据进行多因子回测")
            return {}
    
    def analyze_stock_factor_performance(self, factor_name):
        """股票级别因子表现分析"""
        if factor_name not in self.factors:
            logger.error(f"因子 {factor_name} 不存在")
            return {}
        
        logger.info(f"\n股票级别因子分析: {factor_name}")
        
        stock_performance = {}
        
        # 按股票分组
        stock_groups = self.data.groupby('stock_code')
        
        for stock_code, group in stock_groups:
            if len(group) < 20:
                continue
            
            # 计算因子值与收益的相关性
            if 'change_pct' in group.columns:
                corr = group[factor_name].corr(group['change_pct'])
                stock_performance[stock_code] = {
                    'correlation': corr,
                    'factor_mean': group[factor_name].mean(),
                    'return_mean': group['change_pct'].mean(),
                    'observations': len(group)
                }
        
        # 按相关性排序
        sorted_stocks = sorted(stock_performance.items(), key=lambda x: abs(x[1]['correlation']), reverse=True)
        
        logger.info(f"  分析股票数: {len(stock_performance)}")
        logger.info("  相关性最高的5只股票:")
        for stock, perf in sorted_stocks[:5]:
            logger.info(f"    {stock}: 相关性={perf['correlation']:.3f}, 平均收益={perf['return_mean']:.2%}")
        
        return {
            'factor': factor_name,
            'stock_performance': stock_performance,
            'top_correlation_stocks': sorted_stocks[:10]
        }
    
    def compare_factors(self):
        """因子绩效对比"""
        logger.info("\n因子绩效对比")
        
        comparison_results = {}
        
        for factor in self.factors:
            result = self.backtest_single_factor(factor)
            if result:
                comparison_results[factor] = result
        
        # 按夏普比率排序
        sorted_factors = sorted(comparison_results.items(), key=lambda x: x[1]['sharpe_ratio'], reverse=True)
        
        logger.info("  因子绩效排名:")
        for factor, result in sorted_factors:
            logger.info(f"    {factor}: 夏普比率={result['sharpe_ratio']:.2f}, 平均收益={result['mean_return']:.2%}")
        
        return comparison_results
    
    def save_results(self, results, report_type):
        """保存回测结果"""
        report_date = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = f'reports/factor_backtest_{report_type}_{report_date}.json'
        
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'report_type': report_type,
                'results': results
            }, f, indent=2)
        
        logger.info(f"\n✓ 回测报告已保存: {report_path}")


def main():
    """主函数"""
    logger.info("="*60)
    logger.info("因子回测任务")
    logger.info("="*60)
    
    try:
        backtester = FactorBacktester()
        
        if not backtester.load_data():
            return 1
        
        # 解析命令行参数
        if len(sys.argv) > 1:
            task = sys.argv[1]
        else:
            task = 'single'
        
        if task == '单个因子回测':
            print("\n可用因子:")
            for i, factor in enumerate(backtester.factors, 1):
                print(f"  {i}. {factor}")
            
            factor_idx = input("\n请选择因子编号: ").strip()
            if factor_idx.isdigit():
                idx = int(factor_idx) - 1
                if 0 <= idx < len(backtester.factors):
                    factor_name = backtester.factors[idx]
                    result = backtester.backtest_single_factor(factor_name)
                    backtester.save_results(result, 'single_factor')
        
        elif task == '多因子组合回测':
            print("\n可用因子:")
            for i, factor in enumerate(backtester.factors, 1):
                print(f"  {i}. {factor}")
            
            factor_indices = input("\n请输入因子编号（多个用逗号分隔）: ").strip()
            indices = [int(idx) - 1 for idx in factor_indices.split(',') if idx.isdigit()]
            selected_factors = [backtester.factors[idx] for idx in indices if 0 <= idx < len(backtester.factors)]
            
            if selected_factors:
                weights = {}
                for factor in selected_factors:
                    weight = float(input(f"请输入 {factor} 的权重: "))
                    weights[factor] = weight
                
                # 归一化权重
                total_weight = sum(weights.values())
                if total_weight > 0:
                    weights = {k: v/total_weight for k, v in weights.items()}
                
                result = backtester.backtest_multi_factors(weights)
                backtester.save_results(result, 'multi_factor')
        
        elif task == '股票级别因子分析':
            print("\n可用因子:")
            for i, factor in enumerate(backtester.factors, 1):
                print(f"  {i}. {factor}")
            
            factor_idx = input("\n请选择因子编号: ").strip()
            if factor_idx.isdigit():
                idx = int(factor_idx) - 1
                if 0 <= idx < len(backtester.factors):
                    factor_name = backtester.factors[idx]
                    result = backtester.analyze_stock_factor_performance(factor_name)
                    backtester.save_results(result, 'stock_analysis')
        
        elif task == '因子绩效对比':
            results = backtester.compare_factors()
            backtester.save_results(results, 'factor_comparison')
        
        else:
            # 默认运行因子绩效对比
            results = backtester.compare_factors()
            backtester.save_results(results, 'factor_comparison')
        
        logger.info("="*60)
        logger.info("✅ 因子回测任务完成")
        logger.info("="*60)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ 回测异常: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
