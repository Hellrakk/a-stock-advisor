#!/usr/bin/env python3
"""
因子回测脚本（优化版）
任务：验证新挖掘因子的有效性
支持：单个因子回测、多因子组合回测、股票级别因子分析
优化：使用向量化操作，提升回测速度
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
        logging.FileHandler('logs/factor_backtest.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FactorBacktester:
    """因子回测器（优化版）"""
    
    def __init__(self, data_file='data/akshare_real_data_fixed.pkl'):
        self.data_file = data_file
        self.data = None
        self.factors = []
        self.results = {}
        self.stock_lookup = {}
    
    def load_data(self):
        """加载数据并预处理"""
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
        
        # 预处理：建立日期-股票的快速查询索引
        logger.info("✓ 预处理数据，建立快速查询索引...")
        # 按日期分组
        date_groups = self.data.groupby('date')
        for date, group in date_groups:
            # 为每个日期的股票建立字典，加速查询
            stock_dict = {}
            for _, row in group.iterrows():
                stock_dict[row['stock_code']] = row['change_pct'] if 'change_pct' in row else 0
            self.stock_lookup[date] = stock_dict
        
        logger.info(f"✓ 预处理完成，建立了 {len(self.stock_lookup)} 个日期的股票索引")
        
        return True
    
    def backtest_single_factor(self, factor_name):
        """单个因子回测（优化版）"""
        if factor_name not in self.factors:
            logger.error(f"因子 {factor_name} 不存在")
            return {}
        
        logger.info(f"\n回测因子: {factor_name}")
        
        # 按日期分组
        date_groups = self.data.groupby('date')
        returns = []
        date_labels = []
        
        # 缓存日期排序
        sorted_dates = sorted(self.stock_lookup.keys())
        date_to_index = {date: i for i, date in enumerate(sorted_dates)}
        
        for date, group in date_groups:
            if len(group) < 20:
                continue
            
            # 按因子排序，选择前10%和后10%的股票
            sorted_group = group.sort_values(factor_name, ascending=False)
            top_10pct = sorted_group.head(int(len(sorted_group) * 0.1))
            bottom_10pct = sorted_group.tail(int(len(sorted_group) * 0.1))
            
            if len(top_10pct) > 0 and len(bottom_10pct) > 0:
                # 快速获取下一个日期
                current_idx = date_to_index.get(date, -1)
                if current_idx >= 0 and current_idx + 1 < len(sorted_dates):
                    next_date = sorted_dates[current_idx + 1]
                    next_stocks = self.stock_lookup.get(next_date, {})
                    
                    # 向量化计算收益
                    top_returns = []
                    for stock in top_10pct['stock_code']:
                        if stock in next_stocks:
                            top_returns.append(next_stocks[stock])
                    
                    bottom_returns = []
                    for stock in bottom_10pct['stock_code']:
                        if stock in next_stocks:
                            bottom_returns.append(next_stocks[stock])
                    
                    if top_returns and bottom_returns:
                        long_short_return = np.mean(top_returns) - np.mean(bottom_returns)
                        returns.append(long_short_return)
                        date_labels.append(date)
        
        if returns:
            mean_return = np.mean(returns)
            std_return = np.std(returns)
            sharpe_ratio = mean_return / std_return * np.sqrt(252) if std_return > 0 else 0
            
            # 计算因子衰减
            decay_analysis = self._analyze_factor_decay(returns, date_labels)
            
            result = {
                'factor': factor_name,
                'mean_return': mean_return,
                'std_return': std_return,
                'sharpe_ratio': sharpe_ratio,
                'total_returns': returns,
                'periods': len(returns),
                'decay_analysis': decay_analysis,
                'date_labels': date_labels
            }
            
            logger.info(f"  平均收益: {mean_return:.2%}")
            logger.info(f"  收益标准差: {std_return:.2%}")
            logger.info(f"  夏普比率: {sharpe_ratio:.2f}")
            logger.info(f"  回测周期: {len(returns)}")
            logger.info(f"  因子衰减: {decay_analysis['decay_trend']:.4f}")
            logger.info(f"  最近6个月表现: {decay_analysis['recent_performance']:.2%}")
            logger.info(f"  早期表现: {decay_analysis['early_performance']:.2%}")
            
            return result
        else:
            logger.warning(f"  因子 {factor_name} 无足够数据进行回测")
            return {}
    
    def _analyze_factor_decay(self, returns: List[float], date_labels: List) -> Dict:
        """
        分析因子衰减情况
        
        Args:
            returns: 因子收益序列
            date_labels: 对应的日期标签
            
        Returns:
            因子衰减分析结果
        """
        if len(returns) < 10:
            return {
                'decay_trend': 0,
                'recent_performance': 0,
                'early_performance': 0,
                'half_life': 0,
                'correlation_with_time': 0
            }
        
        # 计算时间序列相关性
        time_indices = np.arange(len(returns))
        correlation = np.corrcoef(time_indices, returns)[0, 1]
        
        # 线性回归分析衰减趋势
        slope, _ = np.polyfit(time_indices, returns, 1)
        
        # 计算早期和近期表现
        split_point = len(returns) // 2
        early_returns = returns[:split_point]
        recent_returns = returns[split_point:]
        
        early_performance = np.mean(early_returns)
        recent_performance = np.mean(recent_returns)
        
        # 计算半衰期（简化版）
        half_life = 0
        if slope < 0:
            # 估算因子效果减半所需的时间
            peak_return = max(returns)
            half_return = peak_return / 2
            for i, ret in enumerate(returns):
                if ret <= half_return:
                    half_life = i
                    break
        
        return {
            'decay_trend': slope,
            'recent_performance': recent_performance,
            'early_performance': early_performance,
            'performance_difference': recent_performance - early_performance,
            'half_life': half_life,
            'correlation_with_time': correlation
        }
    
    def analyze_factor_decay(self):
        """
        分析所有因子的衰减情况
        
        Returns:
            因子衰减分析结果
        """
        logger.info("\n因子衰减分析")
        
        decay_results = {}
        
        for factor in self.factors:
            result = self.backtest_single_factor(factor)
            if result and 'decay_analysis' in result:
                decay_results[factor] = {
                    'decay_trend': result['decay_analysis']['decay_trend'],
                    'recent_performance': result['decay_analysis']['recent_performance'],
                    'early_performance': result['decay_analysis']['early_performance'],
                    'performance_difference': result['decay_analysis']['performance_difference'],
                    'sharpe_ratio': result['sharpe_ratio']
                }
        
        # 按衰减趋势排序（负值表示衰减）
        sorted_factors = sorted(decay_results.items(), key=lambda x: x[1]['decay_trend'], reverse=True)
        
        logger.info("  因子衰减情况排名:")
        for factor, analysis in sorted_factors[:10]:  # 显示前10个
            decay_trend = analysis['decay_trend']
            recent_perf = analysis['recent_performance']
            early_perf = analysis['early_performance']
            perf_diff = analysis['performance_difference']
            
            logger.info(f"    {factor}: 衰减趋势={decay_trend:.4f}, 近期表现={recent_perf:.2%}, 早期表现={early_perf:.2%}, 差异={perf_diff:.2%}")
        
        return decay_results
    
    def process_factor(self, factor_name):
        """处理单个因子的回测（用于并行处理）"""
        return self.backtest_single_factor(factor_name)
    
    def compare_factors(self):
        """因子绩效对比（并行优化版）"""
        logger.info("\n因子绩效对比")
        
        comparison_results = {}
        
        # 使用并行处理加速
        with ProcessPoolExecutor(max_workers=min(4, len(self.factors))) as executor:
            futures = {executor.submit(self.process_factor, factor): factor for factor in self.factors}
            
            for future in futures:
                factor = futures[future]
                try:
                    result = future.result()
                    if result:
                        comparison_results[factor] = result
                except Exception as e:
                    logger.error(f"  因子 {factor} 处理失败: {e}")
        
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
    logger.info("因子回测任务（优化版）")
    logger.info("="*60)
    
    try:
        backtester = FactorBacktester()
        
        if not backtester.load_data():
            return 1
        
        # 解析命令行参数
        if len(sys.argv) > 1:
            task = sys.argv[1]
        else:
            task = 'factor_comparison'
        
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
        
        elif task == '因子绩效对比':
            results = backtester.compare_factors()
            backtester.save_results(results, 'factor_comparison')
        
        elif task == '因子衰减分析':
            results = backtester.analyze_factor_decay()
            backtester.save_results(results, 'factor_decay')
        
        else:
            # 默认运行因子绩效对比（并行）
            results = backtester.compare_factors()
            backtester.save_results(results, 'factor_comparison')
        
        logger.info("="*60)
        logger.info("✅ 因子回测任务完成（优化版）")
        logger.info("="*60)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ 回测异常: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
