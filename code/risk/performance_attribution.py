#!/usr/bin/env python3
"""
业绩归因分析模块
包括因子暴露归因、交易成本归因和可视化诊断面板
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import json
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class FactorAttribution:
    """因子暴露归因分析"""
    
    def __init__(self):
        """
        初始化因子归因分析器
        """
        self.style_factors = ['市值', '估值', '动量', '成长', '质量', '波动率']
        self.industry_factors = []  # 行业因子将在运行时动态添加
    
    def calculate_factor_exposure(self, portfolio: pd.DataFrame, factor_data: pd.DataFrame) -> pd.DataFrame:
        """
        计算投资组合的因子暴露
        
        Args:
            portfolio: 投资组合数据，包含股票权重
            factor_data: 因子数据，包含各股票的因子值
            
        Returns:
            因子暴露DataFrame
        """
        # 确保股票代码匹配
        common_stocks = portfolio.index.intersection(factor_data.index)
        if len(common_stocks) == 0:
            return pd.DataFrame()
        
        portfolio = portfolio.loc[common_stocks]
        factor_data = factor_data.loc[common_stocks]
        
        # 计算加权因子暴露
        weights = portfolio['weight'].values.reshape(-1, 1)
        factor_exposures = np.dot(weights.T, factor_data[self.style_factors])
        
        result = pd.DataFrame(
            factor_exposures,
            columns=self.style_factors,
            index=['exposure']
        )
        
        return result
    
    def brinson_attribution(self, portfolio_returns: pd.Series, benchmark_returns: pd.Series, 
                          portfolio_weights: pd.DataFrame, benchmark_weights: pd.DataFrame) -> Dict[str, float]:
        """
        Brinson归因分析
        
        Args:
            portfolio_returns: 投资组合收益率
            benchmark_returns: 基准收益率
            portfolio_weights: 投资组合权重（行业维度）
            benchmark_weights: 基准权重（行业维度）
            
        Returns:
            归因结果字典
        """
        # 计算行业超额收益
        industry_returns = portfolio_returns.groupby(portfolio_weights['industry']).mean()
        benchmark_industry_returns = benchmark_returns.groupby(benchmark_weights['industry']).mean()
        
        # 确保行业一致
        common_industries = industry_returns.index.intersection(benchmark_industry_returns.index)
        if len(common_industries) == 0:
            return {
                'allocation_effect': 0,
                'selection_effect': 0,
                'interaction_effect': 0,
                'total_excess_return': 0
            }
        
        industry_returns = industry_returns.loc[common_industries]
        benchmark_industry_returns = benchmark_industry_returns.loc[common_industries]
        
        # 计算行业权重
        portfolio_industry_weights = portfolio_weights.groupby('industry')['weight'].sum().loc[common_industries]
        benchmark_industry_weights = benchmark_weights.groupby('industry')['weight'].sum().loc[common_industries]
        
        # 归一化权重
        portfolio_industry_weights = portfolio_industry_weights / portfolio_industry_weights.sum()
        benchmark_industry_weights = benchmark_industry_weights / benchmark_industry_weights.sum()
        
        # 计算归因
        allocation_effect = np.sum((portfolio_industry_weights - benchmark_industry_weights) * benchmark_industry_returns)
        selection_effect = np.sum(benchmark_industry_weights * (industry_returns - benchmark_industry_returns))
        interaction_effect = np.sum((portfolio_industry_weights - benchmark_industry_weights) * (industry_returns - benchmark_industry_returns))
        
        total_excess_return = portfolio_returns.mean() - benchmark_returns.mean()
        
        return {
            'allocation_effect': allocation_effect,
            'selection_effect': selection_effect,
            'interaction_effect': interaction_effect,
            'total_excess_return': total_excess_return
        }
    
    def factor_attribution(self, portfolio_returns: pd.Series, factor_returns: pd.DataFrame, 
                          factor_exposures: pd.DataFrame) -> Dict[str, float]:
        """
        因子归因分析
        
        Args:
            portfolio_returns: 投资组合收益率
            factor_returns: 因子收益率
            factor_exposures: 因子暴露
            
        Returns:
            因子归因结果字典
        """
        # 计算因子贡献
        factor_contributions = {}
        total_factor_contribution = 0
        
        for factor in self.style_factors:
            if factor in factor_returns.columns and factor in factor_exposures.columns:
                contribution = factor_exposures[factor].iloc[0] * factor_returns[factor].mean()
                factor_contributions[factor] = contribution
                total_factor_contribution += contribution
        
        # 计算特质阿尔法
        total_return = portfolio_returns.mean()
        alpha = total_return - total_factor_contribution
        
        factor_contributions['alpha'] = alpha
        factor_contributions['total_return'] = total_return
        
        return factor_contributions


class TransactionCostAttribution:
    """交易成本归因分析"""
    
    def __init__(self, commission_rate: float = 0.0003, slippage_rate: float = 0.001):
        """
        初始化交易成本归因分析器
        
        Args:
            commission_rate: 佣金率
            slippage_rate: 滑点率
        """
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
    
    def calculate_transaction_costs(self, trades: pd.DataFrame) -> Dict[str, float]:
        """
        计算交易成本
        
        Args:
            trades: 交易记录，包含交易量和价格
            
        Returns:
            交易成本分析结果
        """
        if trades.empty:
            return {
                'total_cost': 0,
                'commission_cost': 0,
                'slippage_cost': 0,
                'cost_per_trade': 0,
                'cost_to_volume_ratio': 0
            }
        
        # 计算总交易量
        total_volume = (trades['volume'] * trades['price']).sum()
        
        # 计算佣金成本
        commission_cost = total_volume * self.commission_rate
        
        # 计算滑点成本
        slippage_cost = total_volume * self.slippage_rate
        
        # 计算总成本
        total_cost = commission_cost + slippage_cost
        
        # 计算单位成本指标
        cost_per_trade = total_cost / len(trades)
        cost_to_volume_ratio = total_cost / total_volume if total_volume > 0 else 0
        
        return {
            'total_cost': total_cost,
            'commission_cost': commission_cost,
            'slippage_cost': slippage_cost,
            'cost_per_trade': cost_per_trade,
            'cost_to_volume_ratio': cost_to_volume_ratio
        }
    
    def cost_attribution_over_time(self, trades: pd.DataFrame, time_period: str = 'day') -> pd.DataFrame:
        """
        计算不同时间周期的交易成本
        
        Args:
            trades: 交易记录
            time_period: 时间周期 ('day', 'week', 'month')
            
        Returns:
            时间序列成本分析
        """
        if trades.empty:
            return pd.DataFrame()
        
        # 确保有时间列
        if 'timestamp' not in trades.columns:
            return pd.DataFrame()
        
        # 转换时间戳
        trades['timestamp'] = pd.to_datetime(trades['timestamp'])
        
        # 按时间周期分组
        if time_period == 'day':
            trades['period'] = trades['timestamp'].dt.date
        elif time_period == 'week':
            trades['period'] = trades['timestamp'].dt.to_period('W')
        elif time_period == 'month':
            trades['period'] = trades['timestamp'].dt.to_period('M')
        else:
            return pd.DataFrame()
        
        # 分组计算
        results = []
        for period, group in trades.groupby('period'):
            costs = self.calculate_transaction_costs(group)
            results.append({
                'period': period,
                'total_cost': costs['total_cost'],
                'commission_cost': costs['commission_cost'],
                'slippage_cost': costs['slippage_cost'],
                'trade_count': len(group),
                'total_volume': (group['volume'] * group['price']).sum()
            })
        
        return pd.DataFrame(results)


class AttributionDashboard:
    """归因分析仪表板"""
    
    def __init__(self, output_dir: str = 'reports/attribution'):
        """
        初始化归因仪表板
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.factor_attributor = FactorAttribution()
        self.cost_attributor = TransactionCostAttribution()
    
    def generate_attribution_report(self, portfolio_data: Dict[str, Any], benchmark_data: Dict[str, Any], 
                                  factor_data: pd.DataFrame, trades: pd.DataFrame) -> Dict[str, Any]:
        """
        生成归因分析报告
        
        Args:
            portfolio_data: 投资组合数据
            benchmark_data: 基准数据
            factor_data: 因子数据
            trades: 交易记录
            
        Returns:
            归因报告
        """
        report = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'factor_attribution': {},
            'industry_attribution': {},
            'cost_attribution': {},
            'summary': {}
        }
        
        # 1. 因子归因
        if 'returns' in portfolio_data and 'factor_returns' in portfolio_data:
            portfolio_returns = pd.Series(portfolio_data['returns'])
            factor_returns = pd.DataFrame(portfolio_data['factor_returns'])
            
            # 计算因子暴露
            if 'weights' in portfolio_data:
                portfolio_weights = pd.DataFrame(portfolio_data['weights'])
                factor_exposures = self.factor_attributor.calculate_factor_exposure(portfolio_weights, factor_data)
                
                if not factor_exposures.empty:
                    factor_attr = self.factor_attributor.factor_attribution(
                        portfolio_returns, factor_returns, factor_exposures
                    )
                    report['factor_attribution'] = factor_attr
        
        # 2. 行业归因
        if 'returns' in portfolio_data and 'returns' in benchmark_data:
            portfolio_returns = pd.Series(portfolio_data['returns'])
            benchmark_returns = pd.Series(benchmark_data['returns'])
            
            if 'weights' in portfolio_data and 'weights' in benchmark_data:
                portfolio_weights = pd.DataFrame(portfolio_data['weights'])
                benchmark_weights = pd.DataFrame(benchmark_data['weights'])
                
                if 'industry' in portfolio_weights.columns and 'industry' in benchmark_weights.columns:
                    industry_attr = self.factor_attributor.brinson_attribution(
                        portfolio_returns, benchmark_returns, portfolio_weights, benchmark_weights
                    )
                    report['industry_attribution'] = industry_attr
        
        # 3. 交易成本归因
        if not trades.empty:
            cost_attr = self.cost_attributor.calculate_transaction_costs(trades)
            report['cost_attribution'] = cost_attr
            
            # 计算成本时间序列
            cost_time_series = self.cost_attributor.cost_attribution_over_time(trades, 'day')
            if not cost_time_series.empty:
                report['cost_time_series'] = cost_time_series.to_dict('records')
        
        # 4. 生成摘要
        self._generate_summary(report)
        
        # 保存报告
        report_path = os.path.join(self.output_dir, f'attribution_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"归因报告已生成: {report_path}")
        return report
    
    def _generate_summary(self, report: Dict[str, Any]):
        """
        生成报告摘要
        
        Args:
            report: 报告数据
        """
        summary = {}
        
        # 因子归因摘要
        if report['factor_attribution']:
            factor_attr = report['factor_attribution']
            summary['factor_contributions'] = {}
            
            for factor, contribution in factor_attr.items():
                if factor != 'total_return' and factor != 'alpha':
                    summary['factor_contributions'][factor] = contribution
            
            summary['alpha'] = factor_attr.get('alpha', 0)
            summary['total_return'] = factor_attr.get('total_return', 0)
        
        # 行业归因摘要
        if report['industry_attribution']:
            industry_attr = report['industry_attribution']
            summary['industry_allocation_effect'] = industry_attr.get('allocation_effect', 0)
            summary['industry_selection_effect'] = industry_attr.get('selection_effect', 0)
            summary['total_excess_return'] = industry_attr.get('total_excess_return', 0)
        
        # 成本归因摘要
        if report['cost_attribution']:
            cost_attr = report['cost_attribution']
            summary['total_transaction_cost'] = cost_attr.get('total_cost', 0)
            summary['cost_to_volume_ratio'] = cost_attr.get('cost_to_volume_ratio', 0)
        
        report['summary'] = summary
    
    def generate_visualization_data(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成可视化数据
        
        Args:
            report: 归因报告
            
        Returns:
            可视化数据
        """
        visualization_data = {
            'factor_contribution_chart': {},
            'industry_attribution_chart': {},
            'cost_trend_chart': {},
            'summary_metrics': {}
        }
        
        # 因子贡献图表数据
        if report['factor_attribution']:
            factor_attr = report['factor_attribution']
            factors = []
            contributions = []
            
            for factor, contribution in factor_attr.items():
                if factor != 'total_return' and factor != 'alpha':
                    factors.append(factor)
                    contributions.append(contribution)
            
            factors.append('alpha')
            contributions.append(factor_attr.get('alpha', 0))
            
            visualization_data['factor_contribution_chart'] = {
                'factors': factors,
                'contributions': contributions
            }
        
        # 行业归因图表数据
        if report['industry_attribution']:
            industry_attr = report['industry_attribution']
            visualization_data['industry_attribution_chart'] = {
                'allocation_effect': industry_attr.get('allocation_effect', 0),
                'selection_effect': industry_attr.get('selection_effect', 0),
                'interaction_effect': industry_attr.get('interaction_effect', 0)
            }
        
        # 成本趋势图表数据
        if 'cost_time_series' in report:
            cost_time_series = report['cost_time_series']
            periods = []
            total_costs = []
            commission_costs = []
            slippage_costs = []
            
            for item in cost_time_series:
                periods.append(str(item['period']))
                total_costs.append(item['total_cost'])
                commission_costs.append(item['commission_cost'])
                slippage_costs.append(item['slippage_cost'])
            
            visualization_data['cost_trend_chart'] = {
                'periods': periods,
                'total_costs': total_costs,
                'commission_costs': commission_costs,
                'slippage_costs': slippage_costs
            }
        
        # 摘要指标
        if report['summary']:
            visualization_data['summary_metrics'] = report['summary']
        
        return visualization_data
    
    def export_visualization_data(self, visualization_data: Dict[str, Any], output_file: str):
        """
        导出可视化数据
        
        Args:
            visualization_data: 可视化数据
            output_file: 输出文件路径
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(visualization_data, f, ensure_ascii=False, indent=2)
        
        print(f"可视化数据已导出: {output_file}")


class AttributionManager:
    """归因分析管理器"""
    
    def __init__(self):
        """
        初始化归因分析管理器
        """
        self.dashboard = AttributionDashboard()
    
    def run_full_attribution(self, portfolio_data: Dict[str, Any], benchmark_data: Dict[str, Any], 
                           factor_data: pd.DataFrame, trades: pd.DataFrame) -> Dict[str, Any]:
        """
        运行完整的归因分析
        
        Args:
            portfolio_data: 投资组合数据
            benchmark_data: 基准数据
            factor_data: 因子数据
            trades: 交易记录
            
        Returns:
            完整归因分析结果
        """
        # 生成归因报告
        report = self.dashboard.generate_attribution_report(portfolio_data, benchmark_data, factor_data, trades)
        
        # 生成可视化数据
        visualization_data = self.dashboard.generate_visualization_data(report)
        
        # 导出可视化数据
        viz_output = os.path.join(self.dashboard.output_dir, f'visualization_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        self.dashboard.export_visualization_data(visualization_data, viz_output)
        
        return {
            'attribution_report': report,
            'visualization_data': visualization_data,
            'report_path': os.path.join(self.dashboard.output_dir, f'attribution_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'),
            'visualization_path': viz_output
        }
    
    def quick_attribution(self, portfolio_returns: pd.Series, benchmark_returns: pd.Series, 
                         portfolio_weights: pd.DataFrame, factor_data: pd.DataFrame) -> Dict[str, Any]:
        """
        快速归因分析
        
        Args:
            portfolio_returns: 投资组合收益率
            benchmark_returns: 基准收益率
            portfolio_weights: 投资组合权重
            factor_data: 因子数据
            
        Returns:
            快速归因结果
        """
        result = {}
        
        # 计算因子暴露
        factor_exposures = self.dashboard.factor_attributor.calculate_factor_exposure(portfolio_weights, factor_data)
        
        # 简化的因子归因
        if not factor_exposures.empty:
            # 假设因子收益率
            factor_returns = pd.DataFrame(
                np.random.normal(0.0005, 0.001, (len(portfolio_returns), len(self.dashboard.factor_attributor.style_factors))),
                columns=self.dashboard.factor_attributor.style_factors
            )
            
            factor_attr = self.dashboard.factor_attributor.factor_attribution(
                portfolio_returns, factor_returns, factor_exposures
            )
            result['factor_attribution'] = factor_attr
        
        # 计算超额收益
        excess_return = portfolio_returns.mean() - benchmark_returns.mean()
        result['excess_return'] = excess_return
        
        return result


# 测试代码
if __name__ == "__main__":
    # 创建测试数据
    np.random.seed(42)
    n_stocks = 20
    n_days = 60
    
    # 模拟投资组合数据
    stocks = [f'{i:06d}' for i in range(1, n_stocks+1)]
    weights = np.random.dirichlet(np.ones(n_stocks))
    
    portfolio_data = {
        'weights': pd.DataFrame({
            'weight': weights,
            'industry': np.random.choice(['科技', '金融', '消费', '医药', '能源'], n_stocks)
        }, index=stocks),
        'returns': np.random.normal(0.0008, 0.01, n_days),
        'factor_returns': {
            '市值': np.random.normal(0.0003, 0.0005, n_days),
            '估值': np.random.normal(0.0002, 0.0004, n_days),
            '动量': np.random.normal(0.0004, 0.0006, n_days),
            '成长': np.random.normal(0.0003, 0.0005, n_days),
            '质量': np.random.normal(0.0002, 0.0004, n_days),
            '波动率': np.random.normal(-0.0001, 0.0003, n_days)
        }
    }
    
    # 模拟基准数据
    benchmark_data = {
        'weights': pd.DataFrame({
            'weight': np.random.dirichlet(np.ones(n_stocks)),
            'industry': np.random.choice(['科技', '金融', '消费', '医药', '能源'], n_stocks)
        }, index=stocks),
        'returns': np.random.normal(0.0005, 0.01, n_days)
    }
    
    # 模拟因子数据
    factor_data = pd.DataFrame(
        np.random.normal(0, 1, (n_stocks, 6)),
        index=stocks,
        columns=['市值', '估值', '动量', '成长', '质量', '波动率']
    )
    
    # 模拟交易数据
    trades = pd.DataFrame({
        'timestamp': pd.date_range(start='2023-01-01', periods=100, freq='H'),
        'stock_code': np.random.choice(stocks, 100),
        'volume': np.random.randint(100, 10000, 100),
        'price': np.random.uniform(10, 100, 100),
        'side': np.random.choice(['buy', 'sell'], 100)
    })
    
    # 初始化归因管理器
    manager = AttributionManager()
    
    # 运行完整归因分析
    print("运行完整归因分析...")
    result = manager.run_full_attribution(portfolio_data, benchmark_data, factor_data, trades)
    
    # 打印摘要
    print("\n归因分析摘要:")
    print(f"总收益率: {result['attribution_report']['summary'].get('total_return', 0):.4f}")
    print(f"阿尔法: {result['attribution_report']['summary'].get('alpha', 0):.4f}")
    print(f"超额收益: {result['attribution_report']['summary'].get('total_excess_return', 0):.4f}")
    print(f"总交易成本: {result['attribution_report']['summary'].get('total_transaction_cost', 0):.2f}")
    
    # 运行快速归因
    print("\n运行快速归因分析...")
    quick_result = manager.quick_attribution(
        pd.Series(portfolio_data['returns']),
        pd.Series(benchmark_data['returns']),
        pd.DataFrame(portfolio_data['weights']),
        factor_data
    )
    
    print(f"快速归因超额收益: {quick_result.get('excess_return', 0):.4f}")
