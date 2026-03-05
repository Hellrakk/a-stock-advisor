#!/usr/bin/env python3
"""
Brinson归因分析模块 - BrinsonAttribution
分解组合收益为配置效应、选择效应和交互效应
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import warnings
warnings.filterwarnings('ignore')


@dataclass
class BrinsonConfig:
    """Brinson归因配置"""
    periods_per_year: int = 12


class BrinsonAttribution:
    """Brinson归因分析器"""
    
    def __init__(self, config: Optional[BrinsonConfig] = None):
        self.config = config or BrinsonConfig()
        
        self.portfolio_weights: Optional[pd.DataFrame] = None
        self.portfolio_returns: Optional[pd.DataFrame] = None
        self.benchmark_weights: Optional[pd.DataFrame] = None
        self.benchmark_returns: Optional[pd.DataFrame] = None
        self.categories: Optional[List[str]] = None
    
    def set_data(self, 
                 portfolio_weights: pd.DataFrame,
                 portfolio_returns: pd.DataFrame,
                 benchmark_weights: pd.DataFrame,
                 benchmark_returns: pd.DataFrame,
                 categories: Optional[List[str]] = None):
        self.portfolio_weights = portfolio_weights.copy()
        self.portfolio_returns = portfolio_returns.copy()
        self.benchmark_weights = benchmark_weights.copy()
        self.benchmark_returns = benchmark_returns.copy()
        self.categories = categories or portfolio_weights.columns.tolist()
        
        self._validate_data()
    
    def _validate_data(self):
        if self.portfolio_weights.shape != self.benchmark_weights.shape:
            raise ValueError("组合权重和基准权重形状不匹配")
        
        if self.portfolio_returns.shape != self.benchmark_returns.shape:
            raise ValueError("组合收益和基准收益形状不匹配")
    
    def calculate_allocation_effect(self, 
                                    portfolio_weight: float,
                                    benchmark_weight: float,
                                    benchmark_return: float) -> float:
        return (portfolio_weight - benchmark_weight) * benchmark_return
    
    def calculate_selection_effect(self,
                                   portfolio_weight: float,
                                   portfolio_return: float,
                                   benchmark_return: float) -> float:
        return portfolio_weight * (portfolio_return - benchmark_return)
    
    def calculate_interaction_effect(self,
                                     portfolio_weight: float,
                                     benchmark_weight: float,
                                     portfolio_return: float,
                                     benchmark_return: float) -> float:
        return (portfolio_weight - benchmark_weight) * (portfolio_return - benchmark_return)
    
    def calculate_period_attribution(self,
                                     portfolio_weights: pd.Series,
                                     portfolio_returns: pd.Series,
                                     benchmark_weights: pd.Series,
                                     benchmark_returns: pd.Series) -> Dict:
        allocation_effect = self.calculate_allocation_effect(
            portfolio_weights, benchmark_weights, benchmark_returns
        )
        
        selection_effect = self.calculate_selection_effect(
            portfolio_weights, portfolio_returns, benchmark_returns
        )
        
        interaction_effect = self.calculate_interaction_effect(
            portfolio_weights, benchmark_weights, portfolio_returns, benchmark_returns
        )
        
        portfolio_return = (portfolio_weights * portfolio_returns).sum()
        benchmark_return = (benchmark_weights * benchmark_returns).sum()
        excess_return = portfolio_return - benchmark_return
        
        return {
            'allocation_effect': allocation_effect,
            'selection_effect': selection_effect,
            'interaction_effect': interaction_effect,
            'total_effect': allocation_effect + selection_effect + interaction_effect,
            'portfolio_return': portfolio_return,
            'benchmark_return': benchmark_return,
            'excess_return': excess_return
        }
    
    def run_attribution(self) -> Dict:
        if self.portfolio_weights is None:
            return {"error": "数据未设置"}
        
        n_periods = len(self.portfolio_weights)
        
        results = {
            'periods': [],
            'summary': {
                'total_allocation': 0,
                'total_selection': 0,
                'total_interaction': 0,
                'total_excess': 0
            },
            'by_category': {cat: {'allocation': 0, 'selection': 0, 'interaction': 0} 
                           for cat in self.categories}
        }
        
        for i in range(n_periods):
            period_result = self.calculate_period_attribution(
                self.portfolio_weights.iloc[i],
                self.portfolio_returns.iloc[i],
                self.benchmark_weights.iloc[i],
                self.benchmark_returns.iloc[i]
            )
            
            results['periods'].append(period_result)
            
            results['summary']['total_allocation'] += period_result['allocation_effect'].sum()
            results['summary']['total_selection'] += period_result['selection_effect'].sum()
            results['summary']['total_interaction'] += period_result['interaction_effect'].sum()
            results['summary']['total_excess'] += period_result['excess_return']
            
            for cat in self.categories:
                if cat in period_result['allocation_effect'].index:
                    results['by_category'][cat]['allocation'] += period_result['allocation_effect'][cat]
                    results['by_category'][cat]['selection'] += period_result['selection_effect'][cat]
                    results['by_category'][cat]['interaction'] += period_result['interaction_effect'][cat]
        
        return results
    
    def generate_attribution_report(self, results: Optional[Dict] = None) -> str:
        if results is None:
            results = self.run_attribution()
        
        if "error" in results:
            return f"# Brinson归因分析报告\n\n错误: {results['error']}"
        
        lines = []
        lines.append("# Brinson归因分析报告")
        lines.append("")
        
        lines.append("## 总体归因")
        lines.append("")
        lines.append("| 效应 | 数值 | 贡献比例 |")
        lines.append("|------|------|----------|")
        
        summary = results['summary']
        total = summary['total_excess']
        
        lines.append(f"| 配置效应 | {summary['total_allocation']:.4f} | {summary['total_allocation']/total*100 if total != 0 else 0:.1f}% |")
        lines.append(f"| 选择效应 | {summary['total_selection']:.4f} | {summary['total_selection']/total*100 if total != 0 else 0:.1f}% |")
        lines.append(f"| 交互效应 | {summary['total_interaction']:.4f} | {summary['total_interaction']/total*100 if total != 0 else 0:.1f}% |")
        lines.append(f"| **超额收益** | **{total:.4f}** | **100%** |")
        lines.append("")
        
        lines.append("## 分类归因")
        lines.append("")
        lines.append("| 类别 | 配置效应 | 选择效应 | 交互效应 | 总效应 |")
        lines.append("|------|----------|----------|----------|--------|")
        
        for cat, effects in results['by_category'].items():
            total_cat = effects['allocation'] + effects['selection'] + effects['interaction']
            lines.append(f"| {cat} | {effects['allocation']:.4f} | {effects['selection']:.4f} | {effects['interaction']:.4f} | {total_cat:.4f} |")
        
        lines.append("")
        
        lines.append("## 分析结论")
        lines.append("")
        
        if summary['total_allocation'] > 0:
            lines.append("- ✅ 配置效应为正，说明行业/类别配置决策有效")
        else:
            lines.append("- ⚠️ 配置效应为负，行业/类别配置决策有待改进")
        
        if summary['total_selection'] > 0:
            lines.append("- ✅ 选择效应为正，说明个股选择能力优秀")
        else:
            lines.append("- ⚠️ 选择效应为负，个股选择能力需要提升")
        
        if abs(summary['total_allocation']) > abs(summary['total_selection']):
            lines.append("- 📊 配置效应占主导，收益主要来自行业配置")
        else:
            lines.append("- 📊 选择效应占主导，收益主要来自个股选择")
        
        return "\n".join(lines)


class FactorAttribution:
    """因子归因分析器"""
    
    def __init__(self):
        self.factor_returns: Optional[pd.DataFrame] = None
        self.factor_exposures: Optional[pd.DataFrame] = None
        self.portfolio_returns: Optional[pd.Series] = None
    
    def set_data(self,
                 factor_returns: pd.DataFrame,
                 factor_exposures: pd.DataFrame,
                 portfolio_returns: pd.Series):
        self.factor_returns = factor_returns.copy()
        self.factor_exposures = factor_exposures.copy()
        self.portfolio_returns = portfolio_returns.copy()
    
    def run_attribution(self) -> Dict:
        if self.factor_returns is None:
            return {"error": "数据未设置"}
        
        factor_names = self.factor_returns.columns.tolist()
        
        results = {
            'factor_contributions': {},
            'total_factor_return': 0,
            'residual_return': 0,
            'r_squared': 0
        }
        
        for factor in factor_names:
            exposure = self.factor_exposures[factor].mean()
            factor_return = self.factor_returns[factor].mean()
            contribution = exposure * factor_return
            
            results['factor_contributions'][factor] = {
                'exposure': exposure,
                'factor_return': factor_return,
                'contribution': contribution
            }
            results['total_factor_return'] += contribution
        
        results['residual_return'] = self.portfolio_returns.mean() - results['total_factor_return']
        
        if len(self.portfolio_returns) > 1:
            total_var = self.portfolio_returns.var()
            explained_var = 0
            
            for factor in factor_names:
                exposure = self.factor_exposures[factor]
                factor_ret = self.factor_returns[factor]
                explained_var += (exposure * factor_ret).var()
            
            results['r_squared'] = explained_var / total_var if total_var > 0 else 0
        
        return results
    
    def generate_factor_report(self, results: Optional[Dict] = None) -> str:
        if results is None:
            results = self.run_attribution()
        
        if "error" in results:
            return f"# 因子归因分析报告\n\n错误: {results['error']}"
        
        lines = []
        lines.append("# 因子归因分析报告")
        lines.append("")
        
        lines.append("## 因子贡献")
        lines.append("")
        lines.append("| 因子 | 暴露 | 因子收益 | 贡献 |")
        lines.append("|------|------|----------|------|")
        
        for factor, data in results['factor_contributions'].items():
            lines.append(f"| {factor} | {data['exposure']:.4f} | {data['factor_return']:.4f} | {data['contribution']:.4f} |")
        
        lines.append("")
        lines.append("## 汇总")
        lines.append("")
        lines.append(f"- **总因子收益**: {results['total_factor_return']:.4f}")
        lines.append(f"- **残差收益**: {results['residual_return']:.4f}")
        lines.append(f"- **R²**: {results['r_squared']:.4f}")
        lines.append("")
        
        return "\n".join(lines)


class MultiPeriodAttribution:
    """多期归因分析器"""
    
    def __init__(self):
        self.period_results: List[Dict] = []
    
    def add_period(self, 
                   date: str,
                   portfolio_return: float,
                   benchmark_return: float,
                   attribution: Dict):
        self.period_results.append({
            'date': date,
            'portfolio_return': portfolio_return,
            'benchmark_return': benchmark_return,
            'excess_return': portfolio_return - benchmark_return,
            'attribution': attribution
        })
    
    def get_cumulative_attribution(self) -> Dict:
        if not self.period_results:
            return {}
        
        cumulative = {
            'portfolio_return': 0,
            'benchmark_return': 0,
            'excess_return': 0,
            'allocation': 0,
            'selection': 0,
            'interaction': 0
        }
        
        for period in self.period_results:
            cumulative['portfolio_return'] = (1 + cumulative['portfolio_return']) * (1 + period['portfolio_return']) - 1
            cumulative['benchmark_return'] = (1 + cumulative['benchmark_return']) * (1 + period['benchmark_return']) - 1
            cumulative['excess_return'] += period['excess_return']
            
            attr = period['attribution']
            cumulative['allocation'] += attr.get('allocation', 0)
            cumulative['selection'] += attr.get('selection', 0)
            cumulative['interaction'] += attr.get('interaction', 0)
        
        return cumulative
    
    def get_rolling_attribution(self, window: int = 12) -> pd.DataFrame:
        if len(self.period_results) < window:
            return pd.DataFrame()
        
        dates = [p['date'] for p in self.period_results]
        
        rolling_data = {
            'date': [],
            'allocation': [],
            'selection': [],
            'interaction': [],
            'excess_return': []
        }
        
        for i in range(window - 1, len(self.period_results)):
            window_results = self.period_results[i - window + 1:i + 1]
            
            rolling_data['date'].append(dates[i])
            rolling_data['allocation'].append(sum(p['attribution'].get('allocation', 0) for p in window_results))
            rolling_data['selection'].append(sum(p['attribution'].get('selection', 0) for p in window_results))
            rolling_data['interaction'].append(sum(p['attribution'].get('interaction', 0) for p in window_results))
            rolling_data['excess_return'].append(sum(p['excess_return'] for p in window_results))
        
        return pd.DataFrame(rolling_data)


if __name__ == '__main__':
    print("=" * 60)
    print("Brinson归因分析模块测试")
    print("=" * 60)
    
    np.random.seed(42)
    n_periods = 12
    categories = ['金融', '科技', '消费', '医药', '工业']
    
    portfolio_weights = pd.DataFrame(
        np.random.dirichlet(np.ones(5), n_periods),
        columns=categories
    )
    
    benchmark_weights = pd.DataFrame(
        np.array([[0.25, 0.25, 0.20, 0.15, 0.15]] * n_periods),
        columns=categories
    )
    
    portfolio_returns = pd.DataFrame(
        np.random.normal(0.02, 0.05, (n_periods, 5)),
        columns=categories
    )
    
    benchmark_returns = pd.DataFrame(
        np.random.normal(0.015, 0.04, (n_periods, 5)),
        columns=categories
    )
    
    brinson = BrinsonAttribution()
    brinson.set_data(portfolio_weights, portfolio_returns, 
                     benchmark_weights, benchmark_returns)
    
    results = brinson.run_attribution()
    
    print("\n归因结果:")
    print(f"  配置效应: {results['summary']['total_allocation']:.4f}")
    print(f"  选择效应: {results['summary']['total_selection']:.4f}")
    print(f"  交互效应: {results['summary']['total_interaction']:.4f}")
    print(f"  超额收益: {results['summary']['total_excess']:.4f}")
    
    print("\n" + brinson.generate_attribution_report(results))
