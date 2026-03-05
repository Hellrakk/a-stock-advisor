#!/usr/bin/env python3
"""
滚动绩效分析模块 - RollingPerformanceAnalyzer
提供滚动窗口的绩效指标计算，分析策略稳定性
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import warnings
warnings.filterwarnings('ignore')


class RollingMetricType(Enum):
    """滚动指标类型"""
    RETURN = "return"
    VOLATILITY = "volatility"
    SHARPE = "sharpe"
    MAX_DRAWDOWN = "max_drawdown"
    WIN_RATE = "win_rate"
    CALMAR = "calmar"
    SORTINO = "sortino"
    BETA = "beta"
    ALPHA = "alpha"
    INFORMATION_RATIO = "information_ratio"
    TRACKING_ERROR = "tracking_error"


@dataclass
class RollingConfig:
    """滚动分析配置"""
    window: int = 63
    min_periods: int = 20
    risk_free_rate: float = 0.03
    trading_days_per_year: int = 252


class RollingPerformanceAnalyzer:
    """滚动绩效分析器"""
    
    def __init__(self, config: Optional[RollingConfig] = None):
        self.config = config or RollingConfig()
        self.returns: Optional[pd.Series] = None
        self.benchmark_returns: Optional[pd.Series] = None
    
    def set_returns(self, returns: pd.Series):
        self.returns = returns.copy()
    
    def set_returns_from_values(self, portfolio_values: pd.Series):
        self.returns = portfolio_values.pct_change().dropna()
    
    def set_benchmark(self, benchmark_returns: pd.Series):
        self.benchmark_returns = benchmark_returns.copy()
    
    def _annualize_return(self, returns: pd.Series) -> float:
        if len(returns) == 0:
            return 0.0
        total_return = (1 + returns).prod() - 1
        n_days = len(returns)
        return (1 + total_return) ** (self.config.trading_days_per_year / n_days) - 1
    
    def _annualize_volatility(self, returns: pd.Series) -> float:
        if len(returns) < 2:
            return 0.0
        return returns.std() * np.sqrt(self.config.trading_days_per_year)
    
    def rolling_return(self) -> pd.Series:
        if self.returns is None:
            return pd.Series()
        
        def calc_return(x):
            if len(x) < self.config.min_periods:
                return np.nan
            return self._annualize_return(x)
        
        return self.returns.rolling(
            window=self.config.window,
            min_periods=self.config.min_periods
        ).apply(calc_return)
    
    def rolling_volatility(self) -> pd.Series:
        if self.returns is None:
            return pd.Series()
        
        return self.returns.rolling(
            window=self.config.window,
            min_periods=self.config.min_periods
        ).std() * np.sqrt(self.config.trading_days_per_year)
    
    def rolling_sharpe(self) -> pd.Series:
        if self.returns is None:
            return pd.Series()
        
        rolling_ret = self.rolling_return()
        rolling_vol = self.rolling_volatility()
        
        sharpe = (rolling_ret - self.config.risk_free_rate) / rolling_vol
        sharpe = sharpe.replace([np.inf, -np.inf], np.nan)
        
        return sharpe
    
    def rolling_max_drawdown(self) -> pd.Series:
        if self.returns is None:
            return pd.Series()
        
        def calc_max_dd(x):
            if len(x) < self.config.min_periods:
                return np.nan
            cumulative = (1 + x).cumprod()
            running_max = cumulative.expanding().max()
            drawdown = (cumulative - running_max) / running_max
            return drawdown.min()
        
        return self.returns.rolling(
            window=self.config.window,
            min_periods=self.config.min_periods
        ).apply(calc_max_dd)
    
    def rolling_win_rate(self) -> pd.Series:
        if self.returns is None:
            return pd.Series()
        
        def calc_win_rate(x):
            if len(x) < self.config.min_periods:
                return np.nan
            return (x > 0).sum() / len(x)
        
        return self.returns.rolling(
            window=self.config.window,
            min_periods=self.config.min_periods
        ).apply(calc_win_rate)
    
    def rolling_calmar(self) -> pd.Series:
        if self.returns is None:
            return pd.Series()
        
        rolling_ret = self.rolling_return()
        rolling_dd = self.rolling_max_drawdown()
        
        calmar = rolling_ret / rolling_dd.abs()
        calmar = calmar.replace([np.inf, -np.inf], np.nan)
        
        return calmar
    
    def rolling_sortino(self) -> pd.Series:
        if self.returns is None:
            return pd.Series()
        
        def calc_sortino(x):
            if len(x) < self.config.min_periods:
                return np.nan
            
            annual_ret = self._annualize_return(x)
            downside = x[x < 0]
            
            if len(downside) < 2:
                return np.nan if annual_ret <= self.config.risk_free_rate else float('inf')
            
            downside_vol = downside.std() * np.sqrt(self.config.trading_days_per_year)
            
            if downside_vol == 0:
                return float('inf') if annual_ret > self.config.risk_free_rate else 0
            
            return (annual_ret - self.config.risk_free_rate) / downside_vol
        
        return self.returns.rolling(
            window=self.config.window,
            min_periods=self.config.min_periods
        ).apply(calc_sortino)
    
    def rolling_beta(self) -> pd.Series:
        if self.returns is None or self.benchmark_returns is None:
            return pd.Series()
        
        aligned_returns, aligned_benchmark = self.returns.align(
            self.benchmark_returns, join='inner'
        )
        
        def calc_beta(window_returns):
            if len(window_returns) < self.config.min_periods:
                return np.nan
            
            idx = window_returns.index
            window_benchmark = aligned_benchmark.loc[idx]
            
            cov = window_returns.cov(window_benchmark)
            var = window_benchmark.var()
            
            if var == 0:
                return np.nan
            
            return cov / var
        
        return aligned_returns.rolling(
            window=self.config.window,
            min_periods=self.config.min_periods
        ).apply(calc_beta)
    
    def rolling_alpha(self) -> pd.Series:
        if self.returns is None or self.benchmark_returns is None:
            return pd.Series()
        
        rolling_ret = self.rolling_return()
        
        aligned_returns, aligned_benchmark = self.returns.align(
            self.benchmark_returns, join='inner'
        )
        
        def calc_benchmark_return(x):
            if len(x) < self.config.min_periods:
                return np.nan
            return self._annualize_return(x)
        
        rolling_benchmark_ret = aligned_benchmark.rolling(
            window=self.config.window,
            min_periods=self.config.min_periods
        ).apply(calc_benchmark_return)
        
        rolling_beta = self.rolling_beta()
        
        alpha = rolling_ret - (self.config.risk_free_rate + 
                               rolling_beta * (rolling_benchmark_ret - self.config.risk_free_rate))
        
        return alpha
    
    def rolling_tracking_error(self) -> pd.Series:
        if self.returns is None or self.benchmark_returns is None:
            return pd.Series()
        
        aligned_returns, aligned_benchmark = self.returns.align(
            self.benchmark_returns, join='inner'
        )
        
        excess_returns = aligned_returns - aligned_benchmark
        
        return excess_returns.rolling(
            window=self.config.window,
            min_periods=self.config.min_periods
        ).std() * np.sqrt(self.config.trading_days_per_year)
    
    def rolling_information_ratio(self) -> pd.Series:
        if self.returns is None or self.benchmark_returns is None:
            return pd.Series()
        
        aligned_returns, aligned_benchmark = self.returns.align(
            self.benchmark_returns, join='inner'
        )
        
        def calc_ir(window_returns):
            if len(window_returns) < self.config.min_periods:
                return np.nan
            
            idx = window_returns.index
            window_benchmark = aligned_benchmark.loc[idx]
            
            excess = window_returns - window_benchmark
            excess_ret = self._annualize_return(excess)
            tracking_error = excess.std() * np.sqrt(self.config.trading_days_per_year)
            
            if tracking_error == 0:
                return np.nan
            
            return excess_ret / tracking_error
        
        return aligned_returns.rolling(
            window=self.config.window,
            min_periods=self.config.min_periods
        ).apply(calc_ir)
    
    def calculate_all_rolling_metrics(self) -> Dict[str, pd.Series]:
        metrics = {
            'rolling_return': self.rolling_return(),
            'rolling_volatility': self.rolling_volatility(),
            'rolling_sharpe': self.rolling_sharpe(),
            'rolling_max_drawdown': self.rolling_max_drawdown(),
            'rolling_win_rate': self.rolling_win_rate(),
            'rolling_calmar': self.rolling_calmar(),
            'rolling_sortino': self.rolling_sortino()
        }
        
        if self.benchmark_returns is not None:
            metrics.update({
                'rolling_beta': self.rolling_beta(),
                'rolling_alpha': self.rolling_alpha(),
                'rolling_tracking_error': self.rolling_tracking_error(),
                'rolling_information_ratio': self.rolling_information_ratio()
            })
        
        return metrics
    
    def get_metric_statistics(self, metric_series: pd.Series) -> Dict:
        clean_series = metric_series.dropna()
        
        if len(clean_series) == 0:
            return {
                'mean': np.nan,
                'std': np.nan,
                'min': np.nan,
                'max': np.nan,
                'median': np.nan,
                'q25': np.nan,
                'q75': np.nan
            }
        
        return {
            'mean': clean_series.mean(),
            'std': clean_series.std(),
            'min': clean_series.min(),
            'max': clean_series.max(),
            'median': clean_series.median(),
            'q25': clean_series.quantile(0.25),
            'q75': clean_series.quantile(0.75)
        }
    
    def analyze_stability(self) -> Dict:
        metrics = self.calculate_all_rolling_metrics()
        
        stability_analysis = {}
        
        for name, series in metrics.items():
            stats = self.get_metric_statistics(series)
            
            cv = stats['std'] / abs(stats['mean']) if stats['mean'] != 0 else float('inf')
            
            positive_ratio = (series > 0).sum() / len(series.dropna()) if len(series.dropna()) > 0 else 0
            
            stability_analysis[name] = {
                'statistics': stats,
                'coefficient_of_variation': cv,
                'positive_ratio': positive_ratio,
                'stability_score': self._calculate_stability_score(cv, positive_ratio, name)
            }
        
        return stability_analysis
    
    def _calculate_stability_score(self, cv: float, positive_ratio: float, 
                                   metric_name: str) -> float:
        if 'drawdown' in metric_name:
            return positive_ratio * 0.5 + (1 - min(cv, 1)) * 0.5
        elif 'volatility' in metric_name or 'tracking_error' in metric_name:
            return (1 - min(cv, 1)) * 0.8 + positive_ratio * 0.2
        else:
            return positive_ratio * 0.6 + (1 - min(cv, 1)) * 0.4
    
    def generate_rolling_report(self) -> str:
        metrics = self.calculate_all_rolling_metrics()
        stability = self.analyze_stability()
        
        lines = []
        lines.append("# 滚动绩效分析报告")
        lines.append("")
        lines.append(f"**滚动窗口**: {self.config.window}个交易日")
        lines.append(f"**最小周期**: {self.config.min_periods}个交易日")
        lines.append("")
        
        lines.append("## 滚动指标统计")
        lines.append("")
        lines.append("| 指标 | 均值 | 标准差 | 最小值 | 最大值 | 中位数 |")
        lines.append("|------|------|--------|--------|--------|--------|")
        
        metric_names = {
            'rolling_return': '滚动收益率',
            'rolling_volatility': '滚动波动率',
            'rolling_sharpe': '滚动夏普比率',
            'rolling_max_drawdown': '滚动最大回撤',
            'rolling_win_rate': '滚动胜率',
            'rolling_calmar': '滚动卡玛比率',
            'rolling_sortino': '滚动索提诺比率',
            'rolling_beta': '滚动Beta',
            'rolling_alpha': '滚动Alpha',
            'rolling_tracking_error': '滚动跟踪误差',
            'rolling_information_ratio': '滚动信息比率'
        }
        
        for name, series in metrics.items():
            stats = stability[name]['statistics']
            display_name = metric_names.get(name, name)
            lines.append(f"| {display_name} | {stats['mean']:.4f} | {stats['std']:.4f} | "
                        f"{stats['min']:.4f} | {stats['max']:.4f} | {stats['median']:.4f} |")
        
        lines.append("")
        
        lines.append("## 稳定性分析")
        lines.append("")
        lines.append("| 指标 | 变异系数 | 正值比例 | 稳定性得分 |")
        lines.append("|------|----------|----------|------------|")
        
        for name, analysis in stability.items():
            display_name = metric_names.get(name, name)
            score = analysis['stability_score']
            lines.append(f"| {display_name} | {analysis['coefficient_of_variation']:.4f} | "
                        f"{analysis['positive_ratio']:.2%} | {score:.2f} |")
        
        lines.append("")
        
        lines.append("## 分析结论")
        lines.append("")
        
        sharpe_stability = stability.get('rolling_sharpe', {}).get('stability_score', 0)
        if sharpe_stability > 0.7:
            lines.append("- ✅ 夏普比率稳定性良好，策略表现一致")
        elif sharpe_stability > 0.5:
            lines.append("- ⚠️ 夏普比率稳定性一般，策略表现有一定波动")
        else:
            lines.append("- ❌ 夏普比率稳定性较差，策略表现不稳定")
        
        dd_stats = stability.get('rolling_max_drawdown', {}).get('statistics', {})
        if dd_stats.get('mean', 0) > -0.1:
            lines.append("- ✅ 滚动最大回撤控制良好")
        elif dd_stats.get('mean', 0) > -0.2:
            lines.append("- ⚠️ 滚动最大回撤处于可接受范围")
        else:
            lines.append("- ❌ 滚动最大回撤较大，风险控制需要改进")
        
        win_rate_stats = stability.get('rolling_win_rate', {}).get('statistics', {})
        if win_rate_stats.get('mean', 0) > 0.55:
            lines.append("- ✅ 滚动胜率较高，选股能力优秀")
        elif win_rate_stats.get('mean', 0) > 0.5:
            lines.append("- ⚠️ 滚动胜率略高于50%，选股能力一般")
        else:
            lines.append("- ❌ 滚动胜率低于50%，选股能力需要提升")
        
        return "\n".join(lines)


class PerformanceDecomposition:
    """绩效分解分析器"""
    
    def __init__(self):
        self.returns: Optional[pd.Series] = None
    
    def set_returns(self, returns: pd.Series):
        self.returns = returns.copy()
    
    def decompose_by_period(self) -> Dict:
        if self.returns is None:
            return {}
        
        returns_df = pd.DataFrame({'return': self.returns})
        
        if isinstance(self.returns.index, pd.DatetimeIndex):
            returns_df['year'] = self.returns.index.year
            returns_df['month'] = self.returns.index.month
            returns_df['quarter'] = self.returns.index.quarter
            returns_df['weekday'] = self.returns.index.dayofweek
        
        decomposition = {}
        
        if 'year' in returns_df.columns:
            yearly = returns_df.groupby('year')['return'].apply(
                lambda x: (1 + x).prod() - 1
            )
            decomposition['yearly'] = yearly.to_dict()
        
        if 'month' in returns_df.columns:
            monthly = returns_df.groupby(['year', 'month'])['return'].apply(
                lambda x: (1 + x).prod() - 1
            )
            decomposition['monthly'] = monthly.to_dict()
        
        if 'quarter' in returns_df.columns:
            quarterly = returns_df.groupby(['year', 'quarter'])['return'].apply(
                lambda x: (1 + x).prod() - 1
            )
            decomposition['quarterly'] = quarterly.to_dict()
        
        if 'weekday' in returns_df.columns:
            weekday = returns_df.groupby('weekday')['return'].mean()
            decomposition['weekday'] = weekday.to_dict()
        
        return decomposition
    
    def decompose_by_market_regime(self, 
                                   market_returns: pd.Series,
                                   threshold: float = 0.0) -> Dict:
        if self.returns is None:
            return {}
        
        aligned_returns, aligned_market = self.returns.align(
            market_returns, join='inner'
        )
        
        up_market = aligned_market > threshold
        down_market = aligned_market <= threshold
        
        up_returns = aligned_returns[up_market]
        down_returns = aligned_returns[down_market]
        
        return {
            'up_market': {
                'count': len(up_returns),
                'mean_return': up_returns.mean() if len(up_returns) > 0 else 0,
                'total_return': (1 + up_returns).prod() - 1 if len(up_returns) > 0 else 0,
                'win_rate': (up_returns > 0).mean() if len(up_returns) > 0 else 0
            },
            'down_market': {
                'count': len(down_returns),
                'mean_return': down_returns.mean() if len(down_returns) > 0 else 0,
                'total_return': (1 + down_returns).prod() - 1 if len(down_returns) > 0 else 0,
                'win_rate': (down_returns > 0).mean() if len(down_returns) > 0 else 0
            }
        }


if __name__ == '__main__':
    print("=" * 60)
    print("滚动绩效分析模块测试")
    print("=" * 60)
    
    np.random.seed(42)
    dates = pd.date_range('2023-01-01', periods=252, freq='B')
    
    returns = pd.Series(
        np.random.normal(0.0005, 0.015, 252),
        index=dates
    )
    
    benchmark_returns = pd.Series(
        np.random.normal(0.0004, 0.012, 252),
        index=dates
    )
    
    analyzer = RollingPerformanceAnalyzer(RollingConfig(window=63, min_periods=20))
    analyzer.set_returns(returns)
    analyzer.set_benchmark(benchmark_returns)
    
    print("\n计算滚动指标...")
    metrics = analyzer.calculate_all_rolling_metrics()
    
    for name, series in metrics.items():
        valid_count = series.notna().sum()
        print(f"  {name}: {valid_count} 个有效值")
    
    print("\n稳定性分析:")
    stability = analyzer.analyze_stability()
    for name, analysis in stability.items():
        print(f"  {name}: 稳定性得分 = {analysis['stability_score']:.2f}")
    
    print("\n" + analyzer.generate_rolling_report())
