#!/usr/bin/env python3
"""
基准对比分析模块 - BenchmarkAnalyzer
提供策略与基准的对比分析，包括超额收益、跟踪误差、信息比率等
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import warnings
warnings.filterwarnings('ignore')


class BenchmarkType(Enum):
    """基准类型"""
    CSI300 = "csi300"
    CSI500 = "csi500"
    SSE50 = "sse50"
    CUSTOM = "custom"
    EQUAL_WEIGHT = "equal_weight"


@dataclass
class BenchmarkConfig:
    """基准配置"""
    benchmark_type: BenchmarkType = BenchmarkType.CSI300
    risk_free_rate: float = 0.03
    trading_days_per_year: int = 252


class BenchmarkAnalyzer:
    """基准对比分析器"""
    
    def __init__(self, config: Optional[BenchmarkConfig] = None):
        self.config = config or BenchmarkConfig()
        self.benchmark_returns: Optional[pd.Series] = None
        self.strategy_returns: Optional[pd.Series] = None
        self.excess_returns: Optional[pd.Series] = None
    
    def set_benchmark(self, benchmark_returns: pd.Series):
        self.benchmark_returns = benchmark_returns.copy()
        self._calculate_excess_returns()
    
    def set_benchmark_from_prices(self, benchmark_prices: pd.Series):
        self.benchmark_returns = benchmark_prices.pct_change().dropna()
        self._calculate_excess_returns()
    
    def set_strategy_returns(self, strategy_returns: pd.Series):
        self.strategy_returns = strategy_returns.copy()
        self._calculate_excess_returns()
    
    def set_strategy_from_values(self, portfolio_values: pd.Series):
        self.strategy_returns = portfolio_values.pct_change().dropna()
        self._calculate_excess_returns()
    
    def _calculate_excess_returns(self):
        if self.strategy_returns is not None and self.benchmark_returns is not None:
            aligned_strategy, aligned_benchmark = self.strategy_returns.align(
                self.benchmark_returns, join='inner'
            )
            self.excess_returns = aligned_strategy - aligned_benchmark
    
    def calculate_total_return(self, returns: pd.Series) -> float:
        if returns is None or len(returns) == 0:
            return 0.0
        return (1 + returns).prod() - 1
    
    def calculate_annualized_return(self, returns: pd.Series) -> float:
        if returns is None or len(returns) == 0:
            return 0.0
        total_return = self.calculate_total_return(returns)
        num_days = len(returns)
        return (1 + total_return) ** (self.config.trading_days_per_year / num_days) - 1
    
    def calculate_annualized_volatility(self, returns: pd.Series) -> float:
        if returns is None or len(returns) < 2:
            return 0.0
        return returns.std() * np.sqrt(self.config.trading_days_per_year)
    
    def calculate_tracking_error(self) -> float:
        if self.excess_returns is None or len(self.excess_returns) < 2:
            return 0.0
        return self.excess_returns.std() * np.sqrt(self.config.trading_days_per_year)
    
    def calculate_information_ratio(self) -> float:
        if self.excess_returns is None or len(self.excess_returns) < 2:
            return 0.0
        
        tracking_error = self.calculate_tracking_error()
        if tracking_error == 0:
            return 0.0
        
        excess_return = self.calculate_annualized_return(self.excess_returns)
        return excess_return / tracking_error
    
    def calculate_beta(self) -> float:
        if self.strategy_returns is None or self.benchmark_returns is None:
            return 0.0
        
        aligned_strategy, aligned_benchmark = self.strategy_returns.align(
            self.benchmark_returns, join='inner'
        )
        
        if len(aligned_strategy) < 2:
            return 0.0
        
        covariance = aligned_strategy.cov(aligned_benchmark)
        benchmark_variance = aligned_benchmark.var()
        
        if benchmark_variance == 0:
            return 0.0
        
        return covariance / benchmark_variance
    
    def calculate_alpha(self) -> float:
        if self.strategy_returns is None or self.benchmark_returns is None:
            return 0.0
        
        strategy_return = self.calculate_annualized_return(self.strategy_returns)
        benchmark_return = self.calculate_annualized_return(self.benchmark_returns)
        beta = self.calculate_beta()
        rf = self.config.risk_free_rate
        
        alpha = strategy_return - (rf + beta * (benchmark_return - rf))
        return alpha
    
    def calculate_correlation(self) -> float:
        if self.strategy_returns is None or self.benchmark_returns is None:
            return 0.0
        
        aligned_strategy, aligned_benchmark = self.strategy_returns.align(
            self.benchmark_returns, join='inner'
        )
        
        if len(aligned_strategy) < 2:
            return 0.0
        
        return aligned_strategy.corr(aligned_benchmark)
    
    def calculate_r_squared(self) -> float:
        correlation = self.calculate_correlation()
        return correlation ** 2
    
    def calculate_up_capture(self) -> float:
        if self.strategy_returns is None or self.benchmark_returns is None:
            return 0.0
        
        aligned_strategy, aligned_benchmark = self.strategy_returns.align(
            self.benchmark_returns, join='inner'
        )
        
        up_periods = aligned_benchmark > 0
        if up_periods.sum() == 0:
            return 0.0
        
        strategy_up = aligned_strategy[up_periods].mean()
        benchmark_up = aligned_benchmark[up_periods].mean()
        
        if benchmark_up == 0:
            return 0.0
        
        return strategy_up / benchmark_up
    
    def calculate_down_capture(self) -> float:
        if self.strategy_returns is None or self.benchmark_returns is None:
            return 0.0
        
        aligned_strategy, aligned_benchmark = self.strategy_returns.align(
            self.benchmark_returns, join='inner'
        )
        
        down_periods = aligned_benchmark < 0
        if down_periods.sum() == 0:
            return 0.0
        
        strategy_down = aligned_strategy[down_periods].mean()
        benchmark_down = aligned_benchmark[down_periods].mean()
        
        if benchmark_down == 0:
            return 0.0
        
        return strategy_down / benchmark_down
    
    def calculate_max_drawdown(self, returns: pd.Series) -> float:
        if returns is None or len(returns) == 0:
            return 0.0
        
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        
        return drawdown.min()
    
    def calculate_sharpe_ratio(self, returns: pd.Series) -> float:
        if returns is None or len(returns) < 2:
            return 0.0
        
        annual_return = self.calculate_annualized_return(returns)
        annual_vol = self.calculate_annualized_volatility(returns)
        
        if annual_vol == 0:
            return 0.0
        
        return (annual_return - self.config.risk_free_rate) / annual_vol
    
    def calculate_sortino_ratio(self, returns: pd.Series) -> float:
        if returns is None or len(returns) < 2:
            return 0.0
        
        annual_return = self.calculate_annualized_return(returns)
        downside_returns = returns[returns < 0]
        
        if len(downside_returns) < 2:
            return float('inf') if annual_return > self.config.risk_free_rate else 0.0
        
        downside_vol = downside_returns.std() * np.sqrt(self.config.trading_days_per_year)
        
        if downside_vol == 0:
            return float('inf') if annual_return > self.config.risk_free_rate else 0.0
        
        return (annual_return - self.config.risk_free_rate) / downside_vol
    
    def calculate_treynor_ratio(self) -> float:
        if self.strategy_returns is None:
            return 0.0
        
        annual_return = self.calculate_annualized_return(self.strategy_returns)
        beta = self.calculate_beta()
        
        if beta == 0:
            return 0.0
        
        return (annual_return - self.config.risk_free_rate) / beta
    
    def generate_comparison_report(self) -> Dict:
        if self.strategy_returns is None:
            return {"error": "策略收益数据未设置"}
        
        if self.benchmark_returns is None:
            return {"error": "基准收益数据未设置"}
        
        report = {
            "strategy_metrics": {
                "total_return": self.calculate_total_return(self.strategy_returns),
                "annual_return": self.calculate_annualized_return(self.strategy_returns),
                "annual_volatility": self.calculate_annualized_volatility(self.strategy_returns),
                "sharpe_ratio": self.calculate_sharpe_ratio(self.strategy_returns),
                "sortino_ratio": self.calculate_sortino_ratio(self.strategy_returns),
                "max_drawdown": self.calculate_max_drawdown(self.strategy_returns)
            },
            "benchmark_metrics": {
                "total_return": self.calculate_total_return(self.benchmark_returns),
                "annual_return": self.calculate_annualized_return(self.benchmark_returns),
                "annual_volatility": self.calculate_annualized_volatility(self.benchmark_returns),
                "sharpe_ratio": self.calculate_sharpe_ratio(self.benchmark_returns),
                "max_drawdown": self.calculate_max_drawdown(self.benchmark_returns)
            },
            "relative_metrics": {
                "excess_return": self.calculate_annualized_return(self.excess_returns) if self.excess_returns is not None else 0,
                "tracking_error": self.calculate_tracking_error(),
                "information_ratio": self.calculate_information_ratio(),
                "beta": self.calculate_beta(),
                "alpha": self.calculate_alpha(),
                "correlation": self.calculate_correlation(),
                "r_squared": self.calculate_r_squared(),
                "up_capture": self.calculate_up_capture(),
                "down_capture": self.calculate_down_capture(),
                "treynor_ratio": self.calculate_treynor_ratio()
            }
        }
        
        return report
    
    def format_report_markdown(self, report: Optional[Dict] = None) -> str:
        if report is None:
            report = self.generate_comparison_report()
        
        if "error" in report:
            return f"# 基准对比分析报告\n\n错误: {report['error']}"
        
        lines = []
        lines.append("# 基准对比分析报告")
        lines.append("")
        lines.append("## 策略绩效指标")
        lines.append("")
        lines.append("| 指标 | 数值 |")
        lines.append("|------|------|")
        
        sm = report["strategy_metrics"]
        lines.append(f"| 总收益率 | {sm['total_return']:.2%} |")
        lines.append(f"| 年化收益率 | {sm['annual_return']:.2%} |")
        lines.append(f"| 年化波动率 | {sm['annual_volatility']:.2%} |")
        lines.append(f"| 夏普比率 | {sm['sharpe_ratio']:.2f} |")
        lines.append(f"| 索提诺比率 | {sm['sortino_ratio']:.2f} |")
        lines.append(f"| 最大回撤 | {sm['max_drawdown']:.2%} |")
        lines.append("")
        
        lines.append("## 基准绩效指标")
        lines.append("")
        lines.append("| 指标 | 数值 |")
        lines.append("|------|------|")
        
        bm = report["benchmark_metrics"]
        lines.append(f"| 总收益率 | {bm['total_return']:.2%} |")
        lines.append(f"| 年化收益率 | {bm['annual_return']:.2%} |")
        lines.append(f"| 年化波动率 | {bm['annual_volatility']:.2%} |")
        lines.append(f"| 夏普比率 | {bm['sharpe_ratio']:.2f} |")
        lines.append(f"| 最大回撤 | {bm['max_drawdown']:.2%} |")
        lines.append("")
        
        lines.append("## 相对绩效指标")
        lines.append("")
        lines.append("| 指标 | 数值 | 说明 |")
        lines.append("|------|------|------|")
        
        rm = report["relative_metrics"]
        lines.append(f"| 超额收益 | {rm['excess_return']:.2%} | 策略相对基准的超额收益 |")
        lines.append(f"| 跟踪误差 | {rm['tracking_error']:.2%} | 策略与基准收益的偏离程度 |")
        lines.append(f"| 信息比率 | {rm['information_ratio']:.2f} | 超额收益/跟踪误差 |")
        lines.append(f"| Beta | {rm['beta']:.2f} | 策略对基准的敏感度 |")
        lines.append(f"| Alpha | {rm['alpha']:.2%} | 经风险调整后的超额收益 |")
        lines.append(f"| 相关系数 | {rm['correlation']:.2f} | 与基准的相关性 |")
        lines.append(f"| R² | {rm['r_squared']:.2f} | 基准解释策略收益的比例 |")
        lines.append(f"| 上行捕获比 | {rm['up_capture']:.2f} | 基准上涨时策略的相对表现 |")
        lines.append(f"| 下行捕获比 | {rm['down_capture']:.2f} | 基准下跌时策略的相对表现 |")
        lines.append(f"| 特雷诺比率 | {rm['treynor_ratio']:.2f} | 单位系统性风险的超额收益 |")
        lines.append("")
        
        lines.append("## 分析结论")
        lines.append("")
        
        if rm['information_ratio'] > 0.5:
            lines.append("- ✅ 信息比率良好，策略具有较好的超额收益能力")
        else:
            lines.append("- ⚠️ 信息比率偏低，策略超额收益能力不足")
        
        if rm['beta'] < 0.8:
            lines.append("- ✅ Beta较低，策略相对基准独立性强")
        elif rm['beta'] > 1.2:
            lines.append("- ⚠️ Beta较高，策略波动性大于基准")
        else:
            lines.append("- ✅ Beta适中，策略与基准相关性合理")
        
        if rm['up_capture'] > 1.0 and rm['down_capture'] < 1.0:
            lines.append("- ✅ 上行捕获比>1且下行捕获比<1，策略具有良好的市场适应性")
        elif rm['up_capture'] > rm['down_capture']:
            lines.append("- ✅ 上行捕获比大于下行捕获比，策略在上涨市场表现更好")
        
        if rm['alpha'] > 0.02:
            lines.append("- ✅ Alpha为正，策略具有超额收益能力")
        elif rm['alpha'] < -0.02:
            lines.append("- ⚠️ Alpha为负，策略表现弱于风险调整后的基准")
        
        return "\n".join(lines)


def create_benchmark_from_index(index_data: pd.DataFrame, 
                                index_name: str = 'csi300') -> pd.Series:
    if 'close' not in index_data.columns:
        raise ValueError("指数数据必须包含'close'列")
    
    if 'date' in index_data.columns:
        index_data = index_data.set_index('date')
    
    returns = index_data['close'].pct_change().dropna()
    returns.name = index_name
    
    return returns


if __name__ == '__main__':
    print("=" * 60)
    print("基准对比分析模块测试")
    print("=" * 60)
    
    np.random.seed(42)
    dates = pd.date_range('2023-01-01', periods=252, freq='B')
    
    benchmark_returns = pd.Series(
        np.random.normal(0.0004, 0.015, 252),
        index=dates,
        name='benchmark'
    )
    
    alpha = 0.0002
    beta = 0.9
    strategy_returns = alpha + beta * benchmark_returns + np.random.normal(0, 0.008, 252)
    strategy_returns = pd.Series(strategy_returns, index=dates, name='strategy')
    
    analyzer = BenchmarkAnalyzer()
    analyzer.set_benchmark(benchmark_returns)
    analyzer.set_strategy_returns(strategy_returns)
    
    report = analyzer.generate_comparison_report()
    
    print("\n策略绩效:")
    for key, value in report['strategy_metrics'].items():
        print(f"  {key}: {value:.4f}")
    
    print("\n基准绩效:")
    for key, value in report['benchmark_metrics'].items():
        print(f"  {key}: {value:.4f}")
    
    print("\n相对绩效:")
    for key, value in report['relative_metrics'].items():
        print(f"  {key}: {value:.4f}")
    
    print("\n" + analyzer.format_report_markdown(report))
