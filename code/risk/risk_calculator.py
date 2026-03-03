#!/usr/bin/env python3
"""
风险计算模块
基于真实历史数据计算波动率、Beta、VaR、回撤等风险指标
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
from scipy import stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RiskCalculator:
    """风险指标计算器"""
    
    TRADING_DAYS_PER_YEAR = 252
    
    def __init__(self):
        self._cache = {}
    
    def calculate_volatility(self, returns: List[float], annualize: bool = True) -> float:
        """
        计算波动率
        
        Args:
            returns: 收益率序列
            annualize: 是否年化
            
        Returns:
            波动率（百分比）
        """
        if not returns or len(returns) < 2:
            return 0.0
        
        returns = np.array(returns)
        std = np.std(returns, ddof=1)
        
        if annualize:
            std = std * np.sqrt(self.TRADING_DAYS_PER_YEAR)
        
        return float(std * 100)
    
    def calculate_stock_volatility(self, prices: List[float], annualize: bool = True) -> float:
        """
        基于价格序列计算波动率
        
        Args:
            prices: 价格序列
            annualize: 是否年化
            
        Returns:
            波动率（百分比）
        """
        if not prices or len(prices) < 2:
            return 0.0
        
        prices = np.array(prices)
        returns = np.diff(np.log(prices[prices > 0]))
        
        return self.calculate_volatility(returns.tolist(), annualize)
    
    def calculate_beta(
        self, 
        stock_returns: List[float], 
        benchmark_returns: List[float]
    ) -> float:
        """
        计算Beta值
        
        Args:
            stock_returns: 股票收益率序列
            benchmark_returns: 基准收益率序列
            
        Returns:
            Beta值
        """
        if (not stock_returns or not benchmark_returns or 
            len(stock_returns) < 2 or len(benchmark_returns) < 2):
            return 1.0
        
        min_len = min(len(stock_returns), len(benchmark_returns))
        stock_returns = np.array(stock_returns[-min_len:])
        benchmark_returns = np.array(benchmark_returns[-min_len:])
        
        covariance = np.cov(stock_returns, benchmark_returns)[0, 1]
        benchmark_variance = np.var(benchmark_returns, ddof=1)
        
        if benchmark_variance == 0:
            return 1.0
        
        beta = covariance / benchmark_variance
        return float(beta)
    
    def calculate_var(
        self, 
        returns: List[float], 
        confidence: float = 0.95,
        method: str = 'historical'
    ) -> float:
        """
        计算VaR（在险价值）
        
        Args:
            returns: 收益率序列
            confidence: 置信水平
            method: 计算方法 ('historical', 'parametric')
            
        Returns:
            VaR值（百分比，正数表示潜在损失）
        """
        if not returns or len(returns) < 10:
            return 0.0
        
        returns = np.array(returns)
        
        if method == 'historical':
            var = np.percentile(returns, (1 - confidence) * 100)
        else:
            mean = np.mean(returns)
            std = np.std(returns, ddof=1)
            z_score = stats.norm.ppf(1 - confidence)
            var = mean + z_score * std
        
        return float(abs(var * 100))
    
    def calculate_cvar(
        self, 
        returns: List[float], 
        confidence: float = 0.95
    ) -> float:
        """
        计算CVaR（条件在险价值/期望损失）
        
        Args:
            returns: 收益率序列
            confidence: 置信水平
            
        Returns:
            CVaR值（百分比）
        """
        if not returns or len(returns) < 10:
            return 0.0
        
        returns = np.array(returns)
        var = np.percentile(returns, (1 - confidence) * 100)
        cvar = np.mean(returns[returns <= var])
        
        return float(abs(cvar * 100))
    
    def calculate_max_drawdown(self, values: List[float]) -> Tuple[float, float, int]:
        """
        计算最大回撤
        
        Args:
            values: 净值/价格序列
            
        Returns:
            (最大回撤百分比, 当前回撤百分比, 回撤持续天数)
        """
        if not values or len(values) < 2:
            return 0.0, 0.0, 0
        
        values = np.array(values)
        cumulative_max = np.maximum.accumulate(values)
        
        drawdowns = (cumulative_max - values) / cumulative_max
        drawdowns = np.nan_to_num(drawdowns, nan=0.0, posinf=0.0, neginf=0.0)
        
        max_dd = float(np.max(drawdowns) * 100)
        current_dd = float(drawdowns[-1] * 100)
        
        max_dd_idx = np.argmax(drawdowns)
        peak_idx = np.argmax(values[:max_dd_idx + 1]) if max_dd_idx > 0 else 0
        dd_duration = int(max_dd_idx - peak_idx)
        
        return max_dd, current_dd, dd_duration
    
    def calculate_sharpe_ratio(
        self, 
        returns: List[float], 
        risk_free_rate: float = 0.03
    ) -> float:
        """
        计算夏普比率
        
        Args:
            returns: 收益率序列
            risk_free_rate: 无风险利率（年化）
            
        Returns:
            夏普比率
        """
        if not returns or len(returns) < 2:
            return 0.0
        
        returns = np.array(returns)
        excess_returns = returns - risk_free_rate / self.TRADING_DAYS_PER_YEAR
        
        std = np.std(excess_returns, ddof=1)
        if std == 0:
            return 0.0
        
        mean_excess = np.mean(excess_returns)
        sharpe = mean_excess / std * np.sqrt(self.TRADING_DAYS_PER_YEAR)
        
        return float(sharpe)
    
    def calculate_sortino_ratio(
        self, 
        returns: List[float], 
        risk_free_rate: float = 0.03
    ) -> float:
        """
        计算索提诺比率
        
        Args:
            returns: 收益率序列
            risk_free_rate: 无风险利率（年化）
            
        Returns:
            索提诺比率
        """
        if not returns or len(returns) < 2:
            return 0.0
        
        returns = np.array(returns)
        excess_returns = returns - risk_free_rate / self.TRADING_DAYS_PER_YEAR
        
        downside_returns = excess_returns[excess_returns < 0]
        if len(downside_returns) == 0:
            return float('inf')
        
        downside_std = np.std(downside_returns, ddof=1)
        if downside_std == 0:
            return 0.0
        
        mean_excess = np.mean(excess_returns)
        sortino = mean_excess / downside_std * np.sqrt(self.TRADING_DAYS_PER_YEAR)
        
        return float(sortino)
    
    def calculate_win_rate(self, returns: List[float]) -> float:
        """
        计算胜率
        
        Args:
            returns: 收益率序列
            
        Returns:
            胜率（百分比）
        """
        if not returns:
            return 0.0
        
        returns = np.array(returns)
        win_count = np.sum(returns > 0)
        
        return float(win_count / len(returns) * 100)
    
    def calculate_profit_loss_ratio(self, returns: List[float]) -> float:
        """
        计算盈亏比
        
        Args:
            returns: 收益率序列
            
        Returns:
            盈亏比
        """
        if not returns:
            return 0.0
        
        returns = np.array(returns)
        profits = returns[returns > 0]
        losses = returns[returns < 0]
        
        if len(losses) == 0:
            return float('inf')
        if len(profits) == 0:
            return 0.0
        
        avg_profit = np.mean(profits)
        avg_loss = abs(np.mean(losses))
        
        if avg_loss == 0:
            return float('inf')
        
        return float(avg_profit / avg_loss)
    
    def calculate_portfolio_risk(
        self,
        positions: List[Dict],
        stock_histories: Dict[str, List[float]],
        benchmark_history: List[float]
    ) -> Dict:
        """
        计算组合风险指标
        
        Args:
            positions: 持仓列表 [{'code': xxx, 'weight': xxx}, ...]
            stock_histories: 各股票历史价格 {code: [prices...]}
            benchmark_history: 基准历史价格
            
        Returns:
            组合风险指标字典
        """
        if not positions or not stock_histories:
            return self._empty_risk_result()
        
        portfolio_values = []
        stock_returns = {}
        
        min_len = min(len(h) for h in stock_histories.values() if len(h) > 0)
        min_len = min(min_len, len(benchmark_history)) if benchmark_history else min_len
        
        if min_len < 2:
            return self._empty_risk_result()
        
        for code, prices in stock_histories.items():
            prices = prices[-min_len:]
            returns = np.diff(np.log(np.array(prices[prices > 0]))).tolist()
            stock_returns[code] = returns
        
        benchmark_returns = []
        if benchmark_history and len(benchmark_history) >= min_len:
            bench_prices = benchmark_history[-min_len:]
            benchmark_returns = np.diff(np.log(np.array(bench_prices[bench_prices > 0]))).tolist()
        
        portfolio_returns = []
        for i in range(min_len - 1):
            daily_return = 0
            for pos in positions:
                code = pos['code']
                weight = pos['weight']
                if code in stock_returns and i < len(stock_returns[code]):
                    daily_return += stock_returns[code][i] * weight
            portfolio_returns.append(daily_return)
        
        volatility = self.calculate_volatility(portfolio_returns)
        
        beta = 1.0
        if benchmark_returns:
            beta = self.calculate_beta(portfolio_returns, benchmark_returns)
        
        var_95 = self.calculate_var(portfolio_returns, 0.95)
        var_99 = self.calculate_var(portfolio_returns, 0.99)
        cvar_95 = self.calculate_cvar(portfolio_returns, 0.95)
        
        sharpe = self.calculate_sharpe_ratio(portfolio_returns)
        sortino = self.calculate_sortino_ratio(portfolio_returns)
        
        win_rate = self.calculate_win_rate(portfolio_returns)
        profit_loss_ratio = self.calculate_profit_loss_ratio(portfolio_returns)
        
        cumulative_values = [1.0]
        for r in portfolio_returns:
            cumulative_values.append(cumulative_values[-1] * (1 + r))
        
        max_dd, current_dd, dd_duration = self.calculate_max_drawdown(cumulative_values)
        
        return {
            'volatility': round(volatility, 2),
            'beta': round(beta, 2),
            'var_95': round(var_95, 2),
            'var_99': round(var_99, 2),
            'cvar_95': round(cvar_95, 2),
            'max_drawdown': round(max_dd, 2),
            'current_drawdown': round(current_dd, 2),
            'drawdown_duration': dd_duration,
            'sharpe_ratio': round(sharpe, 2),
            'sortino_ratio': round(sortino, 2),
            'win_rate': round(win_rate, 2),
            'profit_loss_ratio': round(profit_loss_ratio, 2),
            'annual_return': round(np.mean(portfolio_returns) * self.TRADING_DAYS_PER_YEAR * 100, 2)
        }
    
    def calculate_single_stock_risk(
        self,
        prices: List[float],
        benchmark_prices: List[float] = None
    ) -> Dict:
        """
        计算单只股票的风险指标
        
        Args:
            prices: 股票历史价格
            benchmark_prices: 基准历史价格
            
        Returns:
            股票风险指标字典
        """
        if not prices or len(prices) < 2:
            return {
                'volatility': 0,
                'beta': 1.0,
                'var_95': 0,
                'max_drawdown': 0
            }
        
        prices = np.array(prices)
        returns = np.diff(np.log(prices[prices > 0])).tolist()
        
        volatility = self.calculate_volatility(returns)
        var_95 = self.calculate_var(returns, 0.95)
        max_dd, current_dd, _ = self.calculate_max_drawdown(prices.tolist())
        
        beta = 1.0
        if benchmark_prices and len(benchmark_prices) >= len(prices):
            bench_prices = np.array(benchmark_prices[-len(prices):])
            bench_returns = np.diff(np.log(bench_prices[bench_prices > 0])).tolist()
            beta = self.calculate_beta(returns, bench_returns)
        
        return {
            'volatility': round(volatility, 2),
            'beta': round(beta, 2),
            'var_95': round(var_95, 2),
            'max_drawdown': round(max_dd, 2),
            'current_drawdown': round(current_dd, 2)
        }
    
    def _empty_risk_result(self) -> Dict:
        """返回空的风险结果"""
        return {
            'volatility': 0,
            'beta': 1.0,
            'var_95': 0,
            'var_99': 0,
            'cvar_95': 0,
            'max_drawdown': 0,
            'current_drawdown': 0,
            'drawdown_duration': 0,
            'sharpe_ratio': 0,
            'sortino_ratio': 0,
            'win_rate': 0,
            'profit_loss_ratio': 0,
            'annual_return': 0
        }


class FactorRiskModel:
    """因子风险模型"""
    
    def __init__(self, lookback_period: int = 252):
        """
        初始化因子风险模型
        
        Args:
            lookback_period: 回溯期间（交易日）
        """
        self.lookback_period = lookback_period
        self.factor_returns = {}
        self.factor_covariance = None
        self.idio_volatility = {}
        self.factor_exposure_history = []
    
    def estimate_factor_returns(
        self,
        stock_returns: pd.DataFrame,
        factor_exposures: pd.DataFrame
    ) -> pd.Series:
        """
        估计因子收益率
        
        Args:
            stock_returns: 股票收益率 DataFrame (index: stock_code)
            factor_exposures: 因子暴露 DataFrame (index: stock_code, columns: factors)
            
        Returns:
            因子收益率 Series
        """
        common_stocks = stock_returns.index.intersection(factor_exposures.index)
        
        if len(common_stocks) < 20:
            return pd.Series()
        
        Y = stock_returns.loc[common_stocks].values
        X = factor_exposures.loc[common_stocks].values
        
        X = np.column_stack([np.ones(len(X)), X])
        
        try:
            beta = np.linalg.lstsq(X, Y, rcond=None)[0]
            factor_names = ['intercept'] + list(factor_exposures.columns)
            return pd.Series(beta[1:], index=factor_exposures.columns)
        except:
            return pd.Series()
    
    def estimate_factor_covariance(
        self,
        factor_returns_history: pd.DataFrame
    ) -> pd.DataFrame:
        """
        估计因子协方差矩阵
        
        Args:
            factor_returns_history: 因子收益率历史 DataFrame (index: date, columns: factors)
            
        Returns:
            因子协方差矩阵 DataFrame
        """
        if len(factor_returns_history) < 20:
            return pd.DataFrame()
        
        returns = factor_returns_history.tail(self.lookback_period)
        cov_matrix = returns.cov()
        
        n = len(cov_matrix)
        shrinkage_target = np.eye(n) * np.mean(np.diag(cov_matrix))
        
        shrinkage_intensity = 0.2
        shrunk_cov = (1 - shrinkage_intensity) * cov_matrix.values + shrinkage_intensity * shrinkage_target
        
        return pd.DataFrame(shrunk_cov, index=cov_matrix.index, columns=cov_matrix.columns)
    
    def calculate_portfolio_factor_exposure(
        self,
        positions: Dict[str, float],
        factor_exposures: pd.DataFrame
    ) -> pd.Series:
        """
        计算组合因子暴露
        
        Args:
            positions: 持仓权重字典 {stock_code: weight}
            factor_exposures: 因子暴露 DataFrame
            
        Returns:
            组合因子暴露 Series
        """
        portfolio_exposure = pd.Series(0.0, index=factor_exposures.columns)
        
        for stock, weight in positions.items():
            if stock in factor_exposures.index:
                portfolio_exposure += weight * factor_exposures.loc[stock]
        
        return portfolio_exposure
    
    def calculate_factor_risk(
        self,
        portfolio_exposure: pd.Series,
        factor_covariance: pd.DataFrame
    ) -> Dict:
        """
        计算因子风险
        
        Args:
            portfolio_exposure: 组合因子暴露
            factor_covariance: 因子协方差矩阵
            
        Returns:
            因子风险指标
        """
        if factor_covariance.empty or portfolio_exposure.empty:
            return {'total_factor_risk': 0, 'factor_contributions': {}}
        
        common_factors = portfolio_exposure.index.intersection(factor_covariance.index)
        
        if len(common_factors) == 0:
            return {'total_factor_risk': 0, 'factor_contributions': {}}
        
        exposure = portfolio_exposure.loc[common_factors].values
        cov = factor_covariance.loc[common_factors, common_factors].values
        
        total_variance = exposure @ cov @ exposure
        total_risk = np.sqrt(total_variance) if total_variance > 0 else 0
        
        factor_contributions = {}
        for i, factor in enumerate(common_factors):
            marginal_contrib = cov[i, :] @ exposure
            factor_contributions[factor] = {
                'exposure': round(exposure[i], 4),
                'marginal_contribution': round(marginal_contrib, 6),
                'risk_contribution': round(exposure[i] * marginal_contrib / total_variance if total_variance > 0 else 0, 4)
            }
        
        return {
            'total_factor_risk': round(total_risk * 100, 2),
            'factor_contributions': factor_contributions
        }


class FactorExposureMonitor:
    """因子暴露监控器"""
    
    WARNING_THRESHOLDS = {
        'market_cap': {'low': -1.0, 'high': 1.0},
        'value': {'low': -0.5, 'high': 0.5},
        'momentum': {'low': -0.5, 'high': 0.5},
        'volatility': {'low': -0.5, 'high': 0.5},
        'size': {'low': -1.0, 'high': 1.0}
    }
    
    def __init__(self):
        self.exposure_history = []
        self.alerts = []
    
    def check_exposure(
        self,
        portfolio_exposure: pd.Series,
        thresholds: Dict = None
    ) -> List[Dict]:
        """
        检查因子暴露是否超出阈值
        
        Args:
            portfolio_exposure: 组合因子暴露
            thresholds: 自定义阈值
            
        Returns:
            警告列表
        """
        if thresholds is None:
            thresholds = self.WARNING_THRESHOLDS
        
        alerts = []
        
        for factor, exposure in portfolio_exposure.items():
            factor_lower = factor.lower()
            
            for key, thresh in thresholds.items():
                if key in factor_lower:
                    low = thresh['low']
                    high = thresh['high']
                    
                    if exposure < low:
                        alerts.append({
                            'factor': factor,
                            'type': 'LOW_EXPOSURE',
                            'current': round(exposure, 4),
                            'threshold': low,
                            'message': f'{factor}暴露({exposure:.2f})低于下限({low})'
                        })
                    elif exposure > high:
                        alerts.append({
                            'factor': factor,
                            'type': 'HIGH_EXPOSURE',
                            'current': round(exposure, 4),
                            'threshold': high,
                            'message': f'{factor}暴露({exposure:.2f})高于上限({high})'
                        })
                    break
        
        self.alerts.extend(alerts)
        return alerts
    
    def track_exposure(
        self,
        date: str,
        portfolio_exposure: pd.Series
    ):
        """
        追踪因子暴露历史
        
        Args:
            date: 日期
            portfolio_exposure: 组合因子暴露
        """
        record = {'date': date}
        record.update(portfolio_exposure.to_dict())
        self.exposure_history.append(record)
    
    def get_exposure_trend(self, factor: str, periods: int = 20) -> Dict:
        """
        获取因子暴露趋势
        
        Args:
            factor: 因子名称
            periods: 回溯期数
            
        Returns:
            趋势统计
        """
        if len(self.exposure_history) < 2:
            return {'trend': 'N/A', 'change': 0}
        
        recent = self.exposure_history[-periods:]
        
        if factor not in recent[0]:
            return {'trend': 'N/A', 'change': 0}
        
        values = [r[factor] for r in recent if factor in r]
        
        if len(values) < 2:
            return {'trend': 'N/A', 'change': 0}
        
        change = values[-1] - values[0]
        trend = '↑' if change > 0.1 else '↓' if change < -0.1 else '→'
        
        return {
            'trend': trend,
            'change': round(change, 4),
            'current': round(values[-1], 4),
            'mean': round(np.mean(values), 4),
            'std': round(np.std(values), 4)
        }
    
    def generate_exposure_report(self) -> pd.DataFrame:
        """
        生成因子暴露报告
        
        Returns:
            因子暴露报告 DataFrame
        """
        if not self.exposure_history:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.exposure_history)
        
        report_data = []
        factor_cols = [c for c in df.columns if c != 'date']
        
        for factor in factor_cols:
            values = df[factor].dropna()
            
            if len(values) == 0:
                continue
            
            trend_info = self.get_exposure_trend(factor)
            
            report_data.append({
                'factor': factor,
                'current': round(values.iloc[-1], 4) if len(values) > 0 else 0,
                'mean': round(values.mean(), 4),
                'std': round(values.std(), 4),
                'min': round(values.min(), 4),
                'max': round(values.max(), 4),
                'trend': trend_info['trend'],
                'alerts': len([a for a in self.alerts if a['factor'] == factor])
            })
        
        return pd.DataFrame(report_data)


if __name__ == "__main__":
    calculator = RiskCalculator()
    
    print("=" * 60)
    print("风险计算模块测试")
    print("=" * 60)
    
    np.random.seed(42)
    test_returns = np.random.normal(0.001, 0.02, 100).tolist()
    test_prices = [100]
    for r in test_returns:
        test_prices.append(test_prices[-1] * (1 + r))
    
    print("\n【波动率测试】")
    print(f"  年化波动率: {calculator.calculate_volatility(test_returns):.2f}%")
    
    print("\n【VaR测试】")
    print(f"  95% VaR: {calculator.calculate_var(test_returns, 0.95):.2f}%")
    print(f"  99% VaR: {calculator.calculate_var(test_returns, 0.99):.2f}%")
    print(f"  95% CVaR: {calculator.calculate_cvar(test_returns, 0.95):.2f}%")
    
    print("\n【回撤测试】")
    max_dd, current_dd, duration = calculator.calculate_max_drawdown(test_prices)
    print(f"  最大回撤: {max_dd:.2f}%")
    print(f"  当前回撤: {current_dd:.2f}%")
    
    print("\n【夏普比率测试】")
    print(f"  夏普比率: {calculator.calculate_sharpe_ratio(test_returns):.2f}")
    print(f"  索提诺比率: {calculator.calculate_sortino_ratio(test_returns):.2f}")
    
    print("\n【胜率和盈亏比测试】")
    print(f"  胜率: {calculator.calculate_win_rate(test_returns):.2f}%")
    print(f"  盈亏比: {calculator.calculate_profit_loss_ratio(test_returns):.2f}")
