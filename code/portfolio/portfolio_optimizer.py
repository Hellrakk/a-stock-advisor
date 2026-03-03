#!/usr/bin/env python3
"""
组合优化模块 - PortfolioOptimizer
包含均值方差优化、风险平价、风险预算等优化方法
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

try:
    from scipy.optimize import minimize
    from scipy.linalg import inv
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False


class OptimizationMethod(Enum):
    """优化方法枚举"""
    EQUAL_WEIGHT = "equal_weight"
    MEAN_VARIANCE = "mean_variance"
    MIN_VARIANCE = "min_variance"
    MAX_SHARPE = "max_sharpe"
    RISK_PARITY = "risk_parity"
    RISK_BUDGET = "risk_budget"
    MAX_DIVERSIFICATION = "max_diversification"
    HIERARCHICAL_RISK_PARITY = "hrp"


@dataclass
class OptimizationConfig:
    """优化配置"""
    risk_free_rate: float = 0.03
    target_return: Optional[float] = None
    target_volatility: Optional[float] = None
    max_weight: float = 0.15
    min_weight: float = 0.0
    max_position_count: int = 20
    transaction_cost: float = 0.001
    regularization: float = 0.01


class PortfolioOptimizer:
    """组合优化器"""
    
    def __init__(self, config: Optional[OptimizationConfig] = None):
        self.config = config or OptimizationConfig()
        self.expected_returns: Optional[np.ndarray] = None
        self.cov_matrix: Optional[np.ndarray] = None
        self.asset_names: Optional[List[str]] = None
    
    def set_parameters(self, expected_returns: np.ndarray, 
                       cov_matrix: np.ndarray,
                       asset_names: Optional[List[str]] = None):
        n_assets = len(expected_returns)
        
        if cov_matrix.shape != (n_assets, n_assets):
            raise ValueError(f"协方差矩阵形状{cov_matrix.shape}与资产数量{n_assets}不匹配")
        
        self.expected_returns = np.array(expected_returns)
        self.cov_matrix = np.array(cov_matrix)
        self.asset_names = asset_names or [f"Asset_{i}" for i in range(n_assets)]
    
    def set_parameters_from_data(self, returns_data: pd.DataFrame):
        self.expected_returns = returns_data.mean().values * 252
        self.cov_matrix = returns_data.cov().values * 252
        self.asset_names = returns_data.columns.tolist()
    
    def _portfolio_return(self, weights: np.ndarray) -> float:
        return np.dot(weights, self.expected_returns)
    
    def _portfolio_volatility(self, weights: np.ndarray) -> float:
        return np.sqrt(np.dot(weights.T, np.dot(self.cov_matrix, weights)))
    
    def _portfolio_sharpe(self, weights: np.ndarray) -> float:
        ret = self._portfolio_return(weights)
        vol = self._portfolio_volatility(weights)
        if vol == 0:
            return 0
        return (ret - self.config.risk_free_rate) / vol
    
    def _risk_contribution(self, weights: np.ndarray) -> np.ndarray:
        portfolio_vol = self._portfolio_volatility(weights)
        if portfolio_vol == 0:
            return np.zeros_like(weights)
        
        marginal_contrib = np.dot(self.cov_matrix, weights)
        risk_contrib = weights * marginal_contrib / portfolio_vol
        
        return risk_contrib
    
    def equal_weight(self) -> np.ndarray:
        n_assets = len(self.expected_returns)
        return np.ones(n_assets) / n_assets
    
    def mean_variance_optimize(self, target_return: Optional[float] = None) -> np.ndarray:
        if not SCIPY_AVAILABLE:
            return self.equal_weight()
        
        n_assets = len(self.expected_returns)
        target_return = target_return or self.config.target_return
        
        def objective(weights):
            return self._portfolio_volatility(weights)
        
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
        ]
        
        if target_return is not None:
            constraints.append({
                'type': 'ineq',
                'fun': lambda w: self._portfolio_return(w) - target_return
            })
        
        bounds = tuple((self.config.min_weight, self.config.max_weight) 
                       for _ in range(n_assets))
        
        init_weights = np.ones(n_assets) / n_assets
        
        result = minimize(
            objective,
            init_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )
        
        if result.success:
            return result.x
        else:
            return self.equal_weight()
    
    def min_variance_optimize(self) -> np.ndarray:
        if not SCIPY_AVAILABLE:
            return self.equal_weight()
        
        n_assets = len(self.expected_returns)
        
        def objective(weights):
            return self._portfolio_volatility(weights) ** 2
        
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
        ]
        
        bounds = tuple((self.config.min_weight, self.config.max_weight) 
                       for _ in range(n_assets))
        
        init_weights = np.ones(n_assets) / n_assets
        
        result = minimize(
            objective,
            init_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )
        
        if result.success:
            return result.x
        else:
            return self.equal_weight()
    
    def max_sharpe_optimize(self) -> np.ndarray:
        if not SCIPY_AVAILABLE:
            return self.equal_weight()
        
        n_assets = len(self.expected_returns)
        
        def neg_sharpe(weights):
            return -self._portfolio_sharpe(weights)
        
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
        ]
        
        bounds = tuple((self.config.min_weight, self.config.max_weight) 
                       for _ in range(n_assets))
        
        init_weights = np.ones(n_assets) / n_assets
        
        result = minimize(
            neg_sharpe,
            init_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )
        
        if result.success:
            return result.x
        else:
            return self.equal_weight()
    
    def risk_parity_optimize(self) -> np.ndarray:
        if not SCIPY_AVAILABLE:
            return self.equal_weight()
        
        n_assets = len(self.expected_returns)
        target_risk = 1.0 / n_assets
        
        def risk_parity_objective(weights):
            risk_contrib = self._risk_contribution(weights)
            portfolio_vol = self._portfolio_volatility(weights)
            
            if portfolio_vol == 0:
                return 1e10
            
            target_contrib = portfolio_vol * target_risk
            return np.sum((risk_contrib - target_contrib) ** 2)
        
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
        ]
        
        bounds = tuple((self.config.min_weight, self.config.max_weight) 
                       for _ in range(n_assets))
        
        init_weights = np.ones(n_assets) / n_assets
        
        result = minimize(
            risk_parity_objective,
            init_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )
        
        if result.success:
            return result.x
        else:
            return self.equal_weight()
    
    def risk_budget_optimize(self, risk_budgets: np.ndarray) -> np.ndarray:
        if not SCIPY_AVAILABLE:
            return self.equal_weight()
        
        n_assets = len(self.expected_returns)
        
        if len(risk_budgets) != n_assets:
            raise ValueError("风险预算数量必须与资产数量一致")
        
        def risk_budget_objective(weights):
            risk_contrib = self._risk_contribution(weights)
            portfolio_vol = self._portfolio_volatility(weights)
            
            if portfolio_vol == 0:
                return 1e10
            
            target_contrib = portfolio_vol * risk_budgets
            return np.sum((risk_contrib - target_contrib) ** 2)
        
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
        ]
        
        bounds = tuple((self.config.min_weight, self.config.max_weight) 
                       for _ in range(n_assets))
        
        init_weights = np.ones(n_assets) / n_assets
        
        result = minimize(
            risk_budget_objective,
            init_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )
        
        if result.success:
            return result.x
        else:
            return self.equal_weight()
    
    def max_diversification_optimize(self) -> np.ndarray:
        if not SCIPY_AVAILABLE:
            return self.equal_weight()
        
        n_assets = len(self.expected_returns)
        asset_vols = np.sqrt(np.diag(self.cov_matrix))
        
        def diversification_ratio(weights):
            weighted_vol = np.dot(weights, asset_vols)
            portfolio_vol = self._portfolio_volatility(weights)
            if portfolio_vol == 0:
                return 0
            return -weighted_vol / portfolio_vol
        
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
        ]
        
        bounds = tuple((self.config.min_weight, self.config.max_weight) 
                       for _ in range(n_assets))
        
        init_weights = np.ones(n_assets) / n_assets
        
        result = minimize(
            diversification_ratio,
            init_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )
        
        if result.success:
            return result.x
        else:
            return self.equal_weight()
    
    def optimize(self, method: OptimizationMethod = OptimizationMethod.MAX_SHARPE,
                 **kwargs) -> np.ndarray:
        if method == OptimizationMethod.EQUAL_WEIGHT:
            return self.equal_weight()
        elif method == OptimizationMethod.MEAN_VARIANCE:
            return self.mean_variance_optimize(kwargs.get('target_return'))
        elif method == OptimizationMethod.MIN_VARIANCE:
            return self.min_variance_optimize()
        elif method == OptimizationMethod.MAX_SHARPE:
            return self.max_sharpe_optimize()
        elif method == OptimizationMethod.RISK_PARITY:
            return self.risk_parity_optimize()
        elif method == OptimizationMethod.RISK_BUDGET:
            return self.risk_budget_optimize(kwargs.get('risk_budgets', np.ones(len(self.expected_returns)) / len(self.expected_returns)))
        elif method == OptimizationMethod.MAX_DIVERSIFICATION:
            return self.max_diversification_optimize()
        else:
            return self.equal_weight()
    
    def get_optimization_result(self, weights: np.ndarray) -> Dict:
        return {
            'weights': dict(zip(self.asset_names, weights)),
            'expected_return': self._portfolio_return(weights),
            'volatility': self._portfolio_volatility(weights),
            'sharpe_ratio': self._portfolio_sharpe(weights),
            'risk_contribution': dict(zip(self.asset_names, self._risk_contribution(weights)))
        }
    
    def generate_efficient_frontier(self, n_points: int = 20) -> Dict:
        min_ret = min(self.expected_returns)
        max_ret = max(self.expected_returns)
        target_returns = np.linspace(min_ret, max_ret, n_points)
        
        frontier_returns = []
        frontier_volatilities = []
        frontier_weights = []
        
        for target_ret in target_returns:
            try:
                weights = self.mean_variance_optimize(target_ret)
                frontier_returns.append(self._portfolio_return(weights))
                frontier_volatilities.append(self._portfolio_volatility(weights))
                frontier_weights.append(weights)
            except:
                continue
        
        return {
            'returns': frontier_returns,
            'volatilities': frontier_volatilities,
            'weights': frontier_weights
        }


class HierarchicalRiskParity:
    """层次风险平价优化器"""
    
    def __init__(self):
        pass
    
    def _cluster(self, cov_matrix: np.ndarray) -> np.ndarray:
        n = cov_matrix.shape[0]
        dist_matrix = np.sqrt(0.5 * (1 - cov_matrix / np.diag(cov_matrix)[:, None]))
        np.fill_diagonal(dist_matrix, 0)
        return dist_matrix
    
    def _get_quasi_diag(self, link: np.ndarray) -> List[int]:
        link = link.astype(int)
        n = link.shape[0] + 1
        sort_ix = [link[-1, 0], link[-1, 1]]
        while max(sort_ix) >= n:
            sort_ix = [link[el - n, 0] if el >= n else el for el in sort_ix[:1]] + \
                      [link[el - n, 1] if el >= n else el for el in sort_ix[1:]]
        return sort_ix
    
    def _get_rec_bipart(self, cov_matrix: np.ndarray, sort_ix: List[int]) -> np.ndarray:
        n = cov_matrix.shape[0]
        weights = np.ones(n)
        
        clustered_ix = [sort_ix]
        while len(clustered_ix) > 0:
            clustered_ix = [cluster[start:end] for cluster in clustered_ix 
                           for start, end in ((0, len(cluster) // 2), 
                                              (len(cluster) // 2, len(cluster)))
                           if len(cluster) > 1]
            
            for cluster in clustered_ix:
                cov_subset = cov_matrix[np.ix_(cluster, cluster)]
                inv_diag = 1.0 / np.diag(cov_subset)
                parity_weights = inv_diag / np.sum(inv_diag)
                
                cluster_var = np.dot(parity_weights, np.dot(cov_subset, parity_weights))
                
                for i, ix in enumerate(cluster):
                    weights[ix] *= parity_weights[i]
        
        return weights / np.sum(weights)
    
    def optimize(self, returns_data: pd.DataFrame) -> np.ndarray:
        try:
            from scipy.cluster.hierarchy import linkage
            from scipy.spatial.distance import squareform
        except ImportError:
            return np.ones(len(returns_data.columns)) / len(returns_data.columns)
        
        cov_matrix = returns_data.cov().values
        corr_matrix = returns_data.corr().values
        
        dist_matrix = np.sqrt(0.5 * (1 - corr_matrix))
        
        condensed_dist = squareform(dist_matrix)
        link = linkage(condensed_dist, 'single')
        
        sort_ix = self._get_quasi_diag(link)
        weights = self._get_rec_bipart(cov_matrix, sort_ix)
        
        return weights


def apply_constraints(weights: np.ndarray, 
                     max_weight: float = 0.15,
                     min_weight: float = 0.0) -> np.ndarray:
    weights = np.clip(weights, min_weight, max_weight)
    weights = weights / np.sum(weights)
    return weights


def turnover_aware_optimize(current_weights: np.ndarray,
                            target_weights: np.ndarray,
                            max_turnover: float = 0.3) -> np.ndarray:
    turnover = np.sum(np.abs(target_weights - current_weights)) / 2
    
    if turnover <= max_turnover:
        return target_weights
    
    scale = max_turnover / turnover
    adjusted_weights = current_weights + scale * (target_weights - current_weights)
    
    adjusted_weights = np.maximum(adjusted_weights, 0)
    adjusted_weights = adjusted_weights / np.sum(adjusted_weights)
    
    return adjusted_weights


if __name__ == '__main__':
    print("=" * 60)
    print("组合优化模块测试")
    print("=" * 60)
    
    np.random.seed(42)
    n_assets = 10
    n_periods = 252
    
    returns_data = pd.DataFrame(
        np.random.normal(0.0005, 0.02, (n_periods, n_assets)),
        columns=[f"Stock_{i}" for i in range(n_assets)]
    )
    
    optimizer = PortfolioOptimizer()
    optimizer.set_parameters_from_data(returns_data)
    
    print("\n测试不同优化方法:")
    
    methods = [
        OptimizationMethod.EQUAL_WEIGHT,
        OptimizationMethod.MIN_VARIANCE,
        OptimizationMethod.MAX_SHARPE,
        OptimizationMethod.RISK_PARITY,
        OptimizationMethod.MAX_DIVERSIFICATION
    ]
    
    for method in methods:
        weights = optimizer.optimize(method)
        result = optimizer.get_optimization_result(weights)
        print(f"\n{method.value}:")
        print(f"  期望收益: {result['expected_return']:.4f}")
        print(f"  波动率: {result['volatility']:.4f}")
        print(f"  夏普比率: {result['sharpe_ratio']:.4f}")
        print(f"  最大权重: {max(result['weights'].values()):.4f}")
    
    print("\n生成有效前沿...")
    frontier = optimizer.generate_efficient_frontier(n_points=10)
    print(f"有效前沿点数: {len(frontier['returns'])}")
