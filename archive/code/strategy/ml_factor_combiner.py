#!/usr/bin/env python3
"""
ML因子组合模块
借鉴Qlib的机器学习因子组合方法
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge, Lasso
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
import warnings
warnings.filterwarnings('ignore')


class MLFactorCombiner:
    """机器学习因子组合器"""
    
    def __init__(
        self,
        model_type: str = 'gbdt',
        n_estimators: int = 100,
        max_depth: int = 3,
        learning_rate: float = 0.1,
        regularization: float = 0.1
    ):
        """
        初始化ML因子组合器
        
        Args:
            model_type: 模型类型 ('gbdt', 'rf', 'ridge', 'lasso')
            n_estimators: 树模型数量
            max_depth: 树最大深度
            learning_rate: 学习率
            regularization: 正则化系数
        """
        self.model_type = model_type
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.learning_rate = learning_rate
        self.regularization = regularization
        
        self.model = None
        self.scaler = StandardScaler()
        self.feature_importance = {}
        self.is_fitted = False
    
    def _create_model(self):
        """创建模型"""
        if self.model_type == 'gbdt':
            return GradientBoostingRegressor(
                n_estimators=self.n_estimators,
                max_depth=self.max_depth,
                learning_rate=self.learning_rate,
                subsample=0.8,
                random_state=42
            )
        elif self.model_type == 'rf':
            return RandomForestRegressor(
                n_estimators=self.n_estimators,
                max_depth=self.max_depth,
                random_state=42,
                n_jobs=-1
            )
        elif self.model_type == 'ridge':
            return Ridge(alpha=self.regularization)
        elif self.model_type == 'lasso':
            return Lasso(alpha=self.regularization)
        else:
            return GradientBoostingRegressor(
                n_estimators=self.n_estimators,
                max_depth=self.max_depth,
                learning_rate=self.learning_rate,
                random_state=42
            )
    
    def fit(
        self,
        factor_exposures: pd.DataFrame,
        future_returns: pd.Series,
        validate: bool = True
    ) -> Dict:
        """
        训练模型
        
        Args:
            factor_exposures: 因子暴露 DataFrame (index: stock_code, columns: factors)
            future_returns: 未来收益率 Series
            validate: 是否进行交叉验证
            
        Returns:
            训练结果字典
        """
        common_index = factor_exposures.index.intersection(future_returns.index)
        
        if len(common_index) < 50:
            return {'success': False, 'message': '样本量不足'}
        
        X = factor_exposures.loc[common_index].values
        y = future_returns.loc[common_index].values
        
        valid_mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X = X[valid_mask]
        y = y[valid_mask]
        
        if len(X) < 30:
            return {'success': False, 'message': '有效样本不足'}
        
        X_scaled = self.scaler.fit_transform(X)
        
        self.model = self._create_model()
        
        cv_scores = []
        if validate and len(X) >= 100:
            tscv = TimeSeriesSplit(n_splits=5)
            cv_scores = cross_val_score(
                self.model, X_scaled, y, 
                cv=tscv, 
                scoring='neg_mean_squared_error'
            )
            cv_scores = -cv_scores
        
        self.model.fit(X_scaled, y)
        self.is_fitted = True
        
        if hasattr(self.model, 'feature_importances_'):
            self.feature_importance = dict(zip(
                factor_exposures.columns,
                self.model.feature_importances_
            ))
        elif hasattr(self.model, 'coef_'):
            self.feature_importance = dict(zip(
                factor_exposures.columns,
                np.abs(self.model.coef_)
            ))
        
        y_pred = self.model.predict(X_scaled)
        mse = mean_squared_error(y, y_pred)
        mae = mean_absolute_error(y, y_pred)
        
        return {
            'success': True,
            'n_samples': len(X),
            'mse': round(mse, 6),
            'mae': round(mae, 6),
            'cv_mse_mean': round(np.mean(cv_scores), 6) if len(cv_scores) > 0 else None,
            'cv_mse_std': round(np.std(cv_scores), 6) if len(cv_scores) > 0 else None,
            'feature_importance': self.feature_importance
        }
    
    def predict(self, factor_exposures: pd.DataFrame) -> pd.Series:
        """
        预测收益率
        
        Args:
            factor_exposures: 因子暴露 DataFrame
            
        Returns:
            预测收益率 Series
        """
        if not self.is_fitted:
            raise ValueError("模型尚未训练")
        
        X = factor_exposures.values
        X_scaled = self.scaler.transform(X)
        
        predictions = self.model.predict(X_scaled)
        
        return pd.Series(predictions, index=factor_exposures.index)
    
    def get_factor_weights(self) -> Dict[str, float]:
        """
        获取因子权重（基于特征重要性）
        
        Returns:
            因子权重字典
        """
        if not self.feature_importance:
            return {}
        
        total = sum(self.feature_importance.values())
        if total == 0:
            return {k: 1.0/len(self.feature_importance) for k in self.feature_importance}
        
        return {k: v/total for k, v in self.feature_importance.items()}


class EnsembleFactorCombiner:
    """集成因子组合器"""
    
    def __init__(self, models: List[str] = None):
        """
        初始化集成因子组合器
        
        Args:
            models: 模型类型列表
        """
        self.models = models or ['gbdt', 'rf', 'ridge']
        self.combiners = {}
        self.weights = {}
        
        for model_type in self.models:
            self.combiners[model_type] = MLFactorCombiner(model_type=model_type)
    
    def fit(
        self,
        factor_exposures: pd.DataFrame,
        future_returns: pd.Series
    ) -> Dict:
        """
        训练所有模型
        
        Args:
            factor_exposures: 因子暴露
            future_returns: 未来收益率
            
        Returns:
            训练结果
        """
        results = {}
        performances = {}
        
        for model_type, combiner in self.combiners.items():
            result = combiner.fit(factor_exposures, future_returns)
            results[model_type] = result
            
            if result['success']:
                performances[model_type] = result.get('cv_mse_mean', result.get('mse', 1))
        
        if performances:
            min_perf = min(performances.values())
            inv_perfs = {k: min_perf/v for k, v in performances.items()}
            total = sum(inv_perfs.values())
            self.weights = {k: v/total for k, v in inv_perfs.items()}
        else:
            self.weights = {model: 1.0/len(self.models) for model in self.models}
        
        return {
            'model_results': results,
            'ensemble_weights': self.weights
        }
    
    def predict(self, factor_exposures: pd.DataFrame) -> pd.Series:
        """
        集成预测
        
        Args:
            factor_exposures: 因子暴露
            
        Returns:
            预测收益率
        """
        predictions = pd.Series(0.0, index=factor_exposures.index)
        
        for model_type, combiner in self.combiners.items():
            if combiner.is_fitted:
                pred = combiner.predict(factor_exposures)
                predictions += self.weights[model_type] * pred
        
        return predictions
    
    def get_factor_weights(self) -> Dict[str, float]:
        """
        获取集成因子权重
        
        Returns:
            因子权重字典
        """
        all_weights = {}
        
        for model_type, combiner in self.combiners.items():
            if combiner.is_fitted:
                weights = combiner.get_factor_weights()
                for factor, weight in weights.items():
                    if factor not in all_weights:
                        all_weights[factor] = 0
                    all_weights[factor] += self.weights[model_type] * weight
        
        return all_weights


if __name__ == "__main__":
    print("=" * 60)
    print("ML因子组合模块测试")
    print("=" * 60)
    
    np.random.seed(42)
    n_stocks = 200
    n_factors = 10
    
    factor_data = pd.DataFrame(
        np.random.randn(n_stocks, n_factors),
        columns=[f'factor_{i}' for i in range(n_factors)],
        index=[f'stock_{i:04d}' for i in range(n_stocks)]
    )
    
    true_weights = np.random.randn(n_factors)
    returns = factor_data.values @ true_weights + np.random.randn(n_stocks) * 0.1
    future_returns = pd.Series(returns, index=factor_data.index)
    
    combiner = MLFactorCombiner(model_type='gbdt')
    result = combiner.fit(factor_data, future_returns)
    
    print(f"\n【训练结果】")
    print(f"  成功: {result['success']}")
    print(f"  样本量: {result['n_samples']}")
    print(f"  MSE: {result['mse']}")
    print(f"  MAE: {result['mae']}")
    
    print(f"\n【因子权重】")
    weights = combiner.get_factor_weights()
    for factor, weight in sorted(weights.items(), key=lambda x: -x[1]):
        print(f"  {factor}: {weight:.4f}")
    
    print("\n【集成模型测试】")
    ensemble = EnsembleFactorCombiner(models=['gbdt', 'rf', 'ridge'])
    ensemble_result = ensemble.fit(factor_data, future_returns)
    
    print(f"  模型权重: {ensemble_result['ensemble_weights']}")
    print(f"  集成因子权重: {ensemble.get_factor_weights()}")
