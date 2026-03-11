#!/usr/bin/env python3
"""
多因子得分模型
基于IC值加权计算综合得分，支持滚动IC动态权重
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from scipy.stats import spearmanr
import warnings
warnings.filterwarnings('ignore')


class RollingICCalculator:
    """滚动IC计算器"""
    
    def __init__(self, window: int = 20, min_periods: int = 10):
        """
        初始化滚动IC计算器
        
        Args:
            window: 滚动窗口大小（交易日）
            min_periods: 最小计算周期
        """
        self.window = window
        self.min_periods = min_periods
        self.ic_history = {}
    
    def calculate_rolling_ic(
        self, 
        factor_series: pd.Series, 
        return_series: pd.Series,
        date_series: pd.Series = None
    ) -> pd.DataFrame:
        """
        计算滚动IC
        
        Args:
            factor_series: 因子值序列
            return_series: 收益率序列
            date_series: 日期序列（可选）
            
        Returns:
            滚动IC结果DataFrame
        """
        if date_series is not None:
            df = pd.DataFrame({
                'factor': factor_series.values,
                'return': return_series.values,
                'date': date_series.values
            })
            df = df.sort_values('date')
        else:
            df = pd.DataFrame({
                'factor': factor_series.values,
                'return': return_series.values
            })
        
        df = df.dropna()
        
        if len(df) < self.min_periods:
            return pd.DataFrame()
        
        results = []
        
        for i in range(self.min_periods, len(df) + 1):
            window_data = df.iloc[max(0, i-self.window):i]
            
            if len(window_data) >= self.min_periods:
                ic, pval = spearmanr(window_data['factor'], window_data['return'])
                
                results.append({
                    'ic': ic if not np.isnan(ic) else 0,
                    'pval': pval if not np.isnan(pval) else 1,
                    'n': len(window_data),
                    'idx': i - 1
                })
        
        return pd.DataFrame(results)
    
    def get_ic_decay(self, ic_series: pd.Series) -> Dict:
        """
        计算IC衰减
        
        Args:
            ic_series: IC序列
            
        Returns:
            衰减统计信息
        """
        if len(ic_series) < 5:
            return {'decay_rate': 0, 'half_life': np.inf}
        
        ic_values = ic_series.dropna().values
        if len(ic_values) < 5:
            return {'decay_rate': 0, 'half_life': np.inf}
        
        first_half = np.mean(np.abs(ic_values[:len(ic_values)//2]))
        second_half = np.mean(np.abs(ic_values[len(ic_values)//2:]))
        
        decay_rate = (first_half - second_half) / first_half if first_half > 0 else 0
        
        half_life = np.log(2) / abs(decay_rate) if decay_rate != 0 else np.inf
        
        return {
            'decay_rate': round(decay_rate, 4),
            'half_life': round(half_life, 2),
            'ic_mean': round(np.mean(np.abs(ic_values)), 4),
            'ic_std': round(np.std(ic_values), 4),
            'ic_ir': round(np.mean(ic_values) / np.std(ic_values), 4) if np.std(ic_values) > 0 else 0
        }


class DynamicFactorWeightSystem:
    """动态因子权重系统"""
    
    def __init__(
        self, 
        ic_window: int = 20,
        ic_threshold: float = 0.02,
        ir_threshold: float = 0.5,
        decay_factor: float = 0.95,
        min_weight: float = 0.05,
        max_weight: float = 0.40
    ):
        """
        初始化动态因子权重系统
        
        Args:
            ic_window: IC计算窗口
            ic_threshold: IC有效阈值
            ir_threshold: IR有效阈值
            decay_factor: 时间衰减因子
            min_weight: 最小因子权重
            max_weight: 最大因子权重
        """
        self.ic_window = ic_window
        self.ic_threshold = ic_threshold
        self.ir_threshold = ir_threshold
        self.decay_factor = decay_factor
        self.min_weight = min_weight
        self.max_weight = max_weight
        
        self.rolling_ic_calculator = RollingICCalculator(window=ic_window)
        self.factor_ic_history = {}
        self.factor_weights_history = {}
        self.current_weights = {}
    
    def update_weights(
        self, 
        factor_data: pd.DataFrame,
        return_data: pd.Series,
        date: str = None
    ) -> Dict[str, float]:
        """
        更新因子权重
        
        Args:
            factor_data: 因子数据DataFrame (columns: factors)
            return_data: 收益率Series
            date: 当前日期
            
        Returns:
            更新后的因子权重
        """
        factor_names = factor_data.columns.tolist()
        ic_values = {}
        ir_values = {}
        
        for factor in factor_names:
            factor_series = factor_data[factor]
            
            ic_df = self.rolling_ic_calculator.calculate_rolling_ic(
                factor_series, return_data
            )
            
            if len(ic_df) > 0:
                recent_ic = ic_df['ic'].tail(self.ic_window)
                ic_mean = recent_ic.mean()
                ic_std = recent_ic.std()
                ir = ic_mean / ic_std if ic_std > 0 else 0
                
                ic_values[factor] = ic_mean
                ir_values[factor] = ir
                
                if factor not in self.factor_ic_history:
                    self.factor_ic_history[factor] = []
                self.factor_ic_history[factor].append({
                    'date': date or datetime.now().strftime('%Y-%m-%d'),
                    'ic': ic_mean,
                    'ir': ir
                })
            else:
                ic_values[factor] = 0
                ir_values[factor] = 0
        
        weights = self._calculate_weights(ic_values, ir_values)
        
        self.current_weights = weights
        if date:
            self.factor_weights_history[date] = weights.copy()
        
        return weights
    
    def _calculate_weights(
        self, 
        ic_values: Dict[str, float], 
        ir_values: Dict[str, float]
    ) -> Dict[str, float]:
        """
        计算因子权重
        
        权重计算方法：
        1. 过滤无效因子（IC和IR不满足阈值）
        2. 基于IC绝对值加权
        3. 应用权重上下限
        4. 归一化
        """
        valid_factors = {}
        
        for factor, ic in ic_values.items():
            ir = ir_values.get(factor, 0)
            
            if abs(ic) >= self.ic_threshold and abs(ir) >= self.ir_threshold:
                weight_score = abs(ic) * min(abs(ir), 2.0)
                valid_factors[factor] = weight_score
        
        if not valid_factors:
            valid_factors = {f: abs(ic) for f, ic in ic_values.items() if abs(ic) > 0}
        
        if not valid_factors:
            n = len(ic_values)
            return {f: 1.0/n for f in ic_values.keys()} if n > 0 else {}
        
        total_score = sum(valid_factors.values())
        if total_score == 0:
            n = len(valid_factors)
            return {f: 1.0/n for f in valid_factors.keys()}
        
        weights = {f: s/total_score for f, s in valid_factors.items()}
        
        weights = {f: max(self.min_weight, min(self.max_weight, w)) 
                  for f, w in weights.items()}
        
        total = sum(weights.values())
        weights = {f: w/total for f, w in weights.items()}
        
        return weights
    
    def get_weights(self) -> Dict[str, float]:
        """
        获取当前因子权重
        
        Returns:
            当前因子权重字典
        """
        return self.current_weights.copy() if self.current_weights else {}
    
    def get_weight_stability(self) -> Dict:
        """
        获取权重稳定性指标
        
        Returns:
            稳定性统计
        """
        if len(self.factor_weights_history) < 2:
            return {'stability': 1.0, 'turnover': 0}
        
        dates = sorted(self.factor_weights_history.keys())
        recent_dates = dates[-5:] if len(dates) >= 5 else dates
        
        weight_changes = []
        for i in range(1, len(recent_dates)):
            prev_weights = self.factor_weights_history[recent_dates[i-1]]
            curr_weights = self.factor_weights_history[recent_dates[i]]
            
            all_factors = set(prev_weights.keys()) | set(curr_weights.keys())
            change = sum(abs(prev_weights.get(f, 0) - curr_weights.get(f, 0)) 
                        for f in all_factors) / 2
            weight_changes.append(change)
        
        avg_change = np.mean(weight_changes) if weight_changes else 0
        stability = 1 - avg_change
        
        return {
            'stability': round(stability, 4),
            'turnover': round(avg_change, 4),
            'n_periods': len(recent_dates)
        }
    
    def get_factor_effectiveness_report(self) -> pd.DataFrame:
        """
        获取因子有效性报告
        
        Returns:
            因子有效性DataFrame
        """
        reports = []
        
        for factor, history in self.factor_ic_history.items():
            if not history:
                continue
            
            ics = [h['ic'] for h in history]
            irs = [h['ir'] for h in history]
            
            reports.append({
                'factor': factor,
                'ic_mean': round(np.mean(ics), 4),
                'ic_std': round(np.std(ics), 4),
                'ir_mean': round(np.mean(irs), 4),
                'current_weight': round(self.current_weights.get(factor, 0), 4),
                'ic_trend': '↑' if len(ics) >= 2 and ics[-1] > ics[-2] else '↓',
                'effectiveness': '有效' if abs(np.mean(ics)) >= self.ic_threshold else '无效'
            })
        
        return pd.DataFrame(reports).sort_values('current_weight', ascending=False)
    
    def calculate_score(self, factor_df: pd.DataFrame) -> pd.Series:
        """
        计算综合得分
        
        Args:
            factor_df: 因子数据 DataFrame (index: stock_code, columns: factor_names)
            
        Returns:
            综合得分 Series (index: stock_code)
        """
        if not self.current_weights:
            n = len(factor_df.columns)
            weights = {f: 1.0/n for f in factor_df.columns}
        else:
            weights = self.current_weights
        
        available_factors = [f for f in weights.keys() if f in factor_df.columns]
        
        if not available_factors:
            return pd.Series(0.0, index=factor_df.index)
        
        scores = pd.Series(0.0, index=factor_df.index)
        
        for factor in available_factors:
            weight = weights[factor]
            factor_data = factor_df[factor]
            
            mean = factor_data.mean()
            std = factor_data.std()
            if std > 0:
                normalized_factor = (factor_data - mean) / std
            else:
                normalized_factor = pd.Series(0.0, index=factor_data.index)
            
            scores += weight * normalized_factor
        
        return scores


class MultiFactorScoreModel:
    """多因子得分模型"""
    
    def __init__(self):
        self.factor_names = []
        self.factor_weights = {}
        self.factor_ic = {}
        self.normalized_factors = {}
    
    def load_factor_scores(self, score_file: str):
        """加载因子得分"""
        import json
        with open(score_file, 'r') as f:
            factor_scores = json.load(f)
        
        self.factor_ic = factor_scores
    
    def set_ic_weighted(self, factor_ic: Dict[str, float], available_factors: List[str] = None):
        """
        设置IC权重

        Args:
            factor_ic: 因子IC值字典 {factor_name: IC}
            available_factors: 可用的因子列名列表
        """
        self.factor_ic = factor_ic

        # 如果指定了可用因子，只使用这些因子
        if available_factors:
            factor_ic = {k: v for k, v in factor_ic.items() if k in available_factors}

        # 计算IC绝对值作为权重
        ic_abs = {k: abs(v) for k, v in factor_ic.items() if not pd.isna(v)}
        total_ic = sum(ic_abs.values())

        if total_ic > 0:
            self.factor_weights = {k: v/total_ic for k, v in ic_abs.items()}
        else:
            # 如果所有IC都是NaN，使用等权
            factor_names = list(factor_ic.keys())
            if factor_names:
                self.factor_weights = {k: 1.0/len(factor_names) for k in factor_names}
            else:
                self.factor_weights = {}

    def auto_detect_factors(self, factor_df: pd.DataFrame, exclude_columns: List[str] = None):
        """
        自动检测可用因子并创建默认权重

        Args:
            factor_df: 因子数据DataFrame
            exclude_columns: 要排除的列名列表
        """
        if exclude_columns is None:
            exclude_columns = ['date', 'stock_code', 'month', 'factor_score', '股票名称', '股票代码', 'is_suspended', 'index']

        # 找出所有数值型列，排除指定列
        numeric_columns = factor_df.select_dtypes(include=[np.number]).columns.tolist()
        factor_columns = [col for col in numeric_columns if col not in exclude_columns]

        # 为每个因子设置默认IC值
        default_ic = {col: 0.1 for col in factor_columns}

        # 设置权重
        self.set_ic_weighted(default_ic, factor_columns)

        return factor_columns
    
    def normalize_factor(self, factor_data: pd.Series) -> pd.Series:
        """
        标准化因子（Z-score标准化）
        
        Args:
            factor_data: 因子数据
            
        Returns:
            标准化后的因子数据
        """
        mean = factor_data.mean()
        std = factor_data.std()
        
        if std > 0:
            return (factor_data - mean) / std
        else:
            # 如果标准差为0，返回全0
            return pd.Series(0, index=factor_data.index)
    
    def calculate_score(self, factor_df: pd.DataFrame, stock_codes: List[str] = None) -> pd.Series:
        """
        计算综合得分
        
        Args:
            factor_df: 因子数据 DataFrame (index: stock_code, columns: factor_names)
            stock_codes: 股票代码列表
            
        Returns:
            综合得分 Series (index: stock_code)
        """
        if stock_codes is None:
            stock_codes = factor_df.index.tolist()
        
        # 过滤出有因子权重的列
        available_factors = [f for f in self.factor_weights.keys() if f in factor_df.columns]
        
        if not available_factors:
            raise ValueError("没有可用的因子数据")
        
        # 计算加权得分
        scores = pd.Series(0.0, index=factor_df.index)
        
        for factor in available_factors:
            weight = self.factor_weights[factor]
            factor_data = factor_df[factor]
            
            # 标准化因子
            normalized_factor = self.normalize_factor(factor_data)
            
            # 累加加权得分
            scores += weight * normalized_factor
        
        return scores
    
    def get_top_stocks(self, factor_df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """
        获取得分最高的股票
        
        Args:
            factor_df: 因子数据
            n: 选股数量
            
        Returns:
            选中的股票 DataFrame
        """
        scores = self.calculate_score(factor_df)
        
        # 按得分排序
        top_scores = scores.nlargest(n)
        
        # 返回包含得分的数据
        result = factor_df.loc[top_scores.index].copy()
        result['综合得分'] = top_scores
        
        return result.sort_values('综合得分', ascending=False)


class MultiFactorModel:
    """多因子选股模型 - 统一接口"""
    
    def __init__(self, factor_weights: Dict[str, float] = None):
        """
        初始化多因子模型
        
        Args:
            factor_weights: 因子权重字典，默认使用等权重
        """
        self.ic_calculator = RollingICCalculator()
        self.weight_system = DynamicFactorWeightSystem()
        
        self.default_weights = {
            'value': 0.30,
            'quality': 0.30,
            'growth': 0.20,
            'momentum': 0.15,
            'volatility': 0.05
        }
        
        self.factor_weights = factor_weights or self.default_weights
    
    def select_stocks(
        self, 
        factor_df: pd.DataFrame, 
        n_stocks: int = 10,
        method: str = 'weighted'
    ) -> pd.DataFrame:
        """
        多因子选股
        
        Args:
            factor_df: 因子数据 DataFrame (index: stock_code, columns: factor_names)
            n_stocks: 选股数量
            method: 选股方法 ('weighted', 'equal_weight', 'ic_weight')
            
        Returns:
            选中的股票 DataFrame
        """
        if method == 'weighted':
            scores = self.weight_system.calculate_score(factor_df)
        elif method == 'equal_weight':
            scores = factor_df.mean(axis=1)
        else:
            scores = self.weight_system.calculate_score(factor_df)
        
        top_scores = scores.nlargest(n_stocks)
        result = factor_df.loc[top_scores.index].copy()
        result['综合得分'] = top_scores
        
        return result.sort_values('综合得分', ascending=False)
    
    def get_factor_weights(self) -> Dict[str, float]:
        """获取当前因子权重"""
        return self.factor_weights
    
    def update_factor_weights(self, new_weights: Dict[str, float]):
        """更新因子权重"""
        self.factor_weights.update(new_weights)
    
    def evaluate_factor_effectiveness(
        self,
        factor_df: pd.DataFrame,
        returns_df: pd.DataFrame
    ) -> Dict[str, Dict]:
        """
        评估因子有效性
        
        Args:
            factor_df: 因子数据
            returns_df: 收益率数据
            
        Returns:
            各因子的评估结果
        """
        results = {}
        
        for factor_name in factor_df.columns:
            if factor_name in returns_df.columns:
                ic_series = self.ic_calculator.calculate_rolling_ic(
                    factor_df[factor_name],
                    returns_df[factor_name]
                )
                
                if not ic_series.empty:
                    decay_info = self.ic_calculator.get_ic_decay(ic_series['ic'])
                    results[factor_name] = {
                        'ic_mean': decay_info['ic_mean'],
                        'ic_std': decay_info['ic_std'],
                        'ic_ir': decay_info['ic_ir'],
                        'decay_rate': decay_info['decay_rate'],
                        'is_valid': decay_info['ic_mean'] > 0.02 and decay_info['ic_ir'] > 0.5
                    }
        
        return results
