#!/usr/bin/env python3
"""
市场状态识别与策略择时模块
基于宏观指标、市场宽度、波动率等特征识别市场状态
并根据不同状态下的策略表现动态调整策略权重
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')


class MarketStateFeatures:
    """市场状态特征提取"""
    
    def __init__(self, lookback_period: int = 20):
        """
        初始化市场状态特征提取器
        
        Args:
            lookback_period: 回溯期数（交易日）
        """
        self.lookback_period = lookback_period
    
    def calculate_market_breadth(self, index_data: pd.DataFrame) -> Dict[str, float]:
        """
        计算市场宽度指标
        
        Args:
            index_data: 指数数据 DataFrame，包含 close 列
            
        Returns:
            市场宽度指标字典
        """
        if len(index_data) < self.lookback_period:
            return {
                'advancers_ratio': 0.5,
                'new_highs_ratio': 0.5,
                'new_lows_ratio': 0.5,
                'mcclellan_oscillator': 0
            }
        
        # 计算上涨/下跌比率
        changes = index_data['close'].pct_change()
        advancers = (changes > 0).sum()
        decliners = (changes < 0).sum()
        total = advancers + decliners
        
        if total == 0:
            advancers_ratio = 0.5
        else:
            advancers_ratio = advancers / total
        
        # 计算新高/新低比率（简化版）
        rolling_max = index_data['close'].rolling(window=52).max()
        rolling_min = index_data['close'].rolling(window=52).min()
        new_highs = (index_data['close'] == rolling_max).sum()
        new_lows = (index_data['close'] == rolling_min).sum()
        
        new_highs_ratio = new_highs / len(index_data)
        new_lows_ratio = new_lows / len(index_data)
        
        # 计算麦克莱伦震荡指标（简化版）
        adv_dec_line = (advancers - decliners).cumsum()
        mcclellan_oscillator = adv_dec_line.rolling(window=19).mean() - adv_dec_line.rolling(window=39).mean()
        
        return {
            'advancers_ratio': advancers_ratio,
            'new_highs_ratio': new_highs_ratio,
            'new_lows_ratio': new_lows_ratio,
            'mcclellan_oscillator': mcclellan_oscillator.iloc[-1] if not pd.isna(mcclellan_oscillator.iloc[-1]) else 0
        }
    
    def calculate_volatility(self, index_data: pd.DataFrame) -> Dict[str, float]:
        """
        计算波动率指标
        
        Args:
            index_data: 指数数据 DataFrame，包含 close 列
            
        Returns:
            波动率指标字典
        """
        if len(index_data) < self.lookback_period:
            return {
                'volatility_20d': 0.02,
                'volatility_60d': 0.03,
                'volatility_change': 0,
                'vix_proxy': 20
            }
        
        # 计算不同周期的波动率
        returns = index_data['close'].pct_change()
        volatility_20d = returns.rolling(window=20).std().iloc[-1] * np.sqrt(252)
        volatility_60d = returns.rolling(window=60).std().iloc[-1] * np.sqrt(252)
        
        # 波动率变化
        volatility_change = volatility_20d - returns.rolling(window=60).std().iloc[-21] * np.sqrt(252) if len(returns) >= 80 else 0
        
        # VIX代理指标（简化版）
        vix_proxy = volatility_20d * 1000  # 简化的VIX代理
        
        return {
            'volatility_20d': volatility_20d,
            'volatility_60d': volatility_60d,
            'volatility_change': volatility_change,
            'vix_proxy': vix_proxy
        }
    
    def calculate_trend_strength(self, index_data: pd.DataFrame) -> Dict[str, float]:
        """
        计算趋势强度指标
        
        Args:
            index_data: 指数数据 DataFrame，包含 close 列
            
        Returns:
            趋势强度指标字典
        """
        if len(index_data) < self.lookback_period:
            return {
                'trend_strength': 0,
                'momentum_20d': 0,
                'momentum_60d': 0,
                'moving_average_slope': 0
            }
        
        # 计算动量
        momentum_20d = (index_data['close'].iloc[-1] / index_data['close'].iloc[-21]) - 1 if len(index_data) >= 21 else 0
        momentum_60d = (index_data['close'].iloc[-1] / index_data['close'].iloc[-61]) - 1 if len(index_data) >= 61 else 0
        
        # 计算移动平均线斜率
        ma20 = index_data['close'].rolling(window=20).mean()
        if len(ma20) >= 5:
            x = np.arange(5)
            y = ma20.iloc[-5:].values
            slope = np.polyfit(x, y, 1)[0] / ma20.iloc[-3]
        else:
            slope = 0
        
        # 综合趋势强度
        trend_strength = (momentum_20d + momentum_60d + slope) / 3
        
        return {
            'trend_strength': trend_strength,
            'momentum_20d': momentum_20d,
            'momentum_60d': momentum_60d,
            'moving_average_slope': slope
        }
    
    def calculate_sector_rotation(self, sector_data: pd.DataFrame) -> Dict[str, float]:
        """
        计算行业轮动速度
        
        Args:
            sector_data: 行业指数数据 DataFrame，列为不同行业的收益率
            
        Returns:
            行业轮动指标字典
        """
        if len(sector_data) < self.lookback_period or sector_data.shape[1] < 2:
            return {
                'sector_rotation_speed': 0.5,
                'sector_dispersion': 0.01,
                'leading_sectors_change': 0
            }
        
        # 计算行业收益率的标准差（行业分散度）
        sector_returns = sector_data.pct_change()
        sector_dispersion = sector_returns.std().mean()
        
        # 计算行业轮动速度（领先行业变化频率）
        if len(sector_returns) >= 10:
            leading_sectors = sector_returns.rolling(window=5).mean().idxmax(axis=1)
            leading_sectors_change = (leading_sectors != leading_sectors.shift()).sum() / len(leading_sectors)
        else:
            leading_sectors_change = 0
        
        # 综合行业轮动速度
        sector_rotation_speed = (sector_dispersion + leading_sectors_change) / 2
        
        return {
            'sector_rotation_speed': sector_rotation_speed,
            'sector_dispersion': sector_dispersion,
            'leading_sectors_change': leading_sectors_change
        }
    
    def extract_features(self, index_data: pd.DataFrame, sector_data: Optional[pd.DataFrame] = None) -> Dict[str, float]:
        """
        提取所有市场状态特征
        
        Args:
            index_data: 指数数据 DataFrame
            sector_data: 行业指数数据 DataFrame（可选）
            
        Returns:
            综合特征字典
        """
        features = {}
        
        # 提取市场宽度特征
        breadth_features = self.calculate_market_breadth(index_data)
        features.update(breadth_features)
        
        # 提取波动率特征
        volatility_features = self.calculate_volatility(index_data)
        features.update(volatility_features)
        
        # 提取趋势强度特征
        trend_features = self.calculate_trend_strength(index_data)
        features.update(trend_features)
        
        # 提取行业轮动特征（如果有行业数据）
        if sector_data is not None:
            sector_features = self.calculate_sector_rotation(sector_data)
            features.update(sector_features)
        
        return features


class MarketStateClassifier:
    """市场状态分类器"""
    
    def __init__(self, n_states: int = 4, method: str = 'kmeans'):
        """
        初始化市场状态分类器
        
        Args:
            n_states: 市场状态数量
            method: 分类方法 ('kmeans')
        """
        self.n_states = n_states
        self.method = method
        self.scaler = StandardScaler()
        
        if method == 'kmeans':
            self.model = KMeans(n_clusters=n_states, random_state=42)
        else:
            raise ValueError(f"不支持的分类方法: {method}")
        
        self.is_trained = False
        self.state_labels = {}
    
    def train(self, features_list: List[Dict[str, float]]):
        """
        训练市场状态分类器
        
        Args:
            features_list: 特征列表
        """
        if len(features_list) < 10:
            raise ValueError("训练数据不足")
        
        # 转换为DataFrame
        features_df = pd.DataFrame(features_list)
        
        # 标准化特征
        X = self.scaler.fit_transform(features_df)
        
        # 训练模型
        self.model.fit(X)
        
        self.is_trained = True
        
        # 为每个状态生成标签
        self._generate_state_labels(features_df)
    
    def _generate_state_labels(self, features_df: pd.DataFrame):
        """
        为每个市场状态生成标签
        
        Args:
            features_df: 特征DataFrame
        """
        if self.method == 'kmeans':
            # 计算每个集群的中心特征
            centers = self.model.cluster_centers_
            scaled_centers = self.scaler.inverse_transform(centers)
            
            for i in range(self.n_states):
                center = scaled_centers[i]
                
                # 根据特征值判断状态类型
                trend = center[features_df.columns.get_loc('trend_strength')]
                volatility = center[features_df.columns.get_loc('volatility_20d')]
                breadth = center[features_df.columns.get_loc('advancers_ratio')]
                
                if trend > 0.02 and volatility < 0.15 and breadth > 0.6:
                    label = '牛市'
                elif trend < -0.02 and volatility > 0.2 and breadth < 0.4:
                    label = '熊市'
                elif abs(trend) < 0.01 and volatility < 0.15:
                    label = '震荡市'
                else:
                    label = '高波动市'
                
                self.state_labels[i] = label
        
    def predict(self, features: Dict[str, float]) -> Tuple[int, str, Dict[str, float]]:
        """
        预测市场状态
        
        Args:
            features: 市场特征
            
        Returns:
            (状态ID, 状态标签, 状态概率)
        """
        if not self.is_trained:
            raise ValueError("模型尚未训练")
        
        # 转换特征为数组
        features_df = pd.DataFrame([features])
        X = self.scaler.transform(features_df)
        
        # 预测状态
        state_id = self.model.predict(X)[0]
        # KMeans没有概率，返回均匀分布
        probabilities = {i: 1/self.n_states for i in range(self.n_states)}
        
        # 获取状态标签
        state_label = self.state_labels.get(state_id, f'状态{state_id}')
        
        return state_id, state_label, probabilities


class StrategyTimingSystem:
    """策略择时系统"""
    
    def __init__(self, market_state_classifier: MarketStateClassifier):
        """
        初始化策略择时系统
        
        Args:
            market_state_classifier: 市场状态分类器
        """
        self.market_state_classifier = market_state_classifier
        self.strategy_performance = {}  # 不同状态下的策略表现
        self.current_state = None
        self.state_history = []
    
    def add_strategy_performance(self, strategy_name: str, state_performance: Dict[str, float]):
        """
        添加策略在不同市场状态下的表现
        
        Args:
            strategy_name: 策略名称
            state_performance: 不同状态下的表现 {state_label: 夏普比率}
        """
        self.strategy_performance[strategy_name] = state_performance
    
    def calculate_optimal_weights(self, current_state: str) -> Dict[str, float]:
        """
        根据当前市场状态计算最优策略权重
        
        Args:
            current_state: 当前市场状态
            
        Returns:
            策略权重字典
        """
        weights = {}
        total_score = 0
        
        # 计算每个策略在当前状态下的得分
        for strategy, performance in self.strategy_performance.items():
            score = performance.get(current_state, 0)
            if score > 0:
                weights[strategy] = score
                total_score += score
        
        # 归一化权重
        if total_score > 0:
            weights = {k: v/total_score for k, v in weights.items()}
        else:
            # 如果所有策略在当前状态下表现都不好，平均分配
            n_strategies = len(self.strategy_performance)
            weights = {k: 1/n_strategies for k in self.strategy_performance.keys()}
        
        return weights
    
    def update_market_state(self, features: Dict[str, float]) -> Dict:
        """
        更新市场状态并计算最优策略权重
        
        Args:
            features: 市场特征
            
        Returns:
            包含市场状态和策略权重的字典
        """
        # 预测市场状态
        state_id, state_label, probabilities = self.market_state_classifier.predict(features)
        
        # 更新当前状态
        self.current_state = state_label
        self.state_history.append({
            'timestamp': datetime.now(),
            'state_id': state_id,
            'state_label': state_label,
            'probabilities': probabilities
        })
        
        # 计算最优策略权重
        optimal_weights = self.calculate_optimal_weights(state_label)
        
        return {
            'market_state': state_label,
            'state_probabilities': probabilities,
            'optimal_strategy_weights': optimal_weights,
            'timestamp': datetime.now()
        }
    
    def get_state_history(self, lookback: int = 30) -> List[Dict]:
        """
        获取市场状态历史
        
        Args:
            lookback: 回溯期数
            
        Returns:
            市场状态历史列表
        """
        return self.state_history[-lookback:]
    
    def get_strategy_recommendations(self) -> Dict[str, str]:
        """
        获取策略推荐
        
        Returns:
            策略推荐字典 {strategy: recommendation}
        """
        if not self.current_state:
            return {}
        
        recommendations = {}
        
        for strategy, performance in self.strategy_performance.items():
            score = performance.get(self.current_state, 0)
            
            if score > 1.5:
                recommendation = '强烈推荐'
            elif score > 1.0:
                recommendation = '推荐'
            elif score > 0.5:
                recommendation = '谨慎推荐'
            else:
                recommendation = '不推荐'
            
            recommendations[strategy] = recommendation
        
        return recommendations


class MarketStateManager:
    """市场状态管理管理器"""
    
    def __init__(self):
        """
        初始化市场状态管理器
        """
        self.features_extractor = MarketStateFeatures()
        self.classifier = MarketStateClassifier()
        self.timing_system = StrategyTimingSystem(self.classifier)
    
    def train(self, historical_data: pd.DataFrame, sector_data: Optional[pd.DataFrame] = None):
        """
        训练市场状态分类器
        
        Args:
            historical_data: 历史指数数据
            sector_data: 历史行业数据（可选）
        """
        # 提取历史特征
        features_list = []
        for i in range(self.features_extractor.lookback_period, len(historical_data)):
            window_data = historical_data.iloc[i-self.features_extractor.lookback_period:i]
            
            if sector_data is not None:
                window_sector = sector_data.iloc[i-self.features_extractor.lookback_period:i]
            else:
                window_sector = None
            
            features = self.features_extractor.extract_features(window_data, window_sector)
            features_list.append(features)
        
        # 训练分类器
        self.classifier.train(features_list)
    
    def update(self, current_data: pd.DataFrame, sector_data: Optional[pd.DataFrame] = None) -> Dict:
        """
        更新市场状态
        
        Args:
            current_data: 当前指数数据
            sector_data: 当前行业数据（可选）
            
        Returns:
            市场状态更新结果
        """
        # 提取当前特征
        features = self.features_extractor.extract_features(current_data, sector_data)
        
        # 更新市场状态
        result = self.timing_system.update_market_state(features)
        result['features'] = features
        
        return result
    
    def add_strategy(self, strategy_name: str, performance: Dict[str, float]):
        """
        添加策略及其在不同市场状态下的表现
        
        Args:
            strategy_name: 策略名称
            performance: 不同市场状态下的表现 {state_label: 夏普比率}
        """
        self.timing_system.add_strategy_performance(strategy_name, performance)
    
    def get_state_summary(self) -> Dict:
        """
        获取市场状态摘要
        
        Returns:
            市场状态摘要
        """
        if not self.timing_system.current_state:
            return {
                'current_state': '未知',
                'recommendations': {},
                'state_history': []
            }
        
        return {
            'current_state': self.timing_system.current_state,
            'recommendations': self.timing_system.get_strategy_recommendations(),
            'state_history': self.timing_system.get_state_history()
        }


# 测试代码
if __name__ == "__main__":
    # 创建测试数据
    np.random.seed(42)
    n_days = 200
    dates = pd.date_range(start='2023-01-01', periods=n_days, freq='B')
    
    # 创建模拟指数数据
    base_price = 3000
    returns = np.random.normal(0.0005, 0.01, n_days)
    prices = base_price * (1 + returns).cumprod()
    
    index_data = pd.DataFrame({
        'date': dates,
        'close': prices
    })
    
    # 创建模拟行业数据
    n_sectors = 5
    sector_returns = np.random.normal(0.0005, 0.015, (n_days, n_sectors))
    sector_prices = 100 * (1 + sector_returns).cumprod(axis=0)
    
    sector_data = pd.DataFrame(
        sector_prices,
        index=dates,
        columns=[f'行业{i}' for i in range(n_sectors)]
    )
    
    # 初始化市场状态管理器
    manager = MarketStateManager()
    
    # 训练模型
    print("训练市场状态分类器...")
    manager.train(index_data, sector_data)
    
    # 添加策略表现
    manager.add_strategy('价值策略', {
        '牛市': 1.2,
        '熊市': 0.8,
        '震荡市': 1.5,
        '高波动市': 0.6
    })
    
    manager.add_strategy('动量策略', {
        '牛市': 1.8,
        '熊市': 0.4,
        '震荡市': 0.7,
        '高波动市': 1.1
    })
    
    manager.add_strategy('反转策略', {
        '牛市': 0.9,
        '熊市': 1.3,
        '震荡市': 1.2,
        '高波动市': 1.4
    })
    
    # 更新市场状态
    print("\n更新市场状态...")
    result = manager.update(index_data.tail(30), sector_data.tail(30))
    
    print(f"当前市场状态: {result['market_state']}")
    print("\n最优策略权重:")
    for strategy, weight in result['optimal_strategy_weights'].items():
        print(f"  {strategy}: {weight:.2f}")
    
    print("\n策略推荐:")
    recommendations = manager.get_state_summary()['recommendations']
    for strategy, recommendation in recommendations.items():
        print(f"  {strategy}: {recommendation}")
