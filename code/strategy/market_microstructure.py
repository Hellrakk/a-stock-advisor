#!/usr/bin/env python3
"""
市场微观结构模型
功能：
1. 订单簿建模
2. 流动性分析
3. 价格冲击模型
4. 市场深度分析
5. 交易成本估算
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class OrderBookLevel:
    """订单簿层级"""
    price: float
    size: int
    total_size: int = 0


class OrderBook:
    """订单簿模型"""
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: List[OrderBookLevel] = []  # 买单
        self.asks: List[OrderBookLevel] = []  # 卖单
        self.timestamp: Optional[datetime] = None
    
    def update(self, bids: List[Tuple[float, int]], asks: List[Tuple[float, int]], timestamp: datetime):
        """更新订单簿"""
        # 按价格排序
        self.bids = []
        total_bid_size = 0
        for price, size in sorted(bids, key=lambda x: -x[0]):
            total_bid_size += size
            self.bids.append(OrderBookLevel(price=price, size=size, total_size=total_bid_size))
        
        self.asks = []
        total_ask_size = 0
        for price, size in sorted(asks, key=lambda x: x[0]):
            total_ask_size += size
            self.asks.append(OrderBookLevel(price=price, size=size, total_size=total_ask_size))
        
        self.timestamp = timestamp
    
    def get_bid_price(self, level: int = 0) -> float:
        """获取买单价格"""
        if len(self.bids) > level:
            return self.bids[level].price
        return 0.0
    
    def get_ask_price(self, level: int = 0) -> float:
        """获取卖单价格"""
        if len(self.asks) > level:
            return self.asks[level].price
        return 0.0
    
    def get_spread(self) -> float:
        """获取买卖价差"""
        bid = self.get_bid_price()
        ask = self.get_ask_price()
        if bid > 0 and ask > 0:
            return ask - bid
        return 0.0
    
    def get_mid_price(self) -> float:
        """获取中间价"""
        bid = self.get_bid_price()
        ask = self.get_ask_price()
        if bid > 0 and ask > 0:
            return (bid + ask) / 2
        return 0.0
    
    def get_market_depth(self, side: str, depth: int = 5) -> Dict:
        """获取市场深度"""
        if side == 'bid':
            levels = self.bids[:depth]
        else:
            levels = self.asks[:depth]
        
        depth_data = {}
        for i, level in enumerate(levels):
            depth_data[f'level_{i+1}'] = {
                'price': level.price,
                'size': level.size,
                'total_size': level.total_size
            }
        
        return depth_data
    
    def estimate_execution_cost(self, order_size: int, side: str) -> Tuple[float, float]:
        """估算执行成本
        
        Args:
            order_size: 订单大小
            side: 'buy' 或 'sell'
            
        Returns:
            (执行价格, 价格冲击)
        """
        if side == 'buy':
            levels = self.asks
            base_price = self.get_bid_price()
        else:
            levels = self.bids
            base_price = self.get_ask_price()
        
        if not levels or base_price == 0:
            return 0.0, 0.0
        
        remaining_size = order_size
        total_cost = 0.0
        
        for level in levels:
            if remaining_size <= 0:
                break
            
            execute_size = min(remaining_size, level.size)
            total_cost += execute_size * level.price
            remaining_size -= execute_size
        
        if order_size - remaining_size > 0:
            avg_price = total_cost / (order_size - remaining_size)
            price_impact = avg_price - base_price if side == 'buy' else base_price - avg_price
            return avg_price, price_impact
        
        return 0.0, 0.0


class LiquidityAnalyzer:
    """流动性分析器"""
    
    def __init__(self):
        self.order_books = {}
    
    def add_order_book(self, symbol: str, order_book: OrderBook):
        """添加订单簿"""
        self.order_books[symbol] = order_book
    
    def get_liquidity_metrics(self, symbol: str) -> Dict:
        """获取流动性指标"""
        if symbol not in self.order_books:
            return {}
        
        order_book = self.order_books[symbol]
        
        # 买卖价差
        spread = order_book.get_spread()
        mid_price = order_book.get_mid_price()
        
        # 相对价差
        relative_spread = spread / mid_price if mid_price > 0 else 0
        
        # 市场深度
        bid_depth = sum(level.size for level in order_book.bids[:5])
        ask_depth = sum(level.size for level in order_book.asks[:5])
        total_depth = bid_depth + ask_depth
        
        # 深度加权平均价格
        bid_weighted_price = 0.0
        ask_weighted_price = 0.0
        
        if bid_depth > 0:
            bid_weighted_price = sum(level.price * level.size for level in order_book.bids[:5]) / bid_depth
        
        if ask_depth > 0:
            ask_weighted_price = sum(level.price * level.size for level in order_book.asks[:5]) / ask_depth
        
        return {
            'symbol': symbol,
            'timestamp': order_book.timestamp,
            'spread': spread,
            'relative_spread': relative_spread,
            'mid_price': mid_price,
            'bid_depth': bid_depth,
            'ask_depth': ask_depth,
            'total_depth': total_depth,
            'bid_weighted_price': bid_weighted_price,
            'ask_weighted_price': ask_weighted_price
        }
    
    def analyze_market_liquidity(self, symbols: List[str]) -> pd.DataFrame:
        """分析多个股票的流动性"""
        metrics = []
        
        for symbol in symbols:
            metric = self.get_liquidity_metrics(symbol)
            if metric:
                metrics.append(metric)
        
        return pd.DataFrame(metrics)


class PriceImpactModel:
    """价格冲击模型"""
    
    def __init__(self, model_type: str = 'linear'):
        self.model_type = model_type
        self.params = {}
    
    def fit(self, order_sizes: List[int], price_impacts: List[float]):
        """拟合价格冲击模型"""
        X = np.array(order_sizes).reshape(-1, 1)
        y = np.array(price_impacts)
        
        if self.model_type == 'linear':
            # 线性模型: impact = a * size
            from sklearn.linear_model import LinearRegression
            model = LinearRegression()
            model.fit(X, y)
            self.params['slope'] = model.coef_[0]
            self.params['intercept'] = model.intercept_
        elif self.model_type == 'power':
            # 幂律模型: impact = a * size^b
            X_log = np.log(X + 1e-6)
            y_log = np.log(y + 1e-6)
            from sklearn.linear_model import LinearRegression
            model = LinearRegression()
            model.fit(X_log, y_log)
            self.params['slope'] = model.coef_[0]
            self.params['intercept'] = model.intercept_
    
    def predict(self, order_size: int) -> float:
        """预测价格冲击"""
        if not self.params:
            return 0.0
        
        if self.model_type == 'linear':
            return self.params['slope'] * order_size + self.params['intercept']
        elif self.model_type == 'power':
            return np.exp(self.params['intercept']) * (order_size ** self.params['slope'])
        
        return 0.0


class MarketMicrostructureModel:
    """市场微观结构模型"""
    
    def __init__(self):
        self.order_books = {}
        self.liquidity_analyzer = LiquidityAnalyzer()
        self.price_impact_models = {}
    
    def update_order_book(self, symbol: str, bids: List[Tuple[float, int]], 
                         asks: List[Tuple[float, int]], timestamp: datetime):
        """更新订单簿"""
        if symbol not in self.order_books:
            self.order_books[symbol] = OrderBook(symbol)
        
        order_book = self.order_books[symbol]
        order_book.update(bids, asks, timestamp)
        self.liquidity_analyzer.add_order_book(symbol, order_book)
    
    def get_order_book(self, symbol: str) -> Optional[OrderBook]:
        """获取订单簿"""
        return self.order_books.get(symbol)
    
    def get_liquidity_metrics(self, symbol: str) -> Dict:
        """获取流动性指标"""
        return self.liquidity_analyzer.get_liquidity_metrics(symbol)
    
    def estimate_execution_cost(self, symbol: str, order_size: int, side: str) -> Tuple[float, float]:
        """估算执行成本"""
        order_book = self.get_order_book(symbol)
        if order_book:
            return order_book.estimate_execution_cost(order_size, side)
        return 0.0, 0.0
    
    def train_price_impact_model(self, symbol: str, order_sizes: List[int], 
                               price_impacts: List[float], model_type: str = 'linear'):
        """训练价格冲击模型"""
        model = PriceImpactModel(model_type)
        model.fit(order_sizes, price_impacts)
        self.price_impact_models[symbol] = model
    
    def predict_price_impact(self, symbol: str, order_size: int) -> float:
        """预测价格冲击"""
        if symbol in self.price_impact_models:
            return self.price_impact_models[symbol].predict(order_size)
        return 0.0
    
    def generate_market_depth_report(self, symbol: str) -> Dict:
        """生成市场深度报告"""
        order_book = self.get_order_book(symbol)
        if not order_book:
            return {}
        
        report = {
            'symbol': symbol,
            'timestamp': order_book.timestamp,
            'spread': order_book.get_spread(),
            'mid_price': order_book.get_mid_price(),
            'bid_depth': order_book.get_market_depth('bid'),
            'ask_depth': order_book.get_market_depth('ask')
        }
        
        return report


class TradingCostEstimator:
    """交易成本估算器"""
    
    def __init__(self):
        self.microstructure_model = MarketMicrostructureModel()
        self.fixed_cost_per_trade = 5.0  # 固定交易成本
        self.fee_rate = 0.0003  # 交易费率
    
    def estimate_cost(self, symbol: str, order_size: int, side: str, 
                     price: Optional[float] = None) -> Dict:
        """估算交易成本"""
        # 估算执行价格和价格冲击
        exec_price, price_impact = self.microstructure_model.estimate_execution_cost(
            symbol, order_size, side
        )
        
        if price is None:
            order_book = self.microstructure_model.get_order_book(symbol)
            if order_book:
                price = exec_price or order_book.get_mid_price()
            else:
                price = exec_price or 0.0
        
        # 计算总成本
        total_value = order_size * price
        fee_cost = total_value * self.fee_rate
        impact_cost = order_size * price_impact
        total_cost = self.fixed_cost_per_trade + fee_cost + impact_cost
        
        return {
            'symbol': symbol,
            'order_size': order_size,
            'side': side,
            'price': price,
            'exec_price': exec_price,
            'price_impact': price_impact,
            'fixed_cost': self.fixed_cost_per_trade,
            'fee_cost': fee_cost,
            'impact_cost': impact_cost,
            'total_cost': total_cost,
            'cost_percentage': (total_cost / total_value) * 100 if total_value > 0 else 0
        }
    
    def compare_trading_strategies(self, symbol: str, total_size: int, 
                                 strategies: List[Dict]) -> List[Dict]:
        """比较不同交易策略的成本"""
        results = []
        
        for strategy in strategies:
            if strategy['type'] == 'one_shot':
                # 一次性下单
                cost = self.estimate_cost(symbol, total_size, strategy['side'])
                results.append({
                    'strategy': 'one_shot',
                    'total_cost': cost['total_cost'],
                    'cost_percentage': cost['cost_percentage'],
                    'details': cost
                })
            elif strategy['type'] == 'iceberg':
                # 冰山订单
                slice_size = strategy.get('slice_size', total_size // 5)
                slices = (total_size + slice_size - 1) // slice_size
                total_cost = 0.0
                total_value = 0.0
                
                for i in range(slices):
                    current_size = min(slice_size, total_size - i * slice_size)
                    cost = self.estimate_cost(symbol, current_size, strategy['side'])
                    total_cost += cost['total_cost']
                    total_value += current_size * cost['price']
                
                cost_percentage = (total_cost / total_value) * 100 if total_value > 0 else 0
                results.append({
                    'strategy': 'iceberg',
                    'slice_size': slice_size,
                    'slices': slices,
                    'total_cost': total_cost,
                    'cost_percentage': cost_percentage
                })
        
        return results


if __name__ == '__main__':
    print("=== 市场微观结构模型测试 ===")
    
    # 创建市场微观结构模型
    msm = MarketMicrostructureModel()
    
    # 模拟订单簿数据
    symbol = '600519'
    bids = [(1799.0, 100), (1798.5, 200), (1798.0, 300), (1797.5, 400), (1797.0, 500)]
    asks = [(1801.0, 100), (1801.5, 200), (1802.0, 300), (1802.5, 400), (1803.0, 500)]
    
    # 更新订单簿
    msm.update_order_book(symbol, bids, asks, datetime.now())
    
    # 获取流动性指标
    liquidity = msm.get_liquidity_metrics(symbol)
    print("\n流动性指标:")
    for key, value in liquidity.items():
        print(f"{key}: {value}")
    
    # 估算执行成本
    exec_price, impact = msm.estimate_execution_cost(symbol, 500, 'buy')
    print(f"\n执行成本估算 (买入500股):")
    print(f"执行价格: {exec_price:.2f}")
    print(f"价格冲击: {impact:.2f}")
    
    # 生成市场深度报告
    depth_report = msm.generate_market_depth_report(symbol)
    print("\n市场深度报告:")
    print(f"买卖价差: {depth_report['spread']:.2f}")
    print(f"中间价: {depth_report['mid_price']:.2f}")
    
    # 测试交易成本估算器
    cost_estimator = TradingCostEstimator()
    cost = cost_estimator.estimate_cost(symbol, 1000, 'buy')
    print("\n交易成本估算:")
    print(f"总成本: {cost['total_cost']:.2f}")
    print(f"成本百分比: {cost['cost_percentage']:.4f}%")
    
    # 比较交易策略
    strategies = [
        {'type': 'one_shot', 'side': 'buy'},
        {'type': 'iceberg', 'side': 'buy', 'slice_size': 200}
    ]
    comparison = cost_estimator.compare_trading_strategies(symbol, 1000, strategies)
    print("\n交易策略比较:")
    for result in comparison:
        print(f"策略: {result['strategy']}")
        print(f"总成本: {result['total_cost']:.2f}")
        print(f"成本百分比: {result['cost_percentage']:.4f}%")
        print()
