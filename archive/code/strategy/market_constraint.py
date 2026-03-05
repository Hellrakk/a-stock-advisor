#!/usr/bin/env python3
"""
市场约束模块 - 处理涨跌停、停牌等交易限制
确保回测结果更接近实际交易情况
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import warnings
warnings.filterwarnings('ignore')


class TradingStatus(Enum):
    """交易状态枚举"""
    NORMAL = "normal"
    LIMIT_UP = "limit_up"
    LIMIT_DOWN = "limit_down"
    SUSPENDED = "suspended"
    DELISTED = "delisted"


@dataclass
class MarketConstraintConfig:
    """市场约束配置"""
    limit_up_threshold: float = 0.10
    limit_down_threshold: float = -0.10
    st_limit_up: float = 0.05
    st_limit_down: float = -0.05
    volume_participation_rate: float = 0.05
    min_trading_value: float = 10000.0


class MarketConstraintChecker:
    """市场约束检查器"""
    
    def __init__(self, config: Optional[MarketConstraintConfig] = None):
        self.config = config or MarketConstraintConfig()
    
    def check_limit_up(self, current_price: float, prev_close: float, 
                       is_st: bool = False) -> bool:
        if prev_close <= 0:
            return False
        threshold = self.config.st_limit_up if is_st else self.config.limit_up_threshold
        limit_price = prev_close * (1 + threshold)
        return current_price >= limit_price * 0.998
    
    def check_limit_down(self, current_price: float, prev_close: float,
                         is_st: bool = False) -> bool:
        if prev_close <= 0:
            return False
        threshold = self.config.st_limit_down if is_st else self.config.limit_down_threshold
        limit_price = prev_close * (1 + threshold)
        return current_price <= limit_price * 1.002
    
    def check_trading_status(self, stock_data: pd.Series, 
                            prev_close: Optional[float] = None) -> TradingStatus:
        if stock_data.get('is_suspended', 0) == 1:
            return TradingStatus.SUSPENDED
        
        if stock_data.get('is_delisted', 0) == 1:
            return TradingStatus.DELISTED
        
        current_price = stock_data.get('close', 0)
        if current_price <= 0:
            return TradingStatus.SUSPENDED
        
        if prev_close is None:
            prev_close = stock_data.get('prev_close', current_price)
        
        is_st = self._is_st_stock(stock_data)
        
        if self.check_limit_up(current_price, prev_close, is_st):
            return TradingStatus.LIMIT_UP
        
        if self.check_limit_down(current_price, prev_close, is_st):
            return TradingStatus.LIMIT_DOWN
        
        return TradingStatus.NORMAL
    
    def _is_st_stock(self, stock_data: pd.Series) -> bool:
        stock_name = stock_data.get('股票名称', '')
        if pd.isna(stock_name):
            return False
        return str(stock_name).startswith('ST') or str(stock_name).startswith('*ST')
    
    def can_buy(self, stock_data: pd.Series, 
                prev_close: Optional[float] = None) -> Tuple[bool, str]:
        status = self.check_trading_status(stock_data, prev_close)
        
        if status == TradingStatus.SUSPENDED:
            return False, "股票停牌，无法买入"
        
        if status == TradingStatus.DELISTED:
            return False, "股票已退市，无法买入"
        
        if status == TradingStatus.LIMIT_UP:
            return False, "股票涨停，无法买入"
        
        return True, "可以买入"
    
    def can_sell(self, stock_data: pd.Series,
                 prev_close: Optional[float] = None) -> Tuple[bool, str]:
        status = self.check_trading_status(stock_data, prev_close)
        
        if status == TradingStatus.SUSPENDED:
            return False, "股票停牌，无法卖出"
        
        if status == TradingStatus.DELISTED:
            return False, "股票已退市，无法卖出"
        
        if status == TradingStatus.LIMIT_DOWN:
            return False, "股票跌停，无法卖出"
        
        return True, "可以卖出"
    
    def get_limit_price(self, prev_close: float, is_st: bool = False,
                        direction: str = 'up') -> float:
        if direction == 'up':
            threshold = self.config.st_limit_up if is_st else self.config.limit_up_threshold
        else:
            threshold = self.config.st_limit_down if is_st else self.config.limit_down_threshold
        
        return round(prev_close * (1 + threshold), 2)


class LiquidityConstraintChecker:
    """流动性约束检查器"""
    
    def __init__(self, config: Optional[MarketConstraintConfig] = None):
        self.config = config or MarketConstraintConfig()
    
    def check_volume_constraint(self, target_quantity: int,
                                daily_volume: float,
                                participation_rate: Optional[float] = None) -> Tuple[bool, int, str]:
        if participation_rate is None:
            participation_rate = self.config.volume_participation_rate
        
        if daily_volume <= 0:
            return False, 0, "无成交量数据"
        
        max_quantity = int(daily_volume * participation_rate)
        
        if target_quantity <= max_quantity:
            return True, target_quantity, "成交量约束满足"
        else:
            return True, max_quantity, f"成交量约束：目标{target_quantity}调整为{max_quantity}"
    
    def check_value_constraint(self, target_value: float,
                               min_value: Optional[float] = None) -> Tuple[bool, float, str]:
        if min_value is None:
            min_value = self.config.min_trading_value
        
        if target_value < min_value:
            return False, 0, f"交易金额{target_value:.2f}小于最小交易金额{min_value}"
        
        return True, target_value, "交易金额约束满足"
    
    def estimate_impact_cost(self, target_value: float, 
                            daily_turnover: float,
                            avg_spread: float = 0.002) -> float:
        if daily_turnover <= 0:
            return target_value * 0.01
        
        participation = target_value / daily_turnover
        
        spread_cost = target_value * avg_spread
        
        if participation > 0.1:
            impact_cost = target_value * participation * 0.5
        elif participation > 0.05:
            impact_cost = target_value * participation * 0.3
        else:
            impact_cost = target_value * participation * 0.1
        
        return spread_cost + impact_cost


class MarketConstraintModule:
    """市场约束模块 - 整合涨跌停、停牌、流动性约束"""
    
    def __init__(self, config: Optional[MarketConstraintConfig] = None):
        self.config = config or MarketConstraintConfig()
        self.status_checker = MarketConstraintChecker(self.config)
        self.liquidity_checker = LiquidityConstraintChecker(self.config)
        
        self.trade_records: List[Dict] = []
        self.constraint_stats: Dict = {
            'limit_up_blocked': 0,
            'limit_down_blocked': 0,
            'suspended_blocked': 0,
            'volume_adjusted': 0,
            'value_blocked': 0
        }
    
    def check_buy_constraint(self, stock_code: str, stock_data: pd.Series,
                            target_quantity: int, target_value: float,
                            prev_close: Optional[float] = None) -> Dict:
        result = {
            'stock_code': stock_code,
            'action': 'buy',
            'allowed': False,
            'adjusted_quantity': 0,
            'adjusted_value': 0,
            'reasons': [],
            'warnings': []
        }
        
        can_buy, buy_reason = self.status_checker.can_buy(stock_data, prev_close)
        if not can_buy:
            result['reasons'].append(buy_reason)
            if "涨停" in buy_reason:
                self.constraint_stats['limit_up_blocked'] += 1
            elif "停牌" in buy_reason:
                self.constraint_stats['suspended_blocked'] += 1
            return result
        
        vol_allowed, adj_quantity, vol_reason = self.liquidity_checker.check_volume_constraint(
            target_quantity, stock_data.get('volume', 0)
        )
        
        if adj_quantity < target_quantity:
            result['warnings'].append(vol_reason)
            self.constraint_stats['volume_adjusted'] += 1
        
        val_allowed, adj_value, val_reason = self.liquidity_checker.check_value_constraint(
            target_value
        )
        if not val_allowed:
            result['reasons'].append(val_reason)
            self.constraint_stats['value_blocked'] += 1
            return result
        
        result['allowed'] = True
        result['adjusted_quantity'] = adj_quantity
        result['adjusted_value'] = adj_value
        result['reasons'].append("交易约束检查通过")
        
        self.trade_records.append(result)
        return result
    
    def check_sell_constraint(self, stock_code: str, stock_data: pd.Series,
                             target_quantity: int, target_value: float,
                             prev_close: Optional[float] = None) -> Dict:
        result = {
            'stock_code': stock_code,
            'action': 'sell',
            'allowed': False,
            'adjusted_quantity': 0,
            'adjusted_value': 0,
            'reasons': [],
            'warnings': []
        }
        
        can_sell, sell_reason = self.status_checker.can_sell(stock_data, prev_close)
        if not can_sell:
            result['reasons'].append(sell_reason)
            if "跌停" in sell_reason:
                self.constraint_stats['limit_down_blocked'] += 1
            elif "停牌" in sell_reason:
                self.constraint_stats['suspended_blocked'] += 1
            return result
        
        vol_allowed, adj_quantity, vol_reason = self.liquidity_checker.check_volume_constraint(
            target_quantity, stock_data.get('volume', 0)
        )
        
        if adj_quantity < target_quantity:
            result['warnings'].append(vol_reason)
            self.constraint_stats['volume_adjusted'] += 1
        
        result['allowed'] = True
        result['adjusted_quantity'] = adj_quantity
        result['adjusted_value'] = adj_value
        result['reasons'].append("交易约束检查通过")
        
        self.trade_records.append(result)
        return result
    
    def get_constraint_stats(self) -> Dict:
        return self.constraint_stats.copy()
    
    def reset_stats(self):
        self.constraint_stats = {
            'limit_up_blocked': 0,
            'limit_down_blocked': 0,
            'suspended_blocked': 0,
            'volume_adjusted': 0,
            'value_blocked': 0
        }
        self.trade_records = []
    
    def generate_constraint_report(self) -> str:
        lines = []
        lines.append("=" * 60)
        lines.append("市场约束统计报告")
        lines.append("=" * 60)
        lines.append(f"涨停限制买入: {self.constraint_stats['limit_up_blocked']} 次")
        lines.append(f"跌停限制卖出: {self.constraint_stats['limit_down_blocked']} 次")
        lines.append(f"停牌限制交易: {self.constraint_stats['suspended_blocked']} 次")
        lines.append(f"成交量调整: {self.constraint_stats['volume_adjusted']} 次")
        lines.append(f"金额限制: {self.constraint_stats['value_blocked']} 次")
        lines.append("=" * 60)
        return "\n".join(lines)


def prepare_market_data(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    
    if 'prev_close' not in data.columns:
        data = data.sort_values(['stock_code', 'date'])
        data['prev_close'] = data.groupby('stock_code')['close'].shift(1)
        data['prev_close'] = data['prev_close'].fillna(data['close'])
    
    if 'is_suspended' not in data.columns:
        data['is_suspended'] = 0
    
    if 'volume' not in data.columns:
        data['volume'] = 0
    
    return data


if __name__ == '__main__':
    print("=" * 60)
    print("市场约束模块测试")
    print("=" * 60)
    
    config = MarketConstraintConfig()
    module = MarketConstraintModule(config)
    
    test_data = pd.DataFrame({
        'stock_code': ['000001', '000002', '600000', '600036'],
        'close': [10.00, 11.00, 20.00, 15.00],
        'prev_close': [9.09, 10.00, 18.18, 15.00],
        'volume': [1000000, 2000000, 500000, 3000000],
        '股票名称': ['平安银行', '万科A', '浦发银行', 'ST招商'],
        'is_suspended': [0, 0, 0, 0]
    })
    
    print("\n测试1: 涨停股票买入检查")
    result = module.check_buy_constraint(
        '000001', test_data.iloc[0], 
        target_quantity=1000, target_value=10000
    )
    print(f"  结果: {result['allowed']}, 原因: {result['reasons']}")
    
    print("\n测试2: 正常股票买入检查")
    result = module.check_buy_constraint(
        '000002', test_data.iloc[1],
        target_quantity=1000, target_value=11000
    )
    print(f"  结果: {result['allowed']}, 调整数量: {result['adjusted_quantity']}")
    
    print("\n测试3: 跌停股票卖出检查")
    result = module.check_sell_constraint(
        '600036', test_data.iloc[3],
        target_quantity=1000, target_value=15000
    )
    print(f"  结果: {result['allowed']}, 原因: {result['reasons']}")
    
    print("\n" + module.generate_constraint_report())
