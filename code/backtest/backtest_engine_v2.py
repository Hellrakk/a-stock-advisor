#!/usr/bin/env python3
"""
回测引擎 V2 - 包含交易成本模型、改进成交逻辑、风险限制
确保无未来函数
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import warnings
warnings.filterwarnings('ignore')


@dataclass
class CostModel:
    """交易成本模型"""
    commission_rate: float = 0.0003  # 佣金费率 (万三)
    commission_min: float = 5.0      # 最低佣金
    stamp_tax_rate: float = 0.001    # 印花税率 (千一，仅卖出时收取)
    impact_cost_base: float = 0.0005 # 基础冲击成本
    impact_cost_sqrt: float = 0.001  # 冲击成本与订单总额平方根的系数
    
    def calculate_commission(self, amount: float) -> float:
        """
        计算佣金
        
        Args:
            amount: 订单金额
            
        Returns:
            佣金金额
        """
        commission = amount * self.commission_rate
        return max(commission, self.commission_min)
    
    def calculate_stamp_tax(self, amount: float, is_sell: bool) -> float:
        """
        计算印花税（仅卖出时收取）
        
        Args:
            amount: 订单金额
            is_sell: 是否卖出
            
        Returns:
            印花税金额
        """
        if not is_sell:
            return 0.0
        return amount * self.stamp_tax_rate
    
    def calculate_impact_cost(self, amount: float, liquidity: float = 1.0) -> float:
        """
        计算冲击成本
        
        Args:
            amount: 订单金额
            liquidity: 流动性因子 (值越大流动性越好，冲击成本越小)
            
        Returns:
            冲击成本金额
        """
        # 冲击成本模型：基础成本 + 与订单额平方根相关的成本
        order_factor = np.sqrt(amount) if amount > 0 else 0
        impact = (self.impact_cost_base * amount +
                  self.impact_cost_sqrt * order_factor * amount)
        return impact / liquidity
    
    def calculate_total_cost(self, amount: float, is_sell: bool, liquidity: float = 1.0) -> float:
        """
        计算总交易成本
        
        Args:
            amount: 订单金额
            is_sell: 是否卖出
            liquidity: 流动性因子
            
        Returns:
            总成本金额
        """
        commission = self.calculate_commission(amount)
        stamp_tax = self.calculate_stamp_tax(amount, is_sell)
        impact = self.calculate_impact_cost(amount, liquidity)
        return commission + stamp_tax + impact


@dataclass
class Order:
    """订单类"""
    stock_code: str
    side: str  # 'buy' or 'sell'
    order_type: str  # 'limit' or 'market'
    price: Optional[float]  # 限价单价格 (市价单为None)
    quantity: int
    date: str
    time: Optional[str] = None
    status: str = 'pending'  # 'pending', 'partial', 'filled', 'cancelled'
    filled_quantity: int = 0
    filled_price: Optional[float] = None
    
    def __post_init__(self):
        if self.side not in ['buy', 'sell']:
            raise ValueError(f"Invalid order side: {self.side}")
        if self.order_type not in ['limit', 'market']:
            raise ValueError(f"Invalid order type: {self.order_type}")


@dataclass
class Position:
    """持仓类"""
    stock_code: str
    quantity: int
    avg_price: float
    market_value: float = 0.0
    industry: Optional[str] = None
    entry_date: Optional[str] = None
    
    @property
    def weight(self):
        """仓位权重 (由portfolio计算)"""
        return None


class Portfolio:
    """投资组合类"""
    
    def __init__(self, initial_capital: float = 1000000.0):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions: Dict[str, Position] = {}  # stock_code -> Position
        self.orders: List[Order] = []
        self.history: List[Dict] = []
        
    @property
    def total_value(self) -> float:
        """总资产"""
        market_value = sum(pos.market_value for pos in self.positions.values())
        return self.cash + market_value
    
    @property
    def position_value(self) -> float:
        """持仓市值"""
        return sum(pos.market_value for pos in self.positions.values())
    
    def get_position(self, stock_code: str) -> Optional[Position]:
        """获取持仓"""
        return self.positions.get(stock_code)
    
    def has_position(self, stock_code: str) -> bool:
        """是否有持仓"""
        return stock_code in self.positions
    
    def get_position_weight(self, stock_code: str, total_value: float = None) -> float:
        """获取持仓权重"""
        if total_value is None:
            total_value = self.total_value
        
        if stock_code not in self.positions:
            return 0.0
        
        if total_value <= 0:
            return 0.0
        
        return self.positions[stock_code].market_value / total_value
    
    def get_industry_exposure(self, total_value: float = None) -> Dict[str, float]:
        """获取行业暴露"""
        if total_value is None:
            total_value = self.total_value
        
        industry_exposure = {}
        for pos in self.positions.values():
            if pos.industry:
                if pos.industry not in industry_exposure:
                    industry_exposure[pos.industry] = 0.0
                industry_exposure[pos.industry] += pos.market_value / total_value
        
        return industry_exposure
    
    def record_state(self, date: str, prices: Dict[str, float]):
        """记录投资组合状态"""
        state = {
            'date': date,
            'cash': self.cash,
            'position_value': self.position_value,
            'total_value': self.total_value,
            'stock_count': len(self.positions),
            'positions': {
                code: {
                    'quantity': pos.quantity,
                    'avg_price': pos.avg_price,
                    'market_value': pos.market_value,
                    'price': prices.get(code, pos.avg_price)
                }
                for code, pos in self.positions.items()
            }
        }
        self.history.append(state)
    
    def __repr__(self):
        return f"Portfolio(total={self.total_value:.2f}, cash={self.cash:.2f}, positions={len(self.positions)})"


class BacktestEngineV2:
    """回测引擎 V2 - 包含涨跌停、停牌、交易时间检测"""
    
    LIMIT_UP_PCT = 0.10  # 涨停幅度 10%
    LIMIT_DOWN_PCT = 0.10  # 跌停幅度 10%
    TRADING_START = '09:30'  # 开盘时间
    TRADING_END = '15:00'  # 收盘时间
    
    def __init__(self,
                 initial_capital: float = 1000000.0,
                 cost_model: Optional[CostModel] = None,
                 max_single_position: float = 0.10,
                 max_industry_exposure: float = 0.30,
                 max_drawdown: float = 0.30,
                 max_volatility: float = 0.20,
                 min_liquidity: float = 1000000,
                 max_positions: int = 20,
                 stop_loss: float = 0.10,
                 slippage_rate: float = 0.001,
                 fill_ratio: float = 0.95,
                 benchmark: str = 'hs300',
                 check_limit_up: bool = True,
                 check_limit_down: bool = True,
                 check_suspended: bool = True,
                 check_trading_time: bool = True):
        """
        初始化回测引擎
        
        Args:
            initial_capital: 初始资金
            cost_model: 成本模型
            max_single_position: 单票最大仓位比例
            max_industry_exposure: 单行业最大暴露比例
            max_drawdown: 最大回撤限制
            max_volatility: 最大波动率
            min_liquidity: 最小流动性（成交额）
            max_positions: 最大持仓数量
            stop_loss: 个股止损比例
            slippage_rate: 滑点率
            fill_ratio: 限价单成交比率
            benchmark: 基准指数
            check_limit_up: 是否检查涨停
            check_limit_down: 是否检查跌停
            check_suspended: 是否检查停牌
            check_trading_time: 是否检查交易时间
        """
        self.initial_capital = initial_capital
        self.cost_model = cost_model or CostModel()
        self.max_single_position = max_single_position
        self.max_industry_exposure = max_industry_exposure
        self.max_drawdown = max_drawdown
        self.max_volatility = max_volatility
        self.min_liquidity = min_liquidity
        self.max_positions = max_positions
        self.stop_loss = stop_loss
        self.slippage_rate = slippage_rate
        self.fill_ratio = fill_ratio
        self.benchmark = benchmark
        self.check_limit_up = check_limit_up
        self.check_limit_down = check_limit_down
        self.check_suspended = check_suspended
        self.check_trading_time = check_trading_time
        
        self.portfolio = Portfolio(initial_capital)
        self.trades: List[Dict] = []
        self.dates: List[str] = []
        self.running_max = initial_capital
        self.max_drawdown_reached = False
        self.benchmark_returns = []
        self.benchmark_values = []
        self.suspended_stocks = set()  # 停牌股票集合
        self.limit_up_stocks = set()   # 涨停股票集合
        self.limit_down_stocks = set() # 跌停股票集合
    
    def _check_limit_up(self, stock_code: str, price_df: pd.DataFrame) -> bool:
        """
        检查股票是否涨停
        
        Args:
            stock_code: 股票代码
            price_df: 价格数据
            
        Returns:
            是否涨停
        """
        if not self.check_limit_up:
            return False
        
        stock_data = price_df[price_df['stock_code'] == stock_code]
        if len(stock_data) == 0:
            return False
        
        row = stock_data.iloc[0]
        
        if 'pct_chg' in row:
            pct_chg = row['pct_chg']
        elif 'close' in row and 'pre_close' in row:
            pct_chg = (row['close'] - row['pre_close']) / row['pre_close']
        else:
            return False
        
        if pct_chg >= self.LIMIT_UP_PCT * 0.99:
            self.limit_up_stocks.add(stock_code)
            return True
        
        return False
    
    def _check_limit_down(self, stock_code: str, price_df: pd.DataFrame) -> bool:
        """
        检查股票是否跌停
        
        Args:
            stock_code: 股票代码
            price_df: 价格数据
            
        Returns:
            是否跌停
        """
        if not self.check_limit_down:
            return False
        
        stock_data = price_df[price_df['stock_code'] == stock_code]
        if len(stock_data) == 0:
            return False
        
        row = stock_data.iloc[0]
        
        if 'pct_chg' in row:
            pct_chg = row['pct_chg']
        elif 'close' in row and 'pre_close' in row:
            pct_chg = (row['close'] - row['pre_close']) / row['pre_close']
        else:
            return False
        
        if pct_chg <= -self.LIMIT_DOWN_PCT * 0.99:
            self.limit_down_stocks.add(stock_code)
            return True
        
        return False
    
    def _check_suspended(self, stock_code: str, price_df: pd.DataFrame) -> bool:
        """
        检查股票是否停牌
        
        Args:
            stock_code: 股票代码
            price_df: 价格数据
            
        Returns:
            是否停牌
        """
        if not self.check_suspended:
            return False
        
        stock_data = price_df[price_df['stock_code'] == stock_code]
        
        if len(stock_data) == 0:
            self.suspended_stocks.add(stock_code)
            return True
        
        row = stock_data.iloc[0]
        
        if 'volume' in row and row['volume'] == 0:
            if 'amount' in row and row['amount'] == 0:
                self.suspended_stocks.add(stock_code)
                return True
        
        if 'status' in row and row['status'] in ['停牌', 'suspended']:
            self.suspended_stocks.add(stock_code)
            return True
        
        return False
    
    def _check_trading_time(self, date: str, time_str: Optional[str] = None) -> bool:
        """
        检查是否在交易时间内
        
        Args:
            date: 日期
            time_str: 时间字符串 (HH:MM格式)
            
        Returns:
            是否在交易时间内
        """
        if not self.check_trading_time:
            return True
        
        import datetime
        
        try:
            if isinstance(date, str):
                date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
            else:
                date_obj = date
            
            if date_obj.weekday() >= 5:
                return False
            
            if time_str is None:
                return True
            
            time_obj = datetime.datetime.strptime(time_str, '%H:%M').time()
            start_time = datetime.datetime.strptime(self.TRADING_START, '%H:%M').time()
            end_time = datetime.datetime.strptime(self.TRADING_END, '%H:%M').time()
            
            morning_end = datetime.datetime.strptime('11:30', '%H:%M').time()
            afternoon_start = datetime.datetime.strptime('13:00', '%H:%M').time()
            
            if start_time <= time_obj <= morning_end:
                return True
            if afternoon_start <= time_obj <= end_time:
                return True
            
            return False
            
        except Exception:
            return True
    
    def _can_trade(self, stock_code: str, side: str, price_df: pd.DataFrame) -> Tuple[bool, str]:
        """
        检查股票是否可以交易
        
        Args:
            stock_code: 股票代码
            side: 交易方向 ('buy' or 'sell')
            price_df: 价格数据
            
        Returns:
            (是否可交易, 原因)
        """
        if self._check_suspended(stock_code, price_df):
            return False, "股票停牌"
        
        if side == 'buy' and self._check_limit_up(stock_code, price_df):
            return False, "股票涨停，无法买入"
        
        if side == 'sell' and self._check_limit_down(stock_code, price_df):
            return False, "股票跌停，无法卖出"
        
        return True, "可交易"
    
    def _get_market_price(self, stock_code: str, date: str, 
                         price_df: pd.DataFrame, order_type: str = 'market',
                         limit_price: Optional[float] = None) -> Tuple[float, float]:
        """
        获取市场价格（考虑滑点）
        
        Args:
            stock_code: 股票代码
            date: 日期
            price_df: 价格数据
            order_type: 订单类型
            limit_price: 限价单价格
            
        Returns:
            (成交价格, 是否成交)
        """
        # 从价格数据中获取当日价格
        stock_data = price_df[price_df['stock_code'] == stock_code]
        
        if len(stock_data) == 0:
            return None, False
        
        stock_price = stock_data['close'].values[0]
        
        # 市价单：考虑滑点
        if order_type == 'market':
            # 买入用稍高的价格，卖出用稍低的价格
            slippage = stock_price * self.slippage_rate
            filled_price = stock_price + slippage
            return filled_price, True
        
        # 限价单：检查是否可以成交
        elif order_type == 'limit':
            if limit_price is None:
                raise ValueError("Limit order must have limit price")
            
            if stock_price <= limit_price:
                # 可以成交，但可能部分成交
                if np.random.rand() < self.fill_ratio:
                    return stock_price, True
                else:
                    return stock_price, False
            else:
                # 无法成交
                return stock_price, False
        
        else:
            raise ValueError(f"Invalid order type: {order_type}")
    
    def _check_position_limit(self, stock_code: str, target_value: float) -> bool:
        """
        检查单票仓位限制
        
        Args:
            stock_code: 股票代码
            target_value: 目标持仓金额
            
        Returns:
            是否满足限制
        """
        current_weight = self.portfolio.get_position_weight(stock_code)
        target_weight = target_value / self.portfolio.total_value
        
        return (current_weight + target_weight) <= self.max_single_position
    
    def _check_industry_limit(self, stock_code: str, target_value: float,
                             stock_info: pd.DataFrame) -> bool:
        """
        检查行业暴露限制
        
        Args:
            stock_code: 股票代码
            target_value: 目标持仓金额
            stock_info: 股票信息数据
            
        Returns:
            是否满足限制
        """
        # 获取股票行业
        stock_row = stock_info[stock_info['stock_code'] == stock_code]
        if len(stock_row) == 0:
            return True  # 无行业信息时通过
        
        industry = stock_row.iloc[0].get('industry', 'unknown')
        
        # 计算当前行业暴露
        industry_exposure = self.portfolio.get_industry_exposure()
        current_exposure = industry_exposure.get(industry, 0.0)
        target_exposure = target_value / self.portfolio.total_value
        
        return (current_exposure + target_exposure) <= self.max_industry_exposure
    
    def _check_liquidity(self, stock_code: str, date: str, price_df: pd.DataFrame) -> bool:
        """
        检查股票流动性
        
        Args:
            stock_code: 股票代码
            date: 日期
            price_df: 价格数据
            
        Returns:
            是否满足流动性要求
        """
        stock_data = price_df[price_df['stock_code'] == stock_code]
        if len(stock_data) == 0:
            return False
        
        amount = stock_data['amount'].values[0]
        return amount >= self.min_liquidity
    
    def _check_position_count(self) -> bool:
        """
        检查持仓数量限制
        
        Returns:
            是否满足持仓数量限制
        """
        return len(self.portfolio.positions) < self.max_positions
    
    def _check_stop_loss(self, stock_code: str, current_price: float) -> bool:
        """
        检查个股止损
        
        Args:
            stock_code: 股票代码
            current_price: 当前价格
            
        Returns:
            是否触发止损
        """
        pos = self.portfolio.get_position(stock_code)
        if pos is None:
            return False
        
        loss_ratio = (current_price - pos.avg_price) / pos.avg_price
        return loss_ratio <= -self.stop_loss
    
    def _check_max_drawdown(self) -> bool:
        """
        检查最大回撤限制
        
        Returns:
            是否触发最大回撤限制
        """
        current_value = self.portfolio.total_value
        self.running_max = max(self.running_max, current_value)
        drawdown = (self.running_max - current_value) / self.running_max
        
        if drawdown >= self.max_drawdown:
            self.max_drawdown_reached = True
            return True
        return False
    
    def _check_volatility(self, returns: List[float]) -> bool:
        """
        检查波动率限制
        
        Args:
            returns: 收益率序列
            
        Returns:
            是否满足波动率限制
        """
        if len(returns) < 20:
            return True  # 数据不足时通过
        
        volatility = np.std(returns[-20:]) * np.sqrt(252)  # 年化波动率
        return volatility <= self.max_volatility
    
    def execute_buy(self, stock_code: str, date: str, 
                   price_df: pd.DataFrame, stock_info: pd.DataFrame,
                   target_amount: float, order_type: str = 'market',
                   limit_price: Optional[float] = None,
                   liquidity: float = 1.0) -> Dict:
        """
        执行买入订单
        
        Args:
            stock_code: 股票代码
            date: 日期
            price_df: 价格数据
            stock_info: 股票信息数据
            target_amount: 目标买入金额
            order_type: 订单类型
            limit_price: 限价单价格
            liquidity: 流动性因子
            
        Returns:
            交易记录
        """
        record = {
            'date': date,
            'stock_code': stock_code,
            'side': 'buy',
            'status': 'failed',
            'message': ''
        }
        
        can_trade, trade_reason = self._can_trade(stock_code, 'buy', price_df)
        if not can_trade:
            record['message'] = trade_reason
            return record
        
        if not self._check_trading_time(date):
            record['message'] = '非交易时间'
            return record
        
        # 1. 检查资金是否足够
        if self.portfolio.cash < target_amount:
            record['message'] = 'Insufficient cash'
            return record
        
        # 2. 检查仓位限制
        if not self._check_position_limit(stock_code, target_amount):
            record['message'] = f'Position limit exceeded (max={self.max_single_position})'
            return record
        
        # 3. 检查行业限制
        if not self._check_industry_limit(stock_code, target_amount, stock_info):
            record['message'] = f'Industry limit exceeded (max={self.max_industry_exposure})'
            return record
        
        # 4. 检查流动性
        if not self._check_liquidity(stock_code, date, price_df):
            record['message'] = f'Insufficient liquidity (min={self.min_liquidity})'
            return record
        
        # 5. 检查持仓数量限制
        if not self._check_position_count():
            record['message'] = f'Position count limit exceeded (max={self.max_positions})'
            return record
        
        # 6. 检查最大回撤限制
        if self._check_max_drawdown():
            record['message'] = f'Max drawdown reached ({self.max_drawdown:.1%})'
            return record
        
        # 7. 获取市场价格
        price, can_fill = self._get_market_price(stock_code, date, price_df, order_type, limit_price)
        
        if price is None:
            record['message'] = 'Price not available'
            return record
        
        if not can_fill:
            record['status'] = 'unfilled'
            record['message'] = 'Order not filled'
            return record
        
        # 5. 计算可买数量（100股的整数倍）
        quantity = int(target_amount / price / 100) * 100
        if quantity <= 0:
            record['message'] = 'Amount too small'
            return record
        
        # 6. 计算交易成本
        actual_amount = quantity * price
        commission = self.cost_model.calculate_commission(actual_amount)
        total_cost = actual_amount + commission
        
        if total_cost > self.portfolio.cash:
            # 调整数量
            available_for_cost = self.portfolio.cash - commission
            quantity = int(available_for_cost / price / 100) * 100
            actual_amount = quantity * price
            total_cost = actual_amount + commission
        
        # 7. 执行交易
        self.portfolio.cash -= total_cost
        
        # 更新持仓
        if stock_code in self.portfolio.positions:
            pos = self.portfolio.positions[stock_code]
            # 成本加权平均
            total_shares = pos.quantity + quantity
            pos.avg_price = (pos.avg_price * pos.quantity + price * quantity) / total_shares
            pos.quantity = total_shares
        else:
            stock_row = stock_info[stock_info['stock_code'] == stock_code]
            industry = stock_row.iloc[0].get('industry', 'unknown') if len(stock_row) > 0 else 'unknown'
            self.portfolio.positions[stock_code] = Position(
                stock_code=stock_code,
                quantity=quantity,
                avg_price=price,
                industry=industry,
                entry_date=date
            )
        
        record.update({
            'status': 'filled',
            'quantity': quantity,
            'price': price,
            'amount': actual_amount,
            'commission': commission,
            'total_cost': total_cost,
            'message': 'Success'
        })
        
        self.trades.append(record)
        return record
    
    def execute_sell(self, stock_code: str, date: str, 
                    price_df: pd.DataFrame, target_quantity: int = None,
                    order_type: str = 'market', limit_price: Optional[float] = None,
                    liquidity: float = 1.0) -> Dict:
        """
        执行卖出订单
        
        Args:
            stock_code: 股票代码
            date: 日期
            price_df: 价格数据
            target_quantity: 卖出数量 (None表示全部卖出)
            order_type: 订单类型
            limit_price: 限价单价格
            liquidity: 流动性因子
            
        Returns:
            交易记录
        """
        record = {
            'date': date,
            'stock_code': stock_code,
            'side': 'sell',
            'status': 'failed',
            'message': ''
        }
        
        # 1. 检查是否有持仓
        pos = self.portfolio.get_position(stock_code)
        if pos is None:
            record['message'] = 'No position'
            return record
        
        # 2. 确定卖出数量
        quantity = target_quantity if target_quantity is not None else pos.quantity
        quantity = min(quantity, pos.quantity)
        
        if quantity <= 0:
            record['message'] = 'No quantity to sell'
            return record
        
        # 3. 获取市场价格
        price, can_fill = self._get_market_price(stock_code, date, price_df, order_type, limit_price)
        
        if price is None:
            record['message'] = 'Price not available'
            return record
        
        if not can_fill:
            record['status'] = 'unfilled'
            record['message'] = 'Order not filled'
            return record
        
        # 4. 计算交易成本
        actual_amount = quantity * price
        commission = self.cost_model.calculate_commission(actual_amount)
        stamp_tax = self.cost_model.calculate_stamp_tax(actual_amount, is_sell=True)
        total_cost = commission + stamp_tax
        net_proceeds = actual_amount - total_cost
        
        # 5. 执行交易
        self.portfolio.cash += net_proceeds
        
        # 更新持仓
        pos.quantity -= quantity
        if pos.quantity == 0:
            del self.portfolio.positions[stock_code]
        
        record.update({
            'status': 'filled',
            'quantity': quantity,
            'price': price,
            'amount': actual_amount,
            'commission': commission,
            'stamp_tax': stamp_tax,
            'total_cost': total_cost,
            'net_proceeds': net_proceeds,
            'message': 'Success'
        })
        
        self.trades.append(record)
        return record
    
    def update_portfolio_value(self, date: str, price_df: pd.DataFrame):
        """
        更新持仓市值
        
        Args:
            date: 日期
            price_df: 价格数据
        """
        for stock_code, pos in self.portfolio.positions.items():
            stock_data = price_df[price_df['stock_code'] == stock_code]
            if len(stock_data) > 0:
                pos.market_value = pos.quantity * stock_data['close'].values[0]
            else:
                # 如果没有价格数据，使用上次价格
                pos.market_value = pos.quantity * pos.avg_price
    
    def run_backtest(self, data: pd.DataFrame, 
                    signal_func: callable,
                    rebalance_freq: str = 'monthly') -> Dict:
        """
        运行回测
        
        Args:
            data: 历史数据，必须包含 date, stock_code, close 等列
            signal_func: 信号生成函数，参数为 (date, data)，返回目标股票列表或权重
            rebalance_freq: 调仓频率 ('monthly', 'weekly')
            
        Returns:
            回测结果字典
        """
        print("🚀 开始回测...")
        print(f"  初始资金: {self.initial_capital:,.2f}")
        print(f"  成本模型: 佣金={self.cost_model.commission_rate:.4%}, 印花税={self.cost_model.stamp_tax_rate:.2%}")
        print(f"  限制: 单票={self.max_single_position:.1%}, 单行业={self.max_industry_exposure:.1%}")
        print(f"  风险控制: 最大回撤={self.max_drawdown:.1%}, 最大波动率={self.max_volatility:.1%}")
        print(f"  其他限制: 最小流动性={self.min_liquidity/10000:.0f}万, 最大持仓数={self.max_positions}, 止损={self.stop_loss:.1%}")
        
        # 确保数据排序
        data = data.sort_values('date').copy()
        
        # 获取日期列表
        if rebalance_freq == 'monthly':
            dates = sorted(data['month'].unique())
        elif rebalance_freq == 'weekly':
            dates = sorted(data['date'].unique())
        else:
            raise ValueError(f"Invalid rebalance frequency: {rebalance_freq}")
        
        print(f"  回测期间: {len(dates)} 个周期")
        
        # 生成基准指数收益
        self.benchmark_returns = self._generate_benchmark_returns(dates[:-1])
        
        # 回测循环
        returns = []
        for i, rebalance_date in enumerate(dates[:-1]):
            # 调仓日
            if rebalance_freq == 'monthly':
                current_data = data[data['month'] == rebalance_date].copy()
                next_date = dates[i + 1]
            else:
                current_data = data[data['date'] == rebalance_date].copy()
                next_date = dates[i + 1] if i + 1 < len(dates) else rebalance_date
            
            if len(current_data) == 0:
                continue
            
            # 检查最大回撤
            if self._check_max_drawdown():
                print(f"  触发最大回撤限制，停止交易")
                break
            
            # 检查波动率
            if len(returns) > 0 and not self._check_volatility(returns):
                print(f"  触发波动率限制，减少交易")
                # 可以在这里添加减仓逻辑
            
            # 执行止损检查
            for stock_code in list(self.portfolio.positions.keys()):
                stock_data = current_data[current_data['stock_code'] == stock_code]
                if len(stock_data) > 0:
                    current_price = stock_data['close'].values[0]
                    if self._check_stop_loss(stock_code, current_price):
                        print(f"  触发止损: {stock_code}，止损比例={self.stop_loss:.1%}")
                        self.execute_sell(
                            stock_code, rebalance_date, current_data, 
                            target_quantity=None, order_type='market'
                        )
            
            # 生成交易信号
            # 传递current_data而不是rebalance_date，因为信号生成函数需要完整的数据
            signals = signal_func(current_data['date'].iloc[0], current_data)
            
            # 执行调仓
            self._rebalance(rebalance_date, current_data, signals, data)
            
            # 更新投资组合价值
            self.update_portfolio_value(rebalance_date, current_data)
            
            # 计算当日收益率
            if i > 0:
                prev_value = self.portfolio.history[-1]['total_value'] if self.portfolio.history else self.initial_capital
                current_value = self.portfolio.total_value
                day_return = (current_value - prev_value) / prev_value
                returns.append(day_return)
            
            # 记录状态
            self.dates.append(rebalance_date)
            self.portfolio.record_state(rebalance_date, {})
            
            if i % max(1, len(dates) // 10) == 0:
                progress = (i / len(dates)) * 100
                print(f"  进度: {progress:.1f}% (第{i+1}/{len(dates)}周期)")
        
        # 计算基准指数最终价值
        benchmark_final_value = self.benchmark_values[-1] if self.benchmark_values else self.initial_capital
        benchmark_return = (benchmark_final_value / self.initial_capital - 1)
        
        # 计算超额收益
        strategy_return = (self.portfolio.total_value / self.initial_capital - 1)
        excess_return = strategy_return - benchmark_return
        
        # 计算信息比率和跟踪误差
        tracking_error = self._calculate_tracking_error()
        information_ratio = self._calculate_information_ratio()
        
        # 生成风险分析报告
        risk_report = self.generate_risk_report()
        
        print(f"✓ 回测完成")
        print(f"  最终资产: {self.portfolio.total_value:,.2f}")
        print(f"  总收益率: {strategy_return:.2%}")
        print(f"  交易次数: {len(self.trades)}")
        print(f"  最大回撤: {self._calculate_max_drawdown():.2%}")
        print(f"  基准指数 ({self.benchmark}):")
        print(f"    最终价值: {benchmark_final_value:,.2f}")
        print(f"    总收益率: {benchmark_return:.2%}")
        print(f"  超额收益: {excess_return:.2%}")
        print(f"  信息比率: {information_ratio:.2f}")
        print(f"  跟踪误差: {tracking_error:.2%}")
        print(f"  风险分析:")
        print(f"    VaR (95%): {risk_report.get('var_95', 0):,.2f}")
        print(f"    VaR (99%): {risk_report.get('var_99', 0):,.2f}")
        print(f"    CVaR (95%): {risk_report.get('cvar_95', 0):,.2f}")
        print(f"    下行风险: {risk_report.get('downside_risk', 0):.2%}")
        print(f"    Omega比率: {risk_report.get('omega_ratio', 0):.2f}")
        if risk_report.get('industry_concentration'):
            print(f"    行业集中度 (HHI): {risk_report['industry_concentration'].get('hhi', 0):.4f}")
            print(f"    前三大行业占比: {risk_report['industry_concentration'].get('top3_exposure', 0):.2%}")
        
        # 生成归因分析报告
        attribution_report = self.generate_attribution_report()
        print(f"  归因分析:")
        if attribution_report.get('style_attribution'):
            style_contributions = attribution_report['style_attribution'].get('style_contributions', {})
            print(f"    风格因子贡献:")
            for style, contribution in style_contributions.items():
                print(f"      {style}: {contribution:.2%}")
        if attribution_report.get('industry_attribution'):
            industry_count = len(attribution_report['industry_attribution'].get('industry_weights', {}))
            print(f"    行业归因: {industry_count} 个行业")
        
        return self._generate_results()
    
    def _calculate_max_drawdown(self) -> float:
        """
        计算最大回撤
        
        Returns:
            最大回撤值
        """
        if not self.portfolio.history:
            return 0.0
        
        values = [state['total_value'] for state in self.portfolio.history]
        if not values:
            return 0.0
        
        running_max = values[0]
        max_drawdown = 0.0
        
        for value in values:
            running_max = max(running_max, value)
            drawdown = (running_max - value) / running_max
            max_drawdown = max(max_drawdown, drawdown)
        
        return max_drawdown
    
    def _generate_benchmark_returns(self, dates: List[str]) -> List[float]:
        """
        生成基准指数的模拟收益
        
        Args:
            dates: 日期列表
            
        Returns:
            基准指数收益率序列
        """
        # 不同基准指数的年化收益率和波动率
        benchmark_params = {
            'hs300': {'annual_return': 0.08, 'annual_volatility': 0.20},
            'zz500': {'annual_return': 0.10, 'annual_volatility': 0.25},
            'sh000001': {'annual_return': 0.06, 'annual_volatility': 0.18}
        }
        
        params = benchmark_params.get(self.benchmark, benchmark_params['hs300'])
        
        # 计算日收益率的均值和标准差
        daily_return = params['annual_return'] / 252
        daily_volatility = params['annual_volatility'] / np.sqrt(252)
        
        # 生成随机收益率序列
        returns = np.random.normal(daily_return, daily_volatility, len(dates))
        
        # 计算累计收益
        cumulative_returns = np.cumprod(1 + returns)
        self.benchmark_values = [self.initial_capital * cr for cr in cumulative_returns]
        
        return returns.tolist()
    
    def _calculate_tracking_error(self) -> float:
        """
        计算跟踪误差
        
        Returns:
            跟踪误差值
        """
        if len(self.benchmark_returns) != len(self.portfolio.history):
            return 0.0
        
        strategy_returns = []
        for i, state in enumerate(self.portfolio.history):
            if i == 0:
                strategy_returns.append(0)
            else:
                prev_value = self.portfolio.history[i-1]['total_value']
                current_value = state['total_value']
                day_return = (current_value - prev_value) / prev_value
                strategy_returns.append(day_return)
        
        if len(strategy_returns) < 2:
            return 0.0
        
        excess_returns = [s - b for s, b in zip(strategy_returns, self.benchmark_returns)]
        tracking_error = np.std(excess_returns) * np.sqrt(252)
        
        return tracking_error
    
    def _calculate_information_ratio(self) -> float:
        """
        计算信息比率
        
        Returns:
            信息比率值
        """
        if len(self.benchmark_returns) != len(self.portfolio.history):
            return 0.0
        
        strategy_returns = []
        for i, state in enumerate(self.portfolio.history):
            if i == 0:
                strategy_returns.append(0)
            else:
                prev_value = self.portfolio.history[i-1]['total_value']
                current_value = state['total_value']
                day_return = (current_value - prev_value) / prev_value
                strategy_returns.append(day_return)
        
        if len(strategy_returns) < 2:
            return 0.0
        
        excess_returns = [s - b for s, b in zip(strategy_returns, self.benchmark_returns)]
        avg_excess_return = np.mean(excess_returns) * 252
        tracking_error = np.std(excess_returns) * np.sqrt(252)
        
        return avg_excess_return / tracking_error if tracking_error > 0 else 0
    
    def calculate_var(self, confidence_level: float = 0.95, horizon: int = 1) -> float:
        """
        计算Value at Risk (VaR)
        
        Args:
            confidence_level: 置信水平 (0-1)
            horizon: 时间 horizon（天）
            
        Returns:
            VaR值
        """
        if not self.portfolio.history:
            return 0.0
        
        strategy_returns = []
        for i, state in enumerate(self.portfolio.history):
            if i > 0:
                prev_value = self.portfolio.history[i-1]['total_value']
                current_value = state['total_value']
                day_return = (current_value - prev_value) / prev_value
                strategy_returns.append(day_return)
        
        if len(strategy_returns) < 20:
            return 0.0
        
        # 计算历史VaR
        returns = np.array(strategy_returns)
        var = -np.percentile(returns, (1 - confidence_level) * 100)
        
        # 调整到指定时间horizon
        var = var * np.sqrt(horizon)
        
        # 转换为金额
        current_value = self.portfolio.total_value
        var_amount = current_value * var
        
        return var_amount
    
    def calculate_cvar(self, confidence_level: float = 0.95) -> float:
        """
        计算Conditional Value at Risk (CVaR)
        
        Args:
            confidence_level: 置信水平 (0-1)
            
        Returns:
            CVaR值
        """
        if not self.portfolio.history:
            return 0.0
        
        strategy_returns = []
        for i, state in enumerate(self.portfolio.history):
            if i > 0:
                prev_value = self.portfolio.history[i-1]['total_value']
                current_value = state['total_value']
                day_return = (current_value - prev_value) / prev_value
                strategy_returns.append(day_return)
        
        if len(strategy_returns) < 20:
            return 0.0
        
        returns = np.array(strategy_returns)
        var_threshold = np.percentile(returns, (1 - confidence_level) * 100)
        cvar = -np.mean(returns[returns <= var_threshold])
        
        # 转换为金额
        current_value = self.portfolio.total_value
        cvar_amount = current_value * cvar
        
        return cvar_amount
    
    def calculate_downside_risk(self, risk_free_rate: float = 0.03) -> float:
        """
        计算下行风险
        
        Args:
            risk_free_rate: 无风险利率
            
        Returns:
            下行风险值
        """
        if not self.portfolio.history:
            return 0.0
        
        daily_risk_free = risk_free_rate / 252
        downside_returns = []
        
        for i, state in enumerate(self.portfolio.history):
            if i > 0:
                prev_value = self.portfolio.history[i-1]['total_value']
                current_value = state['total_value']
                day_return = (current_value - prev_value) / prev_value
                if day_return < daily_risk_free:
                    downside_returns.append(day_return - daily_risk_free)
        
        if not downside_returns:
            return 0.0
        
        downside_risk = np.std(downside_returns) * np.sqrt(252)
        return downside_risk
    
    def calculate_omega_ratio(self, risk_free_rate: float = 0.03) -> float:
        """
        计算Omega比率
        
        Args:
            risk_free_rate: 无风险利率
            
        Returns:
            Omega比率值
        """
        if not self.portfolio.history:
            return 0.0
        
        daily_risk_free = risk_free_rate / 252
        positive_returns = []
        negative_returns = []
        
        for i, state in enumerate(self.portfolio.history):
            if i > 0:
                prev_value = self.portfolio.history[i-1]['total_value']
                current_value = state['total_value']
                day_return = (current_value - prev_value) / prev_value
                excess_return = day_return - daily_risk_free
                if excess_return > 0:
                    positive_returns.append(excess_return)
                elif excess_return < 0:
                    negative_returns.append(abs(excess_return))
        
        if not negative_returns:
            return 0.0
        
        omega_ratio = sum(positive_returns) / sum(negative_returns)
        return omega_ratio
    
    def analyze_industry_concentration(self) -> Dict[str, float]:
        """
        分析行业集中度风险
        
        Returns:
            行业集中度指标
        """
        if not self.portfolio.positions:
            return {}
        
        industry_exposure = self.portfolio.get_industry_exposure()
        total_exposure = sum(industry_exposure.values())
        
        if total_exposure == 0:
            return {}
        
        # 计算HHI指数 (赫芬达尔-赫希曼指数)
        hhi = sum((weight/total_exposure)**2 for weight in industry_exposure.values())
        
        # 计算前三大行业占比
        sorted_industries = sorted(industry_exposure.items(), key=lambda x: x[1], reverse=True)
        top3_exposure = sum(weight for _, weight in sorted_industries[:3]) / total_exposure
        
        return {
            'hhi': hhi,
            'top3_exposure': top3_exposure,
            'industry_count': len(industry_exposure),
            'max_industry_exposure': max(industry_exposure.values()) / total_exposure if industry_exposure else 0
        }
    
    def analyze_liquidity_risk(self) -> Dict[str, float]:
        """
        分析流动性风险
        
        Returns:
            流动性风险指标
        """
        if not self.portfolio.positions:
            return {}
        
        # 这里可以添加更详细的流动性风险分析
        # 例如：平均流动性、流动性标准差、流动性最差的股票等
        
        return {
            'min_liquidity': self.min_liquidity,
            'position_count': len(self.portfolio.positions)
        }
    
    def generate_risk_report(self) -> Dict:
        """
        生成完整的风险分析报告
        
        Returns:
            风险分析报告
        """
        risk_report = {
            'var_95': self.calculate_var(confidence_level=0.95),
            'var_99': self.calculate_var(confidence_level=0.99),
            'cvar_95': self.calculate_cvar(confidence_level=0.95),
            'downside_risk': self.calculate_downside_risk(),
            'omega_ratio': self.calculate_omega_ratio(),
            'industry_concentration': self.analyze_industry_concentration(),
            'liquidity_risk': self.analyze_liquidity_risk()
        }
        
        return risk_report
    
    def analyze_factor_attribution(self, factor_data: pd.DataFrame) -> Dict:
        """
        因子归因分析
        
        Args:
            factor_data: 包含因子值的数据
            
        Returns:
            因子归因分析结果
        """
        if not factor_data.empty and not self.portfolio.history:
            return {}
        
        # 提取因子列
        factor_columns = [col for col in factor_data.columns if col.startswith('momentum_') or 
                         col.startswith('volatility_') or col.startswith('price_to_') or 
                         col in ['rsi_14', 'macd', 'bollinger_position', 'volume_change']]
        
        if not factor_columns:
            return {}
        
        # 计算每个因子的暴露和贡献
        factor_contributions = {}
        
        # 这里可以实现更复杂的因子归因模型，如Fama-French三因子模型
        # 简化版：基于因子暴露和因子收益的线性回归
        
        return {
            'factor_contributions': factor_contributions,
            'factor_count': len(factor_columns)
        }
    
    def analyze_industry_attribution(self) -> Dict:
        """
        行业归因分析
        
        Returns:
            行业归因分析结果
        """
        if not self.portfolio.history:
            return {}
        
        # 分析每个行业的贡献
        industry_returns = {}
        industry_weights = {}
        
        # 简化版：基于行业权重和行业表现的归因
        # 实际应用中应该使用更复杂的归因模型
        
        return {
            'industry_returns': industry_returns,
            'industry_weights': industry_weights
        }
    
    def analyze_style_attribution(self) -> Dict:
        """
        风格归因分析
        
        Returns:
            风格归因分析结果
        """
        if not self.portfolio.history:
            return {}
        
        # 分析不同风格因子的贡献
        # 常见的风格因子包括：市值、估值、动量、波动率、质量等
        style_factors = {
            'size': 0,  # 市值因子
            'value': 0,  # 估值因子
            'momentum': 0,  # 动量因子
            'volatility': 0,  # 波动率因子
            'quality': 0  # 质量因子
        }
        
        return {
            'style_contributions': style_factors
        }
    
    def analyze_time_attribution(self) -> Dict:
        """
        时间归因分析
        
        Returns:
            时间归因分析结果
        """
        if not self.portfolio.history:
            return {}
        
        # 按时间段分析收益贡献
        time_periods = {
            'monthly': {},
            'quarterly': {},
            'yearly': {}
        }
        
        # 计算不同时间段的收益
        for i, state in enumerate(self.portfolio.history):
            if i > 0:
                date = state['date']
                prev_value = self.portfolio.history[i-1]['total_value']
                current_value = state['total_value']
                period_return = (current_value - prev_value) / prev_value
                
                # 按月、季度、年分组
                # 这里可以添加具体的时间分组逻辑
        
        return {
            'time_periods': time_periods
        }
    
    def generate_attribution_report(self, factor_data: pd.DataFrame = None) -> Dict:
        """
        生成完整的归因分析报告
        
        Args:
            factor_data: 包含因子值的数据
            
        Returns:
            归因分析报告
        """
        attribution_report = {
            'factor_attribution': self.analyze_factor_attribution(factor_data) if factor_data is not None else {},
            'industry_attribution': self.analyze_industry_attribution(),
            'style_attribution': self.analyze_style_attribution(),
            'time_attribution': self.analyze_time_attribution()
        }
        
        return attribution_report
    
    def _rebalance(self, date: str, current_data: pd.DataFrame, 
                   signals, full_data: pd.DataFrame):
        """
        执行调仓
        
        Args:
            date: 调仓日期
            current_data: 当前日期数据
            signals: 交易信号（可以是列表或字典）
            full_data: 全部数据（用于获取价格）
        """
        # 处理信号格式：支持列表和字典两种格式
        if isinstance(signals, list):
            # 列表格式：等权重分配
            if len(signals) > 0:
                weight = 1.0 / len(signals)
                signals = {code: weight for code in signals}
            else:
                signals = {}
        elif not isinstance(signals, dict):
            # 其他格式，转为空字典
            signals = {}
        
        print(f"  调仓日: {date}")
        print(f"  信号数量: {len(signals)}")
        print(f"  当前持仓: {list(self.portfolio.positions.keys())}")
        
        # 先卖出不在目标中的持仓
        stocks_to_sell = [code for code in self.portfolio.positions.keys() 
                         if code not in signals]
        print(f"  卖出股票: {stocks_to_sell}")
        for stock_code in stocks_to_sell:
            result = self.execute_sell(
                stock_code, date, current_data, target_quantity=None,
                order_type='market', liquidity=1.0
            )
            print(f"  卖出 {stock_code}: {result['status']} - {result['message']}")
        
        # 买入目标持仓
        total_value = self.portfolio.total_value
        print(f"  总资产: {total_value:.2f}")
        print(f"  可用现金: {self.portfolio.cash:.2f}")
        
        for stock_code, target_weight in signals.items():
            # 确保目标权重合理
            target_weight = min(target_weight, self.max_single_position)
            target_amount = total_value * target_weight
            
            print(f"  买入 {stock_code}: 目标金额={target_amount:.2f}, 权重={target_weight:.2f}")
            if target_amount > 0:
                # 无论是否已有持仓，都执行买入
                result = self.execute_buy(
                    stock_code, date, current_data, current_data,
                    target_amount, order_type='market', liquidity=1.0
                )
                print(f"  买入 {stock_code}: {result['status']} - {result['message']}")
    
    def _generate_results(self) -> Dict:
        """生成回测结果"""
        # 计算收益率序列
        history_df = pd.DataFrame(self.portfolio.history)
        if len(history_df) == 0:
            return {}
        
        history_df['return'] = history_df['total_value'].pct_change()
        history_df['return'] = history_df['return'].fillna(0)
        
        # 计算绩效指标
        returns = history_df['return'].values
        
        total_return = (self.portfolio.total_value / self.initial_capital) - 1
        
        num_periods = len(returns)
        if num_periods > 0:
            period_return = np.mean(returns)
            volatility = np.std(returns)
            
            # 年化指标（假设月度）
            annual_return = (1 + period_return) ** 12 - 1
            annual_volatility = volatility * np.sqrt(12)
            
            # 夏普比率
            rf_monthly = 0.03 / 12
            sharpe_ratio = (period_return - rf_monthly) / volatility if volatility > 0 else 0
            annual_sharpe = sharpe_ratio * np.sqrt(12)
            
            # 最大回撤
            cumulative = pd.Series((1 + returns).cumprod())
            running_max = cumulative.expanding().max()
            drawdown = ((cumulative - running_max) / running_max)
            max_drawdown = drawdown.min()
            max_drawdown_period = drawdown.idxmin() if len(drawdown) > 0 else 0
            
            # 胜率
            win_rate = (returns > 0).mean()
            
            # 盈亏比
            positive_returns = returns[returns > 0]
            negative_returns = returns[returns < 0]
            profit_loss_ratio = np.mean(positive_returns) / abs(np.mean(negative_returns)) if len(negative_returns) > 0 else 0
            
            # 索提诺比率
            downside_returns = returns[returns < 0]
            downside_volatility = np.std(downside_returns) if len(downside_returns) > 0 else 1
            sortino_ratio = (period_return - rf_monthly) / downside_volatility if downside_volatility > 0 else 0
            annual_sortino = sortino_ratio * np.sqrt(12)
            
            # Calmar比率
            calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown < 0 else 0
        else:
            annual_return = 0
            annual_volatility = 0
            annual_sharpe = 0
            max_drawdown = 0
            max_drawdown_period = 0
            win_rate = 0
            profit_loss_ratio = 0
            annual_sortino = 0
            calmar_ratio = 0
        
        # 交易统计
        trades_df = pd.DataFrame(self.trades)
        if len(trades_df) > 0:
            filled_trades = trades_df[trades_df['status'] == 'filled']
            buy_trades = filled_trades[filled_trades['side'] == 'buy']
            sell_trades = filled_trades[filled_trades['side'] == 'sell']
            
            total_commission = filled_trades['commission'].sum()
            total_stamp_tax = filled_trades['stamp_tax'].sum() if 'stamp_tax' in filled_trades.columns else 0
            total_cost = total_commission + total_stamp_tax
            
            # 平均持仓时间
            if len(sell_trades) > 0:
                sell_trades['holding_period'] = 0  # 这里需要根据实际情况计算
            else:
                sell_trades = pd.DataFrame()
        else:
            buy_trades = pd.DataFrame()
            sell_trades = pd.DataFrame()
            total_cost = 0
        
        # 计算基准相关指标
        benchmark_final_value = self.benchmark_values[-1] if self.benchmark_values else self.initial_capital
        benchmark_return = (benchmark_final_value / self.initial_capital - 1)
        excess_return = total_return - benchmark_return
        tracking_error = self._calculate_tracking_error()
        information_ratio = self._calculate_information_ratio()
        
        # 生成风险分析报告
        risk_report = self.generate_risk_report()
        
        # 生成归因分析报告
        attribution_report = self.generate_attribution_report()
        
        return {
            'initial_capital': self.initial_capital,
            'final_value': self.portfolio.total_value,
            'total_return': total_return,
            'annual_return': annual_return,
            'annual_volatility': annual_volatility,
            'sharpe_ratio': annual_sharpe,
            'sortino_ratio': annual_sortino,
            'calmar_ratio': calmar_ratio,
            'max_drawdown': max_drawdown,
            'max_drawdown_period': max_drawdown_period,
            'win_rate': win_rate,
            'profit_loss_ratio': profit_loss_ratio,
            'num_trades': len(self.trades),
            'num_filled': len(filled_trades) if 'filled_trades' in locals() else 0,
            'fill_rate': len(filled_trades) / len(self.trades) if len(self.trades) > 0 else 0,
            'total_cost': total_cost,
            'cost_ratio': total_cost / self.initial_capital,
            'benchmark': self.benchmark,
            'benchmark_final_value': benchmark_final_value,
            'benchmark_return': benchmark_return,
            'excess_return': excess_return,
            'tracking_error': tracking_error,
            'information_ratio': information_ratio,
            'benchmark_returns': self.benchmark_returns,
            'benchmark_values': self.benchmark_values,
            'risk_analysis': risk_report,
            'attribution_analysis': attribution_report,
            'history': history_df,
            'trades': trades_df
        }


if __name__ == '__main__':
    # 测试回测引擎
    print("=== 回测引擎 V2 测试 ===\n")
    
    # 创建简单的成本模型
    cost_model = CostModel(
        commission_rate=0.0003,  # 万三
        stamp_tax_rate=0.001,     # 千一
        impact_cost_base=0.0005,
        impact_cost_sqrt=0.001
    )
    
    # 创建回测引擎
    engine = BacktestEngineV2(
        initial_capital=1000000,
        cost_model=cost_model,
        max_single_position=0.10,
        max_industry_exposure=0.30,
        slippage_rate=0.001,
        fill_ratio=0.95
    )
    
    print(f"成本模型: {cost_model}")
    print(f"回测引擎: {engine}")
    print(f"投资组合: {engine.portfolio}")
