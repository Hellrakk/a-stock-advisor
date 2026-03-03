#!/usr/bin/env python3
"""
实时交易接口模块
支持连接券商API进行实时交易
"""

import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class RealTimeOrder:
    """实时订单类"""
    stock_code: str
    side: str  # 'buy' or 'sell'
    order_type: str  # 'limit' or 'market'
    price: Optional[float]  # 限价单价格 (市价单为None)
    quantity: int
    order_id: str = None
    status: str = 'pending'  # 'pending', 'filled', 'partial', 'cancelled', 'rejected'
    filled_quantity: int = 0
    filled_price: Optional[float] = None
    submit_time: str = None
    fill_time: str = None
    message: str = ''
    
    def __post_init__(self):
        if self.side not in ['buy', 'sell']:
            raise ValueError(f"Invalid order side: {self.side}")
        if self.order_type not in ['limit', 'market']:
            raise ValueError(f"Invalid order type: {self.order_type}")
        self.submit_time = self.submit_time or datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class TradingAPI:
    """交易API基类"""
    
    def __init__(self):
        self.connected = False
        self.account_info = {}
        self.positions = {}
        
    def connect(self, **kwargs) -> bool:
        """连接API"""
        raise NotImplementedError
    
    def disconnect(self) -> bool:
        """断开连接"""
        raise NotImplementedError
    
    def place_order(self, order: RealTimeOrder) -> str:
        """下单"""
        raise NotImplementedError
    
    def cancel_order(self, order_id: str) -> bool:
        """撤单"""
        raise NotImplementedError
    
    def get_order_status(self, order_id: str) -> Dict:
        """获取订单状态"""
        raise NotImplementedError
    
    def get_account_info(self) -> Dict:
        """获取账户信息"""
        raise NotImplementedError
    
    def get_positions(self) -> Dict:
        """获取持仓信息"""
        raise NotImplementedError
    
    def get_realtime_quote(self, stock_code: str) -> Dict:
        """获取实时行情"""
        raise NotImplementedError


class SimulatedTradingAPI(TradingAPI):
    """模拟交易API"""
    
    def __init__(self, initial_cash: float = 1000000.0):
        super().__init__()
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions = {}
        self.orders = {}
        self.order_counter = 0
        self.connected = False
    
    def connect(self, **kwargs) -> bool:
        """连接API"""
        logger.info("✓ 模拟交易API连接成功")
        self.connected = True
        return True
    
    def disconnect(self) -> bool:
        """断开连接"""
        logger.info("✓ 模拟交易API断开连接")
        self.connected = False
        return True
    
    def place_order(self, order: RealTimeOrder) -> str:
        """下单"""
        if not self.connected:
            order.status = 'rejected'
            order.message = 'Not connected'
            return None
        
        # 生成订单ID
        self.order_counter += 1
        order_id = f"ORDER_{self.order_counter:06d}"
        order.order_id = order_id
        
        # 模拟行情数据
        quote = self.get_realtime_quote(order.stock_code)
        if not quote or quote.get('price', 0) == 0:
            order.status = 'rejected'
            order.message = 'Price not available'
            self.orders[order_id] = order
            return order_id
        
        # 计算成交价格
        if order.order_type == 'market':
            fill_price = quote['price']
        else:  # limit
            if order.side == 'buy' and order.price < quote['price']:
                order.status = 'rejected'
                order.message = 'Limit price too low'
                self.orders[order_id] = order
                return order_id
            elif order.side == 'sell' and order.price > quote['price']:
                order.status = 'rejected'
                order.message = 'Limit price too high'
                self.orders[order_id] = order
                return order_id
            fill_price = order.price
        
        # 检查资金或持仓
        if order.side == 'buy':
            total_cost = order.quantity * fill_price
            if total_cost > self.cash:
                order.status = 'rejected'
                order.message = 'Insufficient cash'
                self.orders[order_id] = order
                return order_id
        else:  # sell
            if order.stock_code not in self.positions or self.positions[order.stock_code]['quantity'] < order.quantity:
                order.status = 'rejected'
                order.message = 'Insufficient position'
                self.orders[order_id] = order
                return order_id
        
        # 执行交易
        time.sleep(0.1)  # 模拟交易延迟
        
        if order.side == 'buy':
            self.cash -= order.quantity * fill_price
            if order.stock_code in self.positions:
                # 更新持仓
                pos = self.positions[order.stock_code]
                total_cost = pos['avg_price'] * pos['quantity'] + fill_price * order.quantity
                total_quantity = pos['quantity'] + order.quantity
                pos['avg_price'] = total_cost / total_quantity
                pos['quantity'] = total_quantity
                pos['market_value'] = total_quantity * fill_price
            else:
                # 新建持仓
                self.positions[order.stock_code] = {
                    'stock_code': order.stock_code,
                    'quantity': order.quantity,
                    'avg_price': fill_price,
                    'market_value': order.quantity * fill_price
                }
        else:  # sell
            self.cash += order.quantity * fill_price
            pos = self.positions[order.stock_code]
            pos['quantity'] -= order.quantity
            if pos['quantity'] == 0:
                del self.positions[order.stock_code]
            else:
                pos['market_value'] = pos['quantity'] * fill_price
        
        # 更新订单状态
        order.status = 'filled'
        order.filled_quantity = order.quantity
        order.filled_price = fill_price
        order.fill_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        order.message = 'Success'
        
        self.orders[order_id] = order
        logger.info(f"✓ 订单执行成功: {order_id} - {order.side} {order.quantity}股 {order.stock_code} @ {fill_price}")
        
        return order_id
    
    def cancel_order(self, order_id: str) -> bool:
        """撤单"""
        if order_id in self.orders:
            order = self.orders[order_id]
            if order.status == 'pending':
                order.status = 'cancelled'
                order.message = 'Cancelled by user'
                logger.info(f"✓ 订单已撤销: {order_id}")
                return True
        return False
    
    def get_order_status(self, order_id: str) -> Dict:
        """获取订单状态"""
        if order_id in self.orders:
            order = self.orders[order_id]
            return {
                'order_id': order.order_id,
                'stock_code': order.stock_code,
                'side': order.side,
                'order_type': order.order_type,
                'price': order.price,
                'quantity': order.quantity,
                'status': order.status,
                'filled_quantity': order.filled_quantity,
                'filled_price': order.filled_price,
                'submit_time': order.submit_time,
                'fill_time': order.fill_time,
                'message': order.message
            }
        return {}
    
    def get_account_info(self) -> Dict:
        """获取账户信息"""
        total_value = self.cash + sum(pos['market_value'] for pos in self.positions.values())
        return {
            'cash': self.cash,
            'total_value': total_value,
            'positions_value': sum(pos['market_value'] for pos in self.positions.values()),
            'pnl': total_value - self.initial_cash
        }
    
    def get_positions(self) -> Dict:
        """获取持仓信息"""
        return self.positions
    
    def get_realtime_quote(self, stock_code: str) -> Dict:
        """获取实时行情"""
        # 模拟行情数据
        import random
        base_price = 100.0
        if stock_code in self.positions:
            base_price = self.positions[stock_code]['avg_price']
        
        price = base_price * (1 + random.uniform(-0.02, 0.02))
        price = round(price, 2)
        
        return {
            'stock_code': stock_code,
            'price': price,
            'open': price * (1 + random.uniform(-0.01, 0.01)),
            'high': price * (1 + random.uniform(0, 0.02)),
            'low': price * (1 - random.uniform(0, 0.02)),
            'volume': random.randint(100000, 1000000),
            'amount': price * random.randint(100000, 1000000),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


class RealTimeTrader:
    """实时交易器"""
    
    def __init__(self, api: TradingAPI):
        self.api = api
        self.order_history = []
        self.position_history = []
        
    def connect(self, **kwargs) -> bool:
        """连接交易API"""
        return self.api.connect(**kwargs)
    
    def disconnect(self) -> bool:
        """断开连接"""
        return self.api.disconnect()
    
    def buy(self, stock_code: str, quantity: int, price: float = None) -> str:
        """买入"""
        order_type = 'market' if price is None else 'limit'
        order = RealTimeOrder(
            stock_code=stock_code,
            side='buy',
            order_type=order_type,
            price=price,
            quantity=quantity
        )
        
        order_id = self.api.place_order(order)
        if order_id:
            self.order_history.append(order)
        return order_id
    
    def sell(self, stock_code: str, quantity: int, price: float = None) -> str:
        """卖出"""
        order_type = 'market' if price is None else 'limit'
        order = RealTimeOrder(
            stock_code=stock_code,
            side='sell',
            order_type=order_type,
            price=price,
            quantity=quantity
        )
        
        order_id = self.api.place_order(order)
        if order_id:
            self.order_history.append(order)
        return order_id
    
    def cancel(self, order_id: str) -> bool:
        """撤单"""
        return self.api.cancel_order(order_id)
    
    def get_order(self, order_id: str) -> Dict:
        """获取订单信息"""
        return self.api.get_order_status(order_id)
    
    def get_account(self) -> Dict:
        """获取账户信息"""
        return self.api.get_account_info()
    
    def get_positions(self) -> Dict:
        """获取持仓信息"""
        positions = self.api.get_positions()
        self.position_history.append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'positions': positions
        })
        return positions
    
    def get_quote(self, stock_code: str) -> Dict:
        """获取实时行情"""
        return self.api.get_realtime_quote(stock_code)
    
    def check_position(self, stock_code: str) -> Optional[Dict]:
        """检查持仓"""
        positions = self.api.get_positions()
        return positions.get(stock_code)
    
    def calculate_pnl(self, stock_code: str) -> float:
        """计算单个股票的盈亏"""
        position = self.check_position(stock_code)
        if not position:
            return 0.0
        
        quote = self.get_quote(stock_code)
        current_price = quote.get('price', position['avg_price'])
        cost = position['avg_price'] * position['quantity']
        current_value = current_price * position['quantity']
        return current_value - cost


if __name__ == '__main__':
    # 测试模拟交易API
    print("=== 模拟交易API测试 ===")
    
    api = SimulatedTradingAPI(initial_cash=1000000)
    trader = RealTimeTrader(api)
    
    # 连接API
    trader.connect()
    
    # 获取账户信息
    account = trader.get_account()
    print(f"初始账户信息: {account}")
    
    # 买入股票
    order_id1 = trader.buy('600519', 100, price=1800.0)
    print(f"买入订单ID: {order_id1}")
    
    # 获取订单状态
    order_status = trader.get_order(order_id1)
    print(f"订单状态: {order_status}")
    
    # 获取持仓
    positions = trader.get_positions()
    print(f"持仓: {positions}")
    
    # 卖出股票
    order_id2 = trader.sell('600519', 50)
    print(f"卖出订单ID: {order_id2}")
    
    # 获取账户信息
    account = trader.get_account()
    print(f"最终账户信息: {account}")
    
    # 断开连接
    trader.disconnect()
