#!/usr/bin/env python3
"""
事件驱动引擎模块
借鉴VNPy的事件驱动架构设计
"""

import threading
import queue
from typing import Callable, Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventType(Enum):
    """事件类型"""
    MARKET_DATA = "market_data"
    TRADE_SIGNAL = "trade_signal"
    ORDER = "order"
    FILL = "fill"
    POSITION = "position"
    RISK = "risk"
    FACTOR_UPDATE = "factor_update"
    SELECTION_COMPLETE = "selection_complete"
    REPORT_GENERATE = "report_generate"
    TIMER = "timer"


@dataclass
class Event:
    """事件数据类"""
    type: EventType
    data: Any
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class EventEngine:
    """事件驱动引擎"""
    
    def __init__(self):
        """初始化事件引擎"""
        self._queue = queue.Queue()
        self._handlers: Dict[EventType, List[Callable]] = {}
        self._thread = None
        self._active = False
        self._lock = threading.Lock()
    
    def register(self, event_type: EventType, handler: Callable):
        """
        注册事件处理器
        
        Args:
            event_type: 事件类型
            handler: 处理函数
        """
        with self._lock:
            if event_type not in self._handlers:
                self._handlers[event_type] = []
            self._handlers[event_type].append(handler)
            logger.info(f"注册事件处理器: {event_type.value} -> {handler.__name__}")
    
    def unregister(self, event_type: EventType, handler: Callable):
        """
        注销事件处理器
        
        Args:
            event_type: 事件类型
            handler: 处理函数
        """
        with self._lock:
            if event_type in self._handlers:
                if handler in self._handlers[event_type]:
                    self._handlers[event_type].remove(handler)
                    logger.info(f"注销事件处理器: {event_type.value} -> {handler.__name__}")
    
    def put(self, event: Event):
        """
        放入事件
        
        Args:
            event: 事件对象
        """
        self._queue.put(event)
    
    def start(self):
        """启动事件引擎"""
        if self._active:
            return
        
        self._active = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("事件引擎已启动")
    
    def stop(self):
        """停止事件引擎"""
        self._active = False
        self._queue.put(Event(EventType.TIMER, None))
        
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        
        logger.info("事件引擎已停止")
    
    def _run(self):
        """事件处理循环"""
        while self._active:
            try:
                event = self._queue.get(timeout=1)
                self._process(event)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"事件处理异常: {e}")
    
    def _process(self, event: Event):
        """
        处理事件
        
        Args:
            event: 事件对象
        """
        handlers = self._handlers.get(event.type, [])
        
        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"处理器执行异常 [{handler.__name__}]: {e}")


class DataHandler:
    """数据处理器基类"""
    
    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.event_engine.register(EventType.MARKET_DATA, self.on_market_data)
    
    def on_market_data(self, event: Event):
        """处理市场数据"""
        pass
    
    def emit_signal(self, signal_data: Any):
        """发送交易信号"""
        event = Event(EventType.TRADE_SIGNAL, signal_data)
        self.event_engine.put(event)


class StrategyHandler:
    """策略处理器基类"""
    
    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.event_engine.register(EventType.TRADE_SIGNAL, self.on_signal)
        self.event_engine.register(EventType.FACTOR_UPDATE, self.on_factor_update)
    
    def on_signal(self, event: Event):
        """处理交易信号"""
        pass
    
    def on_factor_update(self, event: Event):
        """处理因子更新"""
        pass
    
    def emit_order(self, order_data: Any):
        """发送订单"""
        event = Event(EventType.ORDER, order_data)
        self.event_engine.put(event)


class RiskHandler:
    """风险处理器基类"""
    
    def __init__(self, event_engine: EventEngine, risk_limits: Dict = None):
        self.event_engine = event_engine
        self.risk_limits = risk_limits or {}
        self.event_engine.register(EventType.ORDER, self.on_order)
    
    def on_order(self, event: Event) -> bool:
        """
        处理订单（风险检查）
        
        Returns:
            是否通过风险检查
        """
        return True
    
    def emit_risk_alert(self, alert_data: Any):
        """发送风险警报"""
        event = Event(EventType.RISK, alert_data)
        self.event_engine.put(event)


class ExecutionHandler:
    """执行处理器基类"""
    
    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.event_engine.register(EventType.ORDER, self.on_order)
    
    def on_order(self, event: Event):
        """处理订单执行"""
        pass
    
    def emit_fill(self, fill_data: Any):
        """发送成交信息"""
        event = Event(EventType.FILL, fill_data)
        self.event_engine.put(event)


class PortfolioHandler:
    """持仓处理器基类"""
    
    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.positions = {}
        self.event_engine.register(EventType.FILL, self.on_fill)
    
    def on_fill(self, event: Event):
        """处理成交"""
        pass
    
    def update_position(self, stock_code: str, quantity: int, price: float):
        """更新持仓"""
        pass


class QuantTradingSystem:
    """量化交易系统"""
    
    def __init__(self):
        """初始化量化交易系统"""
        self.event_engine = EventEngine()
        self.data_handler = None
        self.strategy_handler = None
        self.risk_handler = None
        self.execution_handler = None
        self.portfolio_handler = None
    
    def initialize(
        self,
        data_handler: DataHandler = None,
        strategy_handler: StrategyHandler = None,
        risk_handler: RiskHandler = None,
        execution_handler: ExecutionHandler = None,
        portfolio_handler: PortfolioHandler = None
    ):
        """
        初始化系统组件
        
        Args:
            data_handler: 数据处理器
            strategy_handler: 策略处理器
            risk_handler: 风险处理器
            execution_handler: 执行处理器
            portfolio_handler: 持仓处理器
        """
        if data_handler:
            self.data_handler = data_handler(self.event_engine)
        if strategy_handler:
            self.strategy_handler = strategy_handler(self.event_engine)
        if risk_handler:
            self.risk_handler = risk_handler(self.event_engine)
        if execution_handler:
            self.execution_handler = execution_handler(self.event_engine)
        if portfolio_handler:
            self.portfolio_handler = portfolio_handler(self.event_engine)
        
        logger.info("量化交易系统初始化完成")
    
    def start(self):
        """启动系统"""
        self.event_engine.start()
        logger.info("量化交易系统已启动")
    
    def stop(self):
        """停止系统"""
        self.event_engine.stop()
        logger.info("量化交易系统已停止")
    
    def on_data(self, data: Any):
        """
        输入数据
        
        Args:
            data: 市场数据
        """
        event = Event(EventType.MARKET_DATA, data)
        self.event_engine.put(event)
    
    def on_timer(self):
        """定时事件"""
        event = Event(EventType.TIMER, datetime.now())
        self.event_engine.put(event)


if __name__ == "__main__":
    print("=" * 60)
    print("事件驱动引擎测试")
    print("=" * 60)
    
    engine = EventEngine()
    
    def on_market_data(event: Event):
        print(f"[{event.timestamp}] 收到市场数据: {event.data}")
        engine.put(Event(EventType.TRADE_SIGNAL, {"action": "buy", "stock": "600000"}))
    
    def on_signal(event: Event):
        print(f"[{event.timestamp}] 收到交易信号: {event.data}")
    
    engine.register(EventType.MARKET_DATA, on_market_data)
    engine.register(EventType.TRADE_SIGNAL, on_signal)
    
    engine.start()
    
    import time
    engine.put(Event(EventType.MARKET_DATA, {"price": 10.5, "volume": 1000}))
    time.sleep(1)
    
    engine.stop()
    print("\n测试完成")
