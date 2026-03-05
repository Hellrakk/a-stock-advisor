#!/usr/bin/env python3
"""
高级功能集成模块
整合所有新实现的功能到现有系统中
"""

import sys
import os
from typing import Dict, List, Optional, Any
import logging

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from code.system.system_manager import SystemManager, Plugin
from code.strategy.market_state_identifier import MarketStateManager
from code.data.alternative_data_framework import AlternativeDataLab
from code.risk.performance_attribution import AttributionManager
from code.strategy.alpha_factory import AlphaFactory
from code.strategy.rl_optimizer import RLPortfolioManager, RLTradingManager
from code.risk.risk_early_warning import RiskEarlyWarningSystem

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MarketStatePlugin(Plugin):
    """市场状态识别插件"""
    
    def __init__(self, name: str, config: Dict = None):
        super().__init__(name, config)
        self.market_state_manager = None
    
    def initialize(self) -> bool:
        try:
            self.market_state_manager = MarketStateManager()
            logger.info("市场状态管理器初始化成功")
            return True
        except Exception as e:
            logger.error(f"市场状态管理器初始化失败: {e}")
            return False
    
    def run(self, *args, **kwargs) -> Any:
        if self.market_state_manager:
            return self.market_state_manager.update(*args, **kwargs)
        return None
    
    def shutdown(self) -> bool:
        logger.info("市场状态管理器关闭")
        return True


class AlternativeDataPlugin(Plugin):
    """另类数据插件"""
    
    def __init__(self, name: str, config: Dict = None):
        super().__init__(name, config)
        self.alternative_data_lab = None
    
    def initialize(self) -> bool:
        try:
            self.alternative_data_lab = AlternativeDataLab()
            logger.info("另类数据实验室初始化成功")
            return True
        except Exception as e:
            logger.error(f"另类数据实验室初始化失败: {e}")
            return False
    
    def run(self, *args, **kwargs) -> Any:
        if self.alternative_data_lab:
            return self.alternative_data_lab.test_data_source(*args, **kwargs)
        return None
    
    def shutdown(self) -> bool:
        logger.info("另类数据实验室关闭")
        return True


class AttributionPlugin(Plugin):
    """业绩归因插件"""
    
    def __init__(self, name: str, config: Dict = None):
        super().__init__(name, config)
        self.attribution_manager = None
    
    def initialize(self) -> bool:
        try:
            self.attribution_manager = AttributionManager()
            logger.info("业绩归因管理器初始化成功")
            return True
        except Exception as e:
            logger.error(f"业绩归因管理器初始化失败: {e}")
            return False
    
    def run(self, *args, **kwargs) -> Any:
        if self.attribution_manager:
            return self.attribution_manager.run_full_attribution(*args, **kwargs)
        return None
    
    def shutdown(self) -> bool:
        logger.info("业绩归因管理器关闭")
        return True


class AlphaFactoryPlugin(Plugin):
    """阿尔法工厂插件"""
    
    def __init__(self, name: str, config: Dict = None):
        super().__init__(name, config)
        self.alpha_factory = None
    
    def initialize(self) -> bool:
        try:
            self.alpha_factory = AlphaFactory()
            logger.info("阿尔法工厂初始化成功")
            return True
        except Exception as e:
            logger.error(f"阿尔法工厂初始化失败: {e}")
            return False
    
    def run(self, *args, **kwargs) -> Any:
        if self.alpha_factory:
            return self.alpha_factory.run_pipeline(*args, **kwargs)
        return None
    
    def shutdown(self) -> bool:
        logger.info("阿尔法工厂关闭")
        return True


class RLOptimizerPlugin(Plugin):
    """强化学习优化器插件"""
    
    def __init__(self, name: str, config: Dict = None):
        super().__init__(name, config)
        self.portfolio_manager = None
        self.trading_manager = None
    
    def initialize(self) -> bool:
        try:
            # 初始化需要数据，这里先创建空实例
            self.portfolio_manager = None
            self.trading_manager = None
            logger.info("强化学习优化器初始化成功")
            return True
        except Exception as e:
            logger.error(f"强化学习优化器初始化失败: {e}")
            return False
    
    def run(self, *args, **kwargs) -> Any:
        action = kwargs.get('action', 'portfolio')
        if action == 'portfolio' and 'market_data' in kwargs and 'strategy_returns' in kwargs:
            if not self.portfolio_manager:
                self.portfolio_manager = RLPortfolioManager(
                    kwargs['market_data'],
                    kwargs['strategy_returns']
                )
            return self.portfolio_manager.run_backtest()
        elif action == 'trading' and 'price_data' in kwargs:
            if not self.trading_manager:
                self.trading_manager = RLTradingManager(kwargs['price_data'])
            return self.trading_manager.execute_trade(kwargs.get('order_size', 10000))
        return None
    
    def shutdown(self) -> bool:
        logger.info("强化学习优化器关闭")
        return True


class RiskEarlyWarningPlugin(Plugin):
    """风险预警插件"""
    
    def __init__(self, name: str, config: Dict = None):
        super().__init__(name, config)
        self.risk_system = None
    
    def initialize(self) -> bool:
        try:
            self.risk_system = RiskEarlyWarningSystem()
            logger.info("风险预警系统初始化成功")
            return True
        except Exception as e:
            logger.error(f"风险预警系统初始化失败: {e}")
            return False
    
    def run(self, *args, **kwargs) -> Any:
        if self.risk_system:
            return self.risk_system.generate_risk_report()
        return None
    
    def shutdown(self) -> bool:
        logger.info("风险预警系统关闭")
        return True


class AdvancedSystemIntegrator:
    """高级系统集成器"""
    
    def __init__(self, system_manager: SystemManager):
        self.system_manager = system_manager
        self.plugins = {
            'market_state': MarketStatePlugin('market_state'),
            'alternative_data': AlternativeDataPlugin('alternative_data'),
            'attribution': AttributionPlugin('attribution'),
            'alpha_factory': AlphaFactoryPlugin('alpha_factory'),
            'rl_optimizer': RLOptimizerPlugin('rl_optimizer'),
            'risk_early_warning': RiskEarlyWarningPlugin('risk_early_warning')
        }
    
    def integrate_all(self):
        """集成所有高级功能"""
        logger.info("开始集成高级功能...")
        
        # 注册插件
        for name, plugin in self.plugins.items():
            if plugin.initialize():
                self.system_manager.register_component(name, plugin)
                logger.info(f"成功集成 {name} 插件")
            else:
                logger.error(f"集成 {name} 插件失败")
        
        # 注册为系统服务
        self.system_manager.register_component('advanced_integrator', self)
        logger.info("高级功能集成完成")
    
    def get_plugin(self, name: str) -> Optional[Plugin]:
        """获取插件"""
        return self.plugins.get(name)
    
    def run_plugin(self, name: str, *args, **kwargs) -> Any:
        """运行插件"""
        plugin = self.get_plugin(name)
        if plugin and plugin.enabled:
            return plugin.run(*args, **kwargs)
        return None
    
    def health_check(self) -> Dict:
        """健康检查"""
        status = {}
        for name, plugin in self.plugins.items():
            try:
                status[name] = {'status': 'ok' if plugin.enabled else 'disabled'}
            except Exception as e:
                status[name] = {'status': 'error', 'message': str(e)}
        return status
    
    def shutdown(self):
        """关闭所有插件"""
        for name, plugin in self.plugins.items():
            plugin.shutdown()
        logger.info("所有高级功能插件已关闭")


def integrate_advanced_features():
    """集成高级功能的主函数"""
    # 创建系统管理器
    system_manager = SystemManager()
    
    # 创建集成器
    integrator = AdvancedSystemIntegrator(system_manager)
    
    # 集成所有功能
    integrator.integrate_all()
    
    # 启动系统
    system_manager.start()
    
    # 运行健康检查
    health = system_manager.health_check()
    logger.info(f"系统健康状态: {health['system_status']}")
    
    # 生成系统报告
    system_manager.save_system_report()
    
    return system_manager, integrator


if __name__ == '__main__':
    print("=== 高级功能集成测试 ===")
    
    # 集成高级功能
    system_manager, integrator = integrate_advanced_features()
    
    # 测试插件运行
    print("\n测试插件运行...")
    
    # 测试市场状态插件
    try:
        # 模拟数据
        import pandas as pd
        import numpy as np
        from datetime import datetime
        
        # 创建测试数据
        dates = pd.date_range(start='2023-01-01', periods=30, freq='B')
        prices = pd.DataFrame({
            'close': np.random.normal(3000, 100, 30)
        }, index=dates)
        
        # 测试市场状态更新
        result = integrator.run_plugin('market_state', prices)
        if result:
            print(f"市场状态: {result['market_state']}")
    except Exception as e:
        logger.error(f"测试市场状态插件失败: {e}")
    
    # 测试风险预警插件
    try:
        result = integrator.run_plugin('risk_early_warning')
        if result:
            print(f"风险报告生成成功，预警数量: {result['alert_count']}")
    except Exception as e:
        logger.error(f"测试风险预警插件失败: {e}")
    
    # 关闭系统
    print("\n关闭系统...")
    system_manager.shutdown()
    integrator.shutdown()
    print("系统已关闭")
