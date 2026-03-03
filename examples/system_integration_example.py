#!/usr/bin/env python3
"""
系统集成示例
展示如何使用SystemManager整合所有组件
"""

import sys
import os

# 添加代码路径
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'code'))

from system.system_manager import SystemManager, DataPipeline
from data.multi_source_fetcher import MultiSourceStockFetcher
from data.data_quality_framework import DataQualityPipeline
from backtest.backtest_engine_v2 import BacktestEngineV2
from backtest.real_time_trading import RealTimeTrader, SimulatedTradingAPI
from backtest.broker_api import BrokerAPIFactory
from trader.trader_assistant import TraderAssistant
from risk.fund_management import FundManager, RiskBudgetManager
from portfolio.portfolio_optimizer import PortfolioOptimizer
from strategy.alpha_stock_selector import AlphaStockSelector
from strategy.multi_factor_model import DynamicFactorWeightSystem
from risk.risk_calculator import FactorRiskModel, FactorExposureMonitor


def initialize_system():
    """初始化系统"""
    print("=== 初始化系统 ===")
    
    # 创建系统管理器
    system_manager = SystemManager()
    
    # 注册核心组件
    print("\n1. 注册核心组件")
    
    # 数据获取组件
    fetcher = MultiSourceStockFetcher()
    system_manager.register_component('data_fetcher', fetcher)
    
    # 数据质量框架
    quality_framework = DataQualityPipeline()
    system_manager.register_component('data_quality', quality_framework)
    
    # 回测引擎
    backtest_engine = BacktestEngineV2()
    system_manager.register_component('backtest_engine', backtest_engine)
    
    # 实时交易
    trading_api = SimulatedTradingAPI()
    real_time_trader = RealTimeTrader(trading_api)
    system_manager.register_component('real_time_trader', real_time_trader)
    
    # 资金管理
    fund_manager = FundManager(initial_capital=1000000)
    system_manager.register_component('fund_manager', fund_manager)
    
    # 风险预算管理
    risk_budget_manager = RiskBudgetManager()
    system_manager.register_component('risk_budget_manager', risk_budget_manager)
    
    # 组合优化
    portfolio_optimizer = PortfolioOptimizer()
    system_manager.register_component('portfolio_optimizer', portfolio_optimizer)
    
    # 选股系统
    stock_selector = AlphaStockSelector()
    system_manager.register_component('stock_selector', stock_selector)
    
    # 因子权重系统
    factor_weight_system = DynamicFactorWeightSystem()
    system_manager.register_component('factor_weight_system', factor_weight_system)
    
    # 因子风险模型
    factor_risk_model = FactorRiskModel()
    system_manager.register_component('factor_risk_model', factor_risk_model)
    
    # 因子暴露监控
    factor_exposure_monitor = FactorExposureMonitor()
    system_manager.register_component('factor_exposure_monitor', factor_exposure_monitor)
    
    # 券商API
    huatai_api = BrokerAPIFactory.create_broker_api('huatai')
    huatai_trader = RealTimeTrader(huatai_api)
    system_manager.register_component('huatai_trader', huatai_trader)
    
    # 交易员辅助功能
    trader_assistant = TraderAssistant()
    system_manager.register_component('trader_assistant', trader_assistant)
    
    return system_manager


def create_data_pipeline(system_manager):
    """创建数据处理管道"""
    print("\n2. 创建数据处理管道")
    
    pipeline = DataPipeline(system_manager)
    
    # 添加数据处理步骤
    def fetch_data_step(data):
        fetcher = system_manager.get_component('data_fetcher')
        print("  - 获取数据")
        # 这里可以根据实际需要获取数据
        return data
    
    def quality_check_step(data):
        quality_framework = system_manager.get_component('data_quality')
        print("  - 数据质量检查")
        # 这里可以根据实际需要进行数据质量检查
        return data
    
    def stock_selection_step(data):
        stock_selector = system_manager.get_component('stock_selector')
        print("  - 选股")
        # 这里可以根据实际需要进行选股
        return data
    
    def portfolio_optimization_step(data):
        portfolio_optimizer = system_manager.get_component('portfolio_optimizer')
        print("  - 组合优化")
        # 这里可以根据实际需要进行组合优化
        return data
    
    def risk_management_step(data):
        factor_risk_model = system_manager.get_component('factor_risk_model')
        print("  - 风险评估")
        # 这里可以根据实际需要进行风险评估
        return data
    
    # 添加步骤到管道
    pipeline.add_step('fetch_data', fetch_data_step)
    pipeline.add_step('quality_check', quality_check_step)
    pipeline.add_step('stock_selection', stock_selection_step)
    pipeline.add_step('portfolio_optimization', portfolio_optimization_step)
    pipeline.add_step('risk_management', risk_management_step)
    
    return pipeline


def run_system_demo(system_manager, pipeline):
    """运行系统演示"""
    print("\n3. 运行系统演示")
    
    # 启动系统
    print("  - 启动系统")
    system_manager.start()
    
    # 运行健康检查
    print("  - 运行健康检查")
    health_status = system_manager.health_check()
    print(f"    系统状态: {health_status['system_status']}")
    print(f"    组件数量: {len(health_status['components'])}")
    
    # 运行数据处理管道
    print("  - 运行数据处理管道")
    test_data = {}
    result = pipeline.run(test_data)
    
    # 生成系统报告
    print("  - 生成系统报告")
    report_path = system_manager.save_system_report()
    print(f"    系统报告已保存: {report_path}")
    
    # 演示券商API
    print("  - 演示券商API")
    huatai_trader = system_manager.get_component('huatai_trader')
    if huatai_trader:
        print("    连接华泰证券API")
        huatai_trader.connect()
        print("    获取账户信息")
        account_info = huatai_trader.get_account()
        print(f"    账户信息: {account_info}")
        print("    断开连接")
        huatai_trader.disconnect()
    
    # 演示交易员辅助功能
    print("  - 演示交易员辅助功能")
    trader_assistant = system_manager.get_component('trader_assistant')
    if trader_assistant:
        print("    生成交易报表")
        test_positions = {
            '600519': {
                'stock_code': '600519',
                'quantity': 100,
                'avg_price': 1800.0,
                'market_value': 180000.0
            }
        }
        test_account = {
            'cash': 820000.0,
            'total_value': 1000000.0,
            'positions_value': 180000.0,
            'pnl': 0.0
        }
        test_trades = [
            {
                'time': '09:30:00',
                'stock_code': '600519',
                'side': 'buy',
                'price': 1800.0,
                'quantity': 100
            }
        ]
        report_path = trader_assistant.generate_report(
            test_positions, test_account, test_trades, 'daily'
        )
        print(f"    每日报表生成成功: {report_path}")
        
        print("    提交交易员反馈")
        feedback = {
            'type': 'system',
            'content': '系统运行良好，建议增加更多数据源',
            'rating': 5
        }
        feedback_id = trader_assistant.submit_feedback('trader_001', feedback)
        print(f"    反馈提交成功: {feedback_id}")
    
    # 关闭系统
    print("  - 关闭系统")
    system_manager.shutdown()


def demonstrate_plugin_system(system_manager):
    """演示插件系统"""
    print("\n4. 演示插件系统")
    
    # 这里可以演示如何加载和运行插件
    print("  - 插件系统演示")
    print("    插件系统已集成到SystemManager中")
    print("    可以通过load_plugin()方法加载自定义插件")


def main():
    """主函数"""
    print("A股量化系统集成示例")
    print("=" * 50)
    
    # 初始化系统
    system_manager = initialize_system()
    
    # 创建数据处理管道
    pipeline = create_data_pipeline(system_manager)
    
    # 运行系统演示
    run_system_demo(system_manager, pipeline)
    
    # 演示插件系统
    demonstrate_plugin_system(system_manager)
    
    print("\n" + "=" * 50)
    print("系统集成示例完成")
    print("提示: 此示例展示了如何使用SystemManager整合所有组件")
    print("在实际应用中，您可以根据需要调整和扩展系统")


if __name__ == '__main__':
    main()
