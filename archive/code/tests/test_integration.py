#!/usr/bin/env python3
"""
集成测试模块 - 验证模块间交互的正确性
测试完整的回测流程和模块协作
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from market_constraint import MarketConstraintModule, MarketConstraintConfig
from benchmark_analyzer import BenchmarkAnalyzer, BenchmarkConfig
from portfolio_optimizer import PortfolioOptimizer, OptimizationConfig, OptimizationMethod
from brinson_attribution import BrinsonAttribution, BrinsonConfig
from rolling_performance import RollingPerformanceAnalyzer, RollingConfig


class TestIntegration(unittest.TestCase):
    """集成测试类"""
    
    def setUp(self):
        np.random.seed(42)
        self.n_periods = 252
        self.n_assets = 20
        self.dates = pd.date_range('2023-01-01', periods=self.n_periods, freq='B')
        
        self.stock_data = self._generate_stock_data()
        self.returns_data = self._generate_returns_data()
    
    def _generate_stock_data(self) -> pd.DataFrame:
        data = []
        for date in self.dates:
            for i in range(self.n_assets):
                prev_close = 10 + np.random.randn() * 2
                change = np.random.normal(0, 0.02)
                close = prev_close * (1 + change)
                
                is_limit_up = change > 0.095
                is_limit_down = change < -0.095
                
                data.append({
                    'date': date,
                    'stock_code': f'00000{i:02d}',
                    'close': close,
                    'prev_close': prev_close,
                    'volume': np.random.randint(100000, 10000000),
                    'is_suspended': 1 if np.random.random() < 0.02 else 0,
                    '股票名称': f'股票{i}',
                    'limit_up': is_limit_up,
                    'limit_down': is_limit_down
                })
        
        return pd.DataFrame(data)
    
    def _generate_returns_data(self) -> pd.DataFrame:
        returns = np.random.normal(0.0005, 0.02, (self.n_periods, self.n_assets))
        return pd.DataFrame(returns, columns=[f'Stock_{i}' for i in range(self.n_assets)])
    
    def test_market_constraint_with_portfolio_optimizer(self):
        config = MarketConstraintConfig()
        constraint_module = MarketConstraintModule(config)
        
        optimizer = PortfolioOptimizer()
        optimizer.set_parameters_from_data(self.returns_data)
        
        target_weights = optimizer.max_sharpe_optimize()
        
        test_date = self.dates[100]
        day_data = self.stock_data[self.stock_data['date'] == test_date]
        
        successful_trades = 0
        for i, row in day_data.iterrows():
            if target_weights[i % self.n_assets] > 0.01:
                result = constraint_module.check_buy_constraint(
                    row['stock_code'], row,
                    target_quantity=1000,
                    target_value=row['close'] * 1000
                )
                if result['allowed']:
                    successful_trades += 1
        
        self.assertGreater(successful_trades, 0)
    
    def test_portfolio_optimizer_with_benchmark_analyzer(self):
        optimizer = PortfolioOptimizer()
        optimizer.set_parameters_from_data(self.returns_data)
        
        weights = optimizer.risk_parity_optimize()
        
        portfolio_returns = (self.returns_data * weights).sum(axis=1)
        
        benchmark_returns = pd.Series(
            np.random.normal(0.0004, 0.015, self.n_periods),
            index=portfolio_returns.index
        )
        
        analyzer = BenchmarkAnalyzer()
        analyzer.set_strategy_returns(portfolio_returns)
        analyzer.set_benchmark(benchmark_returns)
        
        report = analyzer.generate_comparison_report()
        
        self.assertIn('strategy_metrics', report)
        self.assertIn('relative_metrics', report)
        
        beta = report['relative_metrics']['beta']
        self.assertTrue(0 < beta < 2)
    
    def test_full_backtest_flow(self):
        constraint_config = MarketConstraintConfig()
        constraint_module = MarketConstraintModule(constraint_config)
        
        optimizer_config = OptimizationConfig()
        optimizer = PortfolioOptimizer(optimizer_config)
        optimizer.set_parameters_from_data(self.returns_data)
        
        weights = optimizer.equal_weight()
        
        portfolio_values = [1000000]
        for i in range(self.n_periods):
            daily_return = (self.returns_data.iloc[i] * weights).sum()
            
            day_data = self.stock_data[self.stock_data['date'] == self.dates[i]]
            constraint_effect = 0
            
            for j, row in day_data.iterrows():
                if weights[j % self.n_assets] > 0.01:
                    result = constraint_module.check_buy_constraint(
                        row['stock_code'], row,
                        target_quantity=100,
                        target_value=row['close'] * 100
                    )
                    if not result['allowed']:
                        constraint_effect += weights[j % self.n_assets] * 0.01
            
            adjusted_return = daily_return - constraint_effect
            portfolio_values.append(portfolio_values[-1] * (1 + adjusted_return))
        
        portfolio_values = pd.Series(portfolio_values[1:], index=self.dates)
        
        self.assertEqual(len(portfolio_values), self.n_periods)
        self.assertGreater(portfolio_values.iloc[-1], 0)
    
    def test_benchmark_analyzer_with_rolling_performance(self):
        portfolio_returns = self.returns_data.mean(axis=1)
        benchmark_returns = pd.Series(
            np.random.normal(0.0004, 0.015, self.n_periods),
            index=portfolio_returns.index
        )
        
        benchmark_analyzer = BenchmarkAnalyzer()
        benchmark_analyzer.set_strategy_returns(portfolio_returns)
        benchmark_analyzer.set_benchmark(benchmark_returns)
        
        rolling_config = RollingConfig(window=63)
        rolling_analyzer = RollingPerformanceAnalyzer(rolling_config)
        rolling_analyzer.set_returns(portfolio_returns)
        rolling_analyzer.set_benchmark(benchmark_returns)
        
        benchmark_report = benchmark_analyzer.generate_comparison_report()
        rolling_metrics = rolling_analyzer.calculate_all_rolling_metrics()
        
        self.assertIn('strategy_metrics', benchmark_report)
        self.assertIn('rolling_sharpe', rolling_metrics)
        
        avg_rolling_sharpe = rolling_metrics['rolling_sharpe'].dropna().mean()
        static_sharpe = benchmark_report['strategy_metrics']['sharpe_ratio']
        
        self.assertIsInstance(avg_rolling_sharpe, float)
        self.assertIsInstance(static_sharpe, float)
    
    def test_brinson_attribution_with_portfolio_optimizer(self):
        n_periods = 12
        categories = ['金融', '科技', '消费', '医药', '工业']
        
        returns_data = pd.DataFrame(
            np.random.normal(0.02, 0.05, (n_periods, 5)),
            columns=categories
        )
        
        optimizer = PortfolioOptimizer()
        optimizer.set_parameters_from_data(returns_data)
        
        portfolio_weights_list = []
        for i in range(n_periods):
            weights = optimizer.risk_parity_optimize()
            portfolio_weights_list.append(weights)
        
        portfolio_weights = pd.DataFrame(portfolio_weights_list, columns=categories)
        benchmark_weights = pd.DataFrame(
            [[0.25, 0.25, 0.20, 0.15, 0.15]] * n_periods,
            columns=categories
        )
        
        portfolio_returns = returns_data
        benchmark_returns = pd.DataFrame(
            np.random.normal(0.015, 0.04, (n_periods, 5)),
            columns=categories
        )
        
        brinson = BrinsonAttribution()
        brinson.set_data(
            portfolio_weights, portfolio_returns,
            benchmark_weights, benchmark_returns
        )
        
        results = brinson.run_attribution()
        
        self.assertIn('summary', results)
        self.assertIn('total_allocation', results['summary'])
        self.assertIn('total_selection', results['summary'])
    
    def test_complete_performance_analysis_flow(self):
        portfolio_returns = self.returns_data.mean(axis=1)
        benchmark_returns = pd.Series(
            np.random.normal(0.0004, 0.015, self.n_periods),
            index=portfolio_returns.index
        )
        
        benchmark_analyzer = BenchmarkAnalyzer()
        benchmark_analyzer.set_strategy_returns(portfolio_returns)
        benchmark_analyzer.set_benchmark(benchmark_returns)
        
        rolling_analyzer = RollingPerformanceAnalyzer(RollingConfig(window=63))
        rolling_analyzer.set_returns(portfolio_returns)
        rolling_analyzer.set_benchmark(benchmark_returns)
        
        benchmark_report = benchmark_analyzer.generate_comparison_report()
        rolling_report = rolling_analyzer.generate_rolling_report()
        stability = rolling_analyzer.analyze_stability()
        
        self.assertIn('strategy_metrics', benchmark_report)
        self.assertIn('relative_metrics', benchmark_report)
        self.assertIn('# 滚动绩效分析报告', rolling_report)
        self.assertIn('rolling_return', stability)
        
        ir = benchmark_report['relative_metrics']['information_ratio']
        rolling_ir = rolling_metrics.get('rolling_information_ratio', pd.Series())
        
        if len(rolling_ir.dropna()) > 0:
            avg_rolling_ir = rolling_ir.dropna().mean()
            self.assertIsInstance(avg_rolling_ir, float)


class TestEndToEnd(unittest.TestCase):
    """端到端测试"""
    
    def test_complete_backtest_pipeline(self):
        np.random.seed(42)
        n_periods = 252
        n_assets = 10
        dates = pd.date_range('2023-01-01', periods=n_periods, freq='B')
        
        returns_data = pd.DataFrame(
            np.random.normal(0.0005, 0.02, (n_periods, n_assets)),
            columns=[f'Stock_{i}' for i in range(n_assets)]
        )
        
        optimizer = PortfolioOptimizer()
        optimizer.set_parameters_from_data(returns_data)
        weights = optimizer.max_sharpe_optimize()
        
        portfolio_returns = (returns_data * weights).sum(axis=1)
        
        benchmark_returns = pd.Series(
            np.random.normal(0.0004, 0.012, n_periods),
            index=portfolio_returns.index
        )
        
        benchmark_analyzer = BenchmarkAnalyzer()
        benchmark_analyzer.set_strategy_returns(portfolio_returns)
        benchmark_analyzer.set_benchmark(benchmark_returns)
        benchmark_report = benchmark_analyzer.generate_comparison_report()
        
        rolling_analyzer = RollingPerformanceAnalyzer(RollingConfig(window=63))
        rolling_analyzer.set_returns(portfolio_returns)
        rolling_analyzer.set_benchmark(benchmark_returns)
        stability = rolling_analyzer.analyze_stability()
        
        self.assertIn('strategy_metrics', benchmark_report)
        self.assertIn('relative_metrics', benchmark_report)
        self.assertIn('rolling_return', stability)
        
        sharpe = benchmark_report['strategy_metrics']['sharpe_ratio']
        self.assertIsInstance(sharpe, float)
        
        print("\n端到端测试结果:")
        print(f"  策略夏普比率: {sharpe:.2f}")
        print(f"  策略年化收益: {benchmark_report['strategy_metrics']['annual_return']:.2%}")
        print(f"  信息比率: {benchmark_report['relative_metrics']['information_ratio']:.2f}")


def run_integration_tests():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestEndToEnd))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    total_tests = result.testsRun
    passed_tests = total_tests - len(result.failures) - len(result.errors)
    
    print("\n" + "=" * 60)
    print("集成测试结果")
    print("=" * 60)
    print(f"总测试数: {total_tests}")
    print(f"通过测试: {passed_tests}")
    print(f"失败测试: {len(result.failures)}")
    print(f"错误测试: {len(result.errors)}")
    print("=" * 60)
    
    return result


if __name__ == '__main__':
    run_integration_tests()
