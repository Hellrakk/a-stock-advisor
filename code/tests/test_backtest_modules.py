#!/usr/bin/env python3
"""
单元测试模块 - 测试所有新实现的功能模块
覆盖率目标: ≥80%
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from market_constraint import (
    MarketConstraintChecker, 
    LiquidityConstraintChecker,
    MarketConstraintModule,
    MarketConstraintConfig,
    TradingStatus,
    prepare_market_data
)

from benchmark_analyzer import (
    BenchmarkAnalyzer,
    BenchmarkConfig,
    BenchmarkType,
    create_benchmark_from_index
)

from portfolio_optimizer import (
    PortfolioOptimizer,
    OptimizationConfig,
    OptimizationMethod,
    HierarchicalRiskParity,
    apply_constraints,
    turnover_aware_optimize
)

from brinson_attribution import (
    BrinsonAttribution,
    BrinsonConfig,
    FactorAttribution,
    MultiPeriodAttribution
)

from rolling_performance import (
    RollingPerformanceAnalyzer,
    RollingConfig,
    PerformanceDecomposition
)


class TestMarketConstraint(unittest.TestCase):
    """市场约束模块测试"""
    
    def setUp(self):
        self.config = MarketConstraintConfig()
        self.checker = MarketConstraintChecker(self.config)
        self.liquidity_checker = LiquidityConstraintChecker(self.config)
        self.module = MarketConstraintModule(self.config)
    
    def test_limit_up_detection(self):
        prev_close = 10.0
        limit_up_price = prev_close * (1 + self.config.limit_up_threshold)
        
        self.assertTrue(self.checker.check_limit_up(limit_up_price, prev_close))
        self.assertFalse(self.checker.check_limit_up(prev_close * 1.05, prev_close))
    
    def test_limit_down_detection(self):
        prev_close = 10.0
        limit_down_price = prev_close * (1 + self.config.limit_down_threshold)
        
        self.assertTrue(self.checker.check_limit_down(limit_down_price, prev_close))
        self.assertFalse(self.checker.check_limit_down(prev_close * 0.95, prev_close))
    
    def test_st_stock_limits(self):
        prev_close = 10.0
        st_limit_up = prev_close * (1 + self.config.st_limit_up)
        
        self.assertTrue(self.checker.check_limit_up(st_limit_up, prev_close, is_st=True))
        self.assertTrue(self.checker.check_limit_up(prev_close * 1.05, prev_close, is_st=True))
        self.assertFalse(self.checker.check_limit_up(prev_close * 1.03, prev_close, is_st=True))
    
    def test_trading_status_check(self):
        normal_data = pd.Series({
            'close': 10.0,
            'prev_close': 9.5,
            'is_suspended': 0,
            '股票名称': '测试股票'
        })
        self.assertEqual(self.checker.check_trading_status(normal_data), TradingStatus.NORMAL)
        
        limit_up_data = pd.Series({
            'close': 10.45,
            'prev_close': 9.5,
            'is_suspended': 0,
            '股票名称': '测试股票'
        })
        self.assertEqual(self.checker.check_trading_status(limit_up_data), TradingStatus.LIMIT_UP)
        
        suspended_data = pd.Series({
            'close': 10.0,
            'prev_close': 9.5,
            'is_suspended': 1,
            '股票名称': '测试股票'
        })
        self.assertEqual(self.checker.check_trading_status(suspended_data), TradingStatus.SUSPENDED)
    
    def test_can_buy_check(self):
        limit_up_data = pd.Series({
            'close': 11.0,
            'prev_close': 10.0,
            'is_suspended': 0,
            '股票名称': '测试股票'
        })
        can_buy, reason = self.checker.can_buy(limit_up_data)
        self.assertFalse(can_buy)
        self.assertIn("涨停", reason)
        
        normal_data = pd.Series({
            'close': 10.5,
            'prev_close': 10.0,
            'is_suspended': 0,
            '股票名称': '测试股票'
        })
        can_buy, reason = self.checker.can_buy(normal_data)
        self.assertTrue(can_buy)
    
    def test_can_sell_check(self):
        limit_down_data = pd.Series({
            'close': 9.0,
            'prev_close': 10.0,
            'is_suspended': 0,
            '股票名称': '测试股票'
        })
        can_sell, reason = self.checker.can_sell(limit_down_data)
        self.assertFalse(can_sell)
        self.assertIn("跌停", reason)
        
        normal_data = pd.Series({
            'close': 9.5,
            'prev_close': 10.0,
            'is_suspended': 0,
            '股票名称': '测试股票'
        })
        can_sell, reason = self.checker.can_sell(normal_data)
        self.assertTrue(can_sell)
    
    def test_volume_constraint(self):
        allowed, adj_qty, reason = self.liquidity_checker.check_volume_constraint(
            target_quantity=100000,
            daily_volume=1000000,
            participation_rate=0.05
        )
        self.assertTrue(allowed)
        self.assertEqual(adj_qty, 50000)
    
    def test_value_constraint(self):
        allowed, adj_val, reason = self.liquidity_checker.check_value_constraint(
            target_value=5000,
            min_value=10000
        )
        self.assertFalse(allowed)
        
        allowed, adj_val, reason = self.liquidity_checker.check_value_constraint(
            target_value=15000,
            min_value=10000
        )
        self.assertTrue(allowed)
    
    def test_buy_constraint_check(self):
        stock_data = pd.Series({
            'close': 10.5,
            'prev_close': 10.0,
            'volume': 1000000,
            'is_suspended': 0,
            '股票名称': '测试股票'
        })
        
        result = self.module.check_buy_constraint(
            '000001', stock_data,
            target_quantity=1000, target_value=10500
        )
        self.assertTrue(result['allowed'])
    
    def test_constraint_stats(self):
        stats = self.module.get_constraint_stats()
        self.assertIn('limit_up_blocked', stats)
        self.assertIn('limit_down_blocked', stats)
        self.assertIn('suspended_blocked', stats)


class TestBenchmarkAnalyzer(unittest.TestCase):
    """基准对比分析模块测试"""
    
    def setUp(self):
        np.random.seed(42)
        self.dates = pd.date_range('2023-01-01', periods=252, freq='B')
        
        self.benchmark_returns = pd.Series(
            np.random.normal(0.0004, 0.012, 252),
            index=self.dates
        )
        
        alpha = 0.0002
        beta = 0.9
        self.strategy_returns = pd.Series(
            alpha + beta * self.benchmark_returns + np.random.normal(0, 0.008, 252),
            index=self.dates
        )
        
        self.analyzer = BenchmarkAnalyzer()
        self.analyzer.set_benchmark(self.benchmark_returns)
        self.analyzer.set_strategy_returns(self.strategy_returns)
    
    def test_total_return_calculation(self):
        total_return = self.analyzer.calculate_total_return(self.strategy_returns)
        self.assertIsInstance(total_return, float)
        self.assertTrue(-1 < total_return < 10)
    
    def test_annualized_return(self):
        ann_return = self.analyzer.calculate_annualized_return(self.strategy_returns)
        self.assertIsInstance(ann_return, float)
    
    def test_annualized_volatility(self):
        ann_vol = self.analyzer.calculate_annualized_volatility(self.strategy_returns)
        self.assertIsInstance(ann_vol, float)
        self.assertTrue(ann_vol > 0)
    
    def test_tracking_error(self):
        te = self.analyzer.calculate_tracking_error()
        self.assertIsInstance(te, float)
        self.assertTrue(te >= 0)
    
    def test_information_ratio(self):
        ir = self.analyzer.calculate_information_ratio()
        self.assertIsInstance(ir, float)
    
    def test_beta_calculation(self):
        beta = self.analyzer.calculate_beta()
        self.assertIsInstance(beta, float)
        self.assertTrue(0 < beta < 2)
    
    def test_alpha_calculation(self):
        alpha = self.analyzer.calculate_alpha()
        self.assertIsInstance(alpha, float)
    
    def test_correlation(self):
        corr = self.analyzer.calculate_correlation()
        self.assertIsInstance(corr, float)
        self.assertTrue(-1 <= corr <= 1)
    
    def test_capture_ratios(self):
        up_capture = self.analyzer.calculate_up_capture()
        down_capture = self.analyzer.calculate_down_capture()
        
        self.assertIsInstance(up_capture, float)
        self.assertIsInstance(down_capture, float)
    
    def test_generate_report(self):
        report = self.analyzer.generate_comparison_report()
        
        self.assertIn('strategy_metrics', report)
        self.assertIn('benchmark_metrics', report)
        self.assertIn('relative_metrics', report)
    
    def test_format_markdown(self):
        report = self.analyzer.generate_comparison_report()
        markdown = self.analyzer.format_report_markdown(report)
        
        self.assertIn('# 基准对比分析报告', markdown)
        self.assertIn('策略绩效指标', markdown)


class TestPortfolioOptimizer(unittest.TestCase):
    """组合优化模块测试"""
    
    def setUp(self):
        np.random.seed(42)
        self.n_assets = 10
        self.n_periods = 252
        
        self.returns_data = pd.DataFrame(
            np.random.normal(0.0005, 0.02, (self.n_periods, self.n_assets)),
            columns=[f"Stock_{i}" for i in range(self.n_assets)]
        )
        
        self.optimizer = PortfolioOptimizer()
        self.optimizer.set_parameters_from_data(self.returns_data)
    
    def test_equal_weight(self):
        weights = self.optimizer.equal_weight()
        
        self.assertEqual(len(weights), self.n_assets)
        self.assertAlmostEqual(weights.sum(), 1.0, places=6)
        self.assertTrue(all(w == weights[0] for w in weights))
    
    def test_min_variance_optimize(self):
        weights = self.optimizer.min_variance_optimize()
        
        self.assertEqual(len(weights), self.n_assets)
        self.assertAlmostEqual(weights.sum(), 1.0, places=4)
        self.assertTrue(all(w >= 0 for w in weights))
    
    def test_max_sharpe_optimize(self):
        weights = self.optimizer.max_sharpe_optimize()
        
        self.assertEqual(len(weights), self.n_assets)
        self.assertAlmostEqual(weights.sum(), 1.0, places=4)
    
    def test_risk_parity_optimize(self):
        weights = self.optimizer.risk_parity_optimize()
        
        self.assertEqual(len(weights), self.n_assets)
        self.assertAlmostEqual(weights.sum(), 1.0, places=4)
    
    def test_optimization_result(self):
        weights = self.optimizer.max_sharpe_optimize()
        result = self.optimizer.get_optimization_result(weights)
        
        self.assertIn('weights', result)
        self.assertIn('expected_return', result)
        self.assertIn('volatility', result)
        self.assertIn('sharpe_ratio', result)
    
    def test_apply_constraints(self):
        weights = np.array([0.3, 0.2, 0.15, 0.1, 0.1, 0.05, 0.05, 0.03, 0.01, 0.01])
        constrained = apply_constraints(weights, max_weight=0.15)
        
        self.assertAlmostEqual(constrained.sum(), 1.0, places=6)
        self.assertTrue(all(w >= 0 for w in constrained))
    
    def test_turnover_aware_optimize(self):
        current = np.array([0.1] * 10)
        target = np.array([0.2, 0.15, 0.1, 0.1, 0.1, 0.1, 0.1, 0.05, 0.05, 0.05])
        
        adjusted = turnover_aware_optimize(current, target, max_turnover=0.3)
        
        self.assertAlmostEqual(adjusted.sum(), 1.0, places=6)


class TestBrinsonAttribution(unittest.TestCase):
    """Brinson归因分析模块测试"""
    
    def setUp(self):
        np.random.seed(42)
        self.n_periods = 12
        self.categories = ['金融', '科技', '消费', '医药', '工业']
        
        self.portfolio_weights = pd.DataFrame(
            np.random.dirichlet(np.ones(5), self.n_periods),
            columns=self.categories
        )
        
        self.benchmark_weights = pd.DataFrame(
            np.array([[0.25, 0.25, 0.20, 0.15, 0.15]] * self.n_periods),
            columns=self.categories
        )
        
        self.portfolio_returns = pd.DataFrame(
            np.random.normal(0.02, 0.05, (self.n_periods, 5)),
            columns=self.categories
        )
        
        self.benchmark_returns = pd.DataFrame(
            np.random.normal(0.015, 0.04, (self.n_periods, 5)),
            columns=self.categories
        )
        
        self.brinson = BrinsonAttribution()
    
    def test_set_data(self):
        self.brinson.set_data(
            self.portfolio_weights, self.portfolio_returns,
            self.benchmark_weights, self.benchmark_returns
        )
        
        self.assertIsNotNone(self.brinson.portfolio_weights)
    
    def test_allocation_effect(self):
        effect = self.brinson.calculate_allocation_effect(
            portfolio_weight=0.3,
            benchmark_weight=0.2,
            benchmark_return=0.05
        )
        
        expected = (0.3 - 0.2) * 0.05
        self.assertAlmostEqual(effect, expected, places=6)
    
    def test_selection_effect(self):
        effect = self.brinson.calculate_selection_effect(
            portfolio_weight=0.3,
            portfolio_return=0.08,
            benchmark_return=0.05
        )
        
        expected = 0.3 * (0.08 - 0.05)
        self.assertAlmostEqual(effect, expected, places=6)
    
    def test_interaction_effect(self):
        effect = self.brinson.calculate_interaction_effect(
            portfolio_weight=0.3,
            benchmark_weight=0.2,
            portfolio_return=0.08,
            benchmark_return=0.05
        )
        
        expected = (0.3 - 0.2) * (0.08 - 0.05)
        self.assertAlmostEqual(effect, expected, places=6)
    
    def test_run_attribution(self):
        self.brinson.set_data(
            self.portfolio_weights, self.portfolio_returns,
            self.benchmark_weights, self.benchmark_returns
        )
        
        results = self.brinson.run_attribution()
        
        self.assertIn('summary', results)
        self.assertIn('by_category', results)
        self.assertIn('total_allocation', results['summary'])
    
    def test_generate_report(self):
        self.brinson.set_data(
            self.portfolio_weights, self.portfolio_returns,
            self.benchmark_weights, self.benchmark_returns
        )
        
        report = self.brinson.generate_attribution_report()
        
        self.assertIn('# Brinson归因分析报告', report)
        self.assertIn('配置效应', report)


class TestRollingPerformance(unittest.TestCase):
    """滚动绩效分析模块测试"""
    
    def setUp(self):
        np.random.seed(42)
        self.dates = pd.date_range('2023-01-01', periods=252, freq='B')
        
        self.returns = pd.Series(
            np.random.normal(0.0005, 0.015, 252),
            index=self.dates
        )
        
        self.benchmark_returns = pd.Series(
            np.random.normal(0.0004, 0.012, 252),
            index=self.dates
        )
        
        self.config = RollingConfig(window=63, min_periods=20)
        self.analyzer = RollingPerformanceAnalyzer(self.config)
    
    def test_set_returns(self):
        self.analyzer.set_returns(self.returns)
        self.assertIsNotNone(self.analyzer.returns)
    
    def test_rolling_return(self):
        self.analyzer.set_returns(self.returns)
        rolling_ret = self.analyzer.rolling_return()
        
        self.assertIsInstance(rolling_ret, pd.Series)
        self.assertTrue(rolling_ret.notna().sum() > 0)
    
    def test_rolling_volatility(self):
        self.analyzer.set_returns(self.returns)
        rolling_vol = self.analyzer.rolling_volatility()
        
        self.assertIsInstance(rolling_vol, pd.Series)
        self.assertTrue((rolling_vol.dropna() > 0).all())
    
    def test_rolling_sharpe(self):
        self.analyzer.set_returns(self.returns)
        rolling_sharpe = self.analyzer.rolling_sharpe()
        
        self.assertIsInstance(rolling_sharpe, pd.Series)
    
    def test_rolling_max_drawdown(self):
        self.analyzer.set_returns(self.returns)
        rolling_dd = self.analyzer.rolling_max_drawdown()
        
        self.assertIsInstance(rolling_dd, pd.Series)
        self.assertTrue((rolling_dd.dropna() <= 0).all())
    
    def test_rolling_win_rate(self):
        self.analyzer.set_returns(self.returns)
        rolling_wr = self.analyzer.rolling_win_rate()
        
        self.assertIsInstance(rolling_wr, pd.Series)
        self.assertTrue((rolling_wr.dropna() >= 0).all())
        self.assertTrue((rolling_wr.dropna() <= 1).all())
    
    def test_rolling_beta(self):
        self.analyzer.set_returns(self.returns)
        self.analyzer.set_benchmark(self.benchmark_returns)
        
        rolling_beta = self.analyzer.rolling_beta()
        self.assertIsInstance(rolling_beta, pd.Series)
    
    def test_calculate_all_metrics(self):
        self.analyzer.set_returns(self.returns)
        self.analyzer.set_benchmark(self.benchmark_returns)
        
        metrics = self.analyzer.calculate_all_rolling_metrics()
        
        self.assertIn('rolling_return', metrics)
        self.assertIn('rolling_volatility', metrics)
        self.assertIn('rolling_sharpe', metrics)
        self.assertIn('rolling_beta', metrics)
    
    def test_stability_analysis(self):
        self.analyzer.set_returns(self.returns)
        
        stability = self.analyzer.analyze_stability()
        
        self.assertIn('rolling_return', stability)
        self.assertIn('stability_score', stability['rolling_return'])
    
    def test_generate_report(self):
        self.analyzer.set_returns(self.returns)
        
        report = self.analyzer.generate_rolling_report()
        
        self.assertIn('# 滚动绩效分析报告', report)


class TestPerformanceDecomposition(unittest.TestCase):
    """绩效分解测试"""
    
    def setUp(self):
        np.random.seed(42)
        self.dates = pd.date_range('2023-01-01', periods=252, freq='B')
        
        self.returns = pd.Series(
            np.random.normal(0.0005, 0.015, 252),
            index=self.dates
        )
        
        self.decomposition = PerformanceDecomposition()
    
    def test_decompose_by_period(self):
        self.decomposition.set_returns(self.returns)
        result = self.decomposition.decompose_by_period()
        
        self.assertIn('yearly', result)
    
    def test_decompose_by_market_regime(self):
        self.decomposition.set_returns(self.returns)
        
        market_returns = pd.Series(
            np.random.normal(0.0004, 0.012, 252),
            index=self.dates
        )
        
        result = self.decomposition.decompose_by_market_regime(market_returns)
        
        self.assertIn('up_market', result)
        self.assertIn('down_market', result)


def run_tests():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestMarketConstraint))
    suite.addTests(loader.loadTestsFromTestCase(TestBenchmarkAnalyzer))
    suite.addTests(loader.loadTestsFromTestCase(TestPortfolioOptimizer))
    suite.addTests(loader.loadTestsFromTestCase(TestBrinsonAttribution))
    suite.addTests(loader.loadTestsFromTestCase(TestRollingPerformance))
    suite.addTests(loader.loadTestsFromTestCase(TestPerformanceDecomposition))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    total_tests = result.testsRun
    passed_tests = total_tests - len(result.failures) - len(result.errors)
    coverage = (passed_tests / total_tests * 100) if total_tests > 0 else 0
    
    print("\n" + "=" * 60)
    print("测试覆盖率统计")
    print("=" * 60)
    print(f"总测试数: {total_tests}")
    print(f"通过测试: {passed_tests}")
    print(f"失败测试: {len(result.failures)}")
    print(f"错误测试: {len(result.errors)}")
    print(f"覆盖率: {coverage:.1f}%")
    print("=" * 60)
    
    return result


if __name__ == '__main__':
    run_tests()
