#!/usr/bin/env python3
"""
前瞻性的风险预警与策略衰减监测系统
包括因子动量与波动监测、市场微观结构适应和对抗性样本测试
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import json
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class FactorMomentumMonitor:
    """因子动量与波动监测"""
    
    def __init__(self, lookback_period: int = 20):
        """
        初始化因子动量监测器
        
        Args:
            lookback_period: 回溯期数
        """
        self.lookback_period = lookback_period
        self.factor_history = {}
    
    def add_factor_data(self, factor_name: str, ic_values: pd.Series):
        """
        添加因子IC值历史数据
        
        Args:
            factor_name: 因子名称
            ic_values: IC值序列
        """
        if factor_name not in self.factor_history:
            self.factor_history[factor_name] = []
        
        self.factor_history[factor_name].extend(ic_values.tolist())
        
        # 保持历史数据长度
        if len(self.factor_history[factor_name]) > self.lookback_period * 2:
            self.factor_history[factor_name] = self.factor_history[factor_name][-self.lookback_period * 2:]
    
    def calculate_factor_momentum(self, factor_name: str) -> Dict[str, float]:
        """
        计算因子动量
        
        Args:
            factor_name: 因子名称
            
        Returns:
            因子动量指标
        """
        if factor_name not in self.factor_history or len(self.factor_history[factor_name]) < self.lookback_period:
            return {
                'momentum': 0,
                'momentum_change': 0,
                'decay_rate': 0,
                'half_life': np.inf
            }
        
        ic_history = self.factor_history[factor_name]
        recent_ic = ic_history[-self.lookback_period:]
        previous_ic = ic_history[-self.lookback_period*2:-self.lookback_period]
        
        if len(previous_ic) < self.lookback_period:
            return {
                'momentum': 0,
                'momentum_change': 0,
                'decay_rate': 0,
                'half_life': np.inf
            }
        
        # 计算动量
        recent_mean = np.mean(np.abs(recent_ic))
        previous_mean = np.mean(np.abs(previous_ic))
        momentum = recent_mean
        momentum_change = recent_mean - previous_mean
        
        # 计算衰减率
        decay_rate = (previous_mean - recent_mean) / previous_mean if previous_mean > 0 else 0
        half_life = np.log(2) / abs(decay_rate) if decay_rate != 0 else np.inf
        
        return {
            'momentum': momentum,
            'momentum_change': momentum_change,
            'decay_rate': decay_rate,
            'half_life': half_life
        }
    
    def calculate_factor_volatility(self, factor_name: str) -> Dict[str, float]:
        """
        计算因子波动
        
        Args:
            factor_name: 因子名称
            
        Returns:
            因子波动指标
        """
        if factor_name not in self.factor_history or len(self.factor_history[factor_name]) < self.lookback_period:
            return {
                'volatility': 0,
                'volatility_change': 0,
                'volatility_ratio': 1
            }
        
        ic_history = self.factor_history[factor_name]
        recent_ic = ic_history[-self.lookback_period:]
        previous_ic = ic_history[-self.lookback_period*2:-self.lookback_period]
        
        if len(previous_ic) < self.lookback_period:
            return {
                'volatility': 0,
                'volatility_change': 0,
                'volatility_ratio': 1
            }
        
        # 计算波动率
        recent_vol = np.std(recent_ic)
        previous_vol = np.std(previous_ic)
        volatility_change = recent_vol - previous_vol
        volatility_ratio = recent_vol / previous_vol if previous_vol > 0 else 1
        
        return {
            'volatility': recent_vol,
            'volatility_change': volatility_change,
            'volatility_ratio': volatility_ratio
        }
    
    def monitor_factor(self, factor_name: str) -> Dict[str, Any]:
        """
        监测因子状态
        
        Args:
            factor_name: 因子名称
            
        Returns:
            因子监测结果
        """
        momentum = self.calculate_factor_momentum(factor_name)
        volatility = self.calculate_factor_volatility(factor_name)
        
        # 综合评估
        if momentum['momentum'] < 0.02:
            status = 'weak'
        elif momentum['decay_rate'] > 0.3:
            status = 'decaying'
        elif volatility['volatility_ratio'] > 1.5:
            status = 'volatile'
        else:
            status = 'strong'
        
        return {
            'factor': factor_name,
            'status': status,
            'momentum': momentum,
            'volatility': volatility,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def monitor_all_factors(self) -> List[Dict[str, Any]]:
        """
        监测所有因子
        
        Returns:
            所有因子的监测结果
        """
        results = []
        for factor in self.factor_history.keys():
            result = self.monitor_factor(factor)
            results.append(result)
        return results


class MarketMicrostructureMonitor:
    """市场微观结构监测"""
    
    def __init__(self):
        """
        初始化市场微观结构监测器
        """
        self.microstructure_data = []
    
    def add_microstructure_data(self, data: Dict[str, float]):
        """
        添加市场微观结构数据
        
        Args:
            data: 微观结构数据，包含流动性、订单簿深度、算法交易占比等
        """
        data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.microstructure_data.append(data)
        
        # 保持数据长度
        if len(self.microstructure_data) > 100:
            self.microstructure_data = self.microstructure_data[-100:]
    
    def calculate_liquidity_change(self) -> Dict[str, float]:
        """
        计算流动性变化
        
        Returns:
            流动性变化指标
        """
        if len(self.microstructure_data) < 2:
            return {
                'liquidity_change': 0,
                'liquidity_trend': 'stable'
            }
        
        recent_liquidity = self.microstructure_data[-1].get('liquidity', 0)
        previous_liquidity = self.microstructure_data[-2].get('liquidity', 0)
        
        liquidity_change = recent_liquidity - previous_liquidity
        liquidity_ratio = recent_liquidity / previous_liquidity if previous_liquidity > 0 else 1
        
        if liquidity_ratio < 0.8:
            trend = 'deteriorating'
        elif liquidity_ratio > 1.2:
            trend = 'improving'
        else:
            trend = 'stable'
        
        return {
            'liquidity_change': liquidity_change,
            'liquidity_trend': trend,
            'liquidity_ratio': liquidity_ratio
        }
    
    def calculate_order_book_depth_change(self) -> Dict[str, float]:
        """
        计算订单簿深度变化
        
        Returns:
            订单簿深度变化指标
        """
        if len(self.microstructure_data) < 2:
            return {
                'depth_change': 0,
                'depth_trend': 'stable'
            }
        
        recent_depth = self.microstructure_data[-1].get('order_book_depth', 0)
        previous_depth = self.microstructure_data[-2].get('order_book_depth', 0)
        
        depth_change = recent_depth - previous_depth
        depth_ratio = recent_depth / previous_depth if previous_depth > 0 else 1
        
        if depth_ratio < 0.8:
            trend = 'decreasing'
        elif depth_ratio > 1.2:
            trend = 'increasing'
        else:
            trend = 'stable'
        
        return {
            'depth_change': depth_change,
            'depth_trend': trend,
            'depth_ratio': depth_ratio
        }
    
    def calculate_algorithm_trading_ratio(self) -> Dict[str, float]:
        """
        计算算法交易占比
        
        Returns:
            算法交易占比指标
        """
        if len(self.microstructure_data) < 1:
            return {
                'algorithm_trading_ratio': 0,
                'algorithm_trading_trend': 'stable'
            }
        
        recent_ratio = self.microstructure_data[-1].get('algorithm_trading_ratio', 0)
        
        if len(self.microstructure_data) >= 2:
            previous_ratio = self.microstructure_data[-2].get('algorithm_trading_ratio', 0)
            ratio_change = recent_ratio - previous_ratio
        else:
            ratio_change = 0
        
        if recent_ratio > 0.7:
            trend = 'high'
        elif recent_ratio > 0.5:
            trend = 'medium'
        else:
            trend = 'low'
        
        return {
            'algorithm_trading_ratio': recent_ratio,
            'algorithm_trading_change': ratio_change,
            'algorithm_trading_trend': trend
        }
    
    def monitor_market_structure(self) -> Dict[str, Any]:
        """
        监测市场微观结构
        
        Returns:
            市场微观结构监测结果
        """
        liquidity = self.calculate_liquidity_change()
        order_book = self.calculate_order_book_depth_change()
        algorithm = self.calculate_algorithm_trading_ratio()
        
        # 综合评估
        if liquidity['liquidity_trend'] == 'deteriorating' or order_book['depth_trend'] == 'decreasing':
            status = 'unfavorable'
        elif algorithm['algorithm_trading_trend'] == 'high':
            status = 'algorithmic'
        else:
            status = 'normal'
        
        return {
            'status': status,
            'liquidity': liquidity,
            'order_book': order_book,
            'algorithm_trading': algorithm,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


class AdversarialTestGenerator:
    """对抗性样本测试生成器"""
    
    def __init__(self):
        """
        初始化对抗性样本测试生成器
        """
        self.test_scenarios = {
            'interest_rate_spike': {
                'name': '利率急速飙升',
                'description': '模拟利率在短时间内大幅上升的场景',
                'parameters': {'rate_change': 0.5, 'time_frame': '1d'}
            },
            'liquidity_drought': {
                'name': '流动性枯竭',
                'description': '模拟市场流动性突然大幅下降的场景',
                'parameters': {'liquidity_drop': 0.8, 'duration': '3d'}
            },
            'market_crash': {
                'name': '市场 crash',
                'description': '模拟大盘在短时间内大幅下跌的场景',
                'parameters': {'market_drop': 0.15, 'time_frame': '1d'}
            },
            'sector_rotation': {
                'name': '行业轮动',
                'description': '模拟市场风格突然切换的场景',
                'parameters': {'rotation_speed': 'fast', 'sector_change': 'defensive'}
            },
            'volatility_spike': {
                'name': '波动率飙升',
                'description': '模拟市场波动率突然大幅上升的场景',
                'parameters': {'volatility_increase': 3, 'duration': '2d'}
            }
        }
    
    def generate_adversarial_scenario(self, scenario_name: str, market_data: pd.DataFrame) -> pd.DataFrame:
        """
        生成对抗性场景
        
        Args:
            scenario_name: 场景名称
            market_data: 原始市场数据
            
        Returns:
            生成的对抗性场景数据
        """
        if scenario_name not in self.test_scenarios:
            raise ValueError(f"未知的场景名称: {scenario_name}")
        
        scenario = self.test_scenarios[scenario_name]
        scenario_data = market_data.copy()
        
        # 根据场景类型生成对抗性数据
        if scenario_name == 'interest_rate_spike':
            # 利率上升导致股价下跌
            rate_change = scenario['parameters']['rate_change']
            scenario_data['close'] = scenario_data['close'] * (1 - rate_change * 0.5)
            scenario_data['volume'] = scenario_data['volume'] * 1.5  # 成交量放大
        
        elif scenario_name == 'liquidity_drought':
            # 流动性枯竭导致价格波动增大
            liquidity_drop = scenario['parameters']['liquidity_drop']
            scenario_data['close'] = scenario_data['close'] * (1 + np.random.normal(0, 0.02, len(scenario_data)))
            scenario_data['volume'] = scenario_data['volume'] * (1 - liquidity_drop)
        
        elif scenario_name == 'market_crash':
            # 市场 crash
            market_drop = scenario['parameters']['market_drop']
            scenario_data['close'] = scenario_data['close'] * (1 - market_drop)
            scenario_data['volume'] = scenario_data['volume'] * 2  # 成交量大幅放大
        
        elif scenario_name == 'sector_rotation':
            # 行业轮动
            scenario_data['close'] = scenario_data['close'] * (1 + np.random.normal(0, 0.01, len(scenario_data)))
        
        elif scenario_name == 'volatility_spike':
            # 波动率飙升
            volatility_increase = scenario['parameters']['volatility_increase']
            scenario_data['close'] = scenario_data['close'] * (1 + np.random.normal(0, 0.03 * volatility_increase, len(scenario_data)))
            scenario_data['volume'] = scenario_data['volume'] * 1.3
        
        return scenario_data
    
    def run_adversarial_test(self, strategy, market_data: pd.DataFrame) -> Dict[str, Any]:
        """
        运行对抗性测试
        
        Args:
            strategy: 策略对象
            market_data: 市场数据
            
        Returns:
            测试结果
        """
        results = {}
        
        for scenario_name, scenario in self.test_scenarios.items():
            # 生成对抗性场景
            scenario_data = self.generate_adversarial_scenario(scenario_name, market_data)
            
            # 测试策略在对抗性场景下的表现
            # 这里需要根据具体策略实现测试逻辑
            # 简化版：计算策略在场景下的收益率
            returns = scenario_data['close'].pct_change().dropna()
            sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0
            max_drawdown = (np.maximum.accumulate(scenario_data['close']) - scenario_data['close']).max() / scenario_data['close'].max()
            
            results[scenario_name] = {
                'name': scenario['name'],
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'average_return': np.mean(returns),
                'volatility': np.std(returns) * np.sqrt(252),
                'survived': sharpe_ratio > -2  # 简单判断是否存活
            }
        
        # 综合评估
        survived_scenarios = sum(1 for r in results.values() if r['survived'])
        overall_score = survived_scenarios / len(results)
        
        if overall_score >= 0.8:
            robustness = 'strong'
        elif overall_score >= 0.5:
            robustness = 'medium'
        else:
            robustness = 'weak'
        
        return {
            'scenario_results': results,
            'overall_score': overall_score,
            'robustness': robustness,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


class RiskEarlyWarningSystem:
    """风险预警系统"""
    
    def __init__(self, output_dir: str = 'data/risk_monitoring'):
        """
        初始化风险预警系统
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.factor_monitor = FactorMomentumMonitor()
        self.market_monitor = MarketMicrostructureMonitor()
        self.adversarial_tester = AdversarialTestGenerator()
        
        self.alert_history = []
    
    def add_factor_data(self, factor_name: str, ic_values: pd.Series):
        """
        添加因子数据
        
        Args:
            factor_name: 因子名称
            ic_values: IC值序列
        """
        self.factor_monitor.add_factor_data(factor_name, ic_values)
    
    def add_microstructure_data(self, data: Dict[str, float]):
        """
        添加市场微观结构数据
        
        Args:
            data: 微观结构数据
        """
        self.market_monitor.add_microstructure_data(data)
    
    def generate_alert(self, alert_type: str, severity: str, message: str, details: Dict[str, Any]):
        """
        生成预警
        
        Args:
            alert_type: 预警类型
            severity: 严重程度
            message: 预警消息
            details: 详细信息
        """
        alert = {
            'alert_type': alert_type,
            'severity': severity,
            'message': message,
            'details': details,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        self.alert_history.append(alert)
        
        # 保存预警
        alert_path = os.path.join(self.output_dir, f'alert_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(alert_path, 'w', encoding='utf-8') as f:
            json.dump(alert, f, ensure_ascii=False, indent=2)
        
        print(f"预警生成: {alert_type} - {severity} - {message}")
    
    def check_factor_risk(self):
        """
        检查因子风险
        """
        factor_results = self.factor_monitor.monitor_all_factors()
        
        for result in factor_results:
            if result['status'] == 'weak':
                self.generate_alert(
                    'factor_weak',
                    'warning',
                    f"因子 {result['factor']} 表现疲软",
                    result
                )
            elif result['status'] == 'decaying':
                self.generate_alert(
                    'factor_decaying',
                    'danger',
                    f"因子 {result['factor']} 正在衰减",
                    result
                )
            elif result['status'] == 'volatile':
                self.generate_alert(
                    'factor_volatile',
                    'warning',
                    f"因子 {result['factor']} 波动异常",
                    result
                )
    
    def check_market_risk(self):
        """
        检查市场风险
        """
        market_result = self.market_monitor.monitor_market_structure()
        
        if market_result['status'] == 'unfavorable':
            self.generate_alert(
                'market_unfavorable',
                'danger',
                "市场微观结构不利",
                market_result
            )
        elif market_result['status'] == 'algorithmic':
            self.generate_alert(
                'market_algorithmic',
                'warning',
                "算法交易占比高",
                market_result
            )
    
    def run_adversarial_tests(self, strategy, market_data: pd.DataFrame):
        """
        运行对抗性测试
        
        Args:
            strategy: 策略对象
            market_data: 市场数据
            
        Returns:
            测试结果
        """
        test_result = self.adversarial_tester.run_adversarial_test(strategy, market_data)
        
        if test_result['robustness'] == 'weak':
            self.generate_alert(
                'strategy_weak',
                'danger',
                "策略在对抗性场景下表现脆弱",
                test_result
            )
        elif test_result['robustness'] == 'medium':
            self.generate_alert(
                'strategy_medium',
                'warning',
                "策略在对抗性场景下表现一般",
                test_result
            )
        
        # 保存测试结果
        test_path = os.path.join(self.output_dir, f'adversarial_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(test_path, 'w', encoding='utf-8') as f:
            json.dump(test_result, f, ensure_ascii=False, indent=2)
        
        return test_result
    
    def generate_risk_report(self) -> Dict[str, Any]:
        """
        生成风险报告
        
        Returns:
            风险报告
        """
        # 检查因子风险
        self.check_factor_risk()
        
        # 检查市场风险
        self.check_market_risk()
        
        # 生成报告
        report = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'factor_monitoring': self.factor_monitor.monitor_all_factors(),
            'market_monitoring': self.market_monitor.monitor_market_structure(),
            'recent_alerts': self.alert_history[-10:],  # 最近10条预警
            'alert_count': len(self.alert_history)
        }
        
        # 保存报告
        report_path = os.path.join(self.output_dir, f'risk_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"风险报告已生成: {report_path}")
        return report


# 测试代码
if __name__ == "__main__":
    # 创建测试数据
    np.random.seed(42)
    n_days = 100
    dates = pd.date_range(start='2020-01-01', periods=n_days, freq='B')
    
    # 创建市场数据
    market_data = pd.DataFrame({
        'close': np.random.normal(3000, 100, n_days),
        'volume': np.random.normal(10000000000, 2000000000, n_days)
    }, index=dates)
    
    # 创建因子IC数据
    factor_ic = pd.Series(np.random.normal(0.05, 0.02, n_days))
    
    # 初始化风险预警系统
    rws = RiskEarlyWarningSystem()
    
    # 添加因子数据
    rws.add_factor_data('动量因子', factor_ic)
    
    # 添加市场微观结构数据
    rws.add_microstructure_data({
        'liquidity': 10000000000,
        'order_book_depth': 5000000,
        'algorithm_trading_ratio': 0.6
    })
    
    # 模拟因子衰减
    decaying_ic = pd.Series(np.linspace(0.05, 0.01, 50))
    rws.add_factor_data('动量因子', decaying_ic)
    
    # 生成风险报告
    print("生成风险报告...")
    report = rws.generate_risk_report()
    
    # 运行对抗性测试
    print("\n运行对抗性测试...")
    # 简单策略类
    class SimpleStrategy:
        def __init__(self):
            pass
    
    strategy = SimpleStrategy()
    test_result = rws.run_adversarial_tests(strategy, market_data)
    
    # 打印结果
    print("\n对抗性测试结果:")
    print(f"整体评分: {test_result['overall_score']:.2f}")
    print(f"策略稳健性: {test_result['robustness']}")
    
    print("\n场景测试结果:")
    for scenario, result in test_result['scenario_results'].items():
        print(f"{result['name']}: 夏普比率={result['sharpe_ratio']:.2f}, 最大回撤={result['max_drawdown']:.2f}, 存活={result['survived']}")
