#!/usr/bin/env python3
"""
阿尔法工厂自动化流水线
实现因子/策略的自动生成、检验和上线
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import random
import re
import os
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class AlphaGenerator:
    """因子生成器"""
    
    def __init__(self, max_depth: int = 3, n_generations: int = 10, population_size: int = 50):
        """
        初始化因子生成器
        
        Args:
            max_depth: 因子表达式的最大深度
            n_generations: 遗传算法的代数
            population_size: 种群大小
        """
        self.max_depth = max_depth
        self.n_generations = n_generations
        self.population_size = population_size
        
        # 基础数据单元
        self.primitives = {
            'price': ['close', 'open', 'high', 'low'],
            'volume': ['volume'],
            'fundamental': ['pe', 'pb', 'roe', 'roa', 'revenue_growth', 'profit_growth'],
            'technical': ['ma5', 'ma10', 'ma20', 'ma60', 'rsi', 'macd', 'bollinger_upper', 'bollinger_lower']
        }
        
        # 运算符
        self.operators = {
            'unary': ['abs', 'log', 'sqrt', 'sign', 'rank'],
            'binary': ['+', '-', '*', '/', 'max', 'min', 'corr', 'cov']
        }
    
    def generate_random_factor(self, depth: int = 0) -> str:
        """
        生成随机因子表达式
        
        Args:
            depth: 当前深度
            
        Returns:
            因子表达式字符串
        """
        if depth >= self.max_depth:
            # 叶子节点，选择基础数据单元
            category = random.choice(list(self.primitives.keys()))
            return random.choice(self.primitives[category])
        
        # 非叶子节点，选择运算符
        if random.random() < 0.3:
            # 一元运算符
            op = random.choice(self.operators['unary'])
            operand = self.generate_random_factor(depth + 1)
            return f"{op}({operand})"
        else:
            # 二元运算符
            op = random.choice(self.operators['binary'])
            left = self.generate_random_factor(depth + 1)
            right = self.generate_random_factor(depth + 1)
            return f"{op}({left}, {right})"
    
    def generate_initial_population(self) -> List[str]:
        """
        生成初始种群
        
        Returns:
            因子表达式列表
        """
        population = []
        for _ in range(self.population_size):
            factor = self.generate_random_factor()
            population.append(factor)
        return population
    
    def evaluate_factor(self, factor: str, data: pd.DataFrame) -> float:
        """
        评估因子的质量
        
        Args:
            factor: 因子表达式
            data: 股票数据
            
        Returns:
            因子质量得分
        """
        try:
            # 尝试计算因子值
            # 这里需要实现因子计算逻辑
            # 简化版：返回随机得分
            return random.uniform(0, 1)
        except:
            return 0
    
    def evolve_factors(self, data: pd.DataFrame) -> List[str]:
        """
        进化因子
        
        Args:
            data: 股票数据
            
        Returns:
            进化后的因子列表
        """
        population = self.generate_initial_population()
        
        for generation in range(self.n_generations):
            # 评估种群
            scores = []
            for factor in population:
                score = self.evaluate_factor(factor, data)
                scores.append((factor, score))
            
            # 选择优秀个体
            scores.sort(key=lambda x: x[1], reverse=True)
            top_performers = [x[0] for x in scores[:self.population_size//2]]
            
            # 交叉和变异
            new_population = top_performers.copy()
            
            while len(new_population) < self.population_size:
                # 交叉
                parent1 = random.choice(top_performers)
                parent2 = random.choice(top_performers)
                child = self.crossover(parent1, parent2)
                
                # 变异
                if random.random() < 0.1:
                    child = self.mutate(child)
                
                new_population.append(child)
            
            population = new_population
        
        return population
    
    def crossover(self, parent1: str, parent2: str) -> str:
        """
        交叉操作
        
        Args:
            parent1: 父代因子1
            parent2: 父代因子2
            
        Returns:
            子代因子
        """
        # 简化版交叉
        if random.random() < 0.5:
            return parent1
        else:
            return parent2
    
    def mutate(self, factor: str) -> str:
        """
        变异操作
        
        Args:
            factor: 原始因子
            
        Returns:
            变异后的因子
        """
        # 简化版变异
        if random.random() < 0.5:
            return self.generate_random_factor()
        else:
            return factor


class FactorTester:
    """因子检验器"""
    
    def __init__(self, test_periods: List[Tuple[str, str]] = None):
        """
        初始化因子检验器
        
        Args:
            test_periods: 测试周期列表
        """
        self.test_periods = test_periods or [
            ('2020-01-01', '2022-12-31'),  # 训练期
            ('2023-01-01', '2023-12-31')   # 测试期
        ]
    
    def calculate_ic(self, factor_values: pd.Series, returns: pd.Series) -> float:
        """
        计算因子IC值
        
        Args:
            factor_values: 因子值
            returns: 收益率
            
        Returns:
            IC值
        """
        # 计算Spearman相关系数
        ic = factor_values.corr(returns, method='spearman')
        return ic if not pd.isna(ic) else 0
    
    def calculate_ic_ir(self, factor_values: pd.DataFrame, returns: pd.DataFrame) -> Tuple[float, float]:
        """
        计算因子IC和IR值
        
        Args:
            factor_values: 因子值DataFrame
            returns: 收益率DataFrame
            
        Returns:
            (IC均值, IR值)
        """
        ic_values = []
        
        for date in factor_values.index:
            if date in returns.index:
                factor_day = factor_values.loc[date]
                returns_day = returns.loc[date]
                
                ic = self.calculate_ic(factor_day, returns_day)
                ic_values.append(ic)
        
        if len(ic_values) < 10:
            return 0, 0
        
        ic_mean = np.mean(ic_values)
        ic_std = np.std(ic_values)
        ir = ic_mean / ic_std if ic_std > 0 else 0
        
        return ic_mean, ir
    
    def backtest_factor(self, factor_values: pd.DataFrame, prices: pd.DataFrame) -> Dict[str, float]:
        """
        回测因子
        
        Args:
            factor_values: 因子值DataFrame
            prices: 价格DataFrame
            
        Returns:
            回测结果
        """
        # 计算收益率
        returns = prices.pct_change().shift(-1)
        
        # 分层回测
        n_quantiles = 5
        results = {}
        
        for date in factor_values.index:
            if date in returns.index:
                factor_day = factor_values.loc[date]
                returns_day = returns.loc[date]
                
                # 分层
                quantiles = pd.qcut(factor_day, n_quantiles, labels=False, duplicates='drop')
                
                # 计算各层收益率
                for i in range(n_quantiles):
                    mask = quantiles == i
                    if mask.sum() > 0:
                        layer_return = returns_day[mask].mean()
                        if f'layer_{i}' not in results:
                            results[f'layer_{i}'] = []
                        results[f'layer_{i}'].append(layer_return)
        
        # 计算多空收益
        if 'layer_4' in results and 'layer_0' in results:
            long_short_returns = np.array(results['layer_4']) - np.array(results['layer_0'])
            sharpe_ratio = np.mean(long_short_returns) / np.std(long_short_returns) * np.sqrt(252)
        else:
            sharpe_ratio = 0
        
        return {
            'sharpe_ratio': sharpe_ratio,
            'long_short_return': np.mean(long_short_returns) if 'long_short_returns' in locals() else 0,
            'layer_returns': {k: np.mean(v) for k, v in results.items()}
        }
    
    def test_factor(self, factor: str, data: pd.DataFrame) -> Dict[str, Any]:
        """
        测试因子
        
        Args:
            factor: 因子表达式
            data: 股票数据
            
        Returns:
            测试结果
        """
        # 简化版测试
        # 实际实现需要计算因子值并进行回测
        
        # 模拟测试结果
        ic_mean = random.uniform(-0.1, 0.1)
        ir = random.uniform(-2, 2)
        sharpe_ratio = random.uniform(-1, 3)
        
        return {
            'factor': factor,
            'ic_mean': ic_mean,
            'ir': ir,
            'sharpe_ratio': sharpe_ratio,
            'is_valid': abs(ic_mean) > 0.03 and abs(ir) > 0.5 and sharpe_ratio > 0.5
        }


class FactorPool:
    """因子池管理"""
    
    def __init__(self, pool_dir: str = 'data/factor_pool'):
        """
        初始化因子池
        
        Args:
            pool_dir: 因子池目录
        """
        self.pool_dir = pool_dir
        os.makedirs(self.pool_dir, exist_ok=True)
        
        self.candidate_pool = []
        self.active_pool = []
        self.archive_pool = []
    
    def add_to_candidate(self, factor_test: Dict[str, Any]):
        """
        添加因子到候选池
        
        Args:
            factor_test: 因子测试结果
        """
        if factor_test['is_valid']:
            self.candidate_pool.append(factor_test)
            print(f"因子已添加到候选池: {factor_test['factor']}")
    
    def promote_to_active(self, factor_id: int):
        """
        将因子从候选池提升到活跃池
        
        Args:
            factor_id: 因子在候选池中的索引
        """
        if 0 <= factor_id < len(self.candidate_pool):
            factor = self.candidate_pool.pop(factor_id)
            factor['promoted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.active_pool.append(factor)
            print(f"因子已提升到活跃池: {factor['factor']}")
    
    def archive_factor(self, factor_id: int):
        """
        将因子从活跃池归档
        
        Args:
            factor_id: 因子在活跃池中的索引
        """
        if 0 <= factor_id < len(self.active_pool):
            factor = self.active_pool.pop(factor_id)
            factor['archived_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.archive_pool.append(factor)
            print(f"因子已归档: {factor['factor']}")
    
    def get_active_factors(self) -> List[Dict[str, Any]]:
        """
        获取活跃因子
        
        Returns:
            活跃因子列表
        """
        return self.active_pool
    
    def save_pool(self):
        """
        保存因子池
        """
        pool_data = {
            'candidate_pool': self.candidate_pool,
            'active_pool': self.active_pool,
            'archive_pool': self.archive_pool,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        pool_path = os.path.join(self.pool_dir, f'factor_pool_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(pool_path, 'w', encoding='utf-8') as f:
            json.dump(pool_data, f, ensure_ascii=False, indent=2)
        
        print(f"因子池已保存: {pool_path}")
    
    def load_pool(self, pool_path: str):
        """
        加载因子池
        
        Args:
            pool_path: 因子池文件路径
        """
        if os.path.exists(pool_path):
            with open(pool_path, 'r', encoding='utf-8') as f:
                pool_data = json.load(f)
            
            self.candidate_pool = pool_data.get('candidate_pool', [])
            self.active_pool = pool_data.get('active_pool', [])
            self.archive_pool = pool_data.get('archive_pool', [])
            
            print(f"因子池已加载: {pool_path}")
        else:
            print(f"因子池文件不存在: {pool_path}")


class AlphaFactory:
    """阿尔法工厂"""
    
    def __init__(self, output_dir: str = 'data/alpha_factory'):
        """
        初始化阿尔法工厂
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.generator = AlphaGenerator()
        self.tester = FactorTester()
        self.factor_pool = FactorPool()
    
    def run_pipeline(self, data: pd.DataFrame, n_factors: int = 100):
        """
        运行阿尔法工厂流水线
        
        Args:
            data: 股票数据
            n_factors: 生成因子数量
            
        Returns:
            流水线运行结果
        """
        results = {
            'generated_factors': 0,
            'valid_factors': 0,
            'promoted_factors': 0,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 1. 生成因子
        print("生成因子...")
        factors = self.generator.evolve_factors(data)
        factors = factors[:n_factors]  # 限制数量
        results['generated_factors'] = len(factors)
        
        # 2. 测试因子
        print("测试因子...")
        for factor in factors:
            test_result = self.tester.test_factor(factor, data)
            if test_result['is_valid']:
                self.factor_pool.add_to_candidate(test_result)
                results['valid_factors'] += 1
        
        # 3. 自动提升因子
        print("提升因子...")
        # 按IC值排序，选择前5个
        self.factor_pool.candidate_pool.sort(key=lambda x: abs(x['ic_mean']), reverse=True)
        for i in range(min(5, len(self.factor_pool.candidate_pool))):
            self.factor_pool.promote_to_active(0)  # 每次提升第一个
            results['promoted_factors'] += 1
        
        # 4. 保存结果
        self.factor_pool.save_pool()
        
        # 保存运行结果
        result_path = os.path.join(self.output_dir, f'pipeline_result_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"流水线运行完成，结果已保存: {result_path}")
        return results
    
    def get_factor_stats(self) -> Dict[str, Any]:
        """
        获取因子统计信息
        
        Returns:
            因子统计信息
        """
        stats = {
            'candidate_count': len(self.factor_pool.candidate_pool),
            'active_count': len(self.factor_pool.active_pool),
            'archive_count': len(self.factor_pool.archive_pool),
            'active_factors': []
        }
        
        for factor in self.factor_pool.active_pool:
            stats['active_factors'].append({
                'factor': factor['factor'],
                'ic_mean': factor['ic_mean'],
                'ir': factor['ir'],
                'sharpe_ratio': factor['sharpe_ratio'],
                'promoted_at': factor.get('promoted_at', 'N/A')
            })
        
        return stats
    
    def optimize_factor_weights(self) -> Dict[str, float]:
        """
        优化因子权重
        
        Returns:
            因子权重字典
        """
        active_factors = self.factor_pool.get_active_factors()
        
        if not active_factors:
            return {}
        
        # 基于IC值计算权重
        weights = {}
        total_ic = sum(abs(f['ic_mean']) for f in active_factors)
        
        for factor in active_factors:
            weights[factor['factor']] = abs(factor['ic_mean']) / total_ic if total_ic > 0 else 1/len(active_factors)
        
        return weights


# 测试代码
if __name__ == "__main__":
    # 创建测试数据
    np.random.seed(42)
    n_stocks = 100
    n_days = 252
    
    dates = pd.date_range(start='2020-01-01', periods=n_days, freq='B')
    stocks = [f'{i:06d}' for i in range(1, n_stocks+1)]
    
    # 创建价格数据
    prices = pd.DataFrame(
        np.random.normal(100, 10, (n_days, n_stocks)),
        index=dates,
        columns=stocks
    )
    
    # 创建因子数据
    factor_data = pd.DataFrame(
        np.random.normal(0, 1, (n_days, n_stocks)),
        index=dates,
        columns=stocks
    )
    
    # 初始化阿尔法工厂
    factory = AlphaFactory()
    
    # 运行流水线
    print("运行阿尔法工厂流水线...")
    result = factory.run_pipeline(prices, n_factors=50)
    
    # 打印结果
    print("\n流水线运行结果:")
    print(f"生成因子数量: {result['generated_factors']}")
    print(f"有效因子数量: {result['valid_factors']}")
    print(f"提升因子数量: {result['promoted_factors']}")
    
    # 获取因子统计
    stats = factory.get_factor_stats()
    print("\n因子池统计:")
    print(f"候选因子: {stats['candidate_count']}")
    print(f"活跃因子: {stats['active_count']}")
    print(f"归档因子: {stats['archive_count']}")
    
    # 打印活跃因子
    print("\n活跃因子:")
    for factor in stats['active_factors']:
        print(f"因子: {factor['factor']}")
        print(f"  IC: {factor['ic_mean']:.4f}")
        print(f"  IR: {factor['ir']:.4f}")
        print(f"  夏普比率: {factor['sharpe_ratio']:.4f}")
        print()
    
    # 优化因子权重
    weights = factory.optimize_factor_weights()
    print("\n优化后的因子权重:")
    for factor, weight in weights.items():
        print(f"{factor}: {weight:.4f}")
