#!/usr/bin/env python3
"""
强化学习驱动的策略优化与组合管理
包括动态调仓和策略权重分配
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from collections import deque
import random
import os
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class PortfolioEnv:
    """投资组合环境"""
    
    def __init__(self, market_data: pd.DataFrame, strategy_returns: pd.DataFrame, 
                 n_strategies: int = 3, initial_cash: float = 1000000):
        """
        初始化投资组合环境
        
        Args:
            market_data: 市场数据
            strategy_returns: 各策略收益率
            n_strategies: 策略数量
            initial_cash: 初始资金
        """
        self.market_data = market_data
        self.strategy_returns = strategy_returns
        self.n_strategies = n_strategies
        self.initial_cash = initial_cash
        
        # 状态空间维度
        market_features = market_data.shape[1]
        self.observation_shape = (market_features + n_strategies + n_strategies,)
        
        self.current_step = 0
        self.portfolio_value = initial_cash
        self.weights = np.ones(n_strategies) / n_strategies
        self.history = []
    
    def reset(self):
        """
        重置环境
        """
        self.current_step = 0
        self.portfolio_value = self.initial_cash
        self.weights = np.ones(self.n_strategies) / self.n_strategies
        self.history = []
        
        return self._get_observation()
    
    def _get_observation(self):
        """
        获取当前状态
        """
        if self.current_step >= len(self.market_data):
            return np.zeros(self.observation_shape)
        
        market_state = self.market_data.iloc[self.current_step].values
        strategy_perf = self.strategy_returns.iloc[self.current_step].values
        
        observation = np.concatenate([market_state, strategy_perf, self.weights])
        return observation.astype(np.float32)
    
    def step(self, action):
        """
        执行动作
        
        Args:
            action: 策略权重
            
        Returns:
            observation, reward, terminated, info
        """
        # 归一化权重
        weights = action / np.sum(action) if np.sum(action) > 0 else np.ones(self.n_strategies) / self.n_strategies
        
        # 计算组合收益率
        if self.current_step < len(self.strategy_returns):
            strategy_return = self.strategy_returns.iloc[self.current_step].values
            portfolio_return = np.dot(weights, strategy_return)
        else:
            portfolio_return = 0
        
        # 更新组合价值
        self.portfolio_value *= (1 + portfolio_return)
        
        # 计算奖励（考虑风险调整后收益）
        # 简化版：使用夏普比率的近似
        if len(self.history) >= 20:
            returns = [h['return'] for h in self.history[-20:]]
            mean_return = np.mean(returns)
            std_return = np.std(returns) if len(returns) > 1 else 1
            sharpe_ratio = mean_return / std_return * np.sqrt(252)
            reward = sharpe_ratio
        else:
            reward = portfolio_return
        
        # 记录历史
        self.history.append({
            'step': self.current_step,
            'weights': weights.tolist(),
            'return': portfolio_return,
            'portfolio_value': self.portfolio_value
        })
        
        # 更新状态
        self.current_step += 1
        self.weights = weights
        
        # 检查是否终止
        terminated = self.current_step >= len(self.market_data) - 1
        
        return self._get_observation(), reward, terminated, {
            'portfolio_value': self.portfolio_value,
            'weights': weights,
            'return': portfolio_return
        }
    
    def render(self):
        """
        渲染环境
        """
        print(f"Step: {self.current_step}")
        print(f"Portfolio Value: {self.portfolio_value:.2f}")
        print(f"Weights: {self.weights}")
        print()


class TradingEnv:
    """交易执行环境"""
    
    def __init__(self, price_data: pd.DataFrame, max_order_size: int = 10000):
        """
        初始化交易执行环境
        
        Args:
            price_data: 价格数据
            max_order_size: 最大订单规模
        """
        self.price_data = price_data
        self.max_order_size = max_order_size
        
        # 状态空间维度
        self.observation_shape = (3,)
        
        self.current_step = 0
        self.remaining_order = max_order_size
        self.executed_order = 0
        self.total_slippage = 0
        self.history = []
    
    def reset(self):
        """
        重置环境
        """
        self.current_step = 0
        self.remaining_order = self.max_order_size
        self.executed_order = 0
        self.total_slippage = 0
        self.history = []
        
        return self._get_observation()
    
    def _get_observation(self):
        """
        获取当前状态
        """
        if self.current_step >= len(self.price_data):
            return np.zeros(self.observation_shape)
        
        price = self.price_data.iloc[self.current_step]['close']
        volume = self.price_data.iloc[self.current_step].get('volume', 10000000000)
        # 模拟订单簿深度
        depth = volume / 1000
        
        return np.array([price, volume, depth], dtype=np.float32)
    
    def step(self, action):
        """
        执行动作
        
        Args:
            action: [拆单比例, 执行时间比例]
            
        Returns:
            observation, reward, terminated, info
        """
        split_ratio, time_ratio = action
        
        # 计算本次执行的订单量
        execute_size = min(self.remaining_order * split_ratio, self.remaining_order)
        
        # 计算滑点（简化模型）
        if self.current_step < len(self.price_data):
            price = self.price_data.iloc[self.current_step]['close']
            volume = self.price_data.iloc[self.current_step].get('volume', 10000000000)
            
            # 滑点与订单规模成正比，与市场深度成反比
            slippage = (execute_size / volume) * 0.01
            self.total_slippage += slippage
        else:
            slippage = 0
        
        # 更新状态
        self.remaining_order -= execute_size
        self.executed_order += execute_size
        self.current_step += int(len(self.price_data) * time_ratio) + 1
        
        # 计算奖励（最小化滑点）
        reward = -slippage
        
        # 记录历史
        self.history.append({
            'step': self.current_step,
            'execute_size': execute_size,
            'slippage': slippage,
            'remaining_order': self.remaining_order
        })
        
        # 检查是否终止
        terminated = self.remaining_order <= 0 or self.current_step >= len(self.price_data) - 1
        
        return self._get_observation(), reward, terminated, {
            'executed_order': self.executed_order,
            'remaining_order': self.remaining_order,
            'total_slippage': self.total_slippage
        }


class ReplayBuffer:
    """经验回放缓冲区"""
    
    def __init__(self, capacity: int = 10000):
        """
        初始化回放缓冲区
        
        Args:
            capacity: 缓冲区容量
        """
        self.buffer = deque(maxlen=capacity)
    
    def push(self, state, action, reward, next_state, terminated):
        """
        存入经验
        """
        self.buffer.append((state, action, reward, next_state, terminated))
    
    def sample(self, batch_size: int):
        """
        采样经验
        """
        batch = random.sample(self.buffer, min(batch_size, len(self.buffer)))
        states, actions, rewards, next_states, terminateds = zip(*batch)
        return (
            np.array(states),
            np.array(actions),
            np.array(rewards, dtype=np.float32),
            np.array(next_states),
            np.array(terminateds, dtype=np.bool_)
        )
    
    def __len__(self):
        """
        获取缓冲区长度
        """
        return len(self.buffer)


class SimpleAgent:
    """简化版智能体"""
    
    def __init__(self, state_dim: int, action_dim: int):
        """
        初始化简化版智能体
        
        Args:
            state_dim: 状态维度
            action_dim: 动作维度
        """
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.epsilon = 0.1  # 固定探索率
        self.replay_buffer = ReplayBuffer()
    
    def select_action(self, state):
        """
        选择动作
        """
        if random.random() < self.epsilon:
            # 探索：随机动作
            if self.action_dim == 1:
                return np.random.uniform(0, 1, (self.action_dim,))
            else:
                return np.random.dirichlet(np.ones(self.action_dim))
        else:
            # 利用：基于简单策略的动作
            # 对于投资组合，返回均匀分布的权重
            # 对于交易执行，返回固定的拆单比例
            if self.action_dim == 2:
                # 交易执行：固定拆单比例和执行时间
                return np.array([0.25, 0.25])
            else:
                # 投资组合：均匀分布
                return np.ones(self.action_dim) / self.action_dim
    
    def train(self):
        """
        训练智能体（简化版，实际不做任何训练）
        """
        pass
    
    def update_target_network(self):
        """
        更新目标网络（简化版，实际不做任何操作）
        """
        pass


class RLPortfolioManager:
    """强化学习投资组合管理器"""
    
    def __init__(self, market_data: pd.DataFrame, strategy_returns: pd.DataFrame):
        """
        初始化强化学习投资组合管理器
        
        Args:
            market_data: 市场数据
            strategy_returns: 策略收益率数据
        """
        self.market_data = market_data
        self.strategy_returns = strategy_returns
        self.n_strategies = strategy_returns.shape[1]
        
        # 创建环境
        self.env = PortfolioEnv(market_data, strategy_returns, self.n_strategies)
        
        # 创建智能体
        state_dim = self.env.observation_shape[0]
        action_dim = self.n_strategies
        self.agent = SimpleAgent(state_dim, action_dim)
    
    def train(self, episodes: int = 1000, update_target_every: int = 100):
        """
        训练智能体
        
        Args:
            episodes: 训练轮数
            update_target_every: 每多少轮更新目标网络
        """
        rewards = []
        portfolio_values = []
        
        for episode in range(episodes):
            state = self.env.reset()
            episode_reward = 0
            
            while True:
                # 选择动作
                action = self.agent.select_action(state)
                
                # 执行动作
                next_state, reward, terminated, info = self.env.step(action)
                
                # 存储经验
                self.agent.replay_buffer.push(state, action, reward, next_state, terminated)
                
                # 训练
                self.agent.train()
                
                state = next_state
                episode_reward += reward
                
                if terminated:
                    break
            
            # 更新目标网络
            if episode % update_target_every == 0:
                self.agent.update_target_network()
            
            rewards.append(episode_reward)
            portfolio_values.append(self.env.portfolio_value)
            
            if episode % 100 == 0:
                print(f"Episode {episode}, Reward: {episode_reward:.2f}, Portfolio Value: {self.env.portfolio_value:.2f}")
        
        return rewards, portfolio_values
    
    def get_optimal_weights(self, state):
        """
        获取最优策略权重
        
        Args:
            state: 当前状态
            
        Returns:
            最优权重
        """
        # 简化版：返回均匀分布的权重
        return np.ones(self.n_strategies) / self.n_strategies
    
    def run_backtest(self):
        """
        回测智能体表现
        
        Returns:
            回测结果
        """
        state = self.env.reset()
        portfolio_values = [self.env.initial_cash]
        weights_history = []
        
        while True:
            # 选择最优动作（无探索）
            weights = self.get_optimal_weights(state)
            weights_history.append(weights)
            
            # 执行动作
            next_state, reward, terminated, info = self.env.step(weights)
            
            portfolio_values.append(self.env.portfolio_value)
            state = next_state
            
            if terminated:
                break
        
        # 计算绩效指标
        returns = np.array(portfolio_values[1:]) / np.array(portfolio_values[:-1]) - 1
        sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252)
        max_drawdown = (np.maximum.accumulate(portfolio_values) - portfolio_values).max() / np.max(portfolio_values)
        
        return {
            'portfolio_values': portfolio_values,
            'weights_history': weights_history,
            'returns': returns.tolist(),
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'final_value': portfolio_values[-1]
        }


class RLTradingManager:
    """强化学习交易管理器"""
    
    def __init__(self, price_data: pd.DataFrame):
        """
        初始化强化学习交易管理器
        
        Args:
            price_data: 价格数据
        """
        self.price_data = price_data
        
        # 创建环境
        self.env = TradingEnv(price_data)
        
        # 创建智能体
        state_dim = self.env.observation_shape[0]
        action_dim = 2
        self.agent = SimpleAgent(state_dim, action_dim)
    
    def train(self, episodes: int = 1000, update_target_every: int = 100):
        """
        训练智能体
        
        Args:
            episodes: 训练轮数
            update_target_every: 每多少轮更新目标网络
        """
        rewards = []
        slippages = []
        
        for episode in range(episodes):
            state = self.env.reset()
            episode_reward = 0
            
            while True:
                # 选择动作
                action = self.agent.select_action(state)
                
                # 执行动作
                next_state, reward, terminated, info = self.env.step(action)
                
                # 存储经验
                self.agent.replay_buffer.push(state, action, reward, next_state, terminated)
                
                # 训练
                self.agent.train()
                
                state = next_state
                episode_reward += reward
                
                if terminated:
                    break
            
            # 更新目标网络
            if episode % update_target_every == 0:
                self.agent.update_target_network()
            
            rewards.append(episode_reward)
            slippages.append(self.env.total_slippage)
            
            if episode % 100 == 0:
                print(f"Episode {episode}, Reward: {episode_reward:.4f}, Total Slippage: {self.env.total_slippage:.4f}")
        
        return rewards, slippages
    
    def execute_trade(self, order_size: int):
        """
        执行交易
        
        Args:
            order_size: 订单规模
            
        Returns:
            执行结果
        """
        # 重置环境
        self.env.max_order_size = order_size
        state = self.env.reset()
        
        execution_history = []
        
        while True:
            # 选择动作
            action = self.agent.select_action(state)
            
            # 执行动作
            next_state, reward, terminated, info = self.env.step(action)
            
            execution_history.append({
                'step': self.env.current_step,
                'execute_size': info['executed_order'] - sum(h['execute_size'] for h in execution_history),
                'slippage': self.env.total_slippage - sum(h['slippage'] for h in execution_history),
                'remaining_order': info['remaining_order']
            })
            
            state = next_state
            
            if terminated:
                break
        
        return {
            'execution_history': execution_history,
            'total_slippage': self.env.total_slippage,
            'executed_order': self.env.executed_order,
            'remaining_order': self.env.remaining_order
        }


# 测试代码
if __name__ == "__main__":
    # 创建测试数据
    np.random.seed(42)
    n_days = 252
    dates = pd.date_range(start='2020-01-01', periods=n_days, freq='B')
    
    # 创建市场数据
    market_data = pd.DataFrame({
        'close': np.random.normal(3000, 100, n_days),
        'volume': np.random.normal(10000000000, 2000000000, n_days),
        'volatility': np.random.normal(0.01, 0.005, n_days),
        'momentum': np.random.normal(0, 0.02, n_days)
    }, index=dates)
    
    # 创建策略收益率数据
    strategy_returns = pd.DataFrame({
        'value': np.random.normal(0.0005, 0.01, n_days),
        'momentum': np.random.normal(0.0006, 0.012, n_days),
        'growth': np.random.normal(0.0007, 0.015, n_days)
    }, index=dates)
    
    # 测试投资组合管理
    print("测试投资组合管理...")
    portfolio_manager = RLPortfolioManager(market_data, strategy_returns)
    rewards, portfolio_values = portfolio_manager.train(episodes=100)  # 减少训练轮数以加快测试
    
    # 回测
    backtest_result = portfolio_manager.run_backtest()
    print(f"\n回测结果:")
    print(f"最终组合价值: {backtest_result['final_value']:.2f}")
    print(f"夏普比率: {backtest_result['sharpe_ratio']:.4f}")
    print(f"最大回撤: {backtest_result['max_drawdown']:.4f}")
    
    # 测试交易执行
    print("\n测试交易执行...")
    trading_manager = RLTradingManager(market_data)
    rewards, slippages = trading_manager.train(episodes=100)  # 减少训练轮数以加快测试
    
    # 执行交易
    execution_result = trading_manager.execute_trade(100000)
    print(f"\n交易执行结果:")
    print(f"总滑点: {execution_result['total_slippage']:.4f}")
    print(f"执行订单量: {execution_result['executed_order']}")
    print(f"剩余订单量: {execution_result['remaining_order']}")
