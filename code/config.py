#!/usr/bin/env python3
"""
系统配置文件
集中管理股票代码、参数等配置
"""

import os
from typing import List, Dict

DEFAULT_STOCK_POOL: List[str] = [
    'sh600000', 'sh600036', 'sh600519', 'sh600900', 'sh601318', 'sh601888',
    'sh601899', 'sh600938', 'sh600410', 'sh601857', 'sh600989',
    'sz000001', 'sz000002', 'sz000063', 'sz002703'
]

DEFAULT_STOCK_NAMES: Dict[str, str] = {
    'sh600000': '浦发银行',
    'sh600036': '招商银行',
    'sh600519': '贵州茅台',
    'sh600900': '长江电力',
    'sh601318': '中国平安',
    'sh601888': '中国中免',
    'sh601899': '紫金矿业',
    'sh600938': '中国海油',
    'sh600410': '华胜天成',
    'sh601857': '中国石油',
    'sh600989': '宝丰能源',
    'sz000001': '平安银行',
    'sz000002': '万科A',
    'sz000063': '中兴通讯',
    'sz002703': '浙江世宝'
}

TEST_STOCK_CODES: List[str] = ['sh600000', 'sh601899', 'sz002703']

BENCHMARK_INDICES: Dict[str, str] = {
    'hs300': 'sh000300',
    'zz500': 'sh000905',
    'sz50': 'sh000016',
    'sh': 'sh000001',
    'sz': 'sz399001'
}

DEFAULT_BENCHMARK = 'sh000001'

RISK_CONTROL_CONFIG = {
    'max_single_position': 0.10,
    'max_sector_position': 0.30,
    'stop_loss_threshold': -0.10,
    'take_profit_threshold': 0.20,
    'max_drawdown_threshold': -0.15
}

BACKTEST_CONFIG = {
    'default_initial_capital': 1000000,
    'default_commission_rate': 0.0003,
    'default_slippage': 0.001,
    'default_benchmark': 'sh000001'
}

def get_stock_pool() -> List[str]:
    """获取股票池"""
    env_pool = os.environ.get("STOCK_POOL", "")
    if env_pool:
        return [code.strip() for code in env_pool.split(",") if code.strip()]
    return DEFAULT_STOCK_POOL

def get_risk_config() -> Dict:
    """获取风控配置"""
    return RISK_CONTROL_CONFIG

def get_backtest_config() -> Dict:
    """获取回测配置"""
    return BACKTEST_CONFIG
