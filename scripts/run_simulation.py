#!/usr/bin/env python3
"""
A股量化系统 - 模拟交易系统
使用真实数据进行模拟交易
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'code'))

import pickle
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from pathlib import Path

project_root = Path(__file__).parent.parent


class RealDataSimulation:
    """使用真实数据的模拟交易系统"""
    
    def __init__(self):
        self.data = None
        self.results = {}
        self.portfolio = {
            'cash': 1000000,
            'positions': {},
            'history': []
        }
        
    def load_real_data(self):
        """加载真实数据"""
        data_file = project_root / 'data' / 'akshare_real_data_fixed.pkl'
        
        if not data_file.exists():
            print("❌ 数据文件不存在，请先运行数据更新")
            return False
        
        with open(data_file, 'rb') as f:
            self.data = pickle.load(f)
        
        print(f"✓ 加载数据: {len(self.data)} 条记录")
        print(f"✓ 股票数量: {self.data['stock_code'].nunique()}")
        print(f"✓ 时间范围: {self.data['date'].min()} 至 {self.data['date'].max()}")
        
        return True
    
    def calculate_factors(self):
        """计算因子"""
        if self.data is None:
            raise ValueError("数据未加载")
        
        self.data['month'] = pd.to_datetime(self.data['date']).dt.to_period('M')
        
        factor_cols = ['momentum_20', 'volatility_20', 'price_to_ma20', 'turnover']
        available_factors = [f for f in factor_cols if f in self.data.columns]
        
        if not available_factors:
            print("⚠️ 没有可用的因子列，使用默认因子")
            available_factors = ['close']
        
        print(f"✓ 使用因子: {available_factors}")
        
        return self.data
    
    def run_simulation(self):
        """运行模拟交易"""
        dates = sorted(self.data['date'].unique())
        
        if len(dates) < 20:
            print("❌ 数据不足，无法运行模拟")
            return {}
        
        monthly_dates = dates[::20]
        
        for i, date in enumerate(monthly_dates[:-1]):
            current_data = self.data[self.data['date'] == date]
            
            if len(current_data) == 0:
                continue
            
            if 'momentum_20' in current_data.columns:
                selected = current_data.nlargest(10, 'momentum_20')
            else:
                selected = current_data.nlargest(10, 'amount')
            
            self.rebalance_portfolio(selected, date)
            
            self.portfolio['history'].append({
                'date': str(date),
                'total_value': self.calculate_total_value(current_data),
                'positions': len(self.portfolio['positions'])
            })
        
        final_data = self.data[self.data['date'] == dates[-1]]
        final_value = self.calculate_total_value(final_data)
        
        self.results = {
            'initial_capital': 1000000,
            'final_value': final_value,
            'total_return': (final_value - 1000000) / 1000000,
            'trades': len(self.portfolio['history'])
        }
        
        return self.results
    
    def rebalance_portfolio(self, selected_stocks, date):
        """调仓"""
        target_stocks = set(selected_stocks['stock_code'].values)
        current_stocks = set(self.portfolio['positions'].keys())
        
        for stock in current_stocks - target_stocks:
            del self.portfolio['positions'][stock]
        
        for stock in target_stocks:
            if stock not in self.portfolio['positions']:
                self.portfolio['positions'][stock] = {
                    'shares': 1000,
                    'buy_date': str(date)
                }
    
    def calculate_total_value(self, current_data):
        """计算总资产"""
        total = self.portfolio['cash']
        
        for stock, pos in self.portfolio['positions'].items():
            stock_data = current_data[current_data['stock_code'] == stock]
            if len(stock_data) > 0:
                price = stock_data['close'].iloc[0]
                total += pos['shares'] * price
        
        return total
    
    def generate_report(self):
        """生成报告"""
        report = f"""
{'='*60}
A股量化模拟交易报告（真实数据）
{'='*60}

初始资金: ¥{self.results['initial_capital']:,.0f}
最终资产: ¥{self.results['final_value']:,.0f}
总收益率: {self.results['total_return']:.2%}
交易次数: {self.results['trades']}

{'='*60}
注意：此为模拟交易结果，使用真实市场数据
{'='*60}
"""
        return report


def main():
    """主程序"""
    print("="*60)
    print("A股量化模拟交易系统（真实数据版）")
    print("="*60)
    
    os.makedirs('logs', exist_ok=True)
    os.makedirs('reports', exist_ok=True)
    
    system = RealDataSimulation()
    
    print("\n1. 加载真实数据...")
    if not system.load_real_data():
        return 1
    
    print("\n2. 计算因子...")
    system.calculate_factors()
    
    print("\n3. 运行模拟交易...")
    results = system.run_simulation()
    
    if not results:
        return 1
    
    print("\n4. 生成报告...")
    report = system.generate_report()
    print(report)
    
    report_path = project_root / 'reports' / 'simulation_report.txt'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    results_path = project_root / 'reports' / 'simulation_results.json'
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"✓ 报告已保存到: {report_path}")
    print(f"✓ 结果已保存到: {results_path}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
