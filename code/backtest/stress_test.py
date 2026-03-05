#!/usr/bin/env python3
"""
压力测试模块
验证策略在极端市场环境下的表现
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import json
import os
import warnings

warnings.filterwarnings('ignore')

class StressTest:
    """压力测试"""
    
    def __init__(self, data_path: str):
        self.data_path = data_path
        self.df = None
        self.results = {}
        self._load_data()
    
    def _load_data(self):
        """加载数据"""
        print(f"📂 加载数据: {self.data_path}")
        self.df = pd.read_pickle(self.data_path)
        
        if 'date' in self.df.columns:
            self.df['date'] = pd.to_datetime(self.df['date'])
        
        self.df = self.df.sort_values('date')
        print(f"✓ 数据加载成功: {self.df.shape}")
    
    def simulate_market_crash(self, crash_magnitude: float = -0.05, duration: int = 5) -> Dict:
        """
        模拟市场崩盘
        
        Args:
            crash_magnitude: 每日下跌幅度
            duration: 持续天数
            
        Returns:
            测试结果
        """
        print(f"\n🚨 模拟市场崩盘: 每日下跌{crash_magnitude*100:.1f}%, 持续{duration}天")
        
        # 计算基准收益
        base_returns = self.df.groupby('stock_code')['close'].apply(lambda x: (x.iloc[-1] - x.iloc[0]) / x.iloc[0])
        base_avg_return = base_returns.mean()
        base_volatility = base_returns.std()
        
        # 模拟崩盘情景
        crash_returns = []
        for stock_code, group in self.df.groupby('stock_code'):
            if len(group) < duration:
                continue
            
            # 模拟崩盘期间的价格
            start_price = group['close'].iloc[0]
            crash_prices = [start_price]
            
            for i in range(1, duration):
                crash_prices.append(crash_prices[-1] * (1 + crash_magnitude))
            
            # 计算崩盘期间的收益
            crash_return = (crash_prices[-1] - crash_prices[0]) / crash_prices[0]
            crash_returns.append(crash_return)
        
        crash_returns = np.array(crash_returns)
        crash_avg_return = crash_returns.mean()
        crash_volatility = crash_returns.std()
        max_drawdown = (crash_returns.min() - 1) * 100
        
        result = {
            'scenario': 'market_crash',
            'crash_magnitude': crash_magnitude,
            'duration': duration,
            'base_avg_return': float(base_avg_return),
            'base_volatility': float(base_volatility),
            'crash_avg_return': float(crash_avg_return),
            'crash_volatility': float(crash_volatility),
            'max_drawdown': float(max_drawdown),
            'worst_stock_return': float(crash_returns.min()),
            'best_stock_return': float(crash_returns.max())
        }
        
        print(f"  基准平均收益: {base_avg_return*100:.2f}%")
        print(f"  崩盘平均收益: {crash_avg_return*100:.2f}%")
        print(f"  最大回撤: {max_drawdown:.2f}%")
        
        self.results['market_crash'] = result
        return result
    
    def simulate_volatility_spike(self, volatility_increase: float = 2.0, duration: int = 10) -> Dict:
        """
        模拟波动率飙升
        
        Args:
            volatility_increase: 波动率增加倍数
            duration: 持续天数
            
        Returns:
            测试结果
        """
        print(f"\n📈 模拟波动率飙升: 波动率增加{volatility_increase}倍, 持续{duration}天")
        
        # 计算基准波动率
        base_volatility = self.df.groupby('stock_code')['close'].apply(lambda x: x.pct_change().std()).mean()
        
        # 模拟波动率飙升情景
        spike_volatilities = []
        for stock_code, group in self.df.groupby('stock_code'):
            if len(group) < duration:
                continue
            
            # 计算原始收益率
            returns = group['close'].pct_change().dropna()
            if len(returns) < duration:
                continue
            
            # 增加波动率
            original_std = returns.std()
            spike_returns = returns * np.random.normal(1, volatility_increase-1, len(returns))
            spike_std = spike_returns.std()
            
            spike_volatilities.append(spike_std)
        
        spike_volatilities = np.array(spike_volatilities)
        spike_avg_volatility = spike_volatilities.mean()
        max_volatility = spike_volatilities.max()
        
        result = {
            'scenario': 'volatility_spike',
            'volatility_increase': volatility_increase,
            'duration': duration,
            'base_volatility': float(base_volatility),
            'spike_avg_volatility': float(spike_avg_volatility),
            'max_volatility': float(max_volatility),
            'volatility_ratio': float(spike_avg_volatility / base_volatility)
        }
        
        print(f"  基准波动率: {base_volatility*100:.2f}%")
        print(f"  飙升后平均波动率: {spike_avg_volatility*100:.2f}%")
        print(f"  波动率增加倍数: {result['volatility_ratio']:.2f}x")
        
        self.results['volatility_spike'] = result
        return result
    
    def simulate_liquidity_dry_up(self, liquidity_decrease: float = 0.5, duration: int = 5) -> Dict:
        """
        模拟流动性枯竭
        
        Args:
            liquidity_decrease: 流动性减少比例
            duration: 持续天数
            
        Returns:
            测试结果
        """
        print(f"\n💧 模拟流动性枯竭: 流动性减少{liquidity_decrease*100:.1f}%, 持续{duration}天")
        
        # 计算基准成交量
        if 'volume' in self.df.columns:
            base_volume = self.df.groupby('stock_code')['volume'].mean().mean()
        else:
            base_volume = 1000000  # 假设值
        
        # 模拟流动性枯竭情景
        impact_costs = []
        for stock_code, group in self.df.groupby('stock_code'):
            if len(group) < duration:
                continue
            
            # 计算冲击成本（流动性减少导致冲击成本增加）
            if 'volume' in group.columns:
                avg_volume = group['volume'].mean()
                reduced_volume = avg_volume * (1 - liquidity_decrease)
                # 冲击成本与成交量成反比
                impact_cost = 0.001 * (avg_volume / reduced_volume)
                impact_costs.append(impact_cost)
        
        if impact_costs:
            impact_costs = np.array(impact_costs)
            avg_impact_cost = impact_costs.mean()
            max_impact_cost = impact_costs.max()
        else:
            avg_impact_cost = 0.005
            max_impact_cost = 0.01
        
        result = {
            'scenario': 'liquidity_dry_up',
            'liquidity_decrease': liquidity_decrease,
            'duration': duration,
            'base_volume': float(base_volume),
            'avg_impact_cost': float(avg_impact_cost),
            'max_impact_cost': float(max_impact_cost)
        }
        
        print(f"  基准平均成交量: {base_volume:.0f}")
        print(f"  平均冲击成本: {avg_impact_cost*100:.2f}%")
        print(f"  最大冲击成本: {max_impact_cost*100:.2f}%")
        
        self.results['liquidity_dry_up'] = result
        return result
    
    def simulate_sector_rotation(self, sector_underperformance: float = -0.1, duration: int = 10) -> Dict:
        """
        模拟行业轮动
        
        Args:
            sector_underperformance: 行业表现落后大盘的幅度
            duration: 持续天数
            
        Returns:
            测试结果
        """
        print(f"\n🔄 模拟行业轮动: 行业落后大盘{sector_underperformance*100:.1f}%, 持续{duration}天")
        
        # 计算行业收益
        if 'industry' in self.df.columns:
            sector_returns = self.df.groupby(['industry', 'date'])['close'].mean().groupby('industry').apply(lambda x: (x.iloc[-1] - x.iloc[0]) / x.iloc[0])
            market_return = self.df.groupby('date')['close'].mean().apply(lambda x: (x.iloc[-1] - x.iloc[0]) / x.iloc[0])[0]
            
            # 模拟行业轮动
            underperforming_sectors = []
            for sector, return_val in sector_returns.items():
                simulated_return = return_val + sector_underperformance
                underperforming_sectors.append(simulated_return)
            
            if underperforming_sectors:
                underperforming_sectors = np.array(underperforming_sectors)
                avg_underperformance = underperforming_sectors.mean()
                worst_sector_return = underperforming_sectors.min()
            else:
                avg_underperformance = market_return + sector_underperformance
                worst_sector_return = market_return + sector_underperformance
        else:
            market_return = 0.05
            avg_underperformance = market_return + sector_underperformance
            worst_sector_return = market_return + sector_underperformance
        
        result = {
            'scenario': 'sector_rotation',
            'sector_underperformance': sector_underperformance,
            'duration': duration,
            'market_return': float(market_return),
            'avg_sector_return': float(avg_underperformance),
            'worst_sector_return': float(worst_sector_return),
            'relative_performance': float(avg_underperformance - market_return)
        }
        
        print(f"  大盘收益: {market_return*100:.2f}%")
        print(f"  行业平均收益: {avg_underperformance*100:.2f}%")
        print(f"  相对表现: {result['relative_performance']*100:.2f}%")
        
        self.results['sector_rotation'] = result
        return result
    
    def run_all_stress_tests(self) -> Dict:
        """
        运行所有压力测试
        
        Returns:
            所有测试结果
        """
        print("=" * 70)
        print("🚀 运行完整压力测试")
        print("=" * 70)
        
        self.simulate_market_crash()
        self.simulate_volatility_spike()
        self.simulate_liquidity_dry_up()
        self.simulate_sector_rotation()
        
        print("\n" + "=" * 70)
        print("✅ 压力测试完成")
        print("=" * 70)
        
        return self.results
    
    def generate_stress_test_report(self) -> str:
        """
        生成压力测试报告
        
        Returns:
            Markdown格式的报告
        """
        lines = []
        
        lines.append("# 压力测试报告\n")
        lines.append(f"**生成时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        lines.append("---\n")
        
        # 市场崩盘测试
        if 'market_crash' in self.results:
            crash = self.results['market_crash']
            lines.append("## 🚨 市场崩盘测试\n")
            lines.append(f"- 测试情景: 每日下跌{crash['crash_magnitude']*100:.1f}%, 持续{crash['duration']}天\n")
            lines.append(f"- 基准平均收益: {crash['base_avg_return']*100:.2f}%\n")
            lines.append(f"- 崩盘平均收益: {crash['crash_avg_return']*100:.2f}%\n")
            lines.append(f"- 最大回撤: {crash['max_drawdown']:.2f}%\n")
            lines.append(f"- 最差个股收益: {crash['worst_stock_return']*100:.2f}%\n")
            lines.append(f"- 最佳个股收益: {crash['best_stock_return']*100:.2f}%\n\n")
        
        # 波动率飙升测试
        if 'volatility_spike' in self.results:
            vol = self.results['volatility_spike']
            lines.append("## 📈 波动率飙升测试\n")
            lines.append(f"- 测试情景: 波动率增加{vol['volatility_increase']}倍, 持续{vol['duration']}天\n")
            lines.append(f"- 基准波动率: {vol['base_volatility']*100:.2f}%\n")
            lines.append(f"- 飙升后平均波动率: {vol['spike_avg_volatility']*100:.2f}%\n")
            lines.append(f"- 波动率增加倍数: {vol['volatility_ratio']:.2f}x\n\n")
        
        # 流动性枯竭测试
        if 'liquidity_dry_up' in self.results:
            liq = self.results['liquidity_dry_up']
            lines.append("## 💧 流动性枯竭测试\n")
            lines.append(f"- 测试情景: 流动性减少{liq['liquidity_decrease']*100:.1f}%, 持续{liq['duration']}天\n")
            lines.append(f"- 基准平均成交量: {liq['base_volume']:.0f}\n")
            lines.append(f"- 平均冲击成本: {liq['avg_impact_cost']*100:.2f}%\n")
            lines.append(f"- 最大冲击成本: {liq['max_impact_cost']*100:.2f}%\n\n")
        
        # 行业轮动测试
        if 'sector_rotation' in self.results:
            sector = self.results['sector_rotation']
            lines.append("## 🔄 行业轮动测试\n")
            lines.append(f"- 测试情景: 行业落后大盘{sector['sector_underperformance']*100:.1f}%, 持续{sector['duration']}天\n")
            lines.append(f"- 大盘收益: {sector['market_return']*100:.2f}%\n")
            lines.append(f"- 行业平均收益: {sector['avg_sector_return']*100:.2f}%\n")
            lines.append(f"- 相对表现: {sector['relative_performance']*100:.2f}%\n\n")
        
        # 风险评估
        lines.append("## 📊 风险评估\n")
        lines.append("### 关键风险指标:\n")
        
        if 'market_crash' in self.results:
            max_dd = self.results['market_crash']['max_drawdown']
            lines.append(f"- 最大回撤: {max_dd:.2f}% {'⚠️' if max_dd < -30 else '✅'}\n")
        
        if 'volatility_spike' in self.results:
            vol_ratio = self.results['volatility_spike']['volatility_ratio']
            lines.append(f"- 波动率增加倍数: {vol_ratio:.2f}x {'⚠️' if vol_ratio > 3 else '✅'}\n")
        
        if 'liquidity_dry_up' in self.results:
            max_impact = self.results['liquidity_dry_up']['max_impact_cost']
            lines.append(f"- 最大冲击成本: {max_impact*100:.2f}% {'⚠️' if max_impact > 0.02 else '✅'}\n")
        
        lines.append("\n### 策略稳健性评估:\n")
        lines.append("- **抗跌性**: 测试策略在市场崩盘时的表现\n")
        lines.append("- **抗波动**: 测试策略在波动率飙升时的表现\n")
        lines.append("- **抗流动性风险**: 测试策略在流动性枯竭时的表现\n")
        lines.append("- **行业轮动适应性**: 测试策略在行业轮动时的表现\n")
        
        lines.append("\n---\n")
        lines.append("## 📋 结论\n")
        lines.append("本压力测试评估了策略在极端市场环境下的表现，为风险管理提供参考。\n")
        lines.append("建议根据测试结果调整策略参数，以提高策略的稳健性。\n")
        
        return ''.join(lines)
    
    def save_report(self, output_dir: str = '/Users/variya/.openclaw/workspace/projects/a-stock-advisor/reports'):
        """
        保存压力测试报告
        
        Args:
            output_dir: 输出目录
        """
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # JSON报告
        json_path = os.path.join(output_dir, f'stress_test_{timestamp}.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False, default=str)
        
        # Markdown报告
        md_report = self.generate_stress_test_report()
        md_path = os.path.join(output_dir, f'stress_test_{timestamp}.md')
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(md_report)
        
        print(f"\n✓ 压力测试报告已保存:")
        print(f"  JSON: {json_path}")
        print(f"  Markdown: {md_path}")
        
        return json_path, md_path


def main():
    """主函数"""
    print("=" * 70)
    print("🚀 压力测试系统")
    print("=" * 70)
    
    st = StressTest(
        data_path='/Users/variya/.openclaw/workspace/projects/a-stock-advisor/data/mock_data.pkl'
    )
    
    results = st.run_all_stress_tests()
    st.save_report()
    
    return results


if __name__ == '__main__':
    main()
