#!/usr/bin/env python3
"""
构建更有效的因子库
解决现有因子问题：
1. 因子冗余（高度相关）
2. IC不稳定
3. 因子类型单一
"""

import pickle
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from datetime import datetime

def load_data():
    """加载数据"""
    with open('data/akshare_real_data_fixed.pkl', 'rb') as f:
        df = pickle.load(f)
    return df

def calculate_improved_factors(df):
    """计算改进的因子"""
    print("\n=== 计算改进因子 ===")
    
    df = df.sort_values(['stock_code', 'date']).copy()
    
    all_data = []
    for stock_code, group in df.groupby('stock_code'):
        group = group.sort_values('date').copy()
        
        # ===== 1. 改进的动量因子（去冗余，只保留最有效的周期）=====
        # 相对强度：相对于市场的动量
        group['ret_1m'] = group['close'].pct_change(20)
        group['ret_3m'] = group['close'].pct_change(60)
        
        # 动量加速度：短期动量 - 长期动量
        group['momentum_accel'] = group['close'].pct_change(5) - group['close'].pct_change(20)
        
        # ===== 2. 波动率因子（改进计算方式）=====
        # 下行波动率：只计算下跌日的波动
        returns = group['close'].pct_change()
        down_returns = returns.copy()
        down_returns[down_returns > 0] = 0
        group['downside_vol_20'] = down_returns.rolling(20).std() * np.sqrt(252)
        
        # 波动率变化率
        vol_5 = returns.rolling(5).std()
        vol_20 = returns.rolling(20).std()
        group['vol_ratio'] = vol_5 / vol_20
        
        # ===== 3. 流动性因子 =====
        # Amihud非流动性指标
        group['amihud_illiq'] = (abs(returns) / (group['amount'] + 1e-10)).rolling(20).mean()
        
        # 换手率相对强度
        group['turnover_ma20'] = group['turnover'].rolling(20).mean()
        group['turnover_ratio'] = group['turnover'] / (group['turnover_ma20'] + 1e-10)
        
        # ===== 4. 价格形态因子 =====
        # 收盘价位置（在N日高低点之间的位置）
        high_20 = group['high'].rolling(20).max()
        low_20 = group['low'].rolling(20).min()
        group['price_position'] = (group['close'] - low_20) / (high_20 - low_20 + 1e-10)
        
        # 突破因子：价格接近N日高点
        group['breakout_20'] = group['close'] / high_20
        
        # ===== 5. 成交量因子 =====
        # OBV趋势
        obv = (np.sign(returns) * group['volume']).cumsum()
        group['obv_trend'] = obv.rolling(20).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0], raw=False)
        
        # 量价相关性
        def calc_vp_corr(x):
            if len(x) < 10:
                return 0
            vol = x.values
            price = group.loc[x.index, 'close'].values
            if len(vol) == len(price) and len(vol) >= 10:
                return np.corrcoef(vol, price)[0, 1]
            return 0
        
        group['vol_price_corr'] = group['volume'].rolling(20).apply(calc_vp_corr, raw=False)
        
        # ===== 6. 相对强度因子 =====
        # RSI改进版
        delta = group['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / (loss + 1e-10)
        group['rsi_14'] = 100 - (100 / (1 + rs))
        
        # ===== 7. 趋势强度因子 =====
        # ADX趋势强度
        group['tr'] = np.maximum(
            group['high'] - group['low'],
            np.maximum(
                abs(group['high'] - group['close'].shift(1)),
                abs(group['low'] - group['close'].shift(1))
            )
        )
        group['atr_14'] = group['tr'].rolling(14).mean()
        
        # ===== 8. 反转因子 =====
        # 短期反转（5日收益反转）
        group['reversal_5'] = -group['close'].pct_change(5)
        
        # ===== 保留原有因子用于对比 =====
        group['ma20'] = group['close'].rolling(20).mean()
        group['ma60'] = group['close'].rolling(60).mean()
        group['momentum_20'] = group['close'].pct_change(20)
        group['volatility_20'] = returns.rolling(20).std()
        
        all_data.append(group)
    
    df = pd.concat(all_data, ignore_index=True)
    
    # 删除NaN过多的行
    new_factors = ['ret_1m', 'ret_3m', 'momentum_accel', 'downside_vol_20', 
                   'vol_ratio', 'amihud_illiq', 'turnover_ratio', 'price_position',
                   'breakout_20', 'rsi_14', 'reversal_5', 'atr_14']
    df = df.dropna(subset=['ma20', 'momentum_20'])
    
    print(f"✓ 因子计算完成，剩余 {len(df)} 条记录")
    return df, new_factors

def evaluate_new_factors(df, new_factors):
    """评估新因子"""
    print("\n=== 评估新因子 ===")
    
    # 计算return_1d
    df = df.sort_values(['stock_code', 'date'])
    df['return_1d'] = df.groupby('stock_code')['close'].pct_change().shift(-1)
    
    results = {}
    
    # 评估所有因子（包括原有因子）
    all_factors = new_factors + ['momentum_20', 'volatility_20', 'turnover']
    
    for factor in all_factors:
        if factor not in df.columns:
            continue
        
        ic_values = []
        for date, group in df.groupby('date'):
            valid_data = group[[factor, 'return_1d']].dropna()
            if len(valid_data) >= 30:
                ic, pval = spearmanr(valid_data[factor], valid_data['return_1d'])
                if not np.isnan(ic):
                    ic_values.append(ic)
        
        if len(ic_values) >= 5:
            ic_mean = np.mean(ic_values)
            ic_std = np.std(ic_values)
            ir = ic_mean / ic_std if ic_std > 0 else 0
            
            # IC正向率
            ic_positive_rate = sum(1 for x in ic_values if x > 0) / len(ic_values)
            
            # t统计量
            t_stat = ic_mean / (ic_std / np.sqrt(len(ic_values))) if ic_std > 0 else 0
            
            results[factor] = {
                'ic_mean': ic_mean,
                'ic_std': ic_std,
                'ir': ir,
                't_stat': t_stat,
                'ic_positive_rate': ic_positive_rate,
                'effective': bool(abs(ic_mean) >= 0.02 and abs(ir) >= 0.3)
            }
    
    return results

def print_comparison(results, new_factors):
    """打印对比结果"""
    print("\n" + "="*70)
    print("因子效果对比")
    print("="*70)
    
    # 分类显示
    old_factors = ['momentum_20', 'volatility_20', 'turnover']
    
    print("\n【原有因子】")
    print("-"*70)
    print(f"{'因子':<20} {'IC均值':>10} {'IR':>10} {'t统计':>10} {'正向率':>10} {'有效':>6}")
    print("-"*70)
    for factor in old_factors:
        if factor in results:
            r = results[factor]
            status = '✅' if r['effective'] else '❌'
            print(f"{factor:<20} {r['ic_mean']:>10.4f} {r['ir']:>10.2f} {r['t_stat']:>10.2f} {r['ic_positive_rate']:>9.1%} {status:>6}")
    
    print("\n【新增因子】")
    print("-"*70)
    print(f"{'因子':<20} {'IC均值':>10} {'IR':>10} {'t统计':>10} {'正向率':>10} {'有效':>6}")
    print("-"*70)
    
    # 按IC绝对值排序
    new_results = [(f, results[f]) for f in new_factors if f in results]
    new_results.sort(key=lambda x: abs(x[1]['ic_mean']), reverse=True)
    
    for factor, r in new_results:
        status = '✅' if r['effective'] else '❌'
        print(f"{factor:<20} {r['ic_mean']:>10.4f} {r['ir']:>10.2f} {r['t_stat']:>10.2f} {r['ic_positive_rate']:>9.1%} {status:>6}")
    
    # 统计
    old_effective = sum(1 for f in old_factors if f in results and results[f]['effective'])
    new_effective = sum(1 for f in new_factors if f in results and results[f]['effective'])
    
    print("\n" + "="*70)
    print(f"原有因子有效: {old_effective}/3")
    print(f"新增因子有效: {new_effective}/{len(new_factors)}")
    print("="*70)

def main():
    print("="*70)
    print("构建更有效的因子库")
    print("="*70)
    
    # 加载数据
    df = load_data()
    print(f"✓ 加载数据: {len(df)} 条, {df['stock_code'].nunique()} 只股票")
    
    # 计算新因子
    df, new_factors = calculate_improved_factors(df)
    
    # 评估因子
    results = evaluate_new_factors(df, new_factors)
    
    # 打印对比
    print_comparison(results, new_factors)
    
    # 保存改进后的数据
    output_file = 'data/akshare_real_data_fixed.pkl'
    with open(output_file, 'wb') as f:
        pickle.dump(df, f)
    print(f"\n✓ 数据已保存到: {output_file}")
    
    # 保存因子评估结果
    eval_file = 'data/factor_evaluation_improved.json'
    import json
    with open(eval_file, 'w', encoding='utf-8') as f:
        json.dump({k: {kk: float(vv) if isinstance(vv, (np.floating, np.integer)) else vv 
                       for kk, vv in v.items()} 
                   for k, v in results.items()}, f, ensure_ascii=False, indent=2)
    print(f"✓ 因子评估已保存到: {eval_file}")
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
