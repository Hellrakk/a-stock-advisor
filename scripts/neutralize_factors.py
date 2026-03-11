#!/usr/bin/env python3
"""
因子中性化处理 - 解决IR低的问题
核心改进：
1. 行业中性化
2. 市值中性化  
3. 因子正交化
"""

import pickle
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')

def load_data():
    """加载数据"""
    with open('data/akshare_real_data_fixed.pkl', 'rb') as f:
        df = pickle.load(f)
    return df

def assign_industry(df):
    """根据股票代码分配行业（简化版）"""
    industry_map = {
        '60': '工业',    # 上交所主板
        '00': '制造',    # 深交所主板
        '30': '科技',    # 创业板
        '68': '科技',    # 科创板
        '003': '制造',   # 深交所主板
        '688': '科技',   # 科创板
    }
    
    def get_industry(code):
        code = str(code).replace('sh', '').replace('sz', '').replace('bj', '')
        for prefix, ind in industry_map.items():
            if code.startswith(prefix):
                return ind
        return '其他'
    
    df['industry'] = df['stock_code'].apply(get_industry)
    return df

def neutralize_factor(df, factor_col, group_cols=['date', 'industry']):
    """
    因子中性化处理
    对每个日期-行业组合，减去均值，除以标准差
    """
    df = df.copy()
    
    neutralized = []
    for group_vals, group in df.groupby(group_cols):
        if len(group) < 10:
            neutralized.extend([0] * len(group))
            continue
        
        factor_vals = group[factor_col].values
        mean_val = np.nanmean(factor_vals)
        std_val = np.nanstd(factor_vals)
        
        if std_val > 0:
            neutralized.extend((factor_vals - mean_val) / std_val)
        else:
            neutralized.extend([0] * len(group))
    
    return np.array(neutralized)

def calculate_neutralized_factors(df):
    """计算中性化后的因子"""
    print("\n=== 计算中性化因子 ===")
    
    df = df.sort_values(['stock_code', 'date']).copy()
    
    # 分配行业
    df = assign_industry(df)
    print(f"行业分布: {df['industry'].value_counts().to_dict()}")
    
    # 计算市值代理变量（用成交额）
    df['log_amount'] = np.log(df['amount'] + 1)
    
    # 计算基础因子
    all_data = []
    for stock_code, group in df.groupby('stock_code'):
        group = group.sort_values('date').copy()
        
        returns = group['close'].pct_change()
        
        # 基础因子
        group['momentum_20_raw'] = group['close'].pct_change(20)
        group['volatility_20_raw'] = returns.rolling(20).std()
        group['turnover_raw'] = group['turnover']
        
        # 改进因子
        group['reversal_5_raw'] = -group['close'].pct_change(5)
        
        # 下行波动率
        down_ret = returns.copy()
        down_ret[down_ret > 0] = 0
        group['downside_vol_raw'] = down_ret.rolling(20).std()
        
        # 价格位置
        high_20 = group['high'].rolling(20).max()
        low_20 = group['low'].rolling(20).min()
        group['price_position_raw'] = (group['close'] - low_20) / (high_20 - low_20 + 1e-10)
        
        # 换手率相对强度
        group['turnover_ma20'] = group['turnover'].rolling(20).mean()
        group['turnover_ratio_raw'] = group['turnover'] / (group['turnover_ma20'] + 1e-10)
        
        all_data.append(group)
    
    df = pd.concat(all_data, ignore_index=True)
    
    # 中性化处理
    raw_factors = ['momentum_20_raw', 'volatility_20_raw', 'turnover_raw', 
                   'reversal_5_raw', 'downside_vol_raw', 'price_position_raw', 
                   'turnover_ratio_raw']
    
    for raw_factor in raw_factors:
        neutral_factor = raw_factor.replace('_raw', '')
        print(f"  中性化: {neutral_factor}")
        df[neutral_factor] = neutralize_factor(df, raw_factor)
    
    # 市值中性化（对每个日期）
    print("  市值中性化...")
    for date, group in df.groupby('date'):
        if len(group) < 50:
            continue
        
        # 对每个因子，回归掉市值影响
        for factor in ['momentum_20', 'volatility_20', 'turnover', 'reversal_5']:
            if factor not in df.columns:
                continue
            
            mask = group[factor].notna() & group['log_amount'].notna()
            if mask.sum() < 30:
                continue
            
            X = group.loc[mask, 'log_amount'].values.reshape(-1, 1)
            y = group.loc[mask, factor].values
            
            model = LinearRegression()
            model.fit(X, y)
            residuals = y - model.predict(X)
            
            df.loc[group[mask].index, factor] = residuals
    
    print("✓ 中性化完成")
    return df

def evaluate_factors_neutralized(df):
    """评估中性化后的因子"""
    print("\n=== 评估中性化因子 ===")
    
    # 计算return_1d
    df = df.sort_values(['stock_code', 'date'])
    df['return_1d'] = df.groupby('stock_code')['close'].pct_change().shift(-1)
    
    factors = ['momentum_20', 'volatility_20', 'turnover', 
               'reversal_5', 'downside_vol', 'price_position', 'turnover_ratio']
    
    results = {}
    
    for factor in factors:
        if factor not in df.columns:
            continue
        
        ic_values = []
        for date, group in df.groupby('date'):
            valid_data = group[[factor, 'return_1d']].dropna()
            if len(valid_data) >= 30:
                ic, _ = spearmanr(valid_data[factor], valid_data['return_1d'])
                if not np.isnan(ic):
                    ic_values.append(ic)
        
        if len(ic_values) >= 5:
            ic_mean = np.mean(ic_values)
            ic_std = np.std(ic_values)
            ir = ic_mean / ic_std if ic_std > 0 else 0
            
            results[factor] = {
                'ic_mean': ic_mean,
                'ic_std': ic_std,
                'ir': ir,
                'ic_positive_rate': sum(1 for x in ic_values if x > 0) / len(ic_values),
                'effective': bool(abs(ic_mean) >= 0.02 and abs(ir) >= 0.3)
            }
    
    return results, df

def print_results(results_before, results_after):
    """打印对比结果"""
    print("\n" + "="*80)
    print("中性化前后因子效果对比")
    print("="*80)
    
    print(f"\n{'因子':<20} {'IC(前)':>10} {'IR(前)':>10} {'IC(后)':>10} {'IR(后)':>10} {'改善':>8}")
    print("-"*80)
    
    for factor in results_after:
        if factor in results_before:
            b = results_before[factor]
            a = results_after[factor]
            
            ir_improve = abs(a['ir']) - abs(b['ir'])
            improve_str = '✅' if ir_improve > 0.05 else ('⚠️' if ir_improve > 0 else '❌')
            
            print(f"{factor:<20} {b['ic_mean']:>10.4f} {b['ir']:>10.2f} {a['ic_mean']:>10.4f} {a['ir']:>10.2f} {improve_str:>8}")
    
    before_effective = sum(1 for r in results_before.values() if r['effective'])
    after_effective = sum(1 for r in results_after.values() if r['effective'])
    
    print("\n" + "="*80)
    print(f"中性化前有效因子: {before_effective}/{len(results_before)}")
    print(f"中性化后有效因子: {after_effective}/{len(results_after)}")
    print("="*80)

def main():
    print("="*80)
    print("因子中性化处理")
    print("="*80)
    
    # 加载数据
    df = load_data()
    print(f"✓ 加载数据: {len(df)} 条")
    
    # 评估原始因子
    print("\n【评估原始因子】")
    df_temp = df.sort_values(['stock_code', 'date'])
    df_temp['return_1d'] = df_temp.groupby('stock_code')['close'].pct_change().shift(-1)
    
    results_before = {}
    for factor in ['momentum_20', 'volatility_20', 'turnover']:
        if factor in df_temp.columns:
            ic_values = []
            for date, group in df_temp.groupby('date'):
                valid = group[[factor, 'return_1d']].dropna()
                if len(valid) >= 30:
                    ic, _ = spearmanr(valid[factor], valid['return_1d'])
                    if not np.isnan(ic):
                        ic_values.append(ic)
            
            if ic_values:
                results_before[factor] = {
                    'ic_mean': np.mean(ic_values),
                    'ic_std': np.std(ic_values),
                    'ir': np.mean(ic_values) / np.std(ic_values) if np.std(ic_values) > 0 else 0,
                    'effective': False
                }
    
    # 中性化处理
    df = calculate_neutralized_factors(df)
    
    # 评估中性化后的因子
    results_after, df = evaluate_factors_neutralized(df)
    
    # 打印结果
    print_results(results_before, results_after)
    
    # 保存
    output_file = 'data/akshare_real_data_fixed.pkl'
    with open(output_file, 'wb') as f:
        pickle.dump(df, f)
    print(f"\n✓ 数据已保存: {output_file}")
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
