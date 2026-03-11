#!/usr/bin/env python3
"""
合并历史下载数据并计算因子
"""

import pandas as pd
import numpy as np
import pickle
import json
import os
from datetime import datetime

def load_all_downloaded_data():
    """加载所有已下载的数据"""
    progress_file = 'data/real_stock_data_fixed_progress.json'
    
    if not os.path.exists(progress_file):
        print("❌ 未找到进度文件")
        return None
    
    with open(progress_file, 'r') as f:
        progress = json.load(f)
    
    completed_stocks = progress.get('completed', [])
    print(f"✓ 进度文件显示已完成 {len(completed_stocks)} 只股票")
    
    # 检查数据文件
    data_file = 'data/real_stock_data_fixed.pkl'
    if os.path.exists(data_file):
        with open(data_file, 'rb') as f:
            df = pickle.load(f)
        print(f"✓ 加载数据文件: {len(df)} 条记录, {df['stock_code'].nunique()} 只股票")
        return df
    
    return None

def standardize_columns(df):
    """标准化列名"""
    # 中文列名转英文
    column_mapping = {
        '日期': 'date',
        '股票代码': 'stock_code_raw',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'amount',
        '振幅': 'amplitude',
        '涨跌幅': 'change_pct',
        '涨跌额': 'change_amount',
        '换手率': 'turnover'
    }
    
    df = df.rename(columns=column_mapping)
    
    # 确保date列是datetime类型
    df['date'] = pd.to_datetime(df['date'])
    
    # 添加stock_name列（从stock_code映射）
    df['stock_name'] = df['stock_code'].apply(lambda x: get_stock_name(x))
    
    return df

def get_stock_name(stock_code):
    """从股票代码获取股票名称"""
    # 尝试从映射文件获取
    mapping_file = 'data/stock_name_mapping.json'
    if os.path.exists(mapping_file):
        try:
            with open(mapping_file, 'r') as f:
                mapping = json.load(f)
            # 移除前缀
            code = stock_code.replace('sh', '').replace('sz', '').replace('bj', '')
            if code in mapping:
                return mapping[code]
        except:
            pass
    return stock_code

def calculate_factors(df):
    """计算技术因子"""
    print("\n=== 计算技术因子 ===")
    
    df = df.sort_values(['stock_code', 'date']).copy()
    
    # 按股票分组计算
    all_data = []
    for stock_code, group in df.groupby('stock_code'):
        group = group.sort_values('date').copy()
        
        # 移动平均线
        group['ma5'] = group['close'].rolling(5).mean()
        group['ma10'] = group['close'].rolling(10).mean()
        group['ma20'] = group['close'].rolling(20).mean()
        group['ma60'] = group['close'].rolling(60).mean()
        
        # 动量因子
        group['momentum_5'] = group['close'].pct_change(5)
        group['momentum_10'] = group['close'].pct_change(10)
        group['momentum_20'] = group['close'].pct_change(20)
        group['momentum_60'] = group['close'].pct_change(60)
        
        # 波动率因子
        group['volatility_5'] = group['close'].pct_change().rolling(5).std()
        group['volatility_10'] = group['close'].pct_change().rolling(10).std()
        group['volatility_20'] = group['close'].pct_change().rolling(20).std()
        
        # 价格相对强弱
        group['price_to_ma20'] = group['close'] / group['ma20'] - 1
        group['price_to_ma60'] = group['close'] / group['ma60'] - 1
        
        # 日期相关
        group['date_dt'] = pd.to_datetime(group['date'])
        
        all_data.append(group)
    
    df = pd.concat(all_data, ignore_index=True)
    
    # 删除NaN值较多的行
    df = df.dropna(subset=['ma20', 'momentum_20'])
    
    print(f"✓ 因子计算完成，剩余 {len(df)} 条记录")
    return df

def calculate_alpha_score(df):
    """计算综合alpha得分"""
    print("\n=== 计算Alpha得分 ===")
    
    factors = ['momentum_5', 'momentum_10', 'momentum_20', 
               'volatility_5', 'volatility_10', 'volatility_20',
               'price_to_ma20', 'price_to_ma60']
    
    available_factors = [f for f in factors if f in df.columns]
    
    if not available_factors:
        df['alpha_score'] = 0.5
        return df
    
    # 按日期分组计算得分
    all_scores = []
    for date, group in df.groupby('date'):
        group = group.copy()
        factor_values = group[available_factors].copy()
        
        # 标准化
        for col in factor_values.columns:
            col_mean = factor_values[col].mean()
            col_std = factor_values[col].std()
            if col_std > 0:
                factor_values[col] = (factor_values[col] - col_mean) / col_std
            else:
                factor_values[col] = 0
        
        # 权重
        weights = {
            'momentum_5': 0.1,
            'momentum_10': 0.15,
            'momentum_20': 0.2,
            'volatility_5': -0.1,
            'volatility_10': -0.1,
            'volatility_20': -0.1,
            'price_to_ma20': 0.15,
            'price_to_ma60': 0.1
        }
        
        alpha_score = np.zeros(len(group))
        for factor in available_factors:
            if factor in weights:
                alpha_score += factor_values[factor].fillna(0) * weights[factor]
        
        # 归一化到0-1
        alpha_min = alpha_score.min()
        alpha_max = alpha_score.max()
        if alpha_max > alpha_min:
            group['alpha_score'] = (alpha_score - alpha_min) / (alpha_max - alpha_min)
        else:
            group['alpha_score'] = 0.5
        
        all_scores.append(group)
    
    df = pd.concat(all_scores, ignore_index=True)
    print(f"✓ Alpha得分计算完成")
    return df

def main():
    print("="*60)
    print("合并数据并计算因子")
    print("="*60)
    
    # 加载数据
    df = load_all_downloaded_data()
    if df is None:
        print("❌ 无法加载数据")
        return 1
    
    # 标准化列名
    df = standardize_columns(df)
    
    # 计算因子
    df = calculate_factors(df)
    
    # 计算Alpha得分
    df = calculate_alpha_score(df)
    
    # 保存到标准位置
    output_file = 'data/akshare_real_data_fixed.pkl'
    with open(output_file, 'wb') as f:
        pickle.dump(df, f)
    
    print(f"\n✓ 数据已保存到: {output_file}")
    print(f"  总记录数: {len(df)}")
    print(f"  股票数量: {df['stock_code'].nunique()}")
    print(f"  日期范围: {df['date'].min()} 到 {df['date'].max()}")
    print(f"  列名: {df.columns.tolist()}")
    
    # 保存最新数据
    latest_date = df['date'].max()
    latest_df = df[df['date'] == latest_date].copy()
    latest_file = 'data/latest_realtime_data.pkl'
    latest_df.to_pickle(latest_file)
    print(f"✓ 最新数据已保存到: {latest_file} ({len(latest_df)} 只股票)")
    
    # 保存元数据
    metadata = {
        'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source': 'real_akshare_data',
        'stock_count': int(df['stock_code'].nunique()),
        'total_records': int(len(df)),
        'date_range': f"{df['date'].min()} to {df['date'].max()}",
        'columns': list(df.columns)
    }
    
    with open(output_file.replace('.pkl', '_metadata.json'), 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    print("\n" + "="*60)
    print("✅ 数据处理完成")
    print("="*60)
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
