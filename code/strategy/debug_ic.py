#!/usr/bin/env python3
"""
IC值调试脚本 - 深度分析IC值低的原因
"""

import pickle
import pandas as pd
import numpy as np
from scipy.stats import spearmanr


def main():
    print("=" * 80)
    print("IC值深度调试")
    print("=" * 80)
    print()

    # 加载数据
    print("加载数据...")
    with open('/Users/variya/.openclaw/workspace/projects/a-stock-advisor/data/real_stock_data.pkl', 'rb') as f:
        df = pickle.load(f)

    # 选择一个样本股票详细检查
    sample_stock = df['stock_code'].unique()[0]
    stock_df = df[df['stock_code'] == sample_stock].sort_values('date').copy()

    print(f"\n检查样本股票: {sample_stock}")
    print(f"数据点数: {len(stock_df)}")
    print()

    # 手动计算forward return
    print("前10天数据:")
    print(stock_df[['date', 'close', 'change_pct', 'momentum_5', 'volatility_5']].head(10))
    print()

    # 计算forward return
    stock_df['forward_return_5d'] = stock_df['close'].shift(-5) / stock_df['close'] - 1
    stock_df['forward_return_5d_pct'] = stock_df['forward_return_5d'] * 100

    print("前10天的forward return:")
    print(stock_df[['date', 'close', 'forward_return_5d', 'forward_return_5d_pct']].head(10))
    print()

    # 检查momentum_5的计算是否正确
    print("\n检查momentum_5的计算:")
    print("momentum_5应该是过去5天的收益率")
    print("手动计算:")
    for i in range(5, 10):
        current_close = stock_df.iloc[i]['close']
        past_close = stock_df.iloc[i-5]['close']
        manual_momentum = (current_close / past_close - 1) * 100
        df_momentum = stock_df.iloc[i]['momentum_5']
        print(f"  Day {i}: manual={manual_momentum:.4f}, df={df_momentum:.4f}, diff={abs(manual_momentum-df_momentum):.4f}")

    print()

    # 检查volatility_5的计算
    print("\n检查volatility_5的计算:")
    print("volatility_5应该是过去5天的收益率标准差")
    print("手动计算:")
    for i in range(5, 10):
        window = stock_df.iloc[i-5:i]
        returns = window['change_pct']
        manual_volatility = returns.std()
        df_volatility = stock_df.iloc[i]['volatility_5']
        print(f"  Day {i}: manual={manual_volatility:.4f}, df={df_volatility:.4f}, diff={abs(manual_volatility-df_volatility):.4f}")

    print()

    # 计算IC值 - 查看是否真的为0
    print("\n计算momentum_5与forward_return_5d的IC:")
    valid_data = stock_df[['momentum_5', 'forward_return_5d_pct']].dropna()
    ic, pval = spearmanr(valid_data['momentum_5'], valid_data['forward_return_5d_pct'])
    print(f"IC={ic:.6f}, pval={pval:.6f}, n={len(valid_data)}")

    # 检查数据分布
    print("\nmomentum_5分布:")
    print(valid_data['momentum_5'].describe())
    print("\nforward_return_5d_pct分布:")
    print(valid_data['forward_return_5d_pct'].describe())

    # 画散点图（用文字表示）
    print("\n前50个数据点的momentum_5 vs forward_return_5d:")
    for i in range(min(50, len(valid_data))):
        momentum = valid_data['momentum_5'].iloc[i]
        forward_ret = valid_data['forward_return_5d_pct'].iloc[i]
        sign = "✓" if (momentum > 0 and forward_ret > 0) or (momentum < 0 and forward_ret < 0) else "✗"
        print(f"  {momentum:>8.2f} vs {forward_ret:>8.2f} {sign}")

    # 计算一致性
    agreement = ((valid_data['momentum_5'] > 0) == (valid_data['forward_return_5d_pct'] > 0)).sum()
    print(f"\n一致性: {agreement}/{len(valid_data)} = {agreement/len(valid_data)*100:.1f}%")

    print()

    # 全量检查更多因子
    print("=" * 80)
    print("全量因子IC分析 (使用forward return)")
    print("=" * 80)

    # 计算全量forward return
    df_sorted = df.sort_values(['stock_code', 'date']).copy()
    df_sorted['forward_return_1d'] = df_sorted.groupby('stock_code')['close'].shift(-1) / df_sorted['close'] - 1
    df_sorted['forward_return_5d'] = df_sorted.groupby('stock_code')['close'].shift(-5) / df_sorted['close'] - 1

    factor_cols = [
        'momentum_5', 'momentum_10', 'momentum_20',
        'volatility_5', 'volatility_10', 'volatility_20',
        'price_to_ma5', 'price_to_ma10', 'price_to_ma20',
        'rsi_14', 'bollinger_position',
        'volume_change', 'turnover_change'
    ]

    print("\n因子IC值 (使用forward return):")
    for factor in factor_cols:
        if factor in df_sorted.columns:
            # 1日forward return
            valid_1d = df_sorted[[factor, 'forward_return_1d']].dropna()
            ic_1d, pval_1d = spearmanr(valid_1d[factor], valid_1d['forward_return_1d'])

            # 5日forward return
            valid_5d = df_sorted[[factor, 'forward_return_5d']].dropna()
            ic_5d, pval_5d = spearmanr(valid_5d[factor], valid_5d['forward_return_5d'])

            print(f"{factor:25s} | IC_1d={ic_1d:.6f} | pval={pval_1d:.4f} | IC_5d={ic_5d:.6f} | pval={pval_5d:.4f}")


if __name__ == '__main__':
    main()
