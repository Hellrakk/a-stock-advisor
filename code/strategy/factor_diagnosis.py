#!/usr/bin/env python3
"""
因子诊断脚本 - 检查IC值和因子有效性
"""

import pickle
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from datetime import datetime


def calculate_forward_returns(df: pd.DataFrame, periods: list = [1, 5, 10, 20]) -> pd.DataFrame:
    """计算未来收益率"""
    results = pd.DataFrame(index=df.index)

    # 按股票分组计算
    for stock_code in df['stock_code'].unique():
        stock_df = df[df['stock_code'] == stock_code].copy()
        stock_df = stock_df.sort_values('date')

        for period in periods:
            # 计算未来收益率
            future_close = stock_df['close'].shift(-period)
            current_close = stock_df['close']
            forward_return = (future_close - current_close) / current_close * 100

            results.loc[stock_df.index, f'return_{period}d'] = forward_return

    return results


def calculate_ic(df: pd.DataFrame, factor_col: str, return_col: str) -> dict:
    """计算单个因子的IC值"""
    # 过滤空值
    valid_data = df[[factor_col, return_col]].dropna()

    if len(valid_data) < 30:
        return {'ic': np.nan, 'pval': np.nan, 'n': 0}

    # 计算Spearman相关系数
    ic, pval = spearmanr(valid_data[factor_col], valid_data[return_col])

    return {
        'ic': ic if not np.isnan(ic) else 0,
        'pval': pval if not np.isnan(pval) else 1,
        'n': len(valid_data)
    }


def analyze_all_factors(df: pd.DataFrame) -> pd.DataFrame:
    """分析所有因子的IC值"""
    # 计算未来收益率
    print("计算未来收益率...")
    returns_df = calculate_forward_returns(df)
    df = pd.concat([df, returns_df], axis=1)

    # 排除非因子列
    exclude_cols = [
        'date', 'stock_code', 'name', 'open', 'high', 'low', 'close',
        'volume', 'amount', 'change_pct', 'turnover', 'ma5', 'ma10', 'ma20', 'ma60',
        'macd', 'macd_signal', 'macd_hist', 'bollinger_mid', 'bollinger_std',
        'bollinger_upper', 'bollinger_lower', 'date_feature_52', 'month',
        'day_of_week', 'is_month_end', 'is_suspended',
        'return_1d', 'return_5d', 'return_10d', 'return_20d'  # 排除收益率列本身
    ]

    factor_cols = [col for col in df.columns if col not in exclude_cols]

    print(f"找到 {len(factor_cols)} 个因子列")
    print("因子列名:", factor_cols)
    print()

    # 分析每个因子
    results = []
    for factor in factor_cols:
        print(f"分析因子: {factor}")
        ic_1d = calculate_ic(df, factor, 'return_1d')
        ic_5d = calculate_ic(df, factor, 'return_5d')
        ic_10d = calculate_ic(df, factor, 'return_10d')
        ic_20d = calculate_ic(df, factor, 'return_20d')

        # 因子基础统计
        factor_stats = df[factor].describe()

        results.append({
            'factor': factor,
            'ic_1d': ic_1d['ic'],
            'pval_1d': ic_1d['pval'],
            'n_1d': ic_1d['n'],
            'ic_5d': ic_5d['ic'],
            'pval_5d': ic_5d['pval'],
            'n_5d': ic_5d['n'],
            'ic_10d': ic_10d['ic'],
            'ic_20d': ic_20d['ic'],
            'mean': factor_stats['mean'],
            'std': factor_stats['std'],
            'min': factor_stats['min'],
            'max': factor_stats['max']
        })

    results_df = pd.DataFrame(results)
    results_df['abs_ic_1d'] = results_df['ic_1d'].abs()
    results_df['is_valid'] = (results_df['abs_ic_1d'] > 0.02) & (results_df['pval_1d'] < 0.05)

    return results_df


def main():
    """主函数"""
    print("=" * 80)
    print("因子诊断系统 - IC值分析")
    print("=" * 80)
    print()

    # 加载数据
    print("加载数据...")
    with open('/Users/variya/.openclaw/workspace/projects/a-stock-advisor/data/real_stock_data.pkl', 'rb') as f:
        df = pickle.load(f)

    print(f"数据形状: {df.shape}")
    print(f"数据时间范围: {df['date'].min()} 到 {df['date'].max()}")
    print(f"股票数量: {df['stock_code'].nunique()}")
    print()

    # 分析所有因子
    results_df = analyze_all_factors(df)

    # 输出结果
    print()
    print("=" * 80)
    print("因子IC值分析结果")
    print("=" * 80)
    print()

    # 按IC绝对值排序
    results_df = results_df.sort_values('abs_ic_1d', ascending=False)

    # 打印所有因子
    print("所有因子IC值 (按1日IC绝对值排序):")
    print()
    for idx, row in results_df.iterrows():
        status = "✓ 有效" if row['is_valid'] else "✗ 无效"
        print(f"{row['factor']:25s} | IC_1d={row['ic_1d']: .4f} | pval={row['pval_1d']: .4f} | "
              f"IC_5d={row['ic_5d']: .4f} | IC_10d={row['ic_10d']: .4f} | {status}")

    print()
    print("=" * 80)
    print("统计摘要")
    print("=" * 80)
    print(f"有效因子数量 (|IC|>0.02, p<0.05): {results_df['is_valid'].sum()}")
    print(f"总因子数量: {len(results_df)}")
    print(f"有效因子占比: {results_df['is_valid'].sum() / len(results_df) * 100:.1f}%")
    print()

    # 保存结果
    output_file = '/Users/variya/.openclaw/workspace/projects/a-stock-advisor/data/factor_ic_report.csv'
    results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"详细结果已保存到: {output_file}")

    # 保存有效因子列表
    valid_factors = results_df[results_df['is_valid']]['factor'].tolist()
    with open('/Users/variya/.openclaw/workspace/projects/a-stock-advisor/data/valid_factors.txt', 'w') as f:
        f.write('\n'.join(valid_factors))
    print(f"有效因子列表已保存到: /Users/variya/.openclaw/workspace/projects/a-stock-advisor/data/valid_factors.txt")
    print()


if __name__ == '__main__':
    main()
