#!/usr/bin/env python3
"""
因子引擎v2.0 - 重构的高IC因子计算系统

改进内容：
1. 修复现有因子计算逻辑
2. 添加A股特色因子（资金流向、换手率、技术形态）
3. 优化因子组合和权重
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from scipy.stats import spearmanr
import warnings
warnings.filterwarnings('ignore')


class ACashFlowFactorEngine:
    """资金流向因子引擎 - 核心改进"""

    @staticmethod
    def calculate_large_order_ratio(df: pd.DataFrame, large_threshold: float = 0.3, window: int = 5) -> pd.Series:
        """
        计算大单占比因子
        大单占比高通常意味着主力资金介入

        Args:
            df: 原始数据
            large_threshold: 大单阈值（成交量占比）
            window: 统计窗口

        Returns:
            大单占比因子
        """
        # 使用turnover和amount来模拟大单占比
        # 大单交易通常伴随着高成交额和低换手率（机构行为）
        df_sorted = df.sort_values(['stock_code', 'date']).copy()

        # 计算大单信号：成交额突增但换手率不高
        df_sorted['avg_amount_5'] = df_sorted.groupby('stock_code')['amount'].rolling(window).mean().reset_index(0, drop=True)
        df_sorted['avg_turnover_5'] = df_sorted.groupby('stock_code')['turnover'].rolling(window).mean().reset_index(0, drop=True)

        # 大单占比 = 当日成交额/5日平均成交额 * (5日平均换手率/当日换手率)
        # 当成交额放大但换手率不高时，可能是大单行为
        df_sorted['large_order_ratio'] = (
            df_sorted['amount'] / (df_sorted['avg_amount_5'] + 1e-10) *
            (df_sorted['avg_turnover_5'] + 1e-10) / (df_sorted['turnover'] + 1e-10)
        )

        # 标准化
        for stock in df_sorted['stock_code'].unique():
            mask = df_sorted['stock_code'] == stock
            values = df_sorted.loc[mask, 'large_order_ratio']
            mean = values.mean()
            std = values.std()
            if std > 0:
                df_sorted.loc[mask, 'large_order_ratio'] = (values - mean) / std

        return df_sorted['large_order_ratio']

    @staticmethod
    def calculate_net_inflow(df: pd.DataFrame, window: int = 5) -> pd.Series:
        """
        计算净流入因子
        净流入 = 收盘价相对于开盘价的位置 * 成交额

        Args:
            df: 原始数据
            window: 统计窗口

        Returns:
            净流入因子
        """
        df_sorted = df.sort_values(['stock_code', 'date']).copy()

        # 当日净流入 = (收盘价 - 开盘价) / 开盘价 * 成交额
        df_sorted['daily_net_inflow'] = (
            (df_sorted['close'] - df_sorted['open']) / df_sorted['open'] *
            df_sorted['amount']
        )

        # 计算窗口累计净流入
        df_sorted['net_inflow'] = df_sorted.groupby('stock_code')['daily_net_inflow'].transform(
            lambda x: x.rolling(window).sum()
        )

        # 标准化（按股票）
        for stock in df_sorted['stock_code'].unique():
            mask = df_sorted['stock_code'] == stock
            values = df_sorted.loc[mask, 'net_inflow']
            mean = values.mean()
            std = values.std()
            if std > 0:
                df_sorted.loc[mask, 'net_inflow'] = (values - mean) / std

        return df_sorted['net_inflow']

    @staticmethod
    def calculate_institutional_pressure(df: pd.DataFrame, window: int = 10) -> pd.Series:
        """
        计算机构压力因子
        机构通常在价格稳定时买入，在价格剧烈波动时卖出

        Args:
            df: 原始数据
            window: 统计窗口

        Returns:
            机构压力因子
        """
        df_sorted = df.sort_values(['stock_code', 'date']).copy()

        # 计算价格波动率
        df_sorted['volatility_10'] = df_sorted.groupby('stock_code')['change_pct'].transform(
            lambda x: x.rolling(window).std()
        )

        # 计算换手率均值
        df_sorted['turnover_mean_10'] = df_sorted.groupby('stock_code')['turnover'].transform(
            lambda x: x.rolling(window).mean()
        )

        # 机构压力 = 换手率 / (波动率 + 1e-10)
        # 低波动率 + 适度换手率 = 机构积极介入
        df_sorted['institutional_pressure'] = (
            df_sorted['turnover'] / (df_sorted['volatility_10'] + 1e-10)
        )

        # 归一化到 [0, 1]
        for stock in df_sorted['stock_code'].unique():
            mask = df_sorted['stock_code'] == stock
            values = df_sorted.loc[mask, 'institutional_pressure']
            min_val = values.min()
            max_val = values.max()
            if max_val > min_val:
                df_sorted.loc[mask, 'institutional_pressure'] = (values - min_val) / (max_val - min_val)
            else:
                df_sorted.loc[mask, 'institutional_pressure'] = 0.5

        return df_sorted['institutional_pressure']


class TurnoverFactorEngine:
    """换手率因子引擎"""

    @staticmethod
    def calculate_turnover_surge(df: pd.DataFrame, window: int = 20, surge_threshold: float = 2.0) -> pd.Series:
        """
        计算换手率激增因子
        换手率激增通常是变盘信号

        Args:
            df: 原始数据
            window: 统计窗口
            surge_threshold: 激增阈值

        Returns:
            换手率激增因子
        """
        df_sorted = df.sort_values(['stock_code', 'date']).copy()

        # 计算平均换手率
        df_sorted['turnover_avg_20'] = df_sorted.groupby('stock_code')['turnover'].transform(
            lambda x: x.rolling(window).mean()
        )

        # 换手率激增因子
        df_sorted['turnover_surge'] = df_sorted['turnover'] / (df_sorted['turnover_avg_20'] + 1e-10)

        # 标准化
        for stock in df_sorted['stock_code'].unique():
            mask = df_sorted['stock_code'] == stock
            values = df_sorted.loc[mask, 'turnover_surge']
            # 限制在合理范围
            values = values.clip(0, 10)
            mean = values.mean()
            std = values.std()
            if std > 0:
                df_sorted.loc[mask, 'turnover_surge'] = (values - mean) / std

        return df_sorted['turnover_surge']

    @staticmethod
    def calculate_price_volume_trend(df: pd.DataFrame, window: int = 5) -> pd.Series:
        """
        计算量价配合因子
        量价齐升是好信号

        Args:
            df: 原始数据
            window: 统计窗口

        Returns:
            量价配合因子
        """
        df_sorted = df.sort_values(['stock_code', 'date']).copy()

        # 计算收益率的符号
        df_sorted['return_sign'] = np.sign(df_sorted['change_pct'])

        # 计算成交量增长
        df_sorted['volume_growth'] = df_sorted.groupby('stock_code')['volume'].pct_change()

        # 量价配合 = 收益率符号 * 成交量增长率
        df_sorted['price_volume_trend'] = df_sorted['return_sign'] * df_sorted['volume_growth']

        # 平滑处理
        df_sorted['price_volume_trend'] = df_sorted.groupby('stock_code')['price_volume_trend'].transform(
            lambda x: x.rolling(window).mean()
        )

        # 标准化
        for stock in df_sorted['stock_code'].unique():
            mask = df_sorted['stock_code'] == stock
            values = df_sorted.loc[mask, 'price_volume_trend']
            mean = values.mean()
            std = values.std()
            if std > 0:
                df_sorted.loc[mask, 'price_volume_trend'] = (values - mean) / std

        return df_sorted['price_volume_trend']

    @staticmethod
    def calculate_cumulative_turnover(df: pd.DataFrame, window: int = 10) -> pd.Series:
        """
        计算累计换手率因子
        高累计换手率通常意味着主力出货

        Args:
            df: 原始数据
            window: 统计窗口

        Returns:
            累计换手率因子（反向）
        """
        df_sorted = df.sort_values(['stock_code', 'date']).copy()

        # 计算累计换手率
        df_sorted['cumulative_turnover'] = df_sorted.groupby('stock_code')['turnover'].transform(
            lambda x: x.rolling(window).sum()
        )

        # 累计换手率越高，风险越大（取负值）
        # 标准化
        for stock in df_sorted['stock_code'].unique():
            mask = df_sorted['stock_code'] == stock
            values = df_sorted.loc[mask, 'cumulative_turnover']

            # 按百分位分组
            quantiles = values.quantile([0.25, 0.5, 0.75])

            # 超高换手率（>75%）得负分
            # 中等换手率得正分
            scores = pd.Series(0.0, index=values.index)
            scores[values <= quantiles[0.25]] = 0.5
            scores[(values > quantiles[0.25]) & (values <= quantiles[0.75])] = 1.0
            scores[values > quantiles[0.75]] = -0.5

            df_sorted.loc[mask, 'cumulative_turnover_score'] = scores

        return df_sorted['cumulative_turnover_score']


class TechnicalPatternEngine:
    """技术形态因子引擎"""

    @staticmethod
    def calculate_breakout_strength(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """
        计算突破强度因子
        突破均线或阻力位是好信号

        Args:
            df: 原始数据
            window: 统计窗口

        Returns:
            突破强度因子
        """
        df_sorted = df.sort_values(['stock_code', 'date']).copy()

        # 计算最高价的滚动窗口
        df_sorted['highest_20'] = df_sorted.groupby('stock_code')['high'].transform(
            lambda x: x.rolling(window).max()
        )

        # 突破强度 = (收盘价 - 20日最高价) / 20日最高价
        # 如果收盘价创新高，突破强度为正
        df_sorted['breakout_strength'] = (
            df_sorted['close'] - df_sorted['highest_20'].shift(1)
        ) / (df_sorted['highest_20'].shift(1) + 1e-10)

        # 标准化
        for stock in df_sorted['stock_code'].unique():
            mask = df_sorted['stock_code'] == stock
            values = df_sorted.loc[mask, 'breakout_strength']
            mean = values.mean()
            std = values.std()
            if std > 0:
                df_sorted.loc[mask, 'breakout_strength'] = (values - mean) / std

        return df_sorted['breakout_strength']

    @staticmethod
    def calculate_bounce_strength(df: pd.DataFrame, ma_window: int = 20, lookback: int = 10) -> pd.Series:
        """
        计算回踩支撑因子
        回踩均线后反弹是好信号

        Args:
            df: 原始数据
            ma_window: 均线窗口
            lookback: 回看窗口

        Returns:
            回踩支撑因子
        """
        df_sorted = df.sort_values(['stock_code', 'date']).copy()

        # 找到最低价
        df_sorted['lowest_10'] = df_sorted.groupby('stock_code')['low'].transform(
            lambda x: x.rolling(lookback).min()
        )

        # 距离均线的距离
        df_sorted[f'ma_{ma_window}'] = df_sorted.groupby('stock_code')['close'].transform(
            lambda x: x.rolling(ma_window).mean()
        )

        # 回踩强度 = (最低价 / 均线 - 1) 的负值
        # 回踩越深（最低价越低），回踩强度越高
        df_sorted['bounce_strength'] = -(
            df_sorted['lowest_10'] / df_sorted[f'ma_{ma_window}'] - 1
        )

        # 标准化
        for stock in df_sorted['stock_code'].unique():
            mask = df_sorted['stock_code'] == stock
            values = df_sorted.loc[mask, 'bounce_strength']
            mean = values.mean()
            std = values.std()
            if std > 0:
                df_sorted.loc[mask, 'bounce_strength'] = (values - mean) / std

        return df_sorted['bounce_strength']

    @staticmethod
    def calculate_ma_cross_signal(df: pd.DataFrame, short_ma: int = 5, long_ma: int = 20) -> pd.Series:
        """
        计算均线金叉死叉因子

        Args:
            df: 原始数据
            short_ma: 短期均线
            long_ma: 长期均线

        Returns:
            均线交叉信号
        """
        df_sorted = df.sort_values(['stock_code', 'date']).copy()

        # 计算均线
        df_sorted[f'ma_{short_ma}'] = df_sorted.groupby('stock_code')['close'].transform(
            lambda x: x.rolling(short_ma).mean()
        )
        df_sorted[f'ma_{long_ma}'] = df_sorted.groupby('stock_code')['close'].transform(
            lambda x: x.rolling(long_ma).mean()
        )

        # 均线距离
        df_sorted['ma_distance'] = (
            df_sorted[f'ma_{short_ma}'] - df_sorted[f'ma_{long_ma}']
        ) / df_sorted[f'ma_{long_ma}']

        # 金叉死叉信号
        df_sorted['ma_cross_signal'] = df_sorted.groupby('stock_code')['ma_distance'].transform(
            lambda x: x.diff()
        )

        # 标准化
        for stock in df_sorted['stock_code'].unique():
            mask = df_sorted['stock_code'] == stock
            values = df_sorted.loc[mask, 'ma_cross_signal']
            mean = values.mean()
            std = values.std()
            if std > 0:
                df_sorted.loc[mask, 'ma_cross_signal'] = (values - mean) / std

        return df_sorted['ma_cross_signal']


def calculate_all_new_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算所有新因子

    Args:
        df: 原始数据

    Returns:
        包含新因子的DataFrame
    """
    print("开始计算新因子...")

    # 资金流向因子
    print("  计算资金流向因子...")
    df['large_order_ratio'] = ACashFlowFactorEngine.calculate_large_order_ratio(df)
    df['net_inflow'] = ACashFlowFactorEngine.calculate_net_inflow(df)
    df['institutional_pressure'] = ACashFlowFactorEngine.calculate_institutional_pressure(df)

    # 换手率因子
    print("  计算换手率因子...")
    df['turnover_surge'] = TurnoverFactorEngine.calculate_turnover_surge(df)
    df['price_volume_trend'] = TurnoverFactorEngine.calculate_price_volume_trend(df)
    df['cumulative_turnover_score'] = TurnoverFactorEngine.calculate_cumulative_turnover(df)

    # 技术形态因子
    print("  计算技术形态因子...")
    df['breakout_strength'] = TechnicalPatternEngine.calculate_breakout_strength(df)
    df['bounce_strength'] = TechnicalPatternEngine.calculate_bounce_strength(df)
    df['ma_cross_signal'] = TechnicalPatternEngine.calculate_ma_cross_signal(df)

    print("新因子计算完成！")
    print(f"新增因子数量: 9")

    return df


if __name__ == '__main__':
    # 测试代码
    import pickle

    print("加载测试数据...")
    with open('/Users/variya/.openclaw/workspace/projects/a-stock-advisor/data/real_stock_data.pkl', 'rb') as f:
        df = pickle.load(f)

    print(f"原始数据形状: {df.shape}")
    print()

    # 计算新因子
    df_with_new_factors = calculate_all_new_factors(df)

    print()
    print(f"增强后数据形状: {df_with_new_factors.shape}")
    print()
    print("新增因子示例:")
    print(df_with_new_factors[['stock_code', 'date', 'large_order_ratio', 'net_inflow', 'turnover_surge']].head(10))
