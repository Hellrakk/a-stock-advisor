#!/usr/bin/env python3
"""
修复数据源问题 - 下载真实A股历史数据
解决因子有效性极低问题的根本方案
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import os
import sys
from tqdm import tqdm

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def validate_data(data):
    """
    验证数据真实性

    Args:
        data: 股票数据DataFrame

    Returns:
        dict: 验证结果
    """
    print("\n=== 数据真实性验证 ===")

    results = {
        'is_valid': True,
        'issues': []
    }

    # 检查1: 时间戳
    if '日期' in data.columns or 'date' in data.columns:
        date_col = '日期' if '日期' in data.columns else 'date'
        unique_times = data[date_col].dt.strftime('%H:%M').unique()
        print(f"时间戳分布: {unique_times[:5]}")

        if len(unique_times) == 1 and '15:00' not in str(unique_times[0]):
            results['is_valid'] = False
            results['issues'].append(f"时间戳异常: {unique_times[0]} (期望15:00)")

    # 检查2: 日期范围（不包含未来日期）
    today = pd.Timestamp.now()
    max_date = data[date_col].max()

    if max_date > today:
        results['is_valid'] = False
        results['issues'].append(f"包含未来日期: {max_date} (当前: {today})")

    # 检查3: 价格末位数字分布
    if '收盘' in data.columns or 'close' in data.columns:
        price_col = '收盘' if '收盘' in data.columns else 'close'
        close_last_digit = (data[price_col] * 100).astype(int) % 10
        digit_counts = close_last_digit.value_counts().sort_index()

        # 检查是否过于均匀（模拟数据特征）
        expected_count = len(data) / 10
        uniformity = (digit_counts - expected_count).abs().mean() / expected_count

        print(f"价格末位数字均匀性指数: {uniformity:.4f} (越接近0越均匀)")
        if uniformity < 0.05:
            results['issues'].append("价格末尾数字分布过于均匀（可能是模拟数据）")

    # 检查4: 股票间相关性（需要多股票数据）
    print("提示: 需要多股票数据才能验证股票间相关性")

    return results


def download_real_stock_data(stock_codes, start_date, end_date,
                             output_file='data/real_stock_data_fixed.pkl',
                             save_progress=True):
    """
    下载真实A股历史数据

    Args:
        stock_codes: 股票代码列表
        start_date: 开始日期 (格式: YYYYMMDD)
        end_date: 结束日期 (格式: YYYYMMDD)
        output_file: 输出文件路径
        save_progress: 是否保存进度
    """
    print(f"\n=== 开始下载真实A股数据 ===")
    print(f"股票数量: {len(stock_codes)}")
    print(f"日期范围: {start_date} 到 {end_date}")
    print(f"输出文件: {output_file}")

    # 创建输出目录
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # 进度文件
    progress_file = output_file.replace('.pkl', '_progress.json')
    failed_file = output_file.replace('.pkl', '_failed.json')

    # 加载进度
    if save_progress and os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            progress = json.load(f)
        completed_stocks = set(progress.get('completed', []))
        print(f"找到进度文件，已完成 {len(completed_stocks)} 只股票")
    else:
        completed_stocks = set()

    # 加载失败记录
    if os.path.exists(failed_file):
        with open(failed_file, 'r') as f:
            failed_stocks = json.load(f)
        print(f"之前失败的股票: {len(failed_stocks)}")
    else:
        failed_stocks = []

    # 过滤已完成的股票
    pending_stocks = [s for s in stock_codes if s not in completed_stocks]

    if not pending_stocks:
        print("所有股票已下载完成！")
        if os.path.exists(output_file):
            return pd.read_pickle(output_file)
        return None

    print(f"待下载: {len(pending_stocks)} 只股票")

    # 下载数据
    all_data = []

    for i, stock_code in enumerate(tqdm(pending_stocks, desc="下载进度")):
        try:
            # 使用akshare下载
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )

            if df is not None and len(df) > 0:
                df['stock_code'] = stock_code
                all_data.append(df)

                # 更新进度
                completed_stocks.add(stock_code)
                if save_progress and i % 10 == 0:
                    with open(progress_file, 'w') as f:
                        json.dump({
                            'completed': list(completed_stocks),
                            'timestamp': datetime.now().isoformat()
                        }, f, ensure_ascii=False)

        except Exception as e:
            print(f"\n下载 {stock_code} 失败: {e}")
            failed_stocks.append({
                'stock_code': stock_code,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })

        # 避免请求过快
        time.sleep(0.5)

    # 保存失败记录
    if failed_stocks:
        with open(failed_file, 'w') as f:
            json.dump(failed_stocks, f, ensure_ascii=False, indent=2)

    # 合并数据
    if all_data:
        final_data = pd.concat(all_data, ignore_index=True)

        # 保存
        final_data.to_pickle(output_file)
        print(f"\n✓ 数据已保存到: {output_file}")
        print(f"  总数据量: {len(final_data):,} 条")
        print(f"  股票数量: {final_data['stock_code'].nunique()}")
        print(f"  日期范围: {final_data['日期'].min()} 到 {final_data['日期'].max()}")

        # 验证数据
        validation = validate_data(final_data)

        if validation['is_valid']:
            print(f"\n✓ 数据验证通过！")
        else:
            print(f"\n⚠️ 数据验证失败，问题:")
            for issue in validation['issues']:
                print(f"  - {issue}")

        # 更新完成标记
        if save_progress:
            with open(progress_file, 'w') as f:
                json.dump({
                    'completed': list(completed_stocks),
                    'timestamp': datetime.now().isoformat(),
                    'status': 'completed'
                }, f, ensure_ascii=False)

        return final_data
    else:
        print("\n✗ 没有下载到任何数据！")
        return None


def get_stock_list():
    """
    获取A股股票列表

    Returns:
        list: 股票代码列表
    """
    print("=== 获取A股股票列表 ===")

    try:
        # 方法1: 获取沪深300成分股
        hs300 = ak.index_stock_cons(symbol="000300")
        print(f"沪深300成分股: {len(hs300)} 只")

        # 方法2: 获取中证500成分股
        zz500 = ak.index_stock_cons(symbol="000905")
        print(f"中证500成分股: {len(zz500)} 只")

        # 合并去重
        all_stocks = pd.concat([hs300, zz500]).drop_duplicates()
        stock_codes = all_stocks['品种代码'].tolist()

        print(f"合计: {len(stock_codes)} 只股票")
        return stock_codes

    except Exception as e:
        print(f"获取股票列表失败: {e}")

        # 备用方案：手动指定一些知名股票
        backup_stocks = [
            '000001', '000002', '000725', '600000', '600036',
            '600519', '601318', '601398', '601939', '603259',
            '000063', '000333', '000651', '000858', '002001'
        ]
        print(f"使用备用股票列表: {len(backup_stocks)} 只")
        return backup_stocks


def compute_factors(data, output_file='data/real_stock_data_with_factors.pkl'):
    """
    计算技术因子

    Args:
        data: 股票数据
        output_file: 输出文件路径
    """
    print("\n=== 计算技术因子 ===")

    from code.alpha_factory.factor_generator import FactorGenerator

    # 创建因子生成器
    factory = FactorGenerator()

    # 计算因子
    data_with_factors = factory.generate_all_factors(data)

    # 保存
    data_with_factors.to_pickle(output_file)
    print(f"✓ 因子已计算并保存到: {output_file}")

    return data_with_factors


def main():
    """主函数"""
    print("=" * 60)
    print("修复数据源问题 - 下载真实A股历史数据")
    print("=" * 60)

    # 步骤1: 获取股票列表
    stock_codes = get_stock_list()

    if not stock_codes:
        print("✗ 无法获取股票列表！")
        return

    # 步骤2: 下载真实数据
    # 下载过去2年的数据
    start_date = "20230101"
    end_date = datetime.now().strftime('%Y%m%d')

    output_file = 'data/real_stock_data_fixed.pkl'
    data = download_real_stock_data(
        stock_codes=stock_codes,
        start_date=start_date,
        end_date=end_date,
        output_file=output_file
    )

    if data is None:
        print("✗ 数据下载失败！")
        return

    # 步骤3: 计算因子
    factor_output = 'data/real_stock_data_with_factors.pkl'
    try:
        compute_factors(data, factor_output)
    except ImportError:
        print("⚠️ 因子计算模块未找到，跳过因子计算")
        print("提示: 请运行 'python data/clean_factor.py' 来计算因子")

    print("\n" + "=" * 60)
    print("✓ 数据下载完成！")
    print("=" * 60)
    print("\n下一步:")
    print("1. 验证数据真实性（已自动运行）")
    print("2. 重新计算因子（如未自动计算）")
    print("3. 评估因子有效性（IC值应>0.02）")
    print("4. 尝试选股策略")


if __name__ == "__main__":
    main()
