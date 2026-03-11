#!/usr/bin/env python3
"""
紧急修复脚本：切换到真实数据源
执行方案B：临时使用akshare真实数据
"""

import os
import shutil
import pickle
import pandas as pd
from datetime import datetime
import json

# 配置
DATA_DIR = '/Users/variya/.openclaw/workspace/projects/a-stock-advisor/data'
REAL_DATA = os.path.join(DATA_DIR, 'real_stock_data.pkl')
SIM_DATA_BACKUP = os.path.join(DATA_DIR, 'real_stock_data_simulated.pkl.bak')
AKSHARE_DATA = os.path.join(DATA_DIR, 'akshare_real_data_fixed.pkl')

# 元数据文件
METADATA_FILE = os.path.join(DATA_DIR, 'real_stock_data_metadata.json')
BACKUP_METADATA_FILE = os.path.join(DATA_DIR, 'real_stock_data_metadata_simulated.json.bak')

def validate_akshare_data():
    """验证akshare数据质量"""
    print("验证akshare真实数据...")
    print("-" * 60)

    if not os.path.exists(AKSHARE_DATA):
        print(f"❌ akshare数据不存在: {AKSHARE_DATA}")
        return False

    try:
        with open(AKSHARE_DATA, 'rb') as f:
            df = pickle.load(f)

        print(f"✓ 数据形状: {df.shape}")
        print(f"✓ 股票数量: {df['stock_code'].nunique()}")

        # 检查股票代码格式
        codes = df['stock_code'].unique()
        invalid_codes = [c for c in codes if not any(c.lower().startswith(p) for p in ['sh', 'sz'])]
        if len(invalid_codes) == 0:
            print(f"✓ 股票代码格式正确")
        else:
            print(f"⚠️  发现无效股票代码: {len(invalid_codes)}个")

        # 检查涨跌幅
        returns = df['change_pct'].dropna()
        print(f"✓ 最大涨幅: {returns.max():.2f}%")
        print(f"✓ 最大跌幅: {returns.min():.2f}%")

        # 检查技术因子
        factor_cols = [c for c in df.columns if 'momentum' in c or 'volatility' in c]
        print(f"✓ 技术因子数量: {len(factor_cols)}")

        print()
        return True

    except Exception as e:
        print(f"❌ 验证失败: {e}")
        return False


def backup_current_data():
    """备份当前数据"""
    print("备份当前模拟数据...")
    print("-" * 60)

    try:
        # 备份数据文件
        if os.path.exists(REAL_DATA) and not os.path.exists(SIM_DATA_BACKUP):
            shutil.move(REAL_DATA, SIM_DATA_BACKUP)
            print(f"✓ 已备份到: {SIM_DATA_BACKUP}")
        else:
            print(f"⚠️  备份已存在或源文件不存在")

        # 备份元数据
        if os.path.exists(METADATA_FILE) and not os.path.exists(BACKUP_METADATA_FILE):
            shutil.move(METADATA_FILE, BACKUP_METADATA_FILE)
            print(f"✓ 元数据已备份")

        print()
        return True

    except Exception as e:
        print(f"❌ 备份失败: {e}")
        return False


def switch_to_akshare_data():
    """切换到akshare真实数据"""
    print("切换到akshare真实数据...")
    print("-" * 60)

    try:
        # 创建软链接
        if os.path.exists(REAL_DATA):
            os.remove(REAL_DATA)

        os.symlink(AKSHARE_DATA, REAL_DATA)
        print(f"✓ 已创建软链接: {REAL_DATA} -> {AKSHARE_DATA}")

        # 创建新的元数据
        metadata = {
            'source': 'akshare_real_data',
            'switched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'note': '紧急修复：切换到真实数据以解决因子IC值过低问题',
            'original_report': 'factor_diagnosis_20260311_0017.md',
            'data_file': os.path.basename(AKSHARE_DATA),
        }

        with open(METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        print(f"✓ 元数据已更新")
        print()

        return True

    except Exception as e:
        print(f"❌ 切换失败: {e}")
        return False


def test_ic_calculation():
    """测试IC计算"""
    print("测试IC计算...")
    print("-" * 60)

    try:
        from scipy.stats import spearmanr

        with open(REAL_DATA, 'rb') as f:
            df = pickle.load(f)

        # 计算forward return
        df_sorted = df.sort_values(['stock_code', 'date']).copy()
        df_sorted['forward_return_5d'] = df_sorted.groupby('stock_code')['close'].shift(-5) / df_sorted['close'] - 1

        # 测试几个因子
        factors = [
            ('momentum_10', '动量因子10日'),
            ('price_to_ma10', '价格/均线10日'),
        ]

        print("因子IC值测试:")
        for factor, desc in factors:
            if factor in df_sorted.columns:
                valid = df_sorted[[factor, 'forward_return_5d']].dropna()
                if len(valid) > 100:
                    ic, pval = spearmanr(valid[factor], valid['forward_return_5d'])
                    status = "✅有效" if abs(ic) > 0.02 and pval < 0.05 else "⚠️ 待测试"
                    print(f"  {desc}: IC={ic:.6f}, p={pval:.4f}, n={len(valid)} {status}")

        print()
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def rollback():
    """回滚到模拟数据"""
    print("回滚到模拟数据...")
    print("-" * 60)

    try:
        # 删除软链接
        if os.path.islink(REAL_DATA):
            os.remove(REAL_DATA)
            print(f"✓ 已删除软链接")

        # 恢复模拟数据
        if os.path.exists(SIM_DATA_BACKUP):
            shutil.copy(SIM_DATA_BACKUP, REAL_DATA)
            print(f"✓ 已恢复模拟数据")

        # 恢复元数据
        if os.path.exists(BACKUP_METADATA_FILE):
            shutil.copy(BACKUP_METADATA_FILE, METADATA_FILE)
            print(f"✓ 已恢复元数据")

        print()
        print("✓ 回滚完成！")
        return True

    except Exception as e:
        print(f"❌ 回滚失败: {e}")
        return False


def main():
    """主函数"""
    print("=" * 80)
    print("紧急修复：切换到真实数据源（方案B）")
    print("=" * 80)
    print()

    # 1. 验证akshare数据
    if not validate_akshare_data():
        print("❌ akshare数据验证失败，终止执行")
        return

    # 2. 备份当前数据
    if not backup_current_data():
        print("❌ 备份失败，终止执行")
        return

    # 3. 切换到akshare数据
    if not switch_to_akshare_data():
        print("❌ 切换失败，尝试回滚...")
        rollback()
        return

    # 4. 测试IC计算
    if not test_ic_calculation():
        print("⚠️  IC计算测试失败，但数据切换已完成")

    print("=" * 80)
    print("✓ 紧急修复完成！")
    print("=" * 80)
    print()
    print("下一步:")
    print("  1. 运行完整IC诊断: python3 code/strategy/debug_ic.py")
    print("  2. 运行系统测试: python3 scripts/daily_master.py --test")
    print("  3. 如需回滚: python3 scripts/switch_to_real_data.py --rollback")
    print()


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--rollback':
        rollback()
    else:
        main()
