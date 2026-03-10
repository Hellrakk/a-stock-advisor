#!/usr/bin/env python3
"""快速测试修复 - 只获取5只股票验证"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_update_v2 import fetch_real_stock_data_from_akshare
import logging

logging.basicConfig(level=logging.INFO)

print("="*60)
print("快速测试修复：只获取5只股票")
print("="*60)

df = fetch_real_stock_data_from_akshare(n_stocks=5, days=30)

if df is not None and len(df) > 0:
    print("\n✅ 数据获取成功！")
    print(f"  - 股票数量: {df['stock_code'].nunique()}")
    print(f"  - 总记录数: {len(df)}")
    print(f"  - 列数: {len(df.columns)}")
    print(f"  - 列名: {list(df.columns)}")
    print("\n数据示例:")
    print(df.head(3))
    print("\n✅ 修复验证通过！")
    sys.exit(0)
else:
    print("\n❌ 数据获取失败")
    sys.exit(1)
