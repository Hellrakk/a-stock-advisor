#!/usr/bin/env python3
"""
修复股票名称 - 将模拟名称替换为真实名称
"""

import pickle
import pandas as pd
import json
import os
from pathlib import Path

def fix_stock_names():
    """修复股票名称"""
    print("=" * 60)
    print("🔧 修复股票名称")
    print("=" * 60)
    
    # 加载股票名称映射
    mapping_file = Path(__file__).parent.parent / 'data' / 'stock_name_mapping.json'
    with open(mapping_file, 'r', encoding='utf-8') as f:
        stock_name_mapping = json.load(f)
    
    print(f"✓ 加载了 {len(stock_name_mapping)} 个股票名称映射")
    
    # 加载数据文件
    data_file = Path(__file__).parent.parent / 'data' / 'akshare_real_data_fixed.pkl'
    print(f"\n📥 加载数据文件: {data_file}")
    
    with open(data_file, 'rb') as f:
        df = pickle.load(f)
    
    print(f"✓ 数据形状: {df.shape}")
    print(f"✓ 列名: {df.columns.tolist()[:10]}")
    
    # 统计修复前的股票名称
    print("\n📊 修复前的股票名称样例:")
    print(df['stock_name'].head(10))
    
    # 修复股票名称
    print("\n🔧 开始修复股票名称...")
    fixed_count = 0
    
    def get_real_name(row):
        nonlocal fixed_count
        stock_code = str(row['stock_code'])
        # 移除前缀（如果有）
        code = stock_code.replace('sh', '').replace('sz', '').replace('bj', '')
        
        if code in stock_name_mapping:
            fixed_count += 1
            return stock_name_mapping[code]
        else:
            # 如果没有映射，保留原名称
            return row['stock_name']
    
    df['stock_name'] = df.apply(get_real_name, axis=1)
    
    print(f"✓ 修复了 {fixed_count} 个股票名称")
    
    # 统计修复后的股票名称
    print("\n📊 修复后的股票名称样例:")
    print(df['stock_name'].head(10))
    
    # 保存修复后的数据
    backup_file = data_file.parent / 'akshare_real_data_fixed_backup.pkl'
    print(f"\n💾 备份原数据到: {backup_file}")
    
    # 如果备份文件不存在，创建备份
    if not backup_file.exists():
        import shutil
        shutil.copy(data_file, backup_file)
        print("✓ 备份完成")
    else:
        print("⚠️ 备份文件已存在，跳过备份")
    
    # 保存修复后的数据
    print(f"\n💾 保存修复后的数据到: {data_file}")
    with open(data_file, 'wb') as f:
        pickle.dump(df, f)
    
    print("✓ 保存完成")
    
    # 验证修复结果
    print("\n🔍 验证修复结果...")
    with open(data_file, 'rb') as f:
        df_verify = pickle.load(f)
    
    print(f"✓ 验证数据形状: {df_verify.shape}")
    print(f"✓ 验证股票名称样例:")
    print(df_verify['stock_name'].head(10))
    
    print("\n" + "=" * 60)
    print("✅ 股票名称修复完成")
    print("=" * 60)

if __name__ == '__main__':
    fix_stock_names()
