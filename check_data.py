#!/usr/bin/env python3
"""
检查数据文件结构
"""

import pickle
import pandas as pd

# 加载数据文件
data_file = 'data/akshare_real_data_fixed.pkl'
print(f"Loading data from: {data_file}")

with open(data_file, 'rb') as f:
    data = pickle.load(f)

print(f"Data type: {type(data)}")

if isinstance(data, pd.DataFrame):
    print(f"Data shape: {data.shape}")
    print(f"Columns: {data.columns.tolist()}")
    print("\nSample data:")
    print(data.head())
    if 'date' in data.columns:
        print(f"\nDate range: {data['date'].min()} to {data['date'].max()}")
    if 'month' in data.columns:
        print(f"\nMonth range: {data['month'].min()} to {data['month'].max()}")
    if 'stock_code' in data.columns:
        print(f"\nUnique stocks: {data['stock_code'].nunique()}")
else:
    print("Data is not a DataFrame")
    print(f"Data keys: {list(data.keys()) if hasattr(data, 'keys') else 'N/A'}")
