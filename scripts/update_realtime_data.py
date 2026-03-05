#!/usr/bin/env python3
"""
实时数据更新脚本
任务：从AKShare获取最新的A股实时行情数据
执行时机：每日交易时间内定期执行
"""

import sys
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np
import pickle
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/update_realtime_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_realtime_data():
    """从AKShare获取实时行情数据"""
    logger.info("="*60)
    logger.info("获取A股实时行情数据")
    logger.info("="*60)
    
    try:
        import akshare as ak
        logger.info("✓ AKShare导入成功")
    except ImportError:
        logger.error("❌ AKShare未安装，请运行: pip install akshare")
        return None
    
    try:
        logger.info("📥 获取A股实时行情...")
        df = ak.stock_zh_a_spot_em()
        
        if df is not None and len(df) > 0:
            logger.info(f"✓ 获取到 {len(df)} 只股票的实时数据")
            
            # 数据清洗和标准化
            df.columns = ['stock_code', 'stock_name', 'close', 'change_pct', 'change_amount', 
                         'volume', 'amount', 'pre_close', 'open', 'high', 'low', 
                         'turnover', 'pe_ttm', 'pb', 'market_cap']
            
            # 转换数据类型
            numeric_cols = ['close', 'change_pct', 'change_amount', 'volume', 'amount', 
                          'pre_close', 'open', 'high', 'low', 'turnover', 'pe_ttm', 'pb', 'market_cap']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 添加日期时间信息
            df['trade_date'] = datetime.now().strftime('%Y-%m-%d')
            df['date_dt'] = pd.to_datetime(df['trade_date'])
            df['fetch_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return df
        else:
            logger.error("❌ 未获取到实时数据")
            return None
            
    except Exception as e:
        logger.error(f"❌ 获取实时数据失败: {e}", exc_info=True)
        return None

def main():
    """主函数"""
    logger.info("="*60)
    logger.info("实时数据更新任务开始")
    logger.info("="*60)
    
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    
    df = fetch_realtime_data()
    
    if df is not None and len(df) > 0:
        output_file = 'data/latest_realtime_data.pkl'
        
        with open(output_file, 'wb') as f:
            pickle.dump(df, f)
        
        logger.info(f"\n✓ 实时数据已保存至: {output_file}")
        logger.info(f"✓ 股票数量: {len(df)}")
        logger.info(f"✓ 数据获取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        metadata = {
            'source': 'akshare',
            'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data_type': 'realtime',
            'total_stocks': len(df),
            'fetch_method': 'stock_zh_a_spot',
            'processed_stocks': len(df),
            'trade_date': datetime.now().strftime('%Y-%m-%d'),
            'file': output_file,
            'records': len(df),
            'columns': list(df.columns)
        }
        
        with open('data/latest_realtime_data_metadata.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logger.info("="*60)
        logger.info("✅ 实时数据更新任务完成")
        logger.info("="*60)
        return 0
    else:
        logger.error("❌ 实时数据更新失败")
        return 1


if __name__ == "__main__":
    sys.exit(main())
