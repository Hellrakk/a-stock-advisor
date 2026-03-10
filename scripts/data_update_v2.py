#!/usr/bin/env python3
"""
数据更新脚本V3 - 使用真实数据源
任务：从AKShare获取真实A股数据
执行时机：每日开盘前（7:00）和收盘后（16:00）
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
        logging.FileHandler('logs/data_update_v3.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def fetch_real_stock_data_from_akshare(n_stocks=500, days=365):
    """从AKShare获取真实股票历史数据"""
    logger.info("="*60)
    logger.info("从AKShare获取真实A股数据")
    logger.info("="*60)
    
    try:
        import akshare as ak
        logger.info("✓ AKShare导入成功")
    except ImportError:
        logger.error("❌ AKShare未安装，请运行: pip install akshare")
        return None
    
    all_data = []
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y-%m-%d')
    
    try:
        logger.info("📥 获取A股股票列表...")
        stock_list = ak.stock_zh_a_spot_em()
        logger.info(f"✓ 获取到 {len(stock_list)} 只股票")
        
        stock_list = stock_list.head(n_stocks)
        
        for idx, row in stock_list.iterrows():
            stock_code = row['代码']
            stock_name = row['名称']
            
            try:
                logger.info(f"  [{idx+1}/{len(stock_list)}] 获取 {stock_name}({stock_code})...")
                
                df = ak.stock_zh_a_hist(
                    symbol=stock_code,
                    period="daily",
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', ''),
                    adjust="qfq"
                )
                
                if df is not None and len(df) > 0:
                    # AKShare返回12列：日期、股票代码、开盘、收盘、最高、最低、成交量、成交额、振幅、涨跌幅、涨跌额、换手率
                    df.columns = ['date', 'stock_code_api', 'open', 'close', 'high', 'low',
                                  'volume', 'amount', 'amplitude', 'change_pct', 'change_amount', 'turnover']

                    df['stock_code'] = stock_code
                    df['stock_name'] = stock_name
                    df['date'] = pd.to_datetime(df['date'])
                    
                    df = calculate_technical_factors(df)
                    
                    all_data.append(df)
                    logger.info(f"    ✓ 获取 {len(df)} 条记录")
                else:
                    logger.warning(f"    ⚠️ 无数据")
                    
            except Exception as e:
                logger.warning(f"    ⚠️ 获取失败: {e}")
                continue
        
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            logger.info(f"\n✓ 总计获取 {len(combined)} 条记录，{combined['stock_code'].nunique()} 只股票")
            return combined
        else:
            logger.error("❌ 未获取到任何数据")
            return None
            
    except Exception as e:
        logger.error(f"❌ 获取数据失败: {e}", exc_info=True)
        return None


def calculate_technical_factors(df):
    """计算技术因子"""
    df = df.sort_values('date').copy()
    
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    
    df['momentum_5'] = df['close'].pct_change(5)
    df['momentum_10'] = df['close'].pct_change(10)
    df['momentum_20'] = df['close'].pct_change(20)
    df['momentum_60'] = df['close'].pct_change(60)
    
    df['volatility_5'] = df['close'].pct_change().rolling(5).std()
    df['volatility_10'] = df['close'].pct_change().rolling(10).std()
    df['volatility_20'] = df['close'].pct_change().rolling(20).std()
    
    df['price_to_ma20'] = df['close'] / df['ma20'] - 1
    df['price_to_ma60'] = df['close'] / df['ma60'] - 1
    
    df['date_dt'] = df['date']
    df['month'] = df['date_dt'].dt.strftime('%Y-%m')
    
    return df


def main():
    """主函数"""
    logger.info("="*60)
    logger.info("数据更新任务开始 - 使用真实数据源")
    logger.info("="*60)
    
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    
    df = fetch_real_stock_data_from_akshare(n_stocks=500, days=365)
    
    if df is not None and len(df) > 0:
        output_file = 'data/akshare_real_data_fixed.pkl'
        
        with open(output_file, 'wb') as f:
            pickle.dump(df, f)
        
        logger.info(f"\n✓ 数据已保存至: {output_file}")
        logger.info(f"✓ 数据时间范围: {df['date'].min()} 至 {df['date'].max()}")
        logger.info(f"✓ 股票数量: {df['stock_code'].nunique()}")
        logger.info(f"✓ 总记录数: {len(df)}")
        
        metadata = {
            'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'source': 'akshare_real',
            'description': '从AKShare获取的真实A股数据',
            'stock_count': int(df['stock_code'].nunique()),
            'total_records': int(len(df)),
            'date_range': f"{df['date'].min()} to {df['date'].max()}"
        }
        
        with open(output_file.replace('.pkl', '_metadata.json'), 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logger.info("="*60)
        logger.info("✅ 数据更新任务完成")
        logger.info("="*60)
        return 0
    else:
        logger.error("❌ 数据更新失败")
        return 1


if __name__ == "__main__":
    sys.exit(main())
