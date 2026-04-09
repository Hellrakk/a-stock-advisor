#!/usr/bin/env python3
"""
数据更新脚本V3 - 使用真实数据源
任务：从AKShare获取真实A股数据
执行时机：每日开盘前（7:00）和收盘后（16:00）
"""

import sys
import os
import argparse
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np
import pickle
import json

log_dir = project_root / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'data_update_v3.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def _first_existing_column(df, candidates):
    for column in candidates:
        if column in df.columns:
            return column
    return None


def _load_stock_list(ak_module, limit):
    sources = (
        ("stock_info_a_code_name", ("code", "symbol", "\u4ee3\u7801"), ("name", "\u540d\u79f0")),
        ("stock_zh_a_spot", ("\u4ee3\u7801", "code", "symbol"), ("\u540d\u79f0", "name")),
        ("stock_zh_a_spot_em", ("\u4ee3\u7801", "code", "symbol"), ("\u540d\u79f0", "name")),
    )

    errors = []
    for method_name, code_candidates, name_candidates in sources:
        method = getattr(ak_module, method_name, None)
        if method is None:
            continue

        try:
            stock_list = method()
            code_column = _first_existing_column(stock_list, code_candidates)
            name_column = _first_existing_column(stock_list, name_candidates)
            if code_column is None:
                raise KeyError(f"{method_name} missing stock code column: {list(stock_list.columns)}")

            normalized = stock_list.rename(columns={code_column: "stock_code"}).copy()
            if name_column is not None:
                normalized = normalized.rename(columns={name_column: "stock_name"})
            else:
                normalized["stock_name"] = normalized["stock_code"]

            normalized["stock_code"] = normalized["stock_code"].astype(str).str.zfill(6)
            normalized["stock_name"] = normalized["stock_name"].fillna(normalized["stock_code"]).astype(str)
            normalized = normalized[["stock_code", "stock_name"]].dropna(subset=["stock_code"]).drop_duplicates("stock_code")
            return normalized.head(limit), method_name
        except Exception as exc:
            errors.append(f"{method_name}: {exc}")

    raise RuntimeError(" ; ".join(errors))


def _to_bs_code(stock_code):
    stock_code = str(stock_code).zfill(6)
    return f"sh.{stock_code}" if stock_code.startswith("6") else f"sz.{stock_code}"


def _fetch_history_from_baostock(baostock_module, stock_code, start_date, end_date):
    rs = baostock_module.query_history_k_data_plus(
        _to_bs_code(stock_code),
        "date,code,open,high,low,close,volume,amount,turn",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="2",
    )
    if rs.error_code != "0":
        raise RuntimeError(f"baostock query failed: {rs.error_msg}")

    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())

    if not rows:
        return None

    df = pd.DataFrame(
        rows,
        columns=["date", "stock_code_api", "open", "high", "low", "close", "volume", "amount", "turnover"],
    )
    for column in ["open", "high", "low", "close", "volume", "amount", "turnover"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    prev_close = df["close"].shift(1)
    df["amplitude"] = ((df["high"] - df["low"]) / prev_close.replace(0, np.nan)) * 100
    df["change_pct"] = df["close"].pct_change() * 100
    df["change_amount"] = df["close"].diff()
    return df[["date", "stock_code_api", "open", "close", "high", "low", "volume", "amount", "amplitude", "change_pct", "change_amount", "turnover"]]


def _fetch_history_with_fallback(ak_module, baostock_module, stock_code, start_date, end_date):
    errors = []

    try:
        df = ak_module.stock_zh_a_hist(
            symbol=stock_code,
            period="daily",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust="qfq",
        )
        if df is not None and len(df) > 0:
            df.columns = ["date", "stock_code_api", "open", "close", "high", "low", "volume", "amount", "amplitude", "change_pct", "change_amount", "turnover"]
            return df, "akshare"
    except Exception as exc:
        errors.append(f"akshare: {exc}")

    if baostock_module is not None:
        try:
            df = _fetch_history_from_baostock(baostock_module, stock_code, start_date, end_date)
            if df is not None and len(df) > 0:
                return df, "baostock"
        except Exception as exc:
            errors.append(f"baostock: {exc}")

    raise RuntimeError(" ; ".join(errors) if errors else "no history source returned data")


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
    baostock_module = None
    baostock_logged_in = False

    try:
        import baostock as bs
        login_result = bs.login()
        if login_result.error_code == "0":
            baostock_module = bs
            baostock_logged_in = True
            logger.info("✓ Baostock login success")
        else:
            logger.warning("Baostock login failed: %s", login_result.error_msg)
    except Exception as exc:
        logger.warning("Baostock unavailable: %s", exc)
    
    try:
        logger.info("📥 获取A股股票列表...")
        stock_list, source_name = _load_stock_list(ak, n_stocks)
        logger.info("Stock list source: %s", source_name)
        logger.info(f"✓ 获取到 {len(stock_list)} 只股票")
        
        for idx, row in stock_list.iterrows():
            stock_code = row["stock_code"]
            stock_name = row["stock_name"]
            
            try:
                logger.info(f"  [{idx+1}/{len(stock_list)}] 获取 {stock_name}({stock_code})...")

                df, history_source = _fetch_history_with_fallback(
                    ak_module=ak,
                    baostock_module=baostock_module,
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                )

                if df is not None and len(df) > 0:
                    df['stock_code'] = stock_code
                    df['stock_name'] = stock_name
                    df['date'] = pd.to_datetime(df['date'])
                    
                    df = calculate_technical_factors(df)
                    
                    all_data.append(df)
                    logger.info("    ✓ 获取 %s 条记录 (source=%s)", len(df), history_source)
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
    finally:
        if baostock_logged_in and baostock_module is not None:
            try:
                baostock_module.logout()
            except Exception:
                pass


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
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock-count", type=int, default=500)
    parser.add_argument("--days", type=int, default=365)
    args = parser.parse_args()

    logger.info("="*60)
    logger.info("数据更新任务开始 - 使用真实数据源")
    logger.info("="*60)
    
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    
    df = fetch_real_stock_data_from_akshare(n_stocks=args.stock_count, days=args.days)
    
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
