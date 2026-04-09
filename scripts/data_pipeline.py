#!/usr/bin/env python3
"""
数据与因子处理管道
整合以下功能：
1. 数据下载与更新 (fix_data_source.py)
2. 数据合并与基础处理 (merge_and_process_data.py)
3. 高级因子计算 (build_better_factors.py)
4. 因子中性化 (neutralize_factors.py)

使用方式：
  python scripts/data_pipeline.py --download    # 下载数据
  python scripts/data_pipeline.py --process     # 处理数据
  python scripts/data_pipeline.py --factors     # 计算因子
  python scripts/data_pipeline.py --all         # 完整流程
"""

import argparse
import os
import sys
import json
import pickle
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    print("⚠️ AKShare未安装，数据下载功能不可用")


def _first_existing_column(df, candidates):
    for column in candidates:
        if column in df.columns:
            return column
    return None


def _load_stock_codes(ak_module, limit):
    sources = (
        ("stock_info_a_code_name", ("code", "symbol", "\u4ee3\u7801")),
        ("stock_zh_a_spot", ("\u4ee3\u7801", "code", "symbol")),
        ("stock_zh_a_spot_em", ("\u4ee3\u7801", "code", "symbol")),
    )

    errors = []
    for method_name, code_candidates in sources:
        method = getattr(ak_module, method_name, None)
        if method is None:
            continue

        try:
            stock_list = method()
            code_column = _first_existing_column(stock_list, code_candidates)
            if code_column is None:
                raise KeyError(f"{method_name} missing stock code column: {list(stock_list.columns)}")

            stock_codes = (
                stock_list[code_column]
                .astype(str)
                .str.zfill(6)
                .dropna()
                .drop_duplicates()
                .head(limit)
                .tolist()
            )
            return stock_codes, method_name
        except Exception as exc:
            errors.append(f"{method_name}: {exc}")

    raise RuntimeError(" ; ".join(errors))


def _to_bs_code(stock_code):
    stock_code = str(stock_code).zfill(6)
    return f"sh.{stock_code}" if stock_code.startswith("6") else f"sz.{stock_code}"


def _fetch_history_from_baostock(baostock_module, stock_code, start_date, end_date):
    rs = baostock_module.query_history_k_data_plus(
        _to_bs_code(stock_code),
        "date,open,high,low,close,volume,amount,turn",
        start_date=f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}",
        end_date=f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}",
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

    df = pd.DataFrame(rows, columns=["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额", "换手率"])
    for column in ["开盘", "最高", "最低", "收盘", "成交量", "成交额", "换手率"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    prev_close = df["收盘"].shift(1)
    df["振幅"] = ((df["最高"] - df["最低"]) / prev_close.replace(0, np.nan)) * 100
    df["涨跌幅"] = df["收盘"].pct_change() * 100
    return df[["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "换手率"]]


def _fetch_history_frame(ak_module, baostock_module, stock_code, start_date, end_date):
    errors = []

    try:
        df = ak_module.stock_zh_a_hist(
            symbol=stock_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
        if df is not None and len(df) > 0:
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


class DataPipeline:
    """数据处理管道"""
    
    def __init__(self):
        self.data_file = 'data/akshare_real_data_fixed.pkl'
        self.progress_file = 'data/download_progress.json'
        self.metadata_file = 'data/data_metadata.json'
        
    def download_data(self, 
                      stock_count: int = 500,
                      start_date: str = '20230101',
                      end_date: str = None) -> bool:
        """
        下载A股历史数据
        
        Args:
            stock_count: 下载股票数量
            start_date: 开始日期
            end_date: 结束日期
        """
        if not AKSHARE_AVAILABLE:
            print("❌ AKShare未安装，无法下载数据")
            return False
        
        print("\n" + "="*60)
        print("📥 数据下载模块")
        print("="*60)
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        print(f"股票数量: {stock_count}")
        print(f"日期范围: {start_date} 到 {end_date}")
        
        # 获取股票列表
        try:
            stock_codes, source_name = _load_stock_codes(ak, stock_count)
            print(f"股票列表来源: {source_name}")
            print(f"✓ 获取到 {len(stock_codes)} 只股票")
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return False

        baostock_module = None
        baostock_logged_in = False
        try:
            import baostock as bs
            login_result = bs.login()
            if login_result.error_code == "0":
                baostock_module = bs
                baostock_logged_in = True
                print("✓ Baostock 登录成功")
            else:
                print(f"⚠️ Baostock 登录失败: {login_result.error_msg}")
        except Exception as e:
            print(f"⚠️ Baostock 不可用: {e}")
        
        # 加载进度
        completed = set()
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r') as f:
                progress = json.load(f)
                completed = set(progress.get('completed', []))
                print(f"✓ 已完成 {len(completed)} 只股票")
        
        pending = [s for s in stock_codes if s not in completed]
        print(f"待下载: {len(pending)} 只股票")
        
        if not pending:
            print("✓ 所有股票已下载完成")
            return True
        
        # 下载数据
        all_data = []
        try:
            for i, code in enumerate(pending):
                try:
                    df, history_source = _fetch_history_frame(
                        ak_module=ak,
                        baostock_module=baostock_module,
                        stock_code=code,
                        start_date=start_date,
                        end_date=end_date,
                    )

                    if df is not None and len(df) > 0:
                        df['stock_code'] = code
                        all_data.append(df)
                        completed.add(code)
                        print(f"已获取 {code}: {len(df)} 条 (source={history_source})")

                    # 保存进度
                    if (i + 1) % 20 == 0:
                        self._save_progress(completed)
                        print(f"进度: {i+1}/{len(pending)} ({(i+1)/len(pending)*100:.1f}%)")

                    time.sleep(0.3)

                except Exception as e:
                    print(f"下载 {code} 失败: {e}")
        finally:
            if baostock_logged_in and baostock_module is not None:
                try:
                    baostock_module.logout()
                except Exception:
                    pass
        
        # 合并并保存
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            
            # 合并已有数据
            if os.path.exists(self.data_file):
                with open(self.data_file, 'rb') as f:
                    existing_df = pickle.load(f)
                existing_is_raw = '日期' in existing_df.columns and 'date' not in existing_df.columns
                if existing_is_raw:
                    final_df = pd.concat([existing_df, final_df], ignore_index=True)
                    final_df = final_df.drop_duplicates(subset=['日期', 'stock_code'])
                else:
                    print("⚠️ 现有数据文件已是处理后结构，下载阶段不直接合并，避免污染主数据文件")
            
            with open(self.data_file, 'wb') as f:
                pickle.dump(final_df, f)

            print(f"\n✓ 数据已保存: {len(final_df)} 条记录, {final_df['stock_code'].nunique()} 只股票")

            self._save_progress(completed, status='completed')
            return True

        print("❌ 本轮没有下载到任何有效数据")
        self._save_progress(completed, status='failed')
        return False
    
    def process_data(self) -> pd.DataFrame:
        """处理数据：标准化列名、计算基础因子"""
        print("\n" + "="*60)
        print("🔧 数据处理模块")
        print("="*60)
        
        if not os.path.exists(self.data_file):
            print("❌ 数据文件不存在，请先下载数据")
            return None
        
        with open(self.data_file, 'rb') as f:
            df = pickle.load(f)
        
        print(f"✓ 加载数据: {len(df)} 条记录")
        
        # 标准化列名
        column_mapping = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'change_pct',
            '换手率': 'turnover'
        }
        df = df.rename(columns=column_mapping)
        
        # 确保date是datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        # 排序
        df = df.sort_values(['stock_code', 'date'])
        
        # 计算基础因子
        print("计算基础因子...")
        all_data = []
        for stock_code, group in df.groupby('stock_code'):
            group = group.sort_values('date').copy()
            
            returns = group['close'].pct_change()
            
            # 移动平均
            group['ma5'] = group['close'].rolling(5).mean()
            group['ma10'] = group['close'].rolling(10).mean()
            group['ma20'] = group['close'].rolling(20).mean()
            group['ma60'] = group['close'].rolling(60).mean()
            
            # 动量
            group['momentum_5'] = group['close'].pct_change(5)
            group['momentum_10'] = group['close'].pct_change(10)
            group['momentum_20'] = group['close'].pct_change(20)
            group['momentum_60'] = group['close'].pct_change(60)
            
            # 波动率
            group['volatility_5'] = returns.rolling(5).std()
            group['volatility_10'] = returns.rolling(10).std()
            group['volatility_20'] = returns.rolling(20).std()
            
            # 价格相对位置
            group['price_to_ma20'] = group['close'] / group['ma20'] - 1
            group['price_to_ma60'] = group['close'] / group['ma60'] - 1
            
            all_data.append(group)
        
        df = pd.concat(all_data, ignore_index=True)
        
        # 删除NaN过多的行
        df = df.dropna(subset=['ma20', 'momentum_20'])
        
        # 保存
        with open(self.data_file, 'wb') as f:
            pickle.dump(df, f)
        
        # 保存元数据
        metadata = {
            'process_time': datetime.now().isoformat(),
            'stock_count': int(df['stock_code'].nunique()),
            'record_count': int(len(df)),
            'date_range': f"{df['date'].min()} to {df['date'].max()}",
            'columns': list(df.columns)
        }
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"✓ 处理完成: {len(df)} 条记录, {df['stock_code'].nunique()} 只股票")
        return df
    
    def calculate_advanced_factors(self) -> pd.DataFrame:
        """计算高级因子"""
        print("\n" + "="*60)
        print("📊 高级因子计算模块")
        print("="*60)
        
        if not os.path.exists(self.data_file):
            print("❌ 数据文件不存在")
            return None
        
        with open(self.data_file, 'rb') as f:
            df = pickle.load(f)
        
        print(f"✓ 加载数据: {len(df)} 条记录")
        
        df = df.sort_values(['stock_code', 'date']).copy()
        
        all_data = []
        for stock_code, group in df.groupby('stock_code'):
            group = group.sort_values('date').copy()
            returns = group['close'].pct_change()
            
            # 改进的动量因子
            group['momentum_accel'] = group['close'].pct_change(5) - group['close'].pct_change(20)
            
            # 下行波动率
            down_ret = returns.copy()
            down_ret[down_ret > 0] = 0
            group['downside_vol'] = down_ret.rolling(20).std()
            
            # 波动率比率
            vol_5 = returns.rolling(5).std()
            vol_20 = returns.rolling(20).std()
            group['vol_ratio'] = vol_5 / (vol_20 + 1e-10)
            
            # Amihud非流动性
            group['amihud'] = (abs(returns) / (group['amount'] + 1e-10)).rolling(20).mean()
            
            # 换手率相对强度
            group['turnover_ma20'] = group['turnover'].rolling(20).mean()
            group['turnover_ratio'] = group['turnover'] / (group['turnover_ma20'] + 1e-10)
            
            # 价格位置
            high_20 = group['high'].rolling(20).max()
            low_20 = group['low'].rolling(20).min()
            group['price_position'] = (group['close'] - low_20) / (high_20 - low_20 + 1e-10)
            
            # 突破因子
            group['breakout'] = group['close'] / (high_20 + 1e-10)
            
            # RSI
            delta = group['close'].diff()
            gain = delta.where(delta > 0, 0).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / (loss + 1e-10)
            group['rsi'] = 100 - (100 / (1 + rs))
            
            # 短期反转
            group['reversal_5'] = -group['close'].pct_change(5)
            
            all_data.append(group)
        
        df = pd.concat(all_data, ignore_index=True)
        
        # 保存
        with open(self.data_file, 'wb') as f:
            pickle.dump(df, f)
        
        print(f"✓ 高级因子计算完成")
        return df
    
    def neutralize_factors(self) -> Tuple[Dict, pd.DataFrame]:
        """因子中性化"""
        print("\n" + "="*60)
        print("⚖️ 因子中性化模块")
        print("="*60)
        
        if not os.path.exists(self.data_file):
            print("❌ 数据文件不存在")
            return None, None
        
        with open(self.data_file, 'rb') as f:
            df = pickle.load(f)
        
        print(f"✓ 加载数据: {len(df)} 条记录")
        
        # 分配行业
        df['industry'] = df['stock_code'].apply(self._get_industry)
        
        # 市值代理
        df['log_amount'] = np.log(df['amount'] + 1)
        
        # 计算return_1d
        df = df.sort_values(['stock_code', 'date'])
        df['return_1d'] = df.groupby('stock_code')['close'].pct_change().shift(-1)
        
        # 中性化因子
        factors_to_neutralize = [
            'momentum_20', 'volatility_20', 'turnover',
            'momentum_accel', 'downside_vol', 'turnover_ratio',
            'price_position', 'rsi', 'reversal_5'
        ]
        
        results = {}
        
        for factor in factors_to_neutralize:
            if factor not in df.columns:
                continue
            
            # 行业中性化
            df[f'{factor}_neu'] = self._neutralize_by_group(df, factor, ['date', 'industry'])
            
            # 评估中性化前后
            ic_before, ir_before = self._evaluate_factor(df, factor)
            ic_after, ir_after = self._evaluate_factor(df, f'{factor}_neu')
            
            results[factor] = {
                'ic_before': ic_before,
                'ir_before': ir_before,
                'ic_after': ic_after,
                'ir_after': ir_after,
                'improved': abs(ir_after) > abs(ir_before)
            }
        
        # 保存
        with open(self.data_file, 'wb') as f:
            pickle.dump(df, f)
        
        # 打印结果
        print("\n因子中性化效果:")
        print(f"{'因子':<20} {'IC(前)':>10} {'IR(前)':>10} {'IC(后)':>10} {'IR(后)':>10}")
        print("-"*60)
        for factor, r in results.items():
            print(f"{factor:<20} {r['ic_before']:>10.4f} {r['ir_before']:>10.2f} {r['ic_after']:>10.4f} {r['ir_after']:>10.2f}")
        
        return results, df
    
    def run_full_pipeline(self, download: bool = True, stock_count: int = 500) -> pd.DataFrame:
        """运行完整管道"""
        print("\n" + "="*60)
        print("🚀 完整数据处理管道")
        print("="*60)
        
        # 1. 下载
        if download:
            if not self.download_data(stock_count=stock_count):
                print("❌ 数据下载失败")
                return None
        
        # 2. 处理
        df = self.process_data()
        if df is None:
            return None
        
        # 3. 高级因子
        df = self.calculate_advanced_factors()
        if df is None:
            return None
        
        # 4. 中性化
        results, df = self.neutralize_factors()
        
        print("\n" + "="*60)
        print("✅ 完整管道执行完成")
        print("="*60)
        
        return df
    
    def _save_progress(self, completed: set, status: str = 'in_progress'):
        """保存下载进度"""
        with open(self.progress_file, 'w') as f:
            json.dump({
                'completed': list(completed),
                'timestamp': datetime.now().isoformat(),
                'status': status
            }, f)
    
    def _get_industry(self, code: str) -> str:
        """根据代码分配行业"""
        code = str(code).replace('sh', '').replace('sz', '').replace('bj', '')
        if code.startswith('6'):
            return '工业'
        elif code.startswith('00'):
            return '制造'
        elif code.startswith('30') or code.startswith('68'):
            return '科技'
        return '其他'
    
    def _neutralize_by_group(self, df: pd.DataFrame, factor: str, group_cols: List[str]) -> np.ndarray:
        """分组中性化"""
        result = np.zeros(len(df))
        
        for _, group in df.groupby(group_cols):
            if len(group) < 10:
                continue
            
            vals = group[factor].values
            mean_val = np.nanmean(vals)
            std_val = np.nanstd(vals)
            
            if std_val > 0:
                result[group.index] = (vals - mean_val) / std_val
        
        return result
    
    def _evaluate_factor(self, df: pd.DataFrame, factor: str) -> Tuple[float, float]:
        """评估因子"""
        ic_values = []
        for date, group in df.groupby('date'):
            valid = group[[factor, 'return_1d']].dropna()
            if len(valid) >= 30:
                ic, _ = spearmanr(valid[factor], valid['return_1d'])
                if not np.isnan(ic):
                    ic_values.append(ic)
        
        if ic_values:
            ic_mean = np.mean(ic_values)
            ic_std = np.std(ic_values)
            ir = ic_mean / ic_std if ic_std > 0 else 0
            return ic_mean, ir
        return 0, 0


def main():
    parser = argparse.ArgumentParser(description='数据处理管道')
    parser.add_argument('--download', action='store_true', help='下载数据')
    parser.add_argument('--process', action='store_true', help='处理数据')
    parser.add_argument('--factors', action='store_true', help='计算高级因子')
    parser.add_argument('--neutralize', action='store_true', help='因子中性化')
    parser.add_argument('--all', action='store_true', help='运行完整流程')
    parser.add_argument('--stock-count', type=int, default=500, help='下载股票数量')
    
    args = parser.parse_args()
    
    pipeline = DataPipeline()
    
    if args.all:
        pipeline.run_full_pipeline(download=True, stock_count=args.stock_count)
    elif args.download:
        pipeline.download_data(stock_count=args.stock_count)
    elif args.process:
        pipeline.process_data()
    elif args.factors:
        pipeline.calculate_advanced_factors()
    elif args.neutralize:
        pipeline.neutralize_factors()
    else:
        parser.print_help()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
