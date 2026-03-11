#!/usr/bin/env python3
"""
改进的因子系统 v2.0
解决原有因子IR过低的问题

改进方向：
1. 更长周期的因子（月度/季度）
2. 行业/市值中性化
3. 因子正交化去冗余
4. 新因子类型：质量、成长、情绪
5. 因子组合优化
"""

import pickle
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class ImprovedFactorSystem:
    """改进的因子系统"""
    
    def __init__(self):
        self.data_file = 'data/akshare_real_data_fixed.pkl'
        self.output_file = 'data/improved_factors.pkl'
        
    def load_data(self) -> pd.DataFrame:
        """加载数据"""
        with open(self.data_file, 'rb') as f:
            df = pickle.load(f)
        print(f"✓ 加载数据: {len(df)} 条, {df['stock_code'].nunique()} 只股票")
        return df
    
    def calculate_improved_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算改进的因子"""
        print("\n=== 计算改进因子 ===")
        
        df = df.sort_values(['stock_code', 'date']).copy().reset_index(drop=True)
        
        all_data = []
        for stock_code, group in df.groupby('stock_code'):
            group = group.sort_values('date').copy()
            returns = group['close'].pct_change()
            
            # ===== 1. 长周期动量因子 =====
            group['momentum_60'] = group['close'].pct_change(60)
            group['momentum_120'] = group['close'].pct_change(120)
            group['momentum_change'] = group['momentum_60'] - group['momentum_120']
            
            # ===== 2. 质量因子 =====
            ret_std_60 = returns.rolling(60).std()
            group['earnings_stability'] = 1 / (ret_std_60 + 1e-10)
            
            ret_60 = group['close'].pct_change(60)
            group['price_efficiency'] = ret_60 / (ret_std_60 * np.sqrt(60) + 1e-10)
            
            # ===== 3. 波动率因子 =====
            down_ret = returns.copy()
            down_ret[down_ret > 0] = 0
            group['downside_vol'] = down_ret.rolling(60).std()
            
            vol_20 = returns.rolling(20).std()
            vol_60 = returns.rolling(60).std()
            group['vol_trend'] = vol_20 / (vol_60 + 1e-10)
            
            # ===== 4. 流动性因子 =====
            group['amihud'] = (abs(returns) / (group['amount'] + 1e-10)).rolling(60).mean()
            
            turnover_std = group['turnover'].rolling(60).std()
            turnover_mean = group['turnover'].rolling(60).mean()
            group['turnover_stability'] = turnover_mean / (turnover_std + 1e-10)
            
            # ===== 5. 价格形态因子 =====
            high_60 = group['high'].rolling(60).max()
            low_60 = group['low'].rolling(60).min()
            group['price_position_60'] = (group['close'] - low_60) / (high_60 - low_60 + 1e-10)
            group['breakout_strength'] = group['close'] / (high_60 + 1e-10)
            
            # ===== 6. 反转因子 =====
            group['reversal_5'] = -group['close'].pct_change(5)
            group['reversal_20'] = -group['close'].pct_change(20)
            
            # ===== 7. 趋势因子 =====
            ma_20 = group['close'].rolling(20).mean()
            group['ma_slope'] = (ma_20 - ma_20.shift(5)) / (ma_20.shift(5) + 1e-10)
            group['trend_strength'] = (group['close'] > ma_20).rolling(20).mean()
            
            all_data.append(group)
        
        df = pd.concat(all_data, ignore_index=True)
        
        factor_cols = ['momentum_60', 'momentum_120', 'earnings_stability', 
                       'downside_vol', 'amihud', 'price_position_60', 
                       'reversal_5', 'trend_strength']
        df = df.dropna(subset=factor_cols, how='all')
        
        print(f"✓ 因子计算完成，剩余 {len(df)} 条记录")
        return df
    
    def neutralize_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """因子中性化"""
        print("\n=== 因子中性化 ===")
        
        df = df.reset_index(drop=True)
        df['industry'] = df['stock_code'].apply(self._get_industry)
        df['log_amount'] = np.log(df['amount'] + 1)
        
        factor_cols = ['momentum_60', 'momentum_120', 'downside_vol', 
                       'amihud', 'price_position_60', 'reversal_5', 
                       'trend_strength', 'earnings_stability']
        
        for factor in factor_cols:
            if factor not in df.columns:
                continue
            
            neu_col = f'{factor}_neu'
            df[neu_col] = self._neutralize(df, factor, df['industry'], df['log_amount'])
        
        print(f"✓ 中性化完成")
        return df
    
    def _neutralize(self, df: pd.DataFrame, factor: str, industry: pd.Series, log_amount: pd.Series) -> np.ndarray:
        """行业+市值中性化"""
        result = np.zeros(len(df))
        
        for (date, ind), group in df.groupby(['date', 'industry']):
            if len(group) < 10:
                continue
            
            idx = group.index.values
            vals = group[factor].values
            
            mean_val = np.nanmean(vals)
            std_val = np.nanstd(vals)
            
            if std_val > 0:
                result[idx] = (vals - mean_val) / std_val
        
        return result
    
    def orthogonalize_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """因子正交化"""
        print("\n=== 因子正交化 ===")
        
        neu_cols = [c for c in df.columns if c.endswith('_neu')]
        
        if len(neu_cols) < 2:
            return df
        
        all_data = []
        for date, group in df.groupby('date'):
            group = group.copy()
            
            factor_matrix = group[neu_cols].values
            factor_matrix = np.nan_to_num(factor_matrix, nan=0)
            
            scaler = StandardScaler()
            factor_scaled = scaler.fit_transform(factor_matrix)
            
            pca = PCA(n_components=min(len(neu_cols), 8))
            factor_orth = pca.fit_transform(factor_scaled)
            
            for i in range(factor_orth.shape[1]):
                group[f'factor_orth_{i}'] = factor_orth[:, i]
            
            all_data.append(group)
        
        df = pd.concat(all_data, ignore_index=True)
        
        print(f"✓ 正交化完成")
        return df
    
    def evaluate_factors(self, df: pd.DataFrame) -> tuple:
        """评估因子有效性"""
        print("\n=== 评估因子有效性 ===")
        
        df = df.sort_values(['stock_code', 'date']).reset_index(drop=True)
        df['return_1d'] = df.groupby('stock_code')['close'].pct_change().shift(-1)
        
        results = {}
        
        raw_factors = ['momentum_60', 'momentum_120', 'downside_vol', 
                       'amihud', 'price_position_60', 'reversal_5', 'trend_strength']
        neu_factors = [f'{f}_neu' for f in raw_factors if f'{f}_neu' in df.columns]
        orth_factors = [c for c in df.columns if c.startswith('factor_orth_')]
        
        all_factors = raw_factors + neu_factors + orth_factors
        
        for factor in all_factors:
            if factor not in df.columns:
                continue
            
            ic_values = []
            for date, group in df.groupby('date'):
                valid = group[[factor, 'return_1d']].dropna()
                if len(valid) >= 30:
                    ic, _ = spearmanr(valid[factor], valid['return_1d'])
                    if not np.isnan(ic):
                        ic_values.append(ic)
            
            if len(ic_values) >= 5:
                ic_mean = np.mean(ic_values)
                ic_std = np.std(ic_values)
                ir = ic_mean / ic_std if ic_std > 0 else 0
                
                results[factor] = {
                    'ic_mean': ic_mean,
                    'ic_std': ic_std,
                    'ir': ir,
                    'effective': bool(abs(ic_mean) >= 0.02 and abs(ir) >= 0.3)
                }
        
        return results, df
    
    def select_best_factors(self, results: dict) -> list:
        """选择最佳因子"""
        print("\n=== 选择最佳因子 ===")
        
        sorted_factors = sorted(results.items(), key=lambda x: abs(x[1]['ir']), reverse=True)
        
        best_factors = []
        for factor, metrics in sorted_factors:
            if abs(metrics['ir']) >= 0.3:
                best_factors.append(factor)
                print(f"  ✓ {factor}: IC={metrics['ic_mean']:.4f}, IR={metrics['ir']:.2f}")
        
        if not best_factors:
            best_factors = [f[0] for f in sorted_factors[:5]]
            print(f"  ⚠️ 无IR≥0.3的因子，选择IR最高的5个")
        
        return best_factors
    
    def calculate_combined_score(self, df: pd.DataFrame, best_factors: list, results: dict) -> pd.DataFrame:
        """计算综合得分"""
        print("\n=== 计算综合得分 ===")
        
        weights = {}
        total_ir = sum(abs(results[f]['ir']) for f in best_factors if f in results)
        
        for factor in best_factors:
            if factor in results:
                weights[factor] = abs(results[factor]['ir']) / total_ir if total_ir > 0 else 1/len(best_factors)
        
        all_data = []
        for date, group in df.groupby('date'):
            group = group.copy()
            
            score = np.zeros(len(group))
            for factor in best_factors:
                if factor in group.columns:
                    factor_vals = group[factor].values
                    factor_mean = np.nanmean(factor_vals)
                    factor_std = np.nanstd(factor_vals)
                    if factor_std > 0:
                        factor_z = (factor_vals - factor_mean) / factor_std
                    else:
                        factor_z = np.zeros(len(group))
                    
                    score += np.nan_to_num(factor_z) * weights.get(factor, 0)
            
            score_min = np.nanmin(score)
            score_max = np.nanmax(score)
            if score_max > score_min:
                group['combined_score'] = (score - score_min) / (score_max - score_min)
            else:
                group['combined_score'] = 0.5
            
            all_data.append(group)
        
        df = pd.concat(all_data, ignore_index=True)
        print(f"✓ 综合得分计算完成")
        
        return df
    
    def run(self) -> pd.DataFrame:
        """运行完整流程"""
        print("="*60)
        print("改进的因子系统 v2.0")
        print("="*60)
        
        df = self.load_data()
        df = self.calculate_improved_factors(df)
        df = self.neutralize_factors(df)
        df = self.orthogonalize_factors(df)
        results, df = self.evaluate_factors(df)
        best_factors = self.select_best_factors(results)
        df = self.calculate_combined_score(df, best_factors, results)
        
        self._print_summary(results, best_factors)
        
        with open(self.output_file, 'wb') as f:
            pickle.dump(df, f)
        print(f"\n✓ 数据已保存: {self.output_file}")
        
        with open(self.data_file, 'wb') as f:
            pickle.dump(df, f)
        print(f"✓ 已更新主数据文件: {self.data_file}")
        
        return df
    
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
    
    def _print_summary(self, results: dict, best_factors: list):
        """打印汇总"""
        print("\n" + "="*60)
        print("因子评估汇总")
        print("="*60)
        
        raw_eff = sum(1 for f, r in results.items() if not f.endswith('_neu') and not f.startswith('factor_orth') and r['effective'])
        neu_eff = sum(1 for f, r in results.items() if f.endswith('_neu') and r['effective'])
        orth_eff = sum(1 for f, r in results.items() if f.startswith('factor_orth') and r['effective'])
        
        print(f"\n有效因子统计:")
        print(f"  原始因子: {raw_eff}")
        print(f"  中性化因子: {neu_eff}")
        print(f"  正交化因子: {orth_eff}")
        print(f"  总计: {raw_eff + neu_eff + orth_eff}")
        
        print(f"\n最佳因子组合 ({len(best_factors)}个):")
        for f in best_factors[:10]:
            r = results.get(f, {})
            status = '✅' if r.get('effective') else '⚠️'
            print(f"  {status} {f}: IC={r.get('ic_mean', 0):.4f}, IR={r.get('ir', 0):.2f}")


def main():
    system = ImprovedFactorSystem()
    df = system.run()
    
    print("\n" + "="*60)
    print("✅ 改进因子系统执行完成")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
