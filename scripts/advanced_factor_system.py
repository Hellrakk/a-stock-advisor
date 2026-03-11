#!/usr/bin/env python3
"""
高级因子系统 v3.0 - 机器学习增强
解决传统因子IR过低的问题

核心改进：
1. 横截面相对强度因子（行业相对、市场相对）
2. 行业轮动因子
3. 机器学习因子组合（XGBoost/LightGBM）
4. 因子衰减分析
"""

import pickle
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit
from sklearn.linear_model import Ridge
import warnings
warnings.filterwarnings('ignore')


class AdvancedFactorSystem:
    """高级因子系统"""
    
    def __init__(self):
        self.data_file = 'data/akshare_real_data_fixed.pkl'
        self.output_file = 'data/advanced_factors.pkl'
        
    def load_data(self) -> pd.DataFrame:
        """加载数据"""
        with open(self.data_file, 'rb') as f:
            df = pickle.load(f)
        print(f"✓ 加载数据: {len(df)} 条, {df['stock_code'].nunique()} 只股票")
        return df
    
    def calculate_cross_sectional_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算横截面因子"""
        print("\n=== 计算横截面因子 ===")
        
        df = df.sort_values(['stock_code', 'date']).copy().reset_index(drop=True)
        
        # 分配行业
        df['industry'] = df['stock_code'].apply(self._get_industry)
        
        all_data = []
        for stock_code, group in df.groupby('stock_code'):
            group = group.sort_values('date').copy()
            returns = group['close'].pct_change()
            
            # ===== 1. 基础因子 =====
            group['momentum_20'] = group['close'].pct_change(20)
            group['momentum_60'] = group['close'].pct_change(60)
            
            # 波动率
            group['volatility_20'] = returns.rolling(20).std()
            group['volatility_60'] = returns.rolling(60).std()
            
            # 下行波动率
            down_ret = returns.copy()
            down_ret[down_ret > 0] = 0
            group['downside_vol'] = down_ret.rolling(60).std()
            
            # 反转
            group['reversal_5'] = -group['close'].pct_change(5)
            group['reversal_10'] = -group['close'].pct_change(10)
            
            # 价格位置
            high_60 = group['high'].rolling(60).max()
            low_60 = group['low'].rolling(60).min()
            group['price_position'] = (group['close'] - low_60) / (high_60 - low_60 + 1e-10)
            
            # 换手率
            group['turnover_ma'] = group['turnover'].rolling(20).mean()
            
            # Amihud非流动性
            group['amihud'] = (abs(returns) / (group['amount'] + 1e-10)).rolling(20).mean()
            
            all_data.append(group)
        
        df = pd.concat(all_data, ignore_index=True)
        
        # ===== 2. 横截面相对因子 =====
        print("  计算横截面相对因子...")
        
        # 市场相对动量
        df['market_ret'] = df.groupby('date')['change_pct'].transform('mean')
        df['rel_momentum'] = df['momentum_20'] - df['market_ret']
        
        # 行业相对动量
        df['industry_ret'] = df.groupby(['date', 'industry'])['change_pct'].transform('mean')
        df['rel_industry_momentum'] = df['momentum_20'] - df['industry_ret']
        
        # 行业内排名
        df['industry_rank'] = df.groupby(['date', 'industry'])['momentum_20'].rank(pct=True)
        
        # 市场排名
        df['market_rank'] = df.groupby('date')['momentum_20'].rank(pct=True)
        
        # ===== 3. 行业轮动因子 =====
        print("  计算行业轮动因子...")
        
        # 行业动量
        industry_momentum = df.groupby(['date', 'industry'])['momentum_20'].mean().reset_index()
        industry_momentum.columns = ['date', 'industry', 'industry_mom']
        df = df.merge(industry_momentum, on=['date', 'industry'], how='left')
        
        # 行业强度（行业相对市场的超额收益）
        df['industry_strength'] = df['industry_mom'] - df['market_ret']
        
        # ===== 4. 量价因子 =====
        print("  计算量价因子...")
        
        # 成交量变化
        df['vol_change'] = df.groupby('stock_code')['volume'].pct_change()
        df['vol_ma_ratio'] = df['volume'] / (df.groupby('stock_code')['volume'].rolling(20).mean().reset_index(0, drop=True) + 1e-10)
        
        # 量价趋势
        df['vol_price_trend'] = df.groupby('stock_code').apply(
            lambda g: self._calc_vol_price_trend(g)
        ).reset_index(0, drop=True)
        
        print(f"✓ 横截面因子计算完成")
        return df
    
    def _calc_vol_price_trend(self, group: pd.DataFrame) -> pd.Series:
        """计算量价趋势"""
        vol = group['volume'].values
        price = group['close'].values
        
        result = np.zeros(len(group))
        for i in range(20, len(group)):
            if i >= 20:
                vol_window = vol[i-20:i]
                price_window = price[i-20:i]
                if len(vol_window) == 20 and len(price_window) == 20:
                    corr = np.corrcoef(vol_window, price_window)[0, 1]
                    result[i] = corr if not np.isnan(corr) else 0
        
        return pd.Series(result, index=group.index)
    
    def train_ml_factor_combination(self, df: pd.DataFrame) -> tuple:
        """训练机器学习因子组合"""
        print("\n=== 训练机器学习因子组合 ===")
        
        # 准备目标变量
        df = df.sort_values(['stock_code', 'date']).reset_index(drop=True)
        df['return_1d'] = df.groupby('stock_code')['close'].pct_change().shift(-1)
        df['return_5d'] = df.groupby('stock_code')['close'].pct_change(5).shift(-5)
        
        # 特征列
        feature_cols = [
            'momentum_20', 'momentum_60', 'volatility_20', 'volatility_60',
            'downside_vol', 'reversal_5', 'reversal_10', 'price_position',
            'turnover_ma', 'amihud', 'rel_momentum', 'rel_industry_momentum',
            'industry_rank', 'market_rank', 'industry_strength',
            'vol_change', 'vol_ma_ratio', 'vol_price_trend'
        ]
        
        # 过滤有效特征
        feature_cols = [c for c in feature_cols if c in df.columns]
        
        # 训练数据
        df_train = df[feature_cols + ['return_1d', 'date', 'stock_code']].dropna()
        
        if len(df_train) < 1000:
            print("⚠️ 训练数据不足")
            return None, df
        
        # 按时间分割
        dates = sorted(df_train['date'].unique())
        train_dates = dates[:int(len(dates) * 0.7)]
        test_dates = dates[int(len(dates) * 0.7):]
        
        train_df = df_train[df_train['date'].isin(train_dates)]
        test_df = df_train[df_train['date'].isin(test_dates)]
        
        X_train = train_df[feature_cols].values
        y_train = train_df['return_1d'].values
        X_test = test_df[feature_cols].values
        y_test = test_df['return_1d'].values
        
        # 标准化
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # 训练模型
        print(f"  训练数据: {len(X_train)} 条")
        print(f"  测试数据: {len(X_test)} 条")
        
        # 使用HistGradientBoosting（支持NaN）
        model = HistGradientBoostingRegressor(
            max_iter=100,
            max_depth=3,
            learning_rate=0.1,
            random_state=42
        )
        
        model.fit(X_train_scaled, y_train)
        
        # 预测
        y_pred = model.predict(X_test_scaled)
        
        # 计算IC
        ic_values = []
        for date in test_dates:
            mask = test_df['date'] == date
            if mask.sum() >= 30:
                ic, _ = spearmanr(y_test[mask], y_pred[mask])
                if not np.isnan(ic):
                    ic_values.append(ic)
        
        if len(ic_values) > 0:
            ic_mean = np.mean(ic_values)
            ic_std = np.std(ic_values)
            ir = ic_mean / ic_std if ic_std > 0 else 0
            
            print(f"\n  ML模型测试集表现:")
            print(f"    IC均值: {ic_mean:.4f}")
            print(f"    IC标准差: {ic_std:.4f}")
            print(f"    IR: {ir:.2f}")
        
        # 特征重要性（HistGradientBoosting不支持直接获取，跳过）
        print(f"\n  特征重要性: HistGradientBoosting不支持直接获取")
        
        # 对全量数据预测
        X_all = df[feature_cols].values
        # HistGradientBoosting支持NaN，直接预测
        df['ml_score'] = model.predict(X_all)
        
        return model, df
    
    def evaluate_all_factors(self, df: pd.DataFrame) -> dict:
        """评估所有因子"""
        print("\n=== 评估所有因子 ===")
        
        df = df.sort_values(['stock_code', 'date']).reset_index(drop=True)
        df['return_1d'] = df.groupby('stock_code')['close'].pct_change().shift(-1)
        
        results = {}
        
        # 所有因子
        factor_cols = [
            'momentum_20', 'momentum_60', 'volatility_20', 'volatility_60',
            'downside_vol', 'reversal_5', 'reversal_10', 'price_position',
            'turnover_ma', 'amihud', 'rel_momentum', 'rel_industry_momentum',
            'industry_rank', 'market_rank', 'industry_strength',
            'vol_change', 'vol_ma_ratio', 'vol_price_trend', 'ml_score'
        ]
        
        factor_cols = [c for c in factor_cols if c in df.columns]
        
        for factor in factor_cols:
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
                
                # IC正向率
                ic_positive_rate = sum(1 for x in ic_values if x > 0) / len(ic_values)
                
                results[factor] = {
                    'ic_mean': ic_mean,
                    'ic_std': ic_std,
                    'ir': ir,
                    'ic_positive_rate': ic_positive_rate,
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
                print(f"  ✅ {factor}: IC={metrics['ic_mean']:.4f}, IR={metrics['ir']:.2f}, 正向率={metrics['ic_positive_rate']:.1%}")
        
        if not best_factors:
            best_factors = [f[0] for f in sorted_factors[:5]]
            print(f"  ⚠️ 无IR≥0.3的因子，选择IR最高的5个")
            for f in best_factors:
                r = results[f]
                print(f"    {f}: IR={r['ir']:.2f}")
        
        return best_factors
    
    def calculate_final_score(self, df: pd.DataFrame, best_factors: list, results: dict) -> pd.DataFrame:
        """计算最终得分"""
        print("\n=== 计算最终得分 ===")
        
        # 按IR加权
        weights = {}
        total_ir = sum(abs(results[f]['ir']) for f in best_factors if f in results)
        
        for factor in best_factors:
            if factor in results:
                weights[factor] = abs(results[factor]['ir']) / total_ir if total_ir > 0 else 1/len(best_factors)
        
        print(f"因子权重: {weights}")
        
        # 计算得分
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
            
            # 归一化
            score_min = np.nanmin(score)
            score_max = np.nanmax(score)
            if score_max > score_min:
                group['final_score'] = (score - score_min) / (score_max - score_min)
            else:
                group['final_score'] = 0.5
            
            all_data.append(group)
        
        df = pd.concat(all_data, ignore_index=True)
        print(f"✓ 最终得分计算完成")
        
        return df
    
    def run(self) -> pd.DataFrame:
        """运行完整流程"""
        print("="*60)
        print("高级因子系统 v3.0 - 机器学习增强")
        print("="*60)
        
        df = self.load_data()
        df = self.calculate_cross_sectional_factors(df)
        model, df = self.train_ml_factor_combination(df)
        results, df = self.evaluate_all_factors(df)
        best_factors = self.select_best_factors(results)
        df = self.calculate_final_score(df, best_factors, results)
        
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
        
        effective_count = sum(1 for r in results.values() if r['effective'])
        
        print(f"\n有效因子: {effective_count}/{len(results)}")
        
        print(f"\n所有因子表现 (按IR排序):")
        sorted_results = sorted(results.items(), key=lambda x: abs(x[1]['ir']), reverse=True)
        for factor, r in sorted_results[:15]:
            status = '✅' if r['effective'] else '⚠️'
            print(f"  {status} {factor}: IC={r['ic_mean']:.4f}, IR={r['ir']:.2f}, 正向率={r['ic_positive_rate']:.1%}")


def main():
    system = AdvancedFactorSystem()
    df = system.run()
    
    print("\n" + "="*60)
    print("✅ 高级因子系统执行完成")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
