#!/usr/bin/env python3
"""
增强因子系统 v4.0
解决IR过低的核心问题

核心改进：
1. 调整预测目标：1日→5日/20日收益
2. 模拟财务因子：基于价格数据推导
3. 质量因子：收益稳定性、波动率倒数
4. 成长因子：动量加速度、趋势强度
5. 情绪因子：换手率异常、成交额变化
"""

import pickle
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
import warnings
warnings.filterwarnings('ignore')


class EnhancedFactorSystem:
    """增强因子系统 v4.0"""
    
    def __init__(self):
        self.data_file = 'data/akshare_real_data_fixed.pkl'
        self.output_file = 'data/enhanced_factors.pkl'
        
    def load_data(self) -> pd.DataFrame:
        """加载数据"""
        with open(self.data_file, 'rb') as f:
            df = pickle.load(f)
        print(f"✓ 加载数据: {len(df)} 条, {df['stock_code'].nunique()} 只股票")
        return df
    
    def calculate_target_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算多周期目标收益"""
        print("\n=== 计算目标收益 ===")
        
        df = df.sort_values(['stock_code', 'date']).reset_index(drop=True)
        
        # 5日收益（主要目标）
        df['return_5d'] = df.groupby('stock_code')['close'].pct_change(5).shift(-5)
        
        # 10日收益
        df['return_10d'] = df.groupby('stock_code')['close'].pct_change(10).shift(-10)
        
        # 20日收益
        df['return_20d'] = df.groupby('stock_code')['close'].pct_change(20).shift(-20)
        
        # 风险调整收益（5日收益/5日波动）
        vol_5 = df.groupby('stock_code')['close'].pct_change().rolling(5).std().reset_index(0, drop=True)
        df['risk_adj_return_5d'] = df['return_5d'] / (vol_5 * np.sqrt(5) + 1e-10)
        
        print(f"✓ 目标收益计算完成")
        return df
    
    def calculate_quality_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算质量因子"""
        print("\n=== 计算质量因子 ===")
        
        df = df.sort_values(['stock_code', 'date']).reset_index(drop=True)
        
        all_data = []
        for stock_code, group in df.groupby('stock_code'):
            group = group.sort_values('date').copy()
            returns = group['close'].pct_change()
            
            # ===== 1. 收益稳定性 =====
            # 收益标准差的倒数（越稳定越好）
            ret_std_60 = returns.rolling(60).std()
            group['earnings_stability'] = 1 / (ret_std_60 + 1e-10)
            
            # ===== 2. 下行风险控制 =====
            # 下行波动率（只计算负收益）
            down_ret = returns.copy()
            down_ret[down_ret > 0] = 0
            group['downside_risk'] = down_ret.rolling(60).std()
            
            # 下行风险倒数
            group['downside_risk_inv'] = 1 / (group['downside_risk'] + 1e-10)
            
            # ===== 3. 最大回撤控制 =====
            # 滚动最大回撤
            cummax = group['close'].cummax()
            drawdown = (group['close'] - cummax) / cummax
            group['max_drawdown_60'] = drawdown.rolling(60).min()
            
            # 回撤恢复能力
            group['drawdown_recovery'] = 1 / (abs(group['max_drawdown_60']) + 1e-10)
            
            # ===== 4. 波动率质量 =====
            # 波动率倒数
            group['vol_quality'] = 1 / (ret_std_60 + 1e-10)
            
            # 波动率稳定性
            vol_20 = returns.rolling(20).std()
            vol_60 = returns.rolling(60).std()
            group['vol_stability'] = vol_20 / (vol_60 + 1e-10)
            
            # ===== 5. 夏普代理 =====
            ret_60 = group['close'].pct_change(60)
            group['sharpe_proxy'] = ret_60 / (ret_std_60 * np.sqrt(60) + 1e-10)
            
            all_data.append(group)
        
        df = pd.concat(all_data, ignore_index=True)
        print(f"✓ 质量因子计算完成")
        return df
    
    def calculate_growth_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算成长因子"""
        print("\n=== 计算成长因子 ===")
        
        df = df.sort_values(['stock_code', 'date']).reset_index(drop=True)
        
        all_data = []
        for stock_code, group in df.groupby('stock_code'):
            group = group.sort_values('date').copy()
            returns = group['close'].pct_change()
            
            # ===== 1. 动量加速度 =====
            mom_5 = group['close'].pct_change(5)
            mom_20 = group['close'].pct_change(20)
            mom_60 = group['close'].pct_change(60)
            
            # 动量变化率
            group['momentum_accel'] = mom_5 - mom_20
            group['momentum_accel_2'] = mom_20 - mom_60
            
            # ===== 2. 价格趋势强度 =====
            # MA斜率
            ma_20 = group['close'].rolling(20).mean()
            ma_60 = group['close'].rolling(60).mean()
            
            group['ma_slope_20'] = (ma_20 - ma_20.shift(5)) / (ma_20.shift(5) + 1e-10)
            group['ma_slope_60'] = (ma_60 - ma_60.shift(10)) / (ma_60.shift(10) + 1e-10)
            
            # 价格在MA上方比例
            group['price_above_ma20'] = (group['close'] > ma_20).rolling(20).mean()
            group['price_above_ma60'] = (group['close'] > ma_60).rolling(60).mean()
            
            # ===== 3. 突破强度 =====
            high_20 = group['high'].rolling(20).max()
            low_20 = group['low'].rolling(20).min()
            high_60 = group['high'].rolling(60).max()
            low_60 = group['low'].rolling(60).min()
            
            # 价格位置
            group['price_position_20'] = (group['close'] - low_20) / (high_20 - low_20 + 1e-10)
            group['price_position_60'] = (group['close'] - low_60) / (high_60 - low_60 + 1e-10)
            
            # 突破强度
            group['breakout_20'] = group['close'] / (high_20 + 1e-10)
            group['breakout_60'] = group['close'] / (high_60 + 1e-10)
            
            # ===== 4. 成交量趋势 =====
            vol_ma_5 = group['volume'].rolling(5).mean()
            vol_ma_20 = group['volume'].rolling(20).mean()
            vol_ma_60 = group['volume'].rolling(60).mean()
            
            # 成交量趋势
            group['vol_trend_short'] = vol_ma_5 / (vol_ma_20 + 1e-10)
            group['vol_trend_long'] = vol_ma_20 / (vol_ma_60 + 1e-10)
            
            # ===== 5. 相对强度 =====
            # 价格相对60日均线的位置
            group['rel_strength_ma60'] = group['close'] / (ma_60 + 1e-10) - 1
            
            all_data.append(group)
        
        df = pd.concat(all_data, ignore_index=True)
        print(f"✓ 成长因子计算完成")
        return df
    
    def calculate_sentiment_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算情绪因子"""
        print("\n=== 计算情绪因子 ===")
        
        df = df.sort_values(['stock_code', 'date']).reset_index(drop=True)
        
        all_data = []
        for stock_code, group in df.groupby('stock_code'):
            group = group.sort_values('date').copy()
            
            # ===== 1. 换手率异常 =====
            turnover_ma_20 = group['turnover'].rolling(20).mean()
            turnover_std_20 = group['turnover'].rolling(20).std()
            
            # 换手率Z-score
            group['turnover_zscore'] = (group['turnover'] - turnover_ma_20) / (turnover_std_20 + 1e-10)
            
            # 换手率异常（超过2倍标准差）
            group['turnover_abnormal'] = (group['turnover'] > turnover_ma_20 + 2 * turnover_std_20).astype(float)
            
            # ===== 2. 成交额变化 =====
            amount_ma_20 = group['amount'].rolling(20).mean()
            amount_std_20 = group['amount'].rolling(20).std()
            
            # 成交额Z-score
            group['amount_zscore'] = (group['amount'] - amount_ma_20) / (amount_std_20 + 1e-10)
            
            # 成交额相对变化
            group['amount_rel_change'] = group['amount'] / (amount_ma_20 + 1e-10)
            
            # ===== 3. 振幅因子 =====
            group['amplitude'] = (group['high'] - group['low']) / (group['close'].shift(1) + 1e-10)
            amp_ma_20 = group['amplitude'].rolling(20).mean()
            group['amplitude_rel'] = group['amplitude'] / (amp_ma_20 + 1e-10)
            
            # ===== 4. 涨跌停因子 =====
            # 接近涨停
            group['near_limit_up'] = (group['change_pct'] > 9.5).astype(float)
            # 接近跌停
            group['near_limit_down'] = (group['change_pct'] < -9.5).astype(float)
            
            # ===== 5. 连续涨跌 =====
            # 连续上涨天数
            up_days = (group['change_pct'] > 0).astype(int)
            group['consec_up'] = up_days.groupby((up_days == 0).cumsum()).cumsum()
            
            # 连续下跌天数
            down_days = (group['change_pct'] < 0).astype(int)
            group['consec_down'] = down_days.groupby((down_days == 0).cumsum()).cumsum()
            
            all_data.append(group)
        
        df = pd.concat(all_data, ignore_index=True)
        print(f"✓ 情绪因子计算完成")
        return df
    
    def calculate_cross_sectional_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算横截面因子"""
        print("\n=== 计算横截面因子 ===")
        
        # 先计算基础动量因子和波动率
        all_data = []
        for stock_code, group in df.groupby('stock_code'):
            group = group.sort_values('date').copy()
            group['momentum_20'] = group['close'].pct_change(20)
            returns = group['close'].pct_change()
            group['volatility_20'] = returns.rolling(20).std()
            all_data.append(group)
        df = pd.concat(all_data, ignore_index=True)
        
        # 分配行业
        df['industry'] = df['stock_code'].apply(self._get_industry)
        
        # ===== 1. 行业内排名 =====
        # 动量排名
        df['momentum_20_rank'] = df.groupby(['date', 'industry'])['momentum_20'].rank(pct=True)
        
        # 波动率排名（低波动优先）
        df['volatility_rank'] = df.groupby(['date', 'industry'])['volatility_20'].rank(pct=True, ascending=False)
        
        # 成交额排名
        df['amount_rank'] = df.groupby(['date', 'industry'])['amount'].rank(pct=True)
        
        # ===== 2. 市场相对因子 =====
        # 市场平均收益
        df['market_ret'] = df.groupby('date')['change_pct'].transform('mean')
        
        # 行业平均收益
        df['industry_ret'] = df.groupby(['date', 'industry'])['change_pct'].transform('mean')
        
        # 相对市场超额
        df['excess_ret_market'] = df['change_pct'] - df['market_ret']
        
        # 相对行业超额
        df['excess_ret_industry'] = df['change_pct'] - df['industry_ret']
        
        # ===== 3. 行业轮动 =====
        # 行业动量
        industry_mom = df.groupby(['date', 'industry'])['momentum_20'].mean().reset_index()
        industry_mom.columns = ['date', 'industry', 'ind_mom_val']
        df = df.merge(industry_mom, on=['date', 'industry'], how='left')
        
        # 行业强度
        df['industry_strength'] = df['ind_mom_val'] - df['market_ret']
        
        print(f"✓ 横截面因子计算完成")
        return df
    
    def train_ml_model(self, df: pd.DataFrame, target: str = 'return_5d') -> tuple:
        """训练ML模型"""
        print(f"\n=== 训练ML模型（目标: {target}） ===")
        
        # 特征列
        feature_cols = [
            # 质量因子
            'earnings_stability', 'downside_risk_inv', 'drawdown_recovery',
            'vol_quality', 'vol_stability', 'sharpe_proxy',
            # 成长因子
            'momentum_accel', 'momentum_accel_2', 'ma_slope_20', 'ma_slope_60',
            'price_above_ma20', 'price_above_ma60', 'price_position_20', 'price_position_60',
            'breakout_20', 'breakout_60', 'vol_trend_short', 'vol_trend_long',
            'rel_strength_ma60',
            # 情绪因子
            'turnover_zscore', 'turnover_abnormal', 'amount_zscore', 'amount_rel_change',
            'amplitude_rel', 'consec_up', 'consec_down',
            # 横截面因子
            'momentum_20_rank', 'volatility_rank', 'amount_rank',
            'excess_ret_market', 'excess_ret_industry', 'industry_strength'
        ]
        
        # 过滤有效特征
        feature_cols = [c for c in feature_cols if c in df.columns]
        
        # 准备训练数据
        df_train = df[feature_cols + [target, 'date', 'stock_code']].dropna(subset=[target])
        
        if len(df_train) < 1000:
            print("⚠️ 训练数据不足")
            return None, df, {}
        
        # 按时间分割
        dates = sorted(df_train['date'].dropna().unique())
        if len(dates) < 10:
            print("⚠️ 日期数据不足")
            return None, df, {}
        
        train_dates = dates[:int(len(dates) * 0.7)]
        test_dates = dates[int(len(dates) * 0.7):]
        
        train_df = df_train[df_train['date'].isin(train_dates)]
        test_df = df_train[df_train['date'].isin(test_dates)]
        
        X_train = train_df[feature_cols].values
        y_train = train_df[target].values
        X_test = test_df[feature_cols].values
        y_test = test_df[target].values
        
        print(f"  训练数据: {len(X_train)} 条")
        print(f"  测试数据: {len(X_test)} 条")
        
        # 训练模型
        model = HistGradientBoostingRegressor(
            max_iter=200,
            max_depth=4,
            learning_rate=0.05,
            random_state=42,
            l2_regularization=0.1
        )
        
        model.fit(X_train, y_train)
        
        # 预测
        y_pred = model.predict(X_test)
        
        # 计算IC
        ic_values = []
        for date in test_dates:
            mask = test_df['date'] == date
            if mask.sum() >= 30:
                valid = ~np.isnan(y_test[mask]) & ~np.isnan(y_pred[mask])
                if valid.sum() >= 30:
                    ic, _ = spearmanr(y_test[mask][valid], y_pred[mask][valid])
                    if not np.isnan(ic):
                        ic_values.append(ic)
        
        metrics = {}
        if len(ic_values) > 0:
            ic_mean = np.mean(ic_values)
            ic_std = np.std(ic_values)
            ir = ic_mean / ic_std if ic_std > 0 else 0
            
            metrics = {
                'ic_mean': ic_mean,
                'ic_std': ic_std,
                'ir': ir,
                'ic_positive_rate': sum(1 for x in ic_values if x > 0) / len(ic_values)
            }
            
            print(f"\n  ML模型测试集表现:")
            print(f"    IC均值: {ic_mean:.4f}")
            print(f"    IC标准差: {ic_std:.4f}")
            print(f"    IR: {ir:.2f}")
            print(f"    IC正向率: {metrics['ic_positive_rate']:.1%}")
        
        # 对全量数据预测
        X_all = df[feature_cols].values
        df[f'ml_score_{target}'] = model.predict(X_all)
        
        return model, df, metrics
    
    def evaluate_all_factors(self, df: pd.DataFrame, target: str = 'return_5d') -> dict:
        """评估所有因子"""
        print(f"\n=== 评估所有因子（目标: {target}） ===")
        
        # 所有因子
        factor_cols = [
            # 质量因子
            'earnings_stability', 'downside_risk_inv', 'drawdown_recovery',
            'vol_quality', 'vol_stability', 'sharpe_proxy',
            # 成长因子
            'momentum_accel', 'momentum_accel_2', 'ma_slope_20', 'ma_slope_60',
            'price_above_ma20', 'price_above_ma60', 'price_position_20', 'price_position_60',
            'breakout_20', 'breakout_60', 'vol_trend_short', 'vol_trend_long',
            'rel_strength_ma60',
            # 情绪因子
            'turnover_zscore', 'turnover_abnormal', 'amount_zscore', 'amount_rel_change',
            'amplitude_rel', 'consec_up', 'consec_down',
            # 横截面因子
            'momentum_20_rank', 'volatility_rank', 'amount_rank',
            'excess_ret_market', 'excess_ret_industry', 'industry_strength',
            # ML得分
            f'ml_score_{target}'
        ]
        
        factor_cols = [c for c in factor_cols if c in df.columns]
        
        results = {}
        for factor in factor_cols:
            ic_values = []
            for date, group in df.groupby('date'):
                valid = group[[factor, target]].dropna()
                if len(valid) >= 30:
                    ic, _ = spearmanr(valid[factor], valid[target])
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
                    'ic_positive_rate': sum(1 for x in ic_values if x > 0) / len(ic_values),
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
            # 选择IR最高的10个
            best_factors = [f[0] for f in sorted_factors[:10]]
            print(f"  ⚠️ 无IR≥0.3的因子，选择IR最高的10个")
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
        
        print(f"因子权重: {list(weights.items())[:5]}...")
        
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
        print("增强因子系统 v4.0")
        print("="*60)
        
        df = self.load_data()
        
        # 1. 计算目标收益
        df = self.calculate_target_returns(df)
        
        # 2. 计算各类因子
        df = self.calculate_quality_factors(df)
        df = self.calculate_growth_factors(df)
        df = self.calculate_sentiment_factors(df)
        df = self.calculate_cross_sectional_factors(df)
        
        # 3. 训练ML模型（5日收益）
        model, df, ml_metrics = self.train_ml_model(df, target='return_5d')
        
        # 4. 评估因子（5日收益）
        results, df = self.evaluate_all_factors(df, target='return_5d')
        
        # 5. 选择最佳因子
        best_factors = self.select_best_factors(results)
        
        # 6. 计算最终得分
        df = self.calculate_final_score(df, best_factors, results)
        
        # 7. 打印汇总
        self._print_summary(results, best_factors, ml_metrics)
        
        # 8. 保存
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
    
    def _print_summary(self, results: dict, best_factors: list, ml_metrics: dict):
        """打印汇总"""
        print("\n" + "="*60)
        print("因子评估汇总")
        print("="*60)
        
        effective_count = sum(1 for r in results.values() if r['effective'])
        
        print(f"\n有效因子: {effective_count}/{len(results)}")
        
        if ml_metrics:
            print(f"\nML模型表现:")
            print(f"  IC均值: {ml_metrics.get('ic_mean', 0):.4f}")
            print(f"  IR: {ml_metrics.get('ir', 0):.2f}")
            print(f"  IC正向率: {ml_metrics.get('ic_positive_rate', 0):.1%}")
        
        print(f"\n所有因子表现 (按IR排序):")
        sorted_results = sorted(results.items(), key=lambda x: abs(x[1]['ir']), reverse=True)
        for factor, r in sorted_results[:15]:
            status = '✅' if r['effective'] else '⚠️'
            print(f"  {status} {factor}: IC={r['ic_mean']:.4f}, IR={r['ir']:.2f}, 正向率={r['ic_positive_rate']:.1%}")


def main():
    system = EnhancedFactorSystem()
    df = system.run()
    
    print("\n" + "="*60)
    print("✅ 增强因子系统执行完成")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
