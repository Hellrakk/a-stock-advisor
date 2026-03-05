#!/usr/bin/env python3
"""
技术指标验证模块
验证技术指标的预测能力和有效性
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from scipy.stats import spearmanr
from dataclasses import dataclass
import json
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


@dataclass
class IndicatorValidationResult:
    """指标验证结果"""
    indicator_name: str
    ic: float
    ir: float
    ic_pvalue: float
    is_valid: bool
    decay_rate: float
    half_life: float
    recommendation: str
    validation_date: str


class IndicatorValidator:
    """技术指标验证器"""
    
    def __init__(
        self,
        ic_threshold: float = 0.02,
        ir_threshold: float = 0.3,
        pvalue_threshold: float = 0.05,
        rolling_window: int = 20
    ):
        """
        初始化指标验证器
        
        Args:
            ic_threshold: IC有效阈值
            ir_threshold: IR有效阈值
            pvalue_threshold: p值阈值
            rolling_window: 滚动计算窗口
        """
        self.ic_threshold = ic_threshold
        self.ir_threshold = ir_threshold
        self.pvalue_threshold = pvalue_threshold
        self.rolling_window = rolling_window
        self.validation_results: Dict[str, IndicatorValidationResult] = {}
    
    def validate_indicator(
        self,
        indicator_series: pd.Series,
        future_returns: pd.Series,
        indicator_name: str = "indicator"
    ) -> IndicatorValidationResult:
        """
        验证单个指标的预测能力
        
        Args:
            indicator_series: 指标值序列
            future_returns: 未来收益率序列
            indicator_name: 指标名称
            
        Returns:
            验证结果
        """
        indicator_series = indicator_series.dropna()
        future_returns = future_returns.dropna()
        
        common_index = indicator_series.index.intersection(future_returns.index)
        
        if len(common_index) < 30:
            return IndicatorValidationResult(
                indicator_name=indicator_name,
                ic=0.0,
                ir=0.0,
                ic_pvalue=1.0,
                is_valid=False,
                decay_rate=0.0,
                half_life=np.inf,
                recommendation="数据不足，无法验证",
                validation_date=datetime.now().strftime('%Y-%m-%d')
            )
        
        indicator_values = indicator_series.loc[common_index]
        return_values = future_returns.loc[common_index]
        
        ic, pvalue = spearmanr(indicator_values, return_values)
        
        if np.isnan(ic):
            ic = 0.0
        if np.isnan(pvalue):
            pvalue = 1.0
        
        ic_std = self._calculate_rolling_ic_std(indicator_values, return_values)
        ir = ic / ic_std if ic_std > 0 else 0.0
        
        decay_info = self._calculate_ic_decay(indicator_values, return_values)
        
        is_valid = (
            abs(ic) >= self.ic_threshold and
            abs(ir) >= self.ir_threshold and
            pvalue <= self.pvalue_threshold
        )
        
        if is_valid:
            if ic > 0:
                recommendation = "有效正向指标，建议保留"
            else:
                recommendation = "有效负向指标，建议反向使用"
        elif abs(ic) >= self.ic_threshold * 0.5:
            recommendation = "弱有效指标，建议优化参数"
        else:
            recommendation = "无效指标，建议弃用"
        
        result = IndicatorValidationResult(
            indicator_name=indicator_name,
            ic=round(ic, 4),
            ir=round(ir, 4),
            ic_pvalue=round(pvalue, 4),
            is_valid=is_valid,
            decay_rate=decay_info['decay_rate'],
            half_life=decay_info['half_life'],
            recommendation=recommendation,
            validation_date=datetime.now().strftime('%Y-%m-%d')
        )
        
        self.validation_results[indicator_name] = result
        
        return result
    
    def _calculate_rolling_ic_std(
        self,
        indicator_series: pd.Series,
        return_series: pd.Series
    ) -> float:
        """计算滚动IC标准差"""
        if len(indicator_series) < self.rolling_window:
            return 0.01
        
        ic_values = []
        for i in range(self.rolling_window, len(indicator_series)):
            window_indicator = indicator_series.iloc[i-self.rolling_window:i]
            window_return = return_series.iloc[i-self.rolling_window:i]
            
            if len(window_indicator) > 10:
                ic, _ = spearmanr(window_indicator, window_return)
                if not np.isnan(ic):
                    ic_values.append(ic)
        
        return np.std(ic_values) if ic_values else 0.01
    
    def _calculate_ic_decay(
        self,
        indicator_series: pd.Series,
        return_series: pd.Series
    ) -> Dict[str, float]:
        """计算IC衰减"""
        n = len(indicator_series)
        if n < 20:
            return {'decay_rate': 0.0, 'half_life': np.inf}
        
        first_half_ic, _ = spearmanr(
            indicator_series.iloc[:n//2],
            return_series.iloc[:n//2]
        )
        second_half_ic, _ = spearmanr(
            indicator_series.iloc[n//2:],
            return_series.iloc[n//2:]
        )
        
        if np.isnan(first_half_ic) or np.isnan(second_half_ic):
            return {'decay_rate': 0.0, 'half_life': np.inf}
        
        decay_rate = (abs(first_half_ic) - abs(second_half_ic)) / abs(first_half_ic) if first_half_ic != 0 else 0
        
        half_life = np.log(2) / abs(decay_rate) if decay_rate != 0 else np.inf
        
        return {
            'decay_rate': round(decay_rate, 4),
            'half_life': round(half_life, 2) if half_life != np.inf else np.inf
        }
    
    def validate_all_indicators(
        self,
        indicator_df: pd.DataFrame,
        return_series: pd.Series
    ) -> pd.DataFrame:
        """
        验证所有指标
        
        Args:
            indicator_df: 指标数据DataFrame，每列为一个指标
            return_series: 未来收益率序列
            
        Returns:
            验证结果DataFrame
        """
        results = []
        
        for column in indicator_df.columns:
            if column in ['date', 'stock_code', 'return_1d']:
                continue
            
            result = self.validate_indicator(
                indicator_df[column],
                return_series,
                column
            )
            
            results.append({
                'indicator': result.indicator_name,
                'IC': result.ic,
                'IR': result.ir,
                'p-value': result.ic_pvalue,
                'is_valid': result.is_valid,
                'decay_rate': result.decay_rate,
                'half_life': result.half_life,
                'recommendation': result.recommendation
            })
        
        return pd.DataFrame(results).sort_values('IC', key=abs, ascending=False)
    
    def get_valid_indicators(self) -> List[str]:
        """获取有效指标列表"""
        return [
            name for name, result in self.validation_results.items()
            if result.is_valid
        ]
    
    def get_indicator_report(self) -> str:
        """生成指标验证报告"""
        if not self.validation_results:
            return "暂无验证结果"
        
        report_lines = [
            "=" * 60,
            "技术指标验证报告".center(60),
            "=" * 60,
            f"\n验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"验证标准: IC绝对值 >= {self.ic_threshold}, IR绝对值 >= {self.ir_threshold}",
            f"\n共验证 {len(self.validation_results)} 个指标",
            f"有效指标: {len(self.get_valid_indicators())} 个",
            "\n" + "-" * 60,
            "详细结果:",
            "-" * 60
        ]
        
        for name, result in sorted(
            self.validation_results.items(),
            key=lambda x: abs(x[1].ic),
            reverse=True
        ):
            status = "✓ 有效" if result.is_valid else "✗ 无效"
            report_lines.append(
                f"\n{result.indicator_name}: {status}"
            )
            report_lines.append(f"  IC: {result.ic:.4f}, IR: {result.ir:.4f}, p-value: {result.ic_pvalue:.4f}")
            report_lines.append(f"  衰减率: {result.decay_rate:.2%}, 半衰期: {result.half_life}")
            report_lines.append(f"  建议: {result.recommendation}")
        
        return "\n".join(report_lines)
    
    def save_results(self, output_path: str = "data/indicator_validation_results.json"):
        """保存验证结果"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        results_dict = {
            name: {
                'ic': result.ic,
                'ir': result.ir,
                'ic_pvalue': result.ic_pvalue,
                'is_valid': result.is_valid,
                'decay_rate': result.decay_rate,
                'half_life': result.half_life if result.half_life != np.inf else None,
                'recommendation': result.recommendation,
                'validation_date': result.validation_date
            }
            for name, result in self.validation_results.items()
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results_dict, f, ensure_ascii=False, indent=2)
        
        print(f"验证结果已保存到: {output_path}")


class TechnicalIndicatorValidator:
    """技术指标验证器 - 统一接口"""
    
    def __init__(self):
        """初始化技术指标验证器"""
        self.validator = IndicatorValidator()
        self._validation_summary = None
    
    def validate(
        self,
        data: pd.DataFrame,
        indicator_columns: List[str] = None,
        return_column: str = 'return_1d'
    ) -> Dict:
        """
        验证技术指标
        
        Args:
            data: 包含指标和收益率的数据
            indicator_columns: 指标列名列表，如果为None则自动检测
            return_column: 收益率列名
            
        Returns:
            验证结果字典
        """
        if return_column not in data.columns:
            return {
                'success': False,
                'message': f'收益率列 {return_column} 不存在'
            }
        
        if indicator_columns is None:
            exclude_columns = {'date', 'stock_code', 'return_1d', 'close', 'open', 'high', 'low', 'volume', 'amount'}
            indicator_columns = [col for col in data.columns if col not in exclude_columns]
        
        if not indicator_columns:
            return {
                'success': False,
                'message': '未找到指标列'
            }
        
        results_df = self.validator.validate_all_indicators(
            data[indicator_columns],
            data[return_column]
        )
        
        self._validation_summary = {
            'total_indicators': len(indicator_columns),
            'valid_indicators': len(self.validator.get_valid_indicators()),
            'results': results_df.to_dict('records'),
            'validation_date': datetime.now().strftime('%Y-%m-%d')
        }
        
        return {
            'success': True,
            'summary': self._validation_summary,
            'valid_indicators': self.validator.get_valid_indicators()
        }
    
    def get_report(self) -> str:
        """获取验证报告"""
        return self.validator.get_indicator_report()
    
    def save_results(self, output_path: str = "data/indicator_validation_results.json"):
        """保存验证结果"""
        self.validator.save_results(output_path)


if __name__ == "__main__":
    np.random.seed(42)
    n_days = 252
    dates = pd.date_range(start='2023-01-01', periods=n_days, freq='B')
    
    data = pd.DataFrame({
        'date': dates,
        'rsi_14': np.random.uniform(30, 70, n_days),
        'macd': np.random.normal(0, 1, n_days),
        'momentum_20': np.random.normal(0, 0.02, n_days),
        'volatility_20': np.random.uniform(0.01, 0.05, n_days),
        'return_1d': np.random.normal(0.001, 0.02, n_days)
    })
    
    validator = TechnicalIndicatorValidator()
    result = validator.validate(data)
    
    print(validator.get_report())
    validator.save_results()
