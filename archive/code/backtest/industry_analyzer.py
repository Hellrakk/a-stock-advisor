#!/usr/bin/env python3
"""
行业分析模块
基于持仓计算真实的行业权重、偏离度、集中度
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IndustryAnalyzer:
    """行业分析器"""
    
    DEFAULT_INDUSTRY_WEIGHTS = {
        '银行': 15.0,
        '证券': 5.0,
        '保险': 5.0,
        '房地产': 3.0,
        '科技': 12.0,
        '半导体': 8.0,
        '电子': 6.0,
        '医药': 10.0,
        '汽车': 5.0,
        '电力': 4.0,
        '煤炭': 3.0,
        '石油': 3.0,
        '有色金属': 5.0,
        '贵金属': 2.0,
        '白酒': 8.0,
        '食品': 5.0,
        '零售': 3.0,
        '传媒': 2.0,
        '通信': 4.0,
        '建筑': 3.0,
        '钢铁': 2.0,
        '基建': 3.0,
        '机械': 4.0,
        '电气': 4.0,
        '家电': 3.0,
        '轻工': 2.0,
        '其他': 5.0
    }
    
    def __init__(self, benchmark_weights: Dict[str, float] = None):
        """
        初始化
        
        Args:
            benchmark_weights: 基准行业权重，默认使用沪深300近似权重
        """
        self.benchmark_weights = benchmark_weights or self.DEFAULT_INDUSTRY_WEIGHTS.copy()
    
    def calculate_industry_weights(
        self, 
        positions: List[Dict]
    ) -> Dict[str, float]:
        """
        计算持仓的行业权重
        
        Args:
            positions: 持仓列表 [{'code': xxx, 'name': xxx, 'weight': xxx, 'industry': xxx}, ...]
            
        Returns:
            {行业名: 权重百分比}
        """
        if not positions:
            return {}
        
        industry_weights = defaultdict(float)
        
        for pos in positions:
            industry = pos.get('industry', '其他')
            weight = pos.get('weight', 0)
            industry_weights[industry] += weight
        
        total_weight = sum(industry_weights.values())
        if total_weight > 0:
            industry_weights = {
                k: round(v / total_weight * 100, 2) 
                for k, v in industry_weights.items()
            }
        
        logger.info(f"✓ 计算行业权重: {len(industry_weights)}个行业")
        return dict(sorted(industry_weights.items(), key=lambda x: x[1], reverse=True))
    
    def calculate_industry_deviation(
        self, 
        portfolio_weights: Dict[str, float]
    ) -> Dict[str, Dict]:
        """
        计算行业偏离度
        
        Args:
            portfolio_weights: 组合行业权重
            
        Returns:
            {行业名: {'portfolio_weight': xxx, 'benchmark_weight': xxx, 'deviation': xxx}}
        """
        deviations = {}
        
        all_industries = set(portfolio_weights.keys()) | set(self.benchmark_weights.keys())
        
        for industry in all_industries:
            port_weight = portfolio_weights.get(industry, 0)
            bench_weight = self.benchmark_weights.get(industry, 0)
            deviation = port_weight - bench_weight
            
            deviations[industry] = {
                'portfolio_weight': round(port_weight, 2),
                'benchmark_weight': round(bench_weight, 2),
                'deviation': round(deviation, 2)
            }
        
        deviations = dict(
            sorted(deviations.items(), key=lambda x: abs(x[1]['deviation']), reverse=True)
        )
        
        return deviations
    
    def calculate_industry_concentration(
        self, 
        portfolio_weights: Dict[str, float],
        top_n: int = 3
    ) -> Dict:
        """
        计算行业集中度
        
        Args:
            portfolio_weights: 组合行业权重
            top_n: 计算前N大行业的集中度
            
        Returns:
            {
                'top_industries': [(行业名, 权重), ...],
                'top_concentration': 前N大行业集中度,
                'hhi': 赫芬达尔指数,
                'effective_n': 有效行业数量
            }
        """
        if not portfolio_weights:
            return {
                'top_industries': [],
                'top_concentration': 0,
                'hhi': 0,
                'effective_n': 0
            }
        
        sorted_weights = sorted(portfolio_weights.items(), key=lambda x: x[1], reverse=True)
        top_industries = sorted_weights[:top_n]
        top_concentration = sum(w for _, w in top_industries)
        
        hhi = sum(w ** 2 for w in portfolio_weights.values())
        
        effective_n = 1 / (hhi / 10000) if hhi > 0 else 0
        
        return {
            'top_industries': [(ind, round(w, 2)) for ind, w in top_industries],
            'top_concentration': round(top_concentration, 2),
            'hhi': round(hhi, 2),
            'effective_n': round(effective_n, 2)
        }
    
    def analyze_portfolio_industry(
        self, 
        positions: List[Dict]
    ) -> Dict:
        """
        综合分析组合行业配置
        
        Args:
            positions: 持仓列表
            
        Returns:
            完整的行业分析结果
        """
        portfolio_weights = self.calculate_industry_weights(positions)
        deviations = self.calculate_industry_deviation(portfolio_weights)
        concentration = self.calculate_industry_concentration(portfolio_weights)
        
        over_weighted = {
            k: v for k, v in deviations.items() 
            if v['deviation'] > 5
        }
        under_weighted = {
            k: v for k, v in deviations.items() 
            if v['deviation'] < -5
        }
        
        max_deviation = max(
            abs(v['deviation']) for v in deviations.values()
        ) if deviations else 0
        
        return {
            'portfolio_weights': portfolio_weights,
            'deviations': deviations,
            'concentration': concentration,
            'over_weighted': over_weighted,
            'under_weighted': under_weighted,
            'max_deviation': round(max_deviation, 2),
            'industry_count': len(portfolio_weights),
            'summary': self._generate_summary(
                portfolio_weights, 
                deviations, 
                concentration
            )
        }
    
    def _generate_summary(
        self, 
        weights: Dict[str, float],
        deviations: Dict[str, Dict],
        concentration: Dict
    ) -> str:
        """生成行业分析摘要"""
        if not weights:
            return "无持仓数据"
        
        top_industry = list(weights.keys())[0] if weights else 'N/A'
        top_weight = weights.get(top_industry, 0)
        
        max_dev_item = max(
            deviations.items(), 
            key=lambda x: abs(x[1]['deviation'])
        ) if deviations else (None, {'deviation': 0})
        
        summary_parts = [
            f"第一大行业{top_industry}权重{top_weight:.1f}%",
            f"前三大行业集中度{concentration['top_concentration']:.1f}%",
            f"最大偏离{max_dev_item[0]}({max_dev_item[1]['deviation']:+.1f}%)" if max_dev_item[0] else ""
        ]
        
        return "，".join(p for p in summary_parts if p)
    
    def check_industry_limits(
        self, 
        positions: List[Dict],
        max_single_industry: float = 30.0,
        max_deviation: float = 15.0
    ) -> Dict:
        """
        检查行业配置是否超限
        
        Args:
            positions: 持仓列表
            max_single_industry: 单行业最大权重限制
            max_deviation: 最大偏离度限制
            
        Returns:
            {
                'passed': 是否通过,
                'violations': [违规项...],
                'warnings': [警告项...]
            }
        """
        portfolio_weights = self.calculate_industry_weights(positions)
        deviations = self.calculate_industry_deviation(portfolio_weights)
        
        violations = []
        warnings = []
        
        for industry, weight in portfolio_weights.items():
            if weight > max_single_industry:
                violations.append(
                    f"行业{industry}权重{weight:.1f}%超过限制{max_single_industry}%"
                )
            elif weight > max_single_industry * 0.8:
                warnings.append(
                    f"行业{industry}权重{weight:.1f}%接近限制{max_single_industry}%"
                )
        
        for industry, dev_info in deviations.items():
            deviation = abs(dev_info['deviation'])
            if deviation > max_deviation:
                violations.append(
                    f"行业{industry}偏离度{dev_info['deviation']:+.1f}%超过限制±{max_deviation}%"
                )
            elif deviation > max_deviation * 0.8:
                warnings.append(
                    f"行业{industry}偏离度{dev_info['deviation']:+.1f}%接近限制±{max_deviation}%"
                )
        
        return {
            'passed': len(violations) == 0,
            'violations': violations,
            'warnings': warnings
        }
    
    def suggest_rebalance(
        self, 
        positions: List[Dict],
        target_weights: Dict[str, float] = None
    ) -> List[Dict]:
        """
        建议再平衡操作
        
        Args:
            positions: 当前持仓
            target_weights: 目标行业权重（默认使用基准权重）
            
        Returns:
            建议操作列表
        """
        if target_weights is None:
            target_weights = self.benchmark_weights
        
        portfolio_weights = self.calculate_industry_weights(positions)
        deviations = self.calculate_industry_deviation(portfolio_weights)
        
        suggestions = []
        
        for industry, dev_info in deviations.items():
            deviation = dev_info['deviation']
            
            if abs(deviation) > 10:
                if deviation > 0:
                    suggestions.append({
                        'industry': industry,
                        'action': '减仓',
                        'current_weight': dev_info['portfolio_weight'],
                        'target_weight': dev_info['benchmark_weight'],
                        'adjustment': round(-deviation, 2),
                        'reason': f'超配{deviation:.1f}%'
                    })
                else:
                    suggestions.append({
                        'industry': industry,
                        'action': '加仓',
                        'current_weight': dev_info['portfolio_weight'],
                        'target_weight': dev_info['benchmark_weight'],
                        'adjustment': round(abs(deviation), 2),
                        'reason': f'低配{abs(deviation):.1f}%'
                    })
        
        return sorted(suggestions, key=lambda x: abs(x['adjustment']), reverse=True)


if __name__ == "__main__":
    analyzer = IndustryAnalyzer()
    
    print("=" * 60)
    print("行业分析模块测试")
    print("=" * 60)
    
    test_positions = [
        {'code': 'sh600000', 'name': '浦发银行', 'weight': 0.15, 'industry': '银行'},
        {'code': 'sh601899', 'name': '紫金矿业', 'weight': 0.12, 'industry': '有色金属'},
        {'code': 'sz002703', 'name': '浙江世宝', 'weight': 0.08, 'industry': '汽车'},
        {'code': 'sz002678', 'name': '珠江钢琴', 'weight': 0.06, 'industry': '轻工'},
        {'code': 'sh601398', 'name': '工商银行', 'weight': 0.10, 'industry': '银行'},
    ]
    
    print("\n【行业权重】")
    weights = analyzer.calculate_industry_weights(test_positions)
    for ind, w in weights.items():
        print(f"  {ind}: {w:.2f}%")
    
    print("\n【行业偏离度】")
    deviations = analyzer.calculate_industry_deviation(weights)
    for ind, dev in list(deviations.items())[:5]:
        print(f"  {ind}: 组合{dev['portfolio_weight']:.1f}% vs 基准{dev['benchmark_weight']:.1f}% = 偏离{dev['deviation']:+.1f}%")
    
    print("\n【行业集中度】")
    concentration = analyzer.calculate_industry_concentration(weights)
    print(f"  前三大行业: {concentration['top_industries']}")
    print(f"  集中度: {concentration['top_concentration']:.2f}%")
    print(f"  HHI指数: {concentration['hhi']:.2f}")
    print(f"  有效行业数: {concentration['effective_n']:.2f}")
    
    print("\n【行业限制检查】")
    check_result = analyzer.check_industry_limits(test_positions)
    print(f"  通过: {check_result['passed']}")
    if check_result['violations']:
        print(f"  违规: {check_result['violations']}")
    if check_result['warnings']:
        print(f"  警告: {check_result['warnings']}")
