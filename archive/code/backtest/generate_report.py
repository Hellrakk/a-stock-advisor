#!/usr/bin/env python3
"""
报告生成器
生成每日选股报告
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List

def generate_daily_recommendation(run_date: datetime, factor_data: pd.DataFrame,
                                 score_model, stock_selector, risk_controller,
                                 n: int = 10, market_data: Dict = None) -> str:
    """
    生成每日推荐报告
    
    Args:
        run_date: 运行日期
        factor_data: 因子数据
        score_model: 得分模型
        stock_selector: 选股器
        risk_controller: 风险控制器
        n: 选股数量
        market_data: 市场数据
        
    Returns:
        Markdown格式的报告
    """
    report_date = run_date.strftime('%Y-%m-%d')
    
    lines = []
    lines.append("# A股量化选股日报")
    lines.append("")
    lines.append(f"**日期**: {report_date}")
    lines.append(f"**生成时间**: {run_date.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    
    # 获取最新数据日期
    if 'date' in factor_data.columns:
        latest_date = factor_data['date'].max()
        if pd.notna(latest_date):
            lines.append(f"**数据日期**: {latest_date.strftime('%Y-%m-%d')}")
            lines.append("")
    
    # 市场概览
    if market_data:
        lines.append("## 📊 市场概览")
        lines.append("")
        lines.append("| 指数 | 最新价 | 涨跌幅 |")
        lines.append("|------|--------|--------|")
        for idx, data in market_data.items():
            lines.append(f"| {data['name']} | {data['price']} | {data['change_pct']:.2f}% |")
        lines.append("")
    
    # 获取因子权重信息
    lines.append("## 🎯 因子权重")
    lines.append("")
    lines.append("| 因子 | 权重 | IC值 |")
    lines.append("|------|------|------|")
    
    total_weighted = 0
    for factor, weight in sorted(score_model.factor_weights.items(), key=lambda x: -x[1]):
        ic = score_model.factor_ic.get(factor, 0)
        if not pd.isna(weight):
            lines.append(f"| {factor} | {weight:.2%} | {ic:.3f} |")
            total_weighted += weight
    
    lines.append("")
    lines.append(f"*因子数量: {len(score_model.factor_weights)}个*")
    lines.append("")
    
    # 生成选股结果
    lines.append("## 📈 选股结果")
    lines.append("")

    if stock_selector.selected_stocks is not None:
        selected = stock_selector.selected_stocks.head(n)

        # 根据数据适配列名
        pe_col = 'PE_TTM' if 'PE_TTM' in selected.columns else ('pe_ttm' if 'pe_ttm' in selected.columns else None)
        pb_col = 'PB' if 'PB' in selected.columns else ('pb' if 'pb' in selected.columns else None)
        cap_col = '市值_亿' if '市值_亿' in selected.columns else ('market_cap' if 'market_cap' in selected.columns else None)
        name_col = '股票名称' if '股票名称' in selected.columns else ('stock_name' if 'stock_name' in selected.columns else None)
        industry_col = 'industry' if 'industry' in selected.columns else ('行业' if '行业' in selected.columns else None)

        lines.append("| 排名 | 股票代码 | 股票名称 | 行业 | 综合得分 | PE-TTM | PB | 市值(亿) |")
        lines.append("|------|----------|----------|------|----------|--------|-----|---------|")

        for idx, (stock_code, stock_data) in enumerate(selected.iterrows(), 1):
            score = stock_data.get('综合得分', 0)
            pe_ttm = stock_data.get(pe_col, 0) if pe_col else 0
            pb = stock_data.get(pb_col, 0) if pb_col else 0
            market_cap = stock_data.get(cap_col, 0) if cap_col else 0
            stock_name = stock_data.get(name_col, f'股票{stock_code}') if name_col else f'股票{stock_code}'
            industry = stock_data.get(industry_col, '未知') if industry_col else '未知'

            lines.append(f"| {idx} | {stock_code} | {stock_name} | {industry} | **{score:.2f}** | {pe_ttm:.2f} | {pb:.2f} | {market_cap:.2f} |")

        lines.append("")
    else:
        lines.append("*暂无选股结果*")
        lines.append("")
    
    # 行业暴露分析
    lines.append("## 🔍 行业暴露分析")
    lines.append("")
    if hasattr(stock_selector, '_analyze_industry_exposure') and stock_selector.selected_stocks is not None:
        # 模拟行业暴露分析
        industry_counts = {}
        for _, row in stock_selector.selected_stocks.head(n).iterrows():
            industry = row.get('industry', row.get('行业', '未知'))
            industry_counts[industry] = industry_counts.get(industry, 0) + 1
        
        if industry_counts:
            lines.append("| 行业 | 数量 | 占比 |")
            lines.append("|------|------|------|")
            total = sum(industry_counts.values())
            for industry, count in sorted(industry_counts.items(), key=lambda x: -x[1]):
                percentage = (count / total) * 100
                lines.append(f"| {industry} | {count} | {percentage:.1f}% |")
            lines.append("")
        else:
            lines.append("*暂无行业数据*")
            lines.append("")
    
    # 风险控制摘要
    if risk_controller and risk_controller.control_summary:
        lines.append("## 🛡️ 风险控制")
        lines.append("")
        
        # 格式化控制摘要
        summary_text = risk_controller.format_control_summary(risk_controller.control_summary)
        for line in summary_text.split('\n'):
            lines.append(f"{line}")
        
        lines.append("")
    
    # 投资建议
    lines.append("## 💡 投资建议")
    lines.append("")
    lines.append("### 建仓策略:")
    lines.append("- 分批建仓：9:30-11:30（分散执行）")
    lines.append("- 第一批(30%): 9:30-10:00")
    lines.append("- 第二批(30%): 10:00-10:30")
    lines.append("- 第三批(40%): 10:30-11:30")
    lines.append("")
    lines.append("### 止盈止损:")
    lines.append("- 动态止盈：基于波动率和市场环境调整")
    lines.append("- 动态止损：基于波动率和市场环境调整")
    lines.append("- 持仓时间：最长60天")
    lines.append("")
    lines.append("### 风险控制:")
    lines.append("- 单只股票最大持仓：12%")
    lines.append("- 单个行业最大持仓：30%")
    lines.append("- 组合最大回撤：15%")
    lines.append("")
    
    # 免责声明
    lines.append("---")
    lines.append("")
    lines.append("## ⚠️ 免责声明")
    lines.append("")
    lines.append("本报告由A股量化系统自动生成，仅供投资研究参考，不构成投资建议。")
    lines.append("")
    lines.append("**风险提示**:")
    lines.append("- 股票投资有风险，入市需谨慎")
    lines.append("- 本系统基于历史数据回测，不代表未来收益")
    lines.append("- 投资者应根据自身风险承受能力独立决策")
    lines.append("- 本系统不对投资结果负责")
    lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append(f"*Generated by A股量化系统 v2.1 at {run_date.strftime('%Y-%m-%d %H:%M:%S')}*")
    
    return "\n".join(lines)

def save_factor_scores(factor_scores: Dict[str, float], save_path: str):
    """
    保存因子得分
    
    Args:
        factor_scores: 因子得分字典
        save_path: 保存路径
    """
    import json
    
    # 处理NaN值
    clean_scores = {}
    for key, value in factor_scores.items():
        if pd.isna(value):
            clean_scores[key] = None
        else:
            clean_scores[key] = float(value)
    
    with open(save_path, 'w') as f:
        json.dump(clean_scores, f, indent=2)
