#!/usr/bin/env python3
"""
绩效管理模块
- 因子绩效管理
- 策略绩效管理
- 指标绩效管理
- 绩效报告生成

作者: 绩效管理团队
日期: 2026-03-05
版本: v1.0
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PerformanceManager:
    """绩效管理器"""
    
    def __init__(self):
        self.data_dir = 'data'
        self.reports_dir = 'reports'
        
        # 绩效数据文件
        self.factor_performance_file = os.path.join(self.data_dir, 'factor_performance.json')
        self.strategy_performance_file = os.path.join(self.data_dir, 'strategy_performance.json')
        self.indicator_performance_file = os.path.join(self.data_dir, 'indicator_performance.json')
        
        # 加载绩效数据
        self.factor_performance = self._load_performance(self.factor_performance_file)
        self.strategy_performance = self._load_performance(self.strategy_performance_file)
        self.indicator_performance = self._load_performance(self.indicator_performance_file)
    
    def _load_performance(self, file_path: str) -> Dict:
        """加载绩效数据"""
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}
    
    def _save_performance(self, file_path: str, data: Dict):
        """保存绩效数据"""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def update_factor_performance(self, factor_name: str, performance_data: Dict):
        """更新因子绩效"""
        self.factor_performance[factor_name] = {
            'last_update': datetime.now().isoformat(),
            'data': performance_data
        }
        self._save_performance(self.factor_performance_file, self.factor_performance)
    
    def update_strategy_performance(self, strategy_name: str, performance_data: Dict):
        """更新策略绩效"""
        self.strategy_performance[strategy_name] = {
            'last_update': datetime.now().isoformat(),
            'data': performance_data
        }
        self._save_performance(self.strategy_performance_file, self.strategy_performance)
    
    def update_indicator_performance(self, indicator_name: str, performance_data: Dict):
        """更新指标绩效"""
        self.indicator_performance[indicator_name] = {
            'last_update': datetime.now().isoformat(),
            'data': performance_data
        }
        self._save_performance(self.indicator_performance_file, self.indicator_performance)
    
    def get_factor_performance_summary(self) -> pd.DataFrame:
        """获取因子绩效摘要"""
        if not self.factor_performance:
            return pd.DataFrame()
        
        data = []
        for factor_name, perf in self.factor_performance.items():
            perf_data = perf.get('data', {})
            data.append({
                'factor_name': factor_name,
                'ic_mean': perf_data.get('ic_mean', 0),
                'ir': perf_data.get('ir', 0),
                'win_rate': perf_data.get('win_rate', 0),
                'sharpe': perf_data.get('sharpe', 0),
                'last_update': perf.get('last_update', 'N/A')
            })
        
        return pd.DataFrame(data)
    
    def get_strategy_performance_summary(self) -> pd.DataFrame:
        """获取策略绩效摘要"""
        if not self.strategy_performance:
            return pd.DataFrame()
        
        data = []
        for strategy_name, perf in self.strategy_performance.items():
            perf_data = perf.get('data', {})
            data.append({
                'strategy_name': strategy_name,
                'annual_return': perf_data.get('annual_return', 0),
                'sharpe': perf_data.get('sharpe', 0),
                'max_drawdown': perf_data.get('max_drawdown', 0),
                'win_rate': perf_data.get('win_rate', 0),
                'last_update': perf.get('last_update', 'N/A')
            })
        
        return pd.DataFrame(data)
    
    def generate_performance_report(self) -> str:
        """生成绩效报告"""
        now = datetime.now()
        
        report = f"""# 绩效管理报告

**生成时间**: {now.strftime('%Y-%m-%d %H:%M:%S')}

## 📊 因子绩效

"""
        
        factor_summary = self.get_factor_performance_summary()
        if not factor_summary.empty:
            report += f"因子总数: {len(factor_summary)}\n\n"
            report += factor_summary.to_string(index=False)
        else:
            report += "暂无因子绩效数据\n"
        
        report += f"""

## 📈 策略绩效

"""
        
        strategy_summary = self.get_strategy_performance_summary()
        if not strategy_summary.empty:
            report += f"策略总数: {len(strategy_summary)}\n\n"
            report += strategy_summary.to_string(index=False)
        else:
            report += "暂无策略绩效数据\n"
        
        report += f"""

## 📋 指标绩效

"""
        
        if self.indicator_performance:
            report += f"指标总数: {len(self.indicator_performance)}\n"
            for indicator_name, perf in self.indicator_performance.items():
                report += f"- {indicator_name}: 最后更新 {perf.get('last_update', 'N/A')}\n"
        else:
            report += "暂无指标绩效数据\n"
        
        return report


if __name__ == '__main__':
    manager = PerformanceManager()
    print(manager.generate_performance_report())
