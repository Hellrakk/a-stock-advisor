#!/usr/bin/env python3
"""
因子评估与监控模块
功能：监控因子表现，避免因子误用
- 滚动窗口因子评估
- 因子表现趋势分析
- 因子相关性监控
- 因子有效性预警
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json

class FactorMonitor:
    """因子监控类"""
    
    def __init__(self):
        """初始化"""
        self.logger = logging.getLogger(__name__)
        self.factor_history_file = 'data/factor_history.json'
        self.factor_alert_file = 'logs/factor_alerts.log'
        os.makedirs(os.path.dirname(self.factor_history_file), exist_ok=True)
        os.makedirs(os.path.dirname(self.factor_alert_file), exist_ok=True)
        
        # 配置因子监控参数
        self.config = {
            'rolling_window': 20,  # 滚动窗口大小（交易日）
            'min_history': 10,     # 最小历史数据点
            'alert_thresholds': {
                'ic_drop': 0.5,       # IC值下降阈值
                'ir_drop': 0.5,       # IR值下降阈值
                'correlation_high': 0.8,  # 因子相关性过高阈值
                'performance_degradation': 0.3,  # 性能下降阈值
            }
        }
        
        # 加载因子历史数据
        self.factor_history = self._load_factor_history()
    
    def _load_factor_history(self):
        """
        加载因子历史数据
        
        Returns:
            dict: 因子历史数据
        """
        try:
            if os.path.exists(self.factor_history_file):
                with open(self.factor_history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"加载因子历史数据失败: {e}")
        return {}
    
    def _save_factor_history(self):
        """
        保存因子历史数据
        """
        try:
            with open(self.factor_history_file, 'w', encoding='utf-8') as f:
                json.dump(self.factor_history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存因子历史数据失败: {e}")
    
    def evaluate_factors(self, factor_data, date=None):
        """
        评估因子表现
        
        Args:
            factor_data: 因子数据，包含因子值和收益率
            date: 评估日期
            
        Returns:
            dict: 因子评估结果
        """
        try:
            if factor_data is None or len(factor_data) == 0:
                return {
                    'status': 'failed',
                    'message': '因子数据为空',
                    'details': {}
                }
            
            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')
            
            evaluation_results = {
                'date': date,
                'factors': {},
                'correlation_matrix': {}
            }
            
            # 计算每个因子的IC和IR
            factor_cols = [col for col in factor_data.columns if 'factor' in col.lower()]
            for factor_col in factor_cols:
                if 'return' in factor_data.columns:
                    # 计算IC值
                    ic = factor_data[factor_col].corr(factor_data['return'])
                    
                    # 计算IR值（假设使用历史数据）
                    ir = 0
                    if factor_col in self.factor_history:
                        history = self.factor_history[factor_col]
                        if len(history) >= self.config['min_history']:
                            ic_values = [h['ic'] for h in history[-self.config['min_history']:]]
                            if ic_values:
                                ir = np.mean(ic_values) / (np.std(ic_values) + 1e-8)
                    
                    # 计算因子收益率
                    # 这里简化处理，实际应该根据因子值排序后计算多空收益
                    factor_returns = []
                    try:
                        # 按因子值排序，取前20%和后20%
                        sorted_data = factor_data.sort_values(by=factor_col)
                        top_20 = sorted_data.tail(int(len(sorted_data) * 0.2))
                        bottom_20 = sorted_data.head(int(len(sorted_data) * 0.2))
                        factor_return = top_20['return'].mean() - bottom_20['return'].mean()
                        factor_returns.append(factor_return)
                    except:
                        factor_return = 0
                    
                    evaluation_results['factors'][factor_col] = {
                        'ic': float(ic),
                        'ir': float(ir),
                        'return': float(factor_return),
                        'count': len(factor_data)
                    }
            
            # 计算因子相关性矩阵
            if len(factor_cols) > 1:
                corr_matrix = factor_data[factor_cols].corr()
                for i, factor1 in enumerate(factor_cols):
                    for j, factor2 in enumerate(factor_cols):
                        if i < j:
                            key = f"{factor1}_{factor2}"
                            evaluation_results['correlation_matrix'][key] = float(corr_matrix.iloc[i, j])
            
            # 更新因子历史数据
            self._update_factor_history(evaluation_results)
            
            # 检查因子表现异常
            alerts = self._check_factor_alerts(evaluation_results)
            if alerts:
                evaluation_results['alerts'] = alerts
            
            return {
                'status': 'passed',
                'message': '因子评估完成',
                'details': evaluation_results
            }
        except Exception as e:
            self.logger.error(f"因子评估失败: {e}")
            return {
                'status': 'error',
                'message': f'评估过程出错: {str(e)}',
                'details': {}
            }
    
    def _update_factor_history(self, evaluation_results):
        """
        更新因子历史数据
        
        Args:
            evaluation_results: 因子评估结果
        """
        date = evaluation_results['date']
        
        for factor_name, factor_stats in evaluation_results['factors'].items():
            if factor_name not in self.factor_history:
                self.factor_history[factor_name] = []
            
            # 添加新的评估数据
            self.factor_history[factor_name].append({
                'date': date,
                'ic': factor_stats['ic'],
                'ir': factor_stats['ir'],
                'return': factor_stats['return']
            })
            
            # 只保留最近的滚动窗口数据
            if len(self.factor_history[factor_name]) > self.config['rolling_window']:
                self.factor_history[factor_name] = self.factor_history[factor_name][-self.config['rolling_window']:]
        
        # 保存历史数据
        self._save_factor_history()
    
    def _check_factor_alerts(self, evaluation_results):
        """
        检查因子表现异常
        
        Args:
            evaluation_results: 因子评估结果
            
        Returns:
            list: 异常警报
        """
        alerts = []
        date = evaluation_results['date']
        
        # 检查每个因子的表现
        for factor_name, factor_stats in evaluation_results['factors'].items():
            # 检查IC值下降
            if factor_name in self.factor_history:
                history = self.factor_history[factor_name]
                if len(history) >= 2:
                    # 计算IC值变化
                    current_ic = factor_stats['ic']
                    previous_ic = history[-2]['ic'] if len(history) > 1 else 0
                    
                    if previous_ic != 0:
                        ic_change = (current_ic - previous_ic) / abs(previous_ic)
                        if ic_change < -self.config['alert_thresholds']['ic_drop']:
                            alert = {
                                'date': date,
                                'factor': factor_name,
                                'type': 'ic_drop',
                                'message': f'因子{factor_name} IC值大幅下降: {previous_ic:.3f} → {current_ic:.3f}',
                                'severity': 'high'
                            }
                            alerts.append(alert)
                            self._log_alert(alert)
            
            # 检查IR值下降
            if factor_name in self.factor_history:
                history = self.factor_history[factor_name]
                if len(history) >= 2:
                    current_ir = factor_stats['ir']
                    previous_ir = history[-2]['ir'] if len(history) > 1 else 0
                    
                    if previous_ir != 0:
                        ir_change = (current_ir - previous_ir) / abs(previous_ir)
                        if ir_change < -self.config['alert_thresholds']['ir_drop']:
                            alert = {
                                'date': date,
                                'factor': factor_name,
                                'type': 'ir_drop',
                                'message': f'因子{factor_name} IR值大幅下降: {previous_ir:.3f} → {current_ir:.3f}',
                                'severity': 'high'
                            }
                            alerts.append(alert)
                            self._log_alert(alert)
        
        # 检查因子相关性
        for corr_key, correlation in evaluation_results['correlation_matrix'].items():
            if abs(correlation) > self.config['alert_thresholds']['correlation_high']:
                factor1, factor2 = corr_key.split('_')
                alert = {
                    'date': date,
                    'factor': f'{factor1}_{factor2}',
                    'type': 'high_correlation',
                    'message': f'因子{factor1}和{factor2}相关性过高: {correlation:.3f}',
                    'severity': 'medium'
                }
                alerts.append(alert)
                self._log_alert(alert)
        
        return alerts
    
    def _log_alert(self, alert):
        """
        记录因子警报
        
        Args:
            alert: 警报信息
        """
        try:
            with open(self.factor_alert_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(alert, ensure_ascii=False) + '\n')
            
            # 记录到日志
            self.logger.warning(f"因子警报: {alert['message']}")
        except Exception as e:
            self.logger.error(f"记录因子警报失败: {e}")
    
    def get_factor_summary(self):
        """
        获取因子表现摘要
        
        Returns:
            str: 因子表现摘要
        """
        try:
            summary = []
            summary.append('📊 因子监控报告')
            summary.append('────────────────────')
            
            # 检查是否有因子历史数据
            if not self.factor_history:
                summary.append('暂无因子历史数据')
                return '\n'.join(summary)
            
            # 显示每个因子的最近表现
            for factor_name, history in self.factor_history.items():
                if history:
                    latest = history[-1]
                    summary.append(f'')
                    summary.append(f'📈 {factor_name}')
                    summary.append(f'├─ 最近IC: {latest["ic"]:.3f}')
                    summary.append(f'├─ 最近IR: {latest["ir"]:.3f}')
                    summary.append(f'├─ 最近收益: {latest["return"]:.3f}')
                    summary.append(f'└─ 评估日期: {latest["date"]}')
            
            # 检查最近的警报
            if os.path.exists(self.factor_alert_file):
                with open(self.factor_alert_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    if lines:
                        # 获取最近5条警报
                        recent_alerts = [json.loads(line) for line in lines[-5:]]
                        if recent_alerts:
                            summary.append('')
                            summary.append('⚠️ 最近警报:')
                            for alert in reversed(recent_alerts):
                                summary.append(f'- {alert["date"]}: {alert["message"]}')
            
            return '\n'.join(summary)
        except Exception as e:
            self.logger.error(f"获取因子摘要失败: {e}")
            return '📊 因子监控报告\n────────────────────\n获取摘要失败'
    
    def analyze_factor_trends(self, factor_name, window=10):
        """
        分析因子表现趋势
        
        Args:
            factor_name: 因子名称
            window: 分析窗口大小
            
        Returns:
            dict: 因子趋势分析结果
        """
        try:
            if factor_name not in self.factor_history:
                return {
                    'status': 'failed',
                    'message': f'因子{factor_name}无历史数据',
                    'details': {}
                }
            
            history = self.factor_history[factor_name]
            if len(history) < window:
                return {
                    'status': 'warning',
                    'message': f'因子{factor_name}历史数据不足',
                    'details': {
                        'available_data': len(history),
                        'required_data': window
                    }
                }
            
            # 提取最近的历史数据
            recent_history = history[-window:]
            dates = [h['date'] for h in recent_history]
            ic_values = [h['ic'] for h in recent_history]
            ir_values = [h['ir'] for h in recent_history]
            returns = [h['return'] for h in recent_history]
            
            # 计算趋势
            ic_trend = np.polyfit(range(window), ic_values, 1)[0]
            ir_trend = np.polyfit(range(window), ir_values, 1)[0]
            return_trend = np.polyfit(range(window), returns, 1)[0]
            
            # 计算平均值和标准差
            ic_mean = np.mean(ic_values)
            ic_std = np.std(ic_values)
            ir_mean = np.mean(ir_values)
            ir_std = np.std(ir_values)
            return_mean = np.mean(returns)
            return_std = np.std(returns)
            
            return {
                'status': 'passed',
                'message': f'因子{factor_name}趋势分析完成',
                'details': {
                    'factor_name': factor_name,
                    'window': window,
                    'dates': dates,
                    'ic_values': ic_values,
                    'ir_values': ir_values,
                    'returns': returns,
                    'trends': {
                        'ic_trend': float(ic_trend),
                        'ir_trend': float(ir_trend),
                        'return_trend': float(return_trend)
                    },
                    'statistics': {
                        'ic_mean': float(ic_mean),
                        'ic_std': float(ic_std),
                        'ir_mean': float(ir_mean),
                        'ir_std': float(ir_std),
                        'return_mean': float(return_mean),
                        'return_std': float(return_std)
                    }
                }
            }
        except Exception as e:
            self.logger.error(f"因子趋势分析失败: {e}")
            return {
                'status': 'error',
                'message': f'分析过程出错: {str(e)}',
                'details': {}
            }
    
    def recommend_factor_weights(self):
        """
        推荐因子权重
        
        Returns:
            dict: 因子权重推荐
        """
        try:
            recommendations = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'factors': {},
                'total_weight': 0
            }
            
            # 基于最近的因子表现计算权重
            total_score = 0
            factor_scores = {}
            
            for factor_name, history in self.factor_history.items():
                if len(history) >= 5:
                    # 使用最近5个周期的IC和IR计算得分
                    recent_history = history[-5:]
                    avg_ic = np.mean([h['ic'] for h in recent_history])
                    avg_ir = np.mean([h['ir'] for h in recent_history])
                    
                    # 计算综合得分（IC和IR的加权平均）
                    score = abs(avg_ic) * 0.6 + abs(avg_ir) * 0.4
                    factor_scores[factor_name] = score
                    total_score += score
            
            # 计算权重
            if total_score > 0:
                for factor_name, score in factor_scores.items():
                    weight = score / total_score
                    recommendations['factors'][factor_name] = float(weight)
                    recommendations['total_weight'] += weight
            
            return {
                'status': 'passed',
                'message': '因子权重推荐完成',
                'details': recommendations
            }
        except Exception as e:
            self.logger.error(f"因子权重推荐失败: {e}")
            return {
                'status': 'error',
                'message': f'推荐过程出错: {str(e)}',
                'details': {}
            }
