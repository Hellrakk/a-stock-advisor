#!/usr/bin/env python3
"""
自动化质量控制模块
功能：确保每日推送的质量和可靠性
- 数据质量检查
- 因子有效性验证
- 流程完整性验证
- 结果一致性检查
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json

class AutomatedQualityControl:
    """自动化质量控制类"""
    
    def __init__(self):
        """初始化"""
        self.logger = logging.getLogger(__name__)
        self.quality_log_file = 'logs/quality_control.log'
        os.makedirs(os.path.dirname(self.quality_log_file), exist_ok=True)
        
        # 配置质量控制参数
        self.config = {
            'data_quality_thresholds': {
                'min_stocks': 3000,  # 最小股票数量
                'max_missing_data': 0.05,  # 最大缺失数据比例
                'min_liquidity': 1000000,  # 最小流动性（成交额）
            },
            'factor_validity_thresholds': {
                'min_ic': 0.05,  # 最小IC值
                'min_ir': 0.3,   # 最小IR值
                'max_factor_correlation': 0.8,  # 最大因子相关性
            },
            'portfolio_constraints': {
                'max_industry_exposure': 0.3,  # 最大行业暴露
                'max_single_stock_weight': 0.12,  # 最大单股权重
                'min_diversification': 8,  # 最小股票数量
            }
        }
    
    def check_data_quality(self, stock_data):
        """
        检查数据质量
        
        Args:
            stock_data: 股票数据DataFrame
            
        Returns:
            dict: 数据质量检查结果
        """
        try:
            if stock_data is None or len(stock_data) == 0:
                return {
                    'status': 'failed',
                    'message': '数据为空',
                    'details': {}
                }
            
            # 检查股票数量
            stock_count = len(stock_data)
            min_stocks = self.config['data_quality_thresholds']['min_stocks']
            if stock_count < min_stocks:
                return {
                    'status': 'failed',
                    'message': f'股票数量不足：{stock_count} < {min_stocks}',
                    'details': {'stock_count': stock_count}
                }
            
            # 检查缺失数据
            missing_data = stock_data.isnull().sum().sum() / (stock_data.shape[0] * stock_data.shape[1])
            max_missing = self.config['data_quality_thresholds']['max_missing_data']
            if missing_data > max_missing:
                return {
                    'status': 'failed',
                    'message': f'缺失数据比例过高：{missing_data:.2f} > {max_missing}',
                    'details': {'missing_data_ratio': missing_data}
                }
            
            # 检查流动性
            if 'amount' in stock_data.columns:
                avg_liquidity = stock_data['amount'].mean()
                min_liquidity = self.config['data_quality_thresholds']['min_liquidity']
                if avg_liquidity < min_liquidity:
                    return {
                        'status': 'warning',
                        'message': f'平均流动性较低：{avg_liquidity:.2f} < {min_liquidity}',
                        'details': {'avg_liquidity': avg_liquidity}
                    }
            
            return {
                'status': 'passed',
                'message': '数据质量检查通过',
                'details': {
                    'stock_count': stock_count,
                    'missing_data_ratio': missing_data,
                    'avg_liquidity': stock_data['amount'].mean() if 'amount' in stock_data.columns else 'N/A'
                }
            }
        except Exception as e:
            self.logger.error(f"数据质量检查失败: {e}")
            return {
                'status': 'error',
                'message': f'检查过程出错: {str(e)}',
                'details': {}
            }
    
    def validate_factors(self, factor_data):
        """
        验证因子有效性
        
        Args:
            factor_data: 因子数据，包含因子值和收益率
            
        Returns:
            dict: 因子验证结果
        """
        try:
            if factor_data is None or len(factor_data) == 0:
                return {
                    'status': 'failed',
                    'message': '因子数据为空',
                    'details': {}
                }
            
            # 计算IC值
            ic_values = {}
            for factor_col in [col for col in factor_data.columns if 'factor' in col.lower()]:
                if 'return' in factor_data.columns:
                    ic = factor_data[factor_col].corr(factor_data['return'])
                    ic_values[factor_col] = ic
                    
                    # 检查IC阈值
                    if abs(ic) < self.config['factor_validity_thresholds']['min_ic']:
                        return {
                            'status': 'warning',
                            'message': f'因子{factor_col} IC值过低：{ic:.3f}',
                            'details': {'ic_values': ic_values}
                        }
            
            # 检查因子相关性
            factor_cols = [col for col in factor_data.columns if 'factor' in col.lower()]
            if len(factor_cols) > 1:
                corr_matrix = factor_data[factor_cols].corr()
                max_corr = corr_matrix.abs().max().max()
                if max_corr > self.config['factor_validity_thresholds']['max_factor_correlation']:
                    return {
                        'status': 'warning',
                        'message': f'因子相关性过高：{max_corr:.3f}',
                        'details': {'max_correlation': max_corr}
                    }
            
            return {
                'status': 'passed',
                'message': '因子验证通过',
                'details': {'ic_values': ic_values}
            }
        except Exception as e:
            self.logger.error(f"因子验证失败: {e}")
            return {
                'status': 'error',
                'message': f'验证过程出错: {str(e)}',
                'details': {}
            }
    
    def validate_portfolio(self, portfolio_data):
        """
        验证投资组合合规性
        
        Args:
            portfolio_data: 投资组合数据
            
        Returns:
            dict: 投资组合验证结果
        """
        try:
            if portfolio_data is None or not portfolio_data.get('selected_stocks'):
                return {
                    'status': 'failed',
                    'message': '投资组合数据为空',
                    'details': {}
                }
            
            selected_stocks = portfolio_data['selected_stocks']
            
            # 检查股票数量
            stock_count = len(selected_stocks)
            min_diversification = self.config['portfolio_constraints']['min_diversification']
            if stock_count < min_diversification:
                return {
                    'status': 'warning',
                    'message': f'投资组合多样性不足：{stock_count} < {min_diversification}',
                    'details': {'stock_count': stock_count}
                }
            
            # 检查行业暴露（如果有行业信息）
            if 'industry' in selected_stocks[0]:
                industry_counts = {}
                for stock in selected_stocks:
                    industry = stock.get('industry', '未知')
                    industry_counts[industry] = industry_counts.get(industry, 0) + 1
                
                max_industry_exposure = max(industry_counts.values()) / stock_count
                if max_industry_exposure > self.config['portfolio_constraints']['max_industry_exposure']:
                    return {
                        'status': 'failed',
                        'message': f'行业暴露过高：{max_industry_exposure:.2f} > {self.config["portfolio_constraints"]["max_industry_exposure"]}',
                        'details': {'industry_exposure': industry_counts}
                    }
            
            # 检查单股权重
            if 'weight' in selected_stocks[0]:
                max_weight = max(stock['weight'] for stock in selected_stocks)
                if max_weight > self.config['portfolio_constraints']['max_single_stock_weight']:
                    return {
                        'status': 'failed',
                        'message': f'单股权重过高：{max_weight:.2f} > {self.config["portfolio_constraints"]["max_single_stock_weight"]}',
                        'details': {'max_weight': max_weight}
                    }
            
            return {
                'status': 'passed',
                'message': '投资组合验证通过',
                'details': {
                    'stock_count': stock_count,
                    'industry_exposure': industry_counts if 'industry' in selected_stocks[0] else 'N/A'
                }
            }
        except Exception as e:
            self.logger.error(f"投资组合验证失败: {e}")
            return {
                'status': 'error',
                'message': f'验证过程出错: {str(e)}',
                'details': {}
            }
    
    def check_process_completeness(self, process_steps):
        """
        检查流程完整性
        
        Args:
            process_steps: 流程步骤执行记录
            
        Returns:
            dict: 流程完整性检查结果
        """
        try:
            required_steps = [
                '数据加载',
                '因子计算',
                '股票选择',
                '风险分析',
                '交易清单生成',
                '报告生成'
            ]
            
            completed_steps = [step['name'] for step in process_steps if step.get('status') == 'completed']
            missing_steps = [step for step in required_steps if step not in completed_steps]
            
            if missing_steps:
                return {
                    'status': 'failed',
                    'message': f'流程不完整，缺少步骤：{missing_steps}',
                    'details': {
                        'completed_steps': completed_steps,
                        'missing_steps': missing_steps
                    }
                }
            
            # 检查步骤执行顺序
            expected_order = ['数据加载', '因子计算', '股票选择', '风险分析', '交易清单生成', '报告生成']
            actual_order = [step['name'] for step in process_steps if step.get('status') == 'completed']
            
            # 检查关键步骤顺序是否正确
            for i, step in enumerate(expected_order):
                if step in actual_order:
                    actual_index = actual_order.index(step)
                    if i > 0 and expected_order[i-1] in actual_order:
                        expected_prev_index = actual_order.index(expected_order[i-1])
                        if actual_index < expected_prev_index:
                            return {
                                'status': 'warning',
                                'message': f'步骤顺序不正确：{step} 在 {expected_order[i-1]} 之前执行',
                                'details': {'actual_order': actual_order}
                            }
            
            return {
                'status': 'passed',
                'message': '流程完整性检查通过',
                'details': {
                    'completed_steps': completed_steps,
                    'total_steps': len(completed_steps)
                }
            }
        except Exception as e:
            self.logger.error(f"流程完整性检查失败: {e}")
            return {
                'status': 'error',
                'message': f'检查过程出错: {str(e)}',
                'details': {}
            }
    
    def run_quality_check(self, stock_data=None, factor_data=None, portfolio_data=None, process_steps=None):
        """
        运行完整的质量检查
        
        Args:
            stock_data: 股票数据
            factor_data: 因子数据
            portfolio_data: 投资组合数据
            process_steps: 流程步骤
            
        Returns:
            dict: 综合质量检查结果
        """
        results = {
            'timestamp': datetime.now().isoformat(),
            'checks': {}
        }
        
        # 数据质量检查
        if stock_data is not None:
            data_quality_result = self.check_data_quality(stock_data)
            results['checks']['data_quality'] = data_quality_result
        
        # 因子验证
        if factor_data is not None:
            factor_validation_result = self.validate_factors(factor_data)
            results['checks']['factor_validation'] = factor_validation_result
        
        # 投资组合验证
        if portfolio_data is not None:
            portfolio_validation_result = self.validate_portfolio(portfolio_data)
            results['checks']['portfolio_validation'] = portfolio_validation_result
        
        # 流程完整性检查
        if process_steps is not None:
            process_completeness_result = self.check_process_completeness(process_steps)
            results['checks']['process_completeness'] = process_completeness_result
        
        # 综合评估
        all_passed = True
        warnings = []
        errors = []
        
        for check_name, check_result in results['checks'].items():
            if check_result['status'] == 'failed':
                all_passed = False
                errors.append(f"{check_name}: {check_result['message']}")
            elif check_result['status'] == 'warning':
                warnings.append(f"{check_name}: {check_result['message']}")
            elif check_result['status'] == 'error':
                all_passed = False
                errors.append(f"{check_name}: {check_result['message']}")
        
        results['overall_status'] = 'passed' if all_passed else 'failed'
        results['warnings'] = warnings
        results['errors'] = errors
        
        # 记录质量检查结果
        self._log_quality_check(results)
        
        return results
    
    def _log_quality_check(self, results):
        """
        记录质量检查结果
        
        Args:
            results: 质量检查结果
        """
        try:
            with open(self.quality_log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(results, ensure_ascii=False) + '\n')
            
            # 记录到日志
            if results['overall_status'] == 'passed':
                self.logger.info("质量检查通过")
            else:
                self.logger.warning(f"质量检查失败: {results['errors']}")
                if results['warnings']:
                    self.logger.warning(f"质量检查警告: {results['warnings']}")
        except Exception as e:
            self.logger.error(f"记录质量检查结果失败: {e}")
    
    def get_quality_summary(self):
        """
        获取质量检查摘要
        
        Returns:
            str: 质量检查摘要
        """
        try:
            # 读取最近的质量检查结果
            if os.path.exists(self.quality_log_file):
                with open(self.quality_log_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    if lines:
                        last_result = json.loads(lines[-1])
                        
                        summary = []
                        summary.append('📊 质量控制检查结果')
                        summary.append('────────────────────')
                        summary.append(f'检查时间: {last_result["timestamp"]}')
                        summary.append(f'整体状态: {"✅ 通过" if last_result["overall_status"] == "passed" else "❌ 失败"}')
                        
                        if last_result['warnings']:
                            summary.append('')
                            summary.append('⚠️ 警告:')
                            for warning in last_result['warnings']:
                                summary.append(f'- {warning}')
                        
                        if last_result['errors']:
                            summary.append('')
                            summary.append('❌ 错误:')
                            for error in last_result['errors']:
                                summary.append(f'- {error}')
                        
                        return '\n'.join(summary)
            
            return '📊 质量控制检查结果\n────────────────────\n暂无检查记录'
        except Exception as e:
            self.logger.error(f"获取质量摘要失败: {e}")
            return '📊 质量控制检查结果\n────────────────────\n获取摘要失败'
