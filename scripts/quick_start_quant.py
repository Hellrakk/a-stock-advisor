#!/usr/bin/env python3
"""
从0-1量化投资 - 一键启动脚本
完整工作流：数据准备 → 因子研发 → 策略开发 → 回测验证 → 风控配置 → 实盘准备
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'code'))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np

project_root = Path(__file__).parent.parent
logs_dir = project_root / 'logs'
logs_dir.mkdir(exist_ok=True)

class Color:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_header(msg):
    print(f"\n{Color.HEADER}{'='*70}{Color.ENDC}")
    print(f"{Color.HEADER}{msg.center(70)}{Color.ENDC}")
    print(f"{Color.HEADER}{'='*70}{Color.ENDC}\n")

def print_stage(stage_num, total, msg):
    print(f"\n{Color.OKCYAN}{'─'*70}{Color.ENDC}")
    print(f"{Color.OKCYAN}  阶段 {stage_num}/{total}: {msg}{Color.ENDC}")
    print(f"{Color.OKCYAN}{'─'*70}{Color.ENDC}")

def print_step(step_num, total, msg, status="running"):
    if status == "running":
        print(f"  [{step_num}/{total}] ⏳ {msg}...", end=" ", flush=True)
    elif status == "success":
        print(f"{Color.OKGREEN}✓ 完成{Color.ENDC}")
    elif status == "warning":
        print(f"{Color.WARNING}⚠ 警告{Color.ENDC}")
    elif status == "error":
        print(f"{Color.FAIL}✗ 失败{Color.ENDC}")
    elif status == "skip":
        print(f"{Color.WARNING}⊘ 跳过{Color.ENDC}")

def print_success(msg):
    print(f"{Color.OKGREEN}  ✓ {msg}{Color.ENDC}")

def print_warning(msg):
    print(f"{Color.WARNING}  ⚠ {msg}{Color.ENDC}")

def print_error(msg):
    print(f"{Color.FAIL}  ✗ {msg}{Color.ENDC}")

def print_info(msg):
    print(f"{Color.OKBLUE}  ℹ {msg}{Color.ENDC}")


class QuickStartQuant:
    """一键启动从0-1量化投资"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.data_dir = self.project_root / 'data'
        self.logs_dir = self.project_root / 'logs'
        self.results = {}
        self.start_time = time.time()
        
        self.data_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
    
    def run(self) -> Dict:
        """运行完整流程"""
        print_header("从0-1量化投资 - 一键启动")
        print_info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print_info(f"项目路径: {self.project_root}")
        
        try:
            self.stage1_data_preparation()
            self.stage2_factor_research()
            self.stage3_strategy_development()
            self.stage4_backtest_validation()
            self.stage5_risk_control()
            self.stage6_trading_preparation()
            
            self.generate_summary()
            return self.results
            
        except Exception as e:
            print_error(f"流程执行失败: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}
    
    def stage1_data_preparation(self):
        """阶段1: 数据准备"""
        print_stage(1, 6, "数据准备")
        
        print_step(1, 4, "检查数据源状态")
        try:
            from code.data.real_data_fetcher import is_real_data_available, get_data_source_status
            status = get_data_source_status()
            available_sources = [k for k, v in status.items() if v['available']]
            if available_sources:
                print_step(1, 4, f"数据源可用: {', '.join(available_sources)}", "success")
                self.results['data_sources'] = available_sources
            else:
                print_step(1, 4, "数据源检查", "warning")
                print_warning("无可用的在线数据源，将使用本地数据")
        except ImportError:
            print_step(1, 4, "数据源检查", "warning")
            print_warning("真实数据模块未安装，将使用本地数据")
        
        print_step(2, 4, "检查本地数据")
        data_file = self.data_dir / 'akshare_real_data_fixed.pkl'
        if data_file.exists():
            import pickle
            with open(data_file, 'rb') as f:
                data = pickle.load(f)
            if isinstance(data, pd.DataFrame) and len(data) > 0:
                print_step(2, 4, f"本地数据: {len(data)} 条记录", "success")
                self.results['data_rows'] = len(data)
                self.results['data_status'] = 'loaded'
            else:
                print_step(2, 4, "本地数据检查", "warning")
                print_warning("本地数据格式异常")
                self.results['data_status'] = 'invalid'
        else:
            print_step(2, 4, "本地数据检查", "warning")
            print_warning("本地数据不存在，请先运行数据更新")
            self.results['data_status'] = 'missing'
        
        print_step(3, 4, "检查股票池配置")
        try:
            from code.config import DEFAULT_STOCK_POOL
            print_step(3, 4, f"股票池: {len(DEFAULT_STOCK_POOL)} 只", "success")
            self.results['stock_pool_size'] = len(DEFAULT_STOCK_POOL)
        except ImportError:
            print_step(3, 4, "股票池配置检查", "warning")
            print_warning("使用默认股票池")
        
        print_step(4, 4, "检查技术指标")
        indicator_file = self.data_dir / 'indicator_validation_results.json'
        if indicator_file.exists():
            print_step(4, 4, "技术指标验证结果已存在", "success")
        else:
            print_step(4, 4, "技术指标验证", "skip")
            print_info("可运行「因子研发 → 技术指标验证」进行验证")
        
        print_success("阶段1完成: 数据准备")
    
    def stage2_factor_research(self):
        """阶段2: 因子研发"""
        print_stage(2, 6, "因子研发")
        
        print_step(1, 4, "检查因子池")
        factor_dir = self.data_dir / 'factor_pool'
        if factor_dir.exists():
            factor_files = list(factor_dir.glob('*.json'))
            if factor_files:
                print_step(1, 4, f"因子池: {len(factor_files)} 个文件", "success")
                self.results['factor_files'] = len(factor_files)
            else:
                print_step(1, 4, "因子池检查", "warning")
                print_warning("因子池为空")
        else:
            print_step(1, 4, "因子池检查", "warning")
            print_warning("因子池目录不存在")
        
        print_step(2, 4, "检查因子挖掘系统")
        try:
            from code.strategy.alpha_factory import AlphaFactory
            print_step(2, 4, "因子挖掘系统可用", "success")
            self.results['factor_mining'] = 'available'
        except ImportError:
            print_step(2, 4, "因子挖掘系统检查", "warning")
            print_warning("因子挖掘系统导入失败")
        
        print_step(3, 4, "检查因子验证模块")
        try:
            from code.strategy.indicator_validator import IndicatorValidator
            print_step(3, 4, "因子验证模块可用", "success")
            self.results['factor_validator'] = 'available'
        except ImportError:
            print_step(3, 4, "因子验证模块检查", "warning")
        
        print_step(4, 4, "检查动态权重系统")
        try:
            from code.strategy.multi_factor_model import DynamicFactorWeightSystem
            print_step(4, 4, "动态权重系统可用", "success")
            self.results['dynamic_weight'] = 'available'
        except ImportError:
            print_step(4, 4, "动态权重系统检查", "warning")
        
        print_success("阶段2完成: 因子研发")
    
    def stage3_strategy_development(self):
        """阶段3: 策略开发"""
        print_stage(3, 6, "策略开发")
        
        print_step(1, 4, "检查多因子模型")
        try:
            from code.strategy.multi_factor_model import MultiFactorModel
            print_step(1, 4, "多因子模型可用", "success")
            self.results['multi_factor_model'] = 'available'
        except ImportError:
            print_step(1, 4, "多因子模型检查", "warning")
        
        print_step(2, 4, "检查Alpha选股器")
        try:
            from code.strategy.alpha_stock_selector import AlphaStockSelector
            print_step(2, 4, "Alpha选股器可用", "success")
            self.results['alpha_selector'] = 'available'
        except ImportError:
            print_step(2, 4, "Alpha选股器检查", "warning")
        
        print_step(3, 4, "检查ML因子组合器")
        try:
            from code.strategy.ml_factor_combiner import MLFactorCombiner
            print_step(3, 4, "ML因子组合器可用", "success")
            self.results['ml_combiner'] = 'available'
        except ImportError:
            print_step(3, 4, "ML因子组合器检查", "warning")
        
        print_step(4, 4, "检查市场状态识别")
        try:
            from code.strategy.market_state_identifier import MarketStateIdentifier
            print_step(4, 4, "市场状态识别可用", "success")
            self.results['market_state'] = 'available'
        except ImportError:
            print_step(4, 4, "市场状态识别检查", "warning")
        
        print_success("阶段3完成: 策略开发")
    
    def stage4_backtest_validation(self):
        """阶段4: 回测验证"""
        print_stage(4, 6, "回测验证")
        
        print_step(1, 4, "检查回测引擎")
        try:
            from code.backtest.backtest_engine_v2 import BacktestEngineV2
            print_step(1, 4, "回测引擎可用", "success")
            self.results['backtest_engine'] = 'available'
        except ImportError:
            print_step(1, 4, "回测引擎检查", "warning")
        
        print_step(2, 4, "检查压力测试模块")
        try:
            from code.backtest.stress_test import StressTest
            print_step(2, 4, "压力测试模块可用", "success")
            self.results['stress_test'] = 'available'
        except ImportError:
            print_step(2, 4, "压力测试模块检查", "warning")
        
        print_step(3, 4, "检查过拟合检测")
        try:
            from code.backtest.overfitting_detection_enhanced import EnhancedOverfittingDetector
            print_step(3, 4, "过拟合检测可用", "success")
            self.results['overfitting_detector'] = 'available'
        except ImportError:
            print_step(3, 4, "过拟合检测检查", "warning")
        
        print_step(4, 4, "检查回测结果")
        backtest_dir = self.project_root / 'backtest_results'
        if backtest_dir.exists():
            result_files = list(backtest_dir.glob('*.json'))
            if result_files:
                print_step(4, 4, f"回测结果: {len(result_files)} 个文件", "success")
                self.results['backtest_results'] = len(result_files)
            else:
                print_step(4, 4, "回测结果检查", "warning")
                print_warning("暂无回测结果")
        else:
            print_step(4, 4, "回测结果检查", "warning")
        
        print_success("阶段4完成: 回测验证")
    
    def stage5_risk_control(self):
        """阶段5: 风控配置"""
        print_stage(5, 6, "风控配置")
        
        print_step(1, 4, "检查风控模块")
        try:
            from code.portfolio.paper_trading import BasicRiskModule
            risk = BasicRiskModule()
            print_step(1, 4, f"风控模块可用 (止损: {risk.stop_loss_threshold:.1%})", "success")
            self.results['risk_module'] = 'available'
            self.results['stop_loss'] = risk.stop_loss_threshold
        except ImportError:
            print_step(1, 4, "风控模块检查", "warning")
        
        print_step(2, 4, "检查风险计算器")
        try:
            from code.risk_calculator import RiskCalculator
            print_step(2, 4, "风险计算器可用", "success")
            self.results['risk_calculator'] = 'available'
        except ImportError:
            print_step(2, 4, "风险计算器检查", "warning")
        
        print_step(3, 4, "检查持仓追踪器")
        try:
            from code.portfolio.portfolio_tracker import PortfolioTracker
            print_step(3, 4, "持仓追踪器可用", "success")
            self.results['portfolio_tracker'] = 'available'
        except ImportError:
            print_step(3, 4, "持仓追踪器检查", "warning")
        
        print_step(4, 4, "检查组合状态")
        portfolio_file = self.data_dir / 'portfolio_state.json'
        if portfolio_file.exists():
            print_step(4, 4, "组合状态文件已存在", "success")
        else:
            print_step(4, 4, "组合状态检查", "warning")
            print_info("可运行「实盘工程 → 组合管理」创建组合")
        
        print_success("阶段5完成: 风控配置")
    
    def stage6_trading_preparation(self):
        """阶段6: 实盘准备"""
        print_stage(6, 6, "实盘准备")
        
        print_step(1, 4, "检查模拟交易系统")
        try:
            from code.portfolio.paper_trading import PaperTradingSystem
            print_step(1, 4, "模拟交易系统可用", "success")
            self.results['paper_trading'] = 'available'
        except ImportError:
            print_step(1, 4, "模拟交易系统检查", "warning")
        
        print_step(2, 4, "检查推送系统")
        push_script = self.project_root / 'scripts' / 'unified_daily_push.py'
        if push_script.exists():
            print_step(2, 4, "推送系统可用", "success")
            self.results['push_system'] = 'available'
        else:
            print_step(2, 4, "推送系统检查", "warning")
        
        print_step(3, 4, "检查环境变量配置")
        env_example = self.project_root / '.env.example'
        if env_example.exists():
            print_step(3, 4, "环境变量示例文件存在", "success")
            print_info("请复制为 .env 并配置实际值")
        else:
            print_step(3, 4, "环境变量配置检查", "warning")
        
        print_step(4, 4, "检查定时任务")
        cron_script = self.project_root / 'scripts' / 'morning_push_daemon.py'
        if cron_script.exists():
            print_step(4, 4, "定时任务脚本存在", "success")
        else:
            print_step(4, 4, "定时任务检查", "warning")
        
        print_success("阶段6完成: 实盘准备")
    
    def generate_summary(self):
        """生成总结报告"""
        elapsed = time.time() - self.start_time
        
        print_header("从0-1量化投资 - 完成报告")
        
        print(f"{Color.BOLD}【系统状态】{Color.ENDC}")
        
        module_keys = [
            'factor_mining', 'factor_validator', 'dynamic_weight',
            'multi_factor_model', 'alpha_selector', 'ml_combiner',
            'market_state', 'backtest_engine', 'stress_test',
            'overfitting_detector', 'risk_module', 'risk_calculator',
            'portfolio_tracker', 'paper_trading', 'push_system'
        ]
        available_count = sum(1 for k in module_keys if self.results.get(k) == 'available')
        total_modules = len(module_keys)
        print(f"  模块可用性: {available_count}/{total_modules} ({available_count/total_modules*100:.0f}%)")
        
        print(f"\n{Color.BOLD}【数据状态】{Color.ENDC}")
        data_status = self.results.get('data_status', 'unknown')
        if data_status == 'loaded':
            print_success(f"数据已加载: {self.results.get('data_rows', 0)} 条记录")
        elif data_status == 'missing':
            print_warning("数据未准备，请运行「数据工程 → 数据更新」")
        else:
            print_warning(f"数据状态: {data_status}")
        
        print(f"\n{Color.BOLD}【下一步操作建议】{Color.ENDC}")
        
        suggestions = []
        
        if self.results.get('data_status') != 'loaded':
            suggestions.append(('数据工程 → 数据更新', 'data_engineering_menu'))
        
        if not self.results.get('factor_files'):
            suggestions.append(('因子研发 → 因子挖掘系统', 'factor_research_menu'))
        
        if not self.results.get('backtest_results'):
            suggestions.append(('回测验证 → 运行回测', 'backtest_menu'))
        
        suggestions.append(('实盘工程 → 组合管理', 'live_trading_menu'))
        suggestions.append(('实盘工程 → 推送系统', 'live_trading_menu'))
        
        for i, (name, _) in enumerate(suggestions, 1):
            print(f"  {i}. 运行「{name}」")
        
        if suggestions:
            print(f"\n{Color.BOLD}【快捷跳转】{Color.ENDC}")
            print(f"  输入数字 1-{len(suggestions)} 直接跳转到对应功能")
            print(f"  输入 0 返回主菜单")
            
            try:
                jump_choice = input(f"\n{Color.OKCYAN}请选择: {Color.ENDC}").strip()
                if jump_choice.isdigit():
                    jump_idx = int(jump_choice)
                    if 1 <= jump_idx <= len(suggestions):
                        _, target_menu = suggestions[jump_idx - 1]
                        self.results['jump_to'] = target_menu
                        print_info(f"即将跳转到: {suggestions[jump_idx - 1][0]}")
            except (EOFError, KeyboardInterrupt):
                pass
        
        print(f"\n{Color.BOLD}【耗时统计】{Color.ENDC}")
        print(f"  总耗时: {elapsed:.1f} 秒")
        
        print(f"\n{Color.OKGREEN}{'='*70}{Color.ENDC}")
        print(f"{Color.OKGREEN}  从0-1量化投资流程检查完成！{Color.ENDC}")
        print(f"{Color.OKGREEN}{'='*70}{Color.ENDC}")
        
        self.results['elapsed'] = elapsed
        self.results['success'] = True
        
        result_file = Path(__file__).parent.parent / 'logs' / 'script_result.json'
        result_file.parent.mkdir(exist_ok=True)
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        
        return self.results


def main():
    """主函数"""
    starter = QuickStartQuant()
    results = starter.run()
    return 0 if results.get('success') else 1


if __name__ == '__main__':
    sys.exit(main())
