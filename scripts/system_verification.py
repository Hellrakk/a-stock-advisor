#!/usr/bin/env python3
"""
A股量化系统 - 完整系统验证
验证主入口中的所有菜单功能是否正常工作
"""

import sys
import os
import json
import subprocess
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'code'))
sys.path.insert(0, str(project_root))

class Color:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

class SystemVerifier:
    def __init__(self):
        self.results = {
            'passed': [],
            'failed': [],
            'warnings': [],
            'skipped': []
        }
        self.total_checks = 0
        self.passed_checks = 0
        
    def print_header(self, text):
        print(f"\n{Color.BOLD}{Color.HEADER}{'='*60}{Color.ENDC}")
        print(f"{Color.BOLD}{Color.HEADER}{text.center(60)}{Color.ENDC}")
        print(f"{Color.BOLD}{Color.HEADER}{'='*60}{Color.ENDC}\n")

    def print_section(self, text):
        print(f"\n{Color.BOLD}【{text}】{Color.ENDC}")
        
    def check_pass(self, name, detail=""):
        self.passed_checks += 1
        self.total_checks += 1
        self.results['passed'].append(name)
        print(f"  {Color.OKGREEN}✓ {name}{Color.ENDC}")
        if detail:
            print(f"    {Color.OKCYAN}{detail}{Color.ENDC}")
            
    def check_fail(self, name, reason=""):
        self.total_checks += 1
        self.results['failed'].append((name, reason))
        print(f"  {Color.FAIL}✗ {name}{Color.ENDC}")
        if reason:
            print(f"    {Color.FAIL}原因: {reason}{Color.ENDC}")
            
    def check_warn(self, name, detail=""):
        self.total_checks += 1
        self.results['warnings'].append((name, detail))
        print(f"  {Color.WARNING}⚠ {name}{Color.ENDC}")
        if detail:
            print(f"    {Color.WARNING}{detail}{Color.ENDC}")
            
    def check_skip(self, name, reason=""):
        self.total_checks += 1
        self.results['skipped'].append((name, reason))
        print(f"  {Color.OKCYAN}○ {name} (跳过){Color.ENDC}")
        if reason:
            print(f"    {Color.OKCYAN}原因: {reason}{Color.ENDC}")

    def check_file_exists(self, name, path, required=True):
        full_path = project_root / path
        if full_path.exists():
            self.check_pass(name, f"文件存在: {path}")
            return True
        else:
            if required:
                self.check_fail(name, f"文件不存在: {path}")
            else:
                self.check_warn(name, f"文件不存在: {path}")
            return False

    def check_module_import(self, name, module_path, class_name=None):
        try:
            parts = module_path.split('.')
            module = __import__(module_path)
            for part in parts[1:]:
                module = getattr(module, part)
            if class_name:
                cls = getattr(module, class_name)
                self.check_pass(name, f"模块和类可导入: {module_path}.{class_name}")
            else:
                self.check_pass(name, f"模块可导入: {module_path}")
            return True
        except ImportError as e:
            self.check_fail(name, f"导入失败: {module_path} - {e}")
            return False
        except AttributeError as e:
            self.check_fail(name, f"类不存在: {module_path}.{class_name} - {e}")
            return False
        except Exception as e:
            self.check_fail(name, f"导入异常: {module_path} - {e}")
            return False

    def check_data_file(self, name, path, required=True):
        full_path = project_root / path
        if full_path.exists():
            size = full_path.stat().st_size / 1024
            self.check_pass(name, f"数据文件存在: {path} ({size:.1f}KB)")
            return True
        else:
            if required:
                self.check_fail(name, f"数据文件不存在: {path}")
            else:
                self.check_warn(name, f"数据文件不存在: {path}")
            return False

    def check_config_file(self, name, path):
        full_path = project_root / path
        if full_path.exists():
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                self.check_pass(name, f"配置文件有效: {path}")
                return True
            except json.JSONDecodeError:
                self.check_fail(name, f"配置文件JSON格式错误: {path}")
                return False
        else:
            self.check_warn(name, f"配置文件不存在: {path}")
            return False

    def verify_data_engineering(self):
        self.print_section("一级菜单 - 数据工程")
        
        self.check_file_exists("数据更新脚本", "scripts/data_update_v3.py", required=False)
        self.check_file_exists("数据更新脚本v2", "scripts/data_update_v2.py", required=False)
        self.check_module_import("真实数据获取模块", "code.data.real_data_fetcher", "RealDataFetcher")
        
        self.check_module_import("数据质量框架", "code.data.data_quality_framework", "DataQualityPipeline")
        self.check_module_import("数据质量检查器", "code.data.data_quality_framework", "DataQualityChecker")
        self.check_module_import("数据清洗器", "code.data.data_quality_framework", "DataCleaner")
        
        self.check_data_file("股票数据文件", "data/akshare_real_data_fixed.pkl", required=False)
        
        self.check_module_import("多源数据获取", "code.data.multi_source_fetcher", "MultiSourceStockFetcher")
        
        self.check_module_import("另类数据框架", "code.data.alternative_data_framework", "AlternativeDataManager")

    def verify_factor_research(self):
        self.print_section("一级菜单 - 因子研发")
        
        self.check_module_import("Alpha工厂", "code.strategy.alpha_factory", "AlphaGenerator")
        self.check_module_import("因子测试器", "code.strategy.alpha_factory", "FactorTester")
        self.check_module_import("因子池", "code.strategy.alpha_factory", "FactorPool")
        
        self.check_file_exists("因子回测脚本", "scripts/run_factor_backtest_fast.py", required=False)
        
        self.check_module_import("因子监控", "code.quality_control.factor_monitor", "FactorMonitor")
        
        self.check_module_import("创新实验室", "code.strategy.innovation_lab", "InnovationLab")
        
        self.check_module_import("技术指标验证器", "code.strategy.indicator_validator", "TechnicalIndicatorValidator")
        
        self.check_module_import("RDAgent接口", "code.strategy.rdagent_interface", "RDAgentFactorInterface")

    def verify_strategy_development(self):
        self.print_section("一级菜单 - 策略开发")
        
        self.check_module_import("多因子模型", "code.strategy.multi_factor_model", "MultiFactorModel")
        self.check_module_import("动态因子权重系统", "code.strategy.multi_factor_model", "DynamicFactorWeightSystem")
        
        self.check_module_import("Alpha选股器", "code.strategy.alpha_stock_selector", "AlphaStockSelector")
        
        self.check_module_import("市场状态识别", "code.strategy.market_state_identifier", "MarketStateIdentifier")
        
        self.check_module_import("再平衡策略", "code.strategy.rebalance_strategy", "RebalanceStrategy")
        
        self.check_module_import("强化学习优化器", "code.strategy.rl_optimizer", "RLOptimizer")
        
        self.check_module_import("ML因子组合器", "code.strategy.ml_factor_combiner", "MLFactorCombiner")

    def verify_backtest(self):
        self.print_section("一级菜单 - 回测验证")
        
        self.check_file_exists("回测脚本", "scripts/run_backtest.py", required=False)
        self.check_file_exists("回测系统", "scripts/backtest_system.py", required=False)
        
        self.check_module_import("Brinson归因", "code.backtest.brinson_attribution", "BrinsonAttribution")
        
        self.check_module_import("滚动性能分析", "code.backtest.rolling_performance", "RollingPerformanceAnalyzer")
        
        self.check_module_import("压力测试", "code.backtest.stress_test", "StressTest")
        
        self.check_module_import("过拟合检测", "code.backtest.overfitting_detection_enhanced", "EnhancedOverfittingDetector")

    def verify_live_trading(self):
        self.print_section("一级菜单 - 实盘工程")
        
        self.check_file_exists("每日主控脚本", "scripts/daily_master.py", required=False)
        
        self.check_file_exists("统一推送脚本", "scripts/unified_daily_push.py", required=False)
        self.check_config_file("飞书配置", "config/feishu_config.json")
        
        self.check_module_import("持仓跟踪器", "code.portfolio.portfolio_tracker", "PortfolioTracker")
        self.check_data_file("持仓状态文件", "data/portfolio_state.json", required=False)
        
        self.check_module_import("风控系统", "code.risk.risk_control_system", "RiskControlSystem")
        
        self.check_module_import("资金管理", "code.risk.fund_management", "FundManager")
        
        self.check_module_import("风险预警", "code.risk.risk_early_warning", "RiskEarlyWarningSystem")
        
        self.check_module_import("交易员助手", "code.trader.trader_assistant", "TradingReportGenerator")
        
        self.check_module_import("模拟交易", "code.portfolio.paper_trading", "PaperTradingSystem")
        
        self.check_module_import("券商API", "code.backtest.broker_api", "BrokerAPIFactory")

    def verify_system_management(self):
        self.print_section("一级菜单 - 系统管理")
        
        self.check_file_exists("交易日检查脚本", "scripts/is_trading_day.py", required=False)
        
        self.check_file_exists("系统验证脚本", "scripts/verify_system.sh", required=False)
        self.check_file_exists("VERSION文件", "VERSION", required=False)
        self.check_file_exists("CHANGELOG文件", "CHANGELOG.md", required=False)
        
        self.check_config_file("定时任务配置", "config/cron_config_v2.json")
        self.check_file_exists("定时任务安装脚本", "scripts/install_cron_v2.sh", required=False)
        
        self.check_config_file("功能开关配置", "config/feature_flags.json")
        
        self.check_file_exists("健康检查脚本", "scripts/health_check.py", required=False)
        
        self.check_module_import("质量控制", "code.quality_control.automated_quality_control", "AutomatedQualityControl")
        
        self.check_module_import("监控仪表板", "code.utils.monitoring_dashboard", "MonitoringDashboard")

    def verify_quick_access(self):
        self.print_section("快捷入口")
        
        self.check_file_exists("一键启动脚本", "scripts/quick_start_quant.py", required=False)
        self.check_file_exists("每日主控脚本", "scripts/daily_master.py", required=False)
        self.check_file_exists("统一推送脚本", "scripts/unified_daily_push.py", required=False)

    def verify_data_integrity(self):
        self.print_section("数据完整性检查")
        
        data_dir = project_root / 'data'
        if data_dir.exists():
            self.check_pass("数据目录存在", str(data_dir))
            
            data_files = list(data_dir.glob('*.pkl')) + list(data_dir.glob('*.json'))
            self.check_pass(f"数据文件数量: {len(data_files)}个", "")
        else:
            self.check_fail("数据目录不存在", str(data_dir))
            
        backtest_dir = project_root / 'backtest_results'
        if backtest_dir.exists():
            backtest_files = list(backtest_dir.glob('*.json'))
            if backtest_files:
                self.check_pass(f"回测结果: {len(backtest_files)}个文件", "")
            else:
                self.check_warn("回测结果目录为空", "")
        else:
            self.check_warn("回测结果目录不存在", "")

    def verify_dependencies(self):
        self.print_section("依赖检查")
        
        required_packages = [
            ('pandas', '数据分析'),
            ('numpy', '数值计算'),
            ('akshare', '数据获取'),
            ('requests', 'HTTP请求'),
        ]
        
        for package, desc in required_packages:
            try:
                __import__(package)
                self.check_pass(f"{desc} ({package})", "已安装")
            except ImportError:
                self.check_fail(f"{desc} ({package})", "未安装")

    def generate_report(self):
        self.print_header("验证结果汇总")
        
        total = self.total_checks
        passed = len(self.results['passed'])
        failed = len(self.results['failed'])
        warnings = len(self.results['warnings'])
        skipped = len(self.results['skipped'])
        
        print(f"\n{Color.BOLD}【统计】{Color.ENDC}")
        print(f"  总检查项: {total}")
        print(f"  {Color.OKGREEN}通过: {passed}{Color.ENDC}")
        print(f"  {Color.FAIL}失败: {failed}{Color.ENDC}")
        print(f"  {Color.WARNING}警告: {warnings}{Color.ENDC}")
        print(f"  {Color.OKCYAN}跳过: {skipped}{Color.ENDC}")
        
        pass_rate = (passed / total * 100) if total > 0 else 0
        print(f"\n{Color.BOLD}【通过率】{Color.ENDC}")
        if pass_rate >= 80:
            print(f"  {Color.OKGREEN}{pass_rate:.1f}%{Color.ENDC}")
        elif pass_rate >= 60:
            print(f"  {Color.WARNING}{pass_rate:.1f}%{Color.ENDC}")
        else:
            print(f"  {Color.FAIL}{pass_rate:.1f}%{Color.ENDC}")
        
        if self.results['failed']:
            print(f"\n{Color.BOLD}{Color.FAIL}【失败项详情】{Color.ENDC}")
            for name, reason in self.results['failed'][:10]:
                print(f"  {Color.FAIL}✗ {name}{Color.ENDC}")
                if reason:
                    print(f"    {reason}")
            if len(self.results['failed']) > 10:
                print(f"  ... 还有 {len(self.results['failed']) - 10} 个失败项")
        
        if self.results['warnings']:
            print(f"\n{Color.BOLD}{Color.WARNING}【警告项详情】{Color.ENDC}")
            for name, detail in self.results['warnings'][:5]:
                print(f"  {Color.WARNING}⚠ {name}{Color.ENDC}")
                if detail:
                    print(f"    {detail}")
            if len(self.results['warnings']) > 5:
                print(f"  ... 还有 {len(self.results['warnings']) - 5} 个警告项")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'total': total,
            'passed': passed,
            'failed': failed,
            'warnings': warnings,
            'skipped': skipped,
            'pass_rate': pass_rate,
            'failed_items': [{'name': n, 'reason': r} for n, r in self.results['failed']],
            'warning_items': [{'name': n, 'detail': d} for n, d in self.results['warnings']]
        }
        
        report_path = project_root / 'logs' / 'system_verification_report.json'
        report_path.parent.mkdir(exist_ok=True)
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n{Color.OKCYAN}验证报告已保存: {report_path}{Color.ENDC}")
        
        return pass_rate >= 80

    def run_full_verification(self):
        self.print_header("A股量化系统 - 完整系统验证")
        print(f"{Color.OKCYAN}开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Color.ENDC}")
        
        self.verify_dependencies()
        self.verify_data_engineering()
        self.verify_factor_research()
        self.verify_strategy_development()
        self.verify_backtest()
        self.verify_live_trading()
        self.verify_system_management()
        self.verify_quick_access()
        self.verify_data_integrity()
        
        return self.generate_report()

def main():
    verifier = SystemVerifier()
    success = verifier.run_full_verification()
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())
