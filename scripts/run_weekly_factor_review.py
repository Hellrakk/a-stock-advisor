#!/usr/bin/env python3
"""
周度因子评估脚本
作为cron任务调用code/quality_control/factor_monitor.py中的功能
"""
import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径（动态获取）
PROJECT_ROOT = str(Path(__file__).parent.parent)
sys.path.insert(0, PROJECT_ROOT)

try:
    from code.quality_control.factor_monitor import FactorMonitor

    print("📊 开始周度因子评估...")
    fm = FactorMonitor()

    # 获取因子摘要
    summary = fm.get_factor_summary()
    print(f"📈 因子摘要: {summary}")

    # 推荐因子权重
    weights = fm.recommend_factor_weights()
    print(f"⚖️ 推荐因子权重: {weights}")

    # 分析因子趋势
    print("📊 分析因子趋势...")
    # 可以添加更多逻辑来分析因子趋势

    print("✅ 周度因子评估完成")
except Exception as e:
    print(f"❌ 周度因子评估失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
