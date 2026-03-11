#!/bin/bash
#
# 安装Cron任务V3
# 使用 daily_master.py 作为主控脚本
#

WORK_DIR="/Users/variya/.openclaw/workspace/projects/a-stock-advisor"

echo "备份现有crontab..."
crontab -l > /tmp/crontab_backup_$(date +%Y%m%d_%H%M%S).txt 2>/dev/null || echo "无现有crontab"

echo "清理旧的A股任务..."
crontab -l 2>/dev/null | grep -v "a-stock-advisor" | crontab -

echo "安装新的Cron任务..."
(crontab -l 2>/dev/null; cat <<'EOF'
# A股推送系统 - Cron任务 V3
# 更新时间: 2026-03-10
# 使用 daily_master.py 作为主控脚本

# 7:00 - 早盘数据验证（工作日）
0 7 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/data_update_v2.py >> logs/morning_data_check.log 2>&1

# 8:00 - 盘前推送（工作日）- 使用 daily_master.py
0 8 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/daily_master.py >> logs/morning_master.log 2>&1

# 16:00 - 收盘后数据验证（工作日）
0 16 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/data_update_v2.py >> logs/evening_data_check.log 2>&1

# 18:30 - 每日选股和推送（工作日）- 使用 daily_master.py
30 18 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/daily_master.py >> logs/daily_master.log 2>&1

# 3:00 - 系统健康检查（每日）
0 3 * * * cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/health_check.py >> logs/health_check.log 2>&1

# 每小时 - 监控数据收集
0 * * * * cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/monitor_collector.py >> logs/monitor_collector.log 2>&1

# 周日 2:00 - 周度策略回测
0 2 * * 0 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/run_backtest.py >> logs/backtest.log 2>&1

# 周日 3:00 - 周度因子评估
0 3 * * 0 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 -c "from code.quality_control.factor_monitor import FactorMonitor; fm = FactorMonitor(); fm.evaluate_and_update_weights()" >> logs/factor_review.log 2>&1

# 周六 2:00 - 周度创新实验室
0 2 * * 6 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/run_innovation_lab.py >> logs/innovation_lab.log 2>&1
EOF
) | crontab -

echo "验证安装..."
crontab -l | grep "a-stock-advisor"

echo ""
echo "✅ Cron任务安装完成！"
echo ""
echo "已安装任务："
echo "  07:00 - 早盘数据验证"
echo "  08:00 - 盘前推送 (daily_master.py)"
echo "  16:00 - 收盘后数据验证"
echo "  18:30 - 每日选股和推送 (daily_master.py)"
echo "  03:00 - 系统健康检查"
echo "  每小时 - 监控数据收集"
echo "  周日 02:00 - 周度回测"
echo "  周日 03:00 - 周度因子评估"
echo "  周六 02:00 - 周度创新实验室"
echo ""
