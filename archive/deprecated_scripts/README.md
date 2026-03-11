# 已废弃脚本

本目录存放已被废弃的推送脚本，保留作为历史参考。

## 废弃文件

### morning_push_daemon.py
- **废弃时间**: 2026-03-10
- **废弃原因**: 与 cron 定时任务功能重复，且需要持续运行守护进程，资源占用较高
- **替代方案**: 使用 `daily_master.py` + cron 定时任务

### unified_daily_push.py
- **废弃时间**: 2026-03-10
- **废弃原因**: 功能已被 `daily_master.py` 完全覆盖
- **替代方案**: 使用 `daily_master.py`

## 当前推送架构

```
cron 定时任务
    │
    ├── 8:00 (工作日) ──→ daily_master.py ──→ 盘前推送
    │
    └── 18:30 (工作日) ─→ daily_master.py ──→ 日报推送
```

## daily_master.py 功能

`daily_master.py` 是完整的主控脚本，包含：
1. 数据更新
2. 因子计算与动态评估
3. 差异化选股（行业/市值）
4. ML因子组合优化
5. 回测验证
6. 持仓管理
7. 报告生成与推送

## 安装定时任务

```bash
# 运行安装脚本（需要终端权限）
./scripts/install_cron_v3.sh
```

或手动添加 crontab：
```bash
crontab -e
```

添加以下内容：
```
# 8:00 - 盘前推送（工作日）
0 8 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/daily_master.py >> logs/morning_master.log 2>&1

# 18:30 - 每日选股和推送（工作日）
30 18 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/daily_master.py >> logs/daily_master.log 2>&1
```
