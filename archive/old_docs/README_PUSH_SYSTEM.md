# 推送流程优化系统

## 快速开始

### 1. 直接运行脚本

```bash
# 判断今天是否是交易日
python3 scripts/is_trading_day.py

# 执行完整推送流程（代码层负责）
python3 scripts/auto_push_system.py

# 执行推送监控（Agent层负责）
python3 scripts/push_monitor.py
```

### 2. 配置Cron定时任务

编辑crontab：
```bash
crontab -e
```

添加以下任务：
```bash
# 工作日8:00 - 盘前推送（代码执行）
0 8 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/auto_push_system.py >> logs/auto_push.log 2>&1

# 工作日8:05 - 推送监控验证（Agent监督）
5 8 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/push_monitor.py >> logs/push_monitor.log 2>&1

# 工作日18:30 - 日报推送
30 18 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/auto_push_system.py >> logs/auto_push.log 2>&1
```

## 脚本说明

### is_trading_day.py
判断今天是否是交易日，考虑周末和节假日。

- **返回值**：True（是交易日）或 False（非交易日）
- **节假日配置**：维护 `data/holidays.json` 文件

### auto_push_system.py
自动化推送系统，完成整个推送流程。

**功能：**
1. 加载最新市场数据
2. 计算α因子得分
3. 执行α因子选股
4. 检查持仓风险（止盈/止损）
5. 评估换仓决策
6. 生成推送内容
7. 发送飞书推送
8. 记录推送历史

**返回值：**
```python
(success: bool, message: str)
```

### push_monitor.py
推送监控脚本，Agent职责。

**功能：**
1. 检查是否是交易日
2. 检查系统健康状态
3. 调用auto_push_system.py
4. 监控执行结果
5. 记录失败次数
6. 发送告警（如果连续失败）

**返回值：**
```python
result: str  # 结果描述
```

## 系统架构

```
┌─────────────────────────────────────────┐
│         Cron Schedule (定时任务)         │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│      push_monitor.py (Agent层)          │
│  - 监控执行状态                          │
│  - 处理异常情况                          │
│  - 发送严重告警                          │
└─────────┬───────────────────────────────┘
          │
          ├────────────────────┐
          │                    │
          ▼                    ▼
┌───────────────┐      ┌────────────────┐
│is_trading_day │      │  Health Check  │
│   .py         │      │   (健康检查)    │
└───────────────┘      └────────────────┘
                               │
                               ▼
          ┌────────────────────────────────┐
          │   auto_push_system.py (代码层)  │
          │  - 数据加载                     │
          │  - α因子选股                    │
          │  - 风控检查                     │
          │  - 换仓评估                     │
          │  - 报告生成                     │
          │  - 飞书推送                     │
          └────────────────────────────────┘
                      │
                      ├─────────► AlphaStockSelector
                      ├─────────► PortfolioTracker
                      ├─────────► RebalanceStrategy
                      ├─────────► RiskControlSystem
                      └─────────► FeishuPusher
```

## 日志文件

- `logs/auto_push.log` - 自动推送系统日志
- `logs/push_monitor.log` - 推送监控日志
- `logs/unified_push.log` - 统一推送日志（如果使用）

## 数据文件

- `data/akshare_real_data_fixed.pkl` - 市场数据
- `data/push_history.json` - 推送历史记录
- `data/holidays.json` - 节假日列表（可选）
- `data/push_failure_count.json` - 失败计数

## 配置文件

- `config/feishu_config.json` - 飞书推送配置

```json
{
  "webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/xxx",
  "webhook_secret": "xxx"
}
```

## 故障排查

### 推送失败
1. 检查飞书配置是否正确
2. 查看日志文件获取详细错误信息
3. 验证数据文件是否存在

### 模块导入错误
```bash
# 重新验证系统
bash scripts/verify_system.sh
```

### 节假日判断错误
```bash
# 检查节假日配置
cat data/holidays.json

# 更新节假日列表
# 手动编辑 data/holidays.json
```

## 验证系统

运行验证脚本检查所有组件：
```bash
bash scripts/verify_system.sh
```

## 版本信息

- **版本**：v2.0
- **创建日期**：2026-03-02
- **状态**：✓ 所有功能已实现并验证通过

