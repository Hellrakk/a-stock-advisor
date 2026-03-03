# A股量化日报推送流程优化 v2.0

## 优化目标
将推送流程的主要工作交给代码和程序，agent只需收集结果、判断问题和推送给用户。

---

## 设计原则

**职责分离：**
- **代码程序**：执行数据加载、选股、风控、生成报告
- **Agent**：执行调度、异常处理、最终验证、推送

---

## 优化后的推送流程

### 自动化推送系统

**执行脚本：** `scripts/auto_push_system.py`

**功能：**
1. 自动加载最新市场数据
2. 计算α因子得分
3. 执行α因子选股
4. 检查持仓风险（止盈/止损）
5. 评估换仓决策
6. 生成完整推送内容
7. 自动发送飞书推送
8. 记录推送历史

**Agent职责：**
1. 监控推送执行状态
2. 处理推送失败异常
3. 验证推送内容质量
4. 通知用户重要事件

---

## 核心脚本设计

### auto_push_system.py

```python
#!/usr/bin/env python3
"""
自动化推送系统 - 代码承担主要工作
功能：自动执行完整推送流程
"""

class AutoPushSystem:
    def __init__(self):
        """初始化系统组件"""
        self.alpha_selector = AlphaStockSelector()
        self.portfolio_tracker = PortfolioTracker()
        self.rebalance_strategy = RebalanceStrategy()
        self.pusher = FeishuPusher()
    
    def run(self):
        """执行完整推送流程"""
        try:
            # 1. 加载数据
            data = self.load_data()
            
            # 2. 选股
            selected = self.run_selection(data)
            
            # 3. 风控检查
            risk = self.check_risk()
            
            # 4. 换仓评估
            rebalance = self.evaluate_rebalance(data, selected)
            
            # 5. 生成报告
            report = self.generate_report(data, selected, risk, rebalance)
            
            # 6. 发送推送
            self.send_push(report)
            
            # 7. 记录历史
            self.save_history(report, selected, risk, rebalance)
            
            return True, "推送成功"
        
        except Exception as e:
            return False, f"推送失败: {str(e)}"
```

### push_monitor.py（Agent调用）

```python
#!/usr/bin/env python3
"""
推送监控脚本 - Agent职责
功能：监控推送系统状态，处理异常
"""

class PushMonitor:
    def __init__(self):
        """初始化监控器"""
        self.auto_push = AutoPushSystem()
        self.alert_threshold = {
            'max_retry': 3,
            'timeout_seconds': 300
        }
    
    def check_and_push(self):
        """检查并执行推送"""
        # 1. 检查交易日
        if not is_trading_day():
            return "非交易日，跳过推送"
        
        # 2. 执行推送（代码负责）
        success, message = self.auto_push.run()
        
        # 3. Agent判断结果
        if success:
            self.log_success()
            return f"✓ {message}"
        else:
            self.handle_failure(message)
            return f"✗ {message}"
    
    def handle_failure(self, error_message):
        """处理推送失败（Agent职责）"""
        # 记录错误
        self.log_error(error_message)
        
        # 判断是否需要重试
        if self.should_retry():
            print("⚠️ 检测到可恢复错误，准备重试...")
        else:
            print("🚨 严重错误，立即告警")
            self.send_alert(error_message)
```

---

## Cron配置

```bash
# 工作日8:00 - 自动推送（代码负责，Agent监控）
0 8 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/auto_push_system.py >> logs/auto_push.log 2>&1

# 工作日8:05 - 推送监控（Agent负责）
5 8 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/push_monitor.py >> logs/push_monitor.log 2>&1

# 工作日18:30 - 日报推送
30 18 * * 1-5 cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor && /opt/homebrew/bin/python3 scripts/auto_push_system.py >> logs/auto_push.log 2>&1
```

---

## 异常处理流程

### 代码程序负责的异常：
1. 数据加载失败 → 使用备用数据源
2. 选股失败 → 使用简化逻辑
3. 推送网络错误 → 自动重试（最多3次）

### Agent负责的异常：
1. 系统崩溃 → 重启进程
2. 连续失败 → 告警用户
3. 数据严重错误 → 暂停推送，等待修复

---

## 职责总结

| 任务 | 代码程序 | Agent |
|------|---------|-------|
| 数据加载 | ✅ 负责 | ❌ |
| α因子选股 | ✅ 负责 | ❌ |
| 持仓检查 | ✅ 负责 | ❌ |
| 换仓评估 | ✅ 负责 | ❌ |
| 报告生成 | ✅ 负责 | ❌ |
| 推送发送 | ✅ 负责 | ❌ |
| 执行监控 | ❌ | ✅ |
| 异常判断 | ❌ | ✅ |
| 严重告警 | ❌ | ✅ |
| 用户通知 | ❌ | ✅ |

---

## 实施计划

1. 创建 `auto_push_system.py` - 自动推送系统
2. 创建 `push_monitor.py` - 推送监控脚本
3. 更新cron配置
4. 测试运行
5. 验证职责分离

---

**版本：** v2.0
**日期：** 2026-03-02
**状态：** 设计完成，待实施
