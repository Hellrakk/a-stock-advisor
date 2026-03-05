# A股量化系统 Skill

## 项目介绍

A股量化系统是一个完整的量化交易框架，包含α因子选股、持仓跟踪、换仓策略、风控体系、专业推送和**统一命令行入口**等功能。系统设计遵循"从寻找圣杯到管理不确定性"的核心理念，通过工程化方法管理量化投资中的各种不确定性。

## 如何使用项目

### 0. 统一主入口（推荐）

```bash
# 进入项目目录
cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor

# 运行统一主入口（推荐方式）
python3 a_stock_manager.py
```

主入口会显示交互式菜单，包含以下功能：
1. 交易日相关
2. 推送功能（含完整主控流程）
3. 数据与监控
4. 回测与模拟
5. 系统管理

### 1. 环境搭建

```bash
# 克隆项目
git clone https://gitee.com/variyaone/a-stock-advisor.git
cd a-stock-advisor

# 安装依赖
pip install -r requirements.txt

# 配置飞书webhook
echo '{"webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/your_webhook_url"}' > config/feishu_config.json
```

### 2. 可复用脚本列表

系统已经提供了丰富的脚本，AI可以直接复用，无需重新编写：

| 脚本名称 | 功能描述 | 使用场景 |
|---------|---------|--------|
| `a_stock_manager.py` | **统一主入口** - 所有功能通过一个菜单管理 ⭐ | 推荐使用，所有功能一站式访问 |
| `daily_master.py` | **核心主控脚本** - 串联所有环节的完整流程 | 盘前推送、日报推送、自动化 ⭐ |
| `data_update_v2.py` | 数据更新脚本，从多数据源获取最新市场数据 | 定期数据更新 |
| `feishu_pusher.py` | 飞书推送脚本，发送消息到飞书 | 推送通知 |
| `fix_data_quality_v2.py` | 数据质量修复脚本，检查和修复数据质量问题 | 数据质量保证 |
| `health_check.py` | 健康检查脚本，检查系统健康状态 | 系统监控 |
| `install_cron_tasks.sh` | Cron任务安装脚本，安装定时任务 | 自动化部署 |
| `install_cron_v2.sh` | Cron任务安装脚本V2，安装定时任务 | 自动化部署 |
| `is_trading_day.py` | 判断是否是交易日，考虑周末和节假日 | 推送条件判断 |
| `market_monitor.py` | 市场监控脚本，监控市场状态 | 实时市场监控 |
| `monitor_collector.py` | 监控数据收集脚本，收集系统性能指标 | 系统性能监控 |
| `morning_push_daemon.py` | 盘前推送守护进程，确保盘前推送执行 | 盘前推送保障 |
| `official_report.py` | 官方报告生成脚本，生成标准化报告 | 报告生成 |
| `paper_trading_push_v2.py` | 模拟交易推送脚本，推送模拟交易结果 | 模拟交易监控 |
| `portfolio_monitor.py` | 组合监控脚本，监控投资组合表现 | 组合管理 |
| `push_monitor.py` | 推送监控脚本，监控推送执行状态 | 推送可靠性保障 |
| `push_offline_fallback.py` | 离线推送备选脚本，处理推送失败情况 | 推送容错 |
| `run_backtest.py` | 回测脚本，执行策略回测 | 策略验证 |
| `run_simulation.py` | 模拟运行脚本，模拟策略执行 | 策略测试 |
| `unified_daily_push.py` | 统一日报推送脚本，整合推送流程 | 日报推送 |
| `enhanced_monitor.py` | 增强监控系统，实时监控系统运行状态 | 系统监控 |
| `verify_system.sh` | 系统验证脚本，检查所有组件 | 系统完整性验证 |

**使用建议**：
- 根据具体需求选择合适的脚本
- 直接调用现有脚本，不要重新编写功能相同的脚本
- 如需修改脚本，在现有脚本基础上进行修改，保持兼容性

### 3. 数据准备

```bash
# 更新数据
python3 scripts/data_update_v2.py

# 数据质量检查
python3 scripts/fix_data_quality_v2.py
```

### 4. 运行系统

**⭐推荐方式**（通过主入口）：
```bash
# 运行统一主入口
python3 a_stock_manager.py
# 选择"2. 每日主控流程 (完整流水线)"
```

**传统方式**：
```bash
# 【推荐】运行完整主控流程（工作日8:00）
python3 scripts/daily_master.py

# 运行盘前推送（工作日8:00）
python3 scripts/unified_daily_push.py --type morning

# 运行日报推送（工作日18:30）
python3 scripts/unified_daily_push.py --type evening

# 运行回测
python3 scripts/run_backtest.py

# 安装定时任务
chmod +x scripts/install_cron_tasks.sh
./scripts/install_cron_tasks.sh

# 运行系统健康检查
python3 scripts/health_check.py
```

### 5. 前端系统（已归档）

```bash
# 进入前端目录
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm run dev

# 构建生产版本
npm run build
```

### 6. 核心功能使用

#### 说明（v5.1+）
**注意**：`code/` 目录下的模块已归档到 `archive/code/`。推荐使用以下方式：
1. **统一主入口**：`python3 a_stock_manager.py`
2. **每日主控流程**：`python3 scripts/daily_master.py`

如需使用旧模块，请从 `archive/code/` 恢复。

---

#### α因子选股

```python
import sys
sys.path.append('code')
from strategy.alpha_stock_selector import AlphaStockSelector
import pandas as pd

selector = AlphaStockSelector()
# 加载数据并选股
df = pd.read_pickle('data/akshare_real_data_fixed.pkl')
selected_stocks, portfolio_config = selector.select_stocks(df, n=10)
```

#### 持仓检查

```python
import sys
sys.path.append('code')
from portfolio.portfolio_tracker import PortfolioTracker

tracker = PortfolioTracker()
summary = tracker.get_portfolio_summary()
print(summary)
```

#### 动态因子权重系统

```python
from code.strategy.multi_factor_model import DynamicFactorWeightSystem, RollingICCalculator

# 创建动态权重系统
weight_system = DynamicFactorWeightSystem(
    ic_window=20,
    ic_threshold=0.02,
    ir_threshold=0.5,
    min_weight=0.05,
    max_weight=0.40
)

# 更新权重
weights = weight_system.update_weights(factor_data, return_data, date='2026-03-03')
print(f"当前因子权重: {weights}")

# 获取权重稳定性
stability = weight_system.get_weight_stability()
print(f"权重稳定性: {stability}")

# 获取因子有效性报告
report = weight_system.get_factor_effectiveness_report()
print(report)
```

#### 自动化质量控制

```python
import sys
sys.path.append('code')
from quality_control.automated_quality_control import AutomatedQualityControl

# 初始化质量控制模块
qc = AutomatedQualityControl()

# 运行质量检查
result = qc.run_quality_check(
    stock_data=stock_data,        # 股票数据
    factor_data=factor_data,      # 因子数据
    portfolio_data=selection,     # 投资组合数据
    process_steps=process_steps   # 流程步骤
)

# 获取质量检查摘要
summary = qc.get_quality_summary()
print(summary)
```

#### 因子监控

```python
import sys
sys.path.append('code')
from quality_control.factor_monitor import FactorMonitor

# 初始化因子监控模块
fm = FactorMonitor()

# 评估因子表现
result = fm.evaluate_factors(factor_data)

# 分析因子趋势
trend = fm.analyze_factor_trends('value_factor', window=10)

# 获取因子表现摘要
summary = fm.get_factor_summary()
print(summary)

# 获取因子权重推荐
recommendation = fm.recommend_factor_weights()
```

#### 增强监控系统

```python
import sys
sys.path.append('scripts')
from enhanced_monitor import EnhancedMonitor

# 初始化增强监控系统
monitor = EnhancedMonitor()

# 开始监控
monitor.start_monitoring()

# 获取健康状态摘要
summary = monitor.get_health_summary()
print(summary)

# 检查系统是否就绪
is_ready = monitor.check_system_ready()
print(f"系统就绪状态: {is_ready}")

# 停止监控
# monitor.stop_monitoring()
```

#### 每日主控流程（推荐）

```python
# 直接运行主控脚本（推荐方式）
# 该脚本会自动执行完整流程：数据→因子→选股→验证→持仓→报告

import sys
sys.path.append('scripts')

# 主控脚本会自动执行以下所有步骤
# 直接运行即可：python3 scripts/daily_master.py
```

#### 模拟持仓跟踪系统

```python
import sys
sys.path.append('scripts')
from daily_master import PortfolioTracker

# 初始化持仓跟踪器
tracker = PortfolioTracker()

# 获取持仓摘要
summary = tracker.get_portfolio_summary()
print("=== 持仓摘要 ===")
print(f"总资金: {summary['total_assets']:.2f}")
print(f"现金: {summary['cash']:.2f}")
print(f"持仓市值: {summary['portfolio_value']:.2f}")
print(f"持仓数量: {len(summary['positions'])}")

# 查看交易历史
print("\n=== 交易历史 ===")
for trade in summary['trade_history']:
    print(f"{trade['date']}: {trade['action']} {trade['stock_name']} {trade['quantity']}股 @ {trade['price']:.2f}")

# 保存状态
tracker._save_state()
```

#### 因子动态评估系统

```python
import sys
sys.path.append('scripts')
from daily_master import EnhancedFactorEvaluator

# 初始化因子评估器
evaluator = EnhancedFactorEvaluator()

# 评估因子有效性（需要传入因子数据和收益率数据）
# evaluation = evaluator.evaluate_factors(factor_data, return_data)

# 加载历史评估
history = evaluator._load_evaluation_history()
print("=== 因子评估历史 ===")
for date, eval_result in list(history.items())[-5:]:
    print(f"{date}: {len(eval_result)}个因子评估")

# 获取动态因子权重
weights = evaluator._load_dynamic_weights()
print("\n=== 当前因子权重 ===")
for factor, weight in weights.items():
    print(f"{factor}: {weight:.2%}")
```

#### 行业/市值差异化选股

```python
import sys
sys.path.append('scripts')
from daily_master import StockClassifier, DifferentiatedFactorWeights

# 初始化股票分类器
classifier = StockClassifier()

# 分类一只股票
# stock_info = {'industry': '有色金属', 'market_cap': 1000}
# group = classifier.classify_stock(stock_info)
# print(f"股票分组: {group}")

# 初始化差异化因子权重
diff_weights = DifferentiatedFactorWeights()

# 获取某个分组的因子权重
group = '周期股'
weights = diff_weights.get_weights_for_group(group)
print(f"=== {group}因子权重 ===")
for factor_type, weight in weights.items():
    print(f"{factor_type}: {weight:.2%}")
```

#### 推荐股票回实验证

```python
import sys
sys.path.append('scripts')
from daily_master import StockBacktestValidator

# 初始化回实验证器
validator = StockBacktestValidator()

# 验证一只股票（需要传入价格数据）
# validation = validator.validate_stock(stock_code, price_data)
# print(f"=== {stock_code}回测身份证 ===")
# print(f"收益率: {validation['total_return']:.2%}")
# print(f"夏普比率: {validation['sharpe_ratio']:.2f}")
# print(f"最大回撤: {validation['max_drawdown']:.2%}")
# print(f"胜率: {validation['win_rate']:.2%}")
```

## 如何设置自动项目

### 1. 配置定时任务

系统已经提供了定时任务配置文件 `config/cron_config_v2.json`，包含以下任务：

```json
{
  "morning_push": "0 8 * * 1-5",
  "evening_push": "30 18 * * 1-5",
  "health_check": "0 3 * * *",
  "data_update": "0 4 * * *"
}
```

### 2. 安装定时任务

```bash
# 给脚本添加执行权限
chmod +x scripts/install_cron_tasks.sh

# 运行安装脚本
./scripts/install_cron_tasks.sh
```

### 3. 验证定时任务

```bash
# 查看当前定时任务
crontab -l

# 检查日志文件
ls -la logs/
tail -f logs/morning_push.log
```

### 4. 自动项目设置流程

1. **配置系统**：
   - 编辑 `config/feishu_config.json` 设置飞书webhook
   - 编辑 `config/risk_limits.json` 设置风控参数
   - 编辑 `config/cron_config_v2.json` 设置定时任务

2. **初始化数据**：
   - 运行 `python3 scripts/data_update_v2.py` 更新数据
   - 运行 `python3 scripts/fix_data_quality_v2.py` 检查数据质量

3. **安装定时任务**：
   - 运行 `./scripts/install_cron_tasks.sh` 安装定时任务

4. **验证系统**：
   - 运行 `python3 scripts/health_check.py` 检查系统健康状态
   - 运行 `python3 scripts/unified_daily_push.py --type test` 测试推送功能

5. **监控系统**：
   - 查看 `logs/` 目录下的日志文件
   - 检查飞书推送是否正常
   - 定期查看系统报告

## 系统架构

### 核心模块

1. **数据工程**：多数据源整合、数据质量框架、数据处理管道
2. **因子体系**：80+因子库、**因子动态评估（IC/IR）**、因子风险模型
3. **策略体系**：5大类策略、策略组合、**行业/市值差异化权重**
4. **回测系统**：回测引擎、成本模型、风险限制、**股票回测身份证**
5. **实盘工程**：模拟交易、券商API接入、实时风控、**模拟持仓跟踪**
6. **质量控制**：自动化质量控制、因子监控、流程验证
7. **监控层**：市场监控、持仓监控、系统监控、增强监控
8. **推送层**：盘前推送、日报推送、**每日主控流程**
9. **前端系统**：Vue 3 + Vite + Ant Design Vue

### 2026年3月全面升级亮点

✅ **闭环系统**：数据 → 因子 → 选股 → 验证 → 持仓 → 报告  
✅ **动态适应**：因子权重根据IC值自动调整  
✅ **差异化处理**：不同行业/市值使用不同因子权重  
✅ **可追溯**：每只推荐股票都有回测身份证  
✅ **风险控制**：内置止盈止损机制  
✅ **生产就绪**：完整的日志、状态管理、错误处理

### 技术栈

- **后端**：Python 3.8+、Pandas、NumPy、Scikit-learn
- **前端**：Vue 3、Vite、Ant Design Vue、ECharts
- **数据**：智兔数服、腾讯财经、新浪财经、AKShare、Baostock、Tushare
- **部署**：Cron定时任务、系统健康检查

## 常见问题与解决方案

### 1. 推送失败
- 检查网络连接
- 验证飞书webhook配置
- 查看 `logs/unified_push.log` 日志

### 2. 数据获取失败
- 检查数据源配置
- 尝试使用备用数据源
- 运行 `scripts/fix_data_quality_v2.py`

### 3. 回测结果异常
- 检查数据质量
- 验证策略参数
- 查看 `logs/backtest.log` 日志

### 4. 前端页面加载缓慢
- 检查网络连接
- 优化浏览器缓存
- 减少同时打开的页面数量

### 5. 系统性能问题
- 限制股票池大小
- 优化数据缓存机制
- 检查系统资源使用情况

## 维护与更新

### 定期维护
- **每日**：检查推送日志，确保系统正常运行
- **每周**：运行回测，评估策略表现
- **每月**：进行月度绩效分析，调整资金分配
- **季度**：更新因子库，优化策略参数

### 版本更新
- 遵循开发规范：需求分析 → 文档更新 → 代码实现 → 核查测试 → 提交部署
- 更新版本号和版本历史
- 确保向后兼容性

## 免责声明

本系统仅供研究学习，不构成投资建议。投资有风险，入市需谨慎。