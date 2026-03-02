# A股量化系统 v2.0

> 智能α因子选股 + 持仓跟踪 + 换仓策略 + 风控体系

---

## 📋 系统简介

这是一个完整的A股量化交易系统，包含α因子选股、持仓跟踪、换仓策略、风控体系和自动化推送功能。

**核心特性：**
- 🎯 α因子选股（80+因子，低估+高α策略）
- 📊 持仓跟踪（实时监控、止盈止损）
- 🔄 换仓策略（止盈/止损/时间/因子触发）
- ⚠️ 风控体系（多级风险控制）
- 📈 自动化推送（盘前8:00 + 18:30）

---

## 🚀 快速开始

### 1. 日报推送

```bash
cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor
python3 scripts/a_stock_daily_report.py
```

### 2. α因子选股

```python
import sys
sys.path.append('code')
from alpha_stock_selector import AlphaStockSelector
import pandas as pd

selector = AlphaStockSelector()
# 加载数据并选股
df = pd.read_pickle('data/akshare_real_data_fixed.pkl')
selected_stocks, portfolio_config = selector.select_stocks(df, n=10)
```

### 3. 持仓检查

```python
import sys
sys.path.append('code')
from portfolio_tracker import PortfolioTracker

tracker = PortfolioTracker()
summary = tracker.get_portfolio_summary()
print(summary)
```

---

## 📁 目录结构

```
a-stock-advisor/
├── code/                      # 核心代码
│   ├── alpha_stock_selector.py    # α因子选股系统
│   ├── portfolio_tracker.py       # 持仓跟踪系统
│   ├── rebalance_strategy.py      # 换仓策略系统
│   ├── risk_controller.py         # 风控系统
│   └── backtest_engine_v2.py      # 回测引擎V2
├── scripts/                   # 执行脚本
│   ├── a_stock_daily_report.py    # 日报推送系统
│   ├── auto_push_system.py        # 自动推送系统（待实施）
│   ├── data_update.py             # 数据更新脚本
│   └── health_check.py            # 系统健康检查
├── config/                   # 配置文件
│   ├── feishu_config.json         # 飞书推送配置
│   ├── risk_limits.json           # 风控阈值配置
│   └── cron_config.json           # Cron任务配置
├── data/                     # 数据文件
│   ├── portfolio_state.json       # 持仓状态
│   ├── selection_result.json      # 选股结果
│   └── akshare_real_data_fixed.pkl # A股真实数据
├── docs/                     # 文档
│   ├── PUSH_STANDARD_FLOW.md      # 推送标准流程
│   └── PUSH_WORKFLOW_OPTIMIZATION.md # 推送流程优化
└── reports/                  # 报告输出
    └── a_stock_daily_*.md        # 日报推送
```

---

## 🎯 核心功能

### 1. α因子选股系统

**因子分类：**
- 基本面因子（32个）：PE、PB、ROE、营收增长等
- 技术面因子（30个）：动量、反转、波动率、流动性等
- 情绪因子（10个）：融资融券、北向资金等
- 另类因子（10个）：分析师、机构持仓等

**选股策略：**
- 低估值 + 高α
- 核心持仓60%（5只高α股票）
- 卫星持仓20%（2只行业轮动股票）
- 现金20%（应对风险）

### 2. 持仓跟踪系统

**功能：**
- 实时监控持仓状态
- 检查止盈止损触发
- 记录交易决策
- 计算组合收益

**输出：**
- 持仓明细（股票代码、数量、成本、盈亏）
- 组合概览（总资产、总盈亏、胜率）
- 风险指标（回撤、波动率）

### 3. 换仓策略系统

**触发条件：**
- **止盈**：收益>20% → 分3批止盈
- **止损**：亏损<-10% → 立即清仓
- **时间**：持仓>60天 → 评估换仓
- **因子**：α下降>20% → 建议换仓

### 4. 风控体系

**风控级别：**
- 个股止损：-10%
- 个股止盈：+20%
- 组合最大回撤：-15%
- 单股最大仓位：12%

---

## 📊 数据源

**主要数据源（按优先级）：**
1. **AKShare** ⭐⭐⭐⭐⭐ - 完全免费，无限制
2. **Baostock** ⭐⭐⭐ - 开源免费，数据质量高
3. **Tushare** ⭐⭐⭐⭐ - 积分制，数据最完整

**数据质量：**
- 单日涨跌幅：≤ ±10%（A股限制）
- 价格：> 0（无负值）
- 波动率/均价：≤ 1.0（防止极端数据）
- 缺失值比例：< 1%（高完整性）

---

## 🕐 定时任务

### Cron配置

```bash
# 工作日8:00 - 盘前推送
0 8 * * 1-5 cd /path/to/a-stock-advisor && python3 scripts/a_stock_daily_report.py >> logs/morning_push.log 2>&1

# 工作日18:30 - 日报推送
30 18 * * 1-5 cd /path/to/a-stock-advisor && python3 scripts/a_stock_daily_report.py >> logs/daily_push.log 2>&1

# 每日3:00 - 系统健康检查
0 3 * * * cd /path/to/a-stock-advisor && python3 scripts/health_check.py >> logs/health_check.log 2>&1
```

---

## 📖 开发规范（强制执行）

**任何更新必须遵循以下步骤：**

```
1️⃣ 需求分析 → 2️⃣ 文档更新 → 3️⃣ 代码实现 → 4️⃣ 核查测试 → 5️⃣ 提交部署
```

**详细步骤：**

1. **需求分析** - 理解指令，定义目标，拆解流程
2. **文档更新** - 更新README、MANUAL.md、接口定义
3. **代码实现** - 调用架构师/创作者实现
4. **核查测试** - 功能测试、边界测试、数据验证
5. **提交部署** - Git commit、推送到Gitee、更新版本号

**检查清单：**
- [ ] 文档已更新
- [ ] 代码已测试
- [ ] 数据格式正确
- [ ] 异常已处理
- [ ] Git已提交

---

## 🔧 配置文件

### 飞书推送配置

`config/feishu_config.json`:
```json
{
  "webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
}
```

### 风控配置

`config/risk_limits.json`:
```json
{
  "stop_loss_threshold": -0.10,
  "take_profit_threshold": 0.20,
  "max_portfolio_drawdown": -0.15,
  "max_single_stock_weight": 0.12
}
```

---

## 📚 文档

- **MANUAL.md** - 系统完整手册（3800+行）
- **docs/PUSH_STANDARD_FLOW.md** - 推送标准流程
- **docs/PUSH_WORKFLOW_OPTIMIZATION.md** - 推送流程优化

---

## 🌟 版本历史

### v2.0 (2026-03-02)
- ✅ α因子选股系统（80+因子）
- ✅ 持仓跟踪系统
- ✅ 换仓策略系统
- ✅ 风控体系
- ✅ 自动化推送系统
- ✅ 推送标准流程固化
- ✅ 开发流程规范化

### v1.0 (2026-02-28)
- ✅ 基础数据获取
- ✅ 简单推送系统

---

## 📦 依赖

```
pandas >= 1.5.0
numpy >= 1.23.0
akshare >= 1.10.0
requests >= 2.28.0
```

---

## 📞 联系方式

- **项目地址**: https://gitee.com/variyaone/a-stock-advisor
- **Skill**: a-stock-advisor
- **维护者**: 小龙虾🦞（main）

---

**免责声明：本系统仅供研究学习，不构成投资建议。投资有风险，入市需谨慎。**
