# A股量化系统 v5.0

> 智能α因子选股 + 持仓跟踪 + 换仓策略 + 风控体系 + 专业推送 + 因子风险模型

---

## 📋 系统简介

这是一个完整的A股量化交易系统，包含α因子选股、持仓跟踪、换仓策略、风控体系和专业自动化推送功能。

**核心特性：**
- 🎯 α因子选股（100+因子，低估+高α策略）
- 📊 持仓跟踪（实时监控、止盈止损）
- 🔄 换仓策略（止盈/止损/时间/因子触发）
- ⚠️ 风控体系（多级风险控制 + Beta监控）
- 📈 专业推送（完整交易指令 + 因子信息 + 行业标准）
- 🌐 市场状态监控（千股跌停、流动性、恐慌指数）
- 🔬 因子风险模型（因子暴露监控、风险归因）

**最新更新（v5.0）：**
- ✅ 因子公式修复（毛利率等公式错误修正）
- ✅ IC计算优化（最小样本量100只，显著性检验）
- ✅ 动态因子权重系统（滚动IC、权重稳定性监控）
- ✅ 技术面因子扩展（动量、反转、波动率、量价因子）
- ✅ 因子中性化（行业/市值/双重中性化）
- ✅ 因子风险模型（FactorRiskModel、FactorExposureMonitor）
- ✅ 开源项目融合方案（Qlib、QUANTAXIS、VNPy等）

---

## 📖 推送内容详解（v4.0）

### 完整推送包含13部分：

1. **市场概览** - 三大指数、市场情绪、板块表现
2. **组合风险监控** - 波动率、VaR、最大回撤、Beta风险预警
3. **持仓详情** - 每只股票完整信息（代码、名称、α、PE、PB、ROE、行业平均）
4. **新选股详情** - 每只股票完整信息
5. **可执行交易清单** - 买卖方向、价格区间、计划金额、数量、执行时间、参考价
6. **止盈止损监控** - 收益、距止盈/止损、状态
7. **换仓建议** - 触发类型、原因、操作、替代股票
8. **因子表现监控** - IC、RankIC、状态
9. **行业配置建议** - 当前权重、基准权重、偏离度、建议
10. **市场极端情况监控** - 跌停股票、流动性、恐慌指数
11. **长期持仓推荐** - 高股息+低Beta，稳健型配置
12. **历史决策跟踪** - 决策内容、执行情况、结果
13. **明日计划** - 具体行动计划

---

## 🚀 快速开始

### 1. 专业日报推送（v4.0）

```bash
cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor
python3 scripts/a_stock_push_v4.py
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

### 4. 动态因子权重系统（v5.0新增）

```python
from multi_factor_model import DynamicFactorWeightSystem, RollingICCalculator

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

### 5. 滚动IC计算

```python
from multi_factor_model import RollingICCalculator

calculator = RollingICCalculator(window=20, min_periods=10)
ic_df = calculator.calculate_rolling_ic(factor_series, return_series)

# 获取IC衰减信息
decay_info = calculator.get_ic_decay(ic_df['ic'])
print(f"IC衰减率: {decay_info['decay_rate']}")
print(f"半衰期: {decay_info['half_life']}")
```

---

## 📁 目录结构

```
a-stock-advisor/
├── code/                      # 核心代码
│   ├── alpha_stock_selector.py    # α因子选股系统（含中性化）
│   ├── multi_factor_model.py      # 多因子模型（含动态权重系统）
│   ├── market_wide_selector.py    # 全市场选股（含IC计算优化）
│   ├── risk_calculator.py         # 风险计算（含因子风险模型）
│   ├── ml_factor_combiner.py      # ML因子组合（借鉴Qlib/Qbot）
│   ├── event_engine.py            # 事件驱动引擎（借鉴VNPy）
│   ├── portfolio_tracker.py       # 持仓跟踪系统
│   ├── rebalance_strategy.py      # 换仓策略系统
│   ├── risk_controller.py         # 风控系统
│   ├── innovation_lab.py          # 因子创新实验室
│   ├── overfitting_detection.py   # 过拟合检测
│   └── backtest_engine_v2.py      # 回测引擎V2
├── scripts/                   # 执行脚本
│   ├── a_stock_push_v5.py         # 日报推送系统v5
│   ├── a_stock_daily_report.py    # 日报推送系统
│   ├── auto_push_system.py        # 自动推送系统
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
│   ├── integration_plan.md        # 开源项目融合方案
│   ├── PUSH_STANDARD_FLOW.md      # 推送标准流程
│   └── PUSH_WORKFLOW_OPTIMIZATION.md # 推送流程优化
└── reports/                  # 报告输出
    └── a_stock_daily_*.md        # 日报推送
```

---

## 🎯 核心功能

### 1. α因子选股系统

**因子分类：**
- 基本面因子（32个）：PE、PB、ROE、营收增长、毛利率等
- 技术面因子（42个）：动量、反转、波动率、流动性、RSI、MACD等
- 情绪因子（10个）：融资融券、北向资金等
- 另类因子（10个）：分析师、机构持仓等

**选股策略：**
- 低估值 + 高α
- 核心持仓60%（5只高α股票）
- 卫星持仓20%（2只行业轮动股票）
- 现金20%（应对风险）

**因子有效性标准：**
- IC绝对值 ≥ 0.02（弱有效）
- IC绝对值 ≥ 0.05（有效）
- IR ≥ 0.5（可接受）
- IR ≥ 1.0（良好）
- 样本量 ≥ 100只股票

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

### 5. 因子风险模型（v5.0新增）

**核心功能：**
- 因子收益率估计（截面回归）
- 因子协方差矩阵估计（带收缩估计）
- 组合因子暴露计算
- 因子风险归因分析
- 暴露阈值监控
- 暴露趋势追踪

**使用示例：**
```python
from risk_calculator import FactorRiskModel, FactorExposureMonitor

# 因子风险模型
risk_model = FactorRiskModel(lookback_period=252)
factor_returns = risk_model.estimate_factor_returns(stock_returns, factor_exposures)
factor_cov = risk_model.estimate_factor_covariance(factor_returns_history)

# 因子暴露监控
monitor = FactorExposureMonitor()
alerts = monitor.check_exposure(portfolio_exposure)
monitor.track_exposure(date, portfolio_exposure)
report = monitor.generate_exposure_report()
```

### 6. 因子中性化（v5.0新增）

**中性化方法：**
- 行业中性化：`industry_neutralize()`
- 市值中性化：`market_cap_neutralize()` (回归法/分位数法)
- 双重中性化：`double_neutralize()` (顺序法/正交法)
- 批量中性化：`neutralize_all_factors()`

**使用示例：**
```python
from alpha_stock_selector import AlphaStockSelector

selector = AlphaStockSelector()

# 行业中性化
neutral_factor = selector.industry_neutralize(data, 'PE_TTM', 'industry')

# 市值中性化
neutral_factor = selector.market_cap_neutralize(data, 'PE_TTM', 'market_cap', method='regression')

# 双重中性化
neutral_factor = selector.double_neutralize(data, 'PE_TTM', 'industry', 'market_cap')

# 批量中性化
neutralized_data = selector.neutralize_all_factors(data, neutralize_type='double')
```

### 7. ML因子组合（v5.0新增，借鉴Qlib/Qbot）

**核心功能：**
- 支持多种模型：GBDT、随机森林、Ridge、Lasso
- 集成多模型预测
- 自动特征重要性计算
- 交叉验证性能评估

**使用示例：**
```python
from ml_factor_combiner import MLFactorCombiner, EnsembleFactorCombiner

# 单模型因子组合
combiner = MLFactorCombiner(model_type='gbdt')
result = combiner.fit(factor_exposures, future_returns)
ml_weights = combiner.get_factor_weights()
predictions = combiner.predict(current_factor_exposures)

# 集成多模型
ensemble = EnsembleFactorCombiner(models=['gbdt', 'rf', 'ridge'])
ensemble_result = ensemble.fit(factor_exposures, future_returns)
ensemble_predictions = ensemble.predict(current_factor_exposures)
```

### 8. 事件驱动引擎（v5.0新增，借鉴VNPy）

**核心功能：**
- 事件驱动架构
- 解耦数据层、策略层、执行层
- 支持异步事件处理
- 可扩展的处理器注册机制

**使用示例：**
```python
from event_engine import EventEngine, EventType, Event, QuantTradingSystem

# 创建事件引擎
engine = EventEngine()

# 注册事件处理器
engine.register(EventType.MARKET_DATA, on_market_data)
engine.register(EventType.TRADE_SIGNAL, on_signal)

# 启动引擎
engine.start()

# 发送事件
engine.put(Event(EventType.MARKET_DATA, market_data))
```

---

## 📊 数据源

**主要数据源（按优先级）：**
1. **智兔数服** ⭐⭐⭐⭐⭐ - 免费，实时数据，包含财务指标
2. **腾讯财经** ⭐⭐⭐⭐ - 免费，数据格式规整，更新频率约3秒
3. **新浪财经** ⭐⭐⭐⭐ - 免费，老牌数据源，支持买卖五档盘口数据
4. **AKShare** ⭐⭐⭐⭐ - 完全免费，无限制，数据全面
5. **Baostock** ⭐⭐⭐ - 开源免费，数据质量高
6. **Tushare** ⭐⭐⭐⭐ - 积分制，数据最完整

**数据质量：**
- 单日涨跌幅：≤ ±10%（A股限制）
- 价格：> 0（无负值）
- 波动率/均价：≤ 1.0（防止极端数据）
- 缺失值比例：< 1%（高完整性）

**数据缓存：**
- 缓存过期时间：60秒
- 缓存机制：减少API调用，节约token使用
- 自动切换：当首选数据源失败时，自动尝试备选数据源

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

- **docs/integration_plan.md** - 开源项目融合方案
- **docs/PUSH_STANDARD_FLOW.md** - 推送标准流程
- **docs/PUSH_WORKFLOW_OPTIMIZATION.md** - 推送流程优化

---

## 🔗 开源项目借鉴

本项目借鉴了以下优秀开源项目的核心功能：

| 项目 | 借鉴功能 | 实现文件 |
|------|----------|----------|
| [Qbot](https://github.com/UFund-Me/Qbot) | ML因子组合、飞书推送 | `code/ml_factor_combiner.py` |
| [Abu](https://github.com/bbfamily/abu) | 参数优化、回测框架、风险评估 | `code/overfitting_detection_enhanced.py` |
| [Qlib](https://github.com/microsoft/qlib) | ML因子组合、统一数据接口 | `code/ml_factor_combiner.py` |
| [yfinance](https://github.com/ranaroussi/yfinance) | 多数据源获取 | `code/multi_source_fetcher.py` |

详细借鉴状态请查看 [docs/integration_plan.md](docs/integration_plan.md)

---

## 🌟 版本历史

### v5.0 (2026-03-03)
- ✅ 因子公式修复（毛利率公式错误修正）
- ✅ IC计算优化（最小样本量100只，显著性检验，p-value）
- ✅ 动态因子权重系统（RollingICCalculator、DynamicFactorWeightSystem）
- ✅ 技术面因子扩展（12个新因子：动量、反转、波动率、RSI、MACD等）
- ✅ 因子中性化实现（行业/市值/双重中性化）
- ✅ 因子风险模型构建（FactorRiskModel、FactorExposureMonitor）
- ✅ 开源项目融合方案（Qlib、QUANTAXIS、VNPy、Abu等）
- ✅ ML因子组合模块（ml_factor_combiner.py）
- ✅ 事件驱动引擎（event_engine.py）
- ✅ 股票代码格式修复（修正为正确格式如sh600064）
- ✅ 价格信息显示修复（显示真实价格区间）
- ✅ 系统性能优化（限制股票池大小，提高响应速度）
- ✅ 数据获取稳定性增强（多数据源自动切换机制）
- ✅ 多数据源整合（智兔数服、腾讯财经、新浪财经）
- ✅ 数据缓存机制（60秒缓存，节约token使用）
- ✅ 新浪财经API支持（添加Referer请求头）

### v4.0 (2026-03-02)
- ✅ 13部分完整推送内容
- ✅ 每只股票详细因子信息
- ✅ Beta风险监控和预警
- ✅ 长期持仓推荐
- ✅ 可执行交易清单

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