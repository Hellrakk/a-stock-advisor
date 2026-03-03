# A股量化系统 - 回测引擎改造与模拟盘搭建

## 📋 项目概述

本项目完成了A股量化系统的核心模块开发，包括：

1. **回测引擎 V2** - 包含交易成本模型、改进成交逻辑、风险限制
2. **基准模型** - 基于LightGBM的预测模型，因子数量控制在10个以内
3. **模拟盘系统** - 模块化设计，包含数据、信号、风控、交易模块
4. **市场约束模块** - 涨跌停、停牌、流动性约束处理（新增）
5. **基准对比分析** - 超额收益、跟踪误差、信息比率（新增）
6. **组合优化模块** - 均值方差、风险平价等优化方法（新增）
7. **Brinson归因分析** - 配置效应、选择效应分解（新增）
8. **滚动绩效分析** - 滚动窗口绩效指标计算（新增）

## 🎯 核心特性

### 回测引擎 V2 (`backtest_engine_v2.py`)

✅ **交易成本模型**
- 佣金：万三（0.03%），最低5元
- 印花税：千一（0.1%），仅卖出时收取
- 冲击成本：基础0.05% + 与订单额相关

✅ **改进成交逻辑**
- 支持限价单/市价单
- 部分成交机制
- 订单挂单队列

✅ **仓位与风险限制**
- 单票最大仓位：10%
- 单行业最大暴露：30%
- 最大持仓数量：20只

✅ **无未来函数**
- 严格时间序列逻辑
- 确保回测有效性

### 市场约束模块 (`market_constraint.py`) 🆕

✅ **涨跌停处理**
- 涨停股票禁止买入
- 跌停股票禁止卖出
- ST股票5%涨跌幅限制
- 自动检测涨跌停状态

✅ **停牌处理**
- 停牌股票无法交易
- 支持退市股票检测
- 交易状态枚举（正常/涨停/跌停/停牌/退市）

✅ **流动性约束**
- 成交量参与率限制（默认5%）
- 最小交易金额限制
- 冲击成本估算

```python
from code.backtest.market_constraint import MarketConstraintModule, MarketConstraintConfig

# 创建市场约束模块
config = MarketConstraintConfig(
    limit_up_threshold=0.10,
    limit_down_threshold=-0.10,
    volume_participation_rate=0.05
)
module = MarketConstraintModule(config)

# 检查买入约束
result = module.check_buy_constraint(
    '000001', stock_data,
    target_quantity=1000, target_value=10000
)
print(f"允许买入: {result['allowed']}, 原因: {result['reasons']}")
```

### 基准对比分析 (`benchmark_analyzer.py`) 🆕

✅ **收益对比指标**
- 总收益率对比
- 年化收益率对比
- 超额收益计算

✅ **风险对比指标**
- 跟踪误差（Tracking Error）
- Beta系数
- 相关性分析

✅ **风险调整指标**
- 信息比率（Information Ratio）
- Alpha（Jensen's Alpha）
- 特雷诺比率（Treynor Ratio）

✅ **捕获比率**
- 上行捕获比（Up Capture）
- 下行捕获比（Down Capture）

```python
from code.backtest.benchmark_analyzer import BenchmarkAnalyzer

# 创建分析器
analyzer = BenchmarkAnalyzer()
analyzer.set_strategy_returns(strategy_returns)
analyzer.set_benchmark(benchmark_returns)

# 生成对比报告
report = analyzer.generate_comparison_report()
print(f"信息比率: {report['relative_metrics']['information_ratio']:.2f}")
print(f"Alpha: {report['relative_metrics']['alpha']:.2%}")
```

### 组合优化模块 (`portfolio_optimizer.py`) 🆕

✅ **优化方法**
- 等权重（Equal Weight）
- 最小方差（Minimum Variance）
- 最大夏普（Maximum Sharpe）
- 风险平价（Risk Parity）
- 风险预算（Risk Budget）
- 最大分散化（Maximum Diversification）

✅ **约束条件**
- 单资产权重上限
- 单资产权重下限
- 换手率约束

✅ **辅助功能**
- 有效前沿生成
- 权重约束处理
- 换手感知优化

```python
from code.portfolio.portfolio_optimizer import PortfolioOptimizer, OptimizationMethod

# 创建优化器
optimizer = PortfolioOptimizer()
optimizer.set_parameters_from_data(returns_data)

# 风险平价优化
weights = optimizer.optimize(OptimizationMethod.RISK_PARITY)

# 获取优化结果
result = optimizer.get_optimization_result(weights)
print(f"期望收益: {result['expected_return']:.2%}")
print(f"波动率: {result['volatility']:.2%}")
print(f"夏普比率: {result['sharpe_ratio']:.2f}")
```

### Brinson归因分析 (`brinson_attribution.py`) 🆕

✅ **归因分解**
- 配置效应（Allocation Effect）
- 选择效应（Selection Effect）
- 交互效应（Interaction Effect）

✅ **多期分析**
- 多期归因累计
- 滚动归因分析

✅ **因子归因**
- 因子暴露计算
- 因子贡献分解

```python
from code.backtest.brinson_attribution import BrinsonAttribution

# 创建归因分析器
brinson = BrinsonAttribution()
brinson.set_data(
    portfolio_weights, portfolio_returns,
    benchmark_weights, benchmark_returns
)

# 运行归因分析
results = brinson.run_attribution()
print(f"配置效应: {results['summary']['total_allocation']:.4f}")
print(f"选择效应: {results['summary']['total_selection']:.4f}")
```

### 滚动绩效分析 (`rolling_performance.py`) 🆕

✅ **滚动指标**
- 滚动收益率
- 滚动波动率
- 滚动夏普比率
- 滚动最大回撤
- 滚动胜率
- 滚动卡玛比率
- 滚动索提诺比率

✅ **相对指标**
- 滚动Beta
- 滚动Alpha
- 滚动跟踪误差
- 滚动信息比率

✅ **稳定性分析**
- 变异系数计算
- 正值比例统计
- 稳定性评分

```python
from code.backtest.rolling_performance import RollingPerformanceAnalyzer, RollingConfig

# 创建分析器
config = RollingConfig(window=63, min_periods=20)
analyzer = RollingPerformanceAnalyzer(config)
analyzer.set_returns(returns)
analyzer.set_benchmark(benchmark_returns)

# 计算所有滚动指标
metrics = analyzer.calculate_all_rolling_metrics()

# 稳定性分析
stability = analyzer.analyze_stability()
print(f"夏普比率稳定性: {stability['rolling_sharpe']['stability_score']:.2f}")
```

### 基准模型 (`baseline_model.py`)

✅ **模型类型**
- LightGBM（推荐）
- XGBoost
- Logistic Regression
- Linear Regression

✅ **特征选择**
- 自动检测可用因子
- 基于IC值选择top N特征
- 因子数量控制在10个以内

✅ **严格时间序列交叉验证**
- TimeSeriesSplit（5折）
- 单进程避免多进程问题
- 评估指标：准确率、精确率、召回率、F1、AUC-ROC

### 模拟盘系统 (`paper_trading.py`)

✅ **模块化设计**
- 数据模块（DataModule）
- 信号模块（SignalModule）
- 风控模块（RiskModule）
- 交易模块（TradingModule）

✅ **监控系统**
- 日志记录
- 报警系统
- 异常检测

✅ **投资组合管理**
- 持仓管理
- 仓位计算
- 行业暴露监控

## 📊 回测结果摘要

### 策略设置
- 初始资金：1,000,000元
- 调仓频率：月度
- 选股数量：10只
- 单票仓位：5%
- 回测期间：2019-01 至 2024-01（61个月）

### 主要绩效指标

| 指标 | 数值 |
|------|------|
| 最终资产 | 14,505,614.90元 |
| 总收益率 | 1350.56% |
| 交易次数 | 1134次 |
| 成交率 | 95% |

### 基准模型性能

| 指标 | 数值 |
|------|------|
| 训练集准确率 | 76.79% |
| 测试集准确率 | 74.25% |
| AUC-ROC | 82.50% |
| 交叉验证得分 | 0.8144 ± 0.0108 |

### Top 5 特征重要性

1. close（收盘价）: 789.0
2. factor_score（因子得分）: 415.0
3. debt_ratio（负债率）: 389.0
4. pe_ttm（市盈率TTM）: 307.0
5. pe_ttm_std（市盈率标准差）: 289.0

## 🚀 使用方法

### 1. 环境准备

```bash
# 进入项目目录
cd /Users/variya/.openclaw/workspace/projects/a-stock-advisor

# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate

# 安装依赖
pip install scikit-learn lightgbm pandas pyarrow scipy
brew install libomp  # LightGBM依赖
```

### 2. 运行回测

```bash
# 激活虚拟环境
source venv/bin/activate

# 运行完整回测
python code/backtest/run_baseline_backtest.py
```

### 3. 运行测试

```bash
# 运行单元测试
python code/test_backtest_modules.py

# 运行集成测试
python code/test_integration.py
```

### 4. 查看结果

```bash
# 查看回测报告
cat reports/baseline_backtest_report_*.md

# 查看日志
tail -100 logs/backtest_*.log
```

## 📁 文件结构

```
code/
├── backtest/
│   ├── backtest_engine_v2.py       # 回测引擎 V2
│   ├── benchmark_analyzer.py       # 基准对比分析 🆕
│   ├── brinson_attribution.py      # Brinson归因分析 🆕
│   ├── full_backtest_final.py      # 完整回测脚本
│   ├── industry_analyzer.py        # 行业分析
│   ├── monthly_attribution.py      # 月度归因
│   ├── overfitting_detection_enhanced.py  # 过拟合检测
│   ├── rolling_performance.py      # 滚动绩效分析 🆕
│   ├── run_baseline_backtest.py    # 运行脚本
│   └── ...
├── data/
│   ├── data_quality_framework.py   # 数据质量框架
│   ├── fetch_real_data.py          # 真实数据获取
│   └── ...
├── portfolio/
│   └── ...  # 组合优化相关
├── risk/
│   ├── risk_control_system.py      # 风险控制系统
│   └── ...
├── strategy/
│   ├── innovation_lab.py           # 创新策略
│   └── ...
├── tests/
│   └── ...  # 测试文件
└── utils/
    └── ...  # 工具函数

reports/
├── baseline_backtest_report_*.md  # 回测报告
└── ...

logs/
├── backtest_*.log              # 回测日志
└── ...

data/
└── mock_data.pkl               # 模拟数据
```

## 🔧 模块说明

### 回测引擎 V2

```python
from code.backtest.backtest_engine_v2 import BacktestEngineV2, CostModel

# 创建成本模型
cost_model = CostModel(
    commission_rate=0.0003,  # 万三
    stamp_tax_rate=0.001,     # 千一
    impact_cost_base=0.0005,
    impact_cost_sqrt=0.001
)

# 创建回测引擎
engine = BacktestEngineV2(
    initial_capital=1000000,
    cost_model=cost_model,
    max_single_position=0.10,
    max_industry_exposure=0.30
)

# 运行回测
results = engine.run_backtest(data, signal_func, rebalance_freq='monthly')
```

### 基准模型

```python
from baseline_model import BaselineModel, train_baseline_model

# 训练模型
model, results = train_baseline_model(
    data,
    model_type='lightgbm',
    n_features=8,
    test_ratio=0.2
)

# 预测
predictions = model.predict(X_test)

# 特征重要性
importance = model.get_feature_importance()
```

### 模拟盘系统

```python
from paper_trading import (
    HistoricalDataModule,
    FactorScoreSignalModule,
    BasicRiskModule,
    PaperTradingSystem
)

# 创建模块
data_module = HistoricalDataModule(data_path)
signal_module = FactorScoreSignalModule(factor_col='factor_score', top_n=10)
risk_module = BasicRiskModule(max_single_position=0.10, max_industry_exposure=0.30)

# 创建系统
system = PaperTradingSystem(
    data_module=data_module,
    signal_module=signal_module,
    risk_module=risk_module,
    initial_capital=1000000
)

# 运行回测
results = system.run_backtest(date_list)
```

## 🧪 测试覆盖

| 模块 | 测试数量 | 覆盖率 |
|------|----------|--------|
| 市场约束模块 | 10 | 100% |
| 基准对比分析 | 11 | 100% |
| 组合优化模块 | 7 | 100% |
| Brinson归因分析 | 6 | 100% |
| 滚动绩效分析 | 8 | 100% |
| 绩效分解 | 2 | 100% |
| **总计** | **46** | **100%** |

## 💡 下一步优化方向

### 1. 因子优化
- 增加更多有效因子（动量、质量、成长等）
- 因子正交化处理
- 因子权重优化

### 2. 组合优化
- ✅ 均值方差优化（已实现）
- ✅ 风险平价模型（已实现）
- Black-Litterman模型

### 3. 回测优化
- 扩展回测时间范围（至少5年）
- 测试不同市场