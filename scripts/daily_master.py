#!/usr/bin/env python3
"""
A股量化系统 - 主控脚本
功能：完整的自动化流水线
1. 数据更新
2. 因子计算与动态评估
3. 差异化选股（行业/市值）
4. ML因子组合优化
5. 回测验证
6. 持仓管理
7. 报告生成与推送
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'code'))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timedelta
import logging
import json
import pickle
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional

try:
    from code.strategy.ml_factor_combiner import MLFactorCombiner
    ML_COMBINER_AVAILABLE = True
except ImportError:
    ML_COMBINER_AVAILABLE = False

try:
    from code.strategy.multi_factor_model import DynamicFactorWeightSystem
    DYNAMIC_WEIGHT_AVAILABLE = True
except ImportError:
    DYNAMIC_WEIGHT_AVAILABLE = False

try:
    from code.risk.risk_calculator import FactorRiskModel, FactorExposureMonitor
    FACTOR_RISK_AVAILABLE = True
except ImportError:
    FACTOR_RISK_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/daily_master.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class StockClassifier:
    """股票分类器 - 按行业、市值分组"""
    
    def __init__(self):
        self.industry_groups = {
            '周期股': ['有色金属', '煤炭', '钢铁', '化工', '房地产', '建筑材料'],
            '成长股': ['电子', '计算机', '传媒', '通信', '医药生物', '国防军工'],
            '消费股': ['食品饮料', '家用电器', '纺织服装', '休闲服务', '商业贸易'],
            '金融股': ['银行', '非银金融'],
            '公用事业': ['公用事业', '交通运输', '农林牧渔']
        }
        
        self.cap_groups = {
            '大盘股': 1000,
            '中盘股': 200,
            '小盘股': 0
        }
    
    def classify_by_industry(self, industry_name: str) -> str:
        """按行业分类"""
        for group, industries in self.industry_groups.items():
            if any(ind in industry_name for ind in industries):
                return group
        return '其他'
    
    def classify_by_cap(self, market_cap: float) -> str:
        """按市值分类（亿元）"""
        if market_cap >= self.cap_groups['大盘股']:
            return '大盘股'
        elif market_cap >= self.cap_groups['中盘股']:
            return '中盘股'
        else:
            return '小盘股'


class DifferentiatedFactorWeights:
    """差异化因子权重系统"""
    
    def __init__(self):
        self.classifier = StockClassifier()
        
        self.weights_config = {
            '周期股': {
                'momentum': 0.35,
                'valuation': 0.30,
                'quality': 0.15,
                'liquidity': 0.20
            },
            '成长股': {
                'momentum': 0.20,
                'valuation': 0.15,
                'quality': 0.40,
                'liquidity': 0.25
            },
            '消费股': {
                'momentum': 0.25,
                'valuation': 0.25,
                'quality': 0.30,
                'liquidity': 0.20
            },
            '金融股': {
                'momentum': 0.20,
                'valuation': 0.40,
                'quality': 0.25,
                'liquidity': 0.15
            },
            '公用事业': {
                'momentum': 0.20,
                'valuation': 0.35,
                'quality': 0.25,
                'liquidity': 0.20
            },
            '其他': {
                'momentum': 0.25,
                'valuation': 0.25,
                'quality': 0.25,
                'liquidity': 0.25
            }
        }
    
    def get_weights(self, industry_group: str) -> Dict[str, float]:
        """获取特定行业组的因子权重"""
        return self.weights_config.get(industry_group, self.weights_config['其他'])


class EnhancedFactorEvaluator:
    """增强型因子评估器"""
    
    def __init__(self, data_file: str = 'data/akshare_real_data_fixed.pkl'):
        self.data_file = data_file
        self.evaluation_history_file = 'data/factor_evaluation_history.json'
        self.factor_weights_file = 'data/factor_dynamic_weights.json'
        
    def load_data(self) -> pd.DataFrame:
        """加载数据"""
        if os.path.exists(self.data_file):
            with open(self.data_file, 'rb') as f:
                return pickle.load(f)
        return pd.DataFrame()
    
    def calculate_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算收益率"""
        df = df.copy()
        # 使用实际数据中的列名
        if 'date' in df.columns:
            df = df.sort_values(['stock_code', 'date'])
            df['return_1d'] = df.groupby('stock_code')['close'].pct_change().shift(-1)
        return df
    
    def calculate_ic(self, factor_series: pd.Series, return_series: pd.Series) -> float:
        """计算IC值（Spearman相关系数）"""
        common_idx = factor_series.index.intersection(return_series.index)
        if len(common_idx) < 20:
            return 0.0
        
        factor_clean = factor_series.loc[common_idx].dropna()
        return_clean = return_series.loc[common_idx].dropna()
        
        common_clean = factor_clean.index.intersection(return_clean.index)
        if len(common_clean) < 20:
            return 0.0
        
        return factor_clean.loc[common_clean].corr(return_clean.loc[common_clean], method='spearman')
    
    def evaluate_factors(self, df: pd.DataFrame) -> Dict:
        """评估所有因子"""
        df = self.calculate_returns(df)
        
        # 使用实际数据中存在的因子列
        factor_list = [
            'momentum_5', 'momentum_10', 'momentum_20', 'momentum_60',
            'volatility_5', 'volatility_10', 'volatility_20',
            'turnover', 'amount', 'change_pct',
            'price_to_ma20', 'price_to_ma60'
        ]
        available_factors = [f for f in factor_list if f in df.columns]
        
        results = {}
        for factor in available_factors:
            if 'date' in df.columns:
                ic_values = []
                for date, group in df.groupby('date'):
                    if factor in group.columns and 'return_1d' in group.columns:
                        ic = self.calculate_ic(group[factor], group['return_1d'])
                        if not pd.isna(ic):
                            ic_values.append(ic)
                
                if len(ic_values) >= 5:
                    ic_mean = np.mean(ic_values)
                    ic_std = np.std(ic_values) if len(ic_values) > 1 else 1
                    ir = ic_mean / ic_std if ic_std != 0 else 0
                    
                    results[factor] = {
                        'ic_mean': round(float(ic_mean), 4),
                        'ic_std': round(float(ic_std), 4),
                        'ir': round(float(ir), 4),
                        'ic_values': [round(float(x), 4) for x in ic_values[-20:]],
                        'effective': bool(abs(ic_mean) >= 0.02 and abs(ir) >= 0.3)
                    }
        
        return results
    
    def save_evaluation(self, evaluation: Dict):
        """保存评估结果"""
        history = []
        if os.path.exists(self.evaluation_history_file):
            try:
                with open(self.evaluation_history_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            except:
                pass
        
        record = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'evaluation': evaluation
        }
        history.append(record)
        
        if len(history) > 52:
            history = history[-52:]
        
        with open(self.evaluation_history_file, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    
    def update_dynamic_weights(self, evaluation: Dict):
        """基于评估结果更新动态权重"""
        total_ic = sum(abs(f['ic_mean']) for f in evaluation.values() if f['effective'])
        
        weights = {}
        if total_ic > 0:
            for factor, result in evaluation.items():
                if result['effective']:
                    weights[factor] = abs(result['ic_mean']) / total_ic
                else:
                    weights[factor] = 0.01
        else:
            n = len(evaluation)
            if n > 0:
                equal_weight = 1.0 / n
                weights = {f: equal_weight for f in evaluation.keys()}
        
        with open(self.factor_weights_file, 'w', encoding='utf-8') as f:
            json.dump({
                'date': datetime.now().strftime('%Y-%m-%d'),
                'weights': weights
            }, f, indent=2, ensure_ascii=False)
        
        return weights


class StockBacktestValidator:
    """股票回实验证器 - 为每只推荐股票生成回测身份证"""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def quick_backtest(self, stock_code: str, lookback_days: int = 60) -> Dict:
        """快速回测单只股票"""
        if 'date' not in self.df.columns:
            return self._generate_mock_backtest(stock_code)
        
        stock_data = self.df[self.df['stock_code'] == stock_code].copy()
        if len(stock_data) < 20:
            return self._generate_mock_backtest(stock_code)
        
        stock_data = stock_data.sort_values('date').tail(lookback_days)
        stock_data['return'] = stock_data['close'].pct_change()
        
        total_return = (stock_data['close'].iloc[-1] / stock_data['close'].iloc[0] - 1) * 100
        daily_returns = stock_data['return'].dropna()
        
        if len(daily_returns) > 0:
            volatility = daily_returns.std() * np.sqrt(252) * 100
            win_rate = (daily_returns > 0).mean() * 100
            
            positive = daily_returns[daily_returns > 0]
            negative = daily_returns[daily_returns < 0]
            profit_loss_ratio = (abs(positive.mean()) / abs(negative.mean())) if len(negative) > 0 else 1.0
            
            max_drawdown = self._calculate_max_drawdown(stock_data['close'])
        else:
            volatility = 20
            win_rate = 50
            profit_loss_ratio = 1.0
            max_drawdown = -10
        
        return {
            'stock_code': stock_code,
            'lookback_days': len(stock_data),
            'total_return_pct': round(total_return, 2),
            'volatility_pct': round(volatility, 2),
            'win_rate_pct': round(win_rate, 2),
            'profit_loss_ratio': round(profit_loss_ratio, 2),
            'max_drawdown_pct': round(max_drawdown, 2),
            'sharpe_ratio': round(total_return / volatility if volatility > 0 else 0, 2),
            'is_reliable': len(daily_returns) >= 20,
            'data_quality': 'sufficient' if len(daily_returns) >= 20 else 'limited'
        }
    
    def _generate_mock_backtest(self, stock_code: str) -> Dict:
        """生成回测结果（当数据不足时）- 明确标记为不可靠"""
        return {
            'stock_code': stock_code,
            'lookback_days': 0,
            'total_return_pct': 0.0,
            'volatility_pct': 0.0,
            'win_rate_pct': 0.0,
            'profit_loss_ratio': 0.0,
            'max_drawdown_pct': 0.0,
            'sharpe_ratio': 0.0,
            'is_reliable': False,
            'warning': '⚠️ 数据不足，无法进行可靠回测，请先更新数据',
            'data_quality': 'insufficient'
        }
    
    def _calculate_max_drawdown(self, price_series: pd.Series) -> float:
        """计算最大回撤"""
        cumulative = (1 + price_series.pct_change()).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        return drawdown.min() * 100


class PortfolioTracker:
    """模拟持仓跟踪系统"""
    
    def __init__(self, state_file: str = 'data/portfolio_state.json'):
        self.state_file = state_file
        self.state = self._load_state()
    
    def _load_state(self) -> Dict:
        """加载持仓状态"""
        default_state = {
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_assets': 1000000.0,
            'cash': 1000000.0,
            'portfolio_value': 0.0,
            'positions': [],
            'trade_history': []
        }
        
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    for key in default_state:
                        if key not in state:
                            state[key] = default_state[key]
                    return state
            except:
                pass
        
        return default_state
    
    def _save_state(self):
        """保存持仓状态"""
        self.state['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)
    
    def update_prices(self, price_dict: Dict[str, float]):
        """更新持仓价格"""
        for pos in self.state['positions']:
            stock_code = pos.get('stock_code')
            if stock_code in price_dict:
                pos['current_price'] = price_dict[stock_code]
                pos['current_value'] = pos['quantity'] * pos['current_price']
                pos['profit_loss'] = pos['current_value'] - pos['cost_basis']
                pos['profit_loss_pct'] = (pos['profit_loss'] / pos['cost_basis'] * 100) if pos['cost_basis'] > 0 else 0
        
        self._recalculate_portfolio()
        self._save_state()
    
    def _recalculate_portfolio(self):
        """重新计算组合价值"""
        self.state['portfolio_value'] = sum(p.get('current_value', 0) for p in self.state['positions'])
        self.state['total_assets'] = self.state['cash'] + self.state['portfolio_value']
    
    def execute_trade(self, stock_code: str, stock_name: str, 
                     price: float, amount: float, action: str = 'buy'):
        """执行交易"""
        if action == 'buy':
            quantity = int(amount / price / 100) * 100
            if quantity <= 0:
                return False
            
            cost = quantity * price
            
            if self.state['cash'] < cost:
                return False
            
            self.state['cash'] -= cost
            
            existing = next((p for p in self.state['positions'] if p['stock_code'] == stock_code), None)
            if existing:
                total_qty = existing['quantity'] + quantity
                total_cost = existing['cost_basis'] + cost
                existing['quantity'] = total_qty
                existing['avg_price'] = total_cost / total_qty
                existing['cost_basis'] = total_cost
            else:
                self.state['positions'].append({
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'quantity': quantity,
                    'avg_price': price,
                    'cost_basis': cost,
                    'current_price': price,
                    'current_value': cost,
                    'profit_loss': 0,
                    'profit_loss_pct': 0,
                    'entry_date': datetime.now().strftime('%Y-%m-%d'),
                    'holding_days': 0
                })
            
            self.state['trade_history'].append({
                'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'action': 'buy',
                'stock_code': stock_code,
                'stock_name': stock_name,
                'price': price,
                'quantity': quantity,
                'amount': cost
            })
        
        elif action == 'sell':
            pos = next((p for p in self.state['positions'] if p['stock_code'] == stock_code), None)
            if not pos:
                return False
            
            quantity = pos['quantity'] if amount is None else min(int(amount / price / 100) * 100, pos['quantity'])
            
            if quantity <= 0:
                return False
            
            proceeds = quantity * price
            self.state['cash'] += proceeds
            
            if quantity == pos['quantity']:
                self.state['positions'].remove(pos)
            else:
                pos['quantity'] -= quantity
                pos['cost_basis'] = pos['quantity'] * pos['avg_price']
            
            self.state['trade_history'].append({
                'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'action': 'sell',
                'stock_code': stock_code,
                'stock_name': stock_name,
                'price': price,
                'quantity': quantity,
                'amount': proceeds
            })
        
        self._recalculate_portfolio()
        self._save_state()
        return True
    
    def check_risk_signals(self) -> List[Dict]:
        """检查风险信号（止盈止损）"""
        signals = []
        for pos in self.state['positions']:
            pnl_pct = pos.get('profit_loss_pct', 0)
            
            if pnl_pct >= 20:
                signals.append({
                    'type': 'take_profit',
                    'stock_code': pos['stock_code'],
                    'stock_name': pos['stock_name'],
                    'pnl_pct': pnl_pct,
                    'reason': f'止盈触发（收益{pnl_pct:.1f}%>20%）'
                })
            elif pnl_pct <= -10:
                signals.append({
                    'type': 'stop_loss',
                    'stock_code': pos['stock_code'],
                    'stock_name': pos['stock_name'],
                    'pnl_pct': pnl_pct,
                    'reason': f'止损触发（亏损{pnl_pct:.1f}%<-10%）'
                })
        
        return signals


class EnhancedReportGenerator:
    """增强型报告生成器"""
    
    def __init__(self):
        pass
    
    def generate_full_report(self, 
                            selected_stocks: List[Dict],
                            factor_evaluation: Dict,
                            dynamic_weights: Dict,
                            portfolio: Dict,
                            backtest_results: Dict,
                            risk_signals: List[Dict]) -> str:
        """生成完整报告"""
        now = datetime.now()
        
        lines = []
        lines.append('🦞 A股量化日报 - 盘前推送（增强版）')
        lines.append('━━━━━━━━━━━━━━━━━━━━━━━━')
        lines.append(f'📅 推送时间: {now.strftime("%Y-%m-%d %H:%M")}')
        lines.append(f'📌 类型: 实盘推送（含因子评估、回验证）')
        lines.append('')
        
        lines.append('📊 因子有效性评估')
        lines.append('────────────────────')
        if factor_evaluation:
            for factor, result in sorted(factor_evaluation.items(), 
                                         key=lambda x: abs(x[1]['ic_mean']), reverse=True)[:6]:
                status = '✅' if result['effective'] else '⚠️'
                lines.append(f'{status} {factor}: IC={result["ic_mean"]:.3f}, IR={result["ir"]:.2f}')
        else:
            lines.append('暂无因子评估数据')
        lines.append('')
        
        lines.append('⚖️ 动态因子权重')
        lines.append('────────────────────')
        if dynamic_weights:
            for factor, weight in sorted(dynamic_weights.items(), key=lambda x: -x[1]):
                lines.append(f'  • {factor}: {weight*100:.1f}%')
        else:
            lines.append('使用默认等权')
        lines.append('')
        
        lines.append('🎯 今日推荐（附回测身份证）')
        lines.append('────────────────────')
        for i, stock in enumerate(selected_stocks[:10], 1):
            code = stock.get('stock_code', stock.get('code', 'N/A'))
            name = stock.get('stock_name', stock.get('name', 'N/A'))
            score = stock.get('score', stock.get('alpha_score', 0))
            
            backtest = backtest_results.get(str(code), {})
            
            lines.append(f'{i}. {name}({code}) - α得分: {score:.2f}')
            if backtest:
                lines.append(f'   📈 回测: 收益{backtest.get("total_return_pct", 0):.1f}% | '
                            f'夏普{backtest.get("sharpe_ratio", 0):.2f} | '
                            f'回撤{backtest.get("max_drawdown_pct", 0):.1f}% | '
                            f'胜率{backtest.get("win_rate_pct", 0):.0f}%')
        lines.append('')
        
        lines.append('💼 当前持仓')
        lines.append('────────────────────')
        positions = portfolio.get('positions', [])
        if positions:
            total_pnl = sum(p.get('profit_loss', 0) for p in positions)
            total_value = sum(p.get('current_value', 0) for p in positions)
            lines.append(f'总资产: ¥{portfolio.get("total_assets", 0):,.0f} | '
                        f'持仓: ¥{total_value:,.0f} | '
                        f'现金: ¥{portfolio.get("cash", 0):,.0f}')
            lines.append(f'总盈亏: ¥{total_pnl:,.0f} (新建持仓)' if total_pnl == 0 else f'总盈亏: ¥{total_pnl:,.0f} ({total_pnl/total_value*100:.2f}%)')
            lines.append('')
            lines.append('持仓明细:')
            for pos in positions[:5]:
                emoji = '📈' if pos.get('profit_loss_pct', 0) > 0 else '📉'
                pnl_pct = pos.get('profit_loss_pct', 0)
                pnl_text = '新建持仓' if pnl_pct == 0 else f'{pnl_pct:.1f}%'
                lines.append(f'  {emoji} {pos["stock_name"]}({pos["stock_code"]}): '
                            f'{pnl_text} | '
                            f'¥{pos["current_value"]:,.0f}')
        else:
            lines.append('当前无持仓，建议按今日推荐建仓')
        lines.append('')
        
        if risk_signals:
            lines.append('⚠️ 风险信号')
            lines.append('────────────────────')
            for signal in risk_signals:
                emoji = '🛑' if signal['type'] == 'stop_loss' else '🟢'
                lines.append(f'{emoji} {signal["stock_name"]}({signal["stock_code"]}): {signal["reason"]}')
            lines.append('')
        
        lines.append('━━━━━━━━━━━━━━━━━━━━━━')
        lines.append('📊 数据来源: AKShare + 动态因子模型')
        lines.append(f'🦞 A股量化系统 v3.0 | {now.strftime("%Y-%m-%d")}')
        
        return '\n'.join(lines)


class DailyMaster:
    """主控类"""
    
    def __init__(self):
        self.data_file = 'data/akshare_real_data_fixed.pkl'
        self.selection_file = 'data/selection_result.json'
        
        self.factor_evaluator = EnhancedFactorEvaluator()
        self.differentiated_weights = DifferentiatedFactorWeights()
        self.portfolio_tracker = PortfolioTracker()
        self.report_generator = EnhancedReportGenerator()
        
        if ML_COMBINER_AVAILABLE:
            self.ml_combiner = MLFactorCombiner(model_type='gbdt')
            logger.info("✓ ML因子组合器已加载")
        else:
            self.ml_combiner = None
            logger.warning("⚠️ ML因子组合器不可用")
        
        if DYNAMIC_WEIGHT_AVAILABLE:
            self.dynamic_weight_system = DynamicFactorWeightSystem()
            logger.info("✓ 动态因子权重系统已加载")
        else:
            self.dynamic_weight_system = None
        
        if FACTOR_RISK_AVAILABLE:
            self.factor_risk_model = FactorRiskModel()
            self.factor_exposure_monitor = FactorExposureMonitor()
            logger.info("✓ 因子风险模型已加载")
        else:
            self.factor_risk_model = None
            self.factor_exposure_monitor = None
    
    def load_data(self) -> pd.DataFrame:
        """加载数据"""
        logger.info("📥 加载市场数据...")
        if os.path.exists(self.data_file):
            with open(self.data_file, 'rb') as f:
                df = pickle.load(f)
            logger.info(f"✓ 数据加载完成，共{len(df)}条记录")
            return df
        logger.warning("⚠️ 数据文件不存在")
        return pd.DataFrame()
    
    def evaluate_factors(self, df: pd.DataFrame) -> Tuple[Dict, Dict]:
        """评估因子并更新权重"""
        logger.info("🔬 评估因子有效性...")
        evaluation = self.factor_evaluator.evaluate_factors(df)
        self.factor_evaluator.save_evaluation(evaluation)
        dynamic_weights = self.factor_evaluator.update_dynamic_weights(evaluation)
        logger.info(f"✓ 因子评估完成，有效因子{sum(1 for f in evaluation.values() if f['effective'])}个")
        return evaluation, dynamic_weights
    
    def select_stocks(self, df: pd.DataFrame) -> List[Dict]:
        """差异化选股"""
        logger.info("🎯 执行差异化选股...")
        
        if len(df) == 0:
            return []
        
        # 先获取最新日期的数据
        if 'date' in df.columns:
            latest_date = df['date'].max()
            df = df[df['date'] == latest_date].copy()
            logger.info(f"使用最新日期数据: {latest_date}")
        
        # 如果最新日期数据按股票分组后有多条记录，取最新一条
        if 'stock_code' in df.columns:
            df = df.groupby('stock_code').last().reset_index()
        
        logger.info(f"有{len(df)}只股票数据")
        
        # 检查是否有综合得分列
        score_col = None
        for col in ['alpha_score', 'score', '综合得分']:
            if col in df.columns:
                score_col = col
                break
        
        # 如果没有得分列，使用amount作为排序依据
        if score_col is None:
            if 'amount' in df.columns:
                df['temp_score'] = df['amount'] / df['amount'].max() if df['amount'].max() > 0 else 1.0
                score_col = 'temp_score'
                logger.info("使用成交额作为排序依据")
            else:
                logger.error("❌ 无可用排序依据")
                return []
        
        # 按股票分组去重，然后按得分排序选取前20只不同股票
        unique_stocks = df.drop_duplicates(subset=['stock_code'])
        selected = unique_stocks.nlargest(20, score_col).copy()
        
        # 确保选出的股票不重复
        selected = selected.drop_duplicates(subset=['stock_code'])
        
        result = []
        for idx, (_, row) in enumerate(selected.iterrows()):
            code = row.get('stock_code', '')
            # 确保股票代码为字符串
            if not isinstance(code, str):
                code = str(code)
            
            result.append({
                'rank': idx + 1,
                'stock_code': code,
                'stock_name': row.get('stock_name', row.get('股票名称', f'股票{idx+1}')),
                'score': round(float(row.get(score_col, 0)), 4),
                'industry': row.get('industry', row.get('行业', '未知'))
            })
        
        # 去重结果
        seen_codes = set()
        unique_result = []
        for item in result:
            if item['stock_code'] not in seen_codes:
                seen_codes.add(item['stock_code'])
                unique_result.append(item)
        
        with open(self.selection_file, 'w', encoding='utf-8') as f:
            json.dump({
                'selected_stocks': unique_result,
                'timestamp': datetime.now().isoformat()
            }, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✓ 选股完成，选出{len(unique_result)}只股票")
        return unique_result[:10]
    
    def optimize_with_ml(self, df: pd.DataFrame, selected_stocks: List[Dict]) -> List[Dict]:
        """使用ML因子组合优化选股结果"""
        if self.ml_combiner is None:
            logger.warning("⚠️ ML组合器不可用，跳过优化")
            return selected_stocks
        
        logger.info("🤖 ML因子组合优化...")
        
        try:
            factor_cols = [col for col in df.columns if col not in 
                          ['date', 'stock_code', 'stock_name', 'close', 'open', 'high', 'low', 
                           'volume', 'amount', 'return_1d', 'return_5d', 'return_20d']]
            
            if not factor_cols:
                logger.warning("⚠️ 未找到因子列，跳过ML优化")
                return selected_stocks
            
            selected_codes = [s['stock_code'] for s in selected_stocks]
            latest_date = df['date'].max() if 'date' in df.columns else None
            
            if latest_date:
                latest_df = df[df['date'] == latest_date]
            else:
                latest_df = df
            
            factor_data = latest_df[latest_df['stock_code'].isin(selected_codes)][['stock_code'] + factor_cols]
            
            if len(factor_data) == 0:
                logger.warning("⚠️ 因子数据为空，跳过ML优化")
                return selected_stocks
            
            factor_data = factor_data.set_index('stock_code')
            factor_data = factor_data.fillna(0)
            
            ml_scores = self.ml_combiner.predict(factor_data)
            
            if ml_scores is not None and len(ml_scores) > 0:
                for stock in selected_stocks:
                    code = stock['stock_code']
                    if code in ml_scores.index:
                        stock['ml_score'] = float(ml_scores.loc[code])
                        stock['combined_score'] = 0.6 * stock.get('score', 0) + 0.4 * stock.get('ml_score', 0)
                    else:
                        stock['ml_score'] = 0
                        stock['combined_score'] = stock.get('score', 0)
                
                selected_stocks = sorted(selected_stocks, key=lambda x: x.get('combined_score', 0), reverse=True)
                
                for i, stock in enumerate(selected_stocks):
                    stock['rank'] = i + 1
                
                logger.info(f"✓ ML优化完成，已重新排序")
            else:
                logger.warning("⚠️ ML预测结果为空，使用原始排序")
            
        except Exception as e:
            logger.error(f"❌ ML优化失败: {e}")
        
        return selected_stocks
    
    def apply_dynamic_weights(self, df: pd.DataFrame) -> Dict[str, float]:
        """应用动态因子权重"""
        if self.dynamic_weight_system is None:
            logger.warning("⚠️ 动态权重系统不可用，使用默认权重")
            return {}
        
        logger.info("⚖️ 计算动态因子权重...")
        
        try:
            weights = self.dynamic_weight_system.get_weights()
            logger.info(f"✓ 动态权重计算完成")
            return weights
        except Exception as e:
            logger.error(f"❌ 动态权重计算失败: {e}")
            return {}
    
    def analyze_factor_risk(self, df: pd.DataFrame, selected_stocks: List[Dict]) -> Dict:
        """分析因子风险和暴露"""
        if self.factor_risk_model is None:
            logger.warning("⚠️ 因子风险模型不可用")
            return {}
        
        logger.info("📊 分析因子风险暴露...")
        
        try:
            selected_codes = [s['stock_code'] for s in selected_stocks]
            
            factor_cols = [col for col in df.columns if col in [
                'pe_ratio', 'pb_ratio', 'roe', 'roa', 'debt_ratio',
                'current_ratio', 'gross_margin', 'net_margin',
                'revenue_growth', 'profit_growth', 'momentum_1m',
                'momentum_3m', 'volatility_20d', 'turnover_rate'
            ]]
            
            if len(factor_cols) == 0:
                logger.warning("⚠️ 无可用因子数据")
                return {}
            
            latest_df = df
            if 'date' in df.columns:
                latest_date = df['date'].max()
                latest_df = df[df['date'] == latest_date]
            
            factor_data = latest_df[latest_df['stock_code'].isin(selected_codes)][factor_cols].fillna(0)
            
            if len(factor_data) == 0:
                return {}
            
            portfolio_exposure = self.factor_risk_model.calculate_portfolio_exposure(
                factor_data.values,
                weights=None
            )
            
            exposure_alerts = self.factor_exposure_monitor.check_exposure(
                portfolio_exposure
            )
            
            risk_attribution = self.factor_risk_model.risk_attribution(
                factor_data.values,
                weights=None
            )
            
            result = {
                'portfolio_exposure': portfolio_exposure,
                'exposure_alerts': exposure_alerts,
                'risk_attribution': risk_attribution,
                'factor_names': factor_cols
            }
            
            if exposure_alerts:
                logger.warning(f"⚠️ 因子暴露预警: {len(exposure_alerts)}项")
            else:
                logger.info("✓ 因子暴露正常")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 因子风险分析失败: {e}")
            return {}

    def validate_stocks(self, df: pd.DataFrame, selected_stocks: List[Dict]) -> Tuple[Dict, List[Dict]]:
        """回实验证选中的股票"""
        logger.info("📊 回实验证选中股票...")
        validator = StockBacktestValidator(df)
        
        results = {}
        for stock in selected_stocks:
            code = str(stock['stock_code'])
            results[code] = validator.quick_backtest(code)
        
        # 风险过滤：排除回撤>-50%的股票
        filtered_stocks = []
        for stock in selected_stocks:
            code = str(stock['stock_code'])
            backtest = results.get(code, {})
            max_drawdown = backtest.get('max_drawdown_pct', 0)
            
            # 只保留回撤<=50%的股票
            if max_drawdown >= -50:
                filtered_stocks.append(stock)
            else:
                logger.warning(f"⚠️ 股票{code}回撤过大({max_drawdown:.1f}%)，已排除")
        
        # 如果过滤后股票数量不足，记录警告
        if len(filtered_stocks) < len(selected_stocks):
            logger.warning(f"⚠️ 风险过滤后剩余{len(filtered_stocks)}只股票（原{len(selected_stocks)}只）")
        
        logger.info("✓ 回实验证完成")
        return results, filtered_stocks
    
    def update_portfolio(self, df: pd.DataFrame, selected_stocks: List[Dict]):
        """更新持仓"""
        logger.info("💼 更新持仓状态...")
        
        price_dict = {}
        if 'stock_code' in df.columns and 'close' in df.columns:
            latest_df = df
            if 'date' in df.columns:
                latest_date = df['date'].max()
                latest_df = df[df['date'] == latest_date]
            
            for _, row in latest_df.iterrows():
                code = str(row['stock_code'])
                price_dict[code] = row['close']
        
        self.portfolio_tracker.update_prices(price_dict)
        
        risk_signals = self.portfolio_tracker.check_risk_signals()
        
        portfolio = self.portfolio_tracker.state
        if not portfolio['positions']:
            logger.info("📝 无持仓，执行初始建仓...")
            for stock in selected_stocks[:5]:
                code = stock['stock_code']
                name = stock['stock_name']
                price = price_dict.get(code, 10.0)
                amount = portfolio['total_assets'] * 0.12
                self.portfolio_tracker.execute_trade(code, name, price, amount, 'buy')
        
        logger.info("✓ 持仓更新完成")
        return portfolio, risk_signals
    
    def generate_and_save_report(self, selected_stocks, factor_evaluation, 
                                dynamic_weights, portfolio, backtest_results, risk_signals):
        """生成并保存报告"""
        logger.info("📝 生成报告...")
        try:
            report = self.report_generator.generate_full_report(
                selected_stocks, factor_evaluation, dynamic_weights, 
                portfolio, backtest_results, risk_signals
            )
            
            os.makedirs('reports', exist_ok=True)
            report_file = f'reports/morning_push_{datetime.now().strftime("%Y%m%d_%H%M")}.md'
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            
            logger.info(f"✓ 报告已保存: {report_file}")
            return report, report_file
        except Exception as e:
            logger.error(f"❌ 报告生成失败: {e}")
            return None, None
    
    def run(self):
        """运行完整流程"""
        logger.info("\n" + "="*70)
        logger.info("🚀 A股量化系统 - 每日主控流程 v3.1")
        logger.info("="*70)
        
        try:
            df = self.load_data()
            if len(df) == 0:
                logger.error("❌ 无数据，流程终止")
                return False
            
            factor_evaluation, dynamic_weights = self.evaluate_factors(df)
            
            dynamic_factor_weights = self.apply_dynamic_weights(df)
            if dynamic_factor_weights:
                logger.info(f"  动态权重: {list(dynamic_factor_weights.items())[:3]}...")
            
            selected_stocks = self.select_stocks(df)
            
            selected_stocks = self.optimize_with_ml(df, selected_stocks)
            
            factor_risk_analysis = self.analyze_factor_risk(df, selected_stocks)
            
            backtest_results, filtered_stocks = self.validate_stocks(df, selected_stocks)
            
            if len(filtered_stocks) == 0:
                logger.warning("⚠️ 风险过滤后无股票可选，使用原始选股结果")
                filtered_stocks = selected_stocks
            
            portfolio, risk_signals = self.update_portfolio(df, filtered_stocks)
            report, report_file = self.generate_and_save_report(
                filtered_stocks, factor_evaluation, dynamic_weights,
                portfolio, backtest_results, risk_signals
            )
            
            logger.info("\n" + "="*70)
            logger.info("✅ 每日主控流程完成")
            logger.info(f"  - 选股数量: {len(filtered_stocks)}")
            logger.info(f"  - ML优化: {'已启用' if self.ml_combiner else '未启用'}")
            logger.info(f"  - 动态权重: {'已启用' if self.dynamic_weight_system else '未启用'}")
            logger.info("="*70)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 流程执行失败: {e}", exc_info=True)
            return False


def main():
    master = DailyMaster()
    success = master.run()
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
