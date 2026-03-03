#!/usr/bin/env python3
"""
智能资金管理系统
功能：
1. 动态资金分配
2. 风险预算管理
3. 仓位调整策略
4. 资金使用效率优化
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import json
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FundManager:
    """智能资金管理器"""
    
    def __init__(self, initial_capital: float, risk_budget: float = 0.15):
        """
        初始化资金管理器
        
        Args:
            initial_capital: 初始资金
            risk_budget: 风险预算（最大回撤限制）
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.risk_budget = risk_budget
        self.cash = initial_capital
        self.positions = {}
        self.portfolio_value = initial_capital
        self.risk_exposure = 0.0
        self.alpha_signals = {}
        self.risk_signals = {}
        self.transaction_history = []
        self.portfolio_history = []
        self.risk_history = []
        
    def update_portfolio(self, prices: Dict[str, float]):
        """更新投资组合价值"""
        market_value = 0.0
        for stock, pos in self.positions.items():
            if stock in prices:
                pos['market_value'] = pos['quantity'] * prices[stock]
                market_value += pos['market_value']
        
        self.portfolio_value = self.cash + market_value
        self.current_capital = self.portfolio_value
        
        # 记录投资组合历史
        self.portfolio_history.append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'portfolio_value': self.portfolio_value,
            'cash': self.cash,
            'market_value': market_value,
            'positions': {k: v['market_value'] for k, v in self.positions.items()}
        })
    
    def calculate_risk_exposure(self, volatility: Dict[str, float]) -> float:
        """计算风险暴露"""
        if not self.positions:
            return 0.0
        
        total_risk = 0.0
        total_value = self.portfolio_value
        
        for stock, pos in self.positions.items():
            weight = pos['market_value'] / total_value
            stock_volatility = volatility.get(stock, 0.2)  # 默认波动率20%
            total_risk += weight * stock_volatility
        
        self.risk_exposure = total_risk
        
        # 记录风险历史
        self.risk_history.append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'risk_exposure': total_risk
        })
        
        return total_risk
    
    def allocate_funds(self, alpha_scores: Dict[str, float], risk_scores: Dict[str, float]) -> Dict[str, float]:
        """
        基于alpha和风险分数分配资金
        
        Args:
            alpha_scores: 股票alpha分数
            risk_scores: 股票风险分数
            
        Returns:
            资金分配比例
        """
        self.alpha_signals = alpha_scores
        self.risk_signals = risk_scores
        
        # 计算调整后的分数
        adjusted_scores = {}
        for stock, alpha in alpha_scores.items():
            risk = risk_scores.get(stock, 1.0)
            adjusted_score = alpha / risk  # 风险调整后的alpha
            adjusted_scores[stock] = adjusted_score
        
        # 归一化
        total_score = sum(abs(s) for s in adjusted_scores.values())
        if total_score == 0:
            return {}
        
        allocation = {}
        for stock, score in adjusted_scores.items():
            allocation[stock] = abs(score) / total_score
        
        return allocation
    
    def adjust_positions(self, target_allocation: Dict[str, float], prices: Dict[str, float], 
                         max_position: float = 0.15, min_position: float = 0.01):
        """调整仓位"""
        total_value = self.portfolio_value
        
        # 计算目标仓位
        target_positions = {}
        for stock, weight in target_allocation.items():
            # 限制单票仓位
            weight = min(weight, max_position)
            weight = max(weight, min_position)
            target_positions[stock] = {
                'weight': weight,
                'target_value': total_value * weight
            }
        
        # 计算现有仓位
        current_positions = {}
        for stock, pos in self.positions.items():
            if stock in prices:
                current_positions[stock] = {
                    'quantity': pos['quantity'],
                    'current_value': pos['quantity'] * prices[stock]
                }
        
        # 卖出不需要的仓位
        for stock, pos in current_positions.items():
            if stock not in target_positions:
                # 全部卖出
                sell_quantity = pos['quantity']
                if sell_quantity > 0:
                    self.sell(stock, sell_quantity, prices[stock])
        
        # 调整现有仓位
        for stock, target in target_positions.items():
            if stock in current_positions:
                current_value = current_positions[stock]['current_value']
                target_value = target['target_value']
                
                if abs(target_value - current_value) / total_value > 0.01:  # 差异超过1%
                    if target_value > current_value:
                        # 买入
                        buy_amount = target_value - current_value
                        buy_quantity = int(buy_amount / prices[stock] / 100) * 100
                        if buy_quantity > 0:
                            self.buy(stock, buy_quantity, prices[stock])
                    else:
                        # 卖出
                        sell_amount = current_value - target_value
                        current_quantity = current_positions[stock]['quantity']
                        sell_quantity = int(sell_amount / prices[stock] / 100) * 100
                        sell_quantity = min(sell_quantity, current_quantity)
                        if sell_quantity > 0:
                            self.sell(stock, sell_quantity, prices[stock])
            else:
                # 新建仓位
                target_value = target['target_value']
                buy_quantity = int(target_value / prices[stock] / 100) * 100
                if buy_quantity > 0:
                    self.buy(stock, buy_quantity, prices[stock])
    
    def buy(self, stock: str, quantity: int, price: float):
        """买入股票"""
        total_cost = quantity * price
        if total_cost > self.cash:
            logger.warning(f"资金不足，无法买入 {stock} {quantity}股 @ {price}")
            return
        
        self.cash -= total_cost
        
        if stock in self.positions:
            # 更新持仓
            pos = self.positions[stock]
            total_cost = pos['avg_price'] * pos['quantity'] + total_cost
            total_quantity = pos['quantity'] + quantity
            pos['avg_price'] = total_cost / total_quantity
            pos['quantity'] = total_quantity
            pos['market_value'] = total_quantity * price
        else:
            # 新建持仓
            self.positions[stock] = {
                'quantity': quantity,
                'avg_price': price,
                'market_value': quantity * price
            }
        
        # 记录交易
        self.transaction_history.append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'type': 'buy',
            'stock': stock,
            'quantity': quantity,
            'price': price,
            'amount': total_cost
        })
        
        logger.info(f"买入: {stock} {quantity}股 @ {price}，总成本: {total_cost}")
    
    def sell(self, stock: str, quantity: int, price: float):
        """卖出股票"""
        if stock not in self.positions:
            logger.warning(f"没有{stock}的持仓")
            return
        
        pos = self.positions[stock]
        if quantity > pos['quantity']:
            logger.warning(f"卖出数量超过持仓数量")
            quantity = pos['quantity']
        
        total_proceeds = quantity * price
        self.cash += total_proceeds
        
        pos['quantity'] -= quantity
        pos['market_value'] = pos['quantity'] * price
        
        if pos['quantity'] == 0:
            del self.positions[stock]
        
        # 记录交易
        self.transaction_history.append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'type': 'sell',
            'stock': stock,
            'quantity': quantity,
            'price': price,
            'amount': total_proceeds
        })
        
        logger.info(f"卖出: {stock} {quantity}股 @ {price}，总收入: {total_proceeds}")
    
    def rebalance_based_on_risk(self, volatility: Dict[str, float], target_risk: float = 0.15):
        """基于风险重新平衡"""
        current_risk = self.calculate_risk_exposure(volatility)
        
        if current_risk > target_risk:
            # 需要降低风险
            reduction_ratio = target_risk / current_risk
            total_value = self.portfolio_value
            
            # 计算需要卖出的金额
            target_market_value = total_value * (1 - self.cash / total_value) * reduction_ratio
            current_market_value = total_value - self.cash
            sell_amount = current_market_value - target_market_value
            
            if sell_amount > 0:
                # 按风险比例卖出
                risk_contributions = {}
                for stock, pos in self.positions.items():
                    weight = pos['market_value'] / current_market_value
                    stock_volatility = volatility.get(stock, 0.2)
                    risk_contributions[stock] = weight * stock_volatility
                
                total_risk_contribution = sum(risk_contributions.values())
                
                for stock, contribution in risk_contributions.items():
                    if stock in volatility:
                        sell_weight = (contribution / total_risk_contribution) * sell_amount
                        pos = self.positions[stock]
                        sell_quantity = int(sell_weight / volatility[stock] / 100) * 100
                        if sell_quantity > 0:
                            self.sell(stock, sell_quantity, volatility[stock])
    
    def generate_fund_report(self) -> Dict:
        """生成资金管理报告"""
        total_value = self.portfolio_value
        market_value = total_value - self.cash
        cash_ratio = self.cash / total_value
        
        position_details = {}
        for stock, pos in self.positions.items():
            position_details[stock] = {
                'quantity': pos['quantity'],
                'avg_price': pos['avg_price'],
                'current_value': pos['market_value'],
                'weight': pos['market_value'] / total_value
            }
        
        # 计算绩效
        total_return = (total_value / self.initial_capital) - 1
        
        # 计算最大回撤
        max_drawdown = 0.0
        if self.portfolio_history:
            values = [h['portfolio_value'] for h in self.portfolio_history]
            peak = values[0]
            for val in values[1:]:
                if val > peak:
                    peak = val
                drawdown = (peak - val) / peak
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
        
        report = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'initial_capital': self.initial_capital,
            'current_capital': self.current_capital,
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'cash': self.cash,
            'cash_ratio': cash_ratio,
            'market_value': market_value,
            'positions': position_details,
            'risk_exposure': self.risk_exposure,
            'transaction_count': len(self.transaction_history)
        }
        
        return report
    
    def save_report(self, output_dir: str = '/Users/variya/.openclaw/workspace/projects/a-stock-advisor/reports'):
        """保存报告"""
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report = self.generate_fund_report()
        
        # 保存JSON报告
        json_path = os.path.join(output_dir, f'fund_management_{timestamp}.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        # 生成Markdown报告
        md_report = self._generate_markdown_report(report)
        md_path = os.path.join(output_dir, f'fund_management_{timestamp}.md')
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(md_report)
        
        logger.info(f"报告已保存: {json_path}")
        logger.info(f"Markdown报告已保存: {md_path}")
        
        return json_path
    
    def _generate_markdown_report(self, report: Dict) -> str:
        """生成Markdown报告"""
        lines = []
        
        lines.append("# 资金管理报告\n")
        lines.append(f"**生成时间:** {report['timestamp']}\n")
        lines.append("---\n")
        
        # 基本信息
        lines.append("## 📊 基本信息\n")
        lines.append(f"- **初始资金:** ¥{report['initial_capital']:,.2f}\n")
        lines.append(f"- **当前资金:** ¥{report['current_capital']:,.2f}\n")
        lines.append(f"- **总收益率:** {report['total_return']:.2%}\n")
        lines.append(f"- **最大回撤:** {report['max_drawdown']:.2%}\n")
        lines.append(f"- **现金比例:** {report['cash_ratio']:.2%}\n")
        lines.append(f"- **风险暴露:** {report['risk_exposure']:.2%}\n")
        lines.append(f"- **交易次数:** {report['transaction_count']}\n\n")
        
        # 持仓详情
        lines.append("## 📈 持仓详情\n")
        if report['positions']:
            lines.append("| 股票 | 数量 | 成本价 | 当前价值 | 权重 |\n")
            lines.append("|------|------|--------|----------|------|\n")
            for stock, details in report['positions'].items():
                lines.append(f"| {stock} | {details['quantity']} | {details['avg_price']:.2f} | {details['current_value']:,.2f} | {details['weight']:.2%} |\n")
        else:
            lines.append("无持仓\n")
        lines.append("\n")
        
        # 风险分析
        lines.append("## 🛡️ 风险分析\n")
        lines.append(f"- **当前风险暴露:** {report['risk_exposure']:.2%}\n")
        lines.append(f"- **风险预算:** {self.risk_budget:.2%}\n")
        
        if report['risk_exposure'] > self.risk_budget:
            lines.append("- **风险状态:** ⚠️ 超出风险预算\n")
        else:
            lines.append("- **风险状态:** ✅ 在风险预算内\n")
        lines.append("\n")
        
        # 交易历史
        lines.append("## 💱 最近交易\n")
        if self.transaction_history:
            recent_transactions = self.transaction_history[-10:]  # 最近10笔交易
            for tx in reversed(recent_transactions):
                lines.append(f"- {tx['timestamp']}: {tx['type']} {tx['stock']} {tx['quantity']}股 @ {tx['price']} - ¥{tx['amount']:,.2f}\n")
        else:
            lines.append("无交易记录\n")
        lines.append("\n")
        
        lines.append("---\n")
        lines.append("\n## 📋 总结\n\n")
        lines.append("本报告提供了资金管理的详细信息，包括资产配置、风险暴露和交易历史。\n")
        
        return '\n'.join(lines)


class RiskBudgetManager:
    """风险预算管理器"""
    
    def __init__(self, total_risk_budget: float = 0.15):
        """
        初始化风险预算管理器
        
        Args:
            total_risk_budget: 总风险预算（最大回撤限制）
        """
        self.total_risk_budget = total_risk_budget
        self.factor_risk_budgets = {}
        self.sector_risk_budgets = {}
        self.stock_risk_budgets = {}
        self.risk_allocation = {}
    
    def set_factor_risk_budgets(self, factor_budgets: Dict[str, float]):
        """设置因子风险预算"""
        self.factor_risk_budgets = factor_budgets
    
    def set_sector_risk_budgets(self, sector_budgets: Dict[str, float]):
        """设置行业风险预算"""
        self.sector_risk_budgets = sector_budgets
    
    def set_stock_risk_budgets(self, stock_budgets: Dict[str, float]):
        """设置个股风险预算"""
        self.stock_risk_budgets = stock_budgets
    
    def allocate_risk(self, factor_exposures: Dict[str, float], 
                      sector_exposures: Dict[str, float],
                      stock_volatilities: Dict[str, float]) -> Dict:
        """
        分配风险预算
        
        Args:
            factor_exposures: 因子暴露
            sector_exposures: 行业暴露
            stock_volatilities: 个股波动率
            
        Returns:
            风险分配方案
        """
        # 计算因子风险贡献
        factor_risk = {}
        total_factor_exposure = sum(abs(v) for v in factor_exposures.values())
        if total_factor_exposure > 0:
            for factor, exposure in factor_exposures.items():
                budget = self.factor_risk_budgets.get(factor, 0.05)
                factor_risk[factor] = budget * abs(exposure) / total_factor_exposure
        
        # 计算行业风险贡献
        sector_risk = {}
        total_sector_exposure = sum(abs(v) for v in sector_exposures.values())
        if total_sector_exposure > 0:
            for sector, exposure in sector_exposures.items():
                budget = self.sector_risk_budgets.get(sector, 0.10)
                sector_risk[sector] = budget * abs(exposure) / total_sector_exposure
        
        # 计算个股风险贡献
        stock_risk = {}
        total_volatility = sum(stock_volatilities.values())
        if total_volatility > 0:
            for stock, volatility in stock_volatilities.items():
                budget = self.stock_risk_budgets.get(stock, 0.02)
                stock_risk[stock] = budget * volatility / total_volatility
        
        # 综合风险分配
        self.risk_allocation = {
            'factor_risk': factor_risk,
            'sector_risk': sector_risk,
            'stock_risk': stock_risk,
            'total_risk_budget': self.total_risk_budget
        }
        
        return self.risk_allocation
    
    def check_risk_limits(self, current_exposures: Dict[str, float], 
                         risk_type: str = 'factor') -> Dict[str, bool]:
        """
        检查风险限制
        
        Args:
            current_exposures: 当前暴露
            risk_type: 风险类型 ('factor', 'sector', 'stock')
            
        Returns:
            风险限制检查结果
        """
        checks = {}
        
        if risk_type == 'factor':
            budgets = self.factor_risk_budgets
        elif risk_type == 'sector':
            budgets = self.sector_risk_budgets
        else:  # stock
            budgets = self.stock_risk_budgets
        
        for item, exposure in current_exposures.items():
            budget = budgets.get(item, 0.05)
            checks[item] = abs(exposure) <= budget
        
        return checks
    
    def generate_risk_budget_report(self) -> Dict:
        """生成风险预算报告"""
        report = {
            'total_risk_budget': self.total_risk_budget,
            'factor_risk_budgets': self.factor_risk_budgets,
            'sector_risk_budgets': self.sector_risk_budgets,
            'stock_risk_budgets': self.stock_risk_budgets,
            'risk_allocation': self.risk_allocation,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return report


if __name__ == '__main__':
    # 测试资金管理器
    print("=== 资金管理器测试 ===")
    
    # 初始化资金管理器
    fund_manager = FundManager(initial_capital=1000000, risk_budget=0.15)
    
    # 模拟价格
    prices = {
        '600519': 1800.0,
        '000858': 160.0,
        '000333': 60.0,
        '601318': 45.0,
        '600036': 35.0
    }
    
    # 模拟alpha和风险分数
    alpha_scores = {
        '600519': 0.8,
        '000858': 0.7,
        '000333': 0.6,
        '601318': 0.5,
        '600036': 0.4
    }
    
    risk_scores = {
        '600519': 1.2,
        '000858': 1.1,
        '000333': 0.9,
        '601318': 0.8,
        '600036': 0.7
    }
    
    # 分配资金
    allocation = fund_manager.allocate_funds(alpha_scores, risk_scores)
    print(f"资金分配: {allocation}")
    
    # 调整仓位
    fund_manager.adjust_positions(allocation, prices)
    
    # 更新投资组合
    fund_manager.update_portfolio(prices)
    
    # 生成报告
    report = fund_manager.generate_fund_report()
    print(f"总收益率: {report['total_return']:.2%}")
    print(f"风险暴露: {report['risk_exposure']:.2%}")
    
    # 保存报告
    fund_manager.save_report()
    
    # 测试风险预算管理器
    print("\n=== 风险预算管理器测试 ===")
    
    risk_budget_manager = RiskBudgetManager(total_risk_budget=0.15)
    
    # 设置因子风险预算
    factor_budgets = {
        'value': 0.05,
        'momentum': 0.04,
        'quality': 0.03,
        'size': 0.02
    }
    risk_budget_manager.set_factor_risk_budgets(factor_budgets)
    
    # 设置行业风险预算
    sector_budgets = {
        '银行': 0.10,
        '医药': 0.08,
        '消费': 0.07,
        '科技': 0.10
    }
    risk_budget_manager.set_sector_risk_budgets(sector_budgets)
    
    # 模拟暴露
    factor_exposures = {
        'value': 0.6,
        'momentum': 0.4,
        'quality': 0.3,
        'size': -0.2
    }
    
    sector_exposures = {
        '银行': 0.3,
        '医药': 0.2,
        '消费': 0.25,
        '科技': 0.25
    }
    
    stock_volatilities = {
        '600519': 0.25,
        '000858': 0.20,
        '000333': 0.18,
        '601318': 0.15,
        '600036': 0.12
    }
    
    # 分配风险
    risk_allocation = risk_budget_manager.allocate_risk(factor_exposures, sector_exposures, stock_volatilities)
    print(f"风险分配: {risk_allocation}")
    
    # 检查风险限制
    factor_checks = risk_budget_manager.check_risk_limits(factor_exposures, 'factor')
    print(f"因子风险检查: {factor_checks}")
