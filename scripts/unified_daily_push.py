#!/usr/bin/env python3
"""
A股量化日报 - 统一推送系统（v2.1 - 增强版）
功能：生成完整的交易协助推送（盘前和日报合并）
      集成交易执行细节、持仓监控、市场状态监控
执行时机：工作日8:00（盘前推送）和18:30（日报推送）
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'code'))
sys.path.append(os.path.join(os.path.dirname(__file__)))  # 添加scripts路径以导入监控模块

from datetime import datetime, timedelta
import logging
import json
import pickle
import pandas as pd
import numpy as np
import requests

# 导入监控模块
try:
    from portfolio_monitor import PortfolioMonitor
    from market_monitor import MarketMonitor
    MONITORS_AVAILABLE = True
    logger = logging.getLogger(__name__)
    logger.info("监控模块导入成功")
except Exception as e:
    logger = logging.getLogger(__name__)
    logger.error(f"监控模块初始化失败: {e}", exc_info=True)
    MONITORS_AVAILABLE = False

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/unified_push.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class UnifiedDailyPusher:
    """统一日报推送器（增强版）"""
    
    def __init__(self):
        """初始化"""
        self.config = self._load_config()
        self.push_history_file = 'data/push_history.json'
        self.push_history = self._load_push_history()
        
        # 初始化监控模块
        if MONITORS_AVAILABLE:
            self.portfolio_monitor = PortfolioMonitor()
            self.market_monitor = MarketMonitor()
        
    def _load_config(self):
        """加载配置"""
        config_file = 'config/feishu_config.json'
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _load_push_history(self):
        """加载推送历史"""
        if os.path.exists(self.push_history_file):
            try:
                with open(self.push_history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return []
        return []
    
    def _save_push_history(self, push_data):
        """保存推送历史"""
        self.push_history.append(push_data)
        # 只保留最近30条
        if len(self.push_history) > 30:
            self.push_history = self.push_history[-30:]
        
        os.makedirs(os.path.dirname(self.push_history_file), exist_ok=True)
        with open(self.push_history_file, 'w', encoding='utf-8') as f:
            json.dump(self.push_history, f, ensure_ascii=False, indent=2)
    
    def _get_yesterday_push(self):
        """获取昨日推送"""
        if not self.push_history:
            return None
        
        today = datetime.now().strftime('%Y-%m-%d')
        # 找到最近一次非今日的推送
        for push in reversed(self.push_history):
            if push.get('date') != today:
                return push
        return None
    
    def _load_stock_data(self):
        """加载股票数据"""
        try:
            # 优先使用真实数据
            data_file = 'data/akshare_real_data_fixed.pkl'
            if os.path.exists(data_file):
                with open(data_file, 'rb') as f:
                    df = pickle.load(f)
                
                # 获取最新日期数据
                if 'date_dt' in df.columns:
                    latest_date = df['date_dt'].max()
                    latest_df = df[df['date_dt'] == latest_date].copy()
                else:
                    # 如果没有date_dt列，直接使用全部数据
                    latest_df = df.copy()
                
                # 过滤有效数据
                latest_df = latest_df[(latest_df['close'] > 0) & (latest_df['amount'] > 0)]
                
                # 过滤A股（sh和sz开头）
                if 'stock_code' in latest_df.columns:
                    a_stock_df = latest_df[latest_df['stock_code'].str[:2].isin(['sh', 'sz'])].copy()
                    return a_stock_df, latest_date if 'date_dt' in df.columns else datetime.now()
                
                return latest_df, latest_date if 'date_dt' in df.columns else datetime.now()
        except Exception as e:
            logger.error(f"加载数据失败: {e}")
            return None, None
    
    def _load_portfolio_state(self):
        """加载持仓状态"""
        portfolio_file = 'data/portfolio_state.json'
        if os.path.exists(portfolio_file):
            try:
                with open(portfolio_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return None
    
    def _load_selection_result(self):
        """加载选股结果"""
        # 优先使用市场选择器生成的结果
        try:
            from multi_source_fetcher import MultiSourceStockFetcher
            from market_wide_selector import MarketWideStockSelector
            
            fetcher = MultiSourceStockFetcher()
            selector = MarketWideStockSelector(fetcher)
            
            core, satellite, ic = selector.run_full_selection(n_core=5, n_satellite=5, max_stock_pool=100)
            
            selected_stocks = []
            rank = 1
            for stock in core + satellite:
                selected_stocks.append({
                    'rank': rank,
                    'stock_code': stock['code'],
                    'stock_name': stock['name'],
                    'score': stock.get('alpha_score', 0),
                    'reasons': '基于因子得分推荐'
                })
                rank += 1
            
            fetcher.close()
            
            return {
                'selected_stocks': selected_stocks,
                'portfolio_config': {
                    'n': len(selected_stocks),
                    'rebalance_frequency': 'daily',
                    'weighting_method': 'equal_weight',
                    'score_threshold': 0
                }
            }
        except Exception as e:
            logger.error(f"生成选股结果失败: {e}")
            
        # 回退到读取文件
        selection_file = 'data/selection_result.json'
        if os.path.exists(selection_file):
            try:
                with open(selection_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return None
    
    def _load_rebalance_plan(self):
        """加载换仓计划"""
        rebalance_file = 'data/rebalance_plan.json'
        if os.path.exists(rebalance_file):
            try:
                with open(rebalance_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return None
    
    def _generate_executable_trade_list(self, selection, stock_data=None, total_capital=1000000):
        """
        生成可执行交易清单
        
        Args:
            selection: 选股结果
            stock_data: 股票数据
            total_capital: 总资金
            
        Returns:
            交易清单文本
        """
        if not selection or 'selected_stocks' not in selection:
            return None
        
        selected_stocks = selection['selected_stocks']
        if not selected_stocks:
            return None
        
        lines = []
        lines.append('🎯 可执行交易清单')
        lines.append('')
        lines.append('序号 | 股票名称 | 股票代码 | 方向 | 目标权重 | 计划金额(100万) | 价格区间 | 买入时间')
        lines.append('─' * 100)
        
        total_weight = 0
        total_amount = 0
        
        for idx, stock in enumerate(selected_stocks[:10], 1):
            code = stock.get('code', stock.get('stock_code', 'N/A'))
            name = stock.get('name', stock.get('stock_name', 'N/A'))
            
            # 获取当前价格（如果有数据）
            current_price = 0
            price_low = 0
            price_high = 0
            
            if stock_data is not None and len(stock_data) > 0:
                stock_info = stock_data[stock_data['stock_code'] == code]
                if len(stock_info) > 0:
                    current_price = stock_info['close'].iloc[-1]
                    # 价格区间：开盘价±2%（简化为当前价±2%）
                    price_low = current_price * 0.98
                    price_high = current_price * 1.02
            
            # 目标权重（等权重，10%每只）
            target_weight = 10.0
            planned_amount = int(total_capital * target_weight / 100)
            
            direction = '买入'  # 默认买入
            
            lines.append(f'{idx} | {name} | {code} | {direction} | {target_weight}% | {planned_amount:,}元 | {price_low:.2f}-{price_high:.2f}元 | 9:30-10:00')
            
            total_weight += target_weight
            total_amount += planned_amount
        
        lines.append('─' * 100)
        lines.append(f'合计 | - | - | - | {total_weight}% | {total_amount:,}元 | - | -')
        lines.append('')
        
        return '\n'.join(lines)
    
    def _generate_execution_rules(self):
        """
        生成具体执行规则
        
        Returns:
            执行规则文本
        """
        lines = []
        lines.append('⚙️ 具体执行规则')
        lines.append('')
        lines.append('止盈规则：')
        lines.append('• 收益 > 20% → 分3批止盈')
        lines.append('• 第一批(1/3)：当日14:30-15:00')
        lines.append('• 第二批(1/3)：次日9:30-10:00')
        lines.append('• 第三批(1/3)：次日14:30-15:00')
        lines.append('')
        lines.append('止损规则：')
        lines.append('• 亏损 < -10% → 立即清仓（5分钟内）')
        lines.append('• 滑点容忍：±3%')
        lines.append('')
        lines.append('换仓规则：')
        lines.append('• 时间换仓：持仓>60天 → 评估换仓')
        lines.append('• 因子换仓：α下降>20% → 建议换仓')
        lines.append('')
        
        return '\n'.join(lines)
    
    def _generate_risk_monitoring_section(self):
        """
        生成风险监控部分
        
        Returns:
            风险监控文本
        """
        lines = ['']
        
        if not MONITORS_AVAILABLE:
            lines.append('⚠️ 风险监控模块不可用')
            lines.append('')
            return '\n'.join(lines)
        
        # 生成风险报告
        try:
            risk_report = self.portfolio_monitor.generate_risk_report()
            if risk_report:
                lines.append(self.portfolio_monitor.format_report(risk_report))
            else:
                lines.append('⚠️ 风险报告为空，请检查持仓数据')
        except Exception as e:
            logger.error(f"风险监控执行异常: {e}", exc_info=True)
            lines.append('⚠️ 风险监控执行异常，请检查日志')
        
        return '\n'.join(lines)
    
    def _generate_market_monitoring_section(self):
        """
        生成市场状态监控部分
        
        Returns:
            市场状态监控文本
        """
        lines = ['']
        
        if not MONITORS_AVAILABLE:
            lines.append('⚠️ 市场监控模块不可用')
            lines.append('')
            return '\n'.join(lines)
        
        # 生成市场报告
        try:
            market_report = self.market_monitor.generate_market_report()
            lines.append(self.market_monitor.format_report(market_report))
        except Exception as e:
            logger.error(f"生成市场报告失败: {e}")
            lines.append('⚠️ 市场报告生成失败')
        
        return '\n'.join(lines)
    
    def generate_push_content(self, push_type='morning'):
        """生成推送内容（增强版）
        
        Args:
            push_type: 'morning'（盘前）或 'evening'（日报）
        """
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        
        content = []
        
        # 标题
        if push_type == 'morning':
            content.append('🦞 A股量化日报 - 盘前推送')
        else:
            content.append('🦞 A股量化日报 - 日报推送')
        
        content.append('━━━━━━━━━━━━━━━━━━━━━━━━')
        content.append(f'📅 推送时间: {now.strftime("%Y-%m-%d %H:%M")}')
        content.append(f'📌 类型: 实盘推送（含持仓跟踪、风险监控）')
        
        # 加载数据
        stock_data, data_date = self._load_stock_data()
        if stock_data is not None:
            if hasattr(data_date, 'strftime'):
                data_date_str = data_date.strftime('%Y-%m-%d')
            else:
                data_date_str = str(data_date)
            content.append(f'📊 数据日期: {data_date_str}')
            content.append(f'📊 覆盖股票: {len(stock_data)}只A股')
        
        content.append('')
        
        # 1. 昨日回顾
        content.append('🕐 昨日回顾')
        content.append('────────────────────')
        yesterday_push = self._get_yesterday_push()
        if yesterday_push:
            content.append(f'昨日推送: {yesterday_push.get("date", "N/A")}')
            content.append(f'持仓数量: {yesterday_push.get("positions_count", 0)}只')
            
            # 显示昨日决策
            yesterday_decision = yesterday_push.get('today_decision', [])
            if yesterday_decision:
                content.append('')
                content.append('昨日决策执行情况:')
                for decision in yesterday_decision[:5]:
                    emoji = "🟢" if decision.get('type') == '建仓' else ("🔴" if decision.get('type') in ['清仓', '止损'] else "🟡")
                    content.append(f'{emoji} {decision.get("name", "N/A")} - {decision.get("reason", "")}')
            else:
                content.append('昨日: 无调整建议，保持原持仓')
        else:
            content.append('状态: 无历史推送记录（首次推送）')
            content.append('建议: 按照今日选股结果建仓')
        
        content.append('')
        
        # 2. 今日持仓
        content.append('📊 今日持仓')
        content.append('────────────────────')
        portfolio = self._load_portfolio_state()
        if portfolio and 'positions' in portfolio:
            positions = portfolio['positions']
            if positions:
                content.append(f'持仓总数: {len(positions)}只')
                
                # 计算总资产
                total_value = sum(p.get('current_value', 0) for p in positions)
                total_cost = sum(p.get('cost_basis', 0) for p in positions)
                total_profit = total_value - total_cost
                profit_pct = (total_profit / total_cost * 100) if total_cost > 0 else 0
                
                content.append(f'总资产: {total_value:,.0f}元')
                content.append(f'总盈亏: {total_profit:,.0f}元 ({profit_pct:.2f}%)')
                content.append('')
                content.append('持仓明细:')
                for pos in positions[:5]:
                    emoji = "📈" if pos.get('profit_loss', 0) > 0 else ("📉" if pos.get('profit_loss', 0) < 0 else "➡️")
                    content.append(f'{emoji} {pos.get("name", "N/A")}({pos.get("code", "N/A")})')
                    content.append(f'   盈亏: {pos.get("profit_loss", 0):,.0f}元 ({pos.get("profit_loss_pct", 0):.2f}%)')
            else:
                content.append('当前无持仓')
                content.append('→ 建议按照今日选股结果建仓')
        else:
            content.append('当前无持仓')
            content.append('→ 建议按照今日选股结果建仓')
        
        content.append('')
        
        # 3. 🎯 可执行交易清单（新增）
        content.append('🎯 可执行交易清单')
        content.append('────────────────────')
        
        selection = self._load_selection_result()
        trade_list = self._generate_executable_trade_list(selection, stock_data, total_capital=1000000)
        
        if trade_list:
            content.append(trade_list)
        else:
            content.append('⚠️ 暂无可执行交易清单')
        
        content.append('')
        
        # 4. 今日决策
        content.append('🔄 今日决策')
        content.append('────────────────────')
        
        rebalance = self._load_rebalance_plan()
        if rebalance and 'actions' in rebalance:
            actions = rebalance['actions']
            if actions:
                for action in actions[:5]:
                    action_type = action.get('action', '')
                    emoji = "🛑" if action_type == 'sell' else ("⬇️" if action_type == 'reduce' else ("⬆️" if action_type == 'add' else ("📈" if action_type == 'buy' else "👀")))
                    
                    action_name = {
                        'sell': '清仓',
                        'reduce': '减仓',
                        'add': '加仓',
                        'buy': '建仓',
                        'review': '复查'
                    }.get(action_type, action_type)
                    
                    content.append(f'{emoji} {action.get("name", "N/A")}({action.get("code", "N/A")}) - {action_name}')
                    content.append(f'   原因: {action.get("reason", "")}')
            else:
                content.append('✓ 无需调整，当前持仓符合策略')
        else:
            content.append('✓ 无需调整，建议按选股结果建仓')
            content.append('')
            content.append('操作建议:')
            content.append('• 分批建仓，避免一次性买入')
            content.append('• 每只股票占总仓位10-12%')
            content.append('• 保留20%现金应对加仓机会')
        
        content.append('')
        
        # 5. ⚙️ 具体执行规则（新增）
        content.append('⚙️ 具体执行规则')
        content.append('────────────────────')
        content.append(self._generate_execution_rules())
        
        # 6. 📊 风险监控（新增）
        content.append(self._generate_risk_monitoring_section())
        
        # 7. 🌐 市场状态监控（新增）
        content.append(self._generate_market_monitoring_section())
        
        # 8. 换仓逻辑
        content.append('⚙️ 换仓逻辑')
        content.append('────────────────────')
        content.append('止盈触发: 收益>20% → 分批止盈')
        content.append('止损触发: 亏损>10% → 立即止损')
        content.append('时间触发: 持仓>60天 → 评估换仓')
        content.append('因子触发: α得分下降>20% → 建议换仓')
        
        content.append('')
        
        # 9. 明日计划
        content.append('📅 明日计划')
        content.append('────────────────────')
        content.append('• 监控今日建仓执行情况')
        content.append('• 如收盘前30分钟有调仓，及时跟进')
        content.append('• 如未触发调仓，保持当前持仓')
        content.append('• 继续监控止盈止损信号')
        
        content.append('')
        
        # 10. 仓位管理
        content.append('💰 仓位管理建议')
        content.append('────────────────────')
        content.append('• 总仓位: 80%（核心60%+卫星20%）')
        content.append('• 现金: 20%（应对加仓）')
        content.append('• 单股最大仓位: 12%')
        content.append('• 组合最大回撤: -15%风险线')
        
        content.append('')
        
        # 11. 风控规则
        content.append('⚠️ 风控规则')
        content.append('────────────────────')
        content.append('• 单股止损线: -10%')
        content.append('• 单股止盈线: +20%')
        content.append('• 组合最大回撤: -15%')
        content.append('• 根据自身风险承受能力调整')
        
        content.append('')
        
        content.append('━━━━━━━━━━━━━━━━━━━━━━━━')
        content.append('📊 数据来源: AKShare A股实时数据')
        content.append(f'🦞 A股量化系统 v2.1 | {today}')
        
        push_content = '\n'.join(content)
        
        # 保存推送历史
        self._save_push_history({
            'date': today,
            'time': now.strftime('%H:%M:%S'),
            'type': push_type,
            'positions_count': len(portfolio['positions']) if portfolio and 'positions' in portfolio else 0,
            'content': push_content[:500]  # 只保存前500字符
        })
        
        return push_content
    
    def send_feishu_push(self, report):
        """发送飞书推送"""
        webhook_url = self.config.get('webhook_url')
        if not webhook_url:
            logger.warning("未配置webhook_url")
            return False
        
        payload = {
            "msg_type": "text",
            "content": {
                "text": report
            }
        }
        
        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
            if response.status_code == 200:
                logger.info("✓ 飞书推送成功")
                return True
            else:
                logger.error(f"飞书推送失败: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"发送推送失败: {e}", exc_info=True)
            return False
    
    def run(self, push_type='morning'):
        """执行推送"""
        logger.info("="*60)
        logger.info(f"A股量化日报推送开始 - {push_type}")
        logger.info("="*60)
        
        # 检查交易日
        now = datetime.now()
        if now.weekday() >= 5:
            logger.info(f"⚠️ 今天是周末，跳过推送")
            return 0
        
        logger.info(f"✓ 交易日")
        
        # 生成报告
        logger.info("生成推送报告...")
        report = self.generate_push_content(push_type)
        
        if report:
            logger.info("✓ 报告生成成功")
            
            # 保存报告
            os.makedirs('reports', exist_ok=True)
            report_file = f"reports/{push_type}_push_{now.strftime('%Y%m%d_%H%M')}.md"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"✓ 报告已保存: {report_file}")
            
            # 发送推送
            if self.send_feishu_push(report):
                logger.info("="*60)
                logger.info("✅ A股量化日报推送完成")
                logger.info("="*60)
                return 0
            else:
                logger.error("✗ 推送失败")
                return 1
        else:
            logger.error("✗ 报告生成失败")
            return 1

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='A股量化日报统一推送系统（增强版）')
    parser.add_argument('--type', choices=['morning', 'evening'], default='morning',
                       help='推送类型: morning(盘前), evening(日报)')
    
    args = parser.parse_args()
    
    pusher = UnifiedDailyPusher()
    return pusher.run(args.type)

if __name__ == "__main__":
    sys.exit(main())
