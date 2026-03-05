#!/usr/bin/env python3
"""
交易员辅助模块
为没有API接入条件的交易员提供替代解决方案
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TradingReportGenerator:
    """交易报表生成器"""
    
    def __init__(self, report_dir: str = 'reports/trading'):
        self.report_dir = report_dir
        os.makedirs(self.report_dir, exist_ok=True)
    
    def generate_daily_report(self, positions: Dict, account_info: Dict, 
                             trades: List[Dict], date: Optional[str] = None) -> str:
        """生成每日交易报表
        
        Args:
            positions: 持仓信息
            account_info: 账户信息
            trades: 当日交易记录
            date: 日期 (YYYY-MM-DD)
        
        Returns:
            报表文件路径
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        # 生成报表
        report = {
            'date': date,
            'account': account_info,
            'positions': positions,
            'trades': trades,
            'summary': self._generate_summary(positions, account_info, trades)
        }
        
        # 保存JSON报表
        json_path = os.path.join(self.report_dir, f'daily_report_{date}.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        # 生成HTML报表
        html_path = os.path.join(self.report_dir, f'daily_report_{date}.html')
        self._generate_html_report(report, html_path)
        
        # 生成PDF报表
        pdf_path = os.path.join(self.report_dir, f'daily_report_{date}.pdf')
        self._generate_pdf_report(report, pdf_path)
        
        logger.info(f"每日交易报表已生成: {json_path}")
        return json_path
    
    def generate_weekly_report(self, start_date: str, end_date: str, 
                              positions: Dict, account_info: Dict, 
                              trades: List[Dict]) -> str:
        """生成每周交易报表"""
        # 生成报表
        report = {
            'period': f'{start_date} 至 {end_date}',
            'account': account_info,
            'positions': positions,
            'trades': trades,
            'summary': self._generate_summary(positions, account_info, trades),
            'weekly_analysis': self._generate_weekly_analysis(trades)
        }
        
        # 保存JSON报表
        json_path = os.path.join(self.report_dir, f'weekly_report_{start_date}_{end_date}.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        # 生成HTML报表
        html_path = os.path.join(self.report_dir, f'weekly_report_{start_date}_{end_date}.html')
        self._generate_html_report(report, html_path)
        
        logger.info(f"每周交易报表已生成: {json_path}")
        return json_path
    
    def _generate_summary(self, positions: Dict, account_info: Dict, trades: List[Dict]) -> Dict:
        """生成报表摘要"""
        total_positions = len(positions)
        total_value = account_info.get('total_value', 0)
        total_cash = account_info.get('cash', 0)
        total_pnl = account_info.get('pnl', 0)
        
        # 计算当日交易统计
        buy_trades = [t for t in trades if t['side'] == 'buy']
        sell_trades = [t for t in trades if t['side'] == 'sell']
        
        return {
            'total_positions': total_positions,
            'total_value': total_value,
            'total_cash': total_cash,
            'total_pnl': total_pnl,
            'buy_trades': len(buy_trades),
            'sell_trades': len(sell_trades),
            'total_trades': len(trades)
        }
    
    def _generate_weekly_analysis(self, trades: List[Dict]) -> Dict:
        """生成每周分析"""
        # 按日期分组交易
        trades_by_date = {}
        for trade in trades:
            date = trade.get('date', datetime.now().strftime('%Y-%m-%d'))
            if date not in trades_by_date:
                trades_by_date[date] = []
            trades_by_date[date].append(trade)
        
        # 计算每日交易量
        daily_trades = {date: len(trades) for date, trades in trades_by_date.items()}
        
        return {
            'trades_by_date': daily_trades,
            'total_weekly_trades': len(trades),
            'most_active_day': max(daily_trades, key=daily_trades.get) if daily_trades else None
        }
    
    def _generate_html_report(self, report: Dict, output_path: str):
        """生成HTML报表"""
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>交易报表 - {report.get('date', report.get('period', 'Unknown'))}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1, h2, h3 {{ color: #333; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .summary {{ background-color: #f9f9f9; padding: 15px; margin: 20px 0; }}
        .positive {{ color: green; }}
        .negative {{ color: red; }}
    </style>
</head>
<body>
    <h1>交易报表</h1>
    <h2>{report.get('date', report.get('period', 'Unknown'))}</h2>
    
    <div class="summary">
        <h3>摘要</h3>
        <p>总持仓数: {report['summary']['total_positions']}</p>
        <p>总资产: ¥{report['account'].get('total_value', 0):,.2f}</p>
        <p>现金: ¥{report['account'].get('cash', 0):,.2f}</p>
        <p>总盈亏: <span class="{'positive' if report['account'].get('pnl', 0) >= 0 else 'negative'}">
            ¥{report['account'].get('pnl', 0):,.2f}
        </span></p>
        <p>当日交易: {report['summary']['total_trades']} 笔 (买入: {report['summary']['buy_trades']}, 卖出: {report['summary']['sell_trades']})</p>
    </div>
    
    <h3>持仓情况</h3>
    <table>
        <tr>
            <th>股票代码</th>
            <th>持仓数量</th>
            <th>平均成本</th>
            <th>市值</th>
        </tr>
"""
        
        # 添加持仓数据
        for stock_code, pos in report['positions'].items():
            html_content += f"""
        <tr>
            <td>{stock_code}</td>
            <td>{pos.get('quantity', 0)}</td>
            <td>¥{pos.get('avg_price', 0):,.2f}</td>
            <td>¥{pos.get('market_value', 0):,.2f}</td>
        </tr>
"""
        
        html_content += """
    </table>
    
    <h3>交易记录</h3>
    <table>
        <tr>
            <th>时间</th>
            <th>股票代码</th>
            <th>方向</th>
            <th>价格</th>
            <th>数量</th>
            <th>金额</th>
        </tr>
"""
        
        # 添加交易数据
        for trade in report['trades']:
            amount = trade.get('price', 0) * trade.get('quantity', 0)
            html_content += f"""
        <tr>
            <td>{trade.get('time', 'N/A')}</td>
            <td>{trade.get('stock_code', 'N/A')}</td>
            <td>{'买入' if trade.get('side') == 'buy' else '卖出'}</td>
            <td>¥{trade.get('price', 0):,.2f}</td>
            <td>{trade.get('quantity', 0)}</td>
            <td>¥{amount:,.2f}</td>
        </tr>
"""
        
        html_content += """
    </table>
"""
        
        # 添加周度分析（如果有）
        if 'weekly_analysis' in report:
            html_content += f"""
    <h3>周度分析</h3>
    <div class="summary">
        <p>总交易次数: {report['weekly_analysis']['total_weekly_trades']}</p>
        <p>最活跃交易日: {report['weekly_analysis']['most_active_day']}</p>
    </div>
"""
        
        html_content += """
</body>
</html>
"""
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _generate_pdf_report(self, report: Dict, output_path: str):
        """生成PDF报表"""
        # 这里可以使用weasyprint或其他库生成PDF
        # 暂时只创建空文件作为占位符
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(f"PDF Report for {report.get('date', report.get('period', 'Unknown'))}")


class TraderFeedbackSystem:
    """交易员反馈系统"""
    
    def __init__(self, feedback_dir: str = 'data/feedback'):
        self.feedback_dir = feedback_dir
        os.makedirs(self.feedback_dir, exist_ok=True)
    
    def submit_feedback(self, trader_id: str, feedback: Dict) -> str:
        """提交交易员反馈
        
        Args:
            trader_id: 交易员ID
            feedback: 反馈内容
        
        Returns:
            反馈ID
        """
        feedback_id = f"FB_{datetime.now().strftime('%Y%m%d%H%M%S')}_{trader_id}"
        feedback_data = {
            'feedback_id': feedback_id,
            'trader_id': trader_id,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'feedback': feedback
        }
        
        # 保存反馈
        feedback_path = os.path.join(self.feedback_dir, f'{feedback_id}.json')
        with open(feedback_path, 'w', encoding='utf-8') as f:
            json.dump(feedback_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"交易员反馈已提交: {feedback_id}")
        return feedback_id
    
    def get_feedback(self, feedback_id: str) -> Optional[Dict]:
        """获取反馈"""
        feedback_path = os.path.join(self.feedback_dir, f'{feedback_id}.json')
        if os.path.exists(feedback_path):
            with open(feedback_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    
    def get_trader_feedback(self, trader_id: str) -> List[Dict]:
        """获取交易员的所有反馈"""
        feedbacks = []
        for filename in os.listdir(self.feedback_dir):
            if filename.endswith('.json') and trader_id in filename:
                filepath = os.path.join(self.feedback_dir, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    feedbacks.append(json.load(f))
        return feedbacks


class StrategySyncManager:
    """策略同步管理器"""
    
    def __init__(self, sync_dir: str = 'data/sync'):
        self.sync_dir = sync_dir
        os.makedirs(self.sync_dir, exist_ok=True)
    
    def sync_strategy(self, strategy_id: str, strategy_data: Dict) -> str:
        """同步策略数据"""
        sync_id = f"SYNC_{datetime.now().strftime('%Y%m%d%H%M%S')}_{strategy_id}"
        sync_data = {
            'sync_id': sync_id,
            'strategy_id': strategy_id,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'strategy_data': strategy_data
        }
        
        # 保存同步数据
        sync_path = os.path.join(self.sync_dir, f'{sync_id}.json')
        with open(sync_path, 'w', encoding='utf-8') as f:
            json.dump(sync_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"策略数据已同步: {sync_id}")
        return sync_id
    
    def get_strategy_sync(self, sync_id: str) -> Optional[Dict]:
        """获取策略同步数据"""
        sync_path = os.path.join(self.sync_dir, f'{sync_id}.json')
        if os.path.exists(sync_path):
            with open(sync_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None


class TraderAssistant:
    """交易员助手"""
    
    def __init__(self):
        self.report_generator = TradingReportGenerator()
        self.feedback_system = TraderFeedbackSystem()
        self.sync_manager = StrategySyncManager()
    
    def generate_report(self, positions: Dict, account_info: Dict, 
                       trades: List[Dict], report_type: str = 'daily',
                       date: Optional[str] = None, 
                       start_date: Optional[str] = None, 
                       end_date: Optional[str] = None) -> str:
        """生成交易报表"""
        if report_type == 'daily':
            return self.report_generator.generate_daily_report(
                positions, account_info, trades, date
            )
        elif report_type == 'weekly':
            if start_date and end_date:
                return self.report_generator.generate_weekly_report(
                    start_date, end_date, positions, account_info, trades
                )
            else:
                logger.error("周度报表需要提供开始和结束日期")
                return None
        else:
            logger.error(f"不支持的报表类型: {report_type}")
            return None
    
    def submit_feedback(self, trader_id: str, feedback: Dict) -> str:
        """提交交易员反馈"""
        return self.feedback_system.submit_feedback(trader_id, feedback)
    
    def sync_strategy(self, strategy_id: str, strategy_data: Dict) -> str:
        """同步策略数据"""
        return self.sync_manager.sync_strategy(strategy_id, strategy_data)
    
    def get_feedback(self, feedback_id: str) -> Optional[Dict]:
        """获取反馈"""
        return self.feedback_system.get_feedback(feedback_id)
    
    def get_strategy_sync(self, sync_id: str) -> Optional[Dict]:
        """获取策略同步数据"""
        return self.sync_manager.get_strategy_sync(sync_id)


if __name__ == '__main__':
    # 测试交易员助手
    print("=== 交易员助手测试 ===")
    
    assistant = TraderAssistant()
    
    # 测试数据
    test_positions = {
        '600519': {
            'stock_code': '600519',
            'quantity': 100,
            'avg_price': 1800.0,
            'market_value': 180000.0
        },
        '000001': {
            'stock_code': '000001',
            'quantity': 500,
            'avg_price': 10.0,
            'market_value': 5000.0
        }
    }
    
    test_account = {
        'cash': 815000.0,
        'total_value': 1000000.0,
        'positions_value': 185000.0,
        'pnl': 0.0
    }
    
    test_trades = [
        {
            'time': '09:30:00',
            'stock_code': '600519',
            'side': 'buy',
            'price': 1800.0,
            'quantity': 100
        },
        {
            'time': '10:30:00',
            'stock_code': '000001',
            'side': 'buy',
            'price': 10.0,
            'quantity': 500
        }
    ]
    
    # 生成每日报表
    print("\n生成每日报表:")
    daily_report = assistant.generate_report(
        test_positions, test_account, test_trades, 'daily'
    )
    print(f"每日报表生成成功: {daily_report}")
    
    # 生成每周报表
    print("\n生成每周报表:")
    weekly_report = assistant.generate_report(
        test_positions, test_account, test_trades, 'weekly',
        start_date='2024-01-01', end_date='2024-01-07'
    )
    print(f"每周报表生成成功: {weekly_report}")
    
    # 提交反馈
    print("\n提交交易员反馈:")
    feedback = {
        'type': 'strategy',
        'content': '策略表现良好，建议增加止损功能',
        'rating': 4
    }
    feedback_id = assistant.submit_feedback('trader_001', feedback)
    print(f"反馈提交成功: {feedback_id}")
    
    # 同步策略
    print("\n同步策略数据:")
    strategy_data = {
        'name': '动量策略',
        'parameters': {'window': 20, 'threshold': 0.05},
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    sync_id = assistant.sync_strategy('strategy_001', strategy_data)
    print(f"策略同步成功: {sync_id}")
