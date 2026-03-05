#!/usr/bin/env python3
"""
增强监控系统
功能：实时监控系统运行状态、市场状态、因子表现和投资组合风险
- 系统健康监控
- 市场状态监控
- 因子表现监控
- 投资组合风险监控
- 日志分析和预警
"""

import logging
import os
import json
from datetime import datetime, timedelta
import time
import threading
import psutil

class EnhancedMonitor:
    """增强监控系统类"""
    
    def __init__(self, config=None):
        """初始化"""
        self.logger = logging.getLogger(__name__)
        self.config = config or {
            'monitor_interval': 60,  # 监控间隔（秒）
            'alert_thresholds': {
                'cpu_usage': 80,  # CPU使用率阈值
                'memory_usage': 80,  # 内存使用率阈值
                'disk_usage': 80,  # 磁盘使用率阈值
                'response_time': 5,  # 响应时间阈值（秒）
            },
            'log_config': {
                'log_dir': 'logs',
                'log_file': 'system_monitor.log',
                'max_log_size': 10485760,  # 10MB
                'backup_count': 5,
            }
        }
        
        # 初始化监控状态
        self.status = {
            'system': {
                'cpu_usage': 0,
                'memory_usage': 0,
                'disk_usage': 0,
                'network_sent': 0,
                'network_recv': 0,
                'process_count': 0,
            },
            'market': {
                'status': 'unknown',
                'last_update': None,
                'major_indices': {},
            },
            'factors': {
                'last_update': None,
                'performance': {},
                'alerts': [],
            },
            'portfolio': {
                'last_update': None,
                'risk_metrics': {},
                'positions': [],
            },
            'system_health': {
                'status': 'healthy',
                'last_check': None,
                'issues': [],
            }
        }
        
        # 创建日志目录
        os.makedirs(self.config['log_config']['log_dir'], exist_ok=True)
        
        # 启动监控线程
        self.running = False
        self.monitor_thread = None
    
    def start_monitoring(self):
        """
        开始监控
        """
        if not self.running:
            self.running = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
            self.logger.info("监控系统已启动")
    
    def stop_monitoring(self):
        """
        停止监控
        """
        if self.running:
            self.running = False
            if self.monitor_thread:
                self.monitor_thread.join()
            self.logger.info("监控系统已停止")
    
    def _monitor_loop(self):
        """
        监控循环
        """
        while self.running:
            try:
                # 监控系统状态
                self._monitor_system()
                
                # 监控市场状态
                self._monitor_market()
                
                # 监控因子表现
                self._monitor_factors()
                
                # 监控投资组合
                self._monitor_portfolio()
                
                # 检查系统健康状态
                self._check_system_health()
                
                # 记录监控状态
                self._log_status()
                
                time.sleep(self.config['monitor_interval'])
            except Exception as e:
                self.logger.error(f"监控循环出错: {e}")
                time.sleep(self.config['monitor_interval'])
    
    def _monitor_system(self):
        """
        监控系统状态
        """
        try:
            # CPU使用率
            cpu_usage = psutil.cpu_percent(interval=1)
            
            # 内存使用率
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            
            # 磁盘使用率
            disk = psutil.disk_usage('/')
            disk_usage = disk.percent
            
            # 网络流量
            net_io = psutil.net_io_counters()
            network_sent = net_io.bytes_sent
            network_recv = net_io.bytes_recv
            
            # 进程数量
            process_count = len(psutil.pids())
            
            self.status['system'] = {
                'cpu_usage': cpu_usage,
                'memory_usage': memory_usage,
                'disk_usage': disk_usage,
                'network_sent': network_sent,
                'network_recv': network_recv,
                'process_count': process_count,
                'timestamp': datetime.now().isoformat()
            }
            
            # 检查系统资源阈值
            if cpu_usage > self.config['alert_thresholds']['cpu_usage']:
                self._add_alert('system', 'high_cpu_usage', f'CPU使用率过高: {cpu_usage:.1f}%')
            
            if memory_usage > self.config['alert_thresholds']['memory_usage']:
                self._add_alert('system', 'high_memory_usage', f'内存使用率过高: {memory_usage:.1f}%')
            
            if disk_usage > self.config['alert_thresholds']['disk_usage']:
                self._add_alert('system', 'high_disk_usage', f'磁盘使用率过高: {disk_usage:.1f}%')
        except Exception as e:
            self.logger.error(f"监控系统状态失败: {e}")
    
    def _monitor_market(self):
        """
        监控市场状态
        """
        try:
            # 这里可以集成市场数据获取逻辑
            # 暂时使用模拟数据
            self.status['market'] = {
                'status': 'open',
                'last_update': datetime.now().isoformat(),
                'major_indices': {
                    '上证指数': {'price': 4182.59, 'change': 0.47},
                    '深证成指': {'price': 14465.79, 'change': -0.20},
                    '创业板指': {'price': 3294.16, 'change': -0.49},
                    '沪深300': {'price': 3950.23, 'change': 0.15}
                }
            }
        except Exception as e:
            self.logger.error(f"监控市场状态失败: {e}")
    
    def _monitor_factors(self):
        """
        监控因子表现
        """
        try:
            # 这里可以集成因子监控逻辑
            # 暂时使用模拟数据
            self.status['factors'] = {
                'last_update': datetime.now().isoformat(),
                'performance': {
                    'value_factor': {'ic': 0.08, 'ir': 0.45},
                    'momentum_factor': {'ic': 0.06, 'ir': 0.38},
                    'quality_factor': {'ic': 0.07, 'ir': 0.42},
                    'liquidity_factor': {'ic': 0.05, 'ir': 0.35}
                },
                'alerts': []
            }
        except Exception as e:
            self.logger.error(f"监控因子表现失败: {e}")
    
    def _monitor_portfolio(self):
        """
        监控投资组合
        """
        try:
            # 这里可以集成投资组合监控逻辑
            # 暂时使用模拟数据
            self.status['portfolio'] = {
                'last_update': datetime.now().isoformat(),
                'risk_metrics': {
                    'portfolio_volatility': 0.15,
                    'var_95': 0.035,
                    'max_drawdown': 0.12,
                    'sharpe_ratio': 1.8
                },
                'positions': [
                    {'code': '601899', 'name': '紫金矿业', 'weight': 0.12, 'profit': 0.15},
                    {'code': '600938', 'name': '中国海油', 'weight': 0.10, 'profit': 0.12},
                    {'code': '601857', 'name': '中国石油', 'weight': 0.09, 'profit': 0.08},
                    {'code': '600988', 'name': '赤峰黄金', 'weight': 0.08, 'profit': 0.20},
                    {'code': '600547', 'name': '山东黄金', 'weight': 0.08, 'profit': 0.18}
                ]
            }
        except Exception as e:
            self.logger.error(f"监控投资组合失败: {e}")
    
    def _check_system_health(self):
        """
        检查系统健康状态
        """
        try:
            issues = []
            status = 'healthy'
            
            # 检查系统资源
            if self.status['system']['cpu_usage'] > self.config['alert_thresholds']['cpu_usage']:
                issues.append('CPU使用率过高')
                status = 'warning'
            
            if self.status['system']['memory_usage'] > self.config['alert_thresholds']['memory_usage']:
                issues.append('内存使用率过高')
                status = 'warning'
            
            if self.status['system']['disk_usage'] > self.config['alert_thresholds']['disk_usage']:
                issues.append('磁盘使用率过高')
                status = 'warning'
            
            # 检查市场数据更新
            if self.status['market']['last_update']:
                last_market_update = datetime.fromisoformat(self.status['market']['last_update'])
                if (datetime.now() - last_market_update).total_seconds() > 3600:
                    issues.append('市场数据更新不及时')
                    status = 'warning'
            
            # 检查因子数据更新
            if self.status['factors']['last_update']:
                last_factor_update = datetime.fromisoformat(self.status['factors']['last_update'])
                if (datetime.now() - last_factor_update).total_seconds() > 3600:
                    issues.append('因子数据更新不及时')
                    status = 'warning'
            
            # 检查投资组合数据更新
            if self.status['portfolio']['last_update']:
                last_portfolio_update = datetime.fromisoformat(self.status['portfolio']['last_update'])
                if (datetime.now() - last_portfolio_update).total_seconds() > 3600:
                    issues.append('投资组合数据更新不及时')
                    status = 'warning'
            
            self.status['system_health'] = {
                'status': status,
                'last_check': datetime.now().isoformat(),
                'issues': issues
            }
        except Exception as e:
            self.logger.error(f"检查系统健康状态失败: {e}")
    
    def _add_alert(self, category, alert_type, message):
        """
        添加警报
        
        Args:
            category: 警报类别
            alert_type: 警报类型
            message: 警报消息
        """
        alert = {
            'timestamp': datetime.now().isoformat(),
            'category': category,
            'type': alert_type,
            'message': message
        }
        
        # 记录到日志
        self.logger.warning(f"{category.upper()} ALERT: {message}")
        
        # 添加到状态中
        if category in self.status:
            if 'alerts' in self.status[category]:
                self.status[category]['alerts'].append(alert)
                # 只保留最近10条警报
                if len(self.status[category]['alerts']) > 10:
                    self.status[category]['alerts'] = self.status[category]['alerts'][-10:]
    
    def _log_status(self):
        """
        记录监控状态
        """
        try:
            log_file = os.path.join(
                self.config['log_config']['log_dir'],
                self.config['log_config']['log_file']
            )
            
            # 检查日志文件大小
            if os.path.exists(log_file):
                if os.path.getsize(log_file) > self.config['log_config']['max_log_size']:
                    # 轮转日志
                    self._rotate_log(log_file)
            
            # 写入状态
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps({
                    'timestamp': datetime.now().isoformat(),
                    'status': self.status
                }, ensure_ascii=False) + '\n')
        except Exception as e:
            self.logger.error(f"记录监控状态失败: {e}")
    
    def _rotate_log(self, log_file):
        """
        轮转日志文件
        
        Args:
            log_file: 日志文件路径
        """
        try:
            for i in range(self.config['log_config']['backup_count'] - 1, 0, -1):
                old_log = f"{log_file}.{i}"
                new_log = f"{log_file}.{i+1}"
                if os.path.exists(old_log):
                    if os.path.exists(new_log):
                        os.remove(new_log)
                    os.rename(old_log, new_log)
            
            if os.path.exists(log_file):
                os.rename(log_file, f"{log_file}.1")
        except Exception as e:
            self.logger.error(f"轮转日志失败: {e}")
    
    def get_status(self):
        """
        获取当前状态
        
        Returns:
            dict: 当前状态
        """
        return self.status
    
    def get_health_summary(self):
        """
        获取健康状态摘要
        
        Returns:
            str: 健康状态摘要
        """
        try:
            summary = []
            summary.append('📊 系统健康状态')
            summary.append('────────────────────')
            
            # 系统状态
            system = self.status['system']
            summary.append(f'🖥️ 系统资源:')
            summary.append(f'├─ CPU使用率: {system.get("cpu_usage", 0):.1f}%')
            summary.append(f'├─ 内存使用率: {system.get("memory_usage", 0):.1f}%')
            summary.append(f'├─ 磁盘使用率: {system.get("disk_usage", 0):.1f}%')
            summary.append(f'└─ 进程数量: {system.get("process_count", 0)}')
            
            # 市场状态
            market = self.status['market']
            summary.append('')
            summary.append('📈 市场状态:')
            summary.append(f'├─ 状态: {market.get("status", "unknown")}')
            summary.append(f'└─ 最后更新: {market.get("last_update", "N/A")}')
            
            # 因子表现
            factors = self.status['factors']
            summary.append('')
            summary.append('📊 因子表现:')
            summary.append(f'└─ 最后更新: {factors.get("last_update", "N/A")}')
            
            # 投资组合状态
            portfolio = self.status['portfolio']
            summary.append('')
            summary.append('💼 投资组合:')
            summary.append(f'├─ 最后更新: {portfolio.get("last_update", "N/A")}')
            summary.append(f'└─ 持仓数量: {len(portfolio.get("positions", []))}')
            
            # 系统健康
            health = self.status['system_health']
            summary.append('')
            summary.append('🛡️ 整体健康:')
            status_emoji = "✅" if health.get("status") == "healthy" else "⚠️" if health.get("status") == "warning" else "❌"
            summary.append(f'├─ 状态: {status_emoji} {health.get("status", "unknown")}')
            summary.append(f'└─ 最后检查: {health.get("last_check", "N/A")}')
            
            # 问题列表
            if health.get("issues"):
                summary.append('')
                summary.append('⚠️ 存在问题:')
                for issue in health.get("issues", []):
                    summary.append(f'- {issue}')
            
            return '\n'.join(summary)
        except Exception as e:
            self.logger.error(f"获取健康摘要失败: {e}")
            return '📊 系统健康状态\n────────────────────\n获取摘要失败'
    
    def check_system_ready(self):
        """
        检查系统是否就绪
        
        Returns:
            bool: 系统是否就绪
        """
        try:
            # 检查系统资源
            if self.status['system']['cpu_usage'] > self.config['alert_thresholds']['cpu_usage']:
                return False
            
            if self.status['system']['memory_usage'] > self.config['alert_thresholds']['memory_usage']:
                return False
            
            if self.status['system']['disk_usage'] > self.config['alert_thresholds']['disk_usage']:
                return False
            
            # 检查数据更新
            if not self.status['market']['last_update']:
                return False
            
            if not self.status['factors']['last_update']:
                return False
            
            if not self.status['portfolio']['last_update']:
                return False
            
            # 检查系统健康状态
            if self.status['system_health']['status'] == 'critical':
                return False
            
            return True
        except Exception as e:
            self.logger.error(f"检查系统就绪状态失败: {e}")
            return False

if __name__ == "__main__":
    # 测试监控系统
    monitor = EnhancedMonitor()
    monitor.start_monitoring()
    
    try:
        while True:
            print("\n" + "="*60)
            print("系统监控状态")
            print("="*60)
            print(monitor.get_health_summary())
            print("\n按 Ctrl+C 退出...")
            time.sleep(10)
    except KeyboardInterrupt:
        monitor.stop_monitoring()
        print("监控系统已停止")
