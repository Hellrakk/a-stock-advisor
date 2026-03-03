#!/usr/bin/env python3
"""
推送监控脚本 - Agent职责
功能：监控推送系统状态，处理异常
"""

import sys
import os

# 确保可以找到同目录的模块
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

from datetime import datetime
import logging
import json
import time
import subprocess

from is_trading_day import TradingDayChecker

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/push_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PushMonitor:
    """推送监控器"""

    def __init__(self):
        """初始化监控器"""
        # 切换到项目根目录
        os.chdir(project_root)

        self.today = datetime.now().strftime('%Y-%m-%d')
        self.trading_checker = TradingDayChecker()

        # 告警配置
        self.alert_threshold = {
            'max_retry': 3,
            'timeout_seconds': 300,
            'consecutive_failures': 3
        }

        # 监控状态
        self.consecutive_failures = 0
        self.failure_file = 'data/push_failure_count.json'
        self._load_failure_state()

    def _load_failure_state(self):
        """加载失败状态"""
        try:
            if os.path.exists(self.failure_file):
                with open(self.failure_file, 'r') as f:
                    data = json.load(f)
                    # 如果日期不同，重置计数
                    if data.get('date') == self.today:
                        self.consecutive_failures = data.get('count', 0)
                    else:
                        self.consecutive_failures = 0
        except Exception as e:
            logger.warning(f"加载失败状态失败: {e}")
            self.consecutive_failures = 0

    def _save_failure_state(self):
        """保存失败状态"""
        try:
            os.makedirs(os.path.dirname(self.failure_file), exist_ok=True)
            data = {
                'date': self.today,
                'count': self.consecutive_failures,
                'last_update': datetime.now().isoformat()
            }
            with open(self.failure_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"保存失败状态失败: {e}")

    def is_trading_day(self):
        """检查是否是交易日"""
        logger.info("="*70)
        logger.info("检查交易日状态")
        logger.info("="*70)

        is_trading = self.trading_checker.is_trading_day()

        if not is_trading:
            logger.info("✓ 非交易日，跳过推送")
            return False

        logger.info("✓ 是交易日，继续推送")
        return True

    def run_auto_push(self):
        """
        执行自动化推送系统
        """
        logger.info("\n" + "="*70)
        logger.info("调用自动化推送系统")
        logger.info("="*70)

        try:
            # 使用subprocess调用auto_push_system.py
            script_path = os.path.join(script_dir, 'auto_push_system.py')
            cmd = [sys.executable, script_path]

            logger.info(f"执行命令: {' '.join(cmd)}")

            # 执行脚本，设置超时
            start_time = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.alert_threshold['timeout_seconds']
            )
            end_time = time.time()

            execution_time = end_time - start_time
            logger.info(f"执行耗时: {execution_time:.1f}秒")

            # 检查返回码
            if result.returncode == 0:
                logger.info("✓ 推送系统执行成功")
                logger.info(f"输出:\n{result.stdout}")

                # 重置失败计数
                if self.consecutive_failures > 0:
                    logger.info("✓ 重置失败计数")
                    self.consecutive_failures = 0
                    self._save_failure_state()

                return True, result.stdout

            else:
                error_msg = f"推送系统执行失败，返回码: {result.returncode}"
                logger.error(error_msg)
                logger.error(f"错误输出:\n{result.stderr}")

                # 增加失败计数
                self.consecutive_failures += 1
                self._save_failure_state()

                return False, error_msg

        except subprocess.TimeoutExpired:
            error_msg = f"推送系统执行超时（>{self.alert_threshold['timeout_seconds']}秒）"
            logger.error(error_msg)

            # 增加失败计数
            self.consecutive_failures += 1
            self._save_failure_state()

            return False, error_msg

        except Exception as e:
            error_msg = f"推送系统执行异常: {str(e)}"
            logger.error(error_msg)

            # 增加失败计数
            self.consecutive_failures += 1
            self._save_failure_state()

            return False, error_msg

    def should_retry(self):
        """判断是否应该重试"""
        # 检查连续失败次数
        if self.consecutive_failures >= self.alert_threshold['max_retry']:
            logger.warning(f"⚠️ 连续失败{self.consecutive_failures}次，不推荐重试")
            return False

        return True

    def should_alert(self):
        """判断是否需要发送告警"""
        # 连续失败超过阈值，需要告警
        if self.consecutive_failures >= self.alert_threshold['consecutive_failures']:
            return True

        return False

    def check_push_system_health(self):
        """
        检查推送系统健康状态
        """
        logger.info("\n" + "="*70)
        logger.info("检查推送系统健康状态")
        logger.info("="*70)

        checks = {
            'data_file': False,
            'config_file': False,
            'script_exists': False,
            'log_dir': False
        }

        # 检查数据文件
        if os.path.exists('data/akshare_real_data_fixed.pkl'):
            checks['data_file'] = True
            logger.info("✓ 数据文件存在")
        else:
            logger.error("✗ 数据文件不存在")

        # 检查配置文件
        if os.path.exists('config/feishu_config.json'):
            checks['config_file'] = True
            logger.info("✓ 飞书配置存在")
        else:
            logger.warning("⚠️ 飞书配置不存在，推送可能失败")

        # 检查脚本
        script_path = os.path.join(script_dir, 'auto_push_system.py')
        if os.path.exists(script_path):
            checks['script_exists'] = True
            logger.info("✓ 自动推送脚本存在")
        else:
            logger.error("✗ 自动推送脚本不存在")

        # 检查日志目录
        if os.path.exists('logs'):
            checks['log_dir'] = True
            logger.info("✓ 日志目录存在")
        else:
            logger.warning("⚠️ 日志目录不存在，将自动创建")
            try:
                os.makedirs('logs', exist_ok=True)
                checks['log_dir'] = True
                logger.info("✓ 日志目录已创建")
            except Exception as e:
                logger.error(f"✗ 创建日志目录失败: {e}")

        # 检查失败状态
        logger.info(f"\n连续失败次数: {self.consecutive_failures}/{self.alert_threshold['consecutive_failures']}")

        return all(checks.values()), checks

    def log_success(self):
        """记录成功日志"""
        logger.info("\n" + "="*70)
        logger.info("✅ 推送监控完成 - 成功")
        logger.info("="*70)

    def log_error(self, error_message):
        """记录错误日志"""
        logger.error("\n" + "="*70)
        logger.error("❌ 推送监控完成 - 失败")
        logger.error("="*70)
        logger.error(error_message)

    def handle_failure(self, error_message):
        """
        处理推送失败（Agent职责）
        """
        logger.error("\n" + "="*70)
        logger.error("处理推送失败")
        logger.error("="*70)

        # 记录错误
        self.log_error(error_message)

        # 判断是否需要重试
        if self.should_retry():
            logger.info("⚠️ 检测到可恢复错误，可以重试")

            # 记录重试建议
            retry_msg = f"""
失败处理建议:
- 当前连续失败次数: {self.consecutive_failures}/{self.alert_threshold['max_retry']}
- 建议操作: 检查错误日志后手动重试
"""
            logger.info(retry_msg)

        else:
            logger.error("🚨 严重错误，不建议重试，需要人工介入")

            # 发送告警
            if self.should_alert():
                self.send_alert(error_message)

    def send_alert(self, error_message):
        """
        发送告警（如果有配置）
        """
        logger.warning("="*70)
        logger.warning("🚨 发送严重告警")
        logger.warning("="*70)

        alert_msg = f"""
【A股量化日报推送告警】

⚠️ 推送系统连续失败 {self.consecutive_failures} 次

📅 日期: {self.today}
⏰ 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

🔴 错误详情:
{error_message}

📋 监控信息:
- 连续失败次数: {self.consecutive_failures}
- 最大允许次数: {self.alert_threshold['consecutive_failures']}

⚡ 行动建议:
请立即检查系统状态，分析失败原因
"""

        logger.warning(alert_msg)

        # TODO: 可以集成飞书、邮件等告警方式
        # 如果配置了告警webhook，可以直接发送
        logger.warning("（告警消息已记录到日志）")

    def check_and_push(self):
        """
        检查并执行推送（主流程）
        """
        logger.info("\n" + "="*70)
        logger.info("🔍 开始推送监控流程")
        logger.info(f"📅 日期: {self.today}")
        logger.info("="*70)

        # 1. 检查交易日
        if not self.is_trading_day():
            return "非交易日，跳过推送"

        # 2. 检查系统健康状态
        health_ok, checks = self.check_push_system_health()
        if not health_ok:
            logger.error("✗ 系统健康检查失败，无法执行推送")
            return "系统健康检查失败"

        # 3. 执行推送（代码负责）
        success, message = self.run_auto_push()

        # 4. Agent判断结果
        if success:
            self.log_success()
            return f"✓ {message}"
        else:
            self.handle_failure(message)
            return f"✗ {message}"


def main():
    """主函数"""
    try:
        monitor = PushMonitor()
        result = monitor.check_and_push()
        print(f"\n{'='*70}")
        print(f"监控结果: {result}")
        print(f"{'='*70}\n")
        return 0 if "✓" in result else 1
    except Exception as e:
        logger.error(f"监控异常: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
