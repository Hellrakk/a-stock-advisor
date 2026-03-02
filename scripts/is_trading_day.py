#!/usr/bin/env python3
"""
交易日判断模块
功能：判断今天是否是交易日，考虑周末和节假日
"""

import sys
import os
from datetime import datetime, date
import requests
import json


class TradingDayChecker:
    """交易日判断器"""

    def __init__(self):
        """初始化"""
        self.holidays_file = 'data/holidays.json'
        self.holidays = self._load_holidays()
        # 2026年中国节假日（示例）
        self.default_holidays_2026 = [
            '2026-01-01',  # 元旦
            '2026-01-28', '2026-01-29', '2026-01-30', '2026-01-31', # 春节
            '2026-02-01', '2026-02-02', '2026-02-03', '2026-02-04', # 春节
            '2026-04-04', '2026-04-05', '2026-04-06', # 清明节
            '2026-05-01', '2026-05-02', '2026-05-03', # 劳动节
            '2026-06-06', # 端午节
            '2026-09-18', '2026-09-19', '2026-09-20', # 中秋节
            '2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', # 国庆节
            '2026-10-05', '2026-10-06', '2026-10-07', # 国庆节
        ]

    def _load_holidays(self):
        """加载节假日数据"""
        if os.path.exists(self.holidays_file):
            try:
                with open(self.holidays_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 加载节假日文件失败: {e}，使用默认节假日")

        return []

    def _get_holidays_from_api(self, year: int = None) -> list:
        """
        从API获取节假日数据（备用方案）
        目前没有免费的稳定API，这里只提供接口，实际使用建议维护本地节假日列表
        """
        return []

    def is_weekend(self, check_date: date = None) -> bool:
        """
        判断是否是周末

        Args:
            check_date: 检查日期，默认为今天

        Returns:
            True表示是周末
        """
        if check_date is None:
            check_date = date.today()

        # Monday=0, Sunday=6
        return check_date.weekday() >= 5

    def is_holiday(self, check_date: date = None) -> bool:
        """
        判断是否是法定节假日

        Args:
            check_date: 检查日期，默认为今天

        Returns:
            True表示是节假日
        """
        if check_date is None:
            check_date = date.today()

        date_str = check_date.strftime('%Y-%m-%d')

        # 检查本地节假日列表
        if date_str in self.holidays:
            return True

        # 检查默认节假日列表（2026年）
        if date_str in self.default_holidays_2026:
            return True

        return False

    def is_trading_day(self, check_date: date = None) -> bool:
        """
        判断是否是交易日

        Args:
            check_date: 检查日期，默认为今天

        Returns:
            True表示是交易日
        """
        if check_date is None:
            check_date = date.today()

        print(f"📅 检查日期: {check_date} ({check_date.strftime('%A')})")

        # 周末不是交易日
        if self.is_weekend(check_date):
            print(f"❌ 是周末，非交易日")
            return False

        # 法定节假日不是交易日
        if self.is_holiday(check_date):
            print(f"❌ 是法定节假日，非交易日")
            return False

        print(f"✅ 是交易日")
        return True


def main():
    """主函数"""
    checker = TradingDayChecker()

    is_trading = checker.is_trading_day()

    print(f"\n{'='*50}")
    print(f"判断结果: {'交易日' if is_trading else '非交易日'}")
    print(f"{'='*50}\n")

    return 0 if is_trading else 1


if __name__ == '__main__':
    sys.exit(main())
