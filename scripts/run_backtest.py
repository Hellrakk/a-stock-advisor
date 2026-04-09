#!/usr/bin/env python3
"""
Weekly backtest entrypoint for local scheduled runs.
"""

import json
import logging
import os
import pickle
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"
REPORTS_DIR = PROJECT_ROOT / "reports"
DATA_FILE = PROJECT_ROOT / "data" / "akshare_real_data_fixed.pkl"

sys.path.insert(0, str(PROJECT_ROOT))

LOGS_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "backtest.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main():
    logger.info("策略回测任务开始")

    if not DATA_FILE.exists():
        logger.error("数据文件不存在: %s", DATA_FILE)
        return 1

    from code.backtest.backtest_engine_v2 import BacktestEngineV2

    with open(DATA_FILE, "rb") as f:
        stock_data = pickle.load(f)

    if "date" not in stock_data.columns and "日期" in stock_data.columns:
        stock_data["date"] = stock_data["日期"]

    if "date" in stock_data.columns:
        stock_data["date"] = pd.to_datetime(stock_data["date"], errors="coerce")

    if "month" not in stock_data.columns:
        stock_data["month"] = pd.NA

    if "date" in stock_data.columns:
        stock_data["month"] = stock_data["month"].where(stock_data["month"].notna(), stock_data["date"].dt.strftime("%Y-%m"))

    stock_data = stock_data[stock_data["month"].notna()].copy()

    engine = BacktestEngineV2()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    def simple_signal_func(date, data):
        current_data = data[data["date"] == date] if "date" in data.columns else data
        if len(current_data) == 0:
            return []
        if "momentum_20" in current_data.columns:
            selected = current_data.nlargest(10, "momentum_20")
            return selected["stock_code"].tolist() if "stock_code" in current_data.columns else []
        return current_data.sample(min(10, len(current_data)))["stock_code"].tolist() if "stock_code" in current_data.columns else []

    results = engine.run_backtest(stock_data, simple_signal_func, rebalance_freq="monthly")
    report_path = REPORTS_DIR / f"backtest_report_{datetime.now().strftime('%Y%m%d')}.json"

    def convert_dataframes(obj):
        try:
            import pandas as pd
        except Exception:
            pd = None
        if isinstance(obj, dict):
            return {k: convert_dataframes(v) for k, v in obj.items()}
        if pd is not None and isinstance(obj, pd.DataFrame):
            return obj.to_dict("records")
        if isinstance(obj, list):
            return [convert_dataframes(item) for item in obj]
        return obj

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp": datetime.now().isoformat(),
                "backtest_period": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
                "results": convert_dataframes(results or {}),
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    logger.info("回测报告已保存: %s", report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
