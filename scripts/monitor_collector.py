#!/usr/bin/env python3
"""
Windows-friendly monitoring collector for local scheduled runs.
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import psutil

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "monitor_collector.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class MonitorCollector:
    def __init__(self):
        self.base_dir = BASE_DIR
        self.data = {}

    def collect_system_metrics(self):
        self.data["cpu"] = {
            "percent": psutil.cpu_percent(interval=1),
            "count": psutil.cpu_count(),
            "count_logical": psutil.cpu_count(logical=True),
        }
        memory = psutil.virtual_memory()
        self.data["memory"] = {
            "total_gb": round(memory.total / (1024 ** 3), 2),
            "available_gb": round(memory.available / (1024 ** 3), 2),
            "percent": memory.percent,
            "used_gb": round(memory.used / (1024 ** 3), 2),
        }
        disk = psutil.disk_usage(str(self.base_dir))
        self.data["disk"] = {
            "total_gb": round(disk.total / (1024 ** 3), 2),
            "used_gb": round(disk.used / (1024 ** 3), 2),
            "free_gb": round(disk.free / (1024 ** 3), 2),
            "percent": disk.percent,
        }

    def collect_task_status(self):
        reports = sorted((self.base_dir / "reports").glob("morning_push_*.md"))
        self.data["latest_report"] = reports[-1].name if reports else None
        error_logs = {}
        for log_file in LOG_DIR.glob("*.log"):
            with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                error_count = sum(1 for line in f if "ERROR" in line or "Traceback" in line)
            if error_count:
                error_logs[log_file.name] = error_count
        self.data["error_logs"] = error_logs

    def save_metrics(self):
        self.data["timestamp"] = datetime.now().isoformat()
        jsonl_path = LOG_DIR / "monitoring_metrics.jsonl"
        with open(jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(self.data, ensure_ascii=False) + "\n")
        snapshot_path = LOG_DIR / "monitoring_latest.json"
        with open(snapshot_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def run_collection(self):
        self.collect_system_metrics()
        self.collect_task_status()
        self.save_metrics()
        logger.info("监控数据采集完成")
        return 0


def main():
    collector = MonitorCollector()
    return collector.run_collection()


if __name__ == "__main__":
    raise SystemExit(main())
