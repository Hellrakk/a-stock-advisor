#!/usr/bin/env python3
"""
Windows-friendly health check for the local a-stock-advisor deployment.
"""

import json
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)


class HealthChecker:
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent.parent
        self.logs_dir = self.base_dir / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        self.results = {}

    def check_data_integrity(self):
        logger.info("检查数据完整性...")
        data_dir = self.base_dir / "data"
        checks = {
            "akshare_real_data_exists": (data_dir / "akshare_real_data_fixed.pkl").exists(),
            "real_stock_data_exists": (data_dir / "real_stock_data.pkl").exists(),
            "latest_realtime_data_exists": (data_dir / "latest_realtime_data.pkl").exists(),
            "metadata_exists": any(
                (data_dir / name).exists()
                for name in (
                    "real_stock_data_metadata.json",
                    "akshare_real_data_fixed_metadata.json",
                    "latest_realtime_data_metadata.json",
                )
            ),
        }
        required_ok = (checks["akshare_real_data_exists"] or checks["real_stock_data_exists"] or checks["latest_realtime_data_exists"]) and checks["metadata_exists"]
        self.results["data_integrity"] = {"status": "pass" if required_ok else "warn", "details": checks}
        return required_ok

    def check_config_files(self):
        logger.info("检查配置文件...")
        config_dir = self.base_dir / "config"
        checks = {}
        for name in ("feishu_config.json", "risk_limits.json", "feature_flags.json", "windows_task_config.json"):
            path = config_dir / name
            try:
                if path.exists():
                    with open(path, "r", encoding="utf-8") as f:
                        json.load(f)
                    checks[name] = True
                else:
                    checks[name] = False
            except Exception:
                checks[name] = False
        overall = checks.get("feishu_config.json", False) and checks.get("risk_limits.json", False) and checks.get("feature_flags.json", False)
        self.results["config_files"] = {"status": "pass" if overall else "warn", "details": checks}
        return overall

    def check_disk_space(self, threshold_gb=5):
        logger.info("检查磁盘空间...")
        try:
            usage = shutil.disk_usage(self.base_dir)
            total_gb = usage.total / (1024 ** 3)
            free_gb = usage.free / (1024 ** 3)
            used_percent = round((usage.used / usage.total) * 100, 1) if usage.total else 0
            details = {
                "used_percent": used_percent,
                "available_gb": round(free_gb, 2),
                "total_gb": round(total_gb, 2),
            }
            overall = free_gb >= threshold_gb
            self.results["disk_space"] = {"status": "pass" if overall else "warn", "details": details}
            return overall
        except Exception as e:
            self.results["disk_space"] = {"status": "error", "details": str(e)}
            return False

    def check_python_env(self):
        logger.info("检查 Python 环境...")
        checks = {}
        for module_name in ("pandas", "numpy", "akshare", "psutil"):
            try:
                __import__(module_name)
                checks[module_name] = True
            except ImportError:
                checks[module_name] = False
        overall = all(checks.values())
        self.results["python_env"] = {"status": "pass" if overall else "warn", "details": checks}
        return overall

    def check_logs_rotation(self, max_size_mb=100):
        logger.info("检查日志文件大小...")
        checks = {}
        for log_file in self.logs_dir.glob("*.log"):
            size_mb = log_file.stat().st_size / (1024 * 1024)
            checks[log_file.name] = size_mb < max_size_mb
        overall = all(checks.values()) if checks else True
        self.results["logs_rotation"] = {"status": "pass" if overall else "warn", "details": checks}
        return overall

    def run_all_checks(self):
        self.check_data_integrity()
        self.check_config_files()
        self.check_disk_space()
        self.check_python_env()
        self.check_logs_rotation()

        summary = {
            "timestamp": datetime.now().isoformat(),
            "checks": self.results,
            "overall_status": "pass" if all(item["status"] in {"pass", "warn"} for item in self.results.values()) else "fail",
        }

        report_path = self.logs_dir / "health_check_report.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        logger.info("健康检查报告已保存: %s", report_path)
        return 0 if summary["overall_status"] == "pass" else 1


def main():
    checker = HealthChecker()
    return checker.run_all_checks()


if __name__ == "__main__":
    raise SystemExit(main())
