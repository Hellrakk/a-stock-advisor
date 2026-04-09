#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate daily report (optional) + push to Feishu via webhook.

Usage:
  .\.venv\Scripts\python.exe scripts\daily_push.py --mode morning
  .\.venv\Scripts\python.exe scripts\daily_push.py --mode evening

Notes:
- Reads Feishu webhook from env FEISHU_WEBHOOK_URL / FEISHU_SECRET
  or from config/feishu_config.json.
- If webhook not configured, it will still generate the report and exit non-zero.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
REPORTS_DIR = PROJECT_ROOT / "reports"


def _stdout_utf8() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def find_latest_report() -> Path | None:
    if not REPORTS_DIR.exists():
        return None
    candidates = sorted(REPORTS_DIR.glob("morning_push_*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def run_daily_master(python_exe: str, force_update: bool = False) -> None:
    cmd = [python_exe, str(SCRIPTS_DIR / "daily_master.py")]
    if force_update:
        cmd.append("--force-update")
    print("[daily_push] running:", " ".join(cmd))
    subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=True)


def load_feishu_config() -> tuple[str | None, str | None]:
    webhook = os.environ.get("FEISHU_WEBHOOK_URL")
    secret = os.environ.get("FEISHU_SECRET")
    if webhook:
        return webhook, secret

    cfg_path = PROJECT_ROOT / "config" / "feishu_config.json"
    if cfg_path.exists():
        import json
        cfg = json.loads(cfg_path.read_text(encoding="utf-8", errors="replace"))
        return cfg.get("webhook_url"), cfg.get("secret")

    return None, None


def push_report(report_path: Path, webhook: str, secret: str | None) -> bool:
    sys.path.insert(0, str(SCRIPTS_DIR))
    from feishu_pusher import FeishuPusher  # type: ignore

    content = report_path.read_text(encoding="utf-8", errors="replace")
    title = (content.splitlines()[0].replace("#", "").strip() if content else f"A股量化日报 {datetime.now():%Y-%m-%d %H:%M}")

    pusher = FeishuPusher(webhook, secret)
    try:
        ok = pusher.send_markdown(title, content)
    except Exception:
        ok = False
    if not ok:
        ok = pusher.send_text(content)
    return bool(ok)


def main() -> int:
    _stdout_utf8()
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["morning", "evening", "manual"], default="manual")
    ap.add_argument("--force-update", action="store_true")
    ap.add_argument("--skip-generate", action="store_true")
    ap.add_argument("--report")
    args = ap.parse_args()

    if not args.skip_generate:
        try:
            run_daily_master(sys.executable, force_update=args.force_update)
        except subprocess.CalledProcessError as e:
            print(f"[daily_push] daily_master failed: {e}")
            return 1

    report_path = Path(args.report) if args.report else find_latest_report()
    if not report_path or not report_path.exists():
        print("[daily_push] report not found under reports/morning_push_*.md")
        return 1

    webhook, secret = load_feishu_config()
    if not webhook:
        print("[daily_push] FEISHU webhook not configured; report generated but not pushed.")
        print("- set env FEISHU_WEBHOOK_URL (and optional FEISHU_SECRET), or")
        print("- create config/feishu_config.json (ignored by git)")
        return 2

    if push_report(report_path, webhook, secret):
        print(f"[daily_push] pushed: {report_path}")
        return 0

    print("[daily_push] push failed")
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
