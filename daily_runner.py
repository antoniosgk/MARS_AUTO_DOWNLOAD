#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import subprocess
import time
from pathlib import Path

# ----------------- SCHEDULE (UTC) -----------------
RUN_AT_UTC_HH = 22
RUN_AT_UTC_MM = 30
# --------------------------------------------------

CMD = ["/bin/bash", "/home/agkiokas/jobs/run_auto_download.sh"]
LOG_PATH = Path("/home/agkiokas/jobs/daily_runner.log")

def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def next_run_time(now: dt.datetime) -> dt.datetime:
    target = now.replace(hour=RUN_AT_UTC_HH, minute=RUN_AT_UTC_MM, second=0, microsecond=0)
    if target <= now:
        target = target + dt.timedelta(days=1)
    return target

def log(msg: str) -> None:
    stamp = utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"{stamp} | {msg}\n")

def run_once() -> int:
    log(f"RUN start: {CMD}")
    try:
        p = subprocess.run(CMD, capture_output=True, text=True)
        if p.stdout:
            log(f"stdout:\n{p.stdout}")
        if p.stderr:
            log(f"stderr:\n{p.stderr}")
        log(f"RUN end: exit={p.returncode}")
        return p.returncode
    except Exception as e:
        log(f"RUN exception: {e!r}")
        return 99

def main() -> None:
    log("Scheduler started")
    while True:
        now = utc_now()
        nxt = next_run_time(now)
        sleep_s = max(0, int((nxt - now).total_seconds()))
        log(f"Sleeping {sleep_s}s until {nxt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        time.sleep(sleep_s)

        run_once()
        time.sleep(2)

if __name__ == "__main__":
    main()
