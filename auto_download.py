#!/usr/bin/env python3
#%%
from __future__ import annotations

import datetime as dt
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, Tuple, List, Optional
from ecmwfapi import *
from ecmwfapi import ECMWFDataServer,ECMWFService
import math
#KEYS TO ACCESS THE DATA
os.environ["ECMWF_API_URL"] = "https://api.ecmwf.int/v1"
os.environ["ECMWF_API_KEY"] = "fe1dcb573a3baa56c7ac659aa5f35508"
os.environ["ECMWF_API_EMAIL"] = "zerefos@geol.uoa.gr"
server=ECMWFService("mars")
print("✅ Connected to ECMWF API")
DOWNLOAD_ROOT = Path("/home/agkiokas/MARS/data/")  # folder root
REGNAME = "EUROPE"
EXPVER = "icki"  #or 0001
TYPE_ = "fc"   #or reanalysis
LEVTYPE = "sfc"  # sfc / pl / ml / etc.#often ml

# MARS keys
CLASS_ = "rd"  #mc or rd
STREAM = "oper"
PARAM = "207.210/209.210" 
GRID = "0.4/0.4"
AREA = "60/-120/-20/90"  # N/W/S/E
FORMAT_ = "netcdf"   # "netcdf" or "grib"

# Optional vertical levels (ONLY if levtype supports it; set None for sfc)
LEVELIST: Optional[str] = None  # e.g. "110/to/137" for pl/ml if applicable

# Forecast selection
INIT_TIME_UTC = "00:00:00"
STEP_HOURS = 3
MAX_FC_HOURS = 120

# “Days” are 0-indexed here:
# day 0 = init date (first day), day 1 = next day, etc.
KEEP_DAY_INDICES = [0, 2, 4]  # keep day0, day2, day4 (skip day1 and day3)
#HOW MANY DAYS BACK IN TIME YOU WANT TO DOWNLOAD?0 MEANS TODAY for RUN_DATE_OFFSET_DAYS
# Choose ONE:
RUN_DATE_YYYYMMDD: Optional[str] = None   # e.g. "20260114" (if set, overrides offset)
RUN_DATE_OFFSET_DAYS = 3   #HOW MANY DAYS BACK IN TIME YOU WANT TO DOWNLOAD?0 MEANS TODAY

# Logging
LOG_NAME = "CAMS_mars_fc.log"

# Output validation (prevents keeping tiny/invalid/error files)
ENFORCE_VALIDATION = False
ABS_MIN_BYTES = 8_000          # raise if you expect bigger domains
REL_MIN_FACTOR = 0.10          # fail if file is <10% of rough expected size (when expected is large)
EXPECTED_MIN_TRIGGER = 50_000  # only apply relative check above this expected size

# ==========================================
# Helpers
# ============================================================
def setup_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("cams_mars")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def resolve_run_date_yyyymmdd() -> str:
    if RUN_DATE_YYYYMMDD is not None:
        s = RUN_DATE_YYYYMMDD.strip()
        if len(s) != 8 or not s.isdigit():
            raise ValueError(f"RUN_DATE_YYYYMMDD must be 'YYYYMMDD', got: {RUN_DATE_YYYYMMDD}")
        # validate it is a real calendar date
        dt.datetime.strptime(s, "%Y%m%d")
        return s
    return yyyymmdd_utc(RUN_DATE_OFFSET_DAYS)

def yyyymmdd_utc(offset_days: int = 0) -> str:
    d = (now_utc().date() - dt.timedelta(days=offset_days))
    return d.strftime("%Y%m%d")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def nonempty(path: Path) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size > 0


def build_base_dir() -> Path:
    return DOWNLOAD_ROOT / REGNAME / EXPVER / TYPE_.upper() / LEVTYPE.upper()


def build_run_dir(base_dir: Path, run_date: str, init_time_utc: str) -> Path:
    tdir = init_time_utc.replace(":", "_")
    return base_dir / run_date / tdir


def init_datetime_utc(run_date_yyyymmdd: str, init_time_utc: str) -> dt.datetime:
    return dt.datetime.strptime(
        run_date_yyyymmdd + init_time_utc, "%Y%m%d%H:%M:%S"
    ).replace(tzinfo=dt.timezone.utc)


def fmt_dmy(d: dt.datetime) -> str:
    return d.strftime("%d_%m_%Y")


def day_window_hours(day_index: int) -> Tuple[int, int]:
    """
    For a given day index, return (start_hour, end_hour) inclusive,
    with steps 00..21 (3-hourly) => end = start + 21.
    """
    start = day_index * 24
    end = start + 21
    if end > MAX_FC_HOURS:
        raise ValueError(f"Day window exceeds MAX_FC_HOURS: day={day_index}, end={end}")
    return start, end


def steps_as_list(start_h: int, end_h: int, step_h: int) -> Tuple[str, int]:
    vals = list(range(start_h, end_h + 1, step_h))
    return "/".join(str(v) for v in vals), len(vals)


def validity_label(init_dt: dt.datetime, start_h: int, end_h: int) -> str:
    """
    File title as 'DD_MM_YYYY-DD_MM_YYYY' based on validity dates.
    With our day windows (00..21), start/end fall on the same calendar day.
    """
    valid_start = init_dt + dt.timedelta(hours=start_h)
    valid_end = init_dt + dt.timedelta(hours=end_h)
    return f"{fmt_dmy(valid_start)}-{fmt_dmy(valid_end)}"


def build_request(run_date: str, init_time_utc: str, step_str: str, logger: logging.Logger) -> Dict[str, str]:
    req: Dict[str, str] = {
        "class": CLASS_,
        "type": TYPE_,
        "stream": STREAM,
        "expver": EXPVER,
        "levtype": LEVTYPE,
        "param": PARAM,
        "date": run_date,
        "time": init_time_utc,
        "step": step_str,
        "area": AREA,
        "grid": GRID,
        "format": FORMAT_,
    }

    # Only attach levelist if user set it AND levtype is not "sfc"
    if LEVELIST and LEVTYPE.lower() != "sfc":
        req["levelist"] = LEVELIST
    elif LEVELIST and LEVTYPE.lower() == "sfc":
        logger.warning("LEVELIST is set but LEVTYPE='sfc'. Ignoring levelist for this request.")

    return req


# ---------- Validation helpers ----------
def _magic_ok(path: Path, fmt: str) -> bool:
    b = path.read_bytes()[:16]
    fmt = fmt.lower()
    if fmt == "netcdf":
        if b.startswith(b"CDF"):
            return True
        if b.startswith(b"\x89HDF\r\n\x1a\n"):  # NetCDF4/HDF5 signature
            return True
        return False
    if fmt == "grib":
        return b.startswith(b"GRIB")
    return True


def _peek_text(path: Path, n: int = 400) -> str:
    try:
        return path.read_bytes()[:n].decode("utf-8", errors="replace")
    except Exception:
        return ""


def _count_params(param: str) -> int:
    return len([p for p in param.split("/") if p.strip()])


def _parse_grid(grid: str) -> tuple[float, float]:
    a, b = grid.split("/")
    return float(a), float(b)


def _parse_area(area: str) -> tuple[float, float, float, float]:
    n, w, s, e = area.split("/")
    return float(n), float(w), float(s), float(e)


def _estimate_raw_bytes(area: str, grid: str, ntime: int, nparam: int) -> int:
    # Rough lower bound: float32
    dlat, dlon = _parse_grid(grid)
    n, w, s, e = _parse_area(area)
    nlat = int(math.floor(abs(n - s) / dlat + 0.5)) + 1
    nlon = int(math.floor(abs(e - w) / dlon + 0.5)) + 1
    return max(0, nlat * nlon * ntime * nparam * 4)


def mars_execute(server: ECMWFService, req: Dict[str, str], target: Path, ntime: int, logger: logging.Logger) -> None:
    tmp = target.with_suffix(target.suffix + ".part")
    tmp.unlink(missing_ok=True)

    t0 = time.time()
    logger.info(f"Downloading -> {target.name}")
    server.execute(req, str(tmp))
    dt_s = time.time() - t0

    if not tmp.exists():
        raise RuntimeError("Download produced no file.")

    size = tmp.stat().st_size
    size_mb = size / (1024 * 1024)

    if ENFORCE_VALIDATION:
        if not _magic_ok(tmp, FORMAT_):
            snippet = _peek_text(tmp)
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"Output is not valid {FORMAT_}. First bytes look like:\n{snippet}")

        nparam = _count_params(PARAM)
        expected = _estimate_raw_bytes(AREA, GRID, ntime=ntime, nparam=nparam)

        if expected >= EXPECTED_MIN_TRIGGER and size < int(expected * REL_MIN_FACTOR):
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"File too small ({size} bytes) vs expected ~{expected} bytes.")

        if size < ABS_MIN_BYTES:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"File too small ({size} bytes). (ABS_MIN_BYTES={ABS_MIN_BYTES})")

    tmp.replace(target)
    logger.info(f"Done: {target.name} | {size_mb:.2f} MB | {dt_s:.1f} s")

def label_init_to_valid(init_dt: dt.datetime, start_h: int) -> str:
    """
    File title as 'INITDATE-VALIDDATE'.
    Example for init=14/01 and day_idx=2 (48h): '14_01_2026-16_01_2026'
    """
    valid_dt = init_dt + dt.timedelta(hours=start_h)
    return f"{fmt_dmy(init_dt)}-{fmt_dmy(valid_dt)}"

def main() -> int:
    base_dir = build_base_dir()
    ensure_dir(base_dir)

    logger = setup_logger(base_dir / LOG_NAME)
    script_start = now_utc()
    logger.info(f"Script start (UTC): {script_start.isoformat(timespec='seconds')}")
    logger.info(f"Base dir: {base_dir}")

    server = ECMWFService("mars")

    run_date = resolve_run_date_yyyymmdd()
    init_dt = init_datetime_utc(run_date, INIT_TIME_UTC)

    run_dir = build_run_dir(base_dir, run_date, INIT_TIME_UTC)
    ensure_dir(run_dir)

    failures: List[str] = []

    for day_idx in KEEP_DAY_INDICES:
        h0, h1 = day_window_hours(day_idx)
        step_str, ntime = steps_as_list(h0, h1, STEP_HOURS)

        label = label_init_to_valid(init_dt, h0)
        out_name = f"{label}.nc"
        out_path = run_dir / out_name

        if nonempty(out_path):
            logger.info(f"Skip existing: {out_name}")
            continue

        req = build_request(run_date, INIT_TIME_UTC, step_str, logger)

        try:
            mars_execute(server, req, out_path, ntime=ntime, logger=logger)
        except Exception as e:
            logger.error(f"FAILED day_idx={day_idx} ({label}): {e}")
            failures.append(f"day{day_idx}:{label}")

    script_end = now_utc()
    logger.info(f"Script end (UTC): {script_end.isoformat(timespec='seconds')}")
    logger.info(f"Elapsed: {script_end - script_start}")

    if failures:
        logger.warning(f"Some downloads failed: {', '.join(failures)}")
        return 2

    logger.info("All requested day windows downloaded successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
# %%
