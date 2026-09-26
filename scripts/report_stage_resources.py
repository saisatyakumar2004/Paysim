"""Summarise per-stage wall-clock time, peak memory and row counts of real pipeline runs (from ops.task_log).

Also measures the dashboard's first render (time and peak RSS) with Streamlit's AppTest.
Writes data/benchmarks/results_stage_resources_<label>.json.  Read-only with respect to the warehouse.

Usage:  python scripts/report_stage_resources.py --label 30day_backfill [--no-dashboard]
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from payreconcile import config, resources  # noqa: E402

STAGE_NAMES = {"extract": "ingestion (extract + Bronze)", "validate_and_quality": "data quality (rules, gate)",
               "load_silver": "Silver (upserts)", "reconcile": "reconciliation (incremental)", "dbt_build": "dbt (marts + tests)"}


def med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 2) if xs else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="30day_backfill")
    ap.add_argument("--no-dashboard", action="store_true")
    args = ap.parse_args()

    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    runs = con.execute("SELECT count(*), min(run_date), max(run_date) FROM ops.run_log").fetchone()
    stages = []
    for task, label in STAGE_NAMES.items():
        rows = con.execute("SELECT duration_s, peak_rss_mb, rows_in, rows_out, rows_rejected FROM ops.task_log WHERE task = ? "
                           "AND status = 'SUCCESS'", [task]).fetchall()
        if not rows:
            continue
        stages.append({"stage": label, "runs": len(rows), "median_s": med([r[0] for r in rows]),
                       "max_s": round(max(r[0] for r in rows), 2),
                       "median_peak_rss_mb": med([r[1] for r in rows]),
                       "max_peak_rss_mb": round(max((r[1] for r in rows if r[1] is not None), default=0), 1) or None,
                       "median_rows_in": med([r[2] for r in rows]), "median_rows_out": med([r[3] for r in rows]),
                       "median_rows_rejected": med([r[4] for r in rows])})
    total = con.execute("SELECT median(duration_s), max(duration_s), sum(duration_s) FROM ops.run_log").fetchone()
    con.close()

    out = {"run_at": datetime.now().isoformat(timespec="seconds"), "label": args.label,
           "config": f"{runs[0]} pipeline runs, {runs[1]} to {runs[2]}, synthetic data, seed 42",
           "python": platform.python_version(), "psutil_available": resources.available(),
           "note": "peak_rss_mb = sampled (50 ms) peak resident memory of the process tree; a lower bound on the true peak. "
                   "The dbt stage includes the dbt subprocess.",
           "whole_run_seconds": {"median": round(total[0], 2), "max": round(total[1], 2), "total": round(total[2], 1)},
           "stages": stages}

    if not args.no_dashboard:
        from streamlit.testing.v1 import AppTest
        with resources.PeakMemory() as pm:
            t0 = time.perf_counter()
            at = AppTest.from_file(str(ROOT / "dashboard" / "app.py"), default_timeout=120).run()
            first = time.perf_counter() - t0
            t1 = time.perf_counter()
            at.run()
            second = time.perf_counter() - t1
        out["dashboard"] = {"first_render_s": round(first, 2), "second_render_s": round(second, 2),
                            "peak_rss_mb_including_python_and_streamlit_import": round(pm.peak_mb) if pm.peak_mb else None,
                            "exception": bool(at.exception)}
        out["dashboard"] = (f"first render {first:.1f} s, cached re-render {second:.1f} s, peak RSS "
                            f"{round(pm.peak_mb) if pm.peak_mb else 'n/a'} MB (in-process AppTest; a real browser session adds the "
                            "browser's own memory, which is NOT MEASURED)")
    path = ROOT / "data" / "benchmarks" / f"results_stage_resources_{args.label}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
