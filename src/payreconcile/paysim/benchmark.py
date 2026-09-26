"""Measured benchmarks: CSV vs Parquet vs DuckDB table on the same analytical queries, dashboard query latency, and (optionally)
a pandas baseline. Every form runs in a FRESH subprocess so DuckDB's retained memory from one form cannot distort the next
one's peak-RSS reading. Times are the median of `repeats` runs; the first run of the CSV form also warms the OS file cache, so it is
reported separately as `first_run_seconds`. Peak RSS is sampled every 50 ms, a lower bound on the true peak.

The pipeline stage timings (ingestion, Parquet conversion, DQ, transformation, Gold, ML, export) come from run_metrics.json,
written by `pipeline.run_pipeline`; this module adds the query-level comparisons.
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from .. import resources
from . import queries
from .common import SOURCE_COLUMNS, Dataset, connect, connect_memory

# (name, SQL template). {t} is the relation; column names differ per form, so each form supplies its own column map.
QUERY_TEMPLATES = {
    "q1_volume_by_type": "SELECT {type}, count(*), sum({amount}), avg({amount}) FROM {t} GROUP BY {type}",
    "q2_fraud_by_type": "SELECT {type}, sum({fraud}), sum(CASE WHEN {fraud} = 1 THEN {amount} END) FROM {t} GROUP BY {type}",
    "q3_top_senders": "SELECT {orig}, sum({amount}) AS s FROM {t} GROUP BY {orig} ORDER BY s DESC LIMIT 20",
    "q4_range_filter": "SELECT count(*), sum({amount}) FROM {t} WHERE {step} BETWEEN 100 AND 200 AND {type} = 'TRANSFER'",
}
SOURCE_COLS = {"type": "type", "amount": "amount", "fraud": "isFraud", "orig": "nameOrig", "step": "step"}
TABLE_COLS = {"type": "type", "amount": "amount", "fraud": "is_fraud", "orig": "name_orig", "step": "step"}


def _time_queries(con, form: str, rel: str, cols: dict, repeats: int) -> dict:
    out = {}
    for name, tpl in QUERY_TEMPLATES.items():
        sql = tpl.format(t=rel, **cols)
        times = []
        for _ in range(repeats):
            t0 = time.perf_counter()
            con.execute(sql).fetchall()
            times.append(time.perf_counter() - t0)
        out[name] = {"first_run_seconds": round(times[0], 4), "median_seconds": round(statistics.median(times), 4),
                     "min_seconds": round(min(times), 4)}
    return out


def worker(form: str, name: str, source: str, csv_path: str, repeats: int) -> dict:
    """Runs inside a fresh subprocess: open the database, time the queries on one storage form."""
    ds = Dataset(name, Path(source))
    if form == "duckdb_table":
        con = connect(ds, read_only=True)
        rel, cols = "gold.fact_transactions", TABLE_COLS
    elif form == "parquet_bronze":
        con = connect_memory()
        rel, cols = f"read_parquet('{ds.bronze.as_posix()}')", SOURCE_COLS
    else:
        con = connect_memory()
        spec = ", ".join(f"'{c}': '{t}'" for c, t in SOURCE_COLUMNS.items())
        rel, cols = f"read_csv('{csv_path}', header = true, columns = {{{spec}}})", SOURCE_COLS
    return _time_queries(con, form, rel, cols, repeats)


def _run_form(form: str, ds: Dataset, csv_path: str, repeats: int) -> dict:
    code = ("import json,sys; from payreconcile.paysim.benchmark import worker; a=json.loads(sys.argv[1]); "
            "print('@@'+json.dumps(worker(**a)))")
    args = json.dumps({"form": form, "name": ds.name, "source": str(ds.source), "csv_path": csv_path, "repeats": repeats})
    t0 = time.perf_counter()
    with resources.PeakMemory() as mem:
        p = subprocess.run([sys.executable, "-c", code, args], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=3600)
    if p.returncode != 0 or not p.stdout or "@@" not in p.stdout:
        raise RuntimeError(f"benchmark worker for {form!r} failed (exit {p.returncode}): {(p.stderr or '')[-800:]}")
    res = json.loads(p.stdout.split("@@")[-1])
    return {"queries": res, "process_seconds": round(time.perf_counter() - t0, 2),
            "peak_rss_mb": None if mem.peak_mb is None else round(mem.peak_mb, 1)}


def _pandas_baseline(csv_path: str) -> dict:
    code = ("import pandas as pd,time; t=time.perf_counter(); df=pd.read_csv(r'%s'); r=time.perf_counter()-t; "
            "df.groupby('type')['amount'].agg(['count','sum','mean']); print('@@%%f@@%%f@@%%d' %% (r, time.perf_counter()-t, len(df)))" % csv_path)
    t0 = time.perf_counter()
    with resources.PeakMemory() as mem:
        p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=3600)
    if p.returncode != 0 or not p.stdout:
        raise RuntimeError(f"pandas baseline failed (exit {p.returncode}): {(p.stderr or '')[-500:]}")
    _, read_s, total_s, rows = p.stdout.strip().split("@@")
    return {"read_csv_seconds": round(float(read_s), 2), "read_and_groupby_seconds": round(float(total_s), 2), "rows": int(rows),
            "peak_rss_mb": None if mem.peak_mb is None else round(mem.peak_mb, 1), "process_seconds": round(time.perf_counter() - t0, 2)}


def _dashboard(ds: Dataset, repeats: int) -> dict:
    con = connect(ds, read_only=True)
    try:
        res, total = {}, 0.0
        acct = con.execute("SELECT account FROM gold.fraud_account_summary LIMIT 1").fetchone()
        named = dict(queries.DASHBOARD_QUERIES)
        for name, sql in named.items():
            ts = []
            for _ in range(repeats):
                t0 = time.perf_counter()
                con.execute(sql).fetchall()
                ts.append(time.perf_counter() - t0)
            res[name] = round(statistics.median(ts), 4)
            total += res[name]
        if acct:
            ts = []
            for _ in range(repeats):
                t0 = time.perf_counter()
                con.execute(queries.ACCOUNT_HISTORY, [acct[0]] * 6).fetchall()
                ts.append(time.perf_counter() - t0)
            res["account_history"] = round(statistics.median(ts), 4)
            total += res["account_history"]
        return {"median_seconds": res, "sum_of_medians_seconds": round(total, 3), "slowest": max(res, key=res.get)}
    finally:
        con.close()


def run_benchmark(ds: Dataset, repeats: int = 3, include_pandas: bool = False) -> dict:
    csv_path = str(ds.source)
    tmp_csv = None
    if not ds.is_csv:                                    # the development sample is Parquet: write an equivalent CSV to compare with
        tmp_csv = ds.dir / "benchmark_source.csv"
        con = connect_memory(ds.dir / "tmp")
        cols = ", ".join(f'"{c}"' for c in SOURCE_COLUMNS)
        con.execute(f"COPY (SELECT {cols} FROM read_parquet('{ds.bronze.as_posix()}') ORDER BY source_row_number) "
                    f"TO '{tmp_csv.as_posix()}' (HEADER, DELIMITER ',')")
        con.close()
        csv_path = str(tmp_csv)
    out: dict = {"generated_at": datetime.now().isoformat(timespec="seconds"), "dataset": ds.name, "repeats": repeats,
                 "csv_bytes": os.path.getsize(csv_path), "parquet_bytes": ds.bronze.stat().st_size,
                 "duckdb_bytes": ds.db.stat().st_size}
    try:
        out["forms"] = {f: _run_form(f, ds, csv_path, repeats) for f in ("csv", "parquet_bronze", "duckdb_table")}
        out["dashboard"] = _dashboard(ds, repeats)
        if include_pandas:
            out["pandas_read_csv"] = _pandas_baseline(csv_path)
    finally:
        if tmp_csv is not None and tmp_csv.exists():
            tmp_csv.unlink()
    metrics = ds.dir / "run_metrics.json"
    if metrics.exists():
        out["pipeline"] = json.loads(metrics.read_text(encoding="utf-8"))
    (ds.dir / "benchmark.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    return out
