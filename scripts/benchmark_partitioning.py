"""Controlled experiment: does date-partitioned Parquet for Silver beat DuckDB tables for this workload?

Hypothesis under test: partitioning Silver by date makes the incremental reconciliation cheaper, because the daily batch only
touches recent dates. Nothing here changes the production code path; it measures two layouts of the same data.

  baseline     Silver as DuckDB tables (what the pipeline uses today).
  partitioned  Silver as hive-partitioned Parquet (silver/<table>/part_date=YYYY-MM-DD/*.parquet), exposed to the
               UNCHANGED reconciliation engine through views with the same names.

Method (per scale S; history = 30*S days at a FIXED 3,000 orders/day, so the state grows while the daily batch does not):
  1. Synthesise Silver history + one more "next day" batch deterministically in SQL (no randomness beyond hash()).
  2. Build reconciliation state with the real engine (a full run; timed).
  3. Ingest the next-day batch into Silver: DuckDB typed upsert (real _upsert) vs Parquet partition rewrite.
  4. Run the real incremental reconciliation on both layouts; count rows scanned (DuckDB profiling), time, peak RSS.
  5. Compare the two final reconciliation states (must be identical), then time a full recompute and a dbt build.
  6. Micro-benchmark the key-lookup pattern with and without a date bound.

"1x" = the 30-day / 3,000-orders-per-day benchmark (~91k reconciliation items). Scales that do not fit the machine are
reported as NOT RUN with the reason; nothing is extrapolated.

Usage:  python scripts/benchmark_partitioning.py --scales 1,10,25 [--work-dir DIR] [--reps 3] [--dbt-max-scale 10]
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import statistics
import sys
import tempfile
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

START = date(2026, 1, 1)
N_MERCHANTS = 200
OPD = 3000                     # orders per day (fixed)
BASE_DAYS = 30                 # 1x


# ------------------------------------------------------------------------------------------------------------------
def synth(con, days: int) -> None:
    """Create typed temp tables syn_orders / syn_txn / syn_stl covering `days` days of history plus one next day."""
    n_orders = (days + 1) * OPD
    last_plus1 = START + timedelta(days=days)
    con.execute("CREATE OR REPLACE TEMP MACRO u(i, salt) AS ((hash(i * 7919 + salt) % 1000000) / 1000000.0)")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE syn_orders AS
        SELECT i AS n, 'ORD' || lpad(i::VARCHAR, 9, '0') AS order_id,
               'M' || lpad((hash(i) % {N_MERCHANTS})::VARCHAR, 4, '0') AS merchant_id,
               'C' || lpad((hash(i + 1) % 50000)::VARCHAR, 6, '0') AS customer_id,
               least(80000.0, greatest(20.0, round(exp(6.6 + 0.9 * sqrt(-2 * ln(u(i, 1) + 1e-9)) * cos(2 * pi() * u(i, 2))), 2))) AS amount,
               (u(i, 3) < 0.93) AS is_paid,
               (DATE '{START}' + (i // {OPD})::INTEGER) AS d,
               (DATE '{START}' + (i // {OPD})::INTEGER)::TIMESTAMP + to_seconds((u(i, 4) * 86399)::BIGINT) AS created_at
        FROM range({n_orders}) r(i)""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE syn_txn AS
        SELECT 'TXN' || lpad(((n * 2654435761) % 10000000000)::VARCHAR, 10, '0') AS txn_id, order_id, merchant_id, amount,
               CASE WHEN is_paid THEN 'captured' ELSE 'failed' END AS status, 'payment' AS txn_type,
               NULL::VARCHAR AS refund_of, created_at + INTERVAL 5 SECOND AS created_at,
               ['upi', 'card', 'netbanking', 'wallet'][1 + (hash(n + 9) % 4)::INTEGER] AS method, n AS seq
        FROM syn_orders WHERE d < DATE '{last_plus1}' OR d = DATE '{last_plus1}'
        UNION ALL
        SELECT 'TXN' || lpad((((n + {n_orders}) * 2654435761) % 10000000000)::VARCHAR, 10, '0'), order_id, merchant_id, amount,
               'captured', 'refund', 'TXN' || lpad(((n * 2654435761) % 10000000000)::VARCHAR, 10, '0'),
               created_at + to_days(1 + (hash(n + 11) % 5)::INTEGER),
               ['upi', 'card', 'netbanking', 'wallet'][1 + (hash(n + 9) % 4)::INTEGER], n + {n_orders}
        FROM syn_orders WHERE is_paid AND u(n, 5) < 0.03""")
    con.execute(f"DELETE FROM syn_txn WHERE created_at::DATE > DATE '{last_plus1}'")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE syn_stl AS
        SELECT 'STL' || lpad(((seq * 2654435761) % 1000000000)::VARCHAR, 9, '0') AS settlement_id,
               CASE WHEN u(seq, 10) < 0.01
                    THEN substr(txn_id, 1, 12) || ((right(txn_id, 1)::INTEGER + 1) % 10)::VARCHAR ELSE txn_id END AS ref_id,
               (u(seq, 9) < 0.6) AS gw_prefix, merchant_id,
               round((CASE WHEN txn_type = 'refund' THEN -amount ELSE amount END) *
                     (CASE WHEN u(seq, 8) < 0.015 THEN 0.9 ELSE 1.0 END), 2) AS gross_amount,
               created_at::DATE + (CASE WHEN u(seq, 7) < 0.7 THEN 1 ELSE 2 END) AS settlement_date
        FROM syn_txn WHERE status = 'captured' AND u(seq, 6) >= 0.015""")
    con.execute(f"DELETE FROM syn_stl WHERE settlement_date > DATE '{last_plus1}'")


def load_history(con, last_day: date) -> dict:
    """Insert everything up to `last_day` into the real silver tables; return row counts."""
    ld = last_day.isoformat()
    con.execute(f"""INSERT INTO silver.orders
        SELECT order_id, merchant_id, customer_id, amount, CASE WHEN is_paid THEN 'paid' ELSE 'cancelled' END,
               created_at, created_at, 'synth' FROM syn_orders WHERE d <= DATE '{ld}'""")
    con.execute(f"""INSERT INTO silver.gateway_txns
        SELECT txn_id, order_id, merchant_id, amount, 'INR', status, method, txn_type, refund_of, created_at, 'synth'
        FROM syn_txn WHERE created_at::DATE <= DATE '{ld}'""")
    con.execute(f"""INSERT INTO silver.settlements
        SELECT settlement_id, CASE WHEN gw_prefix THEN 'GW-' || ref_id ELSE lower(ref_id) END,
               upper(ref_id), merchant_id, gross_amount, round(abs(gross_amount) * 0.02, 2),
               round(gross_amount - CASE WHEN gross_amount < 0 THEN 0 ELSE abs(gross_amount) * 0.02 END, 2),
               settlement_date, true, 'synth'
        FROM syn_stl WHERE settlement_date <= DATE '{ld}'""")
    for i in range(N_MERCHANTS):
        con.execute("INSERT INTO silver.merchants_scd2 VALUES (?, ?, 'grocery', 'Pune', 'low', 2.0, DATE '1970-01-01', "
                    "DATE '9999-12-31', true, 'h', 'synth')", [f"M{i:04d}", f"Merchant {i}"])
    return {t: con.execute(f"SELECT count(*) FROM silver.{t}").fetchone()[0]
            for t in ("orders", "gateway_txns", "settlements")}


def batch_tables(con, next_day: date) -> None:
    """Typed staging tables (`b_orders`, `b_txn`, `b_stl`) holding exactly the next day's records."""
    nd = next_day.isoformat()
    con.execute(f"""CREATE OR REPLACE TEMP TABLE b_orders AS
        SELECT order_id, merchant_id, customer_id, amount AS order_amount,
               CASE WHEN is_paid THEN 'paid' ELSE 'cancelled' END AS order_status, created_at, created_at AS updated_at,
               'batch' AS _run_id FROM syn_orders WHERE d = DATE '{nd}'""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE b_txn AS
        SELECT txn_id, order_id, merchant_id, amount, 'INR' AS currency, status, method, txn_type, refund_of, created_at,
               'batch' AS _run_id FROM syn_txn WHERE created_at::DATE = DATE '{nd}'""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE b_stl AS
        SELECT settlement_id, CASE WHEN gw_prefix THEN 'GW-' || ref_id ELSE lower(ref_id) END AS gateway_ref,
               upper(ref_id) AS gateway_ref_norm, merchant_id, gross_amount, round(abs(gross_amount) * 0.02, 2) AS fee_amount,
               round(gross_amount - CASE WHEN gross_amount < 0 THEN 0 ELSE abs(gross_amount) * 0.02 END, 2) AS net_amount,
               settlement_date, true AS net_consistent, 'batch' AS _run_id FROM syn_stl WHERE settlement_date = DATE '{nd}'""")


# ------------------------------------------------------------------------------------------------------------------
class ScanCounter:
    """Wraps a connection; after every statement reads DuckDB's profiling output and sums `cumulative_rows_scanned`."""

    def __init__(self, con, path: Path):
        self.con, self.path, self.rows_scanned, self.statements = con, path, 0, 0
        con.execute("PRAGMA enable_profiling = 'json'")
        con.execute(f"PRAGMA profiling_output = '{path.as_posix()}'")

    def _read(self) -> None:
        try:
            self.rows_scanned += int(json.loads(self.path.read_text(encoding="utf-8")).get("cumulative_rows_scanned", 0))
            self.statements += 1
        except (OSError, ValueError):
            pass

    def execute(self, sql, *a):
        r = self.con.execute(sql, *a)
        self._read()
        return r

    def executemany(self, sql, *a):
        r = self.con.executemany(sql, *a)
        self._read()
        return r

    def close(self) -> None:
        self.con.execute("PRAGMA disable_profiling")


def open_db(path: Path, view_mode: bool = False):
    """Plain connection with the same settings as payreconcile.db.connect (which would try to CREATE the silver tables)."""
    con = duckdb.connect(str(path))
    con.execute("SET memory_limit='3GB'")
    con.execute("SET threads=4")
    return con


def export_parquet(con, out: Path) -> dict[str, int]:
    """Partition the three silver tables by date; return files per table."""
    spec = {"gateway_txns": "created_at::DATE", "settlements": "settlement_date", "orders": "created_at::DATE"}
    files = {}
    out.mkdir(parents=True, exist_ok=True)
    for t, expr in spec.items():
        d = out / t
        if d.exists():
            shutil.rmtree(d)
        con.execute(f"COPY (SELECT *, {expr} AS part_date FROM silver.{t}) TO '{d.as_posix()}' "
                    f"(FORMAT parquet, PARTITION_BY (part_date), OVERWRITE_OR_IGNORE true)")
        files[t] = sum(1 for _ in d.rglob("*.parquet"))
    return files


def parquet_upsert(con, out: Path) -> float:
    """Apply the batch to the partitioned Parquet layout: rewrite only the touched date partitions (typed upsert)."""
    t0 = time.perf_counter()
    spec = {"gateway_txns": ("b_txn", "txn_id", "created_at::DATE"), "settlements": ("b_stl", "settlement_id", "settlement_date"),
            "orders": ("b_orders", "order_id", "created_at::DATE")}
    for t, (b, key, expr) in spec.items():
        for (pd_,) in con.execute(f"SELECT DISTINCT {expr} FROM {b}").fetchall():
            part = out / t / f"part_date={pd_.isoformat()}"
            new = f"SELECT * FROM {b} WHERE {expr} = DATE '{pd_.isoformat()}'"
            if part.exists():
                old = f"SELECT * FROM read_parquet('{(part / '*.parquet').as_posix()}') WHERE {key} NOT IN (SELECT {key} FROM {b})"
                q = f"{old} UNION ALL BY NAME {new}"
                tmp = part / "_new.parquet.tmp"
                con.execute(f"COPY ({q}) TO '{tmp.as_posix()}' (FORMAT parquet)")
                for f in part.glob("*.parquet"):
                    f.unlink()
                tmp.replace(part / "data_0.parquet")
            else:
                part.mkdir(parents=True)
                con.execute(f"COPY ({new}) TO '{(part / 'data_0.parquet').as_posix()}' (FORMAT parquet)")
    return time.perf_counter() - t0


def make_views(con, pq: Path) -> None:
    for t in ("gateway_txns", "settlements", "orders"):
        con.execute(f"DROP TABLE silver.{t}")
        con.execute(f"CREATE VIEW silver.{t} AS SELECT * EXCLUDE (part_date) FROM "
                    f"read_parquet('{(pq / t).as_posix()}/*/*.parquet', hive_partitioning = true)")


def state_summary(con) -> dict:
    return {"items": con.execute("SELECT count(*) FROM gold.fact_reconciliation").fetchone()[0],
            "money": round(con.execute("SELECT coalesce(sum(money_at_risk), 0) FROM gold.fact_reconciliation").fetchone()[0], 2),
            "by_status": dict(con.execute("SELECT status, count(*) FROM gold.fact_reconciliation GROUP BY 1 ORDER BY 1").fetchall()),
            "links": con.execute("SELECT count(*) FROM gold.recon_links").fetchone()[0]}


def median(xs):
    return round(statistics.median(xs), 3)


def dir_mb(p: Path) -> float:
    return round(sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) / 1024 / 1024, 1)


# ------------------------------------------------------------------------------------------------------------------
def run_scale(scale: int, work: Path, reps: int, dbt_max_scale: int, full_max_scale: int,
              want_partitioned: bool = True) -> dict:
    from payreconcile import db, reconcile, resources, transform

    days = BASE_DAYS * scale
    last_day = START + timedelta(days=days - 1)
    next_day = last_day + timedelta(days=1)
    res: dict = {"scale": scale, "history_days": days, "orders_per_day": OPD}
    os.environ["PAYRECON_DATA"] = str(work)
    from payreconcile import config
    config.settings.cache_clear()
    base_db, part_db, pq = work / "warehouse.duckdb", work / "part.duckdb", work / "parquet"

    t0 = time.perf_counter()
    con = db.connect(base_db)
    synth(con, days)
    res["history_rows"] = load_history(con, last_day)
    res["synth_seconds"] = round(time.perf_counter() - t0, 1)
    print(f"[{scale}x] history loaded {res['history_rows']} in {res['synth_seconds']}s", flush=True)

    # ---- state build (a real FULL run) ---------------------------------------------------------------------------
    with resources.PeakMemory() as pm:
        st = reconcile.reconcile(con, last_day, "bench-init", last_day, full=True)
    res["initial_full_reconcile"] = {"seconds": round(st.seconds, 2), "items": st.items_total,
                                     "peak_rss_mb": round(pm.peak_mb) if pm.peak_mb else None}
    print(f"[{scale}x] initial full reconcile {st.seconds:.1f}s, {st.items_total:,} items", flush=True)

    # ---- pre-batch parquet export (for the partitioned ingestion path) ---------------------------------------------
    if want_partitioned:
        t1 = time.perf_counter()
        files = export_parquet(con, pq)
        res["parquet_export_seconds"] = round(time.perf_counter() - t1, 1)
        res["parquet_files"] = files

    # ---- ingestion of the next-day batch: DuckDB typed upsert vs parquet partition rewrite -------------------------------
    from payreconcile import silver
    batch_tables(con, next_day)
    res["batch_rows"] = {t: con.execute(f"SELECT count(*) FROM {b}").fetchone()[0]
                         for t, b in (("orders", "b_orders"), ("gateway_txns", "b_txn"), ("settlements", "b_stl"))}
    t2 = time.perf_counter()
    with resources.PeakMemory() as pm_i:
        for tbl, stg, key, ds, aux in (("silver.orders", "b_orders", "order_id", "orders", None),
                                      ("silver.gateway_txns", "b_txn", "txn_id", "gateway", "order_id"),
                                      ("silver.settlements", "b_stl", "settlement_id", "settlements", "gateway_ref_norm")):
            con.execute(f"CREATE OR REPLACE TEMP TABLE stg AS SELECT * FROM {stg}")
            silver._upsert(con, tbl, key, ds, aux, "bench-batch", newer_only=(ds == "orders"))
    ing_base = time.perf_counter() - t2
    ing_pq = parquet_upsert(con, pq) if want_partitioned else None
    res["ingest_next_day"] = {"baseline_duckdb_upsert_s": round(ing_base, 3),
                              "partitioned_parquet_rewrite_s": None if ing_pq is None else round(ing_pq, 3),
                              "baseline_peak_rss_mb": round(pm_i.peak_mb) if pm_i.peak_mb else None,
                              "note": "typed batch in both cases; baseline also records the change log"}
    print(f"[{scale}x] ingest baseline {ing_base:.2f}s vs parquet {ing_pq if ing_pq is None else round(ing_pq, 2)}s", flush=True)

    # ---- variant DB = post-batch baseline with silver replaced by views over the parquet ---------------------------------
    con.close()
    if want_partitioned:
        shutil.copy(base_db, part_db)
        pcon = open_db(part_db)
        make_views(pcon, pq)
        pcon.close()
    res["disk_mb"] = {"duckdb_file_after_batch": round(base_db.stat().st_size / 1024 / 1024, 1),
                      "silver_parquet_dir": dir_mb(pq) if want_partitioned else None}

    layouts = {"baseline": base_db, **({"partitioned": part_db} if want_partitioned else {})}
    for name, path in layouts.items():
        con = open_db(path)
        r: dict = {}
        as_of = next_day
        # rows scanned (compute phase, profiling on)
        scan = ScanCounter(con, work / f"prof_{name}.json")
        reconcile.reconcile(scan, as_of, "bench-scan", next_day, dry_run=True)
        scan.close()
        r["incremental_rows_scanned"] = scan.rows_scanned
        r["incremental_statements"] = scan.statements
        # timings (compute only, repeated)
        times = []
        for k in range(reps):
            with resources.PeakMemory() as pm:
                times.append(reconcile.reconcile(con, as_of, "bench-t", next_day, dry_run=True).seconds)
            if k == 0:
                r["incremental_compute_peak_rss_mb"] = round(pm.peak_mb) if pm.peak_mb else None
        r["incremental_compute_s"] = median(times)
        r["incremental_compute_all_s"] = [round(t, 3) for t in times]
        # the real incremental run (mutates state)
        with resources.PeakMemory() as pm:
            st = reconcile.reconcile(con, as_of, "bench-real", next_day)
        r["incremental_real_s"] = round(st.seconds, 3)
        r["incremental_items_evaluated"] = st.items_evaluated
        r["incremental_items_changed"] = st.items_changed
        r["incremental_real_peak_rss_mb"] = round(pm.peak_mb) if pm.peak_mb else None
        r["state"] = state_summary(con)
        # full recompute (compute only)
        if scale <= full_max_scale:
            scan = ScanCounter(con, work / f"prof_{name}_full.json")
            reconcile.reconcile(scan, as_of, "bench-scan-f", next_day, full=True, dry_run=True)
            scan.close()
            r["full_rows_scanned"] = scan.rows_scanned
            with resources.PeakMemory() as pm:
                r["full_compute_s"] = round(reconcile.reconcile(con, as_of, "bench-full", next_day, full=True, dry_run=True).seconds, 3)
            r["full_compute_peak_rss_mb"] = round(pm.peak_mb) if pm.peak_mb else None
        else:
            r["full_compute_s"] = "NOT RUN (above --full-max-scale)"
        con.close()
        res[name] = r
        print(f"[{scale}x] {name}: incremental compute {r['incremental_compute_s']}s, real {r['incremental_real_s']}s, "
              f"scanned {r['incremental_rows_scanned']:,} rows", flush=True)

    res["layouts_produce_identical_state"] = (res["baseline"]["state"] == res["partitioned"]["state"]
                                              if want_partitioned else "n/a (baseline only)")

    # ---- micro-benchmark: key lookup with / without a date bound ---------------------------------------------------------------
    res["lookup_micro"] = lookup_micro(base_db, pq, last_day) if want_partitioned else "NOT RUN (baseline only)"

    # ---- dbt --------------------------------------------------------------------------------------------------------------------
    if scale <= dbt_max_scale:
        for name, path in layouts.items():
            os.environ["PAYRECON_DATA"] = str(work)
            shutil.copy(path, work / "warehouse.duckdb") if path != base_db else None
            with resources.PeakMemory() as pm:
                t = time.perf_counter()
                d = transform.build(next_day)
                secs = time.perf_counter() - t
            res[name]["dbt_build_s"] = round(secs, 2)
            res[name]["dbt_peak_rss_mb"] = round(pm.peak_mb) if pm.peak_mb else None
            res[name]["dbt_result"] = {"success": d.success, "passed": d.passed, "failed": d.failed}
            print(f"[{scale}x] dbt {name}: {secs:.1f}s success={d.success}", flush=True)
    else:
        for name in layouts:
            res[name]["dbt_build_s"] = "NOT RUN (above --dbt-max-scale)"
    return res


def lookup_micro(base_db: Path, pq: Path, last_day: date, n_ids: int = 3000, reps: int = 5) -> dict:
    """Point lookups of ~3,000 keys, as the engine does. Recent ids with/without a date bound; old ids (a late arrival)."""
    con = open_db(base_db)
    recent = [r[0] for r in con.execute("SELECT txn_id FROM silver.gateway_txns WHERE created_at::DATE = ? LIMIT ?",
                                        [last_day, n_ids]).fetchall()]
    old = [r[0] for r in con.execute("SELECT txn_id FROM silver.gateway_txns WHERE created_at::DATE = ? LIMIT 200",
                                     [START]).fetchall()]
    lo = (last_day - timedelta(days=4)).isoformat()
    glob = f"{(pq / 'gateway_txns').as_posix()}/*/*.parquet"
    out = {}

    def timed(c, sql, ids):
        c.execute("CREATE OR REPLACE TEMP TABLE ids AS SELECT unnest(?::VARCHAR[]) AS id", [ids])
        ts = []
        for _ in range(reps):
            t = time.perf_counter()
            c.execute(sql).fetchall()
            ts.append(time.perf_counter() - t)
        return median(ts)

    out["baseline_no_date_bound_s"] = timed(con, "SELECT * FROM silver.gateway_txns WHERE txn_id IN (SELECT id FROM ids)", recent)
    out["baseline_date_bound_s"] = timed(
        con, f"SELECT * FROM silver.gateway_txns WHERE txn_id IN (SELECT id FROM ids) AND created_at >= TIMESTAMP '{lo}'", recent)
    out["baseline_old_ids_no_bound_s"] = timed(con, "SELECT * FROM silver.gateway_txns WHERE txn_id IN (SELECT id FROM ids)", old)
    pcon = open_db(base_db)
    out["parquet_no_date_bound_s"] = timed(
        pcon, f"SELECT * FROM read_parquet('{glob}', hive_partitioning = true) WHERE txn_id IN (SELECT id FROM ids)", recent)
    out["parquet_date_bound_s"] = timed(
        pcon, f"SELECT * FROM read_parquet('{glob}', hive_partitioning = true) WHERE txn_id IN (SELECT id FROM ids) "
              f"AND part_date >= DATE '{lo}'", recent)
    out["parquet_old_ids_no_bound_s"] = timed(
        pcon, f"SELECT * FROM read_parquet('{glob}', hive_partitioning = true) WHERE txn_id IN (SELECT id FROM ids)", old)
    con.close()
    pcon.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scales", default="1,10,25")
    ap.add_argument("--work-dir", default=str(Path(tempfile.gettempdir()) / "payrecon_bench"))
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--dbt-max-scale", type=int, default=10)
    ap.add_argument("--full-max-scale", type=int, default=25)
    ap.add_argument("--layouts", default="baseline,partitioned", help="baseline,partitioned or baseline")
    ap.add_argument("--out", default=str(ROOT / "docs" / f"results_partitioning_benchmark_{date.today().isoformat()}.json"))
    args = ap.parse_args()

    import dbt.version
    import pandas
    import psutil
    import pyarrow

    from payreconcile import resources
    header = {"run_at": datetime.now().isoformat(timespec="seconds"),
              "hardware": {"cpu": platform.processor(), "logical_cpus": os.cpu_count(),
                           "ram_gb": round(psutil.virtual_memory().total / 1024 ** 3, 1),
                           "free_ram_gb_at_start": round(psutil.virtual_memory().available / 1024 ** 3, 1)},
              "software": {"python": platform.python_version(), "duckdb": duckdb.__version__, "pandas": pandas.__version__,
                           "pyarrow": pyarrow.__version__, "dbt-core": dbt.version.__version__.version if hasattr(dbt.version.__version__, "version") else str(dbt.version.__version__),
                           "os": platform.platform()},
              "config": {"scales": args.scales, "reps": args.reps, "dbt_max_scale": args.dbt_max_scale,
                         "full_max_scale": args.full_max_scale, "layouts": args.layouts, "orders_per_day": OPD, "base_days": BASE_DAYS,
                         "duckdb_memory_limit": "3GB", "duckdb_threads": 4, "psutil_available": resources.available()},
              "method": __doc__.split("Method")[1].split("Usage")[0].strip() if __doc__ else ""}
    results = []
    for s in [int(x) for x in args.scales.split(",")]:
        work = Path(args.work_dir) / f"scale_{s}x"
        if work.exists():
            shutil.rmtree(work)
        work.mkdir(parents=True)
        try:
            results.append(run_scale(s, work, args.reps, args.dbt_max_scale, args.full_max_scale,
                                     want_partitioned="partitioned" in args.layouts))
        except Exception as exc:                                  # noqa: BLE001 - a failed scale is a RESULT, not a crash
            results.append({"scale": s, "status": "FAILED", "error": f"{type(exc).__name__}: {exc}",
                            "trace": traceback.format_exc()[-1500:]})
            print(f"[{s}x] FAILED: {exc}", flush=True)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        Path(args.out).write_text(json.dumps({**header, "results": results}, indent=2, default=str), encoding="utf-8")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
