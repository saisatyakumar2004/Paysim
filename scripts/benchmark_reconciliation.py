"""Benchmark: incremental reconciliation vs a full rebuild, as history grows.

For each simulated day the real pipeline runs (no dbt), and the incremental reconciliation time is read from
ops.recon_runs. At checkpoints the SAME state is also recomputed from scratch two ways:

  engine-full   the engine's full mode (scope = everything), compute only. Same code, so it is a like-for-like comparison.
  dbt-full      `dbt build --select tag:reference` = the independent full-rebuild SQL models that the pipeline used
                before the incremental engine existed (includes dbt start-up, as the daily run did).

It also times a no-op rerun (no new data) and finishes by proving the incremental state equals a full rebuild.

Usage:  python scripts/benchmark_reconciliation.py --days 30 [--orders-per-day 3000] [--checkpoints 10,20,30]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--orders-per-day", type=int, default=3000)
    ap.add_argument("--checkpoints", default=None, help="comma separated day numbers (default: every 5th day)")
    ap.add_argument("--no-dbt-full", action="store_true", help="skip the (slow) dbt full-rebuild timing")
    ap.add_argument("--out", default=str(ROOT / "docs"))
    args = ap.parse_args()

    workdir = ROOT / "data" / f"bench_{args.days}d"
    if workdir.exists():
        shutil.rmtree(workdir)
    os.environ["PAYRECON_DATA"] = str(workdir)

    from payreconcile import config, db, pipeline, reconcile, transform
    from payreconcile.generate import GenConfig, generate

    config.settings.cache_clear()
    checkpoints = ({int(x) for x in args.checkpoints.split(",")} if args.checkpoints
                   else set(range(5, args.days + 1, 5)) | {args.days})
    start = date(2026, 1, 1)
    t = time.perf_counter()
    summary = generate(GenConfig(out=workdir, days=args.days, orders_per_day=args.orders_per_day, start=start))
    print(f"generated {summary['orders']:,} orders / {summary['gateway_records']:,} gateway records in "
          f"{time.perf_counter() - t:.0f}s", flush=True)

    rows = []
    for d in range(1, args.days + 1):
        run_date = start + timedelta(days=d - 1)
        s = pipeline.run_day(run_date, run_dbt=False)
        assert s.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS"), (run_date, s.status, s.error)
        con = db.connect()
        inc = con.execute("SELECT mode, seconds, items_evaluated, items_changed, items_total, scope_txns, "
                          "scope_settlements, scope_orders, changes_read FROM ops.recon_runs "
                          "ORDER BY started_at DESC LIMIT 1").fetchone()
        row = {"day": d, "mode": inc[0], "incremental_s": round(inc[1], 3), "items_evaluated": inc[2],
               "items_changed": inc[3], "items_total": inc[4], "scope_txns": inc[5], "scope_settlements": inc[6],
               "scope_orders": inc[7], "changes_read": inc[8],
               "silver_txns": con.execute("SELECT count(*) FROM silver.gateway_txns").fetchone()[0],
               "silver_settlements": con.execute("SELECT count(*) FROM silver.settlements").fetchone()[0]}
        if d in checkpoints:
            as_of = con.execute("SELECT as_of_date FROM ops.recon_state WHERE id = 1").fetchone()[0]
            full = [reconcile.reconcile(con, as_of, "bench", full=True, dry_run=True).seconds for _ in range(3)]
            row["engine_full_s"] = round(min(full), 3)
            noop = [reconcile.reconcile(con, as_of, "bench-noop").seconds for _ in range(3)]
            row["noop_rerun_s"] = round(min(noop), 3)
            con.close()
            if not args.no_dbt_full:
                t0 = time.perf_counter()
                res = transform.build(as_of, reference=True)
                row["dbt_full_s"] = round(time.perf_counter() - t0, 2)
                assert res.success, res.failures
            con = db.connect()
        con.close()
        rows.append(row)
        extra = f" engine-full={row['engine_full_s']}s dbt-full={row.get('dbt_full_s')}s" if "engine_full_s" in row else ""
        print(f"day {d:>3}: incremental={row['incremental_s']:.3f}s evaluated {row['items_evaluated']:,}/"
              f"{row['items_total']:,} items{extra}", flush=True)

    con = db.connect()
    equivalence = reconcile.verify(con)
    if not args.no_dbt_full:
        equivalence["vs_dbt_reference"] = reconcile.compare_with_reference(con)
    audit_rows = con.execute("SELECT count(*) FROM gold.reconciliation_audit").fetchone()[0]
    con.close()

    inc = [r["incremental_s"] for r in rows[1:]]
    cp = [r for r in rows if "engine_full_s" in r]
    result = {"days": args.days, "orders_per_day": args.orders_per_day, "generated": summary,
              "incremental_median_s": round(statistics.median(inc), 3), "incremental_max_s": max(inc),
              "checkpoints": cp, "equivalence": equivalence, "audit_rows": audit_rows, "per_day": rows}
    out = Path(args.out)
    out.mkdir(exist_ok=True)
    (out / f"results_reconciliation_benchmark_{args.days}d.json").write_text(json.dumps(result, indent=2, default=str),
                                                                             encoding="utf-8")
    lines = ["| day | state items | evaluated (incremental) | incremental | engine full | speedup | dbt full rebuild | speedup |",
             "|---|---|---|---|---|---|---|---|"]
    for r in cp:
        sp1 = r["engine_full_s"] / r["incremental_s"] if r["incremental_s"] else float("nan")
        dbt = r.get("dbt_full_s")
        dbt_txt = f"{dbt:.1f}s" if dbt else "n/a"
        dbt_speed = f"{dbt / r['incremental_s']:.0f}x" if dbt else "n/a"
        share = r["items_evaluated"] / r["items_total"]
        lines.append(f"| {r['day']} | {r['items_total']:,} | {r['items_evaluated']:,} ({share:.0%}) "
                     f"| {r['incremental_s']:.2f}s | {r['engine_full_s']:.2f}s | {sp1:.1f}x | {dbt_txt} | {dbt_speed} |")
    (out / f"results_reconciliation_benchmark_{args.days}d.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"\nequivalence: {json.dumps(equivalence)}\naudit rows: {audit_rows:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
