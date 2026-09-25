"""Command line entry point: python -m payreconcile.cli <command>."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, timedelta

from . import config


def _d(s: str) -> date:
    return date.fromisoformat(s)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="payreconcile")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="generate synthetic messy source data + ground truth")
    g.add_argument("--days", type=int, default=30)
    g.add_argument("--orders-per-day", type=int, default=3000)
    g.add_argument("--merchants", type=int, default=200)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--bad-rate", type=float, default=1.0)
    g.add_argument("--start", type=_d, default=date(2026, 1, 1))

    r = sub.add_parser("run", help="run the pipeline for one day")
    r.add_argument("--date", type=_d, required=True)
    r.add_argument("--no-dbt", action="store_true")

    b = sub.add_parser("backfill", help="run the pipeline for a date range (sequentially)")
    b.add_argument("--start", type=_d, required=True)
    b.add_argument("--end", type=_d, required=True)
    b.add_argument("--no-dbt", action="store_true")
    b.add_argument("--prefect", action="store_true", help="run via the Prefect flow")

    sub.add_parser("evaluate", help="score pipeline output against the generator's ground truth")
    sub.add_parser("anomalies", help="train + score the merchant-day anomaly model on trusted data")
    sub.add_parser("reset", help="delete the warehouse and bronze layers (keeps source data)")

    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("payreconcile").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    if args.cmd == "generate":
        from .generate import GenConfig, generate
        s = generate(GenConfig(days=args.days, orders_per_day=args.orders_per_day, n_merchants=args.merchants,
                               seed=args.seed, bad_rate=args.bad_rate, start=args.start))
        print(json.dumps(s, indent=2, default=str))
    elif args.cmd == "run":
        from .pipeline import run_day
        print(_fmt(run_day(args.date, run_dbt=not args.no_dbt)))
    elif args.cmd == "backfill":
        if args.prefect:
            from .flows import backfill_flow
            backfill_flow(args.start, args.end, not args.no_dbt)
        else:
            from .pipeline import run_day
            d = args.start
            while d <= args.end:
                print(_fmt(run_day(d, run_dbt=not args.no_dbt)), flush=True)
                d += timedelta(days=1)
    elif args.cmd == "evaluate":
        from .evaluate import evaluate_all, print_report
        print_report(evaluate_all())
    elif args.cmd == "anomalies":
        from .anomaly import run_anomaly_model
        print(json.dumps(run_anomaly_model(), indent=2, default=str))
    elif args.cmd == "reset":
        import shutil
        shutil.rmtree(config.bronze_dir(), ignore_errors=True)
        config.warehouse_path().unlink(missing_ok=True)
        print("reset done")
    return 0


def _fmt(s) -> str:
    dbt = f" dbt(pass={s.dbt.passed},fail={s.dbt.failed})" if s.dbt else ""
    return (f"{s.run_date} {s.status:<22} gate={s.gate} dq={s.dq_score:.4f} read={s.rows_read} "
            f"written={s.rows_written} rejected={s.rows_rejected} {s.duration_s:.1f}s{dbt}"
            + (f" ERROR={s.error}" if s.error else "") if s.dq_score is not None else f"{s.run_date} {s.status} {s.error}")


if __name__ == "__main__":
    raise SystemExit(main())
