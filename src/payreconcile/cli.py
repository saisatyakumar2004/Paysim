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
    g.add_argument("--financial-defects", type=float, default=1.0,
                   help="multiplier on the fee / late / duplicate / split / aggregated settlement scenarios (0 = off)")
    g.add_argument("--start", type=_d, default=date(2026, 1, 1))

    r = sub.add_parser("run", help="run the pipeline for one day")
    r.add_argument("--date", type=_d, required=True)
    r.add_argument("--no-dbt", action="store_true")
    r.add_argument("--source", choices=["files", "api"], default="files", help="gateway source (api = local HTTP API)")
    r.add_argument("--api-url", default=None)

    sv = sub.add_parser("serve-api", help="serve the seeded gateway data over HTTP (needs the [api] extra)")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8765)
    sv.add_argument("--fail-first", type=int, default=0, help="answer the first N requests with HTTP 503")
    sv.add_argument("--delay", type=float, default=0.0, help="delay every response (seconds)")

    b = sub.add_parser("backfill", help="run the pipeline for a date range (sequentially)")
    b.add_argument("--start", type=_d, required=True)
    b.add_argument("--end", type=_d, required=True)
    b.add_argument("--no-dbt", action="store_true")
    b.add_argument("--prefect", action="store_true", help="run via the Prefect flow")

    rp = sub.add_parser("replay", help="re-evaluate quarantined rows against current rules/reference data")
    rp.add_argument("--dataset", required=True, choices=["gateway", "orders", "settlements"])
    rp.add_argument("--date", type=_d, required=True)
    rp.add_argument("--no-dbt", action="store_true")

    rc = sub.add_parser("reconcile", help="run reconciliation on its own (incremental by default)")
    rc.add_argument("--full", action="store_true", help="recompute every item from scratch (recovery)")
    vr = sub.add_parser("verify-recon", help="check incremental state against a from-scratch rebuild")
    vr.add_argument("--reference", action="store_true", help="also compare with the independent dbt reference model")

    mf = sub.add_parser("money-flow", help="where did the money go missing? headline, waterfall and per-chain investigation")
    mf.add_argument("--rebuild", action="store_true", help="rebuild gold.fact_money_flow first")
    mf.add_argument("--flow", default=None, help="investigate one chain, e.g. TXN:TXN1234567890")

    ex = sub.add_parser("explain", help="explain a reconciliation item: evidence, audit history, source records")
    ex.add_argument("item_id", help="e.g. TXN:TXN1234567890, STL:STL000000012, ORD:ORD00000012")

    bi = sub.add_parser("export-bi", help="export a Power BI-ready star schema (CSV + Parquet)")
    bi.add_argument("--out", default="exports/powerbi")
    bi.add_argument("--format", choices=["csv", "parquet", "both"], default="both")

    sub.add_parser("evaluate", help="score pipeline output against the generator's ground truth")
    sub.add_parser("anomalies", help="train + score the merchant-day anomaly model on trusted data")
    sub.add_parser("reset", help="delete the warehouse and bronze layers (keeps source data)")

    from .paysim import cli as paysim_cli
    paysim_cli.register(sub)

    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("payreconcile").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    if args.cmd == "paysim":
        paysim_cli.dispatch(args)
    elif args.cmd == "generate":
        from .generate import GenConfig, generate
        s = generate(GenConfig(days=args.days, orders_per_day=args.orders_per_day, n_merchants=args.merchants,
                               seed=args.seed, bad_rate=args.bad_rate, financial_defects=args.financial_defects,
                               start=args.start))
        print(json.dumps(s, indent=2, default=str))
    elif args.cmd == "run":
        from .pipeline import run_day
        client = None
        if args.source == "api":
            from .ingest import HttpGatewayClient
            client = HttpGatewayClient(args.api_url)
        print(_fmt(run_day(args.date, run_dbt=not args.no_dbt, gateway_client=client)))
    elif args.cmd == "serve-api":
        from .gateway_api import serve
        serve(args.host, args.port, args.fail_first, args.delay)
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
    elif args.cmd == "replay":
        from .replay import replay
        print(json.dumps(replay(args.dataset, args.date, run_dbt=not args.no_dbt), indent=2))
    elif args.cmd == "reconcile":
        from . import db, reconcile
        con = db.connect()
        as_of = con.execute("SELECT coalesce(max(as_of_date), max(run_date)) FROM (SELECT as_of_date, NULL AS run_date "
                            "FROM ops.recon_state UNION ALL SELECT NULL, max(run_date) FROM ops.run_log)").fetchone()[0]
        print(json.dumps(reconcile.reconcile(con, as_of, "cli", full=args.full).__dict__, indent=2, default=str))
        from . import moneyflow
        print(json.dumps(moneyflow.build(con, "cli"), default=str))
        con.close()
    elif args.cmd == "money-flow":
        from . import db, moneyflow
        con = db.connect() if args.rebuild else db.connect(read_only=True)
        if args.rebuild:
            print(json.dumps(moneyflow.build(con, "cli"), default=str))
        if args.flow:
            inv = moneyflow.investigate(con, args.flow)
            print("no such chain" if inv is None else json.dumps(
                {"flow": inv["flow"], "stages": inv["stages"], "evidence": inv["evidence"].to_dict("records")}, indent=2,
                default=str))
        else:
            s = moneyflow.summary(con)
            print(moneyflow.headline(con))
            print(json.dumps({"waterfall": s["waterfall"], "earliest_open_breakpoint": s["earliest_open_breakpoint"],
                              "by_stage": s["by_stage"].to_dict("records"), "invariants": moneyflow.check_invariants(con)},
                             indent=2, default=str))
        con.close()
    elif args.cmd == "verify-recon":
        from . import db, reconcile, transform
        con = db.connect()
        out = {"vs_full_recompute": reconcile.verify(con)}
        if args.reference:
            as_of = con.execute("SELECT as_of_date FROM ops.recon_state WHERE id = 1").fetchone()[0]
            con.close()
            res = transform.build(as_of, reference=True)
            con = db.connect()
            out["reference_build"] = {"success": res.success, "failures": res.failures}
            out["vs_dbt_reference"] = reconcile.compare_with_reference(con)
        con.close()
        print(json.dumps(out, indent=2, default=str))
    elif args.cmd == "export-bi":
        from pathlib import Path

        from .bi_export import export
        fmts = ("csv", "parquet") if args.format == "both" else (args.format,)
        m = export(Path(args.out), fmts)
        print(json.dumps({"out": args.out, "as_of_date": m["as_of_date"],
                          "tables": {k: v["rows"] for k, v in m["tables"].items()}}, indent=2))
    elif args.cmd == "explain":
        from . import db, moneyflow
        from .explain import explain
        con = db.connect(read_only=True)
        out = explain(con, args.item_id)
        inv = moneyflow.investigate(con, args.item_id)          # item ids and chain ids share one namespace (TXN:/STL:/ORD:)
        if inv is not None:
            out["money_flow"] = {"flow": inv["flow"], "stages": inv["stages"], "evidence": inv["evidence"].to_dict("records")}
        print(json.dumps(out, indent=2, default=str))
        con.close()
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
