"""`python -m payreconcile paysim <command>`: profile, sample, run, ml, export, benchmark."""

from __future__ import annotations

import json

from .common import dataset


def register(sub) -> None:
    ps = sub.add_parser("paysim", help="PaySim transaction analytics (DuckDB + Parquet); independent of the PayReconcile pipeline")
    cmds = ps.add_subparsers(dest="ps_cmd", required=True)

    def add(name, help_, dataset_default="dev", **extra):
        p = cmds.add_parser(name, help=help_)
        p.add_argument("--dataset", default=dataset_default, help="dev (250k sample), full (whole file) or sample_<rows>")
        return p

    add("profile", "profile the immutable source and write profile.json", "full")
    s = cmds.add_parser("sample", help="create the reproducible development sample from the full file")
    s.add_argument("--rows", type=int, default=None, help="sample size (default: paysim.dev_rows = 250000)")
    r = add("run", "profile -> Bronze -> quality/Silver -> Gold -> ML -> export, with per-stage time and memory")
    r.add_argument("--no-ml", action="store_true")
    r.add_argument("--no-export", action="store_true")
    r.add_argument("--no-profile", action="store_true")
    add("bronze", "source -> Bronze Parquet only")
    add("quality", "Bronze -> Silver, quarantine and DQ results")
    add("gold", "Silver -> Gold tables")
    add("ml", "train and evaluate the baseline models")
    e = add("export", "export Gold tables as Parquet and CSV (Power BI-ready)")
    e.add_argument("--csv-row-limit", type=int, default=1_000_000)
    b = add("benchmark", "CSV vs Parquet vs DuckDB table, dashboard query latency")
    b.add_argument("--repeats", type=int, default=3)
    b.add_argument("--pandas", action="store_true", help="also measure a pandas.read_csv baseline (loads the whole file into memory)")


def dispatch(args) -> None:
    from . import (  # lazy: sklearn is slow to import
        benchmark,
        bronze,
        export,
        gold,
        ml,
        pipeline,
        profile,
        quality,
        sample,
    )

    c = args.ps_cmd
    if c == "sample":
        out = sample.make_dev_sample(rows=args.rows)
    else:
        ds = dataset(args.dataset)
        if c == "profile":
            out = profile.profile_source(ds)
        elif c == "run":
            out = pipeline.run_pipeline(ds, with_profile=not args.no_profile, with_ml=not args.no_ml, with_export=not args.no_export)
        elif c == "bronze":
            out = bronze.ingest_bronze(ds)
        elif c == "quality":
            out = quality.run_quality(ds)
        elif c == "gold":
            out = gold.build_gold(ds)
        elif c == "ml":
            out = ml.train_and_evaluate(ds)
        elif c == "export":
            out = export.export_gold(ds, args.csv_row_limit)
        else:
            out = benchmark.run_benchmark(ds, args.repeats, args.pandas)
    print(json.dumps(out, indent=2, default=str))
