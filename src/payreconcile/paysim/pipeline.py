"""Run the PaySim pipeline for one dataset and record the wall time and peak memory of every stage.

    profile -> bronze (source -> Parquet) -> quality (Silver + DQ + balance consistency) -> gold -> ml -> export

The same code runs on the 250k development sample and on the full file; only the source differs.
"""

from __future__ import annotations

import json
import time
from datetime import datetime

from .. import resources
from . import bronze, export, gold, profile, quality
from .common import Dataset


def _stage(name: str, fn, rows: int | None = None):
    t0 = time.perf_counter()
    with resources.PeakMemory() as mem:
        out = fn()
    return out, {"stage": name, "seconds": round(time.perf_counter() - t0, 3),
                 "peak_rss_mb": None if mem.peak_mb is None else round(mem.peak_mb, 1), "rows": rows}


def run_pipeline(ds: Dataset, with_profile: bool = True, with_ml: bool = True, with_export: bool = True) -> dict:
    stages: list[dict] = []
    result: dict = {"dataset": ds.name, "source": ds.source.name, "started_at": datetime.now().isoformat(timespec="seconds")}
    if with_profile:
        prof, s = _stage("profile", lambda: profile.profile_source(ds))
        s["rows"] = prof["rows"]
        stages.append(s)
    b, s = _stage("bronze", lambda: bronze.ingest_bronze(ds))
    s["rows"] = b["rows"]
    stages.append(s)
    q, s = _stage("quality_and_silver", lambda: quality.run_quality(ds))
    s["rows"] = q["bronze_rows"]
    stages.append(s)
    g, s = _stage("gold", lambda: gold.build_gold(ds))
    s["rows"] = g["fact_transactions"]
    stages.append(s)
    result["rows"] = {"bronze": b["rows"], "silver": q["silver_rows"], "quarantined": q["quarantined_rows"], **{f"gold.{k}": v for k, v in g.items()}}
    result["conserved"] = q["conserved"]
    if with_ml:
        from . import ml  # lazy: sklearn is slow to import

        m, s = _stage("ml", lambda: ml.train_and_evaluate(ds))
        s["rows"] = m["run"]["train_rows_used"] + m["run"]["validation_rows"] + m["run"]["test_rows"]
        stages.append(s)
        result["ml_audit_flagged"] = m["audit_flagged"]
    if with_export:
        e, s = _stage("export", lambda: export.export_gold(ds))
        s["rows"] = sum(t["rows"] for t in e["tables"].values())
        stages.append(s)
    result["stages"] = stages
    result["total_seconds"] = round(sum(x["seconds"] for x in stages), 3)
    (ds.dir / "run_metrics.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result
