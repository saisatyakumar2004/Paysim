"""Demo: what happens when a bad batch arrives, with and without the quality gate.

Scenario: on the last day the payment gateway silently switches to paise (amounts x100). Every row still
looks valid (numeric, positive, correct types), so row-level rules pass. Only the distribution check notices.

Three variants are run on copies of the warehouse (your real data is not touched):
  clean         the normal batch                         -> baseline
  ungated       poisoned batch, gate bypassed (naive)    -> bad rows reach Silver, features and the model
  gated         poisoned batch, gate enforced            -> batch halted, Silver/Gold/ML untouched

Usage:  python scripts/demo_poisoned_batch.py [--last-day 2026-01-30]
Requires a completed backfill (python -m payreconcile backfill ...).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from datetime import date
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def poison_gateway_day(source_dir: Path, day: date) -> int:
    n = 0
    for page in sorted((source_dir / "gateway" / day.isoformat()).glob("page_*.json")):
        payload = json.loads(page.read_text(encoding="utf-8"))
        for rec in payload["data"]:
            try:
                rec["amount"] = float(str(rec["amount"]).replace("₹", "").replace(",", "")) * 100
                n += 1
            except ValueError:
                pass
        page.write_text(json.dumps(payload), encoding="utf-8")
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--last-day", type=date.fromisoformat, default=date(2026, 1, 30))
    args = ap.parse_args()

    from payreconcile import anomaly, config, pipeline

    real = config.data_dir()
    tmp = Path(tempfile.mkdtemp(prefix="payreconcile_demo_"))
    results = {}
    try:
        for variant in ("clean", "ungated", "gated"):
            work = tmp / variant
            work.mkdir()
            shutil.copy(real / "warehouse.duckdb", work / "warehouse.duckdb")
            shutil.copytree(real / "source", work / "source")
            shutil.copytree(real / "truth", work / "truth")
            os.environ["PAYRECON_DATA"] = str(work)
            config.settings.cache_clear()
            if variant != "clean":
                poison_gateway_day(work / "source", args.last_day)
            # re-run the last day (idempotent) on top of the loaded history
            con = duckdb.connect(str(work / "warehouse.duckdb"))
            con.execute("DELETE FROM ops.run_log WHERE run_date = ?", [args.last_day])
            con.execute("DELETE FROM ops.dq_scores WHERE run_date = ?", [args.last_day])
            con.execute("DELETE FROM ops.batch_stats WHERE run_date = ?", [args.last_day])
            con.close()
            summary = pipeline.run_day(args.last_day, run_dbt=False, enforce_gate=(variant != "ungated"))
            model = anomaly.run_anomaly_model(work / "warehouse.duckdb", work / "truth", enforce_gate=True)
            con = duckdb.connect(str(work / "warehouse.duckdb"), read_only=True)
            silver_mean = con.execute(
                "SELECT round(avg(amount), 2) FROM silver.gateway_txns WHERE cast(created_at AS DATE) = ? "
                "AND status = 'captured' AND txn_type = 'payment'", [args.last_day]).fetchone()[0]
            flagged_last_day = con.execute(
                "SELECT count(*) FROM ml.anomaly_scores WHERE d = ? AND iforest_flag", [args.last_day]).fetchone()[0] \
                if model.get("status") == "OK" else None
            con.close()
            results[variant] = {"pipeline_status": summary.status, "gate": summary.gate,
                                "dq_score": round(summary.dq_score or 0, 4), "gate_reasons": summary.reasons,
                                "silver_mean_captured_amount_on_last_day": silver_mean,
                                "model_status": model.get("status"),
                                "merchants_flagged_on_last_day": flagged_last_day}
    finally:
        os.environ["PAYRECON_DATA"] = str(real)
        shutil.rmtree(tmp, ignore_errors=True)
    print(json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
