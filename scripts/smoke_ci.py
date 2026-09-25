"""Small deterministic smoke test for CI: generate -> pipeline (no dbt) -> score against ground truth -> verify state.

Fast (a few seconds), uses no large data, and fails loudly if data quality or reconciliation regress.
Usage:  python scripts/smoke_ci.py
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="payrecon_smoke_"))
    os.environ["PAYRECON_DATA"] = str(work)
    from payreconcile import config, db, pipeline, reconcile
    from payreconcile.evaluate import evaluate_all
    from payreconcile.generate import GenConfig, generate

    config.settings.cache_clear()
    generate(GenConfig(out=work, days=8, orders_per_day=200, n_merchants=30, seed=3))
    for i in range(8):
        s = pipeline.run_day(date(2026, 1, 1) + timedelta(days=i), run_dbt=False)
        assert s.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS"), (s.run_date, s.status, s.error)
    r = evaluate_all()
    checks = {
        "quarantine recall >= 0.99": r["dq_quarantine"]["recall"] >= 0.99,
        "quarantine precision >= 0.97": r["dq_quarantine"]["precision"] >= 0.97,
        "reconciliation accuracy >= 0.97": r["recon_status_accuracy"] >= 0.97,
        "no wrong fuzzy match": r["fuzzy_match"]["fp"] == 0,
        "duplicate precision == 1": r["duplicate_charges_combined"]["precision"] in (1.0, None),
    }
    con = db.connect()
    checks["incremental state equals full rebuild"] = reconcile.verify(con)["ok"]
    con.close()
    for name, ok in checks.items():
        print(("PASS  " if ok else "FAIL  ") + name)
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
