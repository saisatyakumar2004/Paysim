"""Runs the dbt project (Gold layer: reconciliation, marts, tests) against the warehouse.

dbt runs as a subprocess: DuckDB allows one writer process, and isolating dbt keeps its connection/global
state away from the pipeline's own connection (and mirrors how it would run under a real orchestrator).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date

from . import config

_LAUNCH = "import sys; from dbt.cli.main import cli; cli(sys.argv[1:])"


@dataclass
class DbtResult:
    success: bool
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    failures: list[str] = field(default_factory=list)
    error: str | None = None


def _recon_vars() -> dict:
    r = config.settings()["reconciliation"]
    return {"amount_tolerance": r["amount_tolerance"], "sla_days": r["settlement_sla_days"],
            "max_edit_distance": r["fuzzy_max_edit_distance"],
            "dup_window_minutes": r["duplicate_charge_window_minutes"]}


def build(as_of: date, select: str | None = None) -> DbtResult:
    """`dbt build` = run models + run tests, in dependency order. A failing test skips downstream models."""
    target = config.data_dir() / "dbt_target"
    cmd = [sys.executable, "-c", _LAUNCH, "build", "--project-dir", str(config.DBT_DIR),
           "--profiles-dir", str(config.DBT_DIR), "--target-path", str(target),
           "--log-path", str(config.data_dir() / "dbt_logs"), "--quiet",
           "--vars", json.dumps({"as_of_date": as_of.isoformat(), **_recon_vars()})]
    if select:
        cmd += ["--select", select]
    run_results = target / "run_results.json"
    run_results.unlink(missing_ok=True)
    proc = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, "PAYRECON_DB": str(config.warehouse_path())})
    if not run_results.exists():
        return DbtResult(False, error=(proc.stderr or proc.stdout)[-800:])
    out = DbtResult(proc.returncode == 0)
    for r in json.loads(run_results.read_text(encoding="utf-8"))["results"]:
        status = r["status"].lower()
        if status in ("pass", "success"):
            out.passed += 1
        elif status == "skipped":
            out.skipped += 1
        else:
            out.failed += 1
            out.failures.append(f"{r['unique_id'].split('.')[-1]}: {status}")
    return out
