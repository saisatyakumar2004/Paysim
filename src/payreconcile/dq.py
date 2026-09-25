"""Data-quality engine: contracts (YAML) -> DuckDB SQL -> PASS / WARNING / FAIL + quarantine + score.

Rule model
----------
severity  CRITICAL | ERROR | WARN            weights 5 / 3 / 1 in the score
action    quarantine (default for ERROR/CRITICAL) | repair | flag (default for WARN)
status    PASS    no violations
          WARNING violations <= fail_at (row-level problems, batch is still usable)
          FAIL    violations >  fail_at, or a required column is missing (batch is not trustworthy)
"""

from __future__ import annotations

import json
import string
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yaml

from . import config

QUARANTINE_SEVERITIES = {"CRITICAL", "ERROR"}
PREFIX = {"gateway": "GW", "settlements": "ST", "orders": "OR", "merchants": "MR"}


def _p(dataset: str) -> str:
    return PREFIX.get(dataset, dataset.upper()[:2])


@dataclass
class RuleResult:
    rule_id: str
    dataset: str
    severity: str
    description: str
    evaluated: int
    violations: int
    status: str
    action: str

    @property
    def ratio(self) -> float:
        return self.violations / self.evaluated if self.evaluated else 0.0


@dataclass
class DQResult:
    dataset: str
    weight: float
    rows_in: int
    rule_results: list[RuleResult] = field(default_factory=list)
    good: pd.DataFrame | None = None
    quarantined: pd.DataFrame | None = None      # columns: natural_key, order_id, rule_ids, raw_json
    schema_failed: bool = False
    aliased: dict[str, str] = field(default_factory=dict)
    unexpected: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        statuses = {r.status for r in self.rule_results}
        if "FAIL" in statuses:
            return "FAIL"
        return "WARNING" if "WARNING" in statuses else "PASS"

    @property
    def score(self) -> float:
        w = config.settings()["quality"]["severity_weights"]
        total = sum(w[r.severity] for r in self.rule_results)
        if not total:
            return 1.0
        return sum(w[r.severity] * (1 - r.ratio) for r in self.rule_results) / total

    @property
    def rows_quarantined(self) -> int:
        return 0 if self.quarantined is None else len(self.quarantined)


def load_contract(dataset: str, contracts_dir: Path | None = None) -> dict:
    path = (contracts_dir or config.CONTRACTS_DIR) / f"{dataset}.yml"
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _status(rule: dict, evaluated: int, violations: int) -> str:
    q = config.settings()["quality"]
    if violations == 0:
        return "PASS"
    fail_default = q["critical_fail_at"] if rule["severity"] == "CRITICAL" else q["default_fail_at"]
    ratio = violations / evaluated if evaluated else 1.0
    if ratio > rule.get("fail_at", fail_default):
        return "FAIL"
    return "WARNING" if ratio > rule.get("warn_at", q["default_warn_at"]) else "PASS"


def _violation_sql(rule: dict, ctx: dict) -> str:
    kind, col = rule["type"], rule.get("column")
    if kind == "not_null":
        return f"{col} IS NULL OR trim({col}) = ''"
    if kind == "unique":
        return (f"_rid IN (SELECT _rid FROM (SELECT _rid, row_number() OVER (PARTITION BY trim({col}) ORDER BY _rid) rn "
                f"FROM t WHERE {col} IS NOT NULL AND trim({col}) <> '') WHERE rn > 1)")
    if kind == "accepted":
        vals = ", ".join("'" + str(v).lower().replace("'", "''") + "'" for v in rule["values"])
        return f"{col} IS NOT NULL AND lower(trim({col})) NOT IN ({vals})"
    if kind == "fk":
        return f"{col} IS NOT NULL AND trim({col}) NOT IN ({rule['ref_sql']})"
    if kind == "expr":
        return string.Template(rule["violation"]).safe_substitute(ctx)
    raise ValueError(f"unknown rule type {kind!r}")


def evaluate(con, contract: dict, df: pd.DataFrame, run_date: date) -> DQResult:
    """Run schema checks and every rule of `contract` over a raw (all-string) batch."""
    ds = contract["dataset"]
    result = DQResult(dataset=ds, weight=contract.get("weight", 1), rows_in=len(df))
    required = contract["columns"]["required"]
    aliases = contract["columns"].get("aliases", {})

    df = df.copy()
    for old, new in aliases.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
            result.aliased[old] = new
    result.missing = [c for c in required if c not in df.columns]
    result.unexpected = [c for c in df.columns if c not in required and not c.startswith("_")]

    result.rule_results.append(RuleResult(
        f"{_p(ds)}-SCH-01", ds, "CRITICAL", "all required columns present", len(required), len(result.missing),
        "FAIL" if result.missing else "PASS", "reject_batch"))
    result.rule_results.append(RuleResult(
        f"{_p(ds)}-SCH-02", ds, "WARN", "no unexpected columns (schema drift)", len(df.columns),
        len(result.unexpected), "WARNING" if result.unexpected else "PASS", "flag"))
    result.rule_results.append(RuleResult(
        f"{_p(ds)}-SCH-03", ds, "WARN", "no renamed columns (resolved via alias)", len(required),
        len(result.aliased), "WARNING" if result.aliased else "PASS", "flag"))
    if result.missing:
        result.schema_failed = True
        result.good = df.iloc[0:0]
        result.quarantined = pd.DataFrame({"natural_key": [None] * len(df), "order_id": [None] * len(df),
                                           "rule_ids": "SCHEMA_REJECT", "raw_json": _raw(df)})
        return result

    df = df[[c for c in df.columns if c in required]].copy()
    df.insert(0, "_rid", range(len(df)))
    n = len(df)
    con.register("t", df)
    ctx = {"run_date": run_date.isoformat(), "run_date_next": (run_date + timedelta(days=1)).isoformat(),
           "run_date_grace": (run_date + timedelta(days=2)).isoformat()}   # +24h tolerance for events straddling midnight
    bad: dict[int, list[str]] = {}
    for rule in contract["rules"]:
        sev = rule["severity"]
        action = rule.get("action", "quarantine" if sev in QUARANTINE_SEVERITIES else "flag")
        rids = [r[0] for r in con.execute(
            f"SELECT _rid FROM t WHERE coalesce(({_violation_sql(rule, ctx)}), false)").fetchall()] if n else []
        result.rule_results.append(RuleResult(rule["id"], ds, sev, rule["desc"], n, len(rids),
                                              _status(rule, n, len(rids)), action))
        if action == "quarantine":
            for r in rids:
                bad.setdefault(r, []).append(rule["id"])
    con.unregister("t")

    key = contract.get("natural_key")
    if bad:
        bdf = df[df["_rid"].isin(bad)].copy()
        result.quarantined = pd.DataFrame({
            "natural_key": bdf[key].values if key in bdf else None,
            "order_id": bdf["order_id"].values if "order_id" in bdf else None,
            "rule_ids": [",".join(bad[r]) for r in bdf["_rid"]],
            "raw_json": _raw(bdf.drop(columns=["_rid"])),
        })
    else:
        result.quarantined = pd.DataFrame(columns=["natural_key", "order_id", "rule_ids", "raw_json"])
    result.good = df[~df["_rid"].isin(bad)].drop(columns=["_rid"]).reset_index(drop=True)
    return result


def _raw(df: pd.DataFrame) -> list[str]:
    return [json.dumps(r, ensure_ascii=False, default=str) for r in df.to_dict("records")]


# ---- batch-level checks ---------------------------------------------------------------------------
def volume_check(dataset: str, rows: int, history: list[int]) -> RuleResult:
    q = config.settings()["quality"]["volume"]
    rid = f"{_p(dataset)}-VOL-01"
    if len(history) < q["min_history"]:
        return RuleResult(rid, dataset, "WARN", "row count within tolerance of trailing median", 1, 0, "PASS", "flag")
    median = float(pd.Series(history).median())
    drop = 1 - rows / median if median else 0.0
    deviation = abs(rows / median - 1) if median else 0.0
    status = "FAIL" if drop >= q["fail_drop"] else "WARNING" if deviation > q["tolerance"] else "PASS"
    return RuleResult(rid, dataset, "ERROR", f"row count within +-{q['tolerance']:.0%} of trailing median ({median:.0f})",
                      1, 0 if status == "PASS" else 1, status, "flag")


def freshness_check(dataset: str, newest, run_date: date) -> RuleResult:
    q = config.settings()["quality"]["freshness"]
    rid = f"{_p(dataset)}-FRS-01"
    if newest is None or pd.isna(newest):
        return RuleResult(rid, dataset, "ERROR", "newest record is fresh", 1, 1, "FAIL", "flag")
    end_of_run_day = pd.Timestamp(run_date + timedelta(days=1))
    age_h = (end_of_run_day - pd.Timestamp(newest)).total_seconds() / 3600 - 24
    status = "FAIL" if age_h > q["fail_hours"] else "WARNING" if age_h > q["warn_hours"] else "PASS"
    return RuleResult(rid, dataset, "ERROR", f"newest record is < {q['warn_hours']}h old", 1,
                      0 if status == "PASS" else 1, status, "flag")


def distribution_check(dataset: str, metric: str, value: float | None, history: list[float]) -> RuleResult:
    """Catches silent unit/scale changes (e.g. rupees -> paise) that pass every row-level rule."""
    q = config.settings()["quality"]["distribution"]
    rid = f"{_p(dataset)}-DST-01"
    desc = f"batch {metric} within +-{q['tolerance']:.0%} of trailing median"
    if value is None or len(history) < q["min_history"]:
        return RuleResult(rid, dataset, "ERROR", desc, 1, 0, "PASS", "flag")
    median = float(pd.Series(history).median())
    ratio = value / median if median else 1.0
    status = ("FAIL" if ratio >= q["fail_ratio"] or ratio <= 1 / q["fail_ratio"]
              else "WARNING" if abs(ratio - 1) > q["tolerance"] else "PASS")
    return RuleResult(rid, dataset, "ERROR", f"{desc} (ratio {ratio:.2f})", 1, 0 if status == "PASS" else 1, status, "flag")


# ---- scoring / gate ---------------------------------------------------------------------------------
def overall_score(results: list[DQResult]) -> float:
    total_w = sum(r.weight for r in results)
    return sum(r.weight * r.score for r in results) / total_w if total_w else 1.0


def gate_decision(results: list[DQResult], extra: list[RuleResult] | None = None) -> tuple[str, float, list[str]]:
    """Return (gate, score, reasons). gate is PASS, WARN or HALT."""
    q = config.settings()["quality"]
    score = overall_score(results)
    reasons: list[str] = []
    for r in results:
        if r.status == "FAIL":
            failed = [x.rule_id for x in r.rule_results if x.status == "FAIL"]
            reasons.append(f"{r.dataset}: FAIL on {', '.join(failed)}")
    for x in extra or []:
        if x.status == "FAIL":
            reasons.append(f"{x.dataset}: FAIL on {x.rule_id}")
    if score < q["gate_halt_score"]:
        reasons.append(f"overall score {score:.3f} < {q['gate_halt_score']}")
    if reasons:
        return "HALT", score, reasons
    warned = any(r.status == "WARNING" for r in results) or any(x.status == "WARNING" for x in extra or [])
    if score < q["gate_pass_score"]:
        return "WARN", score, [f"overall score {score:.3f} < {q['gate_pass_score']}"]
    return ("WARN" if warned else "PASS"), score, []
