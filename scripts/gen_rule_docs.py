"""Generate docs/dq_rules.md from the YAML contracts and the quality engine, so the documentation cannot drift from the code.

Every rule is listed with: rule id, dataset, field, severity, expectation, implementation (the SQL fragment the engine runs),
and failure behaviour (what happens to the row and to the batch). A coverage matrix maps each quality dimension to its rules
and fails generation if a listed rule id does not exist.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from payreconcile import config, dq  # noqa: E402

DATASETS = ("gateway", "settlements", "orders", "merchants")
PREFIX = dq.PREFIX

HEADER = """# Data-quality rule catalogue

_Generated from `contracts/*.yml` and the quality engine by `scripts/gen_rule_docs.py`. Do not edit by hand._

**Severity** CRITICAL / ERROR / WARN (score weights 5 / 3 / 1).
**Status** per rule: PASS (no violations), WARNING (violations up to the fail threshold), FAIL (above it: default {fail:.0%} of rows,
CRITICAL rules {cfail:.0%}). **Any FAIL halts the batch at the quality gate**: nothing from it is loaded into Silver or Gold.
**Gate** overall = severity-weighted score across datasets; HALT below {halt}, WARN below {passs}.
**Actions** `quarantine` = the row is removed from the load and stored in `quarantine.rows` with its raw payload and rule ids;
`repair` = the value is fixed in Silver and counted; `flag` = the row is kept and the violation recorded; `reject_batch` = the
whole file is rejected.
"""

ENGINE = {
    # id-suffix: (severity, expectation, implementation, failure behaviour, applies to)
    "SCH-01": ("CRITICAL", "every required column is present (after alias resolution)",
               "set difference between the contract's required columns and the file's columns",
               "whole file rejected (`SCHEMA_REJECT` rows in quarantine); dataset FAIL; gate HALT", DATASETS),
    "SCH-02": ("WARN", "no unexpected columns (schema drift)", "file columns minus required columns",
               "extra columns dropped; WARNING", DATASETS),
    "SCH-03": ("WARN", "no renamed columns", "alias map in the contract (for example `txn_reference` -> `gateway_ref`)",
               "column renamed and processed; WARNING", DATASETS),
    "VOL-01": ("ERROR", "row count within tolerance of the trailing median", "compare with the median of the last 7 loaded runs",
               "WARNING outside +-{vtol:.0%}; FAIL (gate HALT) on a drop of {vdrop:.0%} or more", ("gateway", "settlements")),
    "FRS-01": ("ERROR", "newest record is fresh", "newest `created_at` (gateway) or `settlement_date` (bank) vs the run date",
               "WARNING above {fw}h old; FAIL (gate HALT) above {ff}h", ("gateway", "settlements")),
    "DST-01": ("ERROR", "batch mean amount within tolerance of the trailing median (catches unit changes, e.g. rupees -> paise)",
               "mean captured amount (gateway) / mean absolute gross amount (bank) vs the median of earlier loaded runs",
               "WARNING outside +-{dtol:.0%}; FAIL (gate HALT) at {dfail:g}x or more, either way", ("gateway", "settlements")),
}

COVERAGE = {
    "Schema": ["GW-SCH-01", "GW-SCH-02", "GW-SCH-03", "ST-SCH-01", "OR-SCH-01", "MR-SCH-01"],
    "Required fields": ["GW-CMP-01", "GW-CMP-02", "GW-CMP-03", "ST-CMP-01", "ST-CMP-02", "OR-CMP-01", "MR-CMP-01"],
    "Types": ["GW-VAL-01", "GW-VAL-02", "ST-VAL-01", "ST-VAL-02", "ST-VAL-03", "OR-VAL-01", "OR-VAL-03", "MR-VAL-01"],
    "Nulls": ["GW-CMP-01", "GW-CMP-02", "GW-CMP-03", "ST-CMP-01", "ST-CMP-02", "OR-CMP-01", "MR-CMP-01"],
    "Duplicates": ["GW-UNQ-01", "ST-UNQ-01", "MR-UNQ-01"],
    "Amount validity": ["GW-VAL-02", "GW-VAL-03", "ST-VAL-02", "ST-VAL-03", "ST-BUS-01", "OR-VAL-01", "MR-VAL-01"],
    "Currency": ["GW-VAL-07"],
    "Timestamp validity": ["GW-VAL-01", "ST-VAL-01", "OR-VAL-03"],
    "Future dates": ["GW-VAL-01", "ST-VAL-01"],
    "Merchant validity": ["GW-REF-01", "ST-REF-01", "OR-REF-01", "MR-VAL-02"],
    "Referential integrity": ["GW-REF-01", "ST-REF-01", "OR-REF-01"],
    "Volume": ["GW-VOL-01", "ST-VOL-01"],
    "Freshness": ["GW-FRS-01", "ST-FRS-01"],
    "Distribution anomalies": ["GW-DST-01", "ST-DST-01"],
    "Format consistency": ["GW-FIX-01", "GW-FIX-02"],
}


def implementation(rule: dict) -> str:
    ctx = {"run_date": "<run_date>", "run_date_next": "<run_date+1>", "run_date_grace": "<run_date+2>"}
    return "`" + " ".join(dq._violation_sql(rule, ctx).split()) + "`"


def behaviour(rule: dict, s: dict) -> str:
    sev = rule["severity"]
    action = rule.get("action", "quarantine" if sev in dq.QUARANTINE_SEVERITIES else "flag")
    base = {"quarantine": "row quarantined with its raw payload", "repair": "value repaired in Silver and counted",
            "flag": "row kept, violation recorded"}[action]
    fail_at = rule.get("fail_at", s["critical_fail_at"] if sev == "CRITICAL" else s["default_fail_at"])
    warn_at = rule.get("warn_at", s["default_warn_at"])
    return f"{base}; WARNING above {warn_at:.0%} of rows, FAIL (gate HALT) above {fail_at:.0%}"


def main() -> None:
    s = config.settings()["quality"]
    v, f, d = s["volume"], s["freshness"], s["distribution"]
    fmt = dict(vtol=v["tolerance"], vdrop=v["fail_drop"], fw=f["warn_hours"], ff=f["fail_hours"], dtol=d["tolerance"],
               dfail=d["fail_ratio"])
    out = [HEADER.format(fail=s["default_fail_at"], cfail=s["critical_fail_at"], halt=s["gate_halt_score"], passs=s["gate_pass_score"])]
    known: set[str] = set()
    for name in DATASETS:
        c = dq.load_contract(name)
        out.append(f"\n## {name}\n\n{c['description']}. Weight in the overall score: **{c['weight']}**. Natural key: `{c['natural_key']}`.\n")
        out.append("| Rule | Dataset | Field | Severity | Expectation | Implementation | Failure behaviour |\n|---|---|---|---|---|---|---|")
        for r in c["rules"]:
            known.add(r["id"])
            out.append(f"| `{r['id']}` | {name} | {r.get('column', '')} | {r['severity']} | {r['desc']} | {implementation(r)} | {behaviour(r, s)} |")
        for suffix, (sev, exp, impl, fail, applies) in ENGINE.items():
            if name in applies:
                rid = f"{PREFIX[name]}-{suffix}"
                known.add(rid)
                out.append(f"| `{rid}` | {name} | (batch) | {sev} | {exp} | {impl} | {fail.format(**fmt)} |")

    out.append("\n## Coverage by quality dimension\n\n| Dimension | Rules |\n|---|---|")
    for dim, ids in COVERAGE.items():
        missing = [i for i in ids if i not in known]
        if missing:
            raise SystemExit(f"coverage map lists rule ids that do not exist: {missing}")
        out.append(f"| {dim} | {', '.join(f'`{i}`' for i in ids)} |")
    out.append("\nReferential integrity is additionally enforced downstream by dbt `relationships` tests and the "
               "`assert_no_bad_rows_in_gold` test.\n")
    (ROOT / "docs" / "dq_rules.md").write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"wrote docs/dq_rules.md ({len(known)} rules)")


if __name__ == "__main__":
    main()
