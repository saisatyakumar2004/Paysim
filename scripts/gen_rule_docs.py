"""Generate docs/dq_rules.md from the YAML contracts, so the documentation cannot drift from the code."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from payreconcile import config, dq  # noqa: E402

HEADER = """# Data-quality rule catalogue

_Generated from `contracts/*.yml` by `scripts/gen_rule_docs.py`. Do not edit by hand._

**Severity** CRITICAL / ERROR / WARN (score weights 5 / 3 / 1).
**Action** `quarantine` = row is removed from the load and written to `quarantine.rows`; `repair` = value is fixed in Silver and
counted; `flag` = row is kept and the violation is recorded; `reject_batch` = the whole file is rejected.
**Status** PASS (no violations) / WARNING (violations <= fail threshold) / FAIL (above it: default {fail:.0%}, CRITICAL {cfail:.0%}).

Schema rules (`*-SCH-01/02/03`) and batch rules (`*-VOL-01`, `*-FRS-01`, `*-DST-01`) are applied to every dataset by the engine
and are listed at the bottom.
"""

BATCH = """
## Engine-level rules

| Rule | Severity | Expectation | Detection | Action |
|---|---|---|---|---|
| `<DS>-SCH-01` | CRITICAL | every required column is present (after alias resolution) | set difference against the contract | reject_batch |
| `<DS>-SCH-02` | WARN | no unexpected columns (schema drift) | set difference | flag, column dropped |
| `<DS>-SCH-03` | WARN | no renamed columns | alias map in the contract | flag, column renamed |
| `GW-VOL-01`, `ST-VOL-01` | ERROR | row count within +-{vtol:.0%} of the trailing median; a drop of {vdrop:.0%}+ is FAIL | compare with the last 7 loaded runs | flag / halt |
| `GW-FRS-01` | ERROR | newest gateway record is not older than {fw}h (FAIL at {ff}h) | `max(created_at)` vs run date | flag / halt |
| `GW-DST-01`, `ST-DST-01` | ERROR | batch mean amount within +-{dtol:.0%} of the trailing median; {dfail:g}x or more (either way) is FAIL | catches unit changes such as rupees -> paise | flag / halt |
"""


def main() -> None:
    s = config.settings()["quality"]
    out = [HEADER.format(fail=s["default_fail_at"], cfail=s["critical_fail_at"])]
    for name in ("gateway", "settlements", "orders", "merchants"):
        c = dq.load_contract(name)
        out.append(f"\n## {name}\n\n{c['description']}. Dataset weight in the overall score: **{c['weight']}**. "
                   f"Natural key: `{c['natural_key']}`.\n")
        out.append("| Rule | Severity | Column | Expectation | Detection | Action |\n|---|---|---|---|---|---|")
        for r in c["rules"]:
            action = r.get("action", "quarantine" if r["severity"] in dq.QUARANTINE_SEVERITIES else "flag")
            if r["type"] == "expr":
                detect = f"`{r['violation'].strip()}`"
            elif r["type"] == "accepted":
                detect = f"value not in {r['values']}"
            elif r["type"] == "fk":
                detect = "value not found in the merchant master"
            elif r["type"] == "unique":
                detect = "second and later occurrences of the key in the batch"
            else:
                detect = "NULL or blank"
            out.append(f"| `{r['id']}` | {r['severity']} | {r.get('column', '')} | {r['desc']} | {detect} | {action} |")
    v, f, d = s["volume"], s["freshness"], s["distribution"]
    out.append(BATCH.format(vtol=v["tolerance"], vdrop=v["fail_drop"], fw=f["warn_hours"], ff=f["fail_hours"],
                            dtol=d["tolerance"], dfail=d["fail_ratio"]))
    (ROOT / "docs" / "dq_rules.md").write_text("\n".join(out) + "\n", encoding="utf-8")
    print("wrote docs/dq_rules.md")


if __name__ == "__main__":
    main()
