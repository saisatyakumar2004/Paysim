"""Generate docs/kpi_definitions.md from dashboard/kpis.py so the documentation cannot drift from the dashboard."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dashboard"))

from kpis import EXCEPTION_STATUSES, KPIS  # noqa: E402

HEADER = """# KPI definitions

_Generated from `dashboard/kpis.py` by `scripts/gen_kpi_docs.py`. Do not edit by hand._

Every metric in the Streamlit dashboard and in the Power BI export model is defined here once: what it is, how it is computed,
what it means for the business and which tables it comes from. All data is synthetic.

**Exception statuses** (need a human decision): {exc}.
`PENDING` (not yet due) and the two quality holds (`SOURCE_QUARANTINED`, `PAYMENT_QUARANTINED`, fixed upstream) are not exceptions.

**Not available in this dataset:** settlement lag by *settlement partner* (there is a single bank feed and no partner dimension).
"""


def main() -> None:
    out = [HEADER.format(exc=", ".join(f"`{s}`" for s in EXCEPTION_STATUSES))]
    for k in KPIS.values():
        out.append(f"\n## {k.name}\n\n"
                   f"* **Definition:** {k.definition}\n"
                   f"* **Formula:** `{k.formula}`\n"
                   f"* **Business meaning:** {k.meaning}\n"
                   f"* **Source tables:** {', '.join(f'`{s}`' for s in k.sources)}\n")
    (ROOT / "docs" / "kpi_definitions.md").write_text("\n".join(out) + "\n", encoding="utf-8")
    print("wrote docs/kpi_definitions.md")


if __name__ == "__main__":
    main()
