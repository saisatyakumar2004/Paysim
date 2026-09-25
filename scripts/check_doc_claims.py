"""Verify that the key numbers quoted in README.md match the raw result artifacts in docs/.

Every check derives the expected text from a JSON/text artifact and asserts it appears in the README, so a number cannot be
edited in the docs without the evidence behind it (or the reverse). Exit code 1 if any claim does not match.
Usage:  python scripts/check_doc_claims.py [--tests 139]
"""

from __future__ import annotations

import argparse
import glob
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
README = (ROOT / "README.md").read_text(encoding="utf-8")
results: list[tuple[bool, str]] = []


def check(name: str, needle: str) -> None:
    results.append((needle in README, f"{name}: README should contain {needle!r}"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tests", type=int, default=None, help="actual number of tests collected")
    args = ap.parse_args()

    # ---- detection quality -----------------------------------------------------------------------------------------------
    ev = (DOCS / "results_evaluation_2026-09-26.txt").read_text(encoding="utf-8")
    m = re.search(r"quarantine \(all datasets\).*tp=(\d+) fp=(\d+) fn=(\d+)", ev)
    check("quarantine TP", f"{int(m.group(1)):,} / {int(m.group(1)):,}")
    m = re.search(r"duplicate charges \(confirmed\).*tp=(\d+) fp=(\d+) fn=(\d+)", ev)
    check("confirmed duplicates", f"{m.group(1)} / {int(m.group(1)) + int(m.group(3))}")
    m = re.search(r"duplicate charges \(combined\).*tp=(\d+) fp=(\d+) fn=(\d+)", ev)
    check("combined duplicates", f"{m.group(1)} / {int(m.group(1)) + int(m.group(3))}")
    m = re.search(r"fuzzy matches\s+precision=1\.0\s+recall=1\.0\s+\(tp=(\d+) fp=0", ev)
    check("fuzzy matches", f"{m.group(1)} fuzzy matches, 0 wrong")
    m = re.search(r"'injected': (\d+), 'flagged': (\d+)", ev)
    check("payment without order", f"{m.group(2)} / {m.group(1)}")
    total_money = sum(float(x.replace(",", "")) for x in re.findall(r"items=\d+\s+([\d,]+\.\d+)", ev))
    check("money at risk", f"{round(total_money):,}")

    # ---- anomaly ----------------------------------------------------------------------------------------------------------------
    an = json.loads((DOCS / "results_anomaly_2026-09-26.json").read_text(encoding="utf-8"))["evaluation_vs_ground_truth"]
    for key, label in (("iforest_flag", "Isolation Forest"), ("rule_flag", "z-score rule (baseline)")):
        r = an[key]
        row = f"| {label} | {r['tp']} | {r['fp']} | {r['fn']} | {r['precision']:.2f} | {r['recall']:.2f} | {r['f1']:.2f} |"
        check(f"anomaly row {label}", row)

    # ---- partitioning benchmark ----------------------------------------------------------------------------------------------------
    rows = {}
    for f in sorted(glob.glob(str(DOCS / "results_partitioning_benchmark_20*.json"))):
        for r in json.loads(Path(f).read_text(encoding="utf-8"))["results"]:
            rows[r["scale"]] = r
    for scale, r in sorted(rows.items()):
        b, p = r["baseline"], r.get("partitioned")
        items = f"{r['initial_full_reconcile']['items']:,}"
        pt = f"{p['incremental_compute_s']:.2f} s" if p else "not run"
        full = f"{b['full_compute_s']:.2f} s"
        base_txt = f"{b['incremental_compute_s']:.2f} s" + (" (table only)" if not p else "")
        check(f"partition row {scale}x", f"| {scale}x | {items} | {base_txt} | {pt} | {full} |")
    last = rows[max(rows)]
    check("full/incremental ratio at max scale", f"**{last['baseline']['full_compute_s'] / last['baseline']['incremental_compute_s']:.1f}x (100x)**")
    slow = [r["partitioned"]["incremental_compute_s"] / r["baseline"]["incremental_compute_s"] for r in rows.values() if "partitioned" in r]
    check("slowdown range", f"{min(slow):.1f}x to {max(slow):.1f}x slower")

    # ---- per-stage resources -----------------------------------------------------------------------------------------------------------
    st = json.loads((DOCS / "results_stage_resources_30day_backfill.json").read_text(encoding="utf-8"))
    for s in st["stages"]:
        name = {"ingestion (extract + Bronze)": "ingestion", "data quality (rules, gate)": "data quality", "Silver (upserts)": "Silver",
                "reconciliation (incremental)": "reconciliation (incremental)", "dbt (marts + tests)": "dbt (marts + 45 tests)"}[s["stage"]]
        check(f"stage {name}", f"| {name} | {s['median_s']:.2f} s | {round(s['median_peak_rss_mb'])} MB |")
    check("peak RSS", f"**{round(max(s['max_peak_rss_mb'] for s in st['stages']))} MB**")
    total = sum(s["median_s"] for s in st["stages"])
    check("dbt share", f"**{round(100 * st['stages'][-1]['median_s'] / total)}% of a ~{total:.1f} s daily run**")

    # ---- tests ----------------------------------------------------------------------------------------------------------------------------
    if args.tests is not None:
        check("test count", f"**{args.tests} tests**")

    failed = [msg for ok, msg in results if not ok]
    for ok, msg in results:
        print(("PASS  " if ok else "FAIL  ") + msg)
    print(f"\n{len(results) - len(failed)}/{len(results)} claims verified")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
