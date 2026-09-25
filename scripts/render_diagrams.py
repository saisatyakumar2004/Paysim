"""Render docs/images/architecture.svg (authored) and docs/images/dbt_lineage.svg (generated from the dbt manifest).

The lineage is NOT hand-drawn: it is read from dbt's manifest.json (produced by `dbt build`), so it always shows the real graph.
Usage:  python scripts/render_diagrams.py [--manifest path/to/manifest.json]
"""

from __future__ import annotations

import argparse
import json
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "images"

FONT = "font-family='Segoe UI, Helvetica, Arial, sans-serif'"


def box(x, y, w, h, title, lines=(), fill="#eef2f7", stroke="#5b6b7f", title_color="#1b2a3a", dashed=False):
    dash = " stroke-dasharray='6 4'" if dashed else ""
    s = [f"<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='8' fill='{fill}' stroke='{stroke}' stroke-width='1.5'{dash}/>",
         f"<text x='{x + 12}' y='{y + 22}' {FONT} font-size='14' font-weight='600' fill='{title_color}'>{escape(title)}</text>"]
    for i, ln in enumerate(lines):
        s.append(f"<text x='{x + 12}' y='{y + 42 + i * 16}' {FONT} font-size='11.5' fill='#3d4c5e'>{escape(ln)}</text>")
    return "\n".join(s)


def arrow(x1, y1, x2, y2, color="#5b6b7f", label=None, dashed=False):
    dash = " stroke-dasharray='5 4'" if dashed else ""
    s = [f"<line x1='{x1}' y1='{y1}' x2='{x2}' y2='{y2}' stroke='{color}' stroke-width='1.8'{dash} marker-end='url(#arr)'/>"]
    if label:
        s.append(f"<text x='{(x1 + x2) / 2}' y='{(y1 + y2) / 2 - 6}' {FONT} font-size='10.5' fill='{color}' text-anchor='middle'>{escape(label)}</text>")
    return "\n".join(s)


def architecture() -> str:
    parts = ["<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 1500 800' width='1500' height='800'>",
             "<defs><marker id='arr' markerWidth='10' markerHeight='8' refX='9' refY='4' orient='auto'>"
             "<path d='M0,0 L10,4 L0,8 z' fill='#5b6b7f'/></marker></defs>",
             "<rect width='1500' height='800' fill='#ffffff'/>",
             f"<text x='30' y='38' {FONT} font-size='22' font-weight='700' fill='#1b2a3a'>PayReconcile: architecture</text>",
             f"<text x='30' y='60' {FONT} font-size='12' fill='#5b6b7f'>Every box is implemented and runs locally on DuckDB "
             "(synthetic data). Dashed = export only / documented, not a deployed service.</text>"]
    # sources
    src = [("Payment gateway", ["JSON, paginated", "file stand-in or local HTTP API"]),
           ("Bank settlements", ["daily CSV, T+1 / T+2", "schema drift: renames, extras"]),
           ("Order system", ["SQLite change log", "watermark + overlap"]),
           ("Merchant master", ["weekly CSV snapshots", "-> SCD Type 2"])]
    for i, (t, body) in enumerate(src):
        parts.append(box(30, 100 + i * 110, 200, 92, t, body, fill="#f4f1ea", stroke="#8a7a55"))
    parts.append(box(290, 100, 190, 428, "Ingestion + Bronze", ["retry, backoff, timeout", "raw values as strings", "Parquet by ingest_date", "lineage columns", "idempotent overwrite"],
                     fill="#eaf3ea", stroke="#5f8a5f"))
    parts.append(box(540, 100, 220, 190, "Validation", ["schema contract (missing /", "extra / renamed columns)", "YAML rules -> SQL", "PASS / WARNING / FAIL"],
                     fill="#f8efe6", stroke="#b0763a"))
    parts.append(box(540, 318, 220, 100, "Quality gate", ["score + any FAIL", "HALT = load nothing"], fill="#fbe9e7", stroke="#c0503f"))
    parts.append(box(540, 448, 220, 80, "Quarantine", ["rejected rows + raw payload + rule ids"], fill="#fbe9e7", stroke="#c0503f"))
    parts.append(box(820, 100, 200, 190, "Silver (DuckDB)", ["typed, repaired, de-duplicated", "change-aware upserts", "change log", "SCD2 merchants"],
                     fill="#e8f0fa", stroke="#4a79b0"))
    parts.append(box(820, 318, 200, 210, "Quarantine replay", ["re-check against current", "rules and reference data", "RELEASED / STILL_INVALID /", "SUPERSEDED (append-only log)"],
                     fill="#fbe9e7", stroke="#c0503f"))
    parts.append(box(1080, 100, 230, 300, "Reconciliation engine", ["incremental: change-log scope", "exact + constrained fuzzy match", "ambiguity never guessed", "sticky matches", "duplicate: confirmed / suspected",
                                                              "match evidence table", "append-only audit trail", "one atomic transaction"], fill="#e8f0fa", stroke="#4a79b0"))
    parts.append(box(1080, 428, 230, 100, "dbt", ["star schema (SCD2 dim), marts", "45 tests + full-rebuild oracle"], fill="#e8f0fa", stroke="#4a79b0"))
    parts.append(box(1360, 100, 120, 120, "Streamlit", ["8 pages", "exceptions queue", "explain"], fill="#efe9f7", stroke="#7a5fa8"))
    parts.append(box(1360, 244, 120, 120, "Power BI", ["export layer only", "no .pbix file"], fill="#f7f7f7", stroke="#8a8a8a", dashed=True))
    parts.append(box(1360, 388, 120, 140, "Anomaly", ["merchant-day", "signals (not a", "fraud detector)"], fill="#efe9f7", stroke="#7a5fa8"))
    parts.append(box(30, 590, 1450, 80, "Monitoring: ops.* tables", ["run_log, task_log (stage time, peak RSS, rows in/out/rejected), dq_results, dq_scores, batch_stats, recon_runs, replay_log, model_metrics"],
                     fill="#f1f1f1", stroke="#7d7d7d"))
    parts.append(box(30, 690, 1450, 70, "Ground truth (synthetic only)", ["the generator records every injected defect, so detection precision/recall is measured, not asserted; "
                                                                       "it proves the logic is coherent, not that it works on real feeds"], fill="#fffbe6", stroke="#b8a53a"))
    # arrows
    for i in range(4):
        parts.append(arrow(230, 146 + i * 110, 290, 146 + i * 110))
    parts.append(arrow(480, 190, 540, 190))
    parts.append(arrow(650, 290, 650, 318))
    parts.append(arrow(650, 418, 650, 448, label="bad rows"))
    parts.append(arrow(760, 368, 820, 190, label="pass"))
    parts.append(arrow(760, 488, 820, 440, dashed=True))
    parts.append(arrow(1020, 190, 1080, 190))
    parts.append(arrow(1195, 400, 1195, 428))
    parts.append(arrow(1310, 160, 1360, 160))
    parts.append(arrow(1310, 300, 1360, 300, dashed=True))
    parts.append("<polyline points='1020,262 1050,262 1050,552 1420,552 1420,528' fill='none' stroke='#5b6b7f' stroke-width='1.8' "
                 "stroke-dasharray='5 4' marker-end='url(#arr)'/>")
    parts.append(f"<text x='1240' y='546' {FONT} font-size='10.5' fill='#5b6b7f' text-anchor='middle'>trusted Silver rows only</text>")
    parts.append(arrow(920, 318, 920, 292))
    parts.append(f"<text x='930' y='308' {FONT} font-size='10.5' fill='#5b6b7f'>release</text>")
    parts.append("</svg>")
    return "\n".join(parts)


def lineage(manifest_path: Path) -> str:
    """Two panels from the manifest: the daily build (sources -> models) and the reference full-rebuild oracle."""
    m = json.loads(manifest_path.read_text(encoding="utf-8"))
    nodes: dict[str, dict] = {}
    for uid, n in {**m["nodes"], **m["sources"]}.items():
        if n["resource_type"] == "model":
            nodes[uid] = {"label": n["name"], "kind": "reference" if "reference" in n.get("tags", []) else "mart"}
        elif n["resource_type"] == "source":
            nodes[uid] = {"label": f"{n['source_name']}.{n['name']}", "kind": "source"}
    edges = [(p, c) for c, parents in m["parent_map"].items() if c in nodes for p in parents if p in nodes]
    w, h, gx, gy = 220, 28, 80, 12
    colors = {"source": ("#f4f1ea", "#8a7a55"), "reference": ("#f8efe6", "#b0763a"), "mart": ("#e8f0fa", "#4a79b0")}

    def panel(kind: str, title: str, y0: int) -> tuple[list[str], int, int]:
        models = {u for u, n in nodes.items() if n["kind"] == kind}
        used = set(models) | {p for p, c in edges if c in models}
        sub = [(p, c) for p, c in edges if c in models and p in used]
        depth: dict[str, int] = {}

        def d(u: str) -> int:
            if u not in depth:
                ps = [p for p, c in sub if c == u]
                depth[u] = 0 if not ps else 1 + max(d(p) for p in ps)
            return depth[u]

        for u in used:
            d(u)
        cols: dict[int, list[str]] = {}
        for u in sorted(used, key=lambda k: (depth[k], nodes[k]["label"])):
            cols.setdefault(depth[u], []).append(u)
        y_of: dict[str, float] = {u: i for i, u in enumerate(cols.get(0, []))}
        for c in sorted(cols):                       # barycentre ordering: place a node near the average of its parents
            if c == 0:
                continue
            cols[c].sort(key=lambda u: sum(y_of[p] for p, cc in sub if cc == u) / max(1, sum(1 for _, cc in sub if cc == u)))
            for i, u in enumerate(cols[c]):
                y_of[u] = i
        pos = {u: (40 + c * (w + gx), y0 + 40 + i * (h + gy)) for c, us in cols.items() for i, u in enumerate(us)}
        width = 80 + (max(cols) + 1) * (w + gx)
        height = 40 + max(len(v) for v in cols.values()) * (h + gy) + 20
        out = [f"<text x='40' y='{y0 + 20}' {FONT} font-size='14' font-weight='700' fill='#1b2a3a'>{escape(title)}</text>"]
        for p_, c_ in sub:
            (x1, y1), (x2, y2) = pos[p_], pos[c_]
            out.append(f"<path d='M{x1 + w},{y1 + h / 2} C{x1 + w + 40},{y1 + h / 2} {x2 - 40},{y2 + h / 2} {x2},{y2 + h / 2}' fill='none' "
                       f"stroke='#b5bfcc' stroke-width='1.2' marker-end='url(#arr)'/>")
        for u, (x, y) in pos.items():
            fill, stroke = colors[nodes[u]["kind"]]
            out.append(f"<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='6' fill='{fill}' stroke='{stroke}'/>"
                       f"<text x='{x + 10}' y='{y + 19}' {FONT} font-size='12' fill='#1b2a3a'>{escape(nodes[u]['label'])}</text>")
        return out, width, height

    top, w1, h1 = panel("mart", "Daily build: sources -> models (45 dbt tests run on these)", 70)
    bottom, w2, h2 = panel("reference", "Reference oracle: independent full-rebuild reconciliation (run on demand, excluded from the daily build)", 70 + h1 + 30)
    width, height = max(w1, w2, 900), 70 + h1 + 30 + h2 + 20
    return chr(10).join([
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {height}' width='{width}' height='{height}'>",
        "<defs><marker id='arr' markerWidth='8' markerHeight='6' refX='7' refY='3' orient='auto'><path d='M0,0 L8,3 L0,6 z' fill='#8a97a8'/></marker></defs>",
        f"<rect width='{width}' height='{height}' fill='#ffffff'/>",
        f"<text x='40' y='34' {FONT} font-size='20' font-weight='700' fill='#1b2a3a'>dbt lineage (generated from manifest.json)</text>",
        f"<text x='40' y='54' {FONT} font-size='12' fill='#5b6b7f'>grey = source tables (silver.*, quarantine.*, recon.* engine-owned), blue = models, orange = reference models</text>",
        *top, *bottom, "</svg>"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default=str(ROOT / "data" / "dbt_target" / "manifest.json"))
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "architecture.svg").write_text(architecture(), encoding="utf-8")
    print("wrote docs/images/architecture.svg")
    mp = Path(args.manifest)
    if mp.exists():
        (OUT / "dbt_lineage.svg").write_text(lineage(mp), encoding="utf-8")
        print("wrote docs/images/dbt_lineage.svg")
    else:
        print(f"manifest not found at {mp}: run a dbt build first; skipped the lineage diagram")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
