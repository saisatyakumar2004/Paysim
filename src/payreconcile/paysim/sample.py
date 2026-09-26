"""Reproducible development sample of the full file.

Strategy (documented in the metadata JSON next to the Parquet):
  * strata = (transaction type, isFraud); rows are ranked inside each stratum by hash(source_row_number, seed), which is
    deterministic, and the first `quota` are kept;
  * FRAUD strata keep `dev_fraud_fraction` of their rows (a sample of 250k rows drawn proportionally would contain only
    a few hundred fraud rows); the remaining quota is spread over the NON-fraud strata proportionally to their size
    (largest-remainder rounding, so the sample has exactly `dev_rows` rows);
  * every row keeps its `source_row_number` (its position in the source file), so it is traceable and stays in time order.

The sample therefore OVER-REPRESENTS fraud relative to the source. It is for development and tests only; rates measured on
it are not population rates.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

from .common import SOURCE_COLUMNS, Dataset, cfg, connect_memory, dataset, raw_dev_path


def _quotas(strata: list[tuple], dev_rows: int, fraud_fraction: float) -> dict[tuple, int]:
    """strata: (type, is_fraud, count). Returns rows to keep per (type, is_fraud)."""
    q: dict[tuple, int] = {}
    fraud_total = 0
    for t, f, n in strata:
        if f == 1:
            q[(t, f)] = min(n, max(1, round(n * fraud_fraction))) if n else 0
            fraud_total += q[(t, f)]
    rest = max(0, dev_rows - fraud_total)
    clean = [(t, n) for t, f, n in strata if f != 1]
    total = sum(n for _, n in clean) or 1
    exact = {t: rest * n / total for t, n in clean}
    base = {t: min(int(v), dict(clean)[t]) for t, v in exact.items()}
    left = min(rest, sum(dict(clean).values())) - sum(base.values())
    for t in sorted(exact, key=lambda k: exact[k] - int(exact[k]), reverse=True):
        if left <= 0:
            break
        if base[t] < dict(clean)[t]:
            base[t] += 1
            left -= 1
    for t, v in base.items():
        q[(t, 0)] = v
    return q


def make_dev_sample(src: Dataset | None = None, out_path=None, rows: int | None = None) -> dict:
    src = src or dataset("full")
    if not src.source.exists():
        raise FileNotFoundError(f"source not found: {src.source.name} (set PAYSIM_SOURCE or paysim.source_file)")
    c = cfg()
    out_path = out_path or raw_dev_path(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    seed, dev_rows = int(c["seed"]), int(rows or c["dev_rows"])
    con = connect_memory(out_path.parent / "tmp")
    try:
        cols = ", ".join(f'"{k}"' for k in SOURCE_COLUMNS)
        con.execute(f"CREATE TEMP TABLE src AS SELECT row_number() OVER () AS source_row_number, {cols} FROM {src.read_source_sql()}")
        n_src = con.execute("SELECT count(*) FROM src").fetchone()[0]
        strata = con.execute('SELECT type, "isFraud", count(*) FROM src GROUP BY 1, 2 ORDER BY 1, 2').fetchall()
        quotas = _quotas([(t, int(f), int(n)) for t, f, n in strata], dev_rows, float(c["dev_fraud_fraction"]))
        con.execute("CREATE TEMP TABLE quota (type VARCHAR, is_fraud BIGINT, quota BIGINT)")
        con.executemany("INSERT INTO quota VALUES (?, ?, ?)", [(t, f, v) for (t, f), v in quotas.items()])
        con.execute(f"""
            COPY (
              SELECT s.* EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY type, "isFraud" ORDER BY hash(source_row_number, {seed})) AS rn FROM src) s
              JOIN quota q ON q.type = s.type AND q.is_fraud = s."isFraud" AND s.rn <= q.quota
              ORDER BY s.source_row_number)
            TO '{out_path.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
        rows, fraud, flagged = con.execute(
            f"SELECT count(*), sum(\"isFraud\"), sum(\"isFlaggedFraud\") FROM read_parquet('{out_path.as_posix()}')").fetchone()
        by_type = {t: {"rows": int(n), "fraud": int(f)} for t, n, f in con.execute(
            f"SELECT type, count(*), sum(\"isFraud\") FROM read_parquet('{out_path.as_posix()}') GROUP BY 1 ORDER BY 2 DESC").fetchall()}
        src_fraud = con.execute('SELECT sum("isFraud") FROM src').fetchone()[0]
    finally:
        con.close()
    meta = {
        "generated_at": datetime.now().isoformat(timespec="seconds"), "source_file": src.source.name, "source_rows": int(n_src),
        "source_fraud": int(src_fraud), "source_fraud_rate": src_fraud / n_src, "sample_rows": int(rows), "seed": seed,
        "strategy": ("stratified by (type, isFraud); rank by hash(source_row_number, seed); fraud strata keep "
                     f"{c['dev_fraud_fraction']:.0%} of their rows, non-fraud strata share the remaining quota proportionally"),
        "sample_fraud": int(fraud), "sample_fraud_rate": fraud / rows, "sample_flagged": int(flagged),
        "fraud_over_representation": (fraud / rows) / (src_fraud / n_src), "by_type": by_type,
        "quotas": {f"{t}|{f}": v for (t, f), v in sorted(quotas.items())},
        "sha256": hashlib.sha256(out_path.read_bytes()).hexdigest(),
        "note": "development and tests only; fraud is over-represented, so rates measured here are not population rates",
    }
    out_path.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta
