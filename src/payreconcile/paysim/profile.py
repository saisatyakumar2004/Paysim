"""Profile the immutable source (CSV or Parquet) with DuckDB and write a machine-readable JSON. Nothing is hard-coded."""

from __future__ import annotations

import json
from datetime import datetime

from .common import SOURCE_COLUMNS, Dataset, connect_memory


def _inferred(con, ds: Dataset) -> dict[str, str]:
    """Types DuckDB infers from the data itself (before the declared schema is applied)."""
    p = ds.source.as_posix().replace("'", "''")
    rel = f"read_csv_auto('{p}')" if ds.is_csv else f"read_parquet('{p}')"
    return {r[0]: r[1] for r in con.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()}


def profile_source(ds: Dataset, out_path=None) -> dict:
    con = connect_memory(ds.dir / "tmp")
    try:
        rel = ds.read_source_sql()
        try:
            con.execute(f"SELECT 1 FROM {rel} LIMIT 1")
        except Exception:                                # noqa: BLE001 - values that do not parse: profile as text
            rel = ds.read_source_sql(all_varchar=True)
        inferred = _inferred(con, ds)
        cols = list(SOURCE_COLUMNS)
        present = [c for c in cols if c in inferred]
        con.execute(f"CREATE OR REPLACE TEMP VIEW src AS SELECT * FROM {rel}")
        n = con.execute("SELECT count(*) FROM src").fetchone()[0]
        col_stats = {}
        for c in present:
            nulls, distinct, mn, mx = con.execute(
                f'SELECT count(*) FILTER (WHERE "{c}" IS NULL), count(DISTINCT "{c}"), min("{c}"), max("{c}") FROM src').fetchone()
            col_stats[c] = {"declared_type": SOURCE_COLUMNS[c], "inferred_type": inferred[c], "nulls": int(nulls),
                            "distinct": int(distinct), "min": None if mn is None else str(mn), "max": None if mx is None else str(mx)}
        prof: dict = {
            "generated_at": datetime.now().isoformat(timespec="seconds"), "dataset": ds.name,
            "source": {"file": ds.source.name, "extension": ds.source.suffix, "bytes": ds.source.stat().st_size if ds.source.exists() else None},
            "rows": int(n), "columns": col_stats, "missing_columns": [c for c in cols if c not in inferred],
            "extra_columns": [c for c in inferred if c not in cols],
        }
        if not prof["missing_columns"]:
            distinct_rows = con.execute(f"SELECT count(*) FROM (SELECT DISTINCT {', '.join(chr(34) + c + chr(34) for c in cols)} FROM src)").fetchone()[0]
            prof["exact_duplicate_rows"] = int(n - distinct_rows)
            prof["transaction_types"] = [
                {"type": t, "transactions": int(c), "share": c / n, "amount": a, "fraud": int(f), "flagged": int(g)}
                for t, c, a, f, g in con.execute(
                    'SELECT type, count(*), sum(try_cast(amount AS DOUBLE)), sum(try_cast("isFraud" AS BIGINT)), '
                    'sum(try_cast("isFlaggedFraud" AS BIGINT)) FROM src GROUP BY 1 ORDER BY 2 DESC').fetchall()]
            amt = con.execute("SELECT sum(a), avg(a), median(a), min(a), max(a), quantile_cont(a, 0.99) FROM "
                              "(SELECT try_cast(amount AS DOUBLE) a FROM src)").fetchone()
            prof["amount"] = dict(zip(["total", "average", "median", "minimum", "maximum", "p99"], amt, strict=True))
            fr, fl, tp = con.execute('SELECT sum(try_cast("isFraud" AS BIGINT)), sum(try_cast("isFlaggedFraud" AS BIGINT)), '
                                     'sum((try_cast("isFraud" AS BIGINT) = 1 AND try_cast("isFlaggedFraud" AS BIGINT) = 1)::INT) '
                                     'FROM src').fetchone()
            prof["fraud"] = {"fraud_transactions": int(fr or 0), "fraud_rate": (fr or 0) / n if n else None,
                             "flagged_transactions": int(fl or 0), "flagged_and_fraud": int(tp or 0),
                             "fraud_amount": con.execute('SELECT coalesce(sum(try_cast(amount AS DOUBLE)), 0) FROM src '
                                                         'WHERE try_cast("isFraud" AS BIGINT) = 1').fetchone()[0]}
            prof["accounts"] = {"origin_accounts": col_stats["nameOrig"]["distinct"],
                                "destination_accounts": col_stats["nameDest"]["distinct"],
                                "merchant_destinations": int(con.execute('SELECT count(DISTINCT "nameDest") FROM src WHERE "nameDest" LIKE \'M%\'').fetchone()[0])}
            prof["step_range"] = {"min": col_stats["step"]["min"], "max": col_stats["step"]["max"]}
    finally:
        con.close()
    if out_path is None:
        out_path = ds.dir / "profile.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(prof, indent=2, default=str), encoding="utf-8")
    return prof
