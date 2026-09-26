"""Power BI-ready export of the Gold (and data-quality) tables as Parquet and CSV, plus a manifest.

CSV is written only for tables up to `csv_row_limit` rows (default 1,000,000); larger tables are Parquet only, which Power BI reads
natively. The manifest records exactly what was written. No .pbix is produced and nothing here was opened in Power BI Desktop.
"""

from __future__ import annotations

import json
from datetime import datetime

from .common import Dataset, connect, exports_dir

TABLES = ["gold.fact_transactions", "gold.fact_fraud", "gold.transaction_type_summary", "gold.balance_quality",
          "gold.fraud_daily_summary", "gold.account_activity", "gold.fraud_account_summary", "gold.fraud_flag_performance",
          "gold.model_metrics", "gold.model_feature_importance", "gold.model_pr_curve", "gold.model_feature_audit",
          "silver.dq_results", "silver.dq_quarantine"]


def export_gold(ds: Dataset, csv_row_limit: int = 1_000_000, out=None) -> dict:
    out = out or exports_dir(ds.name)
    out.mkdir(parents=True, exist_ok=True)
    con = connect(ds, read_only=True)
    tables: dict = {}
    try:
        existing = {f"{s}.{t}" for s, t in con.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()}
        for full in TABLES:
            if full not in existing:
                continue
            name = full.split(".")[1]
            n = con.execute(f"SELECT count(*) FROM {full}").fetchone()[0]
            cols = [r[0] for r in con.execute(f"DESCRIBE {full}").fetchall()]
            con.execute(f"COPY (SELECT * FROM {full}) TO '{(out / (name + '.parquet')).as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
            entry = {"rows": int(n), "columns": cols, "parquet": f"{name}.parquet", "csv": None}
            if n <= csv_row_limit:
                con.execute(f"COPY (SELECT * FROM {full}) TO '{(out / (name + '.csv')).as_posix()}' (HEADER, DELIMITER ',')")
                entry["csv"] = f"{name}.csv"
            else:
                entry["csv_skipped"] = f"{n} rows exceed the CSV limit of {csv_row_limit}; use the Parquet file"
            tables[name] = entry
    finally:
        con.close()
    manifest = {"generated_at": datetime.now().isoformat(timespec="seconds"), "dataset": ds.name, "tables": tables,
                "note": "Power BI-ready files. No .pbix is produced; nothing was verified inside Power BI Desktop."}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
