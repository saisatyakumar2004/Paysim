"""Bronze: the source fields, unchanged, as Parquet, plus lineage (source_row_number, ingestion_timestamp, source_file).

Business values are never altered here. If the source does not parse with the declared types the file is re-read as text
(nothing is dropped or coerced) and the quality layer reports the values that cannot be typed.
"""

from __future__ import annotations

import json
from datetime import datetime

from .common import SOURCE_COLUMNS, Dataset, connect_memory


class SchemaError(ValueError):
    """The source lacks required columns."""


def _source_columns(con, ds: Dataset) -> list[str]:
    p = ds.source.as_posix().replace("'", "''")
    rel = f"read_csv_auto('{p}')" if ds.is_csv else f"read_parquet('{p}')"
    return [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()]


def check_schema(cols: list[str]) -> dict:
    missing = [c for c in SOURCE_COLUMNS if c not in cols]
    return {"required": len(SOURCE_COLUMNS), "missing": missing, "extra": [c for c in cols if c not in SOURCE_COLUMNS]}


def ingest_bronze(ds: Dataset) -> dict:
    """Write ds.bronze from ds.source. Returns row count, schema check and how the source was read."""
    if not ds.source.exists():
        raise FileNotFoundError(f"source not found: {ds.source.name}")
    ds.dir.mkdir(parents=True, exist_ok=True)
    con = connect_memory(ds.dir / "tmp")
    try:
        schema = check_schema(_source_columns(con, ds))
        if schema["missing"]:
            raise SchemaError(f"source is missing required columns: {schema['missing']}")
        keep = ", ".join(f'"{c}"' for c in SOURCE_COLUMNS)
        has_row = (not ds.is_csv) and "source_row_number" in _source_columns(con, ds)
        row_expr = "source_row_number" if has_row else "row_number() OVER ()"
        stamp = datetime.now().isoformat(sep=" ", timespec="seconds")
        name = ds.source.name.replace("'", "''")
        typed = True
        for all_varchar in (False, True):
            try:
                con.execute(f"""
                    COPY (SELECT {row_expr}::BIGINT AS source_row_number, {keep},
                                 TIMESTAMP '{stamp}' AS ingestion_timestamp, '{name}' AS source_file
                          FROM {ds.read_source_sql(all_varchar)})
                    TO '{ds.bronze.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
                typed = not all_varchar
                break
            except Exception:                              # noqa: BLE001 - only the typed read may fail; retry as text
                if all_varchar or not ds.is_csv:
                    raise
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{ds.bronze.as_posix()}')").fetchone()[0]
    finally:
        con.close()
    meta = {"dataset": ds.name, "source_file": ds.source.name, "rows": int(rows), "schema": schema, "typed_read": typed,
            "ingestion_timestamp": stamp, "bronze_bytes": ds.bronze.stat().st_size}
    (ds.dir / "bronze.meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta
