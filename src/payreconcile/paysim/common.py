"""Paths, settings, the source schema and a DuckDB connection for the PaySim pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .. import config

# The eleven source columns, in file order, with the types the source is read as.
SOURCE_COLUMNS: dict[str, str] = {
    "step": "BIGINT", "type": "VARCHAR", "amount": "DOUBLE", "nameOrig": "VARCHAR", "oldbalanceOrg": "DOUBLE",
    "newbalanceOrig": "DOUBLE", "nameDest": "VARCHAR", "oldbalanceDest": "DOUBLE", "newbalanceDest": "DOUBLE",
    "isFraud": "BIGINT", "isFlaggedFraud": "BIGINT",
}
DATASETS = ("dev", "full")


def cfg() -> dict:
    return config.settings()["paysim"]


def source_csv() -> Path:
    p = Path(os.environ.get("PAYSIM_SOURCE") or cfg()["source_file"])
    return p if p.is_absolute() else config.ROOT / p


def raw_dev_path(rows: int | None = None) -> Path:
    """The development sample (250k rows by default); other sizes (for scaling runs) get their own file."""
    if rows is None or rows == int(cfg()["dev_rows"]):
        return config.data_dir() / "raw" / "paysim_dev_250k.parquet"
    return config.data_dir() / "raw" / f"paysim_sample_{rows}.parquet"


def dataset_dir(name: str) -> Path:
    return config.data_dir() / "paysim" / name


def exports_dir(name: str) -> Path:
    """exports/paysim/<dataset> in the repository; under the redirected data directory when PAYRECON_DATA is set (tests)."""
    base = config.data_dir() / "exports" if "PAYRECON_DATA" in os.environ else config.ROOT / "exports"
    return base / "paysim" / name


@dataclass(frozen=True)
class Dataset:
    """A named dataset and where its immutable input lives (a CSV for `full`, the sample Parquet for `dev`)."""
    name: str
    source: Path

    @property
    def dir(self) -> Path:
        return dataset_dir(self.name)

    @property
    def bronze(self) -> Path:
        return self.dir / "bronze.parquet"

    @property
    def db(self) -> Path:
        return self.dir / "paysim.duckdb"

    @property
    def is_csv(self) -> bool:
        return self.source.suffix.lower() in (".csv", ".gz", ".txt")

    def read_source_sql(self, all_varchar: bool = False) -> str:
        """SQL relation that reads the immutable source with the declared schema (original column names and types)."""
        p = self.source.as_posix().replace("'", "''")
        if self.is_csv:
            cols = ", ".join(f"'{c}': '{'VARCHAR' if all_varchar else t}'" for c, t in SOURCE_COLUMNS.items())
            return f"read_csv('{p}', header = true, columns = {{{cols}}})"
        return f"read_parquet('{p}')"


def dataset(name: str, source: str | Path | None = None) -> Dataset:
    if source is not None:
        return Dataset(name, Path(source))
    if name == "full":
        return Dataset(name, source_csv())
    if name == "dev":
        return Dataset(name, raw_dev_path())
    if name.startswith("sample_"):
        return Dataset(name, raw_dev_path(int(name.split("_", 1)[1])))
    raise ValueError(f"unknown dataset {name!r}: use 'dev', 'full', 'sample_<rows>' or pass a source")


def connect(ds: Dataset, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    ds.dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(ds.db), read_only=read_only)
    c = cfg()
    con.execute(f"SET memory_limit='{c['memory_limit']}'")
    con.execute(f"SET threads={int(c['threads'])}")
    tmp = (ds.dir / "tmp").as_posix()
    con.execute(f"SET temp_directory='{tmp}'")
    return con


def connect_memory(tmp: Path | None = None) -> duckdb.DuckDBPyConnection:
    """In-memory connection with the same limits (profiling and sampling need no database file)."""
    con = duckdb.connect()
    c = cfg()
    con.execute(f"SET memory_limit='{c['memory_limit']}'")
    con.execute(f"SET threads={int(c['threads'])}")
    if tmp is not None:
        tmp.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{tmp.as_posix()}'")
    return con


def sql_list(values) -> str:
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)
