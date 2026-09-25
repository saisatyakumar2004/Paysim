"""Extraction: sources -> raw (all-string) DataFrames -> Bronze Parquet.

Bronze is immutable raw data plus lineage columns. Nothing is cast, cleaned or dropped here.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from . import config

log = logging.getLogger("payreconcile.ingest")


class IngestError(RuntimeError):
    pass


def to_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Everything becomes a string (or None), exactly as received."""
    def conv(v):
        if v is None or (isinstance(v, float) and v != v):
            return None
        return str(v)
    out = df.astype(object).copy()
    for c in out.columns:
        out[c] = out[c].map(conv)
    return out


class GatewayClient:
    """Reads the paginated gateway feed (file-based stand-in for a REST API) with retry + backoff.

    `fail_first` lets tests simulate transient API failures: the first N calls raise.
    """

    def __init__(self, base: Path | None = None, fail_first: int = 0, sleep=time.sleep):
        self.base = Path(base or config.source_dir() / "gateway")
        self.fail_first = fail_first
        self.calls = 0
        self._sleep = sleep

    def _fetch_page(self, day: date, page: int) -> dict | None:
        self.calls += 1
        if self.calls <= self.fail_first:
            raise ConnectionError("simulated gateway API failure (HTTP 503)")
        path = self.base / day.isoformat() / f"page_{page:03d}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def fetch_page(self, day: date, page: int) -> dict | None:
        cfg = config.settings()["ingestion"]
        for attempt in range(1, cfg["api_max_retries"] + 1):
            try:
                return self._fetch_page(day, page)
            except (ConnectionError, TimeoutError) as exc:
                log.warning("gateway page %s/%s attempt %s failed: %s", day, page, attempt, exc)
                if attempt == cfg["api_max_retries"]:
                    raise IngestError(f"gateway API failed after {attempt} attempts for {day} page {page}") from exc
                self._sleep(cfg["api_backoff_seconds"] * 2 ** (attempt - 1))
        return None

    def fetch_day(self, day: date) -> pd.DataFrame:
        records: list[dict] = []
        page: int | None = 1
        while page:
            payload = self.fetch_page(day, page)
            if payload is None:
                break
            records.extend(payload["data"])
            page = payload.get("next_page")
        return to_raw(pd.DataFrame(records))


def read_gateway(run_date: date, client: GatewayClient | None = None) -> tuple[pd.DataFrame, str]:
    client = client or GatewayClient()
    return client.fetch_day(run_date), f"gateway/{run_date.isoformat()}"


def read_settlements(run_date: date) -> tuple[pd.DataFrame | None, str]:
    path = config.source_dir() / "settlements" / f"settlements_{run_date.isoformat()}.csv"
    if not path.exists():
        return None, path.name
    return to_raw(pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])), path.name


def read_merchants(run_date: date) -> tuple[pd.DataFrame | None, str]:
    path = config.source_dir() / "merchants" / f"merchants_{run_date.isoformat()}.csv"
    if not path.exists():
        return None, path.name
    return to_raw(pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])), path.name


def read_orders(run_date: date, watermark: datetime | None) -> tuple[pd.DataFrame, str]:
    """Incremental CDC-style extract: rows with updated_at in [watermark - overlap, run_date + 1)."""
    overlap = timedelta(days=config.settings()["ingestion"]["orders_overlap_days"])
    day_start = datetime.combine(run_date, datetime.min.time())
    lower = min(watermark - overlap, day_start) if watermark else datetime(1970, 1, 1)
    upper = day_start + timedelta(days=1)
    con = sqlite3.connect(config.source_dir() / "orders" / "orders.db")
    try:
        df = pd.read_sql_query(
            "SELECT * FROM order_changes WHERE updated_at >= ? AND updated_at < ? ORDER BY updated_at",
            con, params=(lower.strftime("%Y-%m-%d %H:%M:%S"), upper.strftime("%Y-%m-%d %H:%M:%S")))
    finally:
        con.close()
    return to_raw(df), f"orders.db[{lower:%Y-%m-%d %H:%M}..{upper:%Y-%m-%d}]"


def write_bronze(dataset: str, run_date: date, df: pd.DataFrame, run_id: str, source: str) -> Path:
    """Overwrite the (dataset, ingest_date) partition atomically. Re-running a day is idempotent."""
    folder = config.bronze_dir() / dataset / f"ingest_date={run_date.isoformat()}"
    folder.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out["_run_id"] = run_id
    out["_source_file"] = source
    out["_ingested_at"] = datetime.now().isoformat(timespec="seconds")
    tmp = folder / "part.parquet.tmp"
    out.astype(object).to_parquet(tmp, index=False)
    final = folder / "part.parquet"
    tmp.replace(final)
    return final
