"""Extraction: sources -> raw (all-string) DataFrames -> Bronze Parquet.

Bronze is immutable raw data plus lineage columns. Nothing is cast, cleaned or dropped here.
"""

from __future__ import annotations

import json
import logging
import os
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


class HttpGatewayClient:
    """Reads the gateway feed over HTTP (see gateway_api.py) with a timeout, retries and exponential backoff.

    Retried: connection errors, timeouts, HTTP 429 and 5xx (honouring Retry-After, capped). Not retried: other 4xx
    (a bad request will not get better) and malformed payloads. All failures end in IngestError.
    """

    def __init__(self, base_url: str | None = None, client=None, timeout: float | None = None, sleep=time.sleep,
                 page_size: int | None = None):
        import httpx  # optional dependency: pip install -e ".[api]"
        cfg = config.settings()["ingestion"]
        self._httpx = httpx
        self.timeout = timeout if timeout is not None else cfg["api_timeout_seconds"]
        self.page_size = page_size or cfg["api_page_size"]
        self._http = client or httpx.Client(base_url=base_url or os.environ.get("PAYRECON_API_URL", "http://127.0.0.1:8765"))
        self._sleep = sleep
        self.calls = 0
        self.attempts_log: list[str] = []

    def _get(self, day: date, page: int) -> dict:
        cfg = config.settings()["ingestion"]
        max_retries = cfg["api_max_retries"]
        for attempt in range(1, max_retries + 1):
            self.calls += 1
            try:
                resp = self._http.get("/transactions", params={"date": day.isoformat(), "page": page,
                                                               "page_size": self.page_size}, timeout=self.timeout)
            except (self._httpx.TimeoutException, self._httpx.TransportError) as exc:
                reason, retry_after = f"{type(exc).__name__}", 0.0
            else:
                if resp.status_code == 200:
                    try:
                        payload = resp.json()
                        if not isinstance(payload.get("data"), list):
                            raise ValueError("missing 'data' list")
                        return payload
                    except ValueError as exc:
                        raise IngestError(f"gateway API returned a malformed payload for {day} page {page}: {exc}") from exc
                if resp.status_code != 429 and resp.status_code < 500:
                    raise IngestError(f"gateway API rejected the request for {day} page {page}: HTTP {resp.status_code}")
                reason = f"HTTP {resp.status_code}"
                try:
                    retry_after = min(float(resp.headers.get("Retry-After", 0)), cfg["api_backoff_cap_seconds"])
                except ValueError:
                    retry_after = 0.0
            self.attempts_log.append(f"{day} page {page} attempt {attempt}: {reason}")
            log.warning("gateway %s page %s attempt %s/%s failed: %s", day, page, attempt, max_retries, reason)
            if attempt == max_retries:
                raise IngestError(f"gateway API failed after {attempt} attempts for {day} page {page} ({reason})")
            backoff = min(cfg["api_backoff_seconds"] * 2 ** (attempt - 1), cfg["api_backoff_cap_seconds"])
            self._sleep(max(backoff, retry_after))
        raise AssertionError("unreachable")

    def fetch_day(self, day: date) -> pd.DataFrame:
        records: list[dict] = []
        page: int | None = 1
        while page:
            payload = self._get(day, page)
            records.extend(payload["data"])
            page = payload.get("next_page")
        return to_raw(pd.DataFrame(records))


def read_gateway(run_date: date, client: GatewayClient | HttpGatewayClient | None = None) -> tuple[pd.DataFrame, str]:
    client = client or GatewayClient()
    src = "gateway-api" if isinstance(client, HttpGatewayClient) else "gateway"
    return client.fetch_day(run_date), f"{src}/{run_date.isoformat()}"


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
