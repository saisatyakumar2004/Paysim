"""A small local payment-gateway HTTP API (FastAPI) serving the deterministic, seeded source data.

    GET /health
    GET /transactions?date=YYYY-MM-DD&page=1&page_size=500

The data is exactly what the file-based source contains (same generator seed), re-paginated on request, so the API path and
the file path must produce identical Bronze data (tested). Failure modes can be injected deterministically for tests and
demos: fail the first N requests with HTTP 503 (with Retry-After), or delay every response.

Run:  python -m payreconcile serve-api --port 8765 [--fail-first 2] [--delay 0.5]
Needs the optional extra:  pip install -e ".[api]"
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query

from . import __version__, config

MAX_PAGE_SIZE = 2000
DEFAULT_PAGE_SIZE = 500


@dataclass
class FaultPlan:
    """Deterministic failure injection (counts requests to /transactions only)."""

    fail_first: int = 0          # the first N requests answer HTTP 503
    delay_seconds: float = 0.0   # every response is delayed (used to exercise client timeouts)
    retry_after: int = 0         # value of the Retry-After header on injected 503s
    served: int = 0


def create_app(source_dir: Path | None = None, faults: FaultPlan | None = None) -> FastAPI:
    base = Path(source_dir or config.source_dir() / "gateway")
    plan = faults or FaultPlan()
    app = FastAPI(title="PayReconcile local gateway API", version=__version__)
    app.state.faults = plan

    @lru_cache(maxsize=64)
    def load_day(day: str) -> tuple[dict, ...]:
        folder = base / day
        if not folder.is_dir():
            return ()
        records: list[dict] = []
        for page in sorted(folder.glob("page_*.json")):
            records.extend(json.loads(page.read_text(encoding="utf-8"))["data"])
        return tuple(records)

    @app.get("/health")
    def health() -> dict:
        days = sorted(p.name for p in base.iterdir() if p.is_dir()) if base.is_dir() else []
        return {"status": "ok", "version": __version__, "days_available": len(days),
                "first_day": days[0] if days else None, "last_day": days[-1] if days else None}

    @app.get("/transactions")
    def transactions(day: Annotated[date | None, Query(alias="date", description="YYYY-MM-DD")] = None,
                     page: Annotated[int, Query(ge=1)] = 1,
                     page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE) -> dict:
        plan.served += 1
        if plan.delay_seconds:
            import time
            time.sleep(plan.delay_seconds)
        if plan.served <= plan.fail_first:
            raise HTTPException(status_code=503, detail="injected failure",
                                headers={"Retry-After": str(plan.retry_after)})
        if day is None:                                   # default: the latest available day
            days = sorted(p.name for p in base.iterdir() if p.is_dir()) if base.is_dir() else []
            if not days:
                raise HTTPException(status_code=404, detail="no data available")
            day_key = days[-1]
        else:
            day_key = day.isoformat()
        records = load_day(day_key)
        start = (page - 1) * page_size
        chunk = records[start:start + page_size]
        has_next = start + page_size < len(records)
        return {"date": day_key, "page": page, "page_size": page_size, "total": len(records),
                "next_page": page + 1 if has_next else None, "data": list(chunk)}

    return app


def serve(host: str = "127.0.0.1", port: int = 8765, fail_first: int = 0, delay: float = 0.0) -> None:
    import uvicorn
    uvicorn.run(create_app(faults=FaultPlan(fail_first=fail_first, delay_seconds=delay)), host=host, port=port,
                log_level="warning")
