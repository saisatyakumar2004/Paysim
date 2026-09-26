"""Measure the local gateway API path against the file path (real HTTP on localhost, no in-process shortcut).

Reports, per page size: time to fetch one full day over HTTP, the same day from files, and the record count check; plus the
observed behaviour under injected faults (503s with backoff, slow responses vs the client timeout).
Writes data/benchmarks/results_api_<date>.json.  Requires source data (python -m payreconcile generate) and the [api] extra.
"""

from __future__ import annotations

import json
import platform
import socket
import statistics
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def serve(app, port):
    import uvicorn
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    return server, t


def main() -> int:
    import fastapi
    import httpx
    import uvicorn

    from payreconcile import config, ingest
    from payreconcile.gateway_api import FaultPlan, create_app

    days = sorted(p.name for p in (config.source_dir() / "gateway").iterdir() if p.is_dir())[:10]
    out: dict = {"run_at": datetime.now().isoformat(timespec="seconds"), "python": platform.python_version(),
                 "fastapi": fastapi.__version__, "uvicorn": uvicorn.__version__, "httpx": httpx.__version__,
                 "days_measured": len(days), "note": "localhost HTTP, single client, synthetic data, one machine; "
                 "this measures the stand-in API, not any real gateway"}

    port = free_port()
    server, thread = serve(create_app(), port)
    base = f"http://127.0.0.1:{port}"
    file_client = ingest.GatewayClient()
    rows = []
    for size in (100, 500, 2000):
        api = ingest.HttpGatewayClient(base, page_size=size)
        api_t, file_t, recs = [], [], 0
        for d in days:
            day = date.fromisoformat(d)
            t = time.perf_counter()
            a = api.fetch_day(day)
            api_t.append(time.perf_counter() - t)
            t = time.perf_counter()
            f = file_client.fetch_day(day)
            file_t.append(time.perf_counter() - t)
            assert len(a) == len(f) and list(a["txn_id"]) == list(f["txn_id"]), d
            recs += len(a)
        rows.append({"page_size": size, "days": len(days), "records": recs,
                     "http_median_s_per_day": round(statistics.median(api_t), 4),
                     "file_median_s_per_day": round(statistics.median(file_t), 4),
                     "http_requests_per_day": round(api.calls / len(days), 1), "identical_to_file_source": True})
    out["fetch"] = rows
    server.should_exit = True
    thread.join(timeout=5)

    # ---- injected failure: two 503s then success (real sleeps, real backoff) --------------------------------------------------
    port = free_port()
    server, thread = serve(create_app(faults=FaultPlan(fail_first=2)), port)
    c = ingest.HttpGatewayClient(f"http://127.0.0.1:{port}")
    t = time.perf_counter()
    df = c.fetch_day(date.fromisoformat(days[0]))
    out["retry_after_two_503"] = {"records": len(df), "seconds": round(time.perf_counter() - t, 3), "attempt_log": c.attempts_log,
                                  "expected_backoff_seconds": "0.2 + 0.4"}
    server.should_exit = True
    thread.join(timeout=5)

    # ---- slow server vs client timeout -------------------------------------------------------------------------------------------
    port = free_port()
    server, thread = serve(create_app(faults=FaultPlan(delay_seconds=0.6)), port)
    c = ingest.HttpGatewayClient(f"http://127.0.0.1:{port}", timeout=0.15)
    t = time.perf_counter()
    try:
        c.fetch_day(date.fromisoformat(days[0]))
        outcome = "unexpected success"
    except ingest.IngestError as exc:
        outcome = f"IngestError: {exc}"
    out["timeout_vs_slow_server"] = {"client_timeout_s": 0.15, "server_delay_s": 0.6, "outcome": outcome,
                                     "attempts": c.calls, "seconds_until_giving_up": round(time.perf_counter() - t, 2)}
    server.should_exit = True
    thread.join(timeout=5)

    path = ROOT / "data" / "benchmarks" / f"results_api_{date.today().isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
