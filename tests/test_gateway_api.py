"""Local gateway HTTP API and its client: pagination, validation, determinism, retries, backoff, timeouts, outages.

The file-based source stays the deterministic fixture: the API serves the same seeded data, and the API path and file path
must be interchangeable (identical records; running a day through one and then the other changes nothing in Silver).
"""

from __future__ import annotations

import socket
import threading
import time
from datetime import date

import httpx
import pandas as pd
import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from payreconcile import config, db, ingest, pipeline  # noqa: E402
from payreconcile.gateway_api import FaultPlan, create_app  # noqa: E402
from payreconcile.generate import GenConfig, generate  # noqa: E402

DAY = date(2026, 1, 3)


@pytest.fixture(scope="module")
def source(tmp_path_factory):
    import os
    root = tmp_path_factory.mktemp("api_src")
    old = os.environ.get("PAYRECON_DATA")
    os.environ["PAYRECON_DATA"] = str(root)
    config.settings.cache_clear()
    generate(GenConfig(out=root, days=4, orders_per_day=150, n_merchants=25, seed=11))
    yield root
    if old is None:
        os.environ.pop("PAYRECON_DATA", None)
    else:
        os.environ["PAYRECON_DATA"] = old
    config.settings.cache_clear()


def file_records(day: date) -> pd.DataFrame:
    return ingest.GatewayClient().fetch_day(day)


def client_for(app, **kw) -> ingest.HttpGatewayClient:
    sleeps: list[float] = []
    c = ingest.HttpGatewayClient(client=TestClient(app), sleep=sleeps.append, **kw)
    c.sleeps = sleeps
    return c


# ---- the API itself ------------------------------------------------------------------------------------------------
def test_health_reports_available_days(source):
    r = TestClient(create_app()).get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok" and body["days_available"] >= 1 and body["first_day"] <= body["last_day"]


def test_pagination_covers_every_record_exactly_once_and_ends_cleanly(source):
    api = TestClient(create_app())
    total = api.get("/transactions", params={"date": DAY.isoformat(), "page_size": 1}).json()["total"]
    assert total > 100
    seen, page = [], 1
    while page:
        body = api.get("/transactions", params={"date": DAY.isoformat(), "page": page, "page_size": 40}).json()
        assert len(body["data"]) <= 40 and body["page"] == page
        seen += [r["txn_id"] for r in body["data"]]
        page = body["next_page"]
    assert len(seen) == total == len(file_records(DAY))
    assert seen == list(file_records(DAY)["txn_id"])                       # same records, same order as the file source
    past_end = api.get("/transactions", params={"date": DAY.isoformat(), "page": 999}).json()
    assert past_end["data"] == [] and past_end["next_page"] is None


def test_responses_are_deterministic_and_missing_days_are_empty_not_errors(source):
    api = TestClient(create_app())
    a = api.get("/transactions", params={"date": DAY.isoformat()}).json()
    b = api.get("/transactions", params={"date": DAY.isoformat()}).json()
    assert a == b
    empty = api.get("/transactions", params={"date": "2020-01-01"})
    assert empty.status_code == 200 and empty.json()["total"] == 0 and empty.json()["next_page"] is None


@pytest.mark.parametrize("params", [{"page": 0}, {"page_size": 0}, {"page_size": 5000}, {"date": "not-a-date"}])
def test_invalid_parameters_are_rejected_with_422(source, params):
    assert TestClient(create_app()).get("/transactions", params=params).status_code == 422


# ---- the client --------------------------------------------------------------------------------------------------------------
def test_http_client_returns_exactly_what_the_file_client_returns(source):
    via_api = client_for(create_app(), page_size=37).fetch_day(DAY)        # awkward page size on purpose
    pd.testing.assert_frame_equal(via_api, file_records(DAY))


def test_client_retries_transient_503s_with_exponential_backoff(source):
    c = client_for(create_app(faults=FaultPlan(fail_first=2)))
    df = c.fetch_day(DAY)
    assert len(df) == len(file_records(DAY))
    assert c.sleeps[:2] == [0.2, 0.4]                                       # 0.2 * 2**(attempt-1)
    assert len(c.attempts_log) == 2 and "HTTP 503" in c.attempts_log[0]


def test_client_honours_retry_after_but_caps_it(source):
    c = client_for(create_app(faults=FaultPlan(fail_first=1, retry_after=3)))
    c.fetch_day(DAY)
    assert c.sleeps[0] == 3
    c = client_for(create_app(faults=FaultPlan(fail_first=1, retry_after=3600)))
    c.fetch_day(DAY)
    assert c.sleeps[0] == config.settings()["ingestion"]["api_backoff_cap_seconds"]


def test_client_gives_up_after_max_retries_with_a_clear_error(source):
    c = client_for(create_app(faults=FaultPlan(fail_first=99)))
    with pytest.raises(ingest.IngestError, match="failed after 3 attempts"):
        c.fetch_day(DAY)
    assert c.calls == config.settings()["ingestion"]["api_max_retries"]


def test_client_does_not_retry_a_request_that_can_never_succeed(source):
    c = client_for(create_app(), page_size=5000)                            # server answers 422
    with pytest.raises(ingest.IngestError, match="HTTP 422"):
        c.fetch_day(DAY)
    assert c.calls == 1 and c.sleeps == []


def test_client_rejects_a_malformed_payload_without_retrying():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"nope": 1}))
    c = ingest.HttpGatewayClient(client=httpx.Client(transport=transport, base_url="http://x"), sleep=lambda s: None)
    with pytest.raises(ingest.IngestError, match="malformed payload"):
        c.fetch_day(DAY)
    assert c.calls == 1


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def live_server(source):
    """A real uvicorn server on a free port, with a slow response, to exercise real client timeouts."""
    import uvicorn
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(create_app(faults=FaultPlan(delay_seconds=0.6)), host="127.0.0.1", port=port,
                                           log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)


def test_a_slow_server_triggers_client_timeouts_that_are_retried_then_reported(live_server):
    c = ingest.HttpGatewayClient(live_server, timeout=0.15, sleep=lambda s: None)
    with pytest.raises(ingest.IngestError, match="ReadTimeout"):
        c.fetch_day(DAY)
    assert c.calls == 3 and all("ReadTimeout" in a for a in c.attempts_log)


def test_a_server_that_is_down_is_a_transport_error_that_is_retried_then_reported():
    c = ingest.HttpGatewayClient(f"http://127.0.0.1:{_free_port()}", timeout=0.5, sleep=lambda s: None)
    with pytest.raises(ingest.IngestError, match="failed after 3 attempts"):
        c.fetch_day(DAY)
    assert c.calls == 3


# ---- the API path and the file path are interchangeable ------------------------------------------------------------------------------
def test_pipeline_over_the_api_and_over_files_produce_identical_silver(source):
    for d in (date(2026, 1, 1), date(2026, 1, 2)):                          # history first: merchants must exist
        assert pipeline.run_day(d, run_dbt=False).status in ("SUCCESS", "SUCCESS_WITH_WARNINGS")
    api_client = client_for(create_app(faults=FaultPlan(fail_first=1)))     # includes one injected outage on the way
    s1 = pipeline.run_day(DAY, run_dbt=False, gateway_client=api_client)
    assert s1.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS"), s1.error
    con = db.connect()
    n_before = con.execute("SELECT count(*) FROM ops.change_log WHERE dataset = 'gateway'").fetchone()[0]
    bronze = pd.read_parquet(config.bronze_dir() / "gateway" / f"ingest_date={DAY}" / "part.parquet")
    assert bronze["_source_file"].iloc[0].startswith("gateway-api/")
    con.close()
    s2 = pipeline.run_day(DAY, run_dbt=False)                               # same day again, now from the files
    assert s2.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS"), s2.error
    con = db.connect()
    assert con.execute("SELECT count(*) FROM ops.change_log WHERE dataset = 'gateway'").fetchone()[0] == n_before
    con.close()
