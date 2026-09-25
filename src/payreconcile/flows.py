"""Prefect orchestration (optional, `pip install -e .[orchestration]`).

Prefect runs in-process with no server, so it costs almost no RAM (Airflow would need a scheduler, webserver and
metadata DB). It adds what a plain loop lacks: retries with delay, per-task state, and run history.

Retry policy: infrastructure failures (status FAILED, e.g. the gateway API being down) are retried; a QUALITY_GATE_FAILED
run is deterministic bad data, so retrying it would change nothing. It is reported as a failed state, not retried.
"""

from __future__ import annotations

from datetime import date, timedelta

from prefect import flow, get_run_logger, task

from . import pipeline


class QualityGateFailed(RuntimeError):
    pass


@task(name="run-day", retries=2, retry_delay_seconds=30, retry_condition_fn=lambda t, r, s: _is_retryable(s))
def run_day_task(run_date: date, run_dbt: bool = True) -> dict:
    log = get_run_logger()
    s = pipeline.run_day(run_date, run_dbt=run_dbt)
    log.info("run %s: %s gate=%s score=%s read=%s written=%s rejected=%s (%.1fs)", s.run_id, s.status, s.gate,
             s.dq_score, s.rows_read, s.rows_written, s.rows_rejected, s.duration_s)
    if s.status == "FAILED":
        raise RuntimeError(s.error)                        # retryable
    if s.status == "QUALITY_GATE_FAILED":
        raise QualityGateFailed("; ".join(s.reasons))      # not retryable
    if s.status == "FAILED_TESTS":
        raise QualityGateFailed(s.error or "dbt tests failed")
    return {"run_id": s.run_id, "status": s.status, "dq_score": s.dq_score}


def _is_retryable(state) -> bool:
    try:
        state.result()
    except QualityGateFailed:
        return False
    except Exception:                                      # noqa: BLE001
        return True
    return False


@flow(name="payreconcile-daily")
def daily_flow(run_date: date, run_dbt: bool = True) -> dict:
    return run_day_task(run_date, run_dbt)


@flow(name="payreconcile-backfill")
def backfill_flow(start: date, end: date, run_dbt: bool = True) -> list[dict]:
    """Sequential on purpose: the reconciliation depends on the previous days' Silver."""
    log = get_run_logger()
    results = []
    d = start
    while d <= end:
        try:
            results.append(run_day_task(d, run_dbt))
        except QualityGateFailed as exc:
            log.error("%s halted by quality gate: %s (continuing with the next day)", d, exc)
            results.append({"run_date": str(d), "status": "QUALITY_GATE_FAILED"})
        d += timedelta(days=1)
    return results
