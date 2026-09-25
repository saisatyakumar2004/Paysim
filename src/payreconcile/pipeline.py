"""Daily pipeline: extract -> bronze -> validate -> quality gate -> silver -> dbt (Gold) -> metrics.

Statuses written to ops.run_log:
  SUCCESS | SUCCESS_WITH_WARNINGS | QUALITY_GATE_FAILED | FAILED_TESTS | FAILED
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import pandas as pd

from . import db, dq, ingest, quarantine, reconcile, resources, silver, transform

log = logging.getLogger("payreconcile.pipeline")

DATASETS = ["merchants", "orders", "gateway", "settlements"]   # merchants first: other datasets reference it


@dataclass
class RunSummary:
    run_id: str
    run_date: date
    status: str = "RUNNING"
    gate: str | None = None
    dq_score: float | None = None
    reasons: list[str] = field(default_factory=list)
    rows_read: int = 0
    rows_written: int = 0
    rows_rejected: int = 0
    duration_s: float = 0.0
    dbt: transform.DbtResult | None = None
    error: str | None = None


_TASK_INSERT = ("INSERT INTO ops.task_log (run_id, run_date, task, status, duration_s, rows_in, rows_out, rows_rejected, "
                "detail, peak_rss_mb, rss_start_mb) VALUES (?,?,?,?,?,?,?,?,?,?,?)")


class _Tasks:
    """Times each step and records it in ops.task_log."""

    def __init__(self, con, run_id: str, run_date: date):
        self.con, self.run_id, self.run_date = con, run_id, run_date

    @contextmanager
    def task(self, name: str, rows_in: int | None = None):
        t0 = time.perf_counter()
        rec = {"rows_in": rows_in, "rows_out": None, "rows_rejected": None, "detail": None}
        mem = resources.PeakMemory()
        try:
            with mem:
                yield rec
            status = "SUCCESS"
        except Exception as exc:
            status = "FAILED"
            rec["detail"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            dur = time.perf_counter() - t0
            self.con.execute(_TASK_INSERT, [
                self.run_id, self.run_date, name, status, dur, rec["rows_in"], rec["rows_out"],
                rec["rows_rejected"], rec["detail"], mem.peak_mb, mem.start_mb])
            log.info(json.dumps({"run_id": self.run_id, "task": name, "status": status, "seconds": round(dur, 3),
                                 "rows_in": rec["rows_in"], "rows_out": rec["rows_out"],
                                 "peak_rss_mb": None if mem.peak_mb is None else round(mem.peak_mb)}))


def _history(con, dataset: str, before: date) -> list[int]:
    rows = con.execute("""
        SELECT rows_in FROM ops.dq_scores WHERE dataset = ? AND run_date < ? AND loaded
        ORDER BY run_date DESC LIMIT 7""", [dataset, before]).fetchall()
    return [r[0] for r in rows]


def run_day(run_date: date, run_dbt: bool = True, gateway_client: ingest.GatewayClient | None = None,
            contracts_dir=None, enforce_gate: bool = True) -> RunSummary:
    """`enforce_gate=False` exists ONLY for the bad-data demo (scripts/demo_poisoned_batch.py)."""
    t_start = time.perf_counter()
    started = datetime.now()
    run_id = f"{run_date:%Y%m%d}-{uuid.uuid4().hex[:6]}"
    summary = RunSummary(run_id, run_date)
    con = db.connect()
    attempt = con.execute("SELECT count(*) + 1 FROM ops.run_log WHERE run_date = ?", [run_date]).fetchone()[0]
    tasks = _Tasks(con, run_id, run_date)
    try:
        # ---- extract + bronze -------------------------------------------------------------------
        raw: dict[str, pd.DataFrame] = {}
        stats: dict = {}
        wm_row = con.execute("SELECT watermark FROM ops.watermarks WHERE dataset='orders'").fetchone()
        watermark = wm_row[0] if wm_row else None
        with tasks.task("extract") as rec:
            readers = {
                "merchants": lambda: ingest.read_merchants(run_date),
                "orders": lambda: ingest.read_orders(run_date, watermark),
                "gateway": lambda: ingest.read_gateway(run_date, gateway_client),
                "settlements": lambda: ingest.read_settlements(run_date),
            }
            for name, reader in readers.items():
                df, source = reader()
                if df is None:
                    if name == "settlements":     # a missing bank file is an incident, not a non-event
                        contract = dq.load_contract(name, contracts_dir)
                        df = pd.DataFrame(columns=contract["columns"]["required"])
                    else:
                        continue
                raw[name] = df
                ingest.write_bronze(name, run_date, df, run_id, source)
            rec["rows_in"] = sum(len(d) for d in raw.values())
            rec["rows_out"] = rec["rows_in"]
            summary.rows_read = rec["rows_in"]

        # ---- validate -----------------------------------------------------------------------------
        results: dict[str, dq.DQResult] = {}
        extra: list[dq.RuleResult] = []
        with tasks.task("validate_and_quality", rows_in=summary.rows_read) as rec:
            for name in DATASETS:
                if name not in raw:
                    continue
                contract = dq.load_contract(name, contracts_dir)
                res = dq.evaluate(con, contract, raw[name], run_date)
                results[name] = res
                if name == "merchants" and not res.schema_failed and res.status != "FAIL":
                    silver.apply_merchants_scd2(con, res.good, run_date, run_id)   # needed for FK checks below
            if "gateway" in raw:
                newest = None
                if not results["gateway"].schema_failed and len(results["gateway"].good):
                    con.register("gw_good", results["gateway"].good)
                    newest = con.execute("SELECT max(pr_ts(created_at)) FROM gw_good").fetchone()[0]
                    con.unregister("gw_good")
                extra.append(dq.freshness_check("gateway", newest, run_date))
                extra.append(dq.volume_check("gateway", len(raw["gateway"]), _history(con, "gateway", run_date)))
            if "settlements" in raw:
                extra.append(dq.volume_check("settlements", len(raw["settlements"]),
                                             _history(con, "settlements", run_date)))
            stats = _batch_stats(con, raw, results)
            for (ds, metric), value in stats.items():
                extra.append(dq.distribution_check(ds, metric, value, _stat_history(con, ds, metric, run_date)))
            gate, score, reasons = dq.gate_decision(list(results.values()), extra)
            if gate == "HALT" and not enforce_gate:
                gate = "WARN"
                reasons = [f"(gate bypassed for demo) {r}" for r in reasons]
            summary.gate, summary.dq_score, summary.reasons = gate, score, reasons
            summary.rows_rejected = sum(r.rows_quarantined for r in results.values())
            rec["rows_out"] = sum(len(r.good) for r in results.values() if r.good is not None)
            rec["rows_rejected"] = summary.rows_rejected
            rec["detail"] = f"gate={gate} score={score:.4f}" + (f" reasons={reasons}" if reasons else "")
        _persist_quality(con, run_id, run_date, results, extra, loaded=gate != "HALT")
        for (ds, metric), value in stats.items():
            con.execute("INSERT INTO ops.batch_stats VALUES (?,?,?,?,?,?)", [run_id, run_date, ds, metric, value, gate != "HALT"])

        # ---- load silver (only if the gate lets the batch through) ------------------------------------
        if gate == "HALT":
            summary.status = "QUALITY_GATE_FAILED"
            log.error("quality gate HALT for %s: %s", run_date, reasons)
        else:
            with tasks.task("load_silver") as rec:
                written = 0
                for name, fn in (("orders", silver.apply_orders), ("gateway", silver.apply_gateway),
                                 ("settlements", silver.apply_settlements)):
                    if name in results:
                        written += fn(con, results[name].good, run_id)
                if "orders" in results and len(results["orders"].good):
                    wm = pd.to_datetime(results["orders"].good["updated_at"]).max().to_pydatetime()
                    con.execute("""INSERT INTO ops.watermarks VALUES ('orders', ?)
                                   ON CONFLICT (dataset) DO UPDATE SET watermark = greatest(watermark, excluded.watermark)""", [wm])
                rec["rows_out"] = written
                summary.rows_written = written

            # ---- reconciliation (incremental) --------------------------------------------------------------
            # A replay of an old day must not rewind the reconciliation clock: as-of = latest day ever loaded.
            latest = con.execute("SELECT max(run_date) FROM ops.run_log WHERE status LIKE 'SUCCESS%'").fetchone()[0]
            as_of = max(run_date, latest) if latest else run_date
            with tasks.task("reconcile") as rec:
                rs = reconcile.reconcile(con, as_of, run_id, run_date)
                rec["rows_in"], rec["rows_out"] = rs.items_evaluated, rs.items_changed
                rec["detail"] = (f"mode={rs.mode} changes={rs.changes_read} scope(txn/stl/ord)="
                                 f"{rs.scope_txns}/{rs.scope_settlements}/{rs.scope_orders} "
                                 f"evaluated={rs.items_evaluated}/{rs.items_total} changed={rs.items_changed}")

            # ---- gold marts + tests (dbt) -----------------------------------------------------------------------
            if run_dbt:
                con.close()
                with _closed_task(run_id, run_date, "dbt_build") as rec:
                    summary.dbt = transform.build(as_of)
                    rec["detail"] = f"pass={summary.dbt.passed} fail={summary.dbt.failed} skipped={summary.dbt.skipped}"
                    rec["status"] = "SUCCESS" if summary.dbt.success else "FAILED"
                con = db.connect()
                if not summary.dbt.success:
                    summary.status = "FAILED_TESTS"
                    summary.error = "; ".join(summary.dbt.failures) or summary.dbt.error
            if summary.status == "RUNNING":
                summary.status = "SUCCESS_WITH_WARNINGS" if gate == "WARN" else "SUCCESS"
    except Exception as exc:                        # noqa: BLE001 - recorded, then surfaced in the summary
        summary.status, summary.error = "FAILED", f"{type(exc).__name__}: {exc}"
        log.exception("pipeline failed for %s", run_date)
        try:
            con.close()
        except Exception:                           # noqa: BLE001
            pass
        con = db.connect()
    finally:
        summary.duration_s = time.perf_counter() - t_start
        con.execute("INSERT INTO ops.run_log VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            run_id, run_date, attempt, started, datetime.now(), summary.duration_s, summary.status, summary.dq_score,
            summary.gate, summary.rows_read, summary.rows_written, summary.rows_rejected, summary.error])
        con.close()
    return summary


@contextmanager
def _closed_task(run_id: str, run_date: date, name: str):
    """Task logging for steps that need the warehouse file free (dbt): reopens the DB just to log."""
    t0 = time.perf_counter()
    rec = {"detail": None, "status": "SUCCESS"}
    mem = resources.PeakMemory()                       # includes the dbt subprocess
    try:
        with mem:
            yield rec
    except Exception as exc:
        rec["status"], rec["detail"] = "FAILED", f"{type(exc).__name__}: {exc}"
        raise
    finally:
        con = db.connect()
        con.execute(_TASK_INSERT, [run_id, run_date, name, rec["status"], time.perf_counter() - t0, None, None, None,
                                   rec["detail"], mem.peak_mb, mem.start_mb])
        con.close()


def _batch_stats(con, raw: dict[str, pd.DataFrame], results: dict[str, dq.DQResult]) -> dict:
    """Scale statistics of the incoming batch, measured on rows whose amount parses (before any filtering)."""
    out: dict = {}
    specs = {"gateway": ("amount", "status = 'captured'", "mean_captured_amount"),
             "settlements": ("gross_amount", "true", "mean_abs_gross_amount")}
    for ds, (col, cond, metric) in specs.items():
        res = results.get(ds)
        if ds not in raw or res is None or res.schema_failed or not len(raw[ds]):
            continue
        df = raw[ds].rename(columns=res.aliased)
        con.register("bstat", df[[c for c in df.columns if c in ("status", col)]])
        v = con.execute(f"SELECT avg(abs(pr_amt({col}))) FROM bstat WHERE {cond} AND pr_amt({col}) IS NOT NULL").fetchone()[0]
        con.unregister("bstat")
        if v is not None:
            out[(ds, metric)] = float(v)
    return out


def _stat_history(con, dataset: str, metric: str, before: date) -> list[float]:
    rows = con.execute("""SELECT value FROM ops.batch_stats WHERE dataset = ? AND metric = ? AND run_date < ? AND loaded
                          ORDER BY run_date DESC LIMIT 14""", [dataset, metric, before]).fetchall()
    return [r[0] for r in rows]


def _persist_quality(con, run_id: str, run_date: date, results: dict[str, dq.DQResult],
                     extra: list[dq.RuleResult], loaded: bool) -> None:
    for name, res in results.items():
        for r in res.rule_results:
            con.execute("INSERT INTO ops.dq_results VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
                run_id, run_date, name, r.rule_id, r.severity, r.description, r.evaluated, r.violations,
                r.ratio, r.status, r.action])
        con.execute("INSERT INTO ops.dq_scores VALUES (?,?,?,?,?,?,?,?)", [
            run_id, run_date, name, res.score, res.status, res.rows_in, res.rows_quarantined, loaded])
        quarantine.replace(con, run_id, run_date, name, res.quarantined)
    for x in extra:
        con.execute("INSERT INTO ops.dq_results VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
            run_id, run_date, x.dataset, x.rule_id, x.severity, x.description, x.evaluated, x.violations,
            x.ratio, x.status, x.action])


def run_range(start: date, end: date, run_dbt: bool = True) -> list[RunSummary]:
    out = []
    d = start
    while d <= end:
        out.append(run_day(d, run_dbt=run_dbt))
        d += timedelta(days=1)
    return out
