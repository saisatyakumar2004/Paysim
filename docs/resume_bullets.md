# Resume bullets: PayReconcile

Every number below is taken from an artifact in this repository (path in the last column). All data is **synthetic**; keep that word
in the bullet or the interview answer. Nothing here claims production experience, real fraud detection or a finished Power BI report.

**Project line:** *PayReconcile: incremental payments-reconciliation and data-quality platform* (Python, DuckDB, dbt, FastAPI, Streamlit, scikit-learn, GitHub Actions). Personal project on synthetic data.

## Data Engineer
* Built an **incremental reconciliation engine** (Python + DuckDB SQL) that re-evaluates only the scope derived from a change log and applies state, audit rows and watermark in **one transaction**; its output equals a from-scratch rebuild exactly (0 differences over 91,219 items against two independent implementations).
* Ran a controlled **Silver-partitioning experiment** from 86k to **8.6 M reconciliation items** on an 8 GB laptop: date-partitioned Parquet was **1.4x to 7.1x slower** than DuckDB tables with identical results, so the design kept tables; incremental compute vs full recompute widened from 1.7x to 8.6x as history grew.
* Designed a **YAML-driven data-quality framework** (50 documented rules, quarantine with raw payloads, a scored quality gate, and volume/freshness/distribution checks) plus a **quarantine replay** that never overwrites newer trusted data, with an append-only decision log.
* Implemented a **local FastAPI gateway and resilient client** (pagination, timeout, retry with exponential backoff, `Retry-After`) verified interchangeable with the file source by tests.
* Instrumented every stage for time, peak memory and row counts: a daily run peaks at **380 MB** including dbt, and dbt accounts for **89%** of the ~12.4 s run, which explains why incremental reconciliation (0.45 s) did not shorten the end-to-end run.
* Shipped **139 automated tests** (mutation-checked replay and reconciliation logic, crash/recovery and idempotency scenarios) with CI running lint, a secrets scan, a deterministic smoke test and the end-to-end suite.

## Data Analyst
* Defined **13 reconciliation KPIs** (money at risk, match rate, exception age, duplicate-charge exposure, settlement lag...) once, each with definition, formula, business meaning and source tables, and used them in an **8-page Streamlit dashboard**.
* Built a filterable **exceptions queue** (merchant, date, status, risk tier, payment method, amount, age) with an explain panel that traces every item to its source records, match evidence and status history.
* Surfaced **INR 2.58 M of synthetic money at risk** across five exception types (missing settlements were the largest, about 48%) and separated confirmed from suspected duplicate-charge exposure (INR 737,660 vs 18,968).
* Produced a **Power BI-ready export layer** (11 tables, documented relationships and DAX measures). *Not* included: a `.pbix` report; the measures were not executed in Power BI.

## Data Scientist
* Built **merchant-day anomaly signals** with leakage-safe features (previous days only, gateway data only) and compared **Isolation Forest against a simple z-score baseline** on injected synthetic anomalies: F1 **0.53 vs 0.76**; the simple rule won and this was reported, not tuned away.
* Made the leakage controls **executable tests** (temporal invariance, no use of settlements or labels, deterministic scoring) and evaluated with precision, recall, F1 and explicit FP/FN counts; documented that synthetic anomalies demonstrate a method, not real fraud detection.
* Showed why data quality gates ML: a rupees-to-paise batch that passes every row rule caused **24 spurious anomalous merchants vs 2** without the gate; the gate halted it and blocked the model refresh.

## FinTech / Payments
* Implemented **three-way reconciliation** (gateway, bank settlement, orders) with exact and **constrained fuzzy matching that never guesses ambiguity**: 906 fuzzy matches, 0 wrong on synthetic data, with per-match evidence (features, score, threshold, decision, reason).
* Found and classified the root cause of every missed duplicate charge, then added a separate **suspected-duplicate tier**, raising combined recall from 97.55% to **99.57% (691 / 694) at 100% precision**, while *rejecting* an unanchored rule that would have found the last three but likely misfire on fixed-price merchants.
* Made financial state **auditable**: an append-only status history (`NEW_ITEM`, `INPUT_CHANGE`, `AGED`, `RESOLVED`), sticky matches that unrelated data cannot reopen, late settlement/refund handling, and a full-rebuild oracle that proves the incremental state correct.

## Evidence map

| Claim | Where to verify |
|---|---|
| 0 differences vs full rebuild at 91,219 items | `python -m payreconcile verify-recon --reference`; `tests/test_end_to_end.py`; `tests/test_reconcile*.py` |
| 86k to 8.6 M items, partitioned 1.4-7.1x slower, ratio 1.7x to 8.6x | `docs/results_partitioning_benchmark_2026-09-26*.json`, `docs/benchmarks.md` |
| 50 rules, coverage matrix | `docs/dq_rules.md`, `contracts/*.yml`, `scripts/gen_rule_docs.py` |
| Replay never overwrites newer data; log | `src/payreconcile/replay.py`, `tests/test_replay.py`, `ops.replay_log` |
| API and client behaviour | `src/payreconcile/gateway_api.py`, `ingest.py`, `tests/test_gateway_api.py` |
| 380 MB peak, dbt 89% of the run | `docs/results_stage_resources_30day_backfill.json` |
| 139 tests | `pytest` (see the final validation in the report) |
| 13 KPIs | `dashboard/kpis.py`, `docs/kpi_definitions.md` |
| INR 2.58 M, ~48% missing settlements, duplicate exposures | `docs/results_evaluation_2026-09-26.txt`, dashboard executive page |
| Power BI export, 11 tables | `python -m payreconcile export-bi`, `docs/powerbi.md`, `tests/test_dashboard_export.py` |
| F1 0.53 vs 0.76; leakage tests | `docs/results_anomaly_2026-09-26.json`, `tests/test_anomaly_leakage.py` |
| 24 vs 2 flagged merchants with/without the gate | `docs/results_poisoned_batch.json` (earlier pass), `scripts/demo_poisoned_batch.py` |
| 906 fuzzy matches / 0 wrong; 691 / 694 duplicates | `docs/results_evaluation_2026-09-26.txt` |

## Do not claim
* Real fraud detection, real payment data, or production deployment. Everything is a local simulation on synthetic data.
* A Power BI report. Only the export layer and documentation exist.
* Any performance figure beyond 8.6 M items, or that incremental processing shortened the end-to-end run (it did not: dbt dominates).
* Cloud experience: the cloud design is a document.
