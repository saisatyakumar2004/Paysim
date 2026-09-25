# PayReconcile

**A payments reconciliation and fraud-signal data platform.** It ingests messy payment-gateway, bank-settlement, order-system and
merchant-master data, rejects bad records into a quarantine table, reconciles the three money trails in dbt, and reports **money at risk**.
A quality gate stops a bad batch before it reaches the warehouse or the fraud model.

Python · DuckDB · Parquet · dbt · Prefect (optional) · Streamlit · scikit-learn · pytest · GitHub Actions. Runs on an 8 GB laptop
(measured peak: ~430 MB for a daily run).

> **All data is synthetic** and generated with a fixed seed. Because the generator records what it injected, the pipeline is scored
> against a known answer key. That is a self-consistency check of the logic, not a claim about real-world accuracy (see [Limitations](#limitations)).

## The problem

A merchant is paid from three places that must agree: the gateway says a payment was captured, the bank says money was settled, the
order system says the order was paid. In practice they disagree, and each disagreement is money: a settlement that never arrived, a
customer charged twice, a refund of a payment that does not exist, a bank reference with a typo. Feeds are also dirty: duplicate webhooks,
impossible dates, currency symbols in amounts, renamed columns, an API that goes down, a gateway that silently switches units.

## What it does

```
gateway (JSON API) ─┐
bank files (CSV)    ├─► Bronze ─► schema contract ─► quality rules ─► quality gate ─► Silver ─► dbt Gold ─► dashboards
orders (SQLite CDC) ┤   (raw)                          │    │            HALT = load nothing        (marts + tests)    anomaly model
merchants (CSV)     ┘                         quarantine ◄┘    └─ volume / freshness / distribution        ▲
                                                                                        Silver ─► incremental reconciliation (state + audit)
```

* **Ingestion:** paginated API client with retry and backoff; CDC-style orders extract with a watermark and overlap; weekly merchant
  snapshots feeding an SCD Type 2 dimension.
* **Quality:** 4 YAML contracts, ~30 rules ([catalogue](docs/dq_rules.md)), PASS / WARNING / FAIL per rule, a weighted score, row-level quarantine
  with the raw payload, and batch-level volume, freshness and distribution checks.
* **Reconciliation (incremental):** exact match, then a fuzzy match (same merchant, amount and window, edit distance <= 2, unique from
  both sides). Statuses: `MATCHED`, `AMOUNT_MISMATCH`, `MISSING_SETTLEMENT`, `PENDING`, `ORPHAN_SETTLEMENT`, `ORPHAN_REFUND`,
  `SOURCE_QUARANTINED`, `ORDER_WITHOUT_PAYMENT`, plus duplicate-charge detection. Each run re-evaluates only what changed, keeps resolved
  matches stable, and appends every status change to an audit table ([details](#incremental-reconciliation)).
* **Gold:** `fact_reconciliation` + `recon_links` + `reconciliation_audit` (maintained by the engine), and a dbt star schema
  (`fact_transactions`, `fact_settlements`, SCD2 `dim_merchant`, `dim_date`) with marts
  (daily scorecard, merchant exceptions, settlement lag). 45 dbt tests, including "no quarantined row exists in Gold" and SCD2 integrity.
* **Fraud signals:** merchant-day anomaly detection (Isolation Forest and a z-score rule) on trusted data only.
* **Monitoring:** `ops.run_log`, `ops.task_log`, `ops.dq_results`, `ops.dq_scores`, `ops.batch_stats`, `ops.model_metrics`.
* **Dashboards:** Reconciliation, Exceptions, Data health, Pipeline health, Fraud signals (Streamlit).

Design details and trade-offs: [docs/architecture.md](docs/architecture.md). Production cloud mapping (**designed, not implemented**):
[docs/cloud_design.md](docs/cloud_design.md). Interview notes: [docs/interview_notes.md](docs/interview_notes.md).

## Results (measured, synthetic data)

30 days: 92,370 orders, 96,378 gateway records, 84,102 settlement lines, ~3,700 injected bad rows, 30 injected fraud patterns.
Raw output: [`docs/results_evaluation.txt`](docs/results_evaluation.txt), [`results_anomaly.json`](docs/results_anomaly.json),
[`results_poisoned_batch.json`](docs/results_poisoned_batch.json). Reproduce with the commands below.

| Question | Result |
|---|---|
| Bad rows quarantined vs injected | recall 100% (3,733 / 3,733), precision 100% |
| Reconciliation status vs ground truth (~87k txns) | 100% accuracy; fuzzy matches 906 found, 0 wrong |
| Duplicate charges | precision 100%, recall 97.6% (17 missed, see Limitations) |
| Orphan settlements | precision 100%, recall 100% |
| Money at risk found (INR, synthetic) | 2.58 M across 5 exception types (+ 0.74 M duplicate-charge exposure) |
| Anomaly detection (4,600 merchant-days, 30 injected) | Isolation Forest P 0.43 / R 0.67; simple z >= 4 rule P 0.61 / R 1.00 |
| Peak memory, one daily run incl. dbt | ~430 MB (all Python processes; polled working set) |
| Generate 30 days / full 30-day backfill (with dbt) | 54 s / 385 s (about 13 s per day, dominated by dbt startup, marts and tests) |
| Incremental reconciliation vs a from-scratch rebuild | identical state after 30 and 90 days (0 differences, two independent oracles); see below |

**Honest reading:** the reconciliation and quality numbers are high because the generator and the rules share one defect taxonomy. They show the
logic is coherent and testable. The anomaly numbers are more informative: the simple rule beat the model, and I report that rather than tune it away.

## Incremental reconciliation

Each run re-evaluates only the *scope* derived from `ops.change_log` (new or changed Silver rows, quarantine changes) plus what
time itself changes: unsettled items that age past the SLA and paid orders that become old enough to judge. Everything is applied in
one transaction together with the change-log watermark, so a crash leaves the previous state and a rerun is safe.

* **Late settlements / refunds:** a settlement (or refund, or gateway record, or order) that arrives late touches its match partner,
  its order and its siblings, and nothing else. Orphans and `MISSING_SETTLEMENT` items resolve when the missing side shows up.
* **Stable matches:** a resolved link is re-examined only if one of its two records changes. New look-alike records compete for
  still-unmatched items only. A resolved item is never rewritten by unrelated changes (tested: same row, same run id, no audit rows).
* **Audit trail:** `gold.reconciliation_audit` is append-only: item, old/new status, old/new settlement, money at risk, and a reason
  (`NEW_ITEM`, `INPUT_CHANGE`, `AGED`, `RESOLVED`). A dbt test asserts the last audit row explains every item's current status.
* **Idempotent:** a rerun with no new data evaluates only aged items and writes nothing (0 audit rows); replaying an old day cannot
  rewind the reconciliation clock; identical Silver rows are not rewritten or re-logged.
* **Provably equivalent to a rebuild:** `python -m payreconcile verify-recon --reference` compares the incremental state with the
  engine's own from-scratch mode and with the independent dbt reference model (`dbt/models/reference`). Both agree exactly after
  30 days (91,219 items) and 90 days (272,509 items). `reconcile --full` recomputes from scratch for recovery.
* **Known deviation from a rebuild:** because links are sticky, a later look-alike record cannot un-match an existing fuzzy match. A
  from-scratch rebuild would treat that pair as ambiguous and leave both open. A test pins this behaviour down.

### Benchmark (measured, synthetic data, i5-1335U laptop)
`python scripts/benchmark_reconciliation.py --days 90`. "Engine full" is the same code with scope = everything (compute only);
"dbt full" is the independent dbt full-rebuild models, i.e. the reconciliation path used before the incremental engine, including dbt start-up.

| Day | State items | Evaluated by incremental run | Incremental | Engine full | Gain | dbt full rebuild | Gain |
|---|---|---|---|---|---|---|---|
| 10 | 29,216 | 5,851 (20%) | 0.25 s | 0.17 s | 0.7x | 9.9 s | 39x |
| 30 | 89,911 | 6,307 (7%) | 0.42 s | 0.49 s | 1.2x | 10.8 s | 26x |
| 60 | 182,176 | 5,418 (3%) | 0.43 s | 0.66 s | 1.5x | 11.2 s | 26x |
| 90 | 272,509 | 6,366 (2%) | 0.51 s | 0.81 s | 1.6x | 12.1 s | 24x |

No-op rerun (no new data): 0.15 s at day 90. Audit table after 90 days: 527,097 rows. Raw results:
[`docs/results_reconciliation_benchmark_90d.json`](docs/results_reconciliation_benchmark_90d.json), [`_30d`](docs/results_reconciliation_benchmark_30d.json).

**Honest reading**
* **Work done fell sharply:** the daily run evaluates ~6k items whether the state holds 29k or 272k (from 20% down to 2%). That floor is real:
  each day's transactions wait one or two days for settlement, so about two days of items are always in play.
* **Wall-clock gain versus an in-process full rebuild is modest (1.2x at 30 days, 1.6x at 90) and it is slower on a tiny history.**
  DuckDB recomputes 272k items in 0.8 s, so at this size there is little to save. Incremental time still creeps up with history
  (0.25 s -> 0.51 s) because some statements scan whole Silver tables; DuckDB indexes did not help when I tried them (0.34 s -> 0.37 s).
  The gap should widen with history, but I have measured only up to 90 days and 272k items and would not extrapolate.
* **The 24-39x versus "dbt full" overstates the algorithmic gain.** That comparison includes ~9 s of dbt start-up and materialisation
  that the old path paid every day. It is the honest before/after of that path, not of the algorithm.
* **End to end, nothing got faster:** the full 30-day backfill took 385 s versus 388 s before, because the daily dbt run (start-up, marts,
  45 tests) still dominates at ~9 s of a ~13 s day. What the incremental engine buys here is not speed: it is bounded work per day, stable
  matches, an audit trail and safe reruns.

### Bad data blocked before it reaches the model
`scripts/demo_poisoned_batch.py` simulates the gateway switching to paise (amounts x100). Every row is valid, so the row-level score is 0.9988.

| Variant | Outcome | Merchants flagged as anomalous that day |
|---|---|---|
| clean batch | loaded | 2 |
| poisoned, gate bypassed (naive pipeline) | loaded; mean amount 1,104 -> 110,382 | **24** |
| poisoned, gate on | `QUALITY_GATE_FAILED` (`GW-DST-01`); Silver, Gold and model untouched | model blocked |

## Quick start (Windows PowerShell; needs Python 3.11+)

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -e ".[dev,dashboard,orchestration]"

python -m payreconcile generate --days 30                    # ~1 min, writes data/source and data/truth
python -m payreconcile backfill --start 2026-01-01 --end 2026-01-30    # ~6.5 min; add --no-dbt for a fast Silver-only run
python -m payreconcile evaluate                              # score against the ground truth
python -m payreconcile anomalies                             # fraud-signal model
python -m payreconcile replay --dataset gateway --date 2026-01-05   # re-check quarantined rows against current rules/reference data
streamlit run dashboard/app.py                               # dashboards
python scripts/demo_poisoned_batch.py                        # bad batch with and without the gate
python -m payreconcile verify-recon --reference              # incremental state == full rebuild (engine + independent dbt model)
python scripts/benchmark_reconciliation.py --days 30         # incremental vs full timing (--days 90 for the larger run)
pytest                                                       # 65 tests incl. end-to-end (~4 min)
```

Optional Prefect run (no server needed): `python -m payreconcile backfill --prefect --start ... --end ...`.
Minimum mode for a very small machine: `python -m payreconcile generate --days 10 --orders-per-day 500`.

## Testing

* **Unit:** every quality rule has a batch that must fail and one that must pass (duplicates, null keys, bad dates, invalid values,
  orphan foreign keys, missing/unexpected/renamed columns, thresholds, gate levels); SCD2 and upsert idempotency; API retries.
* **Reconciliation (20 tests):** pending -> matched, aged -> missing -> late settlement, orphan settlement then late txn, refund before
  original, refund after a resolved original (must not reopen it), corrected amounts, fuzzy/ambiguous/sticky matches, order and
  quarantine dependencies, duplicate flag on a late earlier charge, audit trail is append-only and explains state, replay does not
  rewind the clock, a simulated crash mid-apply leaves state and watermark untouched, forced full recompute repairs corruption.
  I mutation-checked them: breaking the scoping, aging or sticky logic makes tests fail.
* **dbt:** unique / not-null / relationships / accepted values, SCD2 integrity, "MATCHED means within tolerance", "every captured txn
  is reconciled exactly once", "no quarantined row in Gold", "the audit trail explains the current state".
* **End-to-end:** generate -> 10 daily runs with dbt -> score against ground truth -> rerun a day -> replay an old day -> incremental state equals a full rebuild (engine and dbt reference) -> bad schema
  halts and leaves Silver and Gold byte-for-byte unchanged -> API outage fails cleanly -> pipeline recovers -> dashboard renders.
* **CI:** GitHub Actions runs lint, unit tests, the end-to-end test, and checks the rule catalogue is in sync with the contracts.

## Repository layout

```
contracts/        data contracts and quality rules (YAML): the source of truth
config/           thresholds (settings.yml)
src/payreconcile/ generate, ingest, dq, silver, quarantine, reconcile (incremental engine), transform (dbt runner), pipeline,
                  flows (Prefect), replay, anomaly, evaluate, cli
dbt/              marts, tests, and the reference (full-rebuild) reconciliation models
dashboard/        Streamlit app
scripts/          poisoned-batch demo, reconciliation benchmark, rule-doc generator
tests/            unit + end-to-end
docs/             architecture, rule catalogue, cloud design, interview notes, measured results
.github/          CI
```

## Limitations

* **Synthetic data**, with defect types that match the rules; real feeds would need iteration and would surface cases I did not think of.
* **One known miss.** 17 duplicate charges are not found because their original payment was itself quarantined, so there is nothing to pair with.
* The gateway "API" is a file-based stand-in; retry logic is tested with injected failures, not a real network.
* **Incremental reconciliation still scans Silver tables** in several statements, so its runtime grows with history (slowly). Its
  measured wall-clock gain over a full in-process rebuild is small at this data size (see the benchmark); sticky matches can differ
  from a from-scratch rebuild in rare ambiguous cases; open items are tracked but not aged out.
* Isolation Forest did not beat a simple rule here; the injected anomalies are big and synthetic.
* No PII handling, authentication, encryption or cloud deployment. See [docs/cloud_design.md](docs/cloud_design.md) for what production needs.
* No screenshots are committed; run the dashboard to see it.

## Future work
Alerting hooks; partitioning Silver by date so scans stay bounded;  deployment to one cloud to turn the
design document into implemented experience.
