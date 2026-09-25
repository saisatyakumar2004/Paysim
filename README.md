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
orders (SQLite CDC) ┤   (raw)                          │    │            HALT = load nothing        (reconciliation,   anomaly model
merchants (CSV)     ┘                         quarantine ◄┘    └─ volume / freshness / distribution   star schema, SCD2)
```

* **Ingestion:** paginated API client with retry and backoff; CDC-style orders extract with a watermark and overlap; weekly merchant
  snapshots feeding an SCD Type 2 dimension.
* **Quality:** 4 YAML contracts, ~30 rules ([catalogue](docs/dq_rules.md)), PASS / WARNING / FAIL per rule, a weighted score, row-level quarantine
  with the raw payload, and batch-level volume, freshness and distribution checks.
* **Reconciliation:** exact match, then a fuzzy match (same merchant, amount and window, edit distance <= 2, unique from both sides).
  Statuses: `MATCHED`, `AMOUNT_MISMATCH`, `MISSING_SETTLEMENT`, `PENDING`, `ORPHAN_SETTLEMENT`, `ORPHAN_REFUND`, `SOURCE_QUARANTINED`,
  `ORDER_WITHOUT_PAYMENT`, plus duplicate-charge detection.
* **Gold:** star schema (`fact_transactions`, `fact_settlements`, `fact_reconciliation`, SCD2 `dim_merchant`, `dim_date`) and marts
  (daily scorecard, merchant exceptions, settlement lag). 44 dbt tests, including "no quarantined row exists in Gold" and SCD2 integrity.
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
| Generate 30 days / full 30-day backfill (with dbt) | 54 s / 388 s (about 13 s per day, dominated by dbt startup and build) |

**Honest reading:** the reconciliation and quality numbers are high because the generator and the rules share one defect taxonomy. They show the
logic is coherent and testable. The anomaly numbers are more informative: the simple rule beat the model, and I report that rather than tune it away.

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
pytest                                                       # 42 tests incl. end-to-end (~3 min)
```

Optional Prefect run (no server needed): `python -m payreconcile backfill --prefect --start ... --end ...`.
Minimum mode for a very small machine: `python -m payreconcile generate --days 10 --orders-per-day 500`.

## Testing

* **Unit:** every quality rule has a batch that must fail and one that must pass (duplicates, null keys, bad dates, invalid values,
  orphan foreign keys, missing/unexpected/renamed columns, thresholds, gate levels); SCD2 and upsert idempotency; API retries.
* **dbt:** unique / not-null / relationships / accepted values, SCD2 integrity, "MATCHED means within tolerance", "every captured txn
  is reconciled exactly once", "no quarantined row in Gold".
* **End-to-end:** generate -> 10 daily runs with dbt -> score against ground truth -> rerun a day -> replay an old day -> bad schema
  halts and leaves Silver and Gold byte-for-byte unchanged -> API outage fails cleanly -> pipeline recovers -> dashboard renders.
* **CI:** GitHub Actions runs lint, unit tests, the end-to-end test, and checks the rule catalogue is in sync with the contracts.

## Repository layout

```
contracts/        data contracts and quality rules (YAML): the source of truth
config/           thresholds (settings.yml)
src/payreconcile/ generate, ingest, dq, silver, transform (dbt runner), pipeline, flows (Prefect), anomaly, evaluate, cli
dbt/              intermediate + marts models, tests, profiles
dashboard/        Streamlit app
scripts/          poisoned-batch demo, rule-doc generator
tests/            unit + end-to-end
docs/             architecture, rule catalogue, cloud design, interview notes, measured results
.github/          CI
```

## Limitations

* **Synthetic data**, with defect types that match the rules; real feeds would need iteration and would surface cases I did not think of.
* **One known miss.** 17 duplicate charges are not found because their original payment was itself quarantined, so there is nothing to pair with.
* The gateway "API" is a file-based stand-in; retry logic is tested with injected failures, not a real network.
* The reconciliation is a **full rebuild** each run: right at this size (seconds), not at 1000x.
* Isolation Forest did not beat a simple rule here; the injected anomalies are big and synthetic.
* No PII handling, authentication, encryption or cloud deployment. See [docs/cloud_design.md](docs/cloud_design.md) for what production needs.
* No screenshots are committed; run the dashboard to see it.

## Future work
Incremental reconciliation over open items only; alerting hooks; deployment to one cloud to turn the
design document into implemented experience.
