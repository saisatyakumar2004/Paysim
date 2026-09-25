# Architecture and design decisions

## Data flow

```
SOURCES (synthetic, messy)                      LOCAL STORAGE                       SERVING
─────────────────────────                       ─────────────                       ───────
gateway   paginated JSON  ─┐
settlements  daily CSV     ├─► extract ─► BRONZE  Parquet, partitioned by ingest_date, all strings + lineage cols
orders    SQLite change log┤   (retries,          │
merchants weekly CSV       ┘    watermark)        ▼
                                          SCHEMA CONTRACT  required / unexpected / renamed columns
                                                  │   (missing column = reject file)
                                                  ▼
                                          DATA-QUALITY ENGINE  YAML rules -> DuckDB SQL, PASS/WARNING/FAIL
                                             │            │
                                   bad rows  ▼            ▼ good rows
                                  QUARANTINE           batch checks: volume, freshness, distribution
                                  (raw payload +             │
                                   rule ids)                 ▼
                                                     QUALITY GATE  score + any FAIL -> PASS / WARN / HALT
                                                             │ HALT: nothing is loaded, Gold is not rebuilt,
                                                             │       the previous good Gold keeps serving
                                                             ▼ PASS/WARN
                                                        SILVER  typed, repaired, de-duplicated, idempotent upserts
                                                             │  (merchants = SCD Type 2)
                                                             ▼
                                                   dbt: RECONCILIATION + STAR SCHEMA + MARTS  (GOLD)  + 44 tests
                                                             │
                              ┌──────────────────────────────┼───────────────────────────┐
                              ▼                              ▼                           ▼
                     Streamlit dashboards            anomaly model (trusted        ops.* monitoring tables
                     (5 tabs)                        data only; blocked on HALT)   (runs, tasks, DQ, stats)
```

Single-file warehouse: `data/warehouse.duckdb` with schemas `silver`, `quarantine`, `gold`, `ml`, `ops`.

## Layer contracts

| Layer | Contains | Never contains |
|---|---|---|
| Bronze | Raw values as received (strings), `_run_id`, `_source_file`, `_ingested_at`. Overwritten per `ingest_date`. | Casts, cleaning, dropped rows |
| Quarantine | Rejected rows with `rule_ids` and the raw payload, per run | Anything that is also in Silver (except the second copy of a duplicate) |
| Silver | Typed, repaired (`₹1,299.00` -> 1299.0, `dd/mm/YYYY` dates), de-duplicated rows | Rows that failed an ERROR/CRITICAL rule |
| Gold | Reconciliation items, star schema, KPI marts, built by dbt from Silver | Anything not in Silver |

## Reconciliation logic (dbt)

1. **Exact pass:** normalised bank reference (`upper`, strip `GW-`) = gateway `txn_id`.
2. **Fuzzy pass** on what is left: same merchant, amount within tolerance, settled inside the SLA window, Levenshtein distance <= 2
   between references, **and the pair is unique from both sides**. Ambiguous candidates are never guessed; they surface as exceptions.
3. **Status per gateway movement:** `MATCHED`, `AMOUNT_MISMATCH` (difference above tolerance), `MISSING_SETTLEMENT` (none after the SLA),
   `PENDING` (not yet due), `ORPHAN_REFUND` (refund of a txn that exists nowhere).
4. **Settlement-side items:** `ORPHAN_SETTLEMENT`, `SOURCE_QUARANTINED` (money moved for a gateway row we rejected: not an orphan, an
   upstream quality problem), `SETTLED_NOT_CAPTURED`.
5. **Order-side items:** `ORDER_WITHOUT_PAYMENT`, `PAYMENT_QUARANTINED`; per-payment flags `NO_ORDER`, `AMOUNT_DIFF`.
6. **Duplicate charge:** same order, merchant and amount, captured again within 10 minutes under a new `txn_id`.
7. `money_at_risk` is defined per status (see `fact_reconciliation.sql`); duplicate exposure is tracked separately.

The reconciliation is a **full rebuild** from Silver on every run (deterministic, so idempotent), not an incremental model: at this
volume it takes seconds, and it is the only way for a late-arriving gateway record to *retroactively* clear an earlier orphan.
`as_of` is the latest day ever loaded, so replaying an old day cannot move the clock backwards (covered by a test).

## Decisions

| Decision | Why | Alternative not taken |
|---|---|---|
| DuckDB single file | Analytical SQL (windows, `QUALIFY`, `levenshtein`) with ~no idle memory; peak ~430 MB for a daily run | PostgreSQL: a server process for no extra learning |
| Custom YAML rule engine | Every rule is readable SQL I can explain; row-level quarantine and a score are first-class | Great Expectations: heavier configuration and memory, and it obscures the mechanics for a portfolio piece |
| dbt for Gold | Reconciliation is SQL-shaped; tests, docs and lineage come with it | Python/pandas joins: harder to test and review |
| dbt as a subprocess | DuckDB allows one writer process; isolates dbt's global state | In-process `dbtRunner` (tried it: it kept its own DuckDB connection open and broke other connections in the same process) |
| Prefect (optional) | In-process, no server, retries and state | Airflow: scheduler + webserver + metadata DB is too heavy for 8 GB; described in `cloud_design.md` |
| Halt = load nothing | Half-loaded batches are harder to reason about than a missed day | Load valid rows anyway and hope |
| Synthetic data with ground truth | Detection quality is *measured*; no licensing questions | Public datasets: no answer key |

## Gate semantics

`gate_decision` returns:
* **HALT** if any dataset has a FAIL rule (including a missing required column, an API outage yielding no rows, a 5x scale change) or the
  overall score is below 0.90. Nothing from that batch is loaded and Gold is not rebuilt.
* **WARN** if there are warnings or the score is below 0.95. The batch is loaded.
* **PASS** otherwise.

Score = severity-weighted mean of `1 - violations/evaluated` per rule, then a dataset-weighted mean across datasets (gateway and
settlements weigh 3, orders 2, merchants 1).

**Important property, shown by `scripts/demo_poisoned_batch.py`:** the score alone is not enough. The paise-scaled batch scores 0.9988
because every row is individually valid. Only the batch-level distribution rule stops it.
