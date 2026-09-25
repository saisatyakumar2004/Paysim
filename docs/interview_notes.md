# Interview preparation: PayReconcile

55 questions with the expected answer, **where it lives in the project**, and likely follow-ups. Numbers are measured and traceable
(`docs/benchmarks.md`, `docs/results_*`). Everything is synthetic; say so early. Never claim production or cloud experience.

**60-second pitch.** "A merchant is paid from three places that must agree: the payment gateway, the bank and the order system.
I built a platform that ingests the three, quarantines bad records, reconciles them with exact and constrained fuzzy matching, and
reports money at risk. The engine is incremental, every match is explainable and every status change is on an append-only audit.
Because the data is synthetic I know the right answer for every record, so I measured detection instead of asserting it, and I
also ran experiments that went against my expectations, like partitioned Parquet, which was slower."

---
## A. Data engineering (15)

**A1. Why DuckDB?** Analytical SQL (window functions, `QUALIFY`, `EXCEPT`, `levenshtein`) in-process with almost no idle memory; the
whole daily run peaks at 380 MB including dbt, on an 8 GB laptop. *In the project:* `src/payreconcile/db.py`, `docs/architecture.md`.
*Follow-ups:* concurrency (one writer process, so dbt runs as a subprocess), when it stops being enough (multi-writer, multi-TB), Postgres/warehouse comparison.

**A2. Why Parquet?** Columnar, typed, compresses well, maps directly to object storage. Bronze is Parquet by `ingest_date`. Note the
honest limit: I tested Parquet for **Silver** and it lost (A14). *In the project:* `ingest.write_bronze`. *Follow-ups:* row groups, predicate pushdown, small-file problem.

**A3. Why not Spark?** ~100k rows a day; Spark adds a JVM, a cluster and startup latency for no benefit here. I can explain what
would change at scale (partitioned object storage, a warehouse, blocked fuzzy joins) but have not run Spark. *In the project:* `docs/cloud_design.md`. *Follow-ups:* where would the fuzzy join partition (merchant and date), skew.

**A4. Why not Kafka?** Feeds are daily files or paginated API pages; there is no event stream to justify it. A fake streaming demo would be a buzzword. *Follow-ups:* how would late webhooks change the design (the change-log/late-arrival handling already exists).

**A5. Why Prefect, not Airflow?** Prefect runs in-process with no server (retries and state, almost no RAM); Airflow needs a scheduler, web server and metadata DB, too heavy for 8 GB. It is optional (`flows.py`); the CLI is the primary path. *Follow-ups:* how a DAG per day and sensors on the bank file would map, backfills.

**A6. Explain Bronze / Silver / Gold and the quarantine.** Bronze = raw strings + lineage; Quarantine = rejected rows with raw payload and rule ids; Silver = typed, repaired, de-duplicated; reconciliation state and Gold = derived only from Silver. *In the project:* layer table in `docs/architecture.md`. *Follow-ups:* why Bronze is strings, what "repair" means (amount symbols, date formats).

**A7. How do you guarantee idempotency?** Bronze partitions are overwritten per `ingest_date`; Silver upserts touch only new or changed rows (`EXCEPT`), so a replay is a true no-op; SCD2 ignores an older snapshot; reconciliation writes only real differences and the audit only status changes. Tests rerun a day, replay an old day, and run three failed attempts then one good one. *In the project:* `silver._upsert`, `tests/test_reconcile.py::test_rerunning_with_no_new_data_changes_nothing`, `tests/test_failure_recovery.py`. *Follow-ups:* what if the process dies mid-write (A11).

**A8. How do you handle late-arriving settlements?** A late record enters Silver, the change log records its key, and the next reconciliation touches its match partner, order and siblings: an orphan or `MISSING_SETTLEMENT` item resolves without a rebuild, and the change is on the audit with reason `INPUT_CHANGE`. Aging into `MISSING` with no new data is `AGED`. *In the project:* `tests/test_reconcile.py` (`..._ages_into_missing_and_a_very_late_settlement_still_resolves_it`). *Follow-ups:* a late refund after the original resolved (the original is not reopened; tested).

**A9. How do you handle schema drift?** A contract per dataset: missing required column -> whole file rejected and the gate halts; unexpected column -> dropped with a WARNING; renamed column -> resolved through an alias with a WARNING. The demo files include a rename (day 12) and an extra column (day 20). *In the project:* `contracts/*.yml`, `dq.evaluate`. *Follow-ups:* what if a column changes type (`GW-VAL-*` rules), versioned contracts.

**A10. How does incremental loading of orders work?** A change log with `updated_at`; extract `updated_at >= min(watermark - 1 day, run-day start)` and `< run day + 1`. The overlap catches late updates; the `min()` makes replaying an old day work. *In the project:* `ingest.read_orders`. *Follow-ups:* CDC vs snapshots, deletes.

**A11. What happens if the pipeline crashes mid-way?** Silver commits per dataset; reconciliation applies delta + audit + links + evidence + watermark in **one transaction**, so a crash leaves the previous state and a rerun catches up from the change log. Tested with an injected failure in the middle of the apply, a crash between Silver and reconciliation, and a dbt failure. *In the project:* `tests/test_reconcile.py::test_a_crash_mid_apply...`, `tests/test_failure_recovery.py`. *Follow-ups:* what is not atomic (Silver vs reconciliation across the boundary; recovery is by the watermark).

**A12. Why is dbt run as a subprocess?** DuckDB allows a single writer process; running dbt's runner in-process kept its own connection open and broke later connections. A subprocess isolates it and mirrors an orchestrator. *In the project:* `transform.py`. *Follow-ups:* the cost (startup ~9 s of the run).

**A13. Tell me about the HTTP API.** A small FastAPI service serving the same seeded records (`/health`, `/transactions?date&page&page_size`), and a client with a per-request timeout, retries for transport errors/timeouts/429/5xx with capped exponential backoff, honouring `Retry-After`, no retry on other 4xx or malformed payloads. Tests inject 503s, delays and outages and prove API and file paths are interchangeable. It is a fixture for realism, not a service claim. *In the project:* `gateway_api.py`, `ingest.HttpGatewayClient`, `tests/test_gateway_api.py`. *Follow-ups:* idempotent retries (GET only), circuit breakers, auth (not implemented).

**A14. You tested partitioned Parquet. What happened?** I expected date partitions to cut scan cost. On 1x to 50x it made the unchanged engine 1.4x to 7.1x **slower** (4-5x more rows scanned) with identical results; at 100x only the table layout was run. The engine looks up by key, not date, so pruning never applies and re-reading many small files costs more. Ingestion was faster but is not the bottleneck and skips the change log. Decision: keep tables. A date-bounded engine is a possible next experiment (a date-bounded lookup was 5 to 12x faster in a micro-benchmark) but is **not built**. *In the project:* `scripts/benchmark_partitioning.py`, `docs/benchmarks.md`. *Follow-ups:* how you controlled the experiment (same data, same batch, state equality check), what you did not test.

**A15. How would you scale it 100x? What breaks first?** Measured to 8.6 M items on this laptop: incremental compute 8.76 s, full recompute 75.5 s, initial full build 157 s and 2.5 GB peak. Incremental compute still grows with history because statements scan whole tables. Next steps: bound scans by date with an unbounded fallback for late arrivals, block the fuzzy join by merchant and date, move to a warehouse. I would not claim a speedup before measuring. *Follow-ups:* memory limits, what to partition, incremental dbt.

---
## B. SQL (10)
All live in `src/payreconcile/reconcile.py`, `silver.py` and `dbt/models`.

**B1. Fuzzy match with a uniqueness rule.** Join open txns to open settlements on merchant, amount within tolerance, date window, `levenshtein <= 2`; then `count(*) over (partition by txn_id)` and `... by settlement_id` must both equal 1. *Follow-ups:* why not pick the closest (silent false matches), complexity of the join.

**B2. Dedup keeping the latest.** `row_number() over (partition by key order by updated_at desc) = 1` or `QUALIFY`. Used for order change-log rows.

**B3. SCD2 close-and-open.** Compare a hash of tracked attributes; close the current row (`valid_to = snapshot date`, `is_current = false`), insert a new current row; facts join on `valid_from <= date < valid_to`. Tests: one current row per merchant, no overlapping windows, and an older snapshot never overwrites a newer version.

**B4. Change detection with `EXCEPT`.** `SELECT ... FROM stg EXCEPT SELECT ... FROM silver` returns new or changed rows (NULL-safe), so identical rows are never rewritten. *Follow-ups:* `EXCEPT` vs hash compare vs `MERGE`.

**B5. `NOT IN` vs `NOT EXISTS`.** `NOT IN` returns nothing if the subquery contains a NULL; I use `NOT IN` only where the subquery filters NULL keys, otherwise `NOT EXISTS` / anti-join.

**B6. Filtered aggregates.** `count(*) filter (where status = 'MATCHED')` for the scorecard; `quantile_cont` for settlement-lag p95.

**B7. Join fan-out.** A payment can have several settlement candidates; joining without the uniqueness step double-counts money. Aggregate first or join at the right grain; dedup duplicate-charge joins with `group by txn_id` (a txn can have several earlier charges).

**B8. Date arithmetic and ageing.** `settlement_date - cast(created_at as date)`; exception age = as-of date minus event date, bucketed 0-3/4-7/8-14/15-30/31+. `PENDING` when `txn_date + sla < as_of` is false.

**B9. Late-arriving data in SQL.** Full rebuild vs scoped recompute: the scope is built with `UNION` of change-log keys and their dependants, then the same item SQL runs over the scope. *Follow-ups:* how you prove equivalence (three implementations).

**B10. Which query is slowest and how do you know?** Profiling per statement (DuckDB `cumulative_rows_scanned`): no single statement dominates; ~60 statements each scan Silver, so rows scanned grows with history (6.3 M at 1x to 509 M at 100x).

---
## C. Reconciliation (10)

**C1. Why fuzzy matching at all?** Banks mistype references; without it a correct payment shows as one missing settlement plus one orphan. It is constrained (merchant, amount, window, edit distance) and unique from both sides. On the synthetic data: 906 fuzzy matches, 0 wrong.

**C2. How do you prevent false matches?** Hard constraints, then a uniqueness rule: if a txn has two candidate settlements (or vice versa) nothing is linked and the candidates are recorded as `AMBIGUOUS_NOT_LINKED` with the reason. Test: two look-alike txns and one typo'd reference stay unmatched.

**C3. How do you audit a match?** `gold.recon_match_evidence` keeps features, candidate counts, score, threshold, decision and reason for every exact link and every fuzzy candidate; `explain` and the dashboard show it with the audit history and source records. *Honest note:* the "score" is reference similarity; the decision is the uniqueness rule.

**C4. What is a sticky match and why?** A resolved link is re-examined only if one of its records changes, so a look-alike arriving later cannot silently un-match money that was signed off. Cost: a rare difference from a from-scratch rebuild in ambiguous fuzzy cases (tested and documented).

**C5. How does duplicate-charge detection work?** Confirmed: two trusted captured payments for one order, same merchant and amount, within 10 minutes. Suspected: one trusted payment plus a quarantined attempt on the same order that is not contradicted by its own fields. Two tiers, never merged. Measured: confirmed 677/694 (R 97.55%), combined 691/694 (R 99.57%), precision 100%.

**C6. Why not lower the threshold to catch the last 3 duplicates?** All 3 have an original quarantined *without an order id*; the only signal left is merchant + amount + time. On this data it finds them with 0 false positives, but the amounts are continuous, so that says nothing about fixed-price merchants where many legitimate payments share merchant, amount and minute. I documented it and did not adopt it. *In the project:* `docs/investigation_misses.md`.

**C7. Why can't you flag a payment without an order when the payment was quarantined?** It is not a trusted item. It stays visible in quarantine with its rule ids and its bank money shows as `SOURCE_QUARANTINED`. On payments that reached trusted data recall is 597/597; the 12 misses are all quarantined payments (98.03% overall).

**C8. PENDING vs MISSING_SETTLEMENT?** `PENDING` = inside the settlement SLA (3 days); `MISSING_SETTLEMENT` = past it with no settlement. The match rate excludes PENDING from the denominator so recent days do not look bad.

**C9. How do refunds reconcile?** A refund expects a negative settlement. A refund whose original exists nowhere is `ORPHAN_REFUND`; if the original arrives late the refund re-resolves; a refund arriving after the original was resolved does not reopen the original (tested).

**C10. How is `money_at_risk` defined?** Per status: mismatch = |settled - expected|; missing = expected; orphan settlement = settled; orphan refund = refund amount; order without payment = order amount. It is exposure, not confirmed loss; duplicate exposure is separate. `docs/kpi_definitions.md`.

---
## D. Data quality (5)

**D1. Why quarantine instead of dropping bad rows?** Dropped rows vanish silently and cannot be fixed or replayed. Quarantine keeps the raw payload and rule ids; reconciliation can also tell "the bank paid for a payment we rejected" (`SOURCE_QUARANTINED`) from a real orphan.

**D2. Explain the score and the gate.** Severity-weighted mean of `1 - violations/evaluated` per rule (CRITICAL 5, ERROR 3, WARN 1), dataset-weighted across datasets. Any FAIL or a score below 0.90 halts the batch (nothing loaded, Gold not rebuilt). *Key point:* the score is not enough.

**D3. What does the gate catch that row rules cannot?** A whole batch being wrong while every row is valid: a rupees-to-paise switch scores 0.9988 but fails the distribution rule (batch mean vs trailing median). Without the gate 24 merchants were flagged as anomalous that day vs 2 normally.

**D4. How do you handle bad reference data?** FK rules against the current merchant master; rows failing them are quarantined; when reference data arrives, `replay` releases them and the incremental reconciliation updates. Tested with merchants and orders.

**D5. How do you keep the rule catalogue truthful?** It is generated from the contracts and the engine (`scripts/gen_rule_docs.py`), lists dataset, field, severity, expectation, implementation and failure behaviour for all 50 rules, validates a coverage matrix, and CI fails if the doc is out of date.

---
## E. dbt (5)

**E1. Why dbt for marts but a Python engine for reconciliation?** Marts, tests and lineage suit dbt. The reconciliation needs scope closure, sticky links, an audit diff and one atomic apply, which a dbt incremental model cannot express cleanly. The old dbt SQL remains as an independent oracle.

**E2. What do your dbt tests prove?** unique/not-null/relationships/accepted values, SCD2 integrity, "MATCHED means within tolerance", "every captured txn is reconciled exactly once", "no quarantined row in Gold", "the audit trail explains the current state": 45 tests per daily build.

**E3. What is the reference model for?** An independent full-rebuild implementation of the same rules used by `verify-recon --reference` and the end-to-end test to prove the incremental state is correct. Excluded from the daily build by tag.

**E4. How is SCD2 tested?** Exactly one current row per merchant, no overlapping validity windows; facts join to the version valid at the event date; risk tier and fee history are never overwritten.

**E5. Why is dbt 89% of the run?** Start-up, parsing, materialising marts and 45 tests cost ~11 s regardless of data size, while ingestion + quality + Silver + reconciliation together take ~1.4 s. *In the project:* `docs/results_stage_resources_30day_backfill.json`.

---
## F. ML / anomaly detection (5)

**F1. Why did Isolation Forest perform worse than the baseline?** Measured F1 0.53 vs 0.76 (recall 0.67 vs 1.00). *Interpretation, not proven:* the injected anomalies are large deviations on single count features, exactly what a z-score rule looks for; Isolation Forest isolates in a 6-D space dominated by normal points, and refund spikes (one feature) were the weakest (recall 0.25). I did not tune it against the answer key. Lesson: try the simple baseline first.

**F2. How do you prevent leakage?** Prediction time = end of the merchant-day; feature cutoff = previous days only (expanding baseline shifted by one); inputs = gateway transactions only; labels read only after scoring. Executable tests: a day's features do not change when later days arrive, scrambling settlements/orders/reconciliation changes nothing, the scoring function has no label parameter.

**F3. Which metrics and why?** Precision, recall, F1 and explicit FP/FN: the classes are extremely imbalanced (30 of 4,600), so accuracy is meaningless.

**F4. What are the limits of the synthetic anomalies?** They are injected by my own generator (bursts, card-testing, refund spikes), large and clean. They show the method works on them, not that it detects real fraud. I call the output "merchant risk signals" and never "fraud detector".

**F5. Why is there no train/test split?** The model is unsupervised and the baseline is a fixed rule, so nothing is fit on labels; evaluation is against injected labels on the same merchant-days. A supervised model would need a time-based split.

---
## G. System design (5)

**G1. Design this for 100 M transactions a day.** Object storage (partitioned by date) for Bronze, a warehouse for Silver/Gold, orchestration by day with sensors, reconciliation as incremental warehouse models over a bounded look-back (SLA + late-arrival window) with the fuzzy join blocked by merchant and date, the change log as a table stream. Design only; none of it is implemented. `docs/cloud_design.md`.

**G2. Multiple banks and gateways?** Add a `source_system` key and a contract per feed; reconciliation keys become `(source, ref)`; settlement lag and SLA per partner (not possible today: one synthetic bank feed).

**G3. Near-real-time?** Keep the same change-log design and run the incremental engine on micro-batches; the sticky/audit semantics carry over. I have not built streaming.

**G4. PII and security?** Not implemented. Would need tokenised identifiers, column-level access, encryption at rest, secrets management and a data-classification review before any real data.

**G5. Monitoring and alerting?** Per-stage time, peak RSS, rows in/out/rejected, DQ scores and gate decisions are recorded and shown on the Pipeline Health page; alert hooks are **not** implemented (a HALT is visible in `ops.run_log`).

---
## Questions to prepare an honest answer for
* **Why does incremental processing not reduce end-to-end runtime?** dbt is 89% of a ~12.4 s run; the reconciliation step is 0.45 s. Incremental's benefit here is bounded work, stable matches, an audit trail and safe reruns. At larger history it does pay: full/incremental compute ratio 1.7x at 1x to 8.6x at 100x (measured).
* **What are the limitations of synthetic data?** Defects match my rules, precision figures are self-consistency, distributions are simpler than real ones, one bank feed, no fixed-price merchants, no PII.
* **What went wrong during the build?** A PowerShell edit mangled the rupee symbol, so the pipeline quarantined repairable values (found by measuring precision); replaying an old day rewound the reconciliation clock (regression test added); replay could overwrite newer trusted data with a stale copy (fixed, tested); an evaluator bug showed 49% duplicate recall when the real figure was 97%. Each is a test now.
