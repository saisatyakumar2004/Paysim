# Interview notes: PayReconcile

Honest answers, written from what is actually in this repo. Numbers are from `docs/results_*.{txt,json}` (synthetic data).

## The 60-second pitch
"Payment gateways, banks and order systems each hold a version of the same money. I built a pipeline that ingests the three,
rejects bad records into a quarantine table, reconciles them in dbt, and reports money at risk. A quality gate stops a bad batch
before it reaches the warehouse or the fraud model. Because the data is synthetic, I know the right answer for every record, so I
measured detection instead of just claiming it."

## Data engineering
**Why quarantine instead of dropping bad rows?** Dropped rows disappear silently. Quarantine keeps the raw payload and rule ids so the
data owner can fix and replay, and reconciliation can tell "the bank paid for a payment we rejected" (`SOURCE_QUARANTINED`, an upstream
quality issue) from a real orphan.

**How is it idempotent?** Bronze partitions are overwritten per `ingest_date`; Silver uses keyed upserts (delete + insert, or "an older
version never overwrites a newer one" for orders); SCD2 ignores a snapshot older than the current version; unchanged rows are not rewritten or re-logged; the reconciliation only writes real
differences. Tests rerun a day, and replay an old day, and assert Silver counts, status counts and money are unchanged.

**Replaying an old day: what went wrong once?** My first version passed the replayed day as the reconciliation `as_of` date, which moved
the clock backwards and flipped MISSING items back to PENDING. I found it while measuring memory (I re-ran a day on the real warehouse),
fixed it (`as_of` = latest day ever loaded) and added a regression test.

**How does incremental loading work?** Orders come from a change log: `updated_at >= min(watermark - 1 day, run day start)` and
`< run day + 1`. The overlap catches late updates; the `min()` makes replays of old days work. Gateway and settlements are date-partitioned
files; late-arriving gateway records land in a later file and the change-log-driven reconciliation picks them up (see below).

**Why does a halted batch load nothing?** A half-loaded day is hard to reason about and hard to replay. The previous good Gold keeps serving,
the failure is visible in `ops.run_log`, and re-running the day after the fix is safe.

**What does the gate catch that row-level rules cannot?** A whole batch being wrong while every row is valid. In the demo the gateway
switches to paise (amounts x100): every row passes, the row-level score is 0.9988, but the distribution rule (batch mean vs trailing
median) fails it. Without the gate, 24 merchants were flagged as anomalous on that day versus 2 normally.

**Why dbt as a subprocess?** DuckDB allows one writer process. In-process `dbtRunner` kept its own connection open and broke later
connections in the same process; a subprocess is isolated and mirrors how an orchestrator would run it.

**Why not Spark/Kafka/Airflow?** Data volume is ~100k rows a day; they add operational cost, not skill signal. I can explain what they
would change (see `cloud_design.md`) but I have not run them.

**How would you scale it?** Partitioned object storage, a warehouse, block the fuzzy join on merchant and date and run it only over open
items, partition Silver by date so the remaining scans become bounded look-backs (SLA + late-arrival window). The reconciliation is
already incremental locally.

## Incremental reconciliation

**Why did you make it incremental?** To bound daily work and to get things a rebuild cannot give: a stable, auditable history of status
changes and matches that are not silently re-decided when unrelated data arrives. Speed was a hoped-for benefit, and I measured it.

**And did it get faster?** Not much, and I say so. The daily run evaluates about 6k items whether the state holds 29k or 272k (20% -> 2%).
Versus the same code run from scratch it is 0.7x on a tiny history, 1.2x at 30 days and 1.6x at 90 (0.51 s vs 0.81 s), because DuckDB
recomputes 272k items in under a second. Versus the old dbt path it is 24-39x, but that includes ~9 s of dbt start-up, so it flatters the
algorithm. End to end, the 30-day backfill took 385 s versus 388 s, since the daily dbt run dominates. I would not extrapolate past the
272k items I measured.

**How do you know it is correct?** Three implementations must agree: the incremental state, the engine's own full mode, and a separate
dbt SQL model. After 30 and 90 days there are 0 differences. Twenty unit tests cover late settlements, late refunds, orphans, corrected
amounts, sticky and ambiguous matches, quarantine and order dependencies, the audit trail, crash safety and idempotency. I also broke
the scoping, aging and stickiness logic on purpose to confirm tests fail (three of four mutants were caught; the fourth was equivalent
because closure already covered it).

**How does it find what to re-evaluate?** Silver upserts log every new or changed key to a change log (plus the old value of the order id
or bank reference of a replaced row); quarantine changes are logged too. The scope is those keys, closed over their dependencies (match
partner, refunds that reference them, siblings on the same order, order items), plus items that change with time (PENDING past the SLA,
orders that just became old enough to judge), plus unmatched records that could fuzzy-compete with a scoped one.

**What is a sticky match and why?** A resolved link is only re-examined when one of its records changes. Otherwise a look-alike
transaction arriving next week could silently un-match money that finance already signed off. The cost is a rare difference from a
rebuild in ambiguous fuzzy cases; a test pins it down and `verify-recon` reports it.

**How is the audit trail guaranteed?** The item diff, audit rows, link changes and the change-log watermark are written in one
transaction, so a crash leaves the old state and a rerun redoes the same work. The audit table is append-only, records the reason
(`NEW_ITEM`, `INPUT_CHANGE`, `AGED`, `RESOLVED`), and a dbt test asserts the last row per item explains the current status.

**A refund arrives after the original was resolved. What happens?** Only the refund's item is evaluated. The original keeps its row,
run id and audit history (tested).

**What would you do next?** Partition Silver by date so scans are bounded look-backs, then re-benchmark at 10-100x the data. I would
not claim a speedup at scale before measuring it.

## SQL (all in `dbt/models` and `src/payreconcile/silver.py`)
* **Fuzzy match with uniqueness:** candidates joined on merchant, amount and date window, `levenshtein <= 2`, then
  `count(*) over (partition by txn_id)` and `... by settlement_id` must both be 1.
* **Dedup:** `row_number() over (partition by key order by ...) = 1` / `QUALIFY`.
* **SCD2:** hash of tracked attributes; close the old row (`valid_to = snapshot date`), insert the new one; facts join on
  `valid_from <= date < valid_to`. Test: exactly one current row per merchant and no overlapping windows.
* **Anti-joins** for orphans (`NOT IN` here is safe because the sub-selects filter NULL keys; otherwise use `NOT EXISTS`).
* **Fan-out trap:** a payment can have several settlement candidates; joining without the uniqueness step would double-count money.
* **Filtered aggregates:** `count(*) filter (where status = ...)` for the daily scorecard; `quantile_cont` for settlement-lag p95.

## Analytics
* **Match rate** excludes PENDING (settlement not yet due), otherwise recent days look artificially bad.
* **Money at risk** is defined per status and shown by status; duplicate-charge exposure is deliberately separate (that money was
  settled correctly, the customer was charged wrongly).
* **A KPI dropped:** check Data health first (freshness, volume, quarantine rate) before assuming a business cause.

## Data science
**Why merchant-day z-scores?** Fraud patterns (velocity bursts, card testing, refund spikes) are a merchant deviating from its own history.
Baselines use previous days only (expanding mean/std shifted by one) so there is no look-ahead leakage.

**Result and honesty:** on 4,600 eligible merchant-days with 30 injected anomalies, Isolation Forest got precision 0.43 / recall 0.67 and a
simple "any z >= 4" rule got precision 0.61 / recall 1.00. The simple rule won. The injected anomalies are large by construction, so I would
not claim either number transfers to real fraud. The point is the trust boundary: features come from Silver only, and the model refuses to
refresh if the latest run was halted by the gate.

**Contamination = 0.01** was set a priori (about the expected anomaly rate) and not tuned against the answer key.

## Limitations I would volunteer
* Synthetic data whose defect types match the rules I wrote. The 100% status accuracy proves the logic is self-consistent, not that it
  works on real feeds. Real data would need iteration with the data owners.
* One known miss: 17 duplicate charges are not found (their original payment was itself quarantined, so nothing to pair with).
* A valid duplicate charge that crossed midnight was quarantined by my first version of the timestamp rule; I added a 24h tolerance and
  the false positive disappeared (precision 99.97% -> 100%).
* `replay` re-evaluates quarantined rows after reference data or a rule changes (e.g. a merchant onboarded late).
* Gateway "API" is a file-based stand-in; retries are tested with injected failures, not a real network.
* No PII handling, auth, encryption or cloud deployment.
* The incremental reconciliation's measured wall-clock gain over a full in-process rebuild is small at this size (1.2x at 30 days, 1.6x
  at 90) and end-to-end daily runtime did not improve (dbt dominates); its value here is bounded work, stable matches, an audit trail
  and safe reruns. Its runtime still creeps up with history because some statements scan whole tables.
* Sticky matches can differ from a from-scratch rebuild in rare ambiguous fuzzy cases (documented and tested).

## Resume bullets (numbers are measured; keep the "synthetic" qualifier)
**Data Engineer**
* Built a Bronze/Silver/Gold payments-reconciliation pipeline (Python, DuckDB, dbt, Parquet) over 3 sources with a schema-contract check,
  YAML-driven quality rules, a quarantine table and a quality gate that halts bad batches before they reach the warehouse or model.
* Implemented incremental, idempotent loads (watermark with overlap, keyed upserts, SCD2 merchant dimension) verified by tests that replay
  days; 65 automated tests plus 45 dbt tests run on every push in GitHub Actions.
* Built an incremental reconciliation engine (change-log scope, sticky matches, one-transaction apply, append-only audit trail) that
  re-evaluates 2-7% of state items per run and matches a from-scratch rebuild exactly (0 differences over 272,509 items, checked against
  two independent implementations); measured 1.6x faster than a full in-process rebuild at 90 days (see the honest caveats).
* Scored the pipeline against a seeded answer key: 100% recall and precision on 3,733 injected bad rows and 100% reconciliation-status accuracy on
  ~87k synthetic transactions (self-consistency check on synthetic data).

**Data Analyst**
* Defined reconciliation KPIs (match rate, money at risk by status, settlement lag, duplicate-charge exposure) and built a five-tab
  Streamlit dashboard for finance operations and data health; surfaced INR 2.58M of synthetic money at risk across 5 exception types.
* Wrote window-function/CTE SQL for fuzzy matching, deduplication, SCD2 and daily scorecards in dbt with tests.

**Data Scientist**
* Built a merchant-day fraud-signal model (Isolation Forest vs a z-score rule baseline) with leakage-free per-merchant baselines and
  evaluated it against injected patterns; reported that the simple rule outperformed the model (recall 1.00 vs 0.67).
* Blocked bad data from reaching the model: a rupees-to-paise batch that passes all row checks was halted by a distribution check
  (24 vs 2 spurious flagged merchants without the gate).
