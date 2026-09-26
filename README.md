# PayReconcile

A payments reconciliation platform (Python, DuckDB, dbt, Streamlit) built around one question:

> **Where exactly did the money go missing?**

A customer pays, the gateway captures, fees are deducted, the bank settles. When these disagree, PayReconcile does not just compare two tables. It follows every chain of money through the stages the source data supports, finds the **earliest stage where the amounts stop agreeing**, names the cause, keeps the records that prove it, and says plainly when the data cannot support a conclusion.

> All data is synthetic (a seeded generator that also records what it injected). Results below are measured against that known truth; they show the logic is coherent and testable, not that it works on real feeds. See [Limitations](#limitations).

The finished system answers, from `python -m payreconcile money-flow`:

> INR X was expected from customers (orders). INR Y reached the gateway (INR R refunded). INR Z should have settled after fees (INR F). INR A reached the bank. The merchant-side amount is not in the source data, so the chain stops at the bank. INR C is currently unexplained, a further INR D cannot be judged because evidence is blocked or ambiguous, and INR E is explained. The earliest confirmed breakpoint is STAGE, supported by chain ids.

This repository holds two analytics projects on one local DuckDB + Parquet + Streamlit platform: **Part 1, PayReconcile** (order -> gateway -> bank money-flow reconciliation, below) and **[Part 2, PaySim](#part-2-paysim-transaction-and-fraud-analytics)** (a 6.36-million-row transaction and fraud-analytics pipeline). They share tooling, not data: PaySim has no orders, gateway or settlements.

## Contents
[Architecture](#architecture-and-data-flow) · [Source data](#source-data-what-the-model-can-and-cannot-see) · [Setup and commands](#setup-and-commands) · [Reconciliation engine](#reconciliation-engine) · [Money-flow methodology](#money-flow-methodology) · [Ground truth](#ground-truth-and-evaluation) · [Dashboard](#dashboard) · [Power BI](#power-bi-export) · [Testing](#testing) · [Measured results](#measured-results) · [Limitations](#limitations) · [Part 2: PaySim](#part-2-paysim-transaction-and-fraud-analytics)

## Architecture and data flow

```
gateway (JSON API / files)  -+
bank settlements (CSV)       +-> Bronze (Parquet) -> schema contract -> quality rules -> QUALITY GATE -> Silver (DuckDB)
order system (SQLite CDC)    |                                   |                          HALT = load nothing
merchant master (CSV, SCD2) -+                          quarantine (raw payload + rule ids) <- replay

Silver -> reconciliation engine (exact + constrained fuzzy match, duplicate charges, sticky links, audit)  -> gold.fact_reconciliation
       -> money-flow lineage (order -> gateway -> fee -> expected settlement -> bank)                      -> gold.fact_money_flow
                                                                                                              gold.money_flow_evidence
       -> dbt marts + tests -> Streamlit dashboard (Money Flow is the default page) -> Power BI-ready export
```

Runs on a laptop; no Spark, Kafka, Airflow or cloud services. One daily run = extract, Bronze, validate, quality gate, Silver, reconcile, money flow, dbt build and tests.

## Source data (what the model can and cannot see)

| Source | Amounts | Identifiers | Dates |
|---|---|---|---|
| Orders (SQLite change log) | `order_amount`, `order_status` | `order_id`, `merchant_id`, `customer_id` | `created_at`, `updated_at` |
| Gateway (paginated JSON) | `amount` of a `payment` or `refund`, `status` captured / failed / pending | `txn_id`, `order_id`, `merchant_id`, `refund_of` | `created_at` |
| Bank settlements (daily CSV) | `gross_amount`, `fee_amount`, `net_amount` | `settlement_id`, `gateway_ref` (the bank's reference to the gateway `txn_id`), `merchant_id` | `settlement_date` |
| Merchant master (weekly CSV) | `fee_pct`, kept as history in an SCD Type 2 table | `merchant_id` | `valid_from`, `valid_to` |
| Quarantine | raw payload of every rejected record | `natural_key`, `order_id`, `rule_ids` | `run_date` |
| Run log | which batches were loaded and which the quality gate halted | dataset | `run_date` |

What is **not** in the data, and therefore never invented: an amount received by the merchant (the chain stops at the bank), a separate "customer paid" amount (the order is the expectation and the gateway capture is what was charged), a gateway fee field (the fee appears only on the settlement row and, as a rate, in the merchant master), a batch reference on settlements, and adjustments or chargebacks.

## Setup and commands

Windows PowerShell, Python 3.11+.

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -e ".[dev,dashboard,api,orchestration]"

python -m payreconcile generate --days 30                              # data/source + data/truth (seed 42)
python -m payreconcile backfill --start 2026-01-01 --end 2026-01-30    # daily pipeline; add --no-dbt to skip dbt
python -m payreconcile money-flow                                      # the answer sentence, waterfall, invariants
python -m payreconcile money-flow --flow TXN:TXN1234567890             # one chain, stage by stage, with its evidence
python -m payreconcile evaluate                                        # score everything against the injected truth
python -m payreconcile verify-recon --reference                        # incremental == full rebuild == dbt reference
python -m payreconcile explain TXN:TXN1234567890                       # engine status history, match evidence, money flow
python -m payreconcile replay --dataset gateway --date 2026-01-05      # re-check quarantined rows
python -m payreconcile export-bi --out exports/powerbi                 # Power BI-ready tables
python -m payreconcile serve-api --port 8765                           # local gateway API (then: run --date ... --source api)
streamlit run dashboard/app.py                                         # dashboard
pytest                                                                 # tests
ruff check .                                                           # lint
python scripts/scan_secrets.py                                         # secrets / personal-data scan
```

Smaller data set: `generate --days 10 --orders-per-day 500`. `generate --financial-defects 0` turns the fee / late / duplicate / split / aggregated settlement scenarios off. No secrets are needed (`.env.example`).

## Reconciliation engine

`payreconcile.reconcile` matches each captured gateway transaction to bank settlement rows and keeps `gold.fact_reconciliation` (one item per transaction, unlinked settlement, or paid order without payment), `gold.recon_links` (transaction to settlement rows), `gold.recon_match_evidence` (why records were or were not matched) and an append-only `gold.reconciliation_audit`.

1. **Exact** match on the normalised bank reference; **constrained fuzzy** match for what is left (same merchant, amount within tolerance, settled inside the SLA window, reference within 2 edits, unique from both sides; ambiguity is never guessed).
2. A transaction may link to **several bank rows** (a split payout or a duplicate settlement). The settled amount is their sum, so a double payment is a visible mismatch and `item_id` stays unique.
3. Duplicate charges are reported in two tiers that are never merged: *confirmed* (two trusted payments) and *suspected* (one trusted payment plus a quarantined attempt on the same order).
4. Incremental: only the scope derived from `ops.change_log` (plus items that age past the SLA) is re-evaluated, links are sticky, and state, audit and watermark are applied in one transaction. `verify-recon` proves it equals a from-scratch rebuild and an independent dbt reference model.

Engine statuses (`MATCHED`, `AMOUNT_MISMATCH`, `MISSING_SETTLEMENT`, `PENDING`, `ORPHAN_REFUND`, `ORPHAN_SETTLEMENT`, `SETTLED_NOT_CAPTURED`, `SOURCE_QUARANTINED`, `ORDER_WITHOUT_PAYMENT`, `PAYMENT_QUARANTINED`) are unchanged. Their `money_at_risk` is on a gross basis. The money-flow layer below is the net-settlement view and is what the Money Flow page uses.

## Money-flow methodology

`payreconcile.moneyflow` rebuilds `gold.fact_money_flow` after every reconciliation. It is a deterministic function of Silver, quarantine, the reconciliation state and the run log, so incremental and full reconciliation give identical rows.

### Chains

One row per money chain; every bank record and every order belongs to exactly one chain, so no money is counted twice.

| Chain id | Flow type | What it is |
|---|---|---|
| `TXN:<txn_id>` | PAYMENT, REFUND | a captured gateway payment or refund with its order and its bank rows |
| `QTX:<txn_id>` | QUARANTINED_PAYMENT | a gateway record that failed validation, with the bank rows and order that reference it |
| `ORD:<order_id>` | ORDER_ONLY | a paid order no payment chain can claim |
| `STL:<settlement_id>` | SETTLEMENT_ONLY | a bank record no payment can claim |

An order amount is carried by exactly one chain (the earliest payment that is not a flagged duplicate). A second capture carries expectation 0, so it shows as a gateway gap instead of being hidden.

### The reconciliation equation

All amounts in INR; `tol` = 1.00 (gross), `fee_tol` = 0.05 (fee and net arithmetic).

```
expected_gateway = order_amount                  0 if the order is absent or carried by another capture
expected_fee     = round_half_up(gateway_gross x fee_pct / 100, 2)     fee_pct = merchant master version valid on the capture date
                                                                       (refunds: money_flow.refund_fee_pct = 0)
expected_net     = gateway_gross - expected_fee
bank_net         = sum of the linked settlement rows' net_amount

gap_gateway      = gateway_gross - expected_gateway                                  gateway vs order
bank_net - expected_net  =  gap_bank_amount + gap_fee + gap_bank_net                 exact, no residual
  gap_bank_amount = (bank_gross - schedule_fee(bank_gross)) - expected_net           gross settled differs from captured
  gap_fee         = schedule_fee(bank_gross) - bank_fee                              fee charged differs from the schedule
  gap_bank_net    = bank_net - (bank_gross - bank_fee)                               bank's own net is not gross - fee
```

Example. Order 1,000, gateway 1,000, fee 2 % = 20, expected settlement 980. If the bank paid gross 970 (fee 19.40, net 950.60): `gap_bank_amount = -29.40`, first breakpoint BANK_SETTLEMENT, nothing wrong at the order, gateway or fee stage. If the bank paid gross 1,000 with fee 50 (net 950): `gap_fee = -30`, first breakpoint PROCESSING, and the gross is fine. The same 30 rupees, two different answers, because the statement shows which component moved.

Each row keeps: `order_amount`, `gateway_gross`, `expected_fee`, `expected_net`, `bank_gross`, `bank_fee`, `bank_net`, `merchant_amount` (always NULL, `merchant_stage = NOT_AVAILABLE`), the four signed gaps, `difference_amount`, `classification`, `first_breakpoint`, `root_cause`, `resolution`, `evidence_level`, `findings` (every finding, not only the first), `exposure_amount`, `counted_in`, ids (`order_id`, `txn_id`, `settlement_ids`, `bank_reference`), dates, `match_method` and a plain-language `evidence_summary`. `gold.money_flow_evidence` holds the underlying records (order, gateway txn, each bank row, quarantine rows, candidates, halted batches) behind every non-reconciled conclusion.

### First breakpoint and resolution

`first_breakpoint` is the earliest stage whose finding is not reconciled: `ORDER` < `GATEWAY` < `PROCESSING` (fees) < `BANK_SETTLEMENT` < `MERCHANT_SETTLEMENT`. Two more values describe evidence rather than money: `SOURCE_DATA_QUALITY` (a record needed for the conclusion is quarantined or its batch was never loaded) and `MATCHING` (a candidate exists but cannot be confirmed). A chain with findings at several stages is filed under the earliest; all of them stay in `findings` and in the four gaps.

`resolution` is the most severe finding of the chain:

| Resolution | Meaning |
|---|---|
| RECONCILED | agrees within tolerance (rounding is recorded in `tolerated_amount`) |
| IN_FLIGHT | not yet due |
| TIMING | the money is right, the date is not |
| EXPLAINED | a mechanism in the data accounts for the whole gap (a duplicate) |
| BLOCKED | cannot be decided from the data |
| OPEN | a confirmed gap at a known stage with no documented cause. This is the "unexplained" money |

### Classifications

| Classification | Breakpoint | Resolution | Meaning |
|---|---|---|---|
| MATCHED | NONE | RECONCILED | every leg agrees |
| PENDING | NONE | IN_FLIGHT | settlement not due (capture + SLA not yet passed), or an order too recent to judge |
| PAYMENT_WITHOUT_ORDER | ORDER | OPEN | captured payment with no order record |
| GATEWAY_AMOUNT_MISMATCH | GATEWAY | OPEN | gateway captured a different amount than the order (or an extra capture on an order) |
| DUPLICATE_PAYMENT | GATEWAY | EXPLAINED confirmed, BLOCKED suspected | second capture of the same order, merchant and amount within 10 minutes |
| ORDER_WITHOUT_PAYMENT | GATEWAY | OPEN | paid order older than 2 days with no captured payment |
| FEE_MISMATCH | PROCESSING | OPEN | bank fee differs from the scheduled fee on the settled gross |
| BANK_AMOUNT_MISMATCH | BANK_SETTLEMENT | OPEN | bank gross differs from the gateway gross |
| UNEXPLAINED | BANK_SETTLEMENT | OPEN | the bank's net is not its gross minus its fee: a residual no component explains |
| MISSING_SETTLEMENT | BANK_SETTLEMENT | OPEN | nothing settled after the SLA and every evidence check came back empty |
| DUPLICATE_SETTLEMENT | BANK_SETTLEMENT | EXPLAINED | two or more bank rows each about the full amount for one reference |
| ORPHAN_SETTLEMENT | BANK_SETTLEMENT | OPEN | bank record no payment can claim |
| REFUND_MISMATCH | GATEWAY or BANK_SETTLEMENT | OPEN | refund of an unknown payment, refunds exceeding the payment (only the excess is flagged), or a bank refund of the wrong amount |
| TIMING_DELAY, DATE_ANOMALY | BANK_SETTLEMENT | TIMING | settled after the SLA, or dated before the capture |
| MERCHANT_SETTLEMENT_MISMATCH | MERCHANT_SETTLEMENT | OPEN | the settlement was credited to a different merchant than the payment's (the only merchant-side fact in the data) |
| QUARANTINED_SOURCE | SOURCE_DATA_QUALITY | BLOCKED | a needed record is in quarantine: the gateway record, the bank record, or the original of a refund |
| DATA_QUALITY_BLOCK | SOURCE_DATA_QUALITY | BLOCKED | the batch that would hold the record was refused by the quality gate |
| INSUFFICIENT_EVIDENCE | MATCHING or SOURCE_DATA_QUALITY | BLOCKED | a candidate exists but cannot be confirmed (bank record under a different reference, aggregated settlement, ambiguous fuzzy match, fee schedule row quarantined, payment quarantined without an order id) |

### "Missing" is a conclusion, not a default

A transaction older than the SLA with no linked bank row is checked, in this order, before it is called `MISSING_SETTLEMENT`: bank rows in quarantine that reference it, halted bank batches in the window, ambiguous fuzzy candidates, unlinked bank records that would explain it (same merchant, same amount within tolerance, within 14 days) and unlinked bank money that equals the sum of up to three unsettled payments (an aggregated settlement; the data has no batch id, so this is only ever a hypothesis). Every candidate found is stored as evidence and counted once.

### Exposure and no double counting

`exposure_amount` is the sum of a chain's out-of-tolerance gaps, counted once and filed under the chain's first breakpoint, so exposure by stage and by root cause both add up to the total. A blocked chain reports the single amount in question (a quarantined payment reports the bank money if it moved, else the order, else the raw amount). When the same money appears on two chains (a candidate bank record and the payment it might belong to) it is counted on one and `counted_in` names it. An in-flight payment carries no exposure, so money explained only by an in-flight payment stays on the bank side.

`python -m payreconcile money-flow` also runs conservation checks (chain ids unique, the bank bridge closes to the cent, the gateway gap equals gateway minus expectation, every bank record sits in exactly one chain, chains add up to the bank file, no merchant amount, nothing reconciled carries exposure). The bank-total, bridge and no-merchant-amount identities also run as dbt tests on every daily build.

### Evidence level

HIGH: every leg present and trusted, exact reference. MEDIUM: fuzzy link, an unverifiable leg (order quarantined), or an absence established after all checks. LOW: a hypothesis, a suspected duplicate, or a quarantined record.

## Ground truth and evaluation

The generator records what it injected. `truth.py` turns that event log into the expected conclusion for every chain (`truth/money_flow.parquet`): `expected_customer_amount`, `expected_gateway_amount`, `expected_fee`, `expected_net_settlement`, `expected_bank_settlement`, `expected_merchant_settlement` (always NULL, the data has none), `expected_final_status`, `expected_breakpoint`, `expected_resolution`, `expected_exposure`. It reasons from the injected events, not from the pipeline's output. "Expected" means the best conclusion the visible data allows: where a needed record is quarantined, or the data cannot separate two explanations, the expected answer is BLOCKED / INSUFFICIENT_EVIDENCE, and where an injected fact is real but invisible to the pipeline (a duplicate charge whose original was quarantined without an order id) the truth keeps the real answer so the miss is reported.

Injected on top of the base defects (missing, mismatched, rounded and mistyped settlements, duplicate charges, orphans, quarantine defects), with their own random stream: fee above the schedule, settlement after the SLA, the same reference settled twice, a settlement split over two rows, and two payments paid as one bank record.

`python -m payreconcile evaluate` scores, chain by chain: classification / breakpoint / resolution accuracy, TP / FP / FN of discrepancy detection, correct vs incorrect root cause, per-classification precision and recall, the confusion of every misclassification, amount agreement at every stage, and absolute and percentage money difference against the truth.

## Dashboard

`streamlit run dashboard/app.py`. **Money Flow** is the landing page:

- the headline sentence, and the conservation check (an error banner appears if any identity fails)
- expected settlement, reconciled, at risk, unexplained, blocked, in flight
- the waterfall: customer / order, gateway, refunds, fees, expected settlement, bank, merchant (not available), unexplained, plus the signed differences split by resolution
- money by first breakpoint and by root cause
- findings by category (missing settlements, amount mismatches, duplicate money, fee, refund, records with no counterpart, quarantined / blocked, timing), filterable by merchant, downloadable
- merchant-level impact
- **investigate an order or transaction**: enter or select an id and see ORDER, GATEWAY, FEE, EXPECTED SETTLEMENT, BANK, MERCHANT and FINAL RESULT with the exact difference between stages, the bridge of the bank difference, the underlying records, and the engine's status history and match evidence

Eight further pages remain: Executive Overview, Reconciliation, Exceptions Queue (with explain drill-down), Data Quality, Settlement Analytics, Merchant Risk, Anomaly Signals, Pipeline Health.

## Power BI export

`export-bi` writes CSV and Parquet plus `manifest.json`: `dim_date`, `dim_merchant` (SCD2), `dim_payment_method`, `dim_status`, `dim_stage`, `dim_resolution`, `fact_transactions`, `fact_settlements`, `fact_reconciliation`, `fact_reconciliation_audit`, **`fact_money_flow`** (all stage amounts, gaps, breakpoint, root cause, resolution, evidence level, exposure), **`money_flow_bridge`** (the gaps one row per chain and step, for waterfall visuals), **`money_flow_evidence`**, `kpi_daily`, `dq_scores`, `pipeline_runs`. Every fact joins `dim_merchant` on `merchant_sk` and `dim_date` on `date_key`; `first_breakpoint` joins `dim_stage`, `resolution` joins `dim_resolution`, bridge and evidence join on `flow_id`.

**Power BI status: the export layer is implemented and tested; no `.pbix` report exists** (Power BI Desktop is not available here), and no DAX measure has been executed.

## Testing

**197 PayReconcile tests** (139 before the money-flow change); with the 41 PaySim tests ([Part 2](#part-2-paysim-transaction-and-fraud-analytics)) the whole suite is **238 tests**, about 250 s on this machine. Data-quality engine 20, Silver and ingestion 12, local API 16, reconciliation 36, money flow 50, replay 9, failure recovery 4, anomaly leakage 7, dashboard and export 29, end-to-end with dbt 14.

- `tests/test_money_flow.py` (33): one hand-computed scenario per way money can stop matching, on the real Silver upserts and reconciliation engine. Order 1,000 at 2 % must give a fee of 20 and an expected settlement of 980. Covered: perfect match, gateway mismatch, bank mismatch, fee mismatch, wrong-merchant credit (and that no merchant amount is ever produced), missing, pending, delayed and early settlement, duplicate settlement, two records that are neither split nor duplicate, split settlement, duplicate payment (confirmed and suspected), refunds (over-refund, two refunds, wrong bank amount, orphan), quarantined gateway record, quarantined bank record, quarantined fee row, halted batch, payment without order, order without payment, orphan settlement, aggregated settlement, same amount under a different reference, an amount that is not a candidate, an unexplained net, traceability, the headline sentence, the conservation checks, idempotence.
- `tests/test_money_flow_truth.py` (17): generate, run the pipeline, score against the injected truth (population, detection, stage, amounts, each injected scenario, known limits reported), the SQL clean path equals the reference path exactly, incremental equals full reconciliation, replay changes nothing, and a real quality-gate halt blocks dependent conclusions instead of calling money missing.
- `tests/test_reconcile.py`: a transaction paid twice by the bank is one item and a visible mismatch, not four hidden matches (this was a defect in the engine, found while writing the scenarios).
- dbt (54 tests per build): the money-flow identities run again in SQL (chains add up to the bank file, the bridge closes, no merchant amount).

Mutation check done by hand during development (not part of the suite): 15 deliberate breaks of `moneyflow.py` (fee ignored, duplicates claiming the order, no candidate search, residual ignored, quarantined bank rows ignored, timing ignored, double counting a quarantined chain, duplicate seen as split, clean path ignoring gross / fee, halted batches ignored, fee schedule not date-aware, refund excess ignored, quarantined originals not claiming). 14 were caught; the 15th (dropping the duplicate flag from the clean-path test) is an equivalent mutant, because a duplicate always sits on an order with two chains, which the same query already excludes.

An existing test bug was fixed on the way: `monkeypatch.undo()` also undid the fixture that redirects the data directory, so two failure-recovery tests silently ran against the real `data/` warehouse and failed only because it existed.

## Measured results

Canonical data set: 30 days, seed 42, 200 merchants, 3,000 orders a day (`generate --days 30`, then `backfill`). Windows 11 laptop, DuckDB, other applications open. Everything below was produced by the commands in [Setup](#setup-and-commands) on 2026-09-26.

**Volume.** 89,960 money chains: 86,973 captured gateway movements (payments and refunds), 1,916 quarantined gateway records, 736 paid orders no payment claims, 335 bank records no payment claims. Bank file 86,053,388.70 net; chains add up to it to the cent.

**Where the money went (INR, synthetic).**

| Line | Amount |
|---|---|
| Expected from customers (orders) | 93,912,080 |
| Reached the gateway (payments) | 93,253,506 |
| Refunds | 2,428,033 |
| Scheduled fees | 1,847,257 |
| Expected settlement | 88,978,216 |
| Reached the bank | 86,053,389 |
| Merchant received | not in the source data |
| Reconciled (agrees, or only late) | 79,930,768 |
| In flight | 4,009,528 |
| **Unexplained** (confirmed gap, no documented cause) | 3,006,185 |
| Blocked (quarantine, ambiguity, halted batch) | 2,149,830 |
| Explained (duplicates) | 971,282 |

Earliest confirmed breakpoint: ORDER (597 payments with no order, 684,937). Largest: BANK_SETTLEMENT (2,863 open chains, 1,717,760). Exposure by stage, by root cause and by resolution each add up to the same 6,127,298, so nothing is counted twice.

**Injected discrepancies against the truth** (8,110 chains whose truth is a discrepancy):

| Truth | Chains | Right classification and breakpoint |
|---|---|---|
| BANK_AMOUNT_MISMATCH | 1,201 | 1,201 |
| DUPLICATE_PAYMENT | 694 | 691 |
| DUPLICATE_SETTLEMENT | 212 | 212 |
| FEE_MISMATCH | 307 | 307 |
| GATEWAY_AMOUNT_MISMATCH | 386 | 386 |
| MISSING_SETTLEMENT | 1,072 | 1,066 |
| ORDER_WITHOUT_PAYMENT | 407 | 407 |
| ORPHAN_SETTLEMENT | 335 | 325 |
| PAYMENT_WITHOUT_ORDER | 597 | 597 |
| QUARANTINED_SOURCE | 1,916 | 1,916 |
| REFUND_MISMATCH | 119 | 119 |
| UNEXPLAINED (bank net is not gross minus fee) | 221 | 221 |
| INSUFFICIENT_EVIDENCE (aggregated, ambiguous, quarantined) | 377 | 374 |
| Total | 8,110 | 8,088 (99.73 %) |

Over all 89,960 chains: classification accuracy 99.98 %, breakpoint accuracy 99.98 %, resolution accuracy 99.97 %. Discrepancy detection: TP 7,841, FP 0, FN 3 (precision 1.0, recall 0.9996). Root cause: 7,822 correct, 22 incorrect of 7,844 truth discrepancies (timing findings are not counted as discrepancies here). Unexplained-residual chains: 221 classified, 221 in the truth. Amounts at every stage (gateway, fee, expected settlement, bank) agree with the injected amounts on every payment and refund chain; customer amount differs on 12 chains (below). Money: truth exposure 6,132,728, pipeline 6,127,298, absolute difference 9,922 (0.16 %).

**The 22 misses, all reported, none tuned away**

| Truth | Pipeline said | Chains | Cause |
|---|---|---|---|
| ORPHAN_SETTLEMENT | INSUFFICIENT_EVIDENCE | 10 | a random bank amount coincides with an unsettled payment of the same merchant; the data cannot rule the pairing out |
| MISSING_SETTLEMENT | INSUFFICIENT_EVIDENCE | 6 | the other side of the same coincidences |
| DUPLICATE_PAYMENT | MATCHED | 3 | the original charge was quarantined without an order id, so the pair cannot be formed |
| INSUFFICIENT_EVIDENCE | ORDER_WITHOUT_PAYMENT | 3 | the only payment of the order was quarantined without an order id and its amount differs from the order, so it cannot be recognised as a candidate |

The 12 chains whose customer amount differs are the same limitation seen from the other side: the true order claimant is a quarantined payment the pipeline cannot see, so the visible payment claims the order amount.

**Base reconciliation and data quality** (unchanged in kind, re-measured on the new data): 3,733 of 3,733 injected bad rows quarantined, precision 1.0; engine status accuracy 1.0 over 86,973 transactions; 906 fuzzy matches, none wrong; duplicate charges found 691 of 694 (precision 1.0). Incremental equals a from-scratch rebuild and the independent dbt reference at 91,219 items, 0 differences.

**Conservation and repeatability** (all checked on this warehouse): 8 of 8 invariants hold; no order amount is carried by two chains; every captured transaction has exactly one chain; every bank record sits in exactly one chain; the SQL clean path, the reference path and the stored state are identical, exactly; incremental and full reconciliation give identical money flow; 12 replays (3 datasets, 2 dates, twice) released nothing and left the table unchanged with unique chain ids; no quarantined record became a `TXN:` chain; no batch was halted in this data set, so `DATA_QUALITY_BLOCK` is exercised only by the tests.

**Speed** (30 daily runs including dbt, sum of per-day durations 460 s):

| Stage | Median | Max |
|---|---|---|
| dbt build and 54 tests | 10.91 s | 12.33 s |
| money flow (full rebuild, up to 89,960 chains) | 2.35 s | 3.76 s |
| reconcile (incremental) | 0.48 s | 0.55 s |
| extract | 0.37 s | 0.59 s |
| validate and quality | 0.37 s | 0.46 s |
| load Silver | 0.32 s | 0.36 s |

Standalone on the final state: money-flow build 3.19 s (SQL clean path for 74,327 of 89,960 chains), 6.86 s all-Python reference path. A first version of the money-flow build took 24.9 s at this size; the daily cost was cut by sending only chains with a finding through Python. Data generation, 30 days: 13 s.

**Power BI export** (`export-bi`): 16 tables; `fact_money_flow` 89,960 rows, `money_flow_bridge` 15,267, `money_flow_evidence` 27,533. Key uniqueness, dimension joins and bridge-to-fact sums are tested.

**Dashboard**: all 9 pages and the transaction investigation view for 15 different classifications rendered without error against this warehouse.

**Not measured**: real-world accuracy, Power BI rendering, behaviour with a real bank feed, scaling beyond 90 thousand chains.

## Limitations

- Synthetic data. The 100% figures show the logic is coherent against a generator that knows what it injected; they say nothing about real feeds.
- The chain stops at the bank: there is no merchant-received amount, so `MERCHANT_SETTLEMENT` can only report a wrong-merchant credit.
- An aggregated settlement (one bank record for several payments) cannot be confirmed because settlements carry no batch id; it is reported as INSUFFICIENT_EVIDENCE with the hypothesis. Candidate search is bounded (groups of at most 3 payments, at most 40 nearest per merchant) and amount coincidences can produce a candidate that is not real (measured below).
- A duplicate charge whose original was quarantined without an order id cannot be linked and is missed; a paid order whose only payment was quarantined without an order id is flagged as missing unless the amount, merchant and time of that quarantined record match.
- Refund handling assumes refunds carry no fee (`money_flow.refund_fee_pct`) and that a refund is checked against the payment amount only (orders carry no refund amount).
- The fee expectation uses the merchant master. If a fee row for a merchant was quarantined after a real fee change, later fee findings for that merchant are BLOCKED, not confirmed.
- Money flow is rebuilt from scratch each run (about 2 s at 90 thousand chains, measured below); it is not incremental, so its cost grows with history.
- No PII handling, authentication, encryption or cloud deployment; no Power BI report file.

## Repository layout

`src/payreconcile/paysim/` (PaySim pipeline: `profile`, `sample`, `bronze`, `quality`, `gold`, `features`, `ml`, `export`, `benchmark`, `queries`), `src/payreconcile/` (`ingest`, `dq`, `silver`, `reconcile`, `moneyflow`, `truth`, `generate`, `evaluate`, `pipeline`, `replay`, `bi_export`, `cli`), `contracts/` (data-quality rules per source), `config/settings.yml` (tolerances, SLA, money-flow parameters), `dbt/` (marts, tests, reference oracle), `dashboard/`, `tests/`, `scripts/` (CI smoke test, secrets scan, poisoned-batch demo, benchmarks that write to `data/benchmarks/`).

---

# Part 2: PaySim transaction and fraud analytics

A large-file data-engineering and fraud-analytics pipeline on the PaySim mobile-money log (6.36 million transactions), built on DuckDB and Parquet and independent of the PayReconcile pipeline above. **PaySim has no orders, payment gateway, bank settlement or merchant receipt, so none of that is modelled here** and the money-flow model above is untouched. The financial flow of this dataset is what its fields carry:

```
origin account -> transaction -> destination account -> balance changes
```

Labels used throughout: **[fact]** a measured property of the dataset, **[measured]** a result produced by the commands below, **[design]** a decision, **[limit]** a limitation.

## 1. Business problem

Given a stream of account-to-account transactions with before/after balances: how much data is bad, do the balances obey accounting logic, where is the fraud, does the existing rule (`isFlaggedFraud`) catch it, and can a model do better without cheating on the label. The pipeline has to do this on the full file on a laptop, and be the same code on a small sample.

## 2. Dataset

**[fact]** `PS_20174392719_1491204439457_log.csv`, 493,534,783 bytes, **6,362,620 rows** (the brief assumed about 6.8 million; the file has 6.36 million), 11 columns, no nulls, no exact duplicate rows, `step` 1 to 743 (one simulated hour per step, 31 days).

| Column | Type | Meaning / measured fact |
|---|---|---|
| `step` | int | simulation hour, 1 to 743; volume collapses after the first days |
| `type` | text | CASH_OUT 2,237,500 · PAYMENT 2,151,495 · CASH_IN 1,399,284 · TRANSFER 532,909 · DEBIT 41,432 |
| `amount` | double | total 1,144,392,944,760; mean 179,862; median 74,872; min 0 (16 rows); max 92,445,517 |
| `nameOrig`, `nameDest` | text | 6,353,307 distinct origins (all customers); 2,722,362 distinct destinations, 2,150,401 of them merchants (`M...`) |
| `oldbalanceOrg`, `newbalanceOrig` | double | origin balance before and after |
| `oldbalanceDest`, `newbalanceDest` | double | destination balance before and after (always 0 for merchants) |
| `isFraud` | 0/1 | the label: 8,213 rows (0.129 %), only in TRANSFER (4,097) and CASH_OUT (4,116); fraud amount 12.06 billion |
| `isFlaggedFraud` | 0/1 | the source's own rule: 16 rows, all TRANSFER, all actually fraud |

The source file is never modified (a test hashes it before and after). `python -m payreconcile paysim profile --dataset full` writes all of this to `data/paysim/full/profile.json` from the data, not from constants.

## 3. Architecture and data flow

```
PS_..._log.csv (immutable)
   |-- sample --> data/raw/paysim_dev_250k.parquet (+ .meta.json)         development sample
   v
Bronze   data/paysim/<dataset>/bronze.parquet   source fields unchanged + source_row_number, ingestion_timestamp, source_file
   v
Silver   paysim.duckdb  silver.transactions     typed, balance-consistency classified, warnings tagged
                        silver.dq_quarantine    rows rejected by an ERROR rule, with reasons
                        silver.dq_results       one row per rule
   v
Gold     gold.fact_transactions  gold.fact_fraud  gold.transaction_type_summary  gold.balance_quality  gold.fraud_daily_summary
         gold.account_activity   gold.fraud_account_summary  gold.fraud_flag_performance
         gold.model_metrics / model_feature_importance / model_pr_curve / model_feature_audit / model_run
   v
Streamlit (7 pages)  and  exports/paysim/<dataset>/*.parquet|csv  (Power BI-ready)
```

**[design]** One DuckDB file and one Bronze file per dataset (`dev`, `full`, `sample_<rows>`), so the same code runs on 250 thousand, 1 million and 6.36 million rows. Everything heavy is DuckDB SQL over Parquet; pandas is used only for the model matrices (a temporal-split training sample and the validation and test windows), never for the raw file. dbt is not used for PaySim: the existing dbt project is bound to the PayReconcile warehouse, and the SQL here is versioned and covered by tests; that is a choice, not a limitation of the tools. No Spark, Kafka or Airflow: the measured full run takes about 3 minutes on a laptop.

## 4. Data quality

`silver.dq_results` has one row per rule: `rule_name`, `total_checked`, `failed_count`, `pass_rate`, `severity` (plus a description). **ERROR** rows are quarantined with their reasons and are not loaded into Silver; **WARNING** rows are kept and the rule names are stored on the row (`dq_warning_rules`); **INFO** records limits of the evidence. Nothing is dropped silently, and a test asserts `bronze = silver + quarantined` for every run.

| Severity | Rules |
|---|---|
| ERROR | `required_columns`, `null_or_unparseable_fields`, `invalid_step`, `negative_amount`, `invalid_transaction_type`, `invalid_fraud_flag`, `negative_balance` |
| WARNING | `zero_amount`, `duplicate_records`, `origin_equals_destination`, `origin_overdraw` (a debit larger than the balance held: an impossible movement), `flag_outside_transfer`, `origin_balance_inconsistent`, `destination_balance_inconsistent` |
| INFO | `origin_balance_evidence_missing`, `destination_balance_evidence_missing`, `destination_is_merchant` |

**[design]** Duplicates are flagged, not removed: the data has no transaction id, so two identical rows can be two real transactions. A 64-bit hash finds candidates and equality is then checked exactly. If the source cannot be parsed with the declared types it is re-read as text (nothing dropped) and the values that cannot be typed are quarantined.

**[measured] full dataset, 6,362,620 rows**

| Rule | Checked | Failed | Severity |
|---|---|---|---|
| all 6 row-level ERROR rules, `required_columns` (11) | 6,362,620 | 0 | ERROR |
| `zero_amount` | 6,362,620 | 16 | WARNING |
| `duplicate_records`, `origin_equals_destination` | 6,362,620 | 0 | WARNING |
| `flag_outside_transfer` | 16 | 0 | WARNING |
| `origin_overdraw` | 4,963,320 | 3,601,544 (72.6 %) | WARNING |
| `origin_balance_inconsistent` | 4,273,635 | 1,512,627 (35.4 %) | WARNING |
| `destination_balance_inconsistent` | 4,045,338 | 474,632 (11.7 %) | WARNING |
| `origin_balance_evidence_missing` | 6,362,620 | 2,088,985 (32.8 %) | INFO |
| `destination_balance_evidence_missing` | 4,211,125 | 165,787 (3.9 %) | INFO |
| `destination_is_merchant` | 6,362,620 | 2,151,495 (33.8 %) | INFO |

Nothing is quarantined: the file is structurally clean. The warnings are properties of the simulator's balances, not corrupt rows. The ERROR paths are exercised by tests on a hand-built file with one row of each kind of invalid record.

## 5. Financial consistency analysis

Rules derived from the data (each measured on the full file before it was written down), not one equation for every type:

| | Origin (always a customer) | Destination |
|---|---|---|
| CASH_IN | expected new = old **+** amount | agent pays out: expected new = old **-** amount |
| TRANSFER, CASH_OUT, DEBIT | expected new = old **-** amount | expected new = old **+** amount |
| PAYMENT | expected new = old **-** amount | merchant: `NOT_APPLICABLE` (balances are 0 in 100 % of rows, so no evidence exists) |

Per side: `CONSISTENT` if `abs(new - expected) <= 0.011`, `INCONSISTENT` otherwise, `INSUFFICIENT_EVIDENCE` if the opening and closing balance are both 0 (the balances are not recorded, so the equation cannot be tested and is never called consistent), `NOT_APPLICABLE` for merchants. The tolerance is data-derived: on rows that satisfy the equation the error is either below 0.005 or exactly one cent of rounding. Row status: `INCONSISTENT` if either side is, else `INSUFFICIENT_EVIDENCE` if either side is, else `CONSISTENT`. Columns: `expected_new_balance_org/dest`, `origin_balance_error`, `destination_balance_error` (NULL where not evaluable), `origin_balance_consistent`, `destination_balance_consistent` (NULL where not evaluable), `balance_consistency_status`.

**[measured] full dataset, row status by type**

| Type | Rows | Consistent | Inconsistent | Insufficient evidence |
|---|---|---|---|---|
| CASH_OUT | 2,237,500 | 233,314 | 1,078,940 | 925,246 |
| PAYMENT | 2,151,495 | 1,050,371 | 326,879 | 774,245 |
| CASH_IN | 1,399,284 | 1,032,039 | 207,240 | 160,005 |
| TRANSFER | 532,909 | 19,230 | 254,243 | 259,436 |
| DEBIT | 41,432 | 27,240 | 8,588 | 5,604 |
| All | 6,362,620 | 2,362,194 | 1,875,890 | 2,124,536 |

**[fact]** Only about 37 % of rows satisfy the accounting logic, and 72.6 % of debits move more than the recorded balance. That is how this simulator writes balances, and it is why "inconsistent" here is a finding about the data, not a fraud signal.

## 6. Fraud analytics

`isFraud` is the label; `isFlaggedFraud` is the existing rule. Accuracy is never reported alone: predicting "never fraud" would score 99.87 %.

**[measured] the existing flag against the label, full dataset**

| Scope | TP | FP | TN | FN | Precision | Recall | F1 |
|---|---|---|---|---|---|---|---|
| All | 16 | 0 | 6,354,407 | 8,197 | 1.000 | 0.0019 | 0.0039 |
| TRANSFER | 16 | 0 | 528,812 | 4,081 | 1.000 | 0.0039 | 0.0078 |
| CASH_OUT | 0 | 0 | 2,233,384 | 4,116 | n/a | 0.0000 | n/a |

The rule is perfectly precise and catches 0.19 % of the fraud. Fraud is 0.77 % of TRANSFER and 0.18 % of CASH_OUT. Gold also holds fraud per day and type, per account (16,382 accounts took part in fraud; none both received and forwarded fraud funds in this data), the amount distribution, and every fraud or flagged transaction with its outcome (TP / FP / FN).

**[fact]** A simulator artifact drives the model results below: among TRANSFER and CASH_OUT, **0 of 2,762,196 legitimate rows** have an opening origin balance exactly equal to the amount, versus **8,034 of 8,213 fraud rows** (97.8 %).

## 7. Feature engineering

28 inputs, all declared in `paysim/features.py` with a definition and a NULL rule; SQL in `FEATURE_EXPR` builds them from `gold.fact_transactions`.

| Group | Features |
|---|---|
| Amount | `amount`, `log_amount` (ln(1 + amount)) |
| Balances | `origin_balance_before/after`, `destination_balance_before/after`, `origin_balance_delta`, `destination_balance_delta` |
| Errors | `origin_balance_error`, `destination_balance_error` (NULL where not evaluable, filled with 0 and marked by the evidence flags) |
| Ratios | `amount_to_origin_balance_ratio`, `amount_to_destination_balance_ratio` (NULL when the balance is 0, no division by zero; filled with -1, capped at 1e6) |
| Time | `hour_of_day`, `day_of_week` (raw `step` and `day` are excluded: they are the time index) |
| Flags | `is_flagged_fraud`, `origin_fully_drained`, `origin_balance_zero_before`, `destination_balance_zero_before`, `origin_/destination_balance_evidence`, `origin_/destination_balance_inconsistent`, `destination_is_merchant` |
| Type | one-hot for the five types |

Guards: no infinity or NaN can reach a model (checked on every matrix). 17 of the 28 are `pre_txn`: known before the transaction completes (no closing balances).

## 8. Machine-learning methodology

- **Temporal split on `step`** (train up to 70 % of the range, validation up to 85 %, test after): a random split would train on hours that come after the hours tested. Full data: train up to step 520, validation to 631, test 632 to 743.
- **Training only**: non-fraud rows are deterministically undersampled (20 per fraud row; 121,737 rows used, 5,781 fraud). **Validation and test keep every row**, so all reported metrics are on the true class balance (test 89,466 rows, 1,252 fraud, 1.4 % because late steps contain little legitimate traffic).
- Models: logistic regression (signed-log scaling, class weights) and random forest (200 trees). The threshold is chosen on validation (maximum F1) and applied unchanged to test. Reported: precision, recall, F1, PR-AUC, ROC-AUC, confusion matrix, feature importance.
- **Two feature sets**: all 28 features, and the 17 `pre_txn` features, so an artifact in the closing balances cannot be mistaken for skill.

**Leakage protection** (`features.py`, enforced before every fit and by tests):
1. the label and anything derived from it (`is_fraud`, `flag_outcome`, any name with "fraud" except the rule flag, "outcome", "label", "target") can never be a feature; account-level aggregates of the label are excluded by the same rule;
2. every model input must be in the registry, and every column of `gold.fact_transactions` must have a declared role, so a new column cannot become a feature unnoticed;
3. a feature identical to the label on the data is rejected (when both classes are present);
4. identifiers and the time index are not features;
5. a univariate audit (`gold.model_feature_audit`) flags any single feature with ROC-AUC >= 0.99.
**[measured]** the guard passes for all 28 features, no feature SQL references the label, and the audit flagged nothing (the strongest single feature is `origin_balance_delta`, ROC-AUC 0.965).

**[measured] test set, full dataset** (89,466 rows, 1,252 fraud)

| Model | Precision | Recall | F1 | PR-AUC | ROC-AUC | TP | FP | FN |
|---|---|---|---|---|---|---|---|---|
| random forest, all features | 0.9992 | 0.9992 | 0.9992 | 1.0000 | 1.0000 | 1,251 | 1 | 1 |
| logistic regression, all features | 0.9952 | 0.9976 | 0.9964 | 1.0000 | 1.0000 | 1,249 | 6 | 3 |
| random forest, pre-transaction only | 0.9992 | 0.9960 | 0.9976 | 1.0000 | 1.0000 | 1,247 | 1 | 5 |
| logistic regression, pre-transaction only | 0.8549 | 0.5128 | 0.6410 | 0.7246 | 0.9906 | 642 | 109 | 610 |
| existing rule `isFlaggedFraud` | 1.0000 | 0.0056 | 0.0111 | n/a | n/a | 7 | 0 | 1,245 |

**Read this honestly.** The near-perfect scores mostly reflect the artifact above (fraud empties the origin account exactly; legitimate traffic never does), which a forest finds even from pre-transaction inputs such as `amount / origin balance = 1`. The linear model, which cannot express that equality, drops to F1 0.64. These numbers show the pipeline is correct and leakage-free by construction; they are not evidence of real-world fraud-detection performance. Development-sample results (2,053 fraud rows, 3,771 test rows, 341 fraud) are in `gold.model_metrics` for `dev`: the same picture, with logistic regression pre-transaction F1 0.726.

## 9. Dashboard

`streamlit run dashboard/app.py` now has two groups; the PayReconcile pages are unchanged. **PaySim analytics** (a dataset selector in the sidebar chooses `full` or a sample; sample datasets carry a warning that fraud is over-represented):

1. **PaySim Overview**: total transactions, total value, fraud transactions, fraud amount, fraud rate, flagged fraud, data-quality failures, volume and value by type.
2. **Transaction Analysis**: volume and amount by type, daily or hourly trend, largest senders and receivers.
3. **Fraud Analysis**: fraud count, rate and amount by type; fraud per day and by hour of day; amount distribution (share per order-of-magnitude band, so classes with very different counts are comparable); the existing flag versus the label; accounts involved in fraud. The daily-rate chart is captioned because it approaches 100 % when few legitimate transactions remain.
4. **Balance Integrity**: consistent / inconsistent / insufficient evidence, by type and by side, error totals and the 50 largest discrepancies.
5. **Account Investigation**: filter by origin, destination, type, fraud status and step; per-account history with counterparty, balances and a balance chart; CSV download.
6. **PaySim Data Quality**: every rule with checked, failed, failure rate, severity, and the quarantine.
7. **Model Evaluation**: test metrics for every model and the existing rule, confusion matrix, precision-recall curve, feature importance, the leakage audit.

Every page runs SQL from `paysim/queries.py` against Gold and Silver only; the benchmark times exactly those queries. All seven pages render in automated tests on a real pipeline run and the account filters are tested through the UI; the Fraud and Model pages were also opened in a browser against the full dataset (the other five were not opened by hand).

## 10. Power BI export

`paysim export` (part of `paysim run`) writes every Gold table plus `dq_results` and `dq_quarantine` to `exports/paysim/<dataset>/` as **Parquet and CSV** with `manifest.json` (rows, columns, files). CSV is written only up to 1,000,000 rows per table, so `fact_transactions` (6,362,620 rows, 431 MB Parquet) is Parquet only, which Power BI reads natively; the manifest says so. **No `.pbix` exists and nothing was opened in Power BI Desktop: the files are Power BI-ready, not verified inside Power BI.**

## 11. Performance benchmarks

**[measured]** Windows 11 laptop, 7.7 GB RAM, 12 logical cores, DuckDB 1.5.5, memory limit 3 GB, other applications open. Peak RSS is sampled every 50 ms (a lower bound) for the whole process. Stage times are single runs from `run_metrics.json`.

| Stage | 250,000 rows (dev) | 1,000,000 rows | 6,362,620 rows (full) | Peak RSS at full |
|---|---|---|---|---|
| profile (several full scans) | 0.2 s | 0.7 s | 36.9 s | 1,023 MB |
| Bronze: source to Parquet | 0.2 s | 1.0 s | 23.8 s | 503 MB |
| Quality + Silver (DQ, duplicates, balance consistency) | 1.4 s | 3.7 s | 30.4 s | 1,970 MB |
| Gold (8 tables) | 2.1 s | 5.0 s | 44.5 s | 2,638 MB |
| ML (2 feature sets x 2 models) | 10.2 s | 11.7 s | 46.8 s | 733 MB |
| Export (Parquet and CSV) | 2.4 s | 4.9 s | 12.7 s | 1,403 MB |
| **Total** | **16.5 s** | **27.0 s** | **195.1 s** | |

Sizes at full scale: CSV 493.5 MB, Bronze Parquet 192.4 MB (2.6x smaller), DuckDB file 1.64 GB (Silver + Gold + models).

**CSV versus Parquet versus DuckDB table**, the same four aggregate queries, median of 3 runs, each form in a fresh process (full dataset):

| Query | CSV | Parquet (Bronze) | DuckDB table |
|---|---|---|---|
| volume by type | 0.837 s | 0.051 s | 0.018 s |
| fraud by type | 0.776 s | 0.049 s | 0.018 s |
| top 20 senders (6.35 M groups) | 0.959 s | 0.444 s | 0.335 s |
| range filter (step and type) | 0.688 s | 0.014 s | 0.003 s |
| peak RSS of the worker process | 621 MB | 539 MB | 571 MB |

On the 250 k sample the CSV queries take 0.09 to 0.11 s against 0.003 to 0.02 s for Parquet. The CSV form is the DuckDB CSV reader, so it already benefits from DuckDB; a **pandas baseline** (`read_csv` on the full file, then one group-by) takes **9.5 s and 1.8 GB** before any analysis, against 0.8 to 1.0 s for DuckDB's CSV reader on the same aggregates (whole worker process 0.6 GB, including the Python interpreter). The 16 dashboard queries at full scale sum to **0.36 s** (slowest: the account-history lookup, 0.115 s). The first CSV run also warms the OS file cache; the table reports the median.

Scaling: rows x25 (250 k to 6.36 M) cost time x12, and no code changed between sizes.

## 12. How to run

```powershell
python -m payreconcile paysim profile --dataset full          # profile the source
python -m payreconcile paysim sample                          # development sample (250,000 rows, seed 42)
python -m payreconcile paysim run --dataset dev               # profile -> Bronze -> quality/Silver -> Gold -> ML -> export
python -m payreconcile paysim run --dataset full              # the same on the whole file (about 3 minutes)
python -m payreconcile paysim sample --rows 1000000           # optional scaling step
python -m payreconcile paysim run --dataset sample_1000000
python -m payreconcile paysim benchmark --dataset full --pandas    # query benchmark (loads the CSV into pandas once)
streamlit run dashboard/app.py                                # PaySim analytics group, sidebar dataset selector
```

Individual stages: `paysim bronze | quality | gold | ml | export --dataset <name>`. The source path is `paysim.source_file` in `config/settings.yml` (relative to the repository root) or the `PAYSIM_SOURCE` environment variable. Everything under `data/` and `exports/` and the source CSV itself are git-ignored.

The development sample keeps every source row's `source_row_number`, so any sampled row traces to its line in the file. **[design]** It is stratified by (type, fraud) and ranked by `hash(source_row_number, seed)`; fraud strata keep 25 % of their rows, the rest is spread proportionally over non-fraud strata, giving exactly 250,000 rows. **[measured]** 2,053 fraud rows (0.82 %, 6.4x the source rate), 5 flagged; CASH_OUT 88,175, PAYMENT 83,951, CASH_IN 54,599, TRANSFER 21,658, DEBIT 1,617. It is for development and tests only: rates measured on it are not population rates. Re-running `sample` reproduced the same SHA-256.

## 13. Testing

**[measured]** 41 PaySim tests (31 pipeline, 10 dashboard); the whole suite is 238 tests. Hand-built files with hand-computed expectations plus a small deterministic synthetic file: schema validation (missing and extra columns), Bronze fidelity and immutability of the source, every DQ rule with exact counts, quarantine with reasons and row conservation, duplicate detection (later copy only, near-duplicate not flagged), the balance equation per transaction type, tolerance boundary, merchants and unrecorded balances, zero amounts and zero balances, fraud metrics on known numbers, feature edge cases (division by zero, NULLs, infinities), Gold tables agreeing with each other, sampling (exact size, stratification, provenance, same bytes for the same seed), the temporal split (strictly ordered, no overlap), model determinism, target leakage (see below), export, profile, and the pipeline's stage metrics and idempotence. Eleven deliberate breaks of the rules were tried by hand (reversed CASH_IN direction, unrecorded balances called consistent, merchant destinations evaluated, wrong duplicate copy, error rows kept, tolerance ignored, label allowed as a feature, future rows in training, swapped confusion cells, unguarded division, disabled leakage name rule). Ten were caught. The eleventh survived, because a second guard layer (the feature registry) still raised, so a test for the name rule alone was added afterwards; that final break was not re-run.

## 14. Validation performed

**[measured]** on the final code: Gold equals the source row by row on both datasets (`EXCEPT ALL` in both directions returns 0 rows for 250,000 and for 6,362,620 rows) and per-type row count, amount, fraud and flag sums are identical; bronze = silver + quarantined on every run; the development sample, the Gold table hash and the model metrics are identical when rebuilt; the leakage guard passes and the audit flags nothing. `ruff` is clean and the secrets scan finds nothing.

## 15. Limitations and future improvements

**[limit]**
- The PaySim data is simulated. Its fraud pattern is an artifact (fraud empties the origin exactly), so model scores here do not predict real-world performance.
- Balances are observed after the transaction; a real-time authoriser would not have them. The `pre_txn` set removes closing balances but still contains the exact-drain signal through `amount / balance`.
- The balance rules are inferred from this data. 37 % of rows are consistent because the simulator writes balances loosely, and 33 % of origins have no recorded balances; neither says anything about real accounts.
- No transaction id exists, so duplicates cannot be told from repeated real transactions; they are flagged only.
- The test window holds 89,466 rows (1.4 % fraud) because late steps have little legitimate traffic; metrics on such a small, fraud-dense window are noisy, and the development-sample test window is smaller still.
- Random-forest probabilities can differ in the last digits between runs (parallel tree averaging); counts are identical.
- The full pipeline needs about 3 GB of memory; the DuckDB file is 1.6 GB; benchmarks ran once on one machine with other applications open.
- `account_activity` covers accounts with at least two transactions or any fraud involvement (480,130 of about 8.9 million accounts).
- Power BI export files were not opened in Power BI.

**Future improvements** (not built): a sequence model on per-account history (fraud here is a TRANSFER followed by a CASH_OUT), time-based cross-validation, threshold tuning on a cost matrix, an incremental Silver load keyed on `source_row_number`, and a small dbt project for the Gold tables if they should share tooling with PayReconcile.
