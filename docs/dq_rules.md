# Data-quality rule catalogue

_Generated from `contracts/*.yml` by `scripts/gen_rule_docs.py`. Do not edit by hand._

**Severity** CRITICAL / ERROR / WARN (score weights 5 / 3 / 1).
**Action** `quarantine` = row is removed from the load and written to `quarantine.rows`; `repair` = value is fixed in Silver and
counted; `flag` = row is kept and the violation is recorded; `reject_batch` = the whole file is rejected.
**Status** PASS (no violations) / WARNING (violations <= fail threshold) / FAIL (above it: default 10%, CRITICAL 2%).

Schema rules (`*-SCH-01/02/03`) and batch rules (`*-VOL-01`, `*-FRS-01`, `*-DST-01`) are applied to every dataset by the engine
and are listed at the bottom.


## gateway

Payment-gateway transactions (paginated JSON API). Dataset weight in the overall score: **3**. Natural key: `txn_id`.

| Rule | Severity | Column | Expectation | Detection | Action |
|---|---|---|---|---|---|
| `GW-CMP-01` | CRITICAL | txn_id | txn_id present | NULL or blank | quarantine |
| `GW-CMP-02` | ERROR | order_id | order_id present (payment traceable to an order) | NULL or blank | quarantine |
| `GW-CMP-03` | ERROR | merchant_id | merchant_id present | NULL or blank | quarantine |
| `GW-UNQ-01` | ERROR | txn_id | txn_id unique within the batch (duplicate webhook) | second and later occurrences of the key in the batch | quarantine |
| `GW-VAL-01` | ERROR |  | created_at parses and lies in [2020-01-01, run_date+1) | `pr_ts(created_at) IS NULL OR pr_ts(created_at) < TIMESTAMP '2020-01-01' OR pr_ts(created_at) >= TIMESTAMP '$run_date_next'` | quarantine |
| `GW-VAL-02` | ERROR |  | amount is numeric | `pr_amt(amount) IS NULL` | quarantine |
| `GW-VAL-03` | ERROR |  | amount > 0 | `pr_amt(amount) <= 0` | quarantine |
| `GW-VAL-04` | ERROR | status | status in allowed set | value not in ['captured', 'failed', 'pending'] | quarantine |
| `GW-VAL-05` | ERROR | method | payment method in allowed set | value not in ['upi', 'card', 'netbanking', 'wallet'] | quarantine |
| `GW-VAL-06` | ERROR | txn_type | txn_type in allowed set | value not in ['payment', 'refund'] | quarantine |
| `GW-VAL-07` | ERROR | currency | currency is INR | value not in ['INR'] | quarantine |
| `GW-VAL-08` | ERROR |  | refund rows carry refund_of | `txn_type = 'refund' AND (refund_of IS NULL OR trim(refund_of) = '')` | quarantine |
| `GW-FIX-01` | WARN |  | amount carried a currency symbol or thousands separator (repaired) | `regexp_matches(amount, '[^0-9.\-]')` | repair |
| `GW-FIX-02` | WARN |  | created_at was not ISO-8601 (repaired) | `trim(created_at) NOT LIKE '____-__-__T__:__:__' AND pr_ts(created_at) IS NOT NULL` | repair |
| `GW-REF-01` | ERROR | merchant_id | merchant exists in the merchant master | value not found in the merchant master | quarantine |

## settlements

Bank settlement files (daily CSV, T+1 / T+2 lag). Dataset weight in the overall score: **3**. Natural key: `settlement_id`.

| Rule | Severity | Column | Expectation | Detection | Action |
|---|---|---|---|---|---|
| `ST-CMP-01` | CRITICAL | settlement_id | settlement_id present | NULL or blank | quarantine |
| `ST-CMP-02` | ERROR | gateway_ref | gateway_ref present | NULL or blank | quarantine |
| `ST-UNQ-01` | ERROR | settlement_id | settlement_id unique within the file (bank re-send) | second and later occurrences of the key in the batch | quarantine |
| `ST-VAL-01` | ERROR |  | settlement_date is a valid date <= run_date | `try_cast(trim(settlement_date) AS DATE) IS NULL OR try_cast(trim(settlement_date) AS DATE) > DATE '$run_date'` | quarantine |
| `ST-VAL-02` | ERROR |  | gross_amount numeric | `pr_amt(gross_amount) IS NULL` | quarantine |
| `ST-VAL-03` | ERROR |  | fee_amount numeric | `pr_amt(fee_amount) IS NULL` | quarantine |
| `ST-BUS-01` | WARN |  | net = gross - fee within 0.01 | `abs(pr_amt(net_amount) - (pr_amt(gross_amount) - pr_amt(fee_amount))) > 0.01` | flag |
| `ST-REF-01` | WARN | merchant_id | merchant exists in the merchant master | value not found in the merchant master | flag |

## orders

Order-system change log (SQLite extract, watermark on updated_at). Dataset weight in the overall score: **2**. Natural key: `order_id`.

| Rule | Severity | Column | Expectation | Detection | Action |
|---|---|---|---|---|---|
| `OR-CMP-01` | CRITICAL | order_id | order_id present | NULL or blank | quarantine |
| `OR-VAL-01` | ERROR |  | order_amount numeric and > 0 | `pr_amt(order_amount) IS NULL OR pr_amt(order_amount) <= 0` | quarantine |
| `OR-VAL-02` | ERROR | order_status | order_status in allowed set | value not in ['paid', 'cancelled', 'refunded', 'pending'] | quarantine |
| `OR-VAL-03` | ERROR |  | created_at and updated_at parse | `pr_ts(created_at) IS NULL OR pr_ts(updated_at) IS NULL` | quarantine |
| `OR-REF-01` | ERROR | merchant_id | merchant exists in the merchant master | value not found in the merchant master | quarantine |

## merchants

Merchant master snapshots (weekly CSV) feeding an SCD Type 2 dimension. Dataset weight in the overall score: **1**. Natural key: `merchant_id`.

| Rule | Severity | Column | Expectation | Detection | Action |
|---|---|---|---|---|---|
| `MR-CMP-01` | CRITICAL | merchant_id | merchant_id present | NULL or blank | quarantine |
| `MR-UNQ-01` | ERROR | merchant_id | merchant_id unique within the snapshot | second and later occurrences of the key in the batch | quarantine |
| `MR-VAL-01` | ERROR |  | fee_pct numeric in (0, 10] | `pr_amt(fee_pct) IS NULL OR pr_amt(fee_pct) <= 0 OR pr_amt(fee_pct) > 10` | quarantine |
| `MR-VAL-02` | ERROR | risk_tier | risk_tier in allowed set | value not in ['low', 'medium', 'high'] | quarantine |

## Engine-level rules

| Rule | Severity | Expectation | Detection | Action |
|---|---|---|---|---|
| `<DS>-SCH-01` | CRITICAL | every required column is present (after alias resolution) | set difference against the contract | reject_batch |
| `<DS>-SCH-02` | WARN | no unexpected columns (schema drift) | set difference | flag, column dropped |
| `<DS>-SCH-03` | WARN | no renamed columns | alias map in the contract | flag, column renamed |
| `GW-VOL-01`, `ST-VOL-01` | ERROR | row count within +-40% of the trailing median; a drop of 80%+ is FAIL | compare with the last 7 loaded runs | flag / halt |
| `GW-FRS-01` | ERROR | newest gateway record is not older than 24h (FAIL at 48h) | `max(created_at)` vs run date | flag / halt |
| `GW-DST-01`, `ST-DST-01` | ERROR | batch mean amount within +-50% of the trailing median; 5x or more (either way) is FAIL | catches unit changes such as rupees -> paise | flag / halt |

