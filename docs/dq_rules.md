# Data-quality rule catalogue

_Generated from `contracts/*.yml` and the quality engine by `scripts/gen_rule_docs.py`. Do not edit by hand._

**Severity** CRITICAL / ERROR / WARN (score weights 5 / 3 / 1).
**Status** per rule: PASS (no violations), WARNING (violations up to the fail threshold), FAIL (above it: default 10% of rows,
CRITICAL rules 2%). **Any FAIL halts the batch at the quality gate**: nothing from it is loaded into Silver or Gold.
**Gate** overall = severity-weighted score across datasets; HALT below 0.9, WARN below 0.95.
**Actions** `quarantine` = the row is removed from the load and stored in `quarantine.rows` with its raw payload and rule ids;
`repair` = the value is fixed in Silver and counted; `flag` = the row is kept and the violation recorded; `reject_batch` = the
whole file is rejected.


## gateway

Payment-gateway transactions (paginated JSON API). Weight in the overall score: **3**. Natural key: `txn_id`.

| Rule | Dataset | Field | Severity | Expectation | Implementation | Failure behaviour |
|---|---|---|---|---|---|---|
| `GW-CMP-01` | gateway | txn_id | CRITICAL | txn_id present | `txn_id IS NULL OR trim(txn_id) = ''` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 2% |
| `GW-CMP-02` | gateway | order_id | ERROR | order_id present (payment traceable to an order) | `order_id IS NULL OR trim(order_id) = ''` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-CMP-03` | gateway | merchant_id | ERROR | merchant_id present | `merchant_id IS NULL OR trim(merchant_id) = ''` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-UNQ-01` | gateway | txn_id | ERROR | txn_id unique within the batch (duplicate webhook) | `_rid IN (SELECT _rid FROM (SELECT _rid, row_number() OVER (PARTITION BY trim(txn_id) ORDER BY _rid) rn FROM t WHERE txn_id IS NOT NULL AND trim(txn_id) <> '') WHERE rn > 1)` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-VAL-01` | gateway |  | ERROR | created_at parses and lies in [2020-01-01, run_date+2) (24h tolerance for events straddling midnight) | `pr_ts(created_at) IS NULL OR pr_ts(created_at) < TIMESTAMP '2020-01-01' OR pr_ts(created_at) >= TIMESTAMP '<run_date+2>'` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-VAL-02` | gateway |  | ERROR | amount is numeric | `pr_amt(amount) IS NULL` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-VAL-03` | gateway |  | ERROR | amount > 0 | `pr_amt(amount) <= 0` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-VAL-04` | gateway | status | ERROR | status in allowed set | `status IS NOT NULL AND lower(trim(status)) NOT IN ('captured', 'failed', 'pending')` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-VAL-05` | gateway | method | ERROR | payment method in allowed set | `method IS NOT NULL AND lower(trim(method)) NOT IN ('upi', 'card', 'netbanking', 'wallet')` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-VAL-06` | gateway | txn_type | ERROR | txn_type in allowed set | `txn_type IS NOT NULL AND lower(trim(txn_type)) NOT IN ('payment', 'refund')` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-VAL-07` | gateway | currency | ERROR | currency is INR | `currency IS NOT NULL AND lower(trim(currency)) NOT IN ('inr')` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-VAL-08` | gateway |  | ERROR | refund rows carry refund_of | `txn_type = 'refund' AND (refund_of IS NULL OR trim(refund_of) = '')` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-FIX-01` | gateway |  | WARN | amount carried a currency symbol or thousands separator (repaired) | `regexp_matches(amount, '[^0-9.\-]')` | value repaired in Silver and counted; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-FIX-02` | gateway |  | WARN | created_at was not ISO-8601 (repaired) | `trim(created_at) NOT LIKE '____-__-__T__:__:__' AND pr_ts(created_at) IS NOT NULL` | value repaired in Silver and counted; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-REF-01` | gateway | merchant_id | ERROR | merchant exists in the merchant master | `merchant_id IS NOT NULL AND trim(merchant_id) NOT IN (SELECT merchant_id FROM silver.merchants_scd2 WHERE is_current)` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `GW-SCH-01` | gateway | (batch) | CRITICAL | every required column is present (after alias resolution) | set difference between the contract's required columns and the file's columns | whole file rejected (`SCHEMA_REJECT` rows in quarantine); dataset FAIL; gate HALT |
| `GW-SCH-02` | gateway | (batch) | WARN | no unexpected columns (schema drift) | file columns minus required columns | extra columns dropped; WARNING |
| `GW-SCH-03` | gateway | (batch) | WARN | no renamed columns | alias map in the contract (for example `txn_reference` -> `gateway_ref`) | column renamed and processed; WARNING |
| `GW-VOL-01` | gateway | (batch) | ERROR | row count within tolerance of the trailing median | compare with the median of the last 7 loaded runs | WARNING outside +-40%; FAIL (gate HALT) on a drop of 80% or more |
| `GW-FRS-01` | gateway | (batch) | ERROR | newest record is fresh | newest `created_at` (gateway) or `settlement_date` (bank) vs the run date | WARNING above 24h old; FAIL (gate HALT) above 48h |
| `GW-DST-01` | gateway | (batch) | ERROR | batch mean amount within tolerance of the trailing median (catches unit changes, e.g. rupees -> paise) | mean captured amount (gateway) / mean absolute gross amount (bank) vs the median of earlier loaded runs | WARNING outside +-50%; FAIL (gate HALT) at 5x or more, either way |

## settlements

Bank settlement files (daily CSV, T+1 / T+2 lag). Weight in the overall score: **3**. Natural key: `settlement_id`.

| Rule | Dataset | Field | Severity | Expectation | Implementation | Failure behaviour |
|---|---|---|---|---|---|---|
| `ST-CMP-01` | settlements | settlement_id | CRITICAL | settlement_id present | `settlement_id IS NULL OR trim(settlement_id) = ''` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 2% |
| `ST-CMP-02` | settlements | gateway_ref | ERROR | gateway_ref present | `gateway_ref IS NULL OR trim(gateway_ref) = ''` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `ST-UNQ-01` | settlements | settlement_id | ERROR | settlement_id unique within the file (bank re-send) | `_rid IN (SELECT _rid FROM (SELECT _rid, row_number() OVER (PARTITION BY trim(settlement_id) ORDER BY _rid) rn FROM t WHERE settlement_id IS NOT NULL AND trim(settlement_id) <> '') WHERE rn > 1)` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `ST-VAL-01` | settlements |  | ERROR | settlement_date is a valid date <= run_date | `try_cast(trim(settlement_date) AS DATE) IS NULL OR try_cast(trim(settlement_date) AS DATE) > DATE '<run_date>'` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `ST-VAL-02` | settlements |  | ERROR | gross_amount numeric | `pr_amt(gross_amount) IS NULL` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `ST-VAL-03` | settlements |  | ERROR | fee_amount numeric | `pr_amt(fee_amount) IS NULL` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `ST-BUS-01` | settlements |  | WARN | net = gross - fee within 0.01 | `abs(pr_amt(net_amount) - (pr_amt(gross_amount) - pr_amt(fee_amount))) > 0.01` | row kept, violation recorded; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `ST-REF-01` | settlements | merchant_id | WARN | merchant exists in the merchant master | `merchant_id IS NOT NULL AND trim(merchant_id) NOT IN (SELECT merchant_id FROM silver.merchants_scd2 WHERE is_current)` | row kept, violation recorded; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `ST-SCH-01` | settlements | (batch) | CRITICAL | every required column is present (after alias resolution) | set difference between the contract's required columns and the file's columns | whole file rejected (`SCHEMA_REJECT` rows in quarantine); dataset FAIL; gate HALT |
| `ST-SCH-02` | settlements | (batch) | WARN | no unexpected columns (schema drift) | file columns minus required columns | extra columns dropped; WARNING |
| `ST-SCH-03` | settlements | (batch) | WARN | no renamed columns | alias map in the contract (for example `txn_reference` -> `gateway_ref`) | column renamed and processed; WARNING |
| `ST-VOL-01` | settlements | (batch) | ERROR | row count within tolerance of the trailing median | compare with the median of the last 7 loaded runs | WARNING outside +-40%; FAIL (gate HALT) on a drop of 80% or more |
| `ST-FRS-01` | settlements | (batch) | ERROR | newest record is fresh | newest `created_at` (gateway) or `settlement_date` (bank) vs the run date | WARNING above 24h old; FAIL (gate HALT) above 48h |
| `ST-DST-01` | settlements | (batch) | ERROR | batch mean amount within tolerance of the trailing median (catches unit changes, e.g. rupees -> paise) | mean captured amount (gateway) / mean absolute gross amount (bank) vs the median of earlier loaded runs | WARNING outside +-50%; FAIL (gate HALT) at 5x or more, either way |

## orders

Order-system change log (SQLite extract, watermark on updated_at). Weight in the overall score: **2**. Natural key: `order_id`.

| Rule | Dataset | Field | Severity | Expectation | Implementation | Failure behaviour |
|---|---|---|---|---|---|---|
| `OR-CMP-01` | orders | order_id | CRITICAL | order_id present | `order_id IS NULL OR trim(order_id) = ''` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 2% |
| `OR-VAL-01` | orders |  | ERROR | order_amount numeric and > 0 | `pr_amt(order_amount) IS NULL OR pr_amt(order_amount) <= 0` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `OR-VAL-02` | orders | order_status | ERROR | order_status in allowed set | `order_status IS NOT NULL AND lower(trim(order_status)) NOT IN ('paid', 'cancelled', 'refunded', 'pending')` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `OR-VAL-03` | orders |  | ERROR | created_at and updated_at parse | `pr_ts(created_at) IS NULL OR pr_ts(updated_at) IS NULL` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `OR-REF-01` | orders | merchant_id | ERROR | merchant exists in the merchant master | `merchant_id IS NOT NULL AND trim(merchant_id) NOT IN (SELECT merchant_id FROM silver.merchants_scd2 WHERE is_current)` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `OR-SCH-01` | orders | (batch) | CRITICAL | every required column is present (after alias resolution) | set difference between the contract's required columns and the file's columns | whole file rejected (`SCHEMA_REJECT` rows in quarantine); dataset FAIL; gate HALT |
| `OR-SCH-02` | orders | (batch) | WARN | no unexpected columns (schema drift) | file columns minus required columns | extra columns dropped; WARNING |
| `OR-SCH-03` | orders | (batch) | WARN | no renamed columns | alias map in the contract (for example `txn_reference` -> `gateway_ref`) | column renamed and processed; WARNING |

## merchants

Merchant master snapshots (weekly CSV) feeding an SCD Type 2 dimension. Weight in the overall score: **1**. Natural key: `merchant_id`.

| Rule | Dataset | Field | Severity | Expectation | Implementation | Failure behaviour |
|---|---|---|---|---|---|---|
| `MR-CMP-01` | merchants | merchant_id | CRITICAL | merchant_id present | `merchant_id IS NULL OR trim(merchant_id) = ''` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 2% |
| `MR-UNQ-01` | merchants | merchant_id | ERROR | merchant_id unique within the snapshot | `_rid IN (SELECT _rid FROM (SELECT _rid, row_number() OVER (PARTITION BY trim(merchant_id) ORDER BY _rid) rn FROM t WHERE merchant_id IS NOT NULL AND trim(merchant_id) <> '') WHERE rn > 1)` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `MR-VAL-01` | merchants |  | ERROR | fee_pct numeric in (0, 10] | `pr_amt(fee_pct) IS NULL OR pr_amt(fee_pct) <= 0 OR pr_amt(fee_pct) > 10` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `MR-VAL-02` | merchants | risk_tier | ERROR | risk_tier in allowed set | `risk_tier IS NOT NULL AND lower(trim(risk_tier)) NOT IN ('low', 'medium', 'high')` | row quarantined with its raw payload; WARNING above 0% of rows, FAIL (gate HALT) above 10% |
| `MR-SCH-01` | merchants | (batch) | CRITICAL | every required column is present (after alias resolution) | set difference between the contract's required columns and the file's columns | whole file rejected (`SCHEMA_REJECT` rows in quarantine); dataset FAIL; gate HALT |
| `MR-SCH-02` | merchants | (batch) | WARN | no unexpected columns (schema drift) | file columns minus required columns | extra columns dropped; WARNING |
| `MR-SCH-03` | merchants | (batch) | WARN | no renamed columns | alias map in the contract (for example `txn_reference` -> `gateway_ref`) | column renamed and processed; WARNING |

## Coverage by quality dimension

| Dimension | Rules |
|---|---|
| Schema | `GW-SCH-01`, `GW-SCH-02`, `GW-SCH-03`, `ST-SCH-01`, `OR-SCH-01`, `MR-SCH-01` |
| Required fields | `GW-CMP-01`, `GW-CMP-02`, `GW-CMP-03`, `ST-CMP-01`, `ST-CMP-02`, `OR-CMP-01`, `MR-CMP-01` |
| Types | `GW-VAL-01`, `GW-VAL-02`, `ST-VAL-01`, `ST-VAL-02`, `ST-VAL-03`, `OR-VAL-01`, `OR-VAL-03`, `MR-VAL-01` |
| Nulls | `GW-CMP-01`, `GW-CMP-02`, `GW-CMP-03`, `ST-CMP-01`, `ST-CMP-02`, `OR-CMP-01`, `MR-CMP-01` |
| Duplicates | `GW-UNQ-01`, `ST-UNQ-01`, `MR-UNQ-01` |
| Amount validity | `GW-VAL-02`, `GW-VAL-03`, `ST-VAL-02`, `ST-VAL-03`, `ST-BUS-01`, `OR-VAL-01`, `MR-VAL-01` |
| Currency | `GW-VAL-07` |
| Timestamp validity | `GW-VAL-01`, `ST-VAL-01`, `OR-VAL-03` |
| Future dates | `GW-VAL-01`, `ST-VAL-01` |
| Merchant validity | `GW-REF-01`, `ST-REF-01`, `OR-REF-01`, `MR-VAL-02` |
| Referential integrity | `GW-REF-01`, `ST-REF-01`, `OR-REF-01` |
| Volume | `GW-VOL-01`, `ST-VOL-01` |
| Freshness | `GW-FRS-01`, `ST-FRS-01` |
| Distribution anomalies | `GW-DST-01`, `ST-DST-01` |
| Format consistency | `GW-FIX-01`, `GW-FIX-02` |

Referential integrity is additionally enforced downstream by dbt `relationships` tests and the `assert_no_bad_rows_in_gold` test.

