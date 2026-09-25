# KPI definitions

_Generated from `dashboard/kpis.py` by `scripts/gen_kpi_docs.py`. Do not edit by hand._

Every metric in the Streamlit dashboard and in the Power BI export model is defined here once: what it is, how it is computed,
what it means for the business and which tables it comes from. All data is synthetic.

**Exception statuses** (need a human decision): `AMOUNT_MISMATCH`, `MISSING_SETTLEMENT`, `ORPHAN_REFUND`, `ORPHAN_SETTLEMENT`, `ORDER_WITHOUT_PAYMENT`, `SETTLED_NOT_CAPTURED`.
`PENDING` (not yet due) and the two quality holds (`SOURCE_QUARANTINED`, `PAYMENT_QUARANTINED`, fixed upstream) are not exceptions.

**Not available in this dataset:** settlement lag by *settlement partner* (there is a single bank feed and no partner dimension).


## Total transactions

* **Definition:** Number of captured customer payments (refunds and failed attempts excluded).
* **Formula:** `count(*) of silver.gateway_txns where status = 'captured' and txn_type = 'payment'`
* **Business meaning:** Volume the platform is responsible for reconciling.
* **Source tables:** `silver.gateway_txns`


## Total processed amount

* **Definition:** Sum of the captured payment amounts (INR).
* **Formula:** `sum(amount) of silver.gateway_txns where status = 'captured' and txn_type = 'payment'`
* **Business meaning:** Gross value that passed through the gateway.
* **Source tables:** `silver.gateway_txns`


## Matched amount

* **Definition:** Value of payments whose bank settlement was found and agrees within tolerance.
* **Formula:** `sum(expected_amount) of gold.fact_reconciliation where item_type = 'txn' and status = 'MATCHED' (refunds are negative, so this is net of matched refunds)`
* **Business meaning:** Money that is fully accounted for across gateway and bank.
* **Source tables:** `gold.fact_reconciliation`


## Money at risk

* **Definition:** Value that is unaccounted for or inconsistent, defined per status: AMOUNT_MISMATCH = |settled - expected|; MISSING_SETTLEMENT = expected amount; ORPHAN_SETTLEMENT / SETTLED_NOT_CAPTURED = settled amount; ORPHAN_REFUND = refund amount; ORDER_WITHOUT_PAYMENT = order amount. PENDING and MATCHED contribute 0.
* **Formula:** `sum(money_at_risk) of gold.fact_reconciliation`
* **Business meaning:** The amount finance would have to chase or explain. It is exposure, not confirmed loss.
* **Source tables:** `gold.fact_reconciliation`


## Match rate

* **Definition:** Share of settlement-due payments that matched. PENDING (not yet due) and orphan refunds are excluded from the denominator so recent days do not look artificially bad.
* **Formula:** `count(MATCHED) / count(MATCHED + AMOUNT_MISMATCH + MISSING_SETTLEMENT) over txn items`
* **Business meaning:** Headline health of the money trail; a fall usually means a bank feed or a merchant problem.
* **Source tables:** `gold.fact_reconciliation`


## Settlement lag (days)

* **Definition:** Days between the gateway capture date and the bank settlement date for matched payments; reported as mean, p50, p95.
* **Formula:** `settlement_date - cast(created_at as date) over gold.recon_links joined to silver.settlements and silver.gateway_txns`
* **Business meaning:** How long merchants wait for their money; drives the settlement-SLA design.
* **Source tables:** `gold.recon_links`, `silver.settlements`, `silver.gateway_txns`


## Open exceptions

* **Definition:** Reconciliation items that need action: AMOUNT_MISMATCH, MISSING_SETTLEMENT, ORPHAN_REFUND, ORPHAN_SETTLEMENT, ORDER_WITHOUT_PAYMENT, SETTLED_NOT_CAPTURED.
* **Formula:** `count(*) of gold.fact_reconciliation where status in (exception statuses)`
* **Business meaning:** The size of the work queue.
* **Source tables:** `gold.fact_reconciliation`


## Exception age (days)

* **Definition:** Age of an open exception = reconciliation as-of date minus the item's event date. Bucketed 0-3, 4-7, 8-14, 15-30, 31+.
* **Formula:** `ops.recon_state.as_of_date - gold.fact_reconciliation.event_date`
* **Business meaning:** Old exceptions are more likely to be real losses and may breach contractual dispute windows.
* **Source tables:** `gold.fact_reconciliation`, `ops.recon_state`


## Duplicate-charge exposure

* **Definition:** Amount of customers charged twice. CONFIRMED = two trusted captured payments for one order, same merchant and amount, within 10 minutes. SUSPECTED = one trusted payment plus a quarantined attempt on the same order that its own fields do not contradict. The two are reported separately and never summed silently.
* **Formula:** `sum(duplicate_exposure) (confirmed) and sum(suspected_duplicate_exposure) (suspected) of gold.fact_reconciliation`
* **Business meaning:** Money that likely has to be refunded to customers.
* **Source tables:** `gold.fact_reconciliation`


## Payment failure rate

* **Definition:** Share of payment attempts that failed, by payment method.
* **Formula:** `count(status = 'failed') / count(*) over silver.gateway_txns where txn_type = 'payment'`
* **Business meaning:** Where customers are failing to pay; a spike on one method points to a provider problem.
* **Source tables:** `silver.gateway_txns`


## Exception contribution to exposure

* **Definition:** Share of total money at risk contributed by each exception status.
* **Formula:** `sum(money_at_risk) per status / sum(money_at_risk)`
* **Business meaning:** Which failure type deserves attention first.
* **Source tables:** `gold.fact_reconciliation`


## Merchant exposure

* **Definition:** Money at risk plus confirmed duplicate exposure for one merchant.
* **Formula:** `sum(money_at_risk) + sum(duplicate_exposure) grouped by merchant_id`
* **Business meaning:** Ranks merchants by the money that needs follow-up.
* **Source tables:** `gold.fact_reconciliation`


## Longest unresolved exception (merchant)

* **Definition:** Largest age in days among a merchant's open exceptions.
* **Formula:** `max(exception age) grouped by merchant_id`
* **Business meaning:** Merchants whose problems have been left the longest.
* **Source tables:** `gold.fact_reconciliation`, `ops.recon_state`

