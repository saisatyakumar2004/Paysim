# Power BI layer

## Status (read this first)

| Item | Status |
|---|---|
| Power BI-ready star schema export (CSV + Parquet) | **IMPLEMENTED**: `python -m payreconcile export-bi` |
| Model documentation: tables, relationships, measures, page guide | **IMPLEMENTED** (this file) |
| Export tested by unit/integration tests | **IMPLEMENTED** (row counts, keys, referential integrity, KPI parity with the dashboard) |
| A `.pbix` report file | **NOT CREATED.** Power BI Desktop is not available in the environment this project was built in, and a `.pbix` cannot be generated honestly without it. |
| The measures below verified inside Power BI Desktop | **NOT VERIFIED.** They were written against the exported schema and cross-checked against the SQL definitions, but never executed in Power BI. |
| Screenshots / mockups | None. Real Streamlit screenshots are in `docs/images/`. |

Everything a report author needs is here; building the report is a manual step in Power BI Desktop.

## Export

```powershell
python -m payreconcile export-bi --out exports/powerbi       # csv + parquet + manifest.json
```
`manifest.json` lists every table with row count and column names, the as-of date and the generation time. The export reads
Silver, the reconciliation state and the `ops` tables directly (it does not need the dbt marts). All data is synthetic.

## Tables

| Table | Grain | Purpose |
|---|---|---|
| `dim_date` | one day | calendar |
| `dim_merchant` | one merchant **version** (SCD2) | `merchant_sk`, risk tier, fee plan, `valid_from/valid_to/is_current` |
| `dim_payment_method` | one method | upi, card, netbanking, wallet |
| `dim_status` | one reconciliation status | category, description, `is_exception`, `is_open`, sort order |
| `fact_transactions` | one gateway transaction | payments, refunds, failed attempts |
| `fact_settlements` | one bank settlement line | includes `txn_id`, `match_method`, `lag_days` when linked |
| `fact_reconciliation` | one reconciliation item | status, amounts, `age_days`, `is_exception`, duplicate flags, match evidence summary |
| `fact_reconciliation_audit` | one status change | append-only history |
| `kpi_daily` | one day | pre-aggregated daily KPIs (cross-check for measures) |
| `dq_scores` | dataset x run | data-quality score and quarantine counts |
| `pipeline_runs` | one run | status, gate, duration |

## Relationships (all many-to-one, single direction from dimension to fact)

| From (many) | To (one) |
|---|---|
| `fact_reconciliation[date_key]` | `dim_date[date_key]` |
| `fact_reconciliation[merchant_sk]` | `dim_merchant[merchant_sk]` |
| `fact_reconciliation[status]` | `dim_status[status]` |
| `fact_reconciliation[method_key]` | `dim_payment_method[method_key]` |
| `fact_transactions[date_key]` / `[merchant_sk]` / `[method_key]` | `dim_date` / `dim_merchant` / `dim_payment_method` |
| `fact_settlements[date_key]` / `[merchant_sk]` | `dim_date` / `dim_merchant` |
| `kpi_daily[date_key]` | `dim_date[date_key]` |

`fact_reconciliation_audit` has **no relationship**: closed items no longer exist in `fact_reconciliation`. Filter it by `item_id`.
`merchant_sk` is the merchant version valid on the event date, so historical facts keep the risk tier they had at the time.

## Measures (DAX)

```DAX
Total Transactions = CALCULATE ( COUNTROWS ( fact_transactions ), fact_transactions[is_captured_payment] = TRUE () )
Processed Amount   = CALCULATE ( SUM ( fact_transactions[amount] ), fact_transactions[is_captured_payment] = TRUE () )

Matched Items   = CALCULATE ( COUNTROWS ( fact_reconciliation ), fact_reconciliation[item_type] = "txn", fact_reconciliation[status] = "MATCHED" )
Unmatched Items = CALCULATE ( COUNTROWS ( fact_reconciliation ), fact_reconciliation[item_type] = "txn",
                              fact_reconciliation[status] IN { "AMOUNT_MISMATCH", "MISSING_SETTLEMENT" } )
Match Rate      = DIVIDE ( [Matched Items], [Matched Items] + [Unmatched Items] )
Matched Amount  = CALCULATE ( SUM ( fact_reconciliation[expected_amount] ), fact_reconciliation[item_type] = "txn", fact_reconciliation[status] = "MATCHED" )

Money at Risk   = SUM ( fact_reconciliation[money_at_risk] )
Exception Count = CALCULATE ( COUNTROWS ( fact_reconciliation ), fact_reconciliation[is_exception] = TRUE () )
Avg Exception Age (days) = CALCULATE ( AVERAGE ( fact_reconciliation[age_days] ), fact_reconciliation[is_exception] = TRUE () )
Oldest Exception (days)  = CALCULATE ( MAX ( fact_reconciliation[age_days] ), fact_reconciliation[is_exception] = TRUE () )

Duplicate Exposure (confirmed) = SUM ( fact_reconciliation[duplicate_exposure] )
Duplicate Exposure (suspected) = SUM ( fact_reconciliation[suspected_duplicate_exposure] )
Merchant Exposure = [Money at Risk] + [Duplicate Exposure (confirmed)]

Avg Settlement Lag (days) = AVERAGE ( fact_settlements[lag_days] )
P95 Settlement Lag (days) = PERCENTILE.INC ( fact_settlements[lag_days], 0.95 )

Payment Failure Rate =
DIVIDE (
    CALCULATE ( COUNTROWS ( fact_transactions ), fact_transactions[txn_type] = "payment", fact_transactions[status] = "failed" ),
    CALCULATE ( COUNTROWS ( fact_transactions ), fact_transactions[txn_type] = "payment" )
)
```
Calculated column for exception ageing (on `fact_reconciliation`):
```DAX
Age Bucket = SWITCH ( TRUE (), [age_days] <= 3, "0-3 days", [age_days] <= 7, "4-7 days", [age_days] <= 14, "8-14 days",
                      [age_days] <= 30, "15-30 days", "31+ days" )
```
Each measure's definition, formula and sources are in [kpi_definitions.md](kpi_definitions.md). `kpi_daily` holds the same daily numbers
as pre-aggregated columns, so a measure can be sanity-checked against it.

## Recommended report pages

| Page | Visuals | Measures |
|---|---|---|
| Reconciliation overview | KPI cards, match-rate line by `dim_date`, status stacked bar by `dim_status[category]` | Total Transactions, Processed Amount, Matched Amount, Match Rate |
| Money at risk | bar by `status`, waterfall by day, decomposition tree by merchant > status | Money at Risk, Merchant Exposure |
| Merchant risk | table + scatter (exception rate vs volume), slicer on `dim_merchant[risk_tier]` | Exception Count, Merchant Exposure, Oldest Exception |
| Payment-method performance | clustered bar by `dim_payment_method` | Payment Failure Rate, Match Rate, Avg Settlement Lag |
| Settlement ageing | histogram of `fact_settlements[lag_days]`, unsettled money by Age Bucket | Avg / P95 Settlement Lag |
| Exception ageing | stacked column by Age Bucket and status, table of oldest exceptions | Exception Count, Avg / Oldest Exception Age |
| Duplicate charges | card + table filtered on `duplicate_basis` | Duplicate Exposure (confirmed / suspected) |

## Implementation guide
1. Run the pipeline and `python -m payreconcile export-bi`.
2. Power BI Desktop > Get data > Folder (or Text/CSV per file) > `exports/powerbi/*.csv` (or the Parquet connector).
3. Set types: dates for `date_key`, `valid_*`, `status_since`; decimals for amounts.
4. Mark `dim_date` as the date table; create the relationships above; add the measures and the calculated column.
5. Build the pages above; set the exception filter (`is_exception = TRUE`) as a page-level filter where relevant.
6. Refresh by re-running the export and pressing Refresh.

## Limitations
* Synthetic data; there is no settlement-partner dimension, so lag by partner cannot be built.
* The `merchant_sk` join uses the merchant version valid on the event date; a fact dated before a merchant's first version has a null key.
