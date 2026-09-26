"""KPI registry: the single source of truth for every metric shown in the dashboard, the docs and the Power BI export.

Every KPI has a definition, a formula, the business meaning and the tables it is computed from. The dashboard uses these as
tooltips.
"""

from __future__ import annotations

from dataclasses import dataclass

# Items that need someone to act. Quality-held items (SOURCE_QUARANTINED / PAYMENT_QUARANTINED) are tracked on the
# Data Quality page instead: the fix is upstream, not a reconciliation decision. PENDING is not yet due.
EXCEPTION_STATUSES = ("AMOUNT_MISMATCH", "MISSING_SETTLEMENT", "ORPHAN_REFUND", "ORPHAN_SETTLEMENT",
                      "ORDER_WITHOUT_PAYMENT", "SETTLED_NOT_CAPTURED")
RESOLVED_STATUS = "MATCHED"
AGE_BUCKETS = [(0, 3, "0-3 days"), (4, 7, "4-7 days"), (8, 14, "8-14 days"), (15, 30, "15-30 days"), (31, 10**6, "31+ days")]


@dataclass(frozen=True)
class KPI:
    key: str
    name: str
    definition: str
    formula: str
    meaning: str
    sources: tuple[str, ...]


KPIS: dict[str, KPI] = {k.key: k for k in [
    KPI("total_transactions", "Total transactions",
        "Number of captured customer payments (refunds and failed attempts excluded).",
        "count(*) of silver.gateway_txns where status = 'captured' and txn_type = 'payment'",
        "Volume the platform is responsible for reconciling.", ("silver.gateway_txns",)),
    KPI("processed_amount", "Total processed amount",
        "Sum of the captured payment amounts (INR).",
        "sum(amount) of silver.gateway_txns where status = 'captured' and txn_type = 'payment'",
        "Gross value that passed through the gateway.", ("silver.gateway_txns",)),
    KPI("matched_amount", "Matched amount",
        "Value of payments whose bank settlement was found and agrees within tolerance.",
        "sum(expected_amount) of gold.fact_reconciliation where item_type = 'txn' and status = 'MATCHED' "
        "(refunds are negative, so this is net of matched refunds)",
        "Money that is fully accounted for across gateway and bank.", ("gold.fact_reconciliation",)),
    KPI("money_at_risk", "Money at risk",
        "Value that is unaccounted for or inconsistent, defined per status: AMOUNT_MISMATCH = |settled - expected|; "
        "MISSING_SETTLEMENT = expected amount; ORPHAN_SETTLEMENT / SETTLED_NOT_CAPTURED = settled amount; "
        "ORPHAN_REFUND = refund amount; ORDER_WITHOUT_PAYMENT = order amount. PENDING and MATCHED contribute 0.",
        "sum(money_at_risk) of gold.fact_reconciliation",
        "The amount finance would have to chase or explain. It is exposure, not confirmed loss.",
        ("gold.fact_reconciliation",)),
    KPI("match_rate", "Match rate",
        "Share of settlement-due payments that matched. PENDING (not yet due) and orphan refunds are excluded from the "
        "denominator so recent days do not look artificially bad.",
        "count(MATCHED) / count(MATCHED + AMOUNT_MISMATCH + MISSING_SETTLEMENT) over txn items",
        "Headline health of the money trail; a fall usually means a bank feed or a merchant problem.",
        ("gold.fact_reconciliation",)),
    KPI("settlement_lag", "Settlement lag (days)",
        "Days between the gateway capture date and the bank settlement date for matched payments; reported as mean, p50, p95.",
        "settlement_date - cast(created_at as date) over gold.recon_links joined to silver.settlements and silver.gateway_txns",
        "How long merchants wait for their money; drives the settlement-SLA design.",
        ("gold.recon_links", "silver.settlements", "silver.gateway_txns")),
    KPI("exception_count", "Open exceptions",
        "Reconciliation items that need action: " + ", ".join(EXCEPTION_STATUSES) + ".",
        "count(*) of gold.fact_reconciliation where status in (exception statuses)",
        "The size of the work queue.", ("gold.fact_reconciliation",)),
    KPI("exception_age", "Exception age (days)",
        "Age of an open exception = reconciliation as-of date minus the item's event date. Bucketed 0-3, 4-7, 8-14, 15-30, 31+.",
        "ops.recon_state.as_of_date - gold.fact_reconciliation.event_date",
        "Old exceptions are more likely to be real losses and may breach contractual dispute windows.",
        ("gold.fact_reconciliation", "ops.recon_state")),
    KPI("duplicate_exposure", "Duplicate-charge exposure",
        "Amount of customers charged twice. CONFIRMED = two trusted captured payments for one order, same merchant and amount, "
        "within 10 minutes. SUSPECTED = one trusted payment plus a quarantined attempt on the same order that its own fields "
        "do not contradict. The two are reported separately and never summed silently.",
        "sum(duplicate_exposure) (confirmed) and sum(suspected_duplicate_exposure) (suspected) of gold.fact_reconciliation",
        "Money that likely has to be refunded to customers.", ("gold.fact_reconciliation",)),
    KPI("payment_failure_rate", "Payment failure rate",
        "Share of payment attempts that failed, by payment method.",
        "count(status = 'failed') / count(*) over silver.gateway_txns where txn_type = 'payment'",
        "Where customers are failing to pay; a spike on one method points to a provider problem.",
        ("silver.gateway_txns",)),
    KPI("exception_contribution", "Exception contribution to exposure",
        "Share of total money at risk contributed by each exception status.",
        "sum(money_at_risk) per status / sum(money_at_risk)", "Which failure type deserves attention first.",
        ("gold.fact_reconciliation",)),
    KPI("merchant_exposure", "Merchant exposure",
        "Money at risk plus confirmed duplicate exposure for one merchant.",
        "sum(money_at_risk) + sum(duplicate_exposure) grouped by merchant_id",
        "Ranks merchants by the money that needs follow-up.", ("gold.fact_reconciliation",)),
    KPI("merchant_exception_age", "Longest unresolved exception (merchant)",
        "Largest age in days among a merchant's open exceptions.", "max(exception age) grouped by merchant_id",
        "Merchants whose problems have been left the longest.", ("gold.fact_reconciliation", "ops.recon_state")),
]}
