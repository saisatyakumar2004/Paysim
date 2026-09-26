"""Feature registry and the target-leakage guard.

Every model input is declared here with its definition. `assert_no_leakage` runs before any model is fitted, and
`classify_columns` forces every column of gold.fact_transactions to carry a role, so a new column cannot silently become a model
input: it must be classified first.

Leakage rules enforced
  * the label (`is_fraud`) and anything derived from it (`flag_outcome`, any name containing "fraud" except the operational flag
    `is_flagged_fraud`, "outcome", "label" or "target") is never a feature;
  * no account-level aggregate of the label may be a feature (they would leak the future);
  * identifiers (`name_orig`, `name_dest`, `source_row_number`) and the time index (`step`, `day`) are not features;
  * a feature that reproduces the label exactly is rejected on the data itself (`verify_matrix_not_target`).
Balances are observed AFTER the transaction, so a real-time authoriser would not have `*_after` values; see README limitations.
"""

from __future__ import annotations

from dataclasses import dataclass

TARGET = "is_fraud"
TARGET_DERIVED = {"is_fraud", "flag_outcome"}
ALLOWED_FRAUD_NAMED = {"is_flagged_fraud"}          # the operational rule flag: an input by design, not the label
TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]


@dataclass(frozen=True)
class Feature:
    name: str
    kind: str                # numeric | binary | onehot
    source: str              # gold.fact_transactions column(s)
    definition: str
    null_rule: str = "never null"
    pre_txn: bool = False     # known BEFORE the transaction completes (no closing balances): usable by a real-time authoriser


FEATURES: list[Feature] = [
    Feature("amount", "numeric", "amount", "transaction amount", pre_txn=True),
    Feature("log_amount", "numeric", "log_amount", "ln(1 + amount)", pre_txn=True),
    Feature("origin_balance_before", "numeric", "old_balance_org", "origin balance before the transaction", pre_txn=True),
    Feature("origin_balance_after", "numeric", "new_balance_org", "origin balance after the transaction"),
    Feature("destination_balance_before", "numeric", "old_balance_dest", "destination balance before the transaction", pre_txn=True),
    Feature("destination_balance_after", "numeric", "new_balance_dest", "destination balance after the transaction"),
    Feature("origin_balance_delta", "numeric", "origin_balance_delta", "origin balance after - before"),
    Feature("destination_balance_delta", "numeric", "destination_balance_delta", "destination balance after - before"),
    Feature("origin_balance_error", "numeric", "origin_balance_error",
            "origin balance after - (before -/+ amount, by type)", "NULL where the balances are not recorded; filled with 0 and flagged by origin_balance_evidence"),
    Feature("destination_balance_error", "numeric", "destination_balance_error",
            "destination balance after - (before +/- amount, by type)",
            "NULL for merchants and unrecorded balances; filled with 0 and flagged by destination_balance_evidence"),
    Feature("amount_to_origin_balance_ratio", "numeric", "amount_to_origin_balance_ratio", "amount / origin balance before",
            "NULL when the balance before is 0 (no division by zero); filled with -1, capped at 1e6", pre_txn=True),
    Feature("amount_to_destination_balance_ratio", "numeric", "amount_to_destination_balance_ratio", "amount / destination balance before",
            "NULL when the balance before is 0; filled with -1, capped at 1e6", pre_txn=True),
    Feature("hour_of_day", "numeric", "hour_of_day", "(step - 1) mod 24", pre_txn=True),
    Feature("day_of_week", "numeric", "day_of_week", "(day - 1) mod 7 (simulated calendar, start weekday unknown)", pre_txn=True),
    Feature("is_flagged_fraud", "binary", "is_flagged_fraud", "the source's operational rule flag (not the label)", pre_txn=True),
    Feature("origin_fully_drained", "binary", "origin_fully_drained", "origin balance before > 0 and after = 0"),
    Feature("origin_balance_zero_before", "binary", "old_balance_org", "origin balance before = 0", pre_txn=True),
    Feature("destination_balance_zero_before", "binary", "old_balance_dest", "destination balance before = 0", pre_txn=True),
    Feature("origin_balance_evidence", "binary", "origin_balance_status", "1 when the origin equation could be tested"),
    Feature("destination_balance_evidence", "binary", "destination_balance_status", "1 when the destination equation could be tested"),
    Feature("origin_balance_inconsistent", "binary", "origin_balance_status", "1 when the origin equation failed"),
    Feature("destination_balance_inconsistent", "binary", "destination_balance_status", "1 when the destination equation failed"),
    Feature("destination_is_merchant", "binary", "dest_account_kind", "destination is a merchant account", pre_txn=True),
    *[Feature(f"type_{t}", "onehot", "type", f"transaction type is {t}", pre_txn=True) for t in TYPES],
]
FEATURE_NAMES = [f.name for f in FEATURES]
PRE_TXN_NAMES = [f.name for f in FEATURES if f.pre_txn]

# Every column of gold.fact_transactions has exactly one role. New columns must be classified here on purpose.
COLUMN_ROLES: dict[str, str] = {
    "source_row_number": "identifier", "name_orig": "identifier", "name_dest": "identifier",
    "step": "time_index", "day": "time_index",
    "type": "feature_source", "amount": "feature_source", "hour_of_day": "feature_source", "day_of_week": "feature_source",
    "old_balance_org": "feature_source", "new_balance_org": "feature_source", "old_balance_dest": "feature_source",
    "new_balance_dest": "feature_source", "is_flagged_fraud": "feature_source", "log_amount": "feature_source",
    "origin_balance_delta": "feature_source", "destination_balance_delta": "feature_source",
    "amount_to_origin_balance_ratio": "feature_source", "amount_to_destination_balance_ratio": "feature_source",
    "origin_fully_drained": "feature_source", "origin_balance_error": "feature_source", "destination_balance_error": "feature_source",
    "origin_balance_status": "feature_source", "destination_balance_status": "feature_source", "dest_account_kind": "feature_source",
    "orig_account_kind": "descriptive", "expected_new_balance_org": "descriptive", "expected_new_balance_dest": "descriptive",
    "origin_balance_consistent": "descriptive", "destination_balance_consistent": "descriptive",
    "balance_consistency_status": "descriptive", "dq_status": "descriptive", "dq_warning_rules": "descriptive",
    "is_fraud": "label", "flag_outcome": "label_derived",
}


# SQL that produces each model input from gold.fact_transactions (NULL handling is part of the definition).
FEATURE_EXPR: dict[str, str] = {
    "amount": "amount", "log_amount": "log_amount",
    "origin_balance_before": "old_balance_org", "origin_balance_after": "new_balance_org",
    "destination_balance_before": "old_balance_dest", "destination_balance_after": "new_balance_dest",
    "origin_balance_delta": "origin_balance_delta", "destination_balance_delta": "destination_balance_delta",
    "origin_balance_error": "coalesce(origin_balance_error, 0)",
    "destination_balance_error": "coalesce(destination_balance_error, 0)",
    "amount_to_origin_balance_ratio": "least(coalesce(amount_to_origin_balance_ratio, -1), 1000000)",
    "amount_to_destination_balance_ratio": "least(coalesce(amount_to_destination_balance_ratio, -1), 1000000)",
    "hour_of_day": "hour_of_day", "day_of_week": "day_of_week", "is_flagged_fraud": "is_flagged_fraud",
    "origin_fully_drained": "origin_fully_drained::INTEGER",
    "origin_balance_zero_before": "(old_balance_org = 0)::INTEGER", "destination_balance_zero_before": "(old_balance_dest = 0)::INTEGER",
    "origin_balance_evidence": "(origin_balance_status IN ('CONSISTENT', 'INCONSISTENT'))::INTEGER",
    "destination_balance_evidence": "(destination_balance_status IN ('CONSISTENT', 'INCONSISTENT'))::INTEGER",
    "origin_balance_inconsistent": "(origin_balance_status = 'INCONSISTENT')::INTEGER",
    "destination_balance_inconsistent": "(destination_balance_status = 'INCONSISTENT')::INTEGER",
    "destination_is_merchant": "(dest_account_kind = 'merchant')::INTEGER",
    **{f"type_{t}": f"(type = '{t}')::INTEGER" for t in TYPES},
}


class LeakageError(AssertionError):
    """A model input is, or is derived from, the label."""


def suspicious_name(name: str) -> bool:
    n = name.lower()
    return (n in TARGET_DERIVED or ("fraud" in n and n not in ALLOWED_FRAUD_NAMED) or "outcome" in n
            or n.startswith(("label", "target")))


def assert_no_leakage(feature_names: list[str]) -> None:
    """Raise LeakageError unless every input is a registered, non-label feature."""
    bad = [n for n in feature_names if suspicious_name(n)]
    if bad:
        raise LeakageError(f"target-derived columns used as features: {bad}")
    unknown = [n for n in feature_names if n not in FEATURE_NAMES]
    if unknown:
        raise LeakageError(f"features that are not in the registry (classify them in features.py first): {unknown}")
    sources = [f.source for f in FEATURES]
    roles = {COLUMN_ROLES.get(s) for s in sources}
    if roles - {"feature_source"}:
        raise LeakageError(f"a feature is built from a column that is not a feature source: {sorted(r for r in roles if r != 'feature_source')}")


def classify_columns(columns: list[str]) -> dict[str, str]:
    """Roles of gold.fact_transactions columns; raises if a column has none (so it cannot become a feature unnoticed)."""
    missing = [c for c in columns if c not in COLUMN_ROLES]
    if missing:
        raise LeakageError(f"unclassified columns in gold.fact_transactions: {missing}")
    return {c: COLUMN_ROLES[c] for c in columns}


def verify_matrix_not_target(x, y) -> None:
    """No single column may equal the label (or its complement) on every row (checked whenever both classes are present)."""
    yv = y.to_numpy() if hasattr(y, "to_numpy") else y
    if len(set(yv.tolist())) < 2:                    # one class only: any constant column "equals" the label, which proves nothing
        return
    for c in x.columns:
        col = x[c].to_numpy()
        if len(col) and ((col == yv).all() or (col == 1 - yv).all()):
            raise LeakageError(f"feature {c!r} is identical to the label")
