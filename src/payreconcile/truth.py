"""Financial ground truth for the synthetic data: what SHOULD the money-flow model conclude for every chain?

Derived only from what the generator injected (true amounts, the fee schedule, the bank rows it actually wrote, the defects it
planted), never from the pipeline's output. It follows the specification in moneyflow.py, but reasons from the event log in
plain Python instead of from observed tables, so agreement between the two is evidence about ingestion, quarantine, SCD2 fee
lookup, linking and the evidence checks, not about a shared formula.

"Expected" means the best conclusion the VISIBLE data allows: where a record needed for the conclusion is quarantined, or the
data cannot separate two explanations (a settlement aggregated over two payments), the expected answer is BLOCKED /
INSUFFICIENT_EVIDENCE, because that is the correct answer for a system that must not invent a cause. Where the injected fact
is real but invisible to the pipeline (a duplicate charge whose original was quarantined without an order id) the truth keeps
the real answer, so the pipeline's miss is reported as a miss.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

import pandas as pd

from .moneyflow import SEVERITY

TRUTH_COLUMNS = ["flow_id", "flow_type", "order_id", "txn_id", "merchant_id", "expected_customer_amount",
                 "expected_gateway_amount", "expected_fee", "expected_net_settlement", "expected_bank_settlement",
                 "expected_merchant_settlement", "expected_final_status", "expected_breakpoint", "expected_resolution",
                 "expected_exposure", "expected_gap_bank_net", "note"]


def _f(stage: int, cls: str, res: str, bp: str) -> tuple:
    return (stage, cls, res, bp)


def _decide(findings: list[tuple]) -> tuple[str, str, str]:
    if not findings:
        return "MATCHED", "RECONCILED", "NONE"
    primary = min(findings, key=lambda f: (f[0], -SEVERITY[f[2]]))
    resolution = max((f[2] for f in findings), key=lambda r: SEVERITY[r])
    return primary[1], resolution, primary[3]


def build_flow_truth(txns: list[dict], no_payment_orders: list[dict], orphans: list[dict], *, start, last_day: int,
                     quarantine_defects: set, fee_true, fee_visible, bad_fee_since: dict, cfg: dict) -> pd.DataFrame:
    """One row per money chain. See moneyflow.py for the definition of a chain, the gaps and the exposure."""
    tol, ftol, sla = cfg["amount_tolerance"], cfg["fee_tolerance"], cfg["settlement_sla_days"]
    as_of = start + timedelta(days=last_day)
    by_id = {t["txn_id"]: t for t in txns}
    captured = [t for t in txns if t["status"] == "captured"]

    pays: dict[str, list[dict]] = defaultdict(list)
    for t in captured:
        if t["txn_type"] == "payment":
            pays[t["order_id"]].append(t)
    claimant = {oid: min(v, key=lambda t: (t["is_dup_charge"], t["created_at"], t["txn_id"]))["txn_id"] for oid, v in pays.items()}

    refund_state: dict[str, tuple[str, float]] = {}
    by_orig: dict[str, list[dict]] = defaultdict(list)
    for t in captured:
        if t["txn_type"] == "refund":
            by_orig[t["refund_of"]].append(t)
    for orig_id, rs in by_orig.items():
        remaining = by_id[orig_id]["amount"] if orig_id in by_id else None
        for r in sorted(rs, key=lambda t: (t["created_at"], t["txn_id"])):
            if remaining is None:
                refund_state[r["txn_id"]] = ("ORPHAN", r["amount"])
                continue
            excess = max(r["amount"] - max(remaining, 0.0), 0.0)
            remaining -= r["amount"]
            refund_state[r["txn_id"]] = ("OVER", excess) if excess > tol else ("OK", 0.0)

    rows: list[dict] = []
    for t in captured:
        rows.append(_txn_chain(t, by_id, claimant, refund_state, start, as_of, quarantine_defects, fee_true, fee_visible,
                               bad_fee_since, tol, ftol, sla))
    # paid orders that no visible payment can claim
    for o in no_payment_orders:
        if o["order_state"] != "VISIBLE":
            continue
        aged = o["created_at"].date() + timedelta(days=2) < as_of
        cls, res, bp = ("ORDER_WITHOUT_PAYMENT", "OPEN", "GATEWAY") if aged else ("PENDING", "IN_FLIGHT", "NONE")
        rows.append(_row(f"ORD:{o['order_id']}", "ORDER_ONLY", o["order_id"], None, o["merchant_id"], o["order_amount"], None,
                         None, None, None, cls, bp, res, o["order_amount"] if res == "OPEN" else 0.0, 0.0, ""))
    for oid, v in pays.items():
        if all(t["defect"] == "null_order_id" for t in v):                         # every payment is quarantined, unlinkable
            t0 = v[0]
            if t0["order_state"] != "VISIBLE":
                continue
            aged = t0["created_at"].date() + timedelta(days=2) < as_of
            cls, res, bp = (("INSUFFICIENT_EVIDENCE", "BLOCKED", "SOURCE_DATA_QUALITY") if aged
                            else ("PENDING", "IN_FLIGHT", "NONE"))
            rows.append(_row(f"ORD:{oid}", "ORDER_ONLY", oid, None, t0["merchant_id"], t0["order_amount"], None, None, None,
                             None, cls, bp, res, 0.0, 0.0, "payment exists but is quarantined without an order id"))
    for s in orphans:
        arith = s["net"] - (s["gross"] - s["fee"])
        exposure = abs(s["gross"] - s["fee"]) + (abs(arith) if abs(arith) > ftol else 0.0)
        rows.append(_row(f"STL:{s['settlement_id']}", "SETTLEMENT_ONLY", None, None, s["merchant_id"], None, None, None, None,
                         s["net"], "ORPHAN_SETTLEMENT", "BANK_SETTLEMENT", "OPEN", exposure, arith, ""))
    return pd.DataFrame(rows, columns=TRUTH_COLUMNS)


def _row(flow_id, flow_type, order_id, txn_id, merchant_id, cust, gateway, fee, net, bank, cls, bp, res, exposure, gap_net, note):
    def r2(x):
        return None if x is None else round(float(x), 2)

    return {"flow_id": flow_id, "flow_type": flow_type, "order_id": order_id, "txn_id": txn_id, "merchant_id": merchant_id,
            "expected_customer_amount": r2(cust), "expected_gateway_amount": r2(gateway), "expected_fee": r2(fee),
            "expected_net_settlement": r2(net), "expected_bank_settlement": r2(bank), "expected_merchant_settlement": None,
            "expected_final_status": cls, "expected_breakpoint": bp, "expected_resolution": res,
            "expected_exposure": r2(exposure), "expected_gap_bank_net": r2(gap_net), "note": note}


def _txn_chain(t, by_id, claimant, refund_state, start, as_of, qdefects, fee_true, fee_visible, bad_fee_since, tol, ftol, sla):
    is_refund = t["txn_type"] == "refund"
    quarantined = t["defect"] in qdefects
    fid = ("QTX:" if quarantined else "TXN:") + t["txn_id"]
    d = (t["day"] - start).days
    G = -t["amount"] if is_refund else t["amount"]
    bank = t.get("bank_rows") or []
    n = len(bank)
    B, f, N = (sum(b[k] for b in bank) for k in ("gross_amount", "fee_amount", "net_amount"))
    mid = t["merchant_id"]
    pct_true = 0.0 if is_refund else fee_true(mid, d)
    pct_vis = 0.0 if is_refund else fee_visible(mid, d)
    fee_expected = round(G * pct_true / 100, 2)
    net_expected = G - fee_expected
    claims = (not is_refund) and claimant.get(t["order_id"]) == t["txn_id"]
    state = t["order_state"]
    ostate_visible = state == "VISIBLE"
    order_amount = t["order_amount"] if (claims and ostate_visible) else None
    flow_type = "REFUND" if is_refund else "QUARANTINED_PAYMENT" if quarantined else "PAYMENT"
    bank_expected = round(N, 2) if n else None
    args = (fid, flow_type, t["order_id"], t["txn_id"], mid, order_amount, G, fee_expected, net_expected, bank_expected)

    if quarantined:
        linked_order = claims and ostate_visible and t["defect"] != "null_order_id"
        if n:
            exposure = abs(N)
        elif linked_order:
            exposure = abs(order_amount)
        else:
            exposure = 0.0 if t["defect"] == "amount_na" else abs(t["amount"])
        return _row(*args, "QUARANTINED_SOURCE", "SOURCE_DATA_QUALITY", "BLOCKED", exposure, 0.0,
                    f"gateway record quarantined ({t['defect']})")

    # ---- expectation (visible data) ------------------------------------------------------------------------------
    if is_refund:
        kind, excess = refund_state.get(t["txn_id"], ("OK", 0.0))
        expected_gateway = 0.0 if kind == "ORPHAN" else G + excess
    elif state == "QUARANTINED" and claims:
        expected_gateway = G
    else:
        expected_gateway = order_amount if claims and order_amount is not None else 0.0
    gap_gateway = G - expected_gateway
    fee_vis = round(G * pct_vis / 100, 2)
    sched = round(B * pct_vis / 100, 2)
    gap_bank_amount = (B - sched) - (G - fee_vis)
    gap_fee = sched - f
    gap_bank_net = N - (B - f)
    gaps = {"gap_gateway": gap_gateway, "gap_bank_amount": gap_bank_amount, "gap_fee": gap_fee, "gap_bank_net": gap_bank_net}
    limit = {"gap_gateway": tol, "gap_bank_amount": tol, "gap_fee": ftol, "gap_bank_net": ftol}
    excluded: set[str] = set()
    findings: list[tuple] = []

    # ---- stage 1-2: order and gateway ------------------------------------------------------------------------------
    if is_refund:
        if kind == "ORPHAN":
            findings.append(_f(2, "REFUND_MISMATCH", "OPEN", "GATEWAY"))
        elif kind == "OVER":
            findings.append(_f(2, "REFUND_MISMATCH", "OPEN", "GATEWAY"))
    else:
        if state == "ABSENT" and claims:
            findings.append(_f(1, "PAYMENT_WITHOUT_ORDER", "OPEN", "ORDER"))
        if t["is_dup_charge"]:
            first = by_id[t["dup_of"]]
            trusted_first = first["defect"] not in qdefects
            findings.append(_f(2, "DUPLICATE_PAYMENT",
                               "EXPLAINED" if trusted_first else "BLOCKED", "GATEWAY"))
        elif state == "VISIBLE" and claims and abs(gap_gateway) > tol:
            findings.append(_f(2, "GATEWAY_AMOUNT_MISMATCH", "OPEN", "GATEWAY"))
        elif state != "ABSENT" and not claims and abs(gap_gateway) > tol:
            findings.append(_f(2, "GATEWAY_AMOUNT_MISMATCH", "OPEN", "GATEWAY"))

    # ---- stage 3: fee -----------------------------------------------------------------------------------------------
    if n and abs(gap_fee) > ftol:
        since = bad_fee_since.get(mid)
        if not is_refund and since is not None and t["day"] >= since:
            findings.append(_f(3, "INSUFFICIENT_EVIDENCE", "BLOCKED", "SOURCE_DATA_QUALITY"))
        else:
            findings.append(_f(3, "FEE_MISMATCH", "OPEN", "PROCESSING"))

    # ---- stage 4: bank ------------------------------------------------------------------------------------------------
    aged = t["day"] + timedelta(days=sla) < as_of
    if n == 0:
        if not aged:
            findings.append(_f(4, "PENDING", "IN_FLIGHT", "NONE"))
        elif t["fin"] == "aggregated_missing":
            findings.append(_f(4, "INSUFFICIENT_EVIDENCE", "BLOCKED", "MATCHING"))
        else:
            findings.append(_f(4, "MISSING_SETTLEMENT", "OPEN", "BANK_SETTLEMENT"))
    else:
        gross_off = abs(B - G) > tol
        duplicate = n >= 2 and abs(B - G) > tol and sum(1 for b in bank if abs(b["gross_amount"] - G) <= tol) >= 2
        if duplicate:
            findings.append(_f(4, "DUPLICATE_SETTLEMENT", "EXPLAINED", "BANK_SETTLEMENT"))
        elif gross_off:
            if t["fin"] == "aggregated_over":
                if by_id[t["agg_partner"]]["day"] + timedelta(days=sla) < as_of:       # the partner carries the money
                    excluded.add("gap_bank_amount")
                findings.append(_f(4, "INSUFFICIENT_EVIDENCE", "BLOCKED", "MATCHING"))
            else:
                findings.append(_f(4, "REFUND_MISMATCH" if is_refund else "BANK_AMOUNT_MISMATCH", "OPEN", "BANK_SETTLEMENT"))
        arith = abs(gap_bank_net) > ftol
        if arith:
            findings.append(_f(4, "UNEXPLAINED", "OPEN", "BANK_SETTLEMENT"))
        if not gross_off and not arith:
            lag = max(b["_day"] for b in bank) - d
            if lag > sla:
                findings.append(_f(4, "TIMING_DELAY", "TIMING", "BANK_SETTLEMENT"))
            elif lag < 0:
                findings.append(_f(4, "DATE_ANOMALY", "TIMING", "BANK_SETTLEMENT"))

    cls, res, bp = _decide(findings)
    exposure = 0.0 if res in ("RECONCILED", "IN_FLIGHT", "TIMING") else sum(
        abs(v) for k, v in gaps.items() if k not in excluded and abs(v) > limit[k])
    return _row(*args, cls, bp, res, exposure, gap_bank_net if n else 0.0, t["fin"] or "")
