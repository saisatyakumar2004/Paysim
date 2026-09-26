"""Money-flow lineage: where exactly does the money stop matching?

Every money chain is followed through the stages the source data actually supports and one row is written to
``gold.fact_money_flow`` per chain (plus the underlying records to ``gold.money_flow_evidence``):

    ORDER  ->  GATEWAY  ->  FEE  ->  EXPECTED SETTLEMENT  ->  BANK  ->  MERCHANT
    order_amount  gateway_gross  expected_fee  expected_net   bank_net   (not in the source data: never fabricated)

Chains: a captured gateway payment or refund (``TXN:``), a quarantined gateway record (``QTX:``), a paid order with no
payment (``ORD:``), a bank record that belongs to no payment (``STL:``). Each bank record and each order belongs to exactly
one chain, so no money is counted twice.

Reconciliation equation (INR, tol = reconciliation.amount_tolerance, fee_tol = money_flow.fee_tolerance):

    expected_gateway = order_amount               (0 when the order is claimed by another payment or does not exist)
    expected_fee     = round_half_up(gateway_gross * fee_pct / 100, 2)   fee_pct: merchant master version valid on the
                                                                         capture date (refunds: money_flow.refund_fee_pct)
    expected_net     = gateway_gross - expected_fee
    bank_net         = sum of the linked settlement rows' net_amount (bank_gross - bank_fee is what a row should say)

    bank_net - expected_net  =  gap_bank_amount + gap_fee + gap_bank_net              (exact, no residual)
        gap_bank_amount = (bank_gross - schedule_fee(bank_gross)) - expected_net      gross settled differs from captured
        gap_fee         = schedule_fee(bank_gross) - bank_fee                          fee charged differs from the schedule
        gap_bank_net    = bank_net - (bank_gross - bank_fee)                           the bank's own net is not gross - fee
    gap_gateway = gateway_gross - expected_gateway                                      gateway vs order

FIRST_BREAKPOINT is the earliest stage (ORDER < GATEWAY < PROCESSING < BANK_SETTLEMENT < MERCHANT_SETTLEMENT) whose
finding is not RECONCILED. Evidence problems are their own breakpoints: SOURCE_DATA_QUALITY (a record needed for the
conclusion is quarantined or its batch was never loaded) and MATCHING (a candidate exists but cannot be confirmed).
Nothing is called MISSING until quarantined bank rows, halted batches, ambiguous references, candidate bank records and
aggregated settlements have been checked.

Resolution of a chain (the most severe of its findings): RECONCILED, IN_FLIGHT (not yet due), TIMING (money is right, the
date is not), EXPLAINED (a mechanism in the data accounts for the whole gap), BLOCKED (cannot be decided from the data),
OPEN (a confirmed gap at a known stage with no documented cause: the "unexplained" money).

exposure_amount is counted once per chain and filed under its first breakpoint. It is the sum of the chain's out-of-tolerance
gaps; a chain whose evidence is blocked reports the single amount in question; a chain whose money is already counted on a
related chain (``counted_in``) reports the remaining gaps only.

The merchant stage: the source data has no amount received by the merchant, so ``merchant_amount`` is NULL and the chain's
evidence stops at the bank. The only merchant-side fact available (the merchant a settlement was credited to) is checked.
"""

from __future__ import annotations

import itertools
import math
from collections import defaultdict
from datetime import date, timedelta

import pandas as pd

from . import config

STAGES = ("ORDER", "GATEWAY", "PROCESSING", "BANK_SETTLEMENT", "MERCHANT_SETTLEMENT")
BREAKPOINTS = (*STAGES, "SOURCE_DATA_QUALITY", "MATCHING", "NONE")
SEVERITY = {"RECONCILED": 0, "IN_FLIGHT": 1, "TIMING": 2, "EXPLAINED": 3, "BLOCKED": 4, "OPEN": 5}
CLASSIFICATIONS = (
    "MATCHED", "PENDING", "GATEWAY_AMOUNT_MISMATCH", "DUPLICATE_PAYMENT", "PAYMENT_WITHOUT_ORDER", "ORDER_WITHOUT_PAYMENT",
    "FEE_MISMATCH", "BANK_AMOUNT_MISMATCH", "MISSING_SETTLEMENT", "ORPHAN_SETTLEMENT", "DUPLICATE_SETTLEMENT",
    "REFUND_MISMATCH", "TIMING_DELAY", "DATE_ANOMALY", "MERCHANT_SETTLEMENT_MISMATCH", "QUARANTINED_SOURCE",
    "DATA_QUALITY_BLOCK", "UNEXPLAINED", "INSUFFICIENT_EVIDENCE")
GAPS = ("gap_gateway", "gap_bank_amount", "gap_fee", "gap_bank_net")

COLUMNS = ["flow_id", "flow_type", "order_id", "txn_id", "merchant_id", "settlement_ids", "bank_reference", "match_method",
           "recon_status", "event_date", "settlement_date", "settlement_lag_days", "n_settlements", "settlement_pattern",
           "order_state", "order_amount", "expected_gateway", "gateway_gross", "fee_pct", "expected_fee", "expected_net",
           "bank_gross", "bank_fee", "bank_net", "merchant_amount", "merchant_stage", "gap_gateway", "gap_bank_amount",
           "gap_fee", "gap_bank_net", "difference_amount", "classification", "first_breakpoint", "root_cause", "resolution",
           "evidence_level", "findings", "exposure_amount", "tolerated_amount", "counted_in", "evidence_summary",
           "as_of_date", "run_id"]


def _r2(x):
    return None if x is None else round(float(x), 2)


def _cents(x: float) -> float:
    """The scheduled fee rounds HALF UP to the cent. One explicit rule, used identically by the SQL clean path
    (floor(x * 100 + 0.5) / 100), because Python's round() and DuckDB's round() disagree on exact half-cent ties."""
    return math.floor(x * 100.0 + 0.5) / 100.0


class _Finding:
    __slots__ = ("stage", "classification", "resolution", "root_cause", "breakpoint")

    def __init__(self, stage: int, classification: str, resolution: str, root_cause: str, breakpoint: str):
        self.stage, self.classification, self.resolution, self.root_cause, self.breakpoint = (
            stage, classification, resolution, root_cause, breakpoint)


# =====================================================================================================================
# 1. load: SQL assembles the observed facts; nothing is decided here
# =====================================================================================================================
def _q(con, sql: str, params: list | None = None) -> list[dict]:
    cur = con.execute(sql, params or [])
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


# The clean path. A payment whose every leg is present, unambiguous and comfortably inside tolerance needs no per-chain
# reasoning: one SQL statement writes its row and only the rest (typically a tenth of the chains) goes through _classify(),
# which stays the reference implementation. Rows within _CLEAN_MARGIN of a tolerance always take the reference path, so the
# two paths cannot disagree on a borderline case; tests compare them on generated data.
_CLEAN_MARGIN = 0.02


def _mark_clean_chains(con, tol: float, ftol: float, sla: int) -> None:
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE mf_clean AS
        WITH pay AS (
            SELECT t.txn_id, t.order_id, t.merchant_id, cast(t.created_at AS DATE) AS event_date, t.amount AS g,
                   r.status AS recon_status, r.match_method,
                   coalesce(r.is_duplicate_charge, false) OR coalesce(r.is_suspected_duplicate, false) AS dup, m.fee_pct
            FROM silver.gateway_txns t
            LEFT JOIN gold.fact_reconciliation r ON r.item_id = 'TXN:' || t.txn_id
            LEFT JOIN silver.merchants_scd2 m ON m.merchant_id = t.merchant_id AND cast(t.created_at AS DATE) >= m.valid_from
                                              AND cast(t.created_at AS DATE) < m.valid_to
            WHERE t.status = 'captured' AND t.txn_type = 'payment' AND t.order_id IS NOT NULL),
        bank AS (
            SELECT l.txn_id, any_value(s.settlement_id) AS sid, any_value(s.gateway_ref) AS ref,
                   any_value(s.merchant_id) AS bank_merchant, sum(s.gross_amount) AS bank_gross, sum(s.fee_amount) AS bank_fee,
                   sum(s.net_amount) AS bank_net, max(s.settlement_date) AS sdate
            FROM gold.recon_links l JOIN silver.settlements s ON s.settlement_id = l.settlement_id
            GROUP BY l.txn_id HAVING count(*) = 1),
        one_payment AS (
            SELECT order_id FROM (
                SELECT order_id FROM silver.gateway_txns WHERE status = 'captured' AND txn_type = 'payment' AND order_id IS NOT NULL
                UNION ALL
                SELECT min(order_id) FROM quarantine.rows
                WHERE dataset = 'gateway' AND natural_key IS NOT NULL AND natural_key NOT IN (SELECT txn_id FROM silver.gateway_txns)
                GROUP BY natural_key HAVING min(order_id) IS NOT NULL)
            GROUP BY order_id HAVING count(*) = 1)
        SELECT p.txn_id, p.order_id, p.merchant_id, p.event_date, p.g, p.recon_status, p.match_method, p.fee_pct,
               b.sid, b.ref, b.bank_gross, b.bank_fee, b.bank_net, b.sdate, o.order_amount,
               floor(p.g * p.fee_pct / 100.0 * 100.0 + 0.5) / 100.0 AS expected_fee,
               floor(b.bank_gross * p.fee_pct / 100.0 * 100.0 + 0.5) / 100.0 AS sched
        FROM pay p
        JOIN bank b ON b.txn_id = p.txn_id AND b.bank_merchant = p.merchant_id
        JOIN silver.orders o ON o.order_id = p.order_id
        JOIN one_payment c ON c.order_id = p.order_id
        WHERE NOT p.dup AND p.fee_pct IS NOT NULL
          AND abs(p.g - o.order_amount) <= {tol - _CLEAN_MARGIN}
          AND abs(b.bank_gross - p.g) <= {tol - _CLEAN_MARGIN}
          AND abs(floor(b.bank_gross * p.fee_pct / 100.0 * 100.0 + 0.5) / 100.0 - b.bank_fee) <= {ftol - _CLEAN_MARGIN}
          AND abs(b.bank_net - (b.bank_gross - b.bank_fee)) <= {ftol - _CLEAN_MARGIN}
          AND date_diff('day', p.event_date, b.sdate) BETWEEN 0 AND {sla}""")


def _insert_clean_rows(con, as_of: date, run_id: str, tol: float, ftol: float) -> int:
    """Write the clean chains straight from SQL. Same columns, same rounding (2 dp) and same wording as _classify()."""
    def m(col):
        return f"printf('%.2f', {col})"

    gaps = {"(g - order_amount)": tol, "((bank_gross - sched) - (g - expected_fee))": tol, "(sched - bank_fee)": ftol,
            "(bank_net - (bank_gross - bank_fee))": ftol}
    tolerated = " + ".join(f"CASE WHEN abs({e}) > 0 AND abs({e}) <= {lim} THEN abs({e}) ELSE 0 END" for e, lim in gaps.items())
    con.execute(f"""
        INSERT INTO gold.fact_money_flow
        SELECT 'TXN:' || txn_id, 'PAYMENT', order_id, txn_id, merchant_id, sid, ref, match_method, recon_status, event_date,
               sdate, date_diff('day', event_date, sdate), 1, 'SINGLE', 'VISIBLE',
               round(order_amount, 2), round(order_amount, 2), round(g, 2), fee_pct, round(expected_fee, 2),
               round(g - expected_fee, 2), round(bank_gross, 2), round(bank_fee, 2), round(bank_net, 2), NULL::DOUBLE,
               'NOT_AVAILABLE', round(g - order_amount, 2), round((bank_gross - sched) - (g - expected_fee), 2),
               round(sched - bank_fee, 2), round(bank_net - (bank_gross - bank_fee), 2), round(bank_net - (g - expected_fee), 2),
               'MATCHED', 'NONE', 'NONE', 'RECONCILED', CASE WHEN match_method = 'fuzzy' THEN 'MEDIUM' ELSE 'HIGH' END,
               'MATCHED@NONE', 0.0, round({tolerated}, 2), NULL::VARCHAR,
               'MATCHED at NONE (RECONCILED); order ' || {m('round(order_amount, 2)')} || ' -> gateway ' || {m('round(g, 2)')}
               || ' -> expected net ' || {m('round(g - expected_fee, 2)')} || ' -> bank ' || {m('round(bank_net, 2)')}
               || ' -> merchant n/a; cause NONE',
               DATE '{as_of.isoformat()}', '{run_id}'
        FROM mf_clean ORDER BY txn_id""")
    return con.execute("SELECT count(*) FROM mf_clean").fetchone()[0]


def _load(con, as_of: date, fast: bool) -> dict:
    skip_txn = "AND t.txn_id NOT IN (SELECT txn_id FROM mf_clean)" if fast else ""
    skip_order = "WHERE order_id NOT IN (SELECT order_id FROM mf_clean)" if fast else ""
    skip_bank = "WHERE l.txn_id IS NULL OR l.txn_id NOT IN (SELECT txn_id FROM mf_clean)" if fast else ""
    txns = _q(con, f"""
        SELECT 'TXN:' || t.txn_id AS flow_id, t.txn_id, t.order_id, t.merchant_id, t.txn_type, t.refund_of, t.created_at,
               cast(t.created_at AS DATE) AS event_date, t.amount,
               CASE WHEN t.txn_type = 'refund' THEN -t.amount ELSE t.amount END AS gateway_gross,
               r.status AS recon_status, r.match_method,
               CASE WHEN r.is_duplicate_charge THEN 'CONFIRMED' WHEN r.is_suspected_duplicate THEN 'SUSPECTED' END AS dup_flag,
               m.fee_pct
        FROM silver.gateway_txns t
        LEFT JOIN gold.fact_reconciliation r ON r.item_id = 'TXN:' || t.txn_id
        LEFT JOIN silver.merchants_scd2 m ON m.merchant_id = t.merchant_id AND cast(t.created_at AS DATE) >= m.valid_from
                                          AND cast(t.created_at AS DATE) < m.valid_to
        WHERE t.status = 'captured' {skip_txn}""")
    qtx = _q(con, """
        SELECT 'QTX:' || txn_id AS flow_id, txn_id, order_id, rule_ids, json_extract_string(raw, '$.merchant_id') AS merchant_id,
               pr_amt(json_extract_string(raw, '$.amount')) AS raw_amount,
               pr_ts(json_extract_string(raw, '$.created_at')) AS raw_created,
               lower(coalesce(json_extract_string(raw, '$.status'), '')) AS raw_status
        FROM (SELECT natural_key AS txn_id, min(order_id) AS order_id, string_agg(DISTINCT rule_ids, ';') AS rule_ids,
                     min(raw_json) AS raw
              FROM quarantine.rows
              WHERE dataset = 'gateway' AND natural_key IS NOT NULL
                AND natural_key NOT IN (SELECT txn_id FROM silver.gateway_txns)
              GROUP BY natural_key)""")
    orders = _q(con, f"SELECT order_id, merchant_id, order_amount, order_status, created_at FROM silver.orders {skip_order}")
    ord_only = _q(con, """
        SELECT order_id FROM silver.orders o
        WHERE order_status IN ('paid', 'refunded')
          AND NOT EXISTS (SELECT 1 FROM silver.gateway_txns t WHERE t.order_id = o.order_id AND t.status = 'captured'
                          AND t.txn_type = 'payment')
          AND order_id NOT IN (SELECT min(order_id) FROM quarantine.rows WHERE dataset = 'gateway' AND natural_key IS NOT NULL
                               AND natural_key NOT IN (SELECT txn_id FROM silver.gateway_txns) AND order_id IS NOT NULL
                               GROUP BY natural_key)""")
    q_orders = {r["natural_key"] for r in _q(con, "SELECT DISTINCT natural_key FROM quarantine.rows "
                                                  "WHERE dataset = 'orders' AND natural_key IS NOT NULL")}
    bank = _q(con, f"""
        SELECT s.settlement_id, s.gateway_ref, s.gateway_ref_norm, s.merchant_id, s.gross_amount, s.fee_amount, s.net_amount,
               s.settlement_date, l.txn_id AS linked_txn, l.match_method AS link_method
        FROM silver.settlements s LEFT JOIN gold.recon_links l ON l.settlement_id = s.settlement_id {skip_bank}""")
    q_stl = _q(con, """
        SELECT natural_key AS settlement_id,
               upper(regexp_replace(trim(coalesce(json_extract_string(raw_json, '$.gateway_ref'),
                                                  json_extract_string(raw_json, '$.txn_reference'))), '^(?i:GW-)', '')) AS ref_norm,
               rule_ids
        FROM quarantine.rows
        WHERE dataset = 'settlements' AND (natural_key IS NULL OR natural_key NOT IN (SELECT settlement_id FROM silver.settlements))""")
    halted = _q(con, "SELECT dataset, run_date FROM ops.dq_scores GROUP BY 1, 2 HAVING bool_and(NOT loaded)")
    ambiguous = {r["txn_id"] for r in _q(con, "SELECT DISTINCT txn_id FROM gold.recon_match_evidence "
                                              "WHERE decision = 'AMBIGUOUS_NOT_LINKED'")}
    q_merchants = {r["natural_key"]: r["since"] for r in _q(
        con, "SELECT natural_key, min(run_date) AS since FROM quarantine.rows WHERE dataset = 'merchants' "
             "AND natural_key IS NOT NULL AND rule_ids NOT LIKE '%MR-UNQ-01%' GROUP BY 1")}
    refund_targets = "SELECT refund_of FROM silver.gateway_txns WHERE txn_type = 'refund' AND refund_of IS NOT NULL"
    orig = {r["txn_id"]: r["amount"] for r in _q(
        con, f"SELECT txn_id, amount FROM silver.gateway_txns WHERE txn_type = 'payment' AND txn_id IN ({refund_targets})")}
    known_ids = {r["txn_id"] for r in _q(
        con, f"""SELECT txn_id FROM silver.gateway_txns WHERE txn_id IN ({refund_targets}) OR txn_id IN (
                     SELECT gateway_ref_norm FROM silver.settlements WHERE settlement_id NOT IN (SELECT settlement_id FROM gold.recon_links))""")}
    q_gw_keys = {r["natural_key"] for r in _q(con, "SELECT DISTINCT natural_key FROM quarantine.rows WHERE dataset = 'gateway' "
                                                   "AND natural_key IS NOT NULL")}
    return {"txns": txns, "qtx": qtx, "orders": orders, "ord_only": {r["order_id"] for r in ord_only}, "q_orders": q_orders,
            "bank": bank, "q_stl": q_stl, "halted": halted, "ambiguous": ambiguous, "q_merchants": q_merchants, "orig": orig,
            "all_ids": known_ids, "q_gw_keys": q_gw_keys, "as_of": as_of}


# =====================================================================================================================
# 2. chains
# =====================================================================================================================
def _bank_by_flow(d: dict, qtx_keys: set) -> dict[str, list[dict]]:
    """Assign every bank record to exactly one chain."""
    out: dict[str, list[dict]] = defaultdict(list)
    for r in d["bank"]:
        if r["linked_txn"] is not None:
            fid = f"TXN:{r['linked_txn']}"
        elif r["gateway_ref_norm"] in qtx_keys:
            fid = f"QTX:{r['gateway_ref_norm']}"
        else:
            fid = f"STL:{r['settlement_id']}"
        out[fid].append(r)
    return out


def _claims(txns: list[dict], qtx: list[dict]) -> dict[str, str]:
    """order_id -> flow_id of the ONE chain that carries the order amount.

    Preference: a trusted payment that is not flagged as a duplicate (earliest first), then a quarantined payment (the
    original of a suspected duplicate), then a flagged duplicate. The others carry expected_gateway = 0, so an extra
    capture shows as a gateway gap instead of being hidden by the order amount."""
    cands: dict[str, list[tuple]] = defaultdict(list)
    for r in txns:
        if r["txn_type"] == "payment" and r["order_id"] is not None:
            cands[r["order_id"]].append((2 if r["dup_flag"] else 0, str(r["created_at"]), r["flow_id"]))
    for r in qtx:
        if r["order_id"] is not None:
            cands[r["order_id"]].append((1, "", r["flow_id"]))
    return {oid: min(c)[2] for oid, c in cands.items()}


def _refund_state(txns: list[dict], amt: dict, all_ids: set, q_keys: set) -> dict[str, dict]:
    """flow_id -> {state, excess}: ORPHAN (original exists nowhere), ORIGINAL_QUARANTINED, OVER (cumulative refunds exceed the
    payment; `excess` is the part beyond it) or OK. Refunds of one payment are taken in capture order."""
    out: dict[str, dict] = {}
    spent: dict[str, float] = defaultdict(float)
    tol = config.settings()["reconciliation"]["amount_tolerance"]
    for r in sorted((r for r in txns if r["txn_type"] == "refund"), key=lambda r: (r["created_at"], r["txn_id"])):
        o = r["refund_of"]
        if o is None or (o not in all_ids and o not in q_keys):
            out[r["flow_id"]] = {"state": "ORPHAN", "excess": r["amount"]}
        elif o in q_keys and o not in all_ids:
            out[r["flow_id"]] = {"state": "ORIGINAL_QUARANTINED", "excess": 0.0}
        else:
            remaining = max(amt.get(o, 0.0) - spent[o], 0.0)
            excess = max(r["amount"] - remaining, 0.0)
            spent[o] += r["amount"]
            out[r["flow_id"]] = {"state": "OVER" if excess > tol else "OK", "excess": excess if excess > tol else 0.0}
    return out


# =====================================================================================================================
# 3. explain-away hypotheses: candidate bank records and aggregated settlements
# =====================================================================================================================
def _hypotheses(pool: list[dict], targets: list[dict], cfg: dict) -> tuple[dict, dict]:
    """Try to explain unattributed bank money with unsettled payments of the same merchant.

    pool    unsettled payment chains: {flow_id, merchant_id, event_date, amount}
    targets bank amounts that need an owner: {flow_id, merchant_id, date, amount}  (orphan bank record, or the excess of an
            over-settled payment)
    A payment set of size 1..N whose amounts add up to the target (within tolerance) is a HYPOTHESIS: the data has no batch
    reference, so it is never a confirmed match. Returns (members_of[target], targets_of[member])."""
    tol = config.settings()["reconciliation"]["amount_tolerance"]
    window, batch_window, kmax = cfg["candidate_window_days"], cfg["settlement_sla_days"], cfg["aggregation_max_members"]
    by_m: dict[str, list[dict]] = defaultdict(list)
    for p in pool:
        by_m[p["merchant_id"]].append(p)
    members_of: dict[str, list[tuple]] = {}
    targets_of: dict[str, list[str]] = defaultdict(list)
    for t in sorted(targets, key=lambda x: x["flow_id"]):
        near = [p for p in by_m.get(t["merchant_id"], [])
                if timedelta(0) <= (t["date"] - p["event_date"]) <= timedelta(days=window) and p["flow_id"] != t["flow_id"]]
        near = sorted(near, key=lambda p: ((t["date"] - p["event_date"]).days, p["flow_id"]))[:40]
        found: list[tuple] = []
        for k in range(1, kmax + 1):
            for combo in itertools.combinations(near, k):
                # one payment may have been settled on another date; several payments are one bank batch: same window as usual
                if k > 1 and any((t["date"] - p["event_date"]).days > batch_window for p in combo):
                    continue
                if abs(sum(p["amount"] for p in combo) - t["amount"]) <= tol:
                    found.append(tuple(sorted(p["flow_id"] for p in combo)))
        if found:
            members_of[t["flow_id"]] = found
            for combo in found:
                for m in combo:
                    if t["flow_id"] not in targets_of[m]:
                        targets_of[m].append(t["flow_id"])
    return members_of, targets_of


# =====================================================================================================================
# 4. build
# =====================================================================================================================
def build(con, run_id: str = "manual", fast: bool = True) -> dict:
    """Rebuild gold.fact_money_flow and gold.money_flow_evidence from Silver, quarantine and the reconciliation state.

    Deterministic: the same inputs give the same rows, so incremental and full reconciliation give identical money flow.
    `fast=False` sends every chain through the reference implementation (used by tests and by verify)."""
    st = con.execute("SELECT as_of_date FROM ops.recon_state WHERE id = 1").fetchone()
    if not st:
        return {"flows": 0, "note": "no reconciliation state"}
    as_of = st[0]
    cfg = {**config.settings()["reconciliation"], **config.settings()["money_flow"]}
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM gold.fact_money_flow")
        con.execute("DELETE FROM gold.money_flow_evidence")
        n_clean = 0
        if fast:
            _mark_clean_chains(con, cfg["amount_tolerance"], cfg["fee_tolerance"], cfg["settlement_sla_days"])
            n_clean = _insert_clean_rows(con, as_of, run_id, cfg["amount_tolerance"], cfg["fee_tolerance"])
        d = _load(con, as_of, fast)
        rows, evidence = _classify_all(d, cfg)
        if rows:
            df = pd.DataFrame(rows, columns=COLUMNS[:-2])
            df["as_of_date"] = as_of
            df["run_id"] = run_id
            con.register("mf_df", df)
            con.execute(f"INSERT INTO gold.fact_money_flow SELECT {', '.join(COLUMNS)} FROM mf_df")
            con.unregister("mf_df")
        if evidence:
            ev = pd.DataFrame(evidence, columns=["flow_id", "evidence_type", "record_ref", "detail", "amount"])
            con.register("mf_ev", ev)
            con.execute("INSERT INTO gold.money_flow_evidence SELECT * FROM mf_ev")
            con.unregister("mf_ev")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return {"flows": n_clean + len(rows), "clean_path": n_clean, "evidence_rows": len(evidence), "as_of": as_of}


def _classify_all(d: dict, cfg: dict) -> tuple[list[dict], list[tuple]]:
    tol, ftol, sla = cfg["amount_tolerance"], cfg["fee_tolerance"], cfg["settlement_sla_days"]
    as_of: date = d["as_of"]
    txns, qtx, orders = d["txns"], d["qtx"], d["orders"]
    qtx_keys = {r["txn_id"] for r in qtx}
    bank = _bank_by_flow(d, qtx_keys)
    claims = _claims(txns, qtx)
    refunds = _refund_state(txns, d["orig"], d["all_ids"], d["q_gw_keys"])
    order_by_id = {r["order_id"]: r for r in orders}
    halted: dict[str, set] = defaultdict(set)
    for r in d["halted"]:
        halted[r["dataset"]].add(r["run_date"])
    q_stl_refs: dict[str, list[str]] = defaultdict(list)
    for r in d["q_stl"]:
        if r["ref_norm"]:
            q_stl_refs[r["ref_norm"]].append(str(r["settlement_id"]))

    def order_state(oid) -> str:
        if oid is None:
            return "ABSENT"
        return "VISIBLE" if oid in order_by_id else "QUARANTINED" if oid in d["q_orders"] else "ABSENT"

    # ---- chains ------------------------------------------------------------------------------------------------
    chains: list[dict] = []
    for r in txns:
        r["kind"] = "REFUND" if r["txn_type"] == "refund" else "PAYMENT"
        r["bank"] = bank.get(r["flow_id"], [])
        chains.append(r)
    for r in qtx:
        r.update(kind="QTX", bank=bank.get(r["flow_id"], []))
        r["event_date"] = r["raw_created"].date() if r.get("raw_created") is not None else None
        chains.append(r)
    for oid in sorted(d["ord_only"]):
        o = order_by_id[oid]
        chains.append({"flow_id": f"ORD:{oid}", "kind": "ORDER", "order_id": oid, "merchant_id": o["merchant_id"],
                       "event_date": o["created_at"].date(), "created_at": o["created_at"], "bank": []})
    for fid, recs in bank.items():
        if fid.startswith("STL:"):
            b = recs[0]
            chains.append({"flow_id": fid, "kind": "ORPHAN", "merchant_id": b["merchant_id"], "bank": recs,
                           "event_date": b["settlement_date"]})

    # ---- money on the bank side, per chain ---------------------------------------------------------------------
    for c in chains:
        b = c["bank"]
        c["n"] = len(b)
        c["B"] = sum(x["gross_amount"] for x in b)
        c["f"] = sum(x["fee_amount"] for x in b)
        c["N"] = sum(x["net_amount"] for x in b)
        c["s_first"] = min((x["settlement_date"] for x in b), default=None)
        c["s_last"] = max((x["settlement_date"] for x in b), default=None)

    # ---- hypotheses about unattributed bank money -----------------------------------------------------------------
    pool = [{"flow_id": c["flow_id"], "merchant_id": c["merchant_id"], "event_date": c["event_date"], "amount": c["gateway_gross"]}
            for c in chains if c["kind"] == "PAYMENT" and c["n"] == 0 and c["gateway_gross"] > 0]   # in-flight ones too
    targets = [{"flow_id": c["flow_id"], "merchant_id": c["merchant_id"], "date": c["s_first"], "amount": c["B"]}
               for c in chains if c["kind"] == "ORPHAN"]
    for c in chains:
        if c["kind"] == "PAYMENT" and c["n"] and c["B"] - c["gateway_gross"] > tol and c["dup_flag"] is None:
            targets.append({"flow_id": c["flow_id"], "merchant_id": c["merchant_id"], "date": c["s_first"],
                            "amount": c["B"] - c["gateway_gross"]})
    members_of, targets_of = _hypotheses(pool, targets, cfg)
    aged_pool = {p["flow_id"] for p in pool if p["event_date"] + timedelta(days=sla) < as_of}   # in-flight members carry no money

    ctx = {"tol": tol, "ftol": ftol, "sla": sla, "as_of": as_of, "cfg": cfg, "claims": claims, "refunds": refunds,
           "order_by_id": order_by_id, "order_state": order_state, "halted": halted, "q_stl_refs": q_stl_refs,
           "ambiguous": d["ambiguous"], "q_merchants": d["q_merchants"], "members_of": members_of, "targets_of": targets_of,
           "aged_pool": aged_pool, "qtx": qtx, "all_ids": d["all_ids"], "qtx_null_order": [r for r in qtx if r.get("order_id") is None]}
    rows, evidence = [], []
    for c in sorted(chains, key=lambda x: x["flow_id"]):
        row, ev = _classify(c, ctx)
        rows.append(row)
        evidence.extend(ev)
    return rows, evidence


# =====================================================================================================================
# 5. one chain
# =====================================================================================================================
def _classify(c: dict, x: dict) -> tuple[dict, list[tuple]]:
    tol, ftol, sla, as_of = x["tol"], x["ftol"], x["sla"], x["as_of"]
    kind, fid = c["kind"], c["flow_id"]
    bank, n = c["bank"], c["n"]
    B, f, N = c["B"], c["f"], c["N"]
    findings: list[_Finding] = []
    ev: list[tuple] = []
    excluded: set[str] = set()            # gaps whose money is counted on a related chain (counted_in)
    counted_in = None
    pattern = "NONE" if n == 0 else "SINGLE"

    G = c.get("gateway_gross")
    pct = c.get("fee_pct")
    if kind == "REFUND":
        pct = x["cfg"]["refund_fee_pct"]
    order_id = c.get("order_id")
    ostate = "NOT_APPLICABLE" if kind in ("REFUND", "ORPHAN") else x["order_state"](order_id)
    order = x["order_by_id"].get(order_id) if kind in ("PAYMENT", "QTX", "ORDER") else None
    claims = kind in ("PAYMENT", "QTX") and order_id is not None and x["claims"].get(order_id) == fid
    if kind == "ORDER":
        ostate = "VISIBLE"

    # ---- expectation ------------------------------------------------------------------------------------------
    order_amount = float(order["order_amount"]) if order is not None and (claims or kind == "ORDER") else None
    if kind == "PAYMENT":
        if ostate == "QUARANTINED" and claims:
            expected_gateway = G                                   # order unverifiable: no expectation, no gap
        else:                                                      # 0 when the order is absent or claimed by another capture
            expected_gateway = order_amount if claims and order_amount is not None else 0.0
    elif kind == "REFUND":
        rs = x["refunds"].get(fid, {"state": "OK", "excess": 0.0})
        expected_gateway = G + rs["excess"] if rs["state"] != "ORPHAN" else 0.0     # G is negative: allowed = G + excess
        if rs["state"] == "ORIGINAL_QUARANTINED":
            expected_gateway = G
    elif kind == "QTX":
        expected_gateway = order_amount if claims and order_amount is not None else 0.0
    elif kind == "ORDER":
        expected_gateway = order_amount
    else:
        expected_gateway = None
    Gv = 0.0 if G is None else G
    gap_gateway = Gv - (0.0 if expected_gateway is None else expected_gateway)

    if kind in ("PAYMENT", "REFUND"):
        pct_known = pct is not None
        expected_fee = _cents(Gv * pct / 100.0) if pct_known else f
        expected_net = Gv - expected_fee
        sched = _cents(B * pct / 100.0) if pct_known else f
    else:
        pct_known, expected_fee, expected_net = False, None, 0.0
        sched = f                                                   # no expectation on the fee for these chains
    gap_bank_amount = (B - sched) - expected_net
    gap_fee = sched - f
    gap_bank_net = N - (B - f)

    if kind == "PAYMENT" and n >= 2:
        if abs(B - Gv) <= tol:
            pattern = "SPLIT"
        elif sum(1 for b in bank if abs(b["gross_amount"] - Gv) <= tol) >= 2:
            pattern = "DUPLICATE"
        else:
            pattern = "MULTIPLE"
    lag = None
    if n and c.get("event_date") is not None and kind in ("PAYMENT", "REFUND"):
        lag = (c["s_last"] - c["event_date"]).days

    def add(stage, cls, res, cause, bp):
        findings.append(_Finding(stage, cls, res, cause, bp))

    # ---- findings by stage (1 ORDER .. 5 MERCHANT), evidence-blocked chains first ---------------------------------------
    if kind == "QTX":
        rules = c.get("rule_ids") or ""
        add(2, "QUARANTINED_SOURCE", "BLOCKED", f"GATEWAY_RECORD_QUARANTINED[{rules}]", "SOURCE_DATA_QUALITY")
        ev.append((fid, "QUARANTINE", f"gateway:{c['txn_id']}", f"rules {rules}; raw amount {c.get('raw_amount')}",
                   c.get("raw_amount")))
    elif kind == "ORDER":
        aged = c["event_date"] + timedelta(days=2) < as_of
        cand = [q for q in x["qtx_null_order"] if q.get("merchant_id") == c["merchant_id"] and q.get("raw_amount") is not None
                and abs(abs(q["raw_amount"]) - order_amount) <= tol
                and (q.get("raw_created") is None or abs((q["raw_created"] - c["created_at"]).days) <= 1)]
        gw_halt = sorted(dt for dt in x["halted"].get("gateway", ()) if c["event_date"] <= dt <= c["event_date"] + timedelta(days=2))
        if not aged:
            add(2, "PENDING", "IN_FLIGHT", "ORDER_TOO_RECENT_TO_JUDGE", "NONE")
        elif cand:
            counted_in = cand[0]["flow_id"]
            excluded.add("gap_gateway")
            add(2, "INSUFFICIENT_EVIDENCE", "BLOCKED", "QUARANTINED_PAYMENT_WITHOUT_ORDER_ID_MAY_BELONG_TO_THIS_ORDER",
                "SOURCE_DATA_QUALITY")
            for q in cand:
                ev.append((fid, "CANDIDATE", q["flow_id"], f"quarantined gateway record, no order_id; amount {q['raw_amount']}", q["raw_amount"]))
        elif gw_halt:
            add(2, "DATA_QUALITY_BLOCK", "BLOCKED", "GATEWAY_BATCH_NOT_LOADED", "SOURCE_DATA_QUALITY")
            ev += [(fid, "HALTED_BATCH", f"gateway@{dt}", "quality gate halted the batch", None) for dt in gw_halt]
        else:
            add(2, "ORDER_WITHOUT_PAYMENT", "OPEN", "PAID_ORDER_HAS_NO_CAPTURED_PAYMENT", "GATEWAY")
        ev.append((fid, "ORDER", order_id, f"status {order['order_status']}", order_amount))
    elif kind == "ORPHAN":
        b0 = bank[0]
        hyp = x["members_of"].get(fid)
        gw_halt = sorted(dt for dt in x["halted"].get("gateway", ())
                         if c["s_first"] - timedelta(days=sla) <= dt <= c["s_first"])
        if hyp:
            members = sorted({m for combo in hyp for m in combo})
            carried = [m for m in members if m in x["aged_pool"]]        # the money is counted on a member that carries it
            if carried:
                counted_in = carried[0]
                excluded.add("gap_bank_amount")
            single = all(len(cb) == 1 for cb in hyp)
            add(4, "INSUFFICIENT_EVIDENCE", "BLOCKED",
                "CANDIDATE_PAYMENT_UNDER_DIFFERENT_REFERENCE" if single else "POSSIBLE_AGGREGATED_SETTLEMENT", "MATCHING")
            ev += [(fid, "CANDIDATE", m, "unsettled payment whose amount would explain this bank record", None) for m in members]
        elif gw_halt:
            add(2, "DATA_QUALITY_BLOCK", "BLOCKED", "GATEWAY_BATCH_NOT_LOADED", "SOURCE_DATA_QUALITY")
            ev += [(fid, "HALTED_BATCH", f"gateway@{dt}", "quality gate halted the batch", None) for dt in gw_halt]
        else:
            not_captured = b0["gateway_ref_norm"] in x["all_ids"]
            add(4, "ORPHAN_SETTLEMENT", "OPEN",
                "BANK_PAID_A_PAYMENT_THE_GATEWAY_DID_NOT_CAPTURE" if not_captured else "BANK_RECORD_HAS_NO_GATEWAY_PAYMENT",
                "BANK_SETTLEMENT")
    else:
        # PAYMENT or REFUND ------------------------------------------------------------------------------------------
        if kind == "PAYMENT":
            if ostate == "ABSENT" and claims:
                add(1, "PAYMENT_WITHOUT_ORDER", "OPEN", "ORDER_NOT_FOUND", "ORDER")
            if c["dup_flag"] == "CONFIRMED":
                add(2, "DUPLICATE_PAYMENT", "EXPLAINED", "SECOND_CAPTURE_SAME_ORDER_AMOUNT_WITHIN_WINDOW", "GATEWAY")
            elif c["dup_flag"] == "SUSPECTED":
                add(2, "DUPLICATE_PAYMENT", "BLOCKED", "SECOND_CAPTURE_ORIGINAL_QUARANTINED", "GATEWAY")
            elif ostate == "VISIBLE" and claims and abs(gap_gateway) > tol:
                add(2, "GATEWAY_AMOUNT_MISMATCH", "OPEN", "GATEWAY_AMOUNT_DIFFERS_FROM_ORDER", "GATEWAY")
            elif ostate != "ABSENT" and not claims and abs(gap_gateway) > tol:
                add(2, "GATEWAY_AMOUNT_MISMATCH", "OPEN", "ADDITIONAL_CAPTURE_ON_ORDER", "GATEWAY")
        else:
            rs = x["refunds"].get(fid, {"state": "OK"})
            if rs["state"] == "ORPHAN":
                add(2, "REFUND_MISMATCH", "OPEN", "REFUND_OF_UNKNOWN_PAYMENT", "GATEWAY")
            elif rs["state"] == "ORIGINAL_QUARANTINED":
                add(2, "QUARANTINED_SOURCE", "BLOCKED", "ORIGINAL_PAYMENT_QUARANTINED", "SOURCE_DATA_QUALITY")
            elif rs["state"] == "OVER":
                add(2, "REFUND_MISMATCH", "OPEN", "REFUNDS_EXCEED_THE_ORIGINAL_PAYMENT", "GATEWAY")
        if n and abs(gap_fee) > ftol:
            since = x["q_merchants"].get(c["merchant_id"])
            if kind == "PAYMENT" and since is not None and c["event_date"] >= since:
                add(3, "INSUFFICIENT_EVIDENCE", "BLOCKED", "FEE_SCHEDULE_ROW_QUARANTINED", "SOURCE_DATA_QUALITY")
                ev.append((fid, "QUARANTINE", f"merchants:{c['merchant_id']}", "fee schedule row failed validation", None))
            else:
                add(3, "FEE_MISMATCH", "OPEN", "BANK_FEE_DIFFERS_FROM_SCHEDULE", "PROCESSING")
        refund_cls = "REFUND_MISMATCH" if kind == "REFUND" else "BANK_AMOUNT_MISMATCH"
        if n == 0:
            aged = c["event_date"] + timedelta(days=sla) < as_of
            refs = x["q_stl_refs"].get(c["txn_id"], [])
            bank_halt = sorted(dt for dt in x["halted"].get("settlements", ())
                               if c["event_date"] <= dt <= c["event_date"] + timedelta(days=x["cfg"]["candidate_window_days"])
                               and dt <= as_of)
            hyp = x["targets_of"].get(fid)
            if not aged:
                add(4, "PENDING", "IN_FLIGHT", "SETTLEMENT_NOT_YET_DUE", "NONE")
            elif refs:
                add(4, "QUARANTINED_SOURCE", "BLOCKED", "SETTLEMENT_RECORD_QUARANTINED", "SOURCE_DATA_QUALITY")
                ev += [(fid, "QUARANTINE", f"settlements:{s}", "bank record failed validation", None) for s in refs]
            elif bank_halt:
                add(4, "DATA_QUALITY_BLOCK", "BLOCKED", "SETTLEMENT_BATCH_NOT_LOADED", "SOURCE_DATA_QUALITY")
                ev += [(fid, "HALTED_BATCH", f"settlements@{dt}", "quality gate halted the batch", None) for dt in bank_halt]
            elif c["txn_id"] in x["ambiguous"]:
                add(4, "INSUFFICIENT_EVIDENCE", "BLOCKED", "AMBIGUOUS_REFERENCE_MATCH", "MATCHING")
            elif hyp:
                diff_ref = any(len(cb) == 1 and t.startswith("STL:") for t in hyp for cb in x["members_of"][t] if fid in cb)
                add(4, "INSUFFICIENT_EVIDENCE", "BLOCKED",
                    "CANDIDATE_BANK_RECORD_UNDER_DIFFERENT_REFERENCE" if diff_ref else "POSSIBLE_AGGREGATED_SETTLEMENT",
                    "MATCHING")
                ev += [(fid, "CANDIDATE", t, "bank money whose amount this payment would explain", None) for t in hyp]
            else:
                add(4, "MISSING_SETTLEMENT", "OPEN", "NO_BANK_RECORD_AFTER_ALL_EVIDENCE_CHECKS", "BANK_SETTLEMENT")
        else:
            gross_off = abs(B - Gv) > tol
            if pattern == "DUPLICATE":
                add(4, "DUPLICATE_SETTLEMENT", "EXPLAINED", "SAME_REFERENCE_SETTLED_MORE_THAN_ONCE", "BANK_SETTLEMENT")
            elif gross_off:
                hyp = x["members_of"].get(fid)
                if hyp:
                    members = sorted({m for combo in hyp for m in combo})
                    carried = [m for m in members if m in x["aged_pool"]]
                    if carried:
                        counted_in = carried[0]
                        excluded.add("gap_bank_amount")
                    add(4, "INSUFFICIENT_EVIDENCE", "BLOCKED", "POSSIBLE_AGGREGATED_SETTLEMENT", "MATCHING")
                    ev += [(fid, "CANDIDATE", m, "unsettled payment that would explain the excess", None) for m in members]
                else:
                    cause = "BANK_PAID_LESS_THAN_CAPTURED" if abs(B) < abs(Gv) else "BANK_PAID_MORE_THAN_CAPTURED"
                    add(4, refund_cls, "OPEN", cause, "BANK_SETTLEMENT")
            if abs(gap_bank_net) > ftol:
                add(4, "UNEXPLAINED", "OPEN", "BANK_NET_IS_NOT_GROSS_MINUS_FEE", "BANK_SETTLEMENT")
            if not gross_off and abs(gap_bank_net) <= ftol and pattern != "DUPLICATE" and lag is not None:
                if lag > sla:
                    add(4, "TIMING_DELAY", "TIMING", "SETTLED_AFTER_THE_SLA", "BANK_SETTLEMENT")
                elif lag < 0:
                    add(4, "DATE_ANOMALY", "TIMING", "SETTLEMENT_DATED_BEFORE_CAPTURE", "BANK_SETTLEMENT")
            if any(b["merchant_id"] != c["merchant_id"] for b in bank):
                add(5, "MERCHANT_SETTLEMENT_MISMATCH", "OPEN", "SETTLEMENT_CREDITED_TO_DIFFERENT_MERCHANT", "MERCHANT_SETTLEMENT")
        ev.append((fid, "GATEWAY", c["txn_id"], f"captured {c['txn_type']}", G))
        if order_id is not None:
            ev.append((fid, "ORDER", order_id, f"order {ostate}", order_amount))

    # ---- decide -------------------------------------------------------------------------------------------------
    if not findings:
        findings.append(_Finding(0, "MATCHED", "RECONCILED", "NONE", "NONE"))
    primary = min(findings, key=lambda fd: (fd.stage, -SEVERITY[fd.resolution]))
    resolution = max((fd.resolution for fd in findings), key=lambda s: SEVERITY[s])
    gaps = {"gap_gateway": gap_gateway, "gap_bank_amount": gap_bank_amount, "gap_fee": gap_fee, "gap_bank_net": gap_bank_net}
    limit = {"gap_gateway": tol, "gap_bank_amount": tol, "gap_fee": ftol, "gap_bank_net": ftol}
    if resolution in ("RECONCILED", "IN_FLIGHT", "TIMING"):
        exposure = 0.0
    elif kind == "QTX":            # one amount in question: the bank money if it moved, else the order, else the raw amount
        uncaptured = c.get("raw_status") in ("failed", "pending") and not n
        exposure = 0.0 if uncaptured else abs(N) if n else abs(order_amount) if order_amount else abs(c.get("raw_amount") or 0.0)
        excluded = set(GAPS)
    else:
        exposure = sum(abs(v) for k, v in gaps.items() if k not in excluded and abs(v) > limit[k])
        if primary.classification == "MERCHANT_SETTLEMENT_MISMATCH" and exposure == 0:
            exposure = abs(N)
    if kind == "QTX":
        exposure = 0.0 if counted_in else exposure
    tolerated = sum(abs(v) for k, v in gaps.items() if 0 < abs(v) <= limit[k]) if resolution == "RECONCILED" else 0.0

    level = _evidence_level(kind, resolution, c, ostate, pct_known if kind in ("PAYMENT", "REFUND") else True)
    row = {
        "flow_id": fid,
        "flow_type": {"PAYMENT": "PAYMENT", "REFUND": "REFUND", "QTX": "QUARANTINED_PAYMENT", "ORDER": "ORDER_ONLY",
                      "ORPHAN": "SETTLEMENT_ONLY"}[kind],
        "order_id": order_id, "txn_id": c.get("txn_id"), "merchant_id": c.get("merchant_id"),
        "settlement_ids": ",".join(sorted(b["settlement_id"] for b in bank)) or None,
        "bank_reference": bank[0]["gateway_ref"] if bank else None,
        "match_method": c.get("match_method") if n else None, "recon_status": c.get("recon_status"),
        "event_date": c.get("event_date"), "settlement_date": c["s_last"], "settlement_lag_days": lag,
        "n_settlements": n, "settlement_pattern": pattern,
        "order_state": ostate, "order_amount": order_amount, "expected_gateway": expected_gateway,
        "gateway_gross": G, "fee_pct": pct if kind in ("PAYMENT", "REFUND") else None, "expected_fee": expected_fee,
        "expected_net": expected_net if kind in ("PAYMENT", "REFUND") else None,
        "bank_gross": B if n else None, "bank_fee": f if n else None, "bank_net": N if n else None,
        "merchant_amount": None, "merchant_stage": "NOT_AVAILABLE",
        "gap_gateway": _r2(gap_gateway), "gap_bank_amount": _r2(gap_bank_amount), "gap_fee": _r2(gap_fee),
        "gap_bank_net": _r2(gap_bank_net), "difference_amount": _r2(N - expected_net),
        "classification": primary.classification, "first_breakpoint": primary.breakpoint,
        "root_cause": primary.root_cause, "resolution": resolution, "evidence_level": level,
        "findings": ";".join(f"{fd.classification}@{fd.breakpoint}" for fd in sorted(findings, key=lambda z: z.stage)),
        "exposure_amount": _r2(exposure), "tolerated_amount": _r2(tolerated), "counted_in": counted_in,
    }
    for k in ("order_amount", "expected_gateway", "gateway_gross", "expected_fee", "expected_net", "bank_gross", "bank_fee",
              "bank_net"):
        row[k] = _r2(row[k])
    row["evidence_summary"] = _narrative(row)
    if resolution == "RECONCILED":
        return row, []                       # a reconciled chain keeps its ids in the row; evidence is kept for findings
    for b in bank:
        ev.append((fid, "SETTLEMENT", b["settlement_id"],
                   f"ref {b['gateway_ref']} date {b['settlement_date']} gross {b['gross_amount']:.2f} "
                   f"fee {b['fee_amount']:.2f} net {b['net_amount']:.2f} merchant {b['merchant_id']}", b["net_amount"]))
    return row, ev


def _evidence_level(kind, resolution, c, ostate, pct_known) -> str:
    """HIGH: every leg present, trusted and linked by an exact reference. MEDIUM: fuzzy link, a leg unverifiable, or an
    absence established after every check. LOW: a hypothesis, a suspected duplicate, or a quarantined record."""
    if resolution == "BLOCKED" or kind == "QTX":
        return "LOW"
    if kind in ("ORDER", "ORPHAN"):
        return "MEDIUM"
    if not pct_known or ostate == "QUARANTINED" or c.get("match_method") == "fuzzy" or c["n"] == 0:
        return "MEDIUM"
    return "HIGH"


def _narrative(r: dict) -> str:
    def m(v):
        return "n/a" if v is None else f"{v:.2f}"

    parts = [f"{r['classification']} at {r['first_breakpoint']} ({r['resolution']})",
             f"order {m(r['order_amount'])} -> gateway {m(r['gateway_gross'])} -> expected net {m(r['expected_net'])}"
             f" -> bank {m(r['bank_net'])} -> merchant n/a",
             f"cause {r['root_cause']}"]
    return "; ".join(parts)


# =====================================================================================================================
# 6. read side
# =====================================================================================================================
def summary(con) -> dict:
    """Waterfall, breakdowns and the headline numbers, all from gold.fact_money_flow (each chain counted once)."""
    wf = con.execute("""
        SELECT coalesce(sum(order_amount), 0) AS customer_expected,
               coalesce(sum(gateway_gross) FILTER (WHERE flow_type = 'PAYMENT'), 0) AS gateway_gross,
               coalesce(sum(gateway_gross) FILTER (WHERE flow_type = 'REFUND'), 0) AS refunds,
               coalesce(sum(expected_fee), 0) AS expected_fees, coalesce(sum(expected_net), 0) AS expected_settlement,
               coalesce(sum(bank_net), 0) AS bank_actual,
               coalesce(sum(exposure_amount) FILTER (WHERE resolution = 'OPEN'), 0) AS unexplained,
               coalesce(sum(exposure_amount) FILTER (WHERE resolution = 'BLOCKED'), 0) AS blocked,
               coalesce(sum(exposure_amount) FILTER (WHERE resolution = 'EXPLAINED'), 0) AS explained,
               coalesce(sum(exposure_amount), 0) AS at_risk,
               coalesce(sum(expected_net) FILTER (WHERE resolution IN ('RECONCILED', 'TIMING')
                                                  AND flow_type IN ('PAYMENT', 'REFUND')), 0) AS reconciled,
               coalesce(sum(expected_net) FILTER (WHERE resolution = 'IN_FLIGHT'), 0) AS in_flight,
               count(*) AS flows,
               count(*) FILTER (WHERE resolution IN ('OPEN', 'BLOCKED', 'EXPLAINED')) AS flows_with_findings,
               coalesce(sum(gateway_gross) FILTER (WHERE flow_type = 'PAYMENT' AND order_state = 'QUARANTINED'), 0) AS unverifiable_order_side
        FROM gold.fact_money_flow""").df().iloc[0].to_dict()
    steps = con.execute("""
        SELECT resolution, coalesce(sum(gap_gateway), 0) AS gateway_vs_order, coalesce(sum(gap_bank_amount), 0) AS bank_amount,
               coalesce(sum(gap_fee), 0) AS fee, coalesce(sum(gap_bank_net), 0) AS bank_net_arithmetic, count(*) AS flows
        FROM gold.fact_money_flow GROUP BY 1""").df()
    by_stage = con.execute("""
        SELECT first_breakpoint AS stage, resolution, count(*) AS flows, sum(exposure_amount) AS exposure
        FROM gold.fact_money_flow WHERE exposure_amount > 0 GROUP BY 1, 2 ORDER BY 1, 2""").df()
    by_cause = con.execute("""
        SELECT classification, first_breakpoint AS stage, resolution, count(*) AS flows, sum(exposure_amount) AS exposure
        FROM gold.fact_money_flow WHERE classification NOT IN ('MATCHED') GROUP BY 1, 2, 3 ORDER BY exposure DESC NULLS LAST""").df()
    earliest = con.execute("""
        SELECT first_breakpoint FROM gold.fact_money_flow
        WHERE resolution = 'OPEN' AND exposure_amount > 0 AND first_breakpoint IN ('ORDER', 'GATEWAY', 'PROCESSING',
              'BANK_SETTLEMENT', 'MERCHANT_SETTLEMENT')
        ORDER BY CASE first_breakpoint WHEN 'ORDER' THEN 1 WHEN 'GATEWAY' THEN 2 WHEN 'PROCESSING' THEN 3
                                       WHEN 'BANK_SETTLEMENT' THEN 4 ELSE 5 END LIMIT 1""").fetchone()
    return {"waterfall": {k: (int(v) if k.startswith("flows") else round(float(v), 2)) for k, v in wf.items()},
            "steps_by_resolution": steps, "by_stage": by_stage, "by_root_cause": by_cause,
            "earliest_open_breakpoint": earliest[0] if earliest else None}


def headline(con) -> str:
    """The one-sentence answer: where did the money go missing?"""
    s = summary(con)
    w = s["waterfall"]

    def inr(v):
        return f"INR {v:,.0f}"

    top = con.execute("""
        SELECT first_breakpoint, count(*), sum(exposure_amount) FROM gold.fact_money_flow WHERE resolution = 'OPEN'
        AND exposure_amount > 0 GROUP BY 1 ORDER BY 3 DESC LIMIT 1""").fetchone()
    ev = con.execute("""SELECT flow_id FROM gold.fact_money_flow WHERE resolution = 'OPEN' AND exposure_amount > 0
                        AND first_breakpoint = ? ORDER BY exposure_amount DESC, flow_id LIMIT 3""",
                     [s["earliest_open_breakpoint"]]).fetchall() if s["earliest_open_breakpoint"] else []
    text = (f"{inr(w['customer_expected'])} was expected from customers (orders). {inr(w['gateway_gross'])} reached the gateway "
            f"({inr(-w['refunds'])} refunded). {inr(w['expected_settlement'])} should have settled after fees "
            f"({inr(w['expected_fees'])}). {inr(w['bank_actual'])} reached the bank. The merchant-side amount is not in the "
            f"source data, so the chain stops at the bank. ")
    text += (f"{inr(w['unexplained'])} is currently unexplained (a confirmed gap with no documented cause), a further "
             f"{inr(w['blocked'])} cannot be judged because evidence is blocked or ambiguous, and {inr(w['explained'])} is "
             f"explained (for example duplicate charges). ")
    if s["earliest_open_breakpoint"]:
        text += (f"The earliest confirmed breakpoint is {s['earliest_open_breakpoint']}, supported by "
                 f"{', '.join(e[0] for e in ev)}")
        if top:
            text += f"; the largest is {top[0]} ({top[1]:,} chains, {inr(top[2])})"
        text += "."
    else:
        text += "No confirmed breakpoint."
    return text


def investigate(con, flow_id: str) -> dict | None:
    """Everything an analyst needs for one chain: the stage table with the exact difference between stages, the conclusion,
    and the underlying records."""
    cur = con.execute("SELECT * FROM gold.fact_money_flow WHERE flow_id = ?", [flow_id])
    cols = [c[0] for c in cur.description]
    rec = cur.fetchone()
    if rec is None:
        return None
    r = dict(zip(cols, rec, strict=True))
    v = lambda k: r[k]  # noqa: E731
    stages = [
        {"stage": "ORDER", "label": "Customer / order", "amount": v("order_amount"), "difference": None,
         "note": f"order {r['order_id']} ({r['order_state']})" if r["order_id"] else "no order on this chain"},
        {"stage": "GATEWAY", "label": "Gateway gross", "amount": v("gateway_gross"), "difference": v("gap_gateway"),
         "note": f"vs expected {v('expected_gateway')}; txn {r['txn_id']}" if r["txn_id"] else "no gateway record"},
        {"stage": "PROCESSING", "label": "Fee (schedule)", "amount": v("expected_fee"), "difference": None,
         "note": f"{v('fee_pct')}% of gateway gross (merchant master on the capture date)" if v("fee_pct") is not None
         else "no fee expectation for this chain"},
        {"stage": "EXPECTED", "label": "Expected settlement", "amount": v("expected_net"), "difference": None,
         "note": "gateway gross - scheduled fee"},
        {"stage": "BANK_SETTLEMENT", "label": "Bank settlement", "amount": v("bank_net"),
         "difference": v("difference_amount"),
         "note": (f"{r['n_settlements']} record(s) {r['settlement_ids']}; {r['settlement_pattern']}; gross {v('bank_gross')}, "
                  f"fee {v('bank_fee')}" if r["n_settlements"] else "no bank record linked")},
        {"stage": "MERCHANT_SETTLEMENT", "label": "Merchant received", "amount": None, "difference": None,
         "note": "NOT AVAILABLE: the source data has no merchant-received amount; evidence stops at the bank"},
    ]
    bridge = [("Gateway vs order", r["gap_gateway"]), ("Bank gross vs captured (net of scheduled fee)", r["gap_bank_amount"]),
              ("Fee charged vs schedule", r["gap_fee"]), ("Bank net vs bank gross - fee", r["gap_bank_net"])]
    ev = con.execute("SELECT evidence_type, record_ref, detail, amount FROM gold.money_flow_evidence WHERE flow_id = ? "
                     "ORDER BY evidence_type, record_ref", [flow_id]).df()
    return {"flow": r, "stages": stages, "bridge": bridge, "evidence": ev}


def check_invariants(con) -> dict:
    """Machine-checkable conservation and uniqueness properties of gold.fact_money_flow. All must hold."""
    q = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731
    checks = {
        "flow_ids_unique": q("SELECT count(*) - count(DISTINCT flow_id) FROM gold.fact_money_flow") == 0,
        "bank_gap_bridge_is_exact": q("SELECT count(*) FROM gold.fact_money_flow WHERE abs(difference_amount - "
                                      "(gap_bank_amount + gap_fee + gap_bank_net)) > 0.02") == 0,
        "gateway_gap_is_gateway_minus_expected": q(
            "SELECT count(*) FROM gold.fact_money_flow WHERE abs(gap_gateway - (coalesce(gateway_gross, 0) - "
            "coalesce(expected_gateway, 0))) > 0.011") == 0,
        "every_bank_record_in_exactly_one_chain": q(
            "SELECT count(*) FROM (SELECT settlement_id FROM (SELECT unnest(string_split(settlement_ids, ',')) AS settlement_id "
            "FROM gold.fact_money_flow WHERE settlement_ids IS NOT NULL) GROUP BY 1 HAVING count(*) > 1)") == 0,
        "bank_totals_agree_with_silver": abs(q("SELECT coalesce(sum(bank_net), 0) FROM gold.fact_money_flow") - q(
            "SELECT coalesce(sum(net_amount), 0) FROM silver.settlements")) < 0.01,
        "no_merchant_amount_invented": q("SELECT count(*) FROM gold.fact_money_flow WHERE merchant_amount IS NOT NULL") == 0,
        "reconciled_chains_have_no_exposure": q("SELECT count(*) FROM gold.fact_money_flow WHERE resolution IN "
                                                "('RECONCILED', 'IN_FLIGHT', 'TIMING') AND exposure_amount > 0") == 0,
        "counted_elsewhere_chains_carry_no_amount_twice": q(
            "SELECT count(*) FROM gold.fact_money_flow WHERE counted_in IS NOT NULL AND flow_type IN "
            "('ORDER_ONLY', 'SETTLEMENT_ONLY') AND exposure_amount > 0") == 0,
    }
    return {"ok": all(checks.values()), "checks": checks}
