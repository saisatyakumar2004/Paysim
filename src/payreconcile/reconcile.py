"""Incremental three-way reconciliation.

State lives in three tables (all in the `gold` schema):

* ``fact_reconciliation``   current status of every reconciliation item (txn / settlement / order)
* ``recon_links``           the txn <-> settlement matches (exact or fuzzy)
* ``reconciliation_audit``  append-only history of every status change

Each run re-evaluates only a *scope*, derived from ``ops.change_log`` (new or changed Silver rows and quarantine state
changes since the last successful reconciliation) plus items whose status depends on the passage of time:

    scope = items touched by a change
          + their match partners / order siblings / refunds that reference them        (closure)
          + PENDING items that have now aged past the settlement SLA
          + unmatched candidates that could fuzzy-match a scoped item                    (closure)
          + paid orders that just became old enough to be judged

Resolved matches are STICKY: a link is only re-examined when one of its two records changes. New candidates compete for
still-unmatched items only. (A from-scratch rebuild can, in rare ambiguous cases, choose differently; `verify` reports it.)

Everything is applied in one transaction, and the change-log watermark advances inside it, so a crash leaves the
previous state intact and a rerun is safe. `full=True` recomputes everything with the same code (recovery / benchmark).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime

from . import config

ITEM_COLUMNS = ("item_id, item_type, txn_id, settlement_id, order_id, merchant_id, event_date, expected_amount, "
                "settled_amount, diff_amount, status, match_method, is_duplicate_charge, duplicate_exposure, "
                "order_issue, is_suspected_duplicate, duplicate_of, duplicate_basis, suspected_duplicate_exposure, "
                "money_at_risk")
COMPARED = ["item_type", "txn_id", "settlement_id", "order_id", "merchant_id", "event_date", "expected_amount",
            "settled_amount", "diff_amount", "status", "match_method", "is_duplicate_charge", "duplicate_exposure",
            "order_issue", "is_suspected_duplicate", "duplicate_of", "duplicate_basis",
            "suspected_duplicate_exposure", "money_at_risk"]


@dataclass
class ReconStats:
    mode: str
    as_of: date
    changes_read: int = 0
    scope_txns: int = 0
    scope_settlements: int = 0
    scope_orders: int = 0
    items_total: int = 0
    items_evaluated: int = 0
    items_changed: int = 0
    seconds: float = 0.0


def _count(con, table: str) -> int:
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def _quarantined_gateway_keys() -> str:
    return "(SELECT natural_key FROM quarantine.rows WHERE dataset = 'gateway' AND natural_key IS NOT NULL)"


def reconcile(con, as_of: date, run_id: str = "manual", run_date: date | None = None, full: bool = False,
              dry_run: bool = False) -> ReconStats:
    """Reconcile incrementally (default) or from scratch (`full=True`). `dry_run` computes `new_items` and applies nothing."""
    t0 = time.perf_counter()
    started = datetime.now()
    cfg = config.settings()["reconciliation"]
    sla, tol, edit, dup_min = (cfg["settlement_sla_days"], cfg["amount_tolerance"], cfg["fuzzy_max_edit_distance"],
                               cfg["duplicate_charge_window_minutes"])
    run_date = run_date or as_of

    state = con.execute("SELECT last_seq, as_of_date FROM ops.recon_state WHERE id = 1").fetchone()
    last_seq, prev_as_of = (state[0], state[1]) if state else (0, None)
    if prev_as_of and as_of < prev_as_of:
        as_of = prev_as_of                 # a replay of an old day must not rewind the reconciliation clock
    full = full or state is None
    max_seq = con.execute("SELECT coalesce(max(seq), 0) FROM ops.change_log").fetchone()[0]
    AS_OF = f"DATE '{as_of.isoformat()}'"
    stats = ReconStats("full" if full else "incremental", as_of)

    # ---- 1. scope -------------------------------------------------------------------------------------------
    if full:
        con.execute("CREATE OR REPLACE TEMP TABLE scope_txn AS SELECT txn_id FROM silver.gateway_txns")
        con.execute("CREATE OR REPLACE TEMP TABLE scope_stl AS SELECT settlement_id FROM silver.settlements")
        con.execute("CREATE OR REPLACE TEMP TABLE scope_ord AS SELECT order_id FROM silver.orders")
        con.execute("CREATE OR REPLACE TEMP TABLE input_txn AS SELECT * FROM scope_txn")
        con.execute("CREATE OR REPLACE TEMP TABLE input_stl AS SELECT * FROM scope_stl")
        con.execute("CREATE OR REPLACE TEMP TABLE input_ord AS SELECT * FROM scope_ord")
    else:
        _incremental_scope(con, last_seq, max_seq, prev_as_of, as_of, sla, stats)

    # ---- 2. matching inside the scope -------------------------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE TEMP TABLE t_scope AS
        SELECT txn_id, order_id, merchant_id, txn_type, refund_of, created_at, cast(created_at AS DATE) AS txn_date,
               CASE WHEN txn_type = 'refund' THEN -amount ELSE amount END AS expected_gross, amount
        FROM silver.gateway_txns WHERE status = 'captured' AND txn_id IN (SELECT txn_id FROM scope_txn)""")
    con.execute("CREATE OR REPLACE TEMP TABLE s_scope AS SELECT * FROM silver.settlements "
                "WHERE settlement_id IN (SELECT settlement_id FROM scope_stl)")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE links_new AS
        SELECT t.txn_id, s.settlement_id, 'exact' AS match_method, 0 AS edit_distance
        FROM t_scope t JOIN s_scope s ON s.gateway_ref_norm = t.txn_id""")
    if not full:
        _fuzzy_closure(con, sla, tol, edit)
    _fuzzy_links(con, sla, tol, edit)
    _build_evidence(con, sla, tol, edit)

    # ---- 3. new state of every scoped item ---------------------------------------------------------------------
    _build_items(con, AS_OF, sla, tol, dup_min)
    stats.scope_txns, stats.scope_settlements, stats.scope_orders = (
        _count(con, "scope_txn"), _count(con, "scope_stl"), _count(con, "scope_ord"))
    stats.items_evaluated = _count(con, "new_items")
    if dry_run:
        stats.items_total = stats.items_evaluated
        stats.seconds = time.perf_counter() - t0
        return stats

    # ---- 4. diff against the stored state ------------------------------------------------------------------------
    _build_delta(con, AS_OF, full)
    stats.items_changed = _count(con, "delta")

    # ---- 5. apply atomically -------------------------------------------------------------------------------------------
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM gold.fact_reconciliation WHERE item_id IN (SELECT item_id FROM delta)")
        con.execute(f"""
            INSERT INTO gold.fact_reconciliation ({ITEM_COLUMNS}, status_since, updated_run_id)
            SELECT {', '.join('n.' + c.strip() for c in ITEM_COLUMNS.split(','))}, d.keep_since, ?
            FROM new_items n JOIN delta d ON d.item_id = n.item_id WHERE d.change_kind <> 'CLOSED'""", [run_id])
        con.execute("""
            INSERT INTO gold.reconciliation_audit (run_id, run_date, as_of_date, item_id, item_type, change_kind, reason,
                   old_status, new_status, old_settlement_id, new_settlement_id, old_money_at_risk, new_money_at_risk,
                   changed_at)
            SELECT ?, ?, ?, item_id, item_type, change_kind, reason, old_status, coalesce(new_status, 'CLOSED'),
                   old_settlement_id, new_settlement_id, old_money, new_money, now()
            FROM delta""", [run_id, run_date, as_of])
        if full:
            con.execute("DELETE FROM gold.recon_links")
            con.execute("DELETE FROM gold.recon_match_evidence")
        else:
            con.execute("DELETE FROM gold.recon_links WHERE txn_id IN (SELECT txn_id FROM scope_txn) "
                        "OR settlement_id IN (SELECT settlement_id FROM scope_stl)")
            con.execute("DELETE FROM gold.recon_match_evidence WHERE txn_id IN (SELECT txn_id FROM scope_txn) "
                        "OR settlement_id IN (SELECT settlement_id FROM scope_stl)")
        con.execute("INSERT INTO gold.recon_links SELECT * FROM links_new")
        con.execute("INSERT INTO gold.recon_match_evidence SELECT *, ?, now() FROM match_evidence", [run_id])
        con.execute("DELETE FROM ops.recon_state")
        con.execute("INSERT INTO ops.recon_state VALUES (1, ?, ?, now())", [max_seq, as_of])
        stats.items_total = _count(con, "gold.fact_reconciliation")
        stats.seconds = time.perf_counter() - t0
        con.execute("INSERT INTO ops.recon_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            run_id, run_date, stats.mode, as_of, stats.changes_read, stats.scope_txns, stats.scope_settlements,
            stats.scope_orders, stats.items_total, stats.items_evaluated, stats.items_changed, stats.seconds, started])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return stats


# =====================================================================================================================
def _incremental_scope(con, last_seq: int, max_seq: int, prev_as_of: date, as_of: date, sla: int, stats: ReconStats) -> None:
    con.execute(f"CREATE OR REPLACE TEMP TABLE chg AS SELECT dataset, key, aux FROM ops.change_log "
                f"WHERE seq > {last_seq} AND seq <= {max_seq}")
    stats.changes_read = _count(con, "chg")
    gw = "('gateway', 'quarantine_gateway')"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE scope_txn AS
        SELECT key AS txn_id FROM chg WHERE dataset = 'gateway'
        UNION SELECT txn_id FROM silver.gateway_txns WHERE order_id IN (
                 SELECT aux FROM chg WHERE dataset IN {gw} AND aux IS NOT NULL
                 UNION SELECT key FROM chg WHERE dataset IN ('orders', 'quarantine_orders'))
        UNION SELECT txn_id FROM silver.gateway_txns WHERE refund_of IN (SELECT key FROM chg WHERE dataset IN {gw})
        UNION SELECT aux FROM chg WHERE dataset = 'settlements' AND aux IS NOT NULL
        UNION SELECT txn_id FROM gold.recon_links WHERE settlement_id IN (SELECT key FROM chg WHERE dataset = 'settlements')
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE scope_stl AS
        SELECT key AS settlement_id FROM chg WHERE dataset = 'settlements'
        UNION SELECT settlement_id FROM silver.settlements
              WHERE gateway_ref_norm IN (SELECT key FROM chg WHERE dataset IN {gw})
        UNION SELECT settlement_id FROM gold.recon_links WHERE txn_id IN (SELECT key FROM chg WHERE dataset = 'gateway')
    """)
    # closure: siblings of the same order (duplicate-charge flags), then match partners in both directions
    for _ in range(5):
        before = _count(con, "scope_txn") + _count(con, "scope_stl")
        con.execute("""INSERT INTO scope_txn
            SELECT txn_id FROM silver.gateway_txns
            WHERE order_id IN (SELECT order_id FROM silver.gateway_txns WHERE txn_id IN (SELECT txn_id FROM scope_txn))
              AND txn_id NOT IN (SELECT txn_id FROM scope_txn)""")
        con.execute("""INSERT INTO scope_txn
            SELECT txn_id FROM gold.recon_links WHERE settlement_id IN (SELECT settlement_id FROM scope_stl)
              AND txn_id NOT IN (SELECT txn_id FROM scope_txn)""")
        con.execute("""INSERT INTO scope_txn
            SELECT DISTINCT gateway_ref_norm FROM silver.settlements
            WHERE settlement_id IN (SELECT settlement_id FROM scope_stl)
              AND gateway_ref_norm IN (SELECT txn_id FROM silver.gateway_txns)
              AND gateway_ref_norm NOT IN (SELECT txn_id FROM scope_txn)""")
        con.execute("""INSERT INTO scope_stl
            SELECT settlement_id FROM gold.recon_links WHERE txn_id IN (SELECT txn_id FROM scope_txn)
              AND settlement_id NOT IN (SELECT settlement_id FROM scope_stl)""")
        con.execute("""INSERT INTO scope_stl
            SELECT settlement_id FROM silver.settlements WHERE gateway_ref_norm IN (SELECT txn_id FROM scope_txn)
              AND settlement_id NOT IN (SELECT settlement_id FROM scope_stl)""")
        if _count(con, "scope_txn") + _count(con, "scope_stl") == before:
            break
    # an order item depends on (a) the order row, (b) whether a captured payment exists, (c) quarantined payments,
    # (d) time. Only (a)-(c) changes are logged, so only those orders are touched; txns dragged in as match
    # partners do not change whether their order has a payment.
    ord_sql = f"""
        SELECT key AS order_id FROM chg WHERE dataset IN ('orders', 'quarantine_orders')
        UNION SELECT aux FROM chg WHERE dataset IN {gw} AND aux IS NOT NULL"""
    if as_of > prev_as_of:      # orders that just became old enough to be judged "paid but never captured"
        ord_sql += (f" UNION SELECT order_id FROM silver.orders WHERE cast(created_at AS DATE) >= "
                    f"DATE '{prev_as_of.isoformat()}' - 2 AND cast(created_at AS DATE) < DATE '{as_of.isoformat()}' - 2")
    con.execute(f"CREATE OR REPLACE TEMP TABLE scope_ord AS {ord_sql}")
    # snapshot of what was touched by a real input change (used to tell "aged" from "input changed" in the audit)
    con.execute("CREATE OR REPLACE TEMP TABLE input_txn AS SELECT * FROM scope_txn")
    con.execute("CREATE OR REPLACE TEMP TABLE input_stl AS SELECT * FROM scope_stl")
    con.execute("CREATE OR REPLACE TEMP TABLE input_ord AS SELECT * FROM scope_ord")
    if as_of > prev_as_of:      # PENDING items whose SLA has now elapsed change status without any input change
        con.execute(f"""INSERT INTO scope_txn
            SELECT substr(item_id, 5) FROM gold.fact_reconciliation
            WHERE status = 'PENDING' AND event_date + {sla} < DATE '{as_of.isoformat()}'
              AND substr(item_id, 5) NOT IN (SELECT txn_id FROM scope_txn)""")


def _pair_predicate(sla: int, tol: float, edit: int) -> str:
    return (f"s.merchant_id = t.merchant_id AND abs(s.gross_amount - t.expected_gross) <= {tol} "
            f"AND s.settlement_date BETWEEN t.txn_date AND t.txn_date + {sla} "
            f"AND levenshtein(s.gateway_ref_norm, t.txn_id) <= {edit}")


def _fuzzy_closure(con, sla: int, tol: float, edit: int) -> None:
    """Pull every unmatched record that could compete for a scoped unmatched record into the scope, so the
    'unique from both sides' test sees ALL candidates. Already-linked records are never pulled in (sticky links)."""
    pred = _pair_predicate(sla, tol, edit)
    quarantined = _quarantined_gateway_keys()
    for _ in range(4):
        added = 0
        # settlements in scope that are still unmatched -> candidate txns anywhere
        new_t = con.execute(f"""
            SELECT DISTINCT t.txn_id FROM (
                SELECT txn_id, merchant_id, cast(created_at AS DATE) AS txn_date,
                       CASE WHEN txn_type = 'refund' THEN -amount ELSE amount END AS expected_gross
                FROM silver.gateway_txns WHERE status = 'captured'
                  AND txn_id NOT IN (SELECT txn_id FROM scope_txn)
                  AND txn_id NOT IN (SELECT txn_id FROM gold.recon_links)) t
            JOIN (SELECT * FROM s_scope WHERE settlement_id NOT IN (SELECT settlement_id FROM links_new)
                    AND gateway_ref_norm NOT IN {quarantined}) s ON {pred}""").fetchall()
        # txns in scope that are still unmatched -> candidate settlements anywhere
        new_s = con.execute(f"""
            SELECT DISTINCT s.settlement_id FROM (
                SELECT * FROM silver.settlements
                WHERE settlement_id NOT IN (SELECT settlement_id FROM scope_stl)
                  AND settlement_id NOT IN (SELECT settlement_id FROM gold.recon_links)
                  AND gateway_ref_norm NOT IN {quarantined}) s
            JOIN (SELECT * FROM t_scope WHERE txn_id NOT IN (SELECT txn_id FROM links_new)) t ON {pred}""").fetchall()
        if new_t:
            con.executemany("INSERT INTO scope_txn VALUES (?)", new_t)
            con.execute("""INSERT INTO t_scope
                SELECT txn_id, order_id, merchant_id, txn_type, refund_of, created_at, cast(created_at AS DATE),
                       CASE WHEN txn_type = 'refund' THEN -amount ELSE amount END, amount
                FROM silver.gateway_txns WHERE status = 'captured'
                  AND txn_id IN (SELECT txn_id FROM scope_txn) AND txn_id NOT IN (SELECT txn_id FROM t_scope)""")
            added += len(new_t)
        if new_s:
            con.executemany("INSERT INTO scope_stl VALUES (?)", new_s)
            con.execute("""INSERT INTO s_scope SELECT * FROM silver.settlements
                WHERE settlement_id IN (SELECT settlement_id FROM scope_stl)
                  AND settlement_id NOT IN (SELECT settlement_id FROM s_scope)""")
            added += len(new_s)
        if not added:
            break


def _fuzzy_links(con, sla: int, tol: float, edit: int) -> None:
    pred = _pair_predicate(sla, tol, edit)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fuzzy_cand AS
        SELECT t.txn_id, s.settlement_id, levenshtein(s.gateway_ref_norm, t.txn_id)::INTEGER AS edit_distance,
               abs(s.gross_amount - t.expected_gross) AS amount_diff, (s.settlement_date - t.txn_date)::INTEGER AS days_gap,
               s.merchant_id = t.merchant_id AS merchant_match,
               (count(*) OVER (PARTITION BY t.txn_id))::INTEGER AS n_t,
               (count(*) OVER (PARTITION BY s.settlement_id))::INTEGER AS n_s
        FROM (SELECT * FROM t_scope WHERE txn_id NOT IN (SELECT txn_id FROM links_new)) t
        JOIN (SELECT * FROM s_scope WHERE settlement_id NOT IN (SELECT settlement_id FROM links_new)
                AND gateway_ref_norm NOT IN {_quarantined_gateway_keys()}) s ON {pred}""")
    con.execute("""INSERT INTO links_new
                   SELECT txn_id, settlement_id, 'fuzzy', edit_distance FROM fuzzy_cand WHERE n_t = 1 AND n_s = 1""")


def _build_evidence(con, sla: int, tol: float, edit: int) -> None:
    """One row per exact link and per fuzzy CANDIDATE pair: features, score, threshold, decision and reason.

    Fuzzy matching is a constraint matcher (merchant, amount, window, edit distance) plus a uniqueness rule; `score`
    is only the reference similarity 1 - edit_distance / (max_edit_distance + 1). The decision is the uniqueness rule.
    """
    threshold = f"max_edit_distance={edit};amount_tolerance={tol};window_days={sla};must_be_unique_from_both_sides"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE match_evidence AS
        SELECT l.txn_id, l.settlement_id, 'exact' AS method, 'LINKED' AS decision, 0::INTEGER AS edit_distance,
               abs(s.gross_amount - t.expected_gross) AS amount_diff, (s.settlement_date - t.txn_date)::INTEGER AS days_gap,
               s.merchant_id = t.merchant_id AS merchant_match, 1::INTEGER AS txn_candidates,
               1::INTEGER AS settlement_candidates, 1.0 AS score, '{threshold}' AS threshold,
               'gateway txn_id equals the normalised bank reference' AS reason
        FROM links_new l JOIN t_scope t ON t.txn_id = l.txn_id JOIN s_scope s ON s.settlement_id = l.settlement_id
        WHERE l.match_method = 'exact'
        UNION ALL
        SELECT c.txn_id, c.settlement_id, 'fuzzy',
               CASE WHEN c.n_t = 1 AND c.n_s = 1 THEN 'LINKED' ELSE 'AMBIGUOUS_NOT_LINKED' END,
               c.edit_distance, c.amount_diff, c.days_gap, c.merchant_match, c.n_t, c.n_s,
               round(1 - c.edit_distance / ({edit} + 1.0), 4), '{threshold}',
               CASE WHEN c.n_t = 1 AND c.n_s = 1
                    THEN 'unique candidate from both sides: references differ by ' || c.edit_distance
                         || ' character(s); same merchant; amount within tolerance; settled inside the SLA window'
                    ELSE 'ambiguous: the txn has ' || c.n_t || ' candidate settlement(s) and the settlement has '
                         || c.n_s || ' candidate txn(s); no match was made' END
        FROM fuzzy_cand c""")


def _build_items(con, AS_OF: str, sla: int, tol: float, dup_min: int) -> None:
    quarantined = _quarantined_gateway_keys()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dup AS
        SELECT t.txn_id, t.amount AS duplicate_exposure, min(p.txn_id) AS original_txn_id
        FROM t_scope t
        JOIN (SELECT txn_id, order_id, merchant_id, amount, created_at FROM silver.gateway_txns
              WHERE status = 'captured' AND txn_type = 'payment'
                AND order_id IN (SELECT order_id FROM t_scope WHERE order_id IS NOT NULL)) p
          ON p.order_id = t.order_id AND p.merchant_id = t.merchant_id AND p.amount = t.amount
         AND p.txn_id <> t.txn_id AND p.created_at < t.created_at
         AND t.created_at <= p.created_at + INTERVAL '{dup_min} minutes'
        WHERE t.txn_type = 'payment'
        GROUP BY t.txn_id, t.amount""")
    # A second charge whose ORIGINAL failed validation cannot be paired with a trusted record. If the order also has a
    # quarantined payment attempt that is not contradicted by its own (parseable) fields, the charge is SUSPECTED, not
    # confirmed: separate flag and evidence tier, never merged into the confirmed duplicate count.
    win = dup_min * 60
    qamt = "pr_amt(json_extract_string(q.raw_json, '$.amount'))"
    qts = "pr_ts(json_extract_string(q.raw_json, '$.created_at'))"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE qdup AS
        SELECT t.txn_id, min(q.natural_key) AS duplicate_of
        FROM t_scope t
        JOIN quarantine.rows q ON q.dataset = 'gateway' AND q.order_id = t.order_id AND q.natural_key <> t.txn_id
        WHERE t.txn_type = 'payment' AND t.order_id IS NOT NULL
          AND q.natural_key NOT IN (SELECT txn_id FROM silver.gateway_txns)
          AND coalesce(lower(json_extract_string(q.raw_json, '$.txn_type')), 'payment') = 'payment'
          AND coalesce(lower(json_extract_string(q.raw_json, '$.status')), '') NOT IN ('failed', 'pending')
          AND ({qamt} IS NULL OR {qamt} <= 0 OR {qamt} = t.amount)
          AND ({qts} IS NULL OR abs(epoch({qts} - t.created_at)) > 86400 OR abs(epoch({qts} - t.created_at)) <= {win})
        GROUP BY t.txn_id""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE new_items AS
        WITH txn_items AS (
            SELECT 'TXN:' || t.txn_id AS item_id, 'txn' AS item_type, t.txn_id, l.settlement_id, t.order_id,
                   t.merchant_id, t.txn_date AS event_date, t.expected_gross AS expected_amount,
                   s.gross_amount AS settled_amount, s.gross_amount - t.expected_gross AS diff_amount,
                   CASE
                     WHEN t.txn_type = 'refund'
                          AND t.refund_of NOT IN (SELECT txn_id FROM silver.gateway_txns)
                          AND t.refund_of NOT IN {quarantined} THEN 'ORPHAN_REFUND'
                     WHEN l.settlement_id IS NULL THEN
                          CASE WHEN t.txn_date + {sla} < {AS_OF} THEN 'MISSING_SETTLEMENT' ELSE 'PENDING' END
                     WHEN abs(s.gross_amount - t.expected_gross) <= {tol} THEN 'MATCHED'
                     ELSE 'AMOUNT_MISMATCH' END AS status,
                   l.match_method,
                   d.txn_id IS NOT NULL AS is_duplicate_charge,
                   coalesce(d.duplicate_exposure, 0) AS duplicate_exposure,
                   CASE WHEN t.txn_type = 'payment' AND o.order_id IS NULL
                             AND t.order_id NOT IN (SELECT natural_key FROM quarantine.rows
                                                    WHERE dataset = 'orders' AND natural_key IS NOT NULL) THEN 'NO_ORDER'
                        WHEN t.txn_type = 'payment' AND o.order_id IS NOT NULL
                             AND abs(o.order_amount - t.amount) > {tol} THEN 'AMOUNT_DIFF' END AS order_issue,
                   d.txn_id IS NULL AND qd.txn_id IS NOT NULL AS is_suspected_duplicate,
                   coalesce(d.original_txn_id, qd.duplicate_of) AS duplicate_of,
                   CASE WHEN d.txn_id IS NOT NULL THEN 'TRUSTED_PAIR'
                        WHEN qd.txn_id IS NOT NULL THEN 'QUARANTINED_ORIGINAL' END AS duplicate_basis,
                   CASE WHEN d.txn_id IS NULL AND qd.txn_id IS NOT NULL THEN t.amount ELSE 0 END AS suspected_duplicate_exposure
            FROM t_scope t
            LEFT JOIN links_new l ON l.txn_id = t.txn_id
            LEFT JOIN silver.settlements s ON s.settlement_id = l.settlement_id
            LEFT JOIN dup d ON d.txn_id = t.txn_id
            LEFT JOIN qdup qd ON qd.txn_id = t.txn_id
            LEFT JOIN silver.orders o ON o.order_id = t.order_id),
        settlement_items AS (
            SELECT 'STL:' || s.settlement_id, 'settlement', NULL::VARCHAR, s.settlement_id, NULL::VARCHAR, s.merchant_id,
                   s.settlement_date, NULL::DOUBLE, s.gross_amount, NULL::DOUBLE,
                   CASE WHEN s.gateway_ref_norm IN {quarantined} THEN 'SOURCE_QUARANTINED'
                        WHEN s.gateway_ref_norm IN (SELECT txn_id FROM silver.gateway_txns) THEN 'SETTLED_NOT_CAPTURED'
                        ELSE 'ORPHAN_SETTLEMENT' END,
                   NULL::VARCHAR, false, 0::DOUBLE, NULL::VARCHAR, false, NULL::VARCHAR, NULL::VARCHAR, 0::DOUBLE
            FROM s_scope s WHERE s.settlement_id NOT IN (SELECT settlement_id FROM links_new)),
        order_items AS (
            SELECT 'ORD:' || o.order_id, 'order', NULL::VARCHAR, NULL::VARCHAR, o.order_id, o.merchant_id,
                   cast(o.created_at AS DATE), o.order_amount, NULL::DOUBLE, NULL::DOUBLE,
                   CASE WHEN o.order_id IN (SELECT order_id FROM quarantine.rows
                                            WHERE dataset = 'gateway' AND order_id IS NOT NULL)
                        THEN 'PAYMENT_QUARANTINED' ELSE 'ORDER_WITHOUT_PAYMENT' END,
                   NULL::VARCHAR, false, 0::DOUBLE, NULL::VARCHAR, false, NULL::VARCHAR, NULL::VARCHAR, 0::DOUBLE
            FROM silver.orders o
            WHERE o.order_id IN (SELECT order_id FROM scope_ord) AND o.order_status IN ('paid', 'refunded')
              AND cast(o.created_at AS DATE) + 2 < {AS_OF}
              AND o.order_id NOT IN (SELECT order_id FROM silver.gateway_txns
                                     WHERE status = 'captured' AND txn_type = 'payment' AND order_id IS NOT NULL
                                       AND order_id IN (SELECT order_id FROM scope_ord))),
        unioned AS (SELECT * FROM txn_items UNION ALL SELECT * FROM settlement_items UNION ALL SELECT * FROM order_items)
        SELECT unioned.*,
               CASE status WHEN 'AMOUNT_MISMATCH' THEN abs(diff_amount)
                           WHEN 'MISSING_SETTLEMENT' THEN abs(expected_amount)
                           WHEN 'ORPHAN_SETTLEMENT' THEN abs(settled_amount)
                           WHEN 'SETTLED_NOT_CAPTURED' THEN abs(settled_amount)
                           WHEN 'ORPHAN_REFUND' THEN abs(expected_amount)
                           WHEN 'ORDER_WITHOUT_PAYMENT' THEN expected_amount ELSE 0 END AS money_at_risk
        FROM unioned""")


def _build_delta(con, AS_OF: str, full: bool) -> None:
    con.execute("""CREATE OR REPLACE TEMP TABLE scope_ids AS
                   SELECT 'TXN:' || txn_id AS item_id FROM scope_txn
                   UNION ALL SELECT 'STL:' || settlement_id FROM scope_stl
                   UNION ALL SELECT 'ORD:' || order_id FROM scope_ord""")
    con.execute("""CREATE OR REPLACE TEMP TABLE input_ids AS
                   SELECT 'TXN:' || txn_id AS item_id FROM input_txn
                   UNION ALL SELECT 'STL:' || settlement_id FROM input_stl
                   UNION ALL SELECT 'ORD:' || order_id FROM input_ord""")
    where = "" if full else "WHERE item_id IN (SELECT item_id FROM scope_ids)"
    con.execute(f"CREATE OR REPLACE TEMP TABLE old_items AS SELECT * FROM gold.fact_reconciliation {where}")
    changed = " OR ".join(f"o.{c} IS DISTINCT FROM n.{c}" for c in COMPARED)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE delta AS
        SELECT coalesce(n.item_id, o.item_id) AS item_id, coalesce(n.item_type, o.item_type) AS item_type,
               o.status AS old_status, n.status AS new_status,
               o.settlement_id AS old_settlement_id, n.settlement_id AS new_settlement_id,
               o.money_at_risk AS old_money, n.money_at_risk AS new_money,
               CASE WHEN o.item_id IS NULL THEN 'NEW_ITEM' WHEN n.item_id IS NULL THEN 'CLOSED'
                    WHEN o.status IS DISTINCT FROM n.status THEN 'STATUS_CHANGE' ELSE 'ATTRIBUTE_CHANGE' END AS change_kind,
               CASE WHEN o.item_id IS NULL THEN 'NEW_ITEM'
                    WHEN n.item_id IS NULL THEN 'RESOLVED'
                    WHEN o.status = 'PENDING' AND n.status = 'MISSING_SETTLEMENT'
                         AND coalesce(n.item_id, o.item_id) NOT IN (SELECT item_id FROM input_ids) THEN 'AGED'
                    ELSE 'INPUT_CHANGE' END AS reason,
               CASE WHEN o.status = n.status THEN o.status_since ELSE {AS_OF} END AS keep_since
        FROM old_items o FULL OUTER JOIN new_items n ON o.item_id = n.item_id
        WHERE o.item_id IS NULL OR n.item_id IS NULL OR {changed}""")


def verify(con) -> dict:
    """Compare the stored incremental state with a from-scratch recomputation over the same Silver data."""
    state = con.execute("SELECT as_of_date FROM ops.recon_state WHERE id = 1").fetchone()
    if not state:
        return {"ok": False, "reason": "no reconciliation state"}
    reconcile(con, state[0], run_id="verify", full=True, dry_run=True)
    cols = ", ".join(COMPARED)
    only_state = con.execute(f"SELECT item_id FROM (SELECT item_id, {cols} FROM gold.fact_reconciliation EXCEPT "
                             f"SELECT item_id, {cols} FROM new_items)").fetchall()
    only_full = con.execute(f"SELECT item_id FROM (SELECT item_id, {cols} FROM new_items EXCEPT "
                            f"SELECT item_id, {cols} FROM gold.fact_reconciliation)").fetchall()
    return {"ok": not only_state and not only_full, "state_items": _count(con, "gold.fact_reconciliation"),
            "full_items": _count(con, "new_items"), "differ_in_state": len(only_state),
            "differ_in_full_rebuild": len(only_full),
            "sample": [r[0] for r in (only_state + only_full)[:5]]}


def compare_with_reference(con) -> dict:
    """Compare the stored state with the independent dbt full-rebuild model (`gold.recon_full_rebuild`)."""
    cols = ", ".join(COMPARED)
    only_state = con.execute(f"SELECT item_id FROM (SELECT item_id, {cols} FROM gold.fact_reconciliation EXCEPT "
                             f"SELECT item_id, {cols} FROM gold.recon_full_rebuild)").fetchall()
    only_ref = con.execute(f"SELECT item_id FROM (SELECT item_id, {cols} FROM gold.recon_full_rebuild EXCEPT "
                           f"SELECT item_id, {cols} FROM gold.fact_reconciliation)").fetchall()
    return {"ok": not only_state and not only_ref, "state_items": _count(con, "gold.fact_reconciliation"),
            "reference_items": _count(con, "gold.recon_full_rebuild"), "differ_in_state": len(only_state),
            "differ_in_reference": len(only_ref), "sample": [r[0] for r in (only_state + only_ref)[:5]]}
