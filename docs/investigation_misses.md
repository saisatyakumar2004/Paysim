# Investigation: reconciliation misses

Reproduce: `python scripts/analyze_misses.py` (reads the warehouse and the generator's ground truth; writes
`docs/results_miss_analysis.json`). Dataset: 30 days, seed 42, 92,370 orders. Historical numbers were re-verified from
`docs/results_evaluation.txt` before this pass.

## A. Duplicate-charge misses (17 of 694)

| Category | Count | Explanation |
|---|---|---|
| original payment quarantined, traceable by `order_id` | 14 | The first charge failed validation (bad status, amount, timestamp or merchant), so Silver holds only the second charge and there is nothing trusted to pair it with. |
| original payment quarantined **without** an `order_id` | 3 | Same cause, but the original also lacked its order id (rule `GW-CMP-02`), so the quarantine row cannot be tied to the order. |
| unexplained | 0 | |

Every miss has the same root cause: **a duplicate needs two trusted payments, and one of them was rejected.** When both charges
were trusted the detector found 677 of 677 (recall 100%, precision 100%). This is a limitation of the evidence available, not a
threshold problem, so lowering the 10-minute window or the amount rule would not have helped and was not tried.

### Change made: a second, explicitly weaker evidence tier
`is_suspected_duplicate` (basis `QUARANTINED_ORIGINAL`) is set on a trusted captured payment when the **same order** also has a
quarantined gateway payment attempt with a different `txn_id`, and that quarantined payload does not contradict the hypothesis:

* it is a payment (not a refund) and its raw status is not `failed` / `pending` (a failed attempt followed by a success is normal);
* if its amount parses and is positive, it equals this charge's amount;
* if its timestamp parses and is within a day of this charge, it is within the duplicate window;
* unparseable fields are treated as *unknown*, not as agreement;
* the quarantined key must not belong to a trusted txn (a webhook copy of a trusted txn is not a second charge).

It is a separate flag, a separate exposure (`suspected_duplicate_exposure`) and a separate count. It is **never merged into the
confirmed duplicate count**. The flagged charge may be the original or the duplicate; the exposure is one charge either way.

### Measured effect (same data, same seed)
| Metric | Before | After |
|---|---|---|
| Confirmed duplicates (trusted pairs) | P 100% / R 97.55% (677 / 694) | unchanged: P 100% / R 97.55% |
| Suspected tier (new) | n/a | P 100% (14 TP, 0 FP) |
| Combined (confirmed or suspected) | P 100% / R 97.55% | **P 100% / R 99.57% (691 / 694)** |

Old result 97.55% -> new combined 99.57%. Reason: the order-anchored suspected tier. Caveat: precision is measured against a
generator that only creates duplicates in the two ways above, so 100% precision here shows the rule is coherent, not that it is
safe on real feeds.

### Considered and NOT adopted: unanchored matching for the 3 remaining cases
An unanchored rule (same merchant + same amount + within 10 minutes of a quarantined row that has *no* order id) would recover
the last 3. On this data it flags exactly 3 charges with 0 false positives (`candidate_rule_unanchored` in the JSON). It was
**not adopted**: with no identifier anchor it would misfire on merchants with fixed prices (many legitimate payments share merchant,
amount and minute), and the synthetic amounts are continuous, so "0 false positives" here says nothing about that risk.
The 3 remaining misses are documented as an accepted limitation.

## B. Payment-without-order misses (12 of 609)

All 12 are payments that **failed validation and were quarantined** (`GW-VAL-04` x5, `GW-REF-01` x3, `GW-VAL-01` x2,
`GW-VAL-02` x1, `GW-VAL-03` x1). None reached trusted data, so there is no Gold item to carry a `NO_ORDER` flag.

* On payments that reached trusted data the detector found **597 of 597 (recall 100%, 0 false positives)**.
* The 12 are not lost: the payment sits in `quarantine.rows` with its rule ids, and the bank money for it appears as a
  `SOURCE_QUARANTINED` settlement item (tested: `test_quarantined_payment_with_no_order_is_not_lost_...`).

**Decision: intentional limitation, no new status.** A payment that failed validation is already an exception in the
quarantine queue; adding an order check on untrusted data would create a second exception on data that cannot be relied on.

| Metric | Historical | Now |
|---|---|---|
| Recall over all injected cases | 597 / 609 = 98.03% | 597 / 609 = 98.03% (unchanged, still reported) |
| Recall on payments that reached trusted data | not reported | 597 / 597 = 100% |
| Not detectable by design (payment quarantined) | not reported | 12 |

## Regression tests
`tests/test_reconcile_evidence.py`: suspected duplicate; failed / different-amount / refund quarantined rows must not flag;
unparseable fields; webhook copy; confirmed pair takes precedence and replay upgrades a suspicion (with an audit row);
trusted payment without order is flagged; quarantined payment without order is surfaced through quarantine and its settlement.
