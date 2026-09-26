"""PaySim: model evaluation."""

from __future__ import annotations

import streamlit as st
from ps_common import current, note_if_sample, pq, queries


def render() -> None:
    name = current()
    st.title("PaySim model evaluation")
    st.caption("Baseline fraud models on a temporal split (train on earlier hours, validate on later, test on the latest). The threshold is chosen on "
               "validation and applied unchanged to test. Accuracy is deliberately not shown: it is meaningless at this prevalence.")
    note_if_sample(name)
    m = pq(name, queries.MODEL_METRICS)
    if m.empty:
        st.info("No model results yet: run `python -m payreconcile paysim ml --dataset " + name + "`.")
        return
    run = dict(pq(name, queries.MODEL_RUN).to_numpy())
    st.write(f"Split by `step`: train up to {run['train_end']}, validation up to {run['validation_end']}, test after. Training rows used: "
             f"{int(run['train_rows_used']):,} ({int(run['train_fraud']):,} fraud; non-fraud undersampled). Validation: {int(run['validation_rows']):,} rows / "
             f"{int(run['validation_fraud']):,} fraud. Test: {int(run['test_rows']):,} rows / {int(run['test_fraud']):,} fraud (all rows kept).")
    test = m[m["split"] == "test"].copy()
    st.subheader("Test-set metrics")
    st.dataframe(test[["model", "precision", "recall", "f1", "pr_auc", "roc_auc", "threshold", "prevalence"]], hide_index=True, width="stretch")
    st.caption("`*_pre_txn` models use only information known before the transaction completes (no closing balances). `operational_flag_isFlaggedFraud` is the "
               "source's own rule, for comparison. Near-perfect scores on this simulator mostly reflect its artefacts (see the feature audit and README), "
               "not expected real-world performance.")

    st.subheader("Confusion matrix")
    choice = st.selectbox("Model", list(test["model"]))
    r = test[test["model"] == choice].iloc[0]
    st.dataframe({"": ["actual fraud", "actual not fraud"], "predicted fraud": [int(r["tp"]), int(r["fp"])],
                  "predicted not fraud": [int(r["fn"]), int(r["tn"])]}, hide_index=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Precision-recall curve (test)")
        curve = pq(name, queries.MODEL_PR_CURVE)
        if not curve.empty:
            sel = curve[curve["model"] == choice]
            st.line_chart(sel.set_index("recall")[["precision"]])
            st.caption("Sampled to at most 200 points per model.")
    with right:
        st.subheader("Feature importance")
        imp = pq(name, queries.MODEL_IMPORTANCE)
        imp = imp[imp["model"] == choice]
        if imp.empty:
            st.write("No importance stored for this model.")
        else:
            st.bar_chart(imp.sort_values("importance", ascending=False).head(15).set_index("feature")["importance"])
            st.caption("Random forest: impurity importance. Logistic regression: absolute standardised coefficient.")

    st.subheader("Leakage audit")
    audit = pq(name, queries.MODEL_AUDIT)
    st.write("`isFraud` and everything derived from it are excluded by construction and by test. The table shows how well each single feature separates "
             "fraud on the training rows; a flag means one feature alone is almost decisive.")
    st.dataframe(audit, hide_index=True, width="stretch")
