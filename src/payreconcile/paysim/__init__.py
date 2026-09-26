"""PaySim transaction analytics: profiling, data quality, balance consistency, fraud analytics and baseline ML on
DuckDB + Parquet.

Independent of the PayReconcile order -> gateway -> bank pipeline: PaySim has no orders, gateway or settlements, so nothing
here pretends it does. The financial flow of this dataset is

    origin account -> transaction -> destination account -> balance changes

Layers: raw CSV (immutable) -> development sample (Parquet) -> Bronze (Parquet + lineage) -> Silver (typed, quality-checked,
balance-consistency classified) -> Gold (facts, summaries, model results). One DuckDB file and one Bronze file per dataset
("dev" = the 250k sample, "full" = the whole file), so the same code scales without redesign.
"""
