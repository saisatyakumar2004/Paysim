# Production cloud design

> **Status: DESIGNED, NOT IMPLEMENTED.** Everything in this repository runs locally on DuckDB. Nothing below has been deployed, and I
> have no production cloud experience from this project. This page shows how I would move the design, not something I have done.

| Local (implemented) | AWS | Azure | GCP |
|---|---|---|---|
| Parquet Bronze on disk | S3 (partitioned by `ingest_date`, lifecycle rules, object lock on Bronze) | ADLS Gen2 | GCS |
| Python extract (`ingest.py`) | ECS Fargate task / Lambda per source; API keys in Secrets Manager | Container Apps job / Functions; Key Vault | Cloud Run job; Secret Manager |
| Bank SFTP / CSV drop | Transfer Family -> S3 event | SFTP on Blob + Event Grid | GCS + Pub/Sub notification |
| DQ engine (`dq.py`) | Same code in the container, or Glue Data Quality for declarative rules | Same code; Purview for catalog | Same code; Dataplex data quality |
| Quarantine table | S3 prefix + Athena table | ADLS folder + Synapse serverless | GCS + BigQuery external table |
| DuckDB warehouse | Redshift Serverless or Snowflake | Synapse / Fabric warehouse or Snowflake | BigQuery |
| dbt-duckdb | dbt-redshift / dbt-snowflake (SQL is mostly portable; `levenshtein`, `QUALIFY`, `md5` need checking) | dbt-synapse / dbt-snowflake | dbt-bigquery |
| Prefect flow | MWAA (managed Airflow) or Step Functions | Data Factory / Managed Airflow | Cloud Composer |
| `ops.*` monitoring tables | CloudWatch metrics + a warehouse table | Azure Monitor | Cloud Monitoring |
| Alerts (log line today) | SNS -> Slack/PagerDuty | Action Groups | Cloud Monitoring alerting |
| Streamlit | App Runner / ECS behind Cognito | App Service | Cloud Run behind IAP |
| Anomaly model | Batch job writing to the warehouse; SageMaker only if it needs serving | Azure ML job | Vertex AI job |
| GitHub Actions CI | + OIDC role for deploys, `dbt build --target ci` in a scratch schema | same | same |

## What changes when data is bigger than one machine
* The **fuzzy matching join** is the expensive step (merchant x amount x date window x edit distance). It is blocked on merchant and date,
  so it partitions naturally; in a warehouse it should be its own incremental model over only unmatched, recent items.
* Reconciliation becomes **incremental**: only open items (PENDING / unmatched) plus newly arrived rows are re-evaluated, with a
  bounded look-back equal to the settlement SLA plus the late-arrival window.
* **Secrets, IAM, encryption at rest, PII handling:** none of this exists locally. Payments data carries account-level risk; a real
  deployment needs a data-classification review, tokenised identifiers and column-level access before anything else.
* **Exactly-once ingestion** would rely on idempotent keys (`txn_id`, `settlement_id`) plus a load ledger, as it does locally.
* **Orchestration:** the local Prefect flow maps to one Airflow DAG per day with sensors on the bank file; back-fills use the same
  `run_date` parameter.
