# trade-reconciliation-engine
In post-trade financial market infrastructure, transaction feeds containing minor discrepancies are known as "trade breaks". This project investigates how to ingest bilateral financial transaction feeds, execute distributed reconciliation algorithms to detect and categorize trade breaks.

Sara Patano, Alberto Tonni, Mehrdad Vafaee — Data-Intensive Computing. The full description is in [report/report.pdf](report/report.pdf).

## How it works
An Airflow DAG runs three steps:

1. `jobs/generate_feeds.py` (Spark) builds two broker feeds from the source dataset: A is the clean reference, B has an error injected in about 75% of the rows.
2. `jobs/reconcile.py` (Spark) validates every field, joins A and B on the transaction id and labels each pair as `MATCHED` or as a break on the field that differs. Rows with invalid values, duplicated ids or no counterpart go to quarantine. At the end it checks its counts against those expected by the generator and fails if they differ.
3. `jobs/load_bigquery.py` loads the results into the BigQuery tables `reconciled` and `quarantine`.

The views in `bigquery/views.sql` aggregate the two tables for the Looker Studio dashboard.

## Repository structure
```
dags/trade_reconciliation.py   Airflow DAG
jobs/                          Spark jobs and BigQuery loader
bigquery/schema.sql            BigQuery dataset and tables
bigquery/views.sql             views used by the dashboard
airflow/Dockerfile             Airflow image with Java and PySpark
docker-compose.yml             Spark master, 2 workers and Airflow
docs/                          architecture diagrams
report/                        project report (LaTeX source and PDF)
```

## Requirements
Docker, and a Google Cloud project with BigQuery and a service account key.

## Setup
1. Download `transactions_data.csv` from the Kaggle dataset [Financial Transactions Dataset: Analytics](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets) and save it as `data/source/transactions_data-selected-columns.csv`. Only the columns `id, date, client_id, card_id, amount, merchant_id` are used; the others are ignored.
2. Save the service account key as `secrets/gcp-key.json`.
3. In the BigQuery console, run `bigquery/schema.sql` to create the tables, then `bigquery/views.sql` to create the views.

## Run
1. Start the containers:
   ```bash
   docker compose up --build
   ```
2. Open Airflow at http://localhost:8081 and trigger the `trade_reconciliation` DAG. Spark progress is visible at http://localhost:8080.
3. Results appear in BigQuery under `trade_reconciliation_dev`, in the `reconciled` and `quarantine` tables. The whole run takes about 12 minutes on the full dataset.

Intermediate files are written to `data/raw/` (feeds and expected counts) and `data/output/` (Parquet results).
