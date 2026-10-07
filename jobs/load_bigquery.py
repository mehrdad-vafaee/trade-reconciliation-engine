"""Load reconciled/ and quarantine/ into the BigQuery tables trade_reconciliation_dev.*, replacing their content.

The tables must already exist (bigquery/schema.sql). Project and credentials come from the
service account key pointed to by GOOGLE_APPLICATION_CREDENTIALS.
"""
import glob

from google.cloud import bigquery

client = bigquery.Client()
config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                write_disposition="WRITE_TRUNCATE",
                                create_disposition="CREATE_NEVER")

for table in ("reconciled", "quarantine"):
    [path] = glob.glob(f"/opt/data/output/{table}/*.parquet")  # reconcile.py writes a single file
    with open(path, "rb") as f:
        job = client.load_table_from_file(f, f"{client.project}.trade_reconciliation_dev.{table}", job_config=config)
    job.result()
    print(f"{table}: {job.output_rows:,} rows loaded")
