"""Carica reconciled/ e quarantine/ nelle tabelle BigQuery trade_reconciliation_dev.*, sostituendone il contenuto.

Le tabelle devono già esistere (bigquery/schema.sql). Progetto e credenziali arrivano dalla
chiave del service account indicata da GOOGLE_APPLICATION_CREDENTIALS.
"""
import glob

from google.cloud import bigquery

client = bigquery.Client()
config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                write_disposition="WRITE_TRUNCATE",
                                create_disposition="CREATE_NEVER")

for table in ("reconciled", "quarantine"):
    [path] = glob.glob(f"/opt/data/output/{table}/*.parquet")  # reconcile.py scrive un solo file
    with open(path, "rb") as f:
        job = client.load_table_from_file(f, f"{client.project}.trade_reconciliation_dev.{table}", job_config=config)
    job.result()
    print(f"{table}: {job.output_rows:,} righe caricate")
