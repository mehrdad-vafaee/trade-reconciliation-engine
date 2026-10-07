"""Pipeline completa: genera i feed, li riconcilia su Spark, carica l'esito su BigQuery."""
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Il driver gira qui (client mode), gli executor sui worker: tutti vedono /opt/data e /opt/jobs.
SUBMIT = "spark-submit --master spark://spark-master:7077 --conf spark.driver.host=airflow /opt/jobs/"

with DAG("trade_reconciliation", schedule=None, catchup=False):
    gen = BashOperator(task_id="generate_feeds", bash_command=SUBMIT + "generate_feeds.py")
    rec = BashOperator(task_id="reconcile", bash_command=SUBMIT + "reconcile.py")
    load = BashOperator(task_id="load_bigquery", bash_command="python /opt/jobs/load_bigquery.py")
    gen >> rec >> load
