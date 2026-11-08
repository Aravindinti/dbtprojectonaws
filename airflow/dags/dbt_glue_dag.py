from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt/dbtbyaravind"
CMD = f"cd {DBT_DIR} && dbt"

with DAG(
    "dbt_glue_daily",
    start_date=datetime(2026, 11, 1),
    schedule=None,          # trigger manually while learning
    catchup=False,
    tags=["dbt"],
) as dag:
    run = BashOperator(task_id="dbt_run", bash_command=f"{CMD} run")
    test = BashOperator(task_id="dbt_test", bash_command=f"{CMD} test")
    run >> test