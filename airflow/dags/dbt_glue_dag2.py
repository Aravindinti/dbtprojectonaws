from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.dags.dbt_dag_helpers2 import (
    build_dag,
    build_dbt_task_group,
    load_config,
    run_single_model,
)


CONFIG = load_config()
DEFAULT_ARGS = CONFIG["default_args"]
DBT = CONFIG["dbt"]
PIPELINES = CONFIG["pipelines"]


with build_dag(
    PIPELINES["staging"], "Build and test staging models.", CONFIG
) as staging_dag:
    staging_models = build_dbt_task_group(
        "staging_models", PIPELINES["staging"]["selector"], DBT, DEFAULT_ARGS
    )
    trigger_intermediate = TriggerDagRunOperator(
        task_id="trigger_intermediate",
        trigger_dag_id=PIPELINES["staging"]["next_dag"],
        wait_for_completion=True,
        deferrable=True,
    )
    staging_models >> trigger_intermediate


with build_dag(
    PIPELINES["intermediate"], "Build and test intermediate models.", CONFIG
) as intermediate_dag:
    intermediate_models = build_dbt_task_group(
        "intermediate_models",
        PIPELINES["intermediate"]["selector"],
        DBT,
        DEFAULT_ARGS,
    )
    trigger_marts = TriggerDagRunOperator(
        task_id="trigger_marts",
        trigger_dag_id=PIPELINES["intermediate"]["next_dag"],
        wait_for_completion=True,
        deferrable=True,
    )
    intermediate_models >> trigger_marts


with build_dag(
    PIPELINES["marts"], "Build and test dimensions, then facts.", CONFIG
) as marts_dag:
    dimensions = build_dbt_task_group(
        "dimensions", PIPELINES["marts"]["dimensions_selector"], DBT, DEFAULT_ARGS
    )
    facts = build_dbt_task_group(
        "facts", PIPELINES["marts"]["facts_selector"], DBT, DEFAULT_ARGS
    )
    dimensions >> facts


single_model_dag = DAG(
    dag_id=PIPELINES["single_model"]["dag_id"],
    description="Build and test one dbt model selected at trigger time.",
    start_date=CONFIG["start_date"],
    schedule=None,
    catchup=False,
    max_active_runs=CONFIG["max_active_runs"],
    dagrun_timeout=CONFIG["dagrun_timeout"],
    default_args=DEFAULT_ARGS,
    params={
        "model_name": Param(
            PIPELINES["single_model"]["default_model"],
            type="string",
            minLength=1,
            maxLength=128,
            pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
            description="Exact dbt model name, for example stg_customers or fact_sales.",
        )
    },
    tags=["dbt", "glue"],
)

with single_model_dag:
    run_model = PythonOperator(
        task_id="run_model",
        python_callable=run_single_model,
        op_kwargs={"dbt_config": DBT},
    )
