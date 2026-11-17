from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import shlex

import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator


CONFIG_PATH = Path(__file__).with_name("staging_dag_config.yml")
MODEL_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_config():
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    if not isinstance(config, dict):
        raise ValueError(f"{CONFIG_PATH} must contain a YAML mapping")

    dag_config = config.get("dag")
    dbt_config = config.get("dbt")
    models = config.get("staging_models")
    if not isinstance(dag_config, dict) or not isinstance(dbt_config, dict):
        raise ValueError("Config must define dag and dbt mappings")
    if not isinstance(models, list) or not models:
        raise ValueError("staging_models must be a non-empty ordered list")

    required_dag_values = (
        "dag_id",
        "description",
        "start_date",
        "schedule",
        "catchup",
        "retries",
        "retry_delay_minutes",
        "max_active_runs",
    )
    for key in required_dag_values:
        if key not in dag_config:
            raise ValueError(f"Missing dag.{key} in {CONFIG_PATH}")

    for key in ("project_dir", "profiles_dir", "target", "executable", "indirect_selection"):
        if not isinstance(dbt_config.get(key), str) or not dbt_config[key].strip():
            raise ValueError(f"dbt.{key} must be a non-empty string")

    if any(
        not isinstance(model, str) or not MODEL_NAME_PATTERN.fullmatch(model)
        for model in models
    ):
        raise ValueError("Every staging model must be a valid dbt model name")
    if len(models) != len(set(models)):
        raise ValueError("staging_models must not contain duplicates")

    return dag_config, dbt_config, models


DAG_CONFIG, DBT_CONFIG, STAGING_MODELS = load_config()
START_DATE = datetime.fromisoformat(DAG_CONFIG["start_date"])
if START_DATE.tzinfo is None:
    START_DATE = START_DATE.replace(tzinfo=timezone.utc)


with DAG(
    dag_id=DAG_CONFIG["dag_id"],
    description=DAG_CONFIG["description"],
    start_date=START_DATE,
    schedule=DAG_CONFIG["schedule"],
    catchup=DAG_CONFIG["catchup"],
    max_active_runs=DAG_CONFIG["max_active_runs"],
    default_args={
        "owner": "airflow",
        "retries": DAG_CONFIG["retries"],
        "retry_delay": timedelta(minutes=DAG_CONFIG["retry_delay_minutes"]),
    },
    tags=["dbt", "glue", "staging"],
) as staging_dag:
    previous_task = None
    for model_name in STAGING_MODELS:
        command = [
            DBT_CONFIG["executable"],
            "build",
            "--project-dir",
            DBT_CONFIG["project_dir"],
            "--profiles-dir",
            DBT_CONFIG["profiles_dir"],
            "--target",
            DBT_CONFIG["target"],
            "--select",
            f"resource_type:model,{model_name}",
            "--indirect-selection",
            DBT_CONFIG["indirect_selection"],
        ]
        current_task = BashOperator(
            task_id=f"build_{model_name}",
            bash_command=shlex.join(command),
            cwd=DBT_CONFIG["project_dir"],
        )
        if previous_task is not None:
            previous_task >> current_task
        previous_task = current_task
