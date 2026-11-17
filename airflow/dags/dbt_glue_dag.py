from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from cosmos import DbtTaskGroup
from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import (
    ExecutionMode,
    InvocationMode,
    LoadMode,
    TestBehavior,
    TestIndirectSelection,
)


CONFIG_PATH = Path(__file__).with_name("dbt_dag_config.yml")


def _load_config():
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    if not isinstance(config, dict):
        raise ValueError(f"{CONFIG_PATH} must contain a YAML mapping")

    defaults = config.get("defaults")
    dbt = config.get("dbt")
    pipelines = config.get("pipelines")
    if not all(isinstance(value, dict) for value in (defaults, dbt, pipelines)):
        raise ValueError("Airflow dbt config must define defaults, dbt, and pipelines mappings")

    required_pipelines = {"staging", "intermediate", "marts"}
    if set(pipelines) != required_pipelines:
        raise ValueError(f"pipelines must define exactly: {', '.join(sorted(required_pipelines))}")

    for key in ("start_date", "schedule", "catchup", "retries", "retry_delay_minutes",
                "execution_timeout_minutes", "dagrun_timeout_minutes", "max_active_runs"):
        if key not in defaults:
            raise ValueError(f"Missing required Airflow default: {key}")
    try:
        datetime.fromisoformat(defaults["start_date"])
    except (TypeError, ValueError) as error:
        raise ValueError("defaults.start_date must be an ISO-8601 datetime") from error
    if defaults["schedule"] is not None and not isinstance(defaults["schedule"], str):
        raise ValueError("defaults.schedule must be a cron/timetable string or null")
    if not isinstance(defaults["catchup"], bool):
        raise ValueError("defaults.catchup must be a boolean")
    for key in ("retries", "retry_delay_minutes", "execution_timeout_minutes",
                "dagrun_timeout_minutes", "max_active_runs"):
        value = defaults[key]
        minimum = 0 if key == "retries" else 1
        if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
            raise ValueError(f"defaults.{key} must be an integer >= {minimum}")

    for key in (
        "project_dir",
        "profiles_dir",
        "profile_name",
        "target",
        "executable_path",
        "execution_mode",
        "invocation_mode",
        "load_method",
        "test_behavior",
        "test_indirect_selection",
    ):
        if not isinstance(dbt.get(key), str) or not dbt[key].strip():
            raise ValueError(f"dbt.{key} must be a non-empty string")
    if not isinstance(dbt.get("install_deps"), bool):
        raise ValueError("dbt.install_deps must be a boolean")
    for key, allowed in (
        ("execution_mode", {mode.value for mode in ExecutionMode}),
        ("invocation_mode", {mode.value for mode in InvocationMode}),
        ("load_method", {mode.value for mode in LoadMode}),
        ("test_behavior", {behavior.value for behavior in TestBehavior}),
        ("test_indirect_selection", {selection.value for selection in TestIndirectSelection}),
    ):
        if dbt[key] not in allowed:
            raise ValueError(f"dbt.{key} must be one of: {', '.join(sorted(allowed))}")

    dag_ids = []
    for name, pipeline in pipelines.items():
        if (not isinstance(pipeline, dict)
                or not isinstance(pipeline.get("dag_id"), str)
                or not pipeline["dag_id"].strip()):
            raise ValueError(f"pipelines.{name}.dag_id must be a non-empty string")
        dag_ids.append(pipeline["dag_id"])
    if len(dag_ids) != len(set(dag_ids)):
        raise ValueError("Every pipeline must have a unique dag_id")

    for name in ("staging", "intermediate"):
        pipeline = pipelines[name]
        if not isinstance(pipeline.get("selector"), str) or not pipeline["selector"].strip():
            raise ValueError(f"pipelines.{name}.selector must be a non-empty string")
        next_dag = pipeline.get("next_dag")
        if not isinstance(next_dag, str) or next_dag not in dag_ids:
            raise ValueError(f"pipelines.{name}.next_dag must reference a configured dag_id")

    marts = pipelines["marts"]
    for key in ("dimensions_selector", "facts_selector"):
        if not isinstance(marts.get(key), str) or not marts[key].strip():
            raise ValueError(f"pipelines.marts.{key} must be a non-empty string")

    return defaults, dbt, pipelines


DEFAULTS, DBT, PIPELINES = _load_config()
START_DATE = datetime.fromisoformat(DEFAULTS["start_date"])
if START_DATE.tzinfo is None:
    START_DATE = START_DATE.replace(tzinfo=timezone.utc)

DEFAULT_ARGS = {
    "owner": DEFAULTS.get("owner", "airflow"),
    "retries": DEFAULTS["retries"],
    "retry_delay": timedelta(minutes=DEFAULTS["retry_delay_minutes"]),
    "execution_timeout": timedelta(minutes=DEFAULTS["execution_timeout_minutes"]),
}


def _dbt_task_group(group_id, selector):
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(
            dbt_project_path=DBT["project_dir"],
            install_dbt_deps=DBT["install_deps"],
        ),
        profile_config=ProfileConfig(
            profile_name=DBT["profile_name"],
            target_name=DBT["target"],
            profiles_yml_filepath=str(Path(DBT["profiles_dir"]) / "profiles.yml"),
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode(DBT["execution_mode"]),
            invocation_mode=InvocationMode(DBT["invocation_mode"]),
            dbt_executable_path=DBT["executable_path"],
            test_indirect_selection=TestIndirectSelection(DBT["test_indirect_selection"]),
        ),
        render_config=RenderConfig(
            select=[selector],
            load_method=LoadMode(DBT["load_method"]),
            test_behavior=TestBehavior(DBT["test_behavior"]),
            dbt_executable_path=DBT["executable_path"],
            enable_mock_profile=False,
            # Keep parse-time and task-time dependency installation consistent in Cosmos 1.x.
            dbt_deps=DBT["install_deps"],
        ),
        operator_args={"install_deps": DBT["install_deps"]},
        default_args=DEFAULT_ARGS,
    )


def _trigger_next_dag(task_id, dag_id):
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        trigger_run_id="{{ dag.dag_id }}__{{ run_id }}",
        wait_for_completion=True,
        deferrable=True,
    )


def _new_dag(pipeline, description):
    return DAG(
        dag_id=pipeline["dag_id"],
        description=description,
        start_date=START_DATE,
        schedule=DEFAULTS["schedule"],
        catchup=DEFAULTS["catchup"],
        max_active_runs=DEFAULTS["max_active_runs"],
        dagrun_timeout=timedelta(minutes=DEFAULTS["dagrun_timeout_minutes"]),
        default_args=DEFAULT_ARGS,
        tags=["dbt", "glue"],
    )


staging_dag = _new_dag(
    PIPELINES["staging"],
    "Build and test dbt staging models, then trigger intermediate models.",
)
with staging_dag:
    staging_build = _dbt_task_group(
        "staging_models", PIPELINES["staging"]["selector"]
    )
    trigger_intermediate = _trigger_next_dag(
        "trigger_intermediate", PIPELINES["staging"]["next_dag"]
    )
    staging_build >> trigger_intermediate


intermediate_dag = _new_dag(
    PIPELINES["intermediate"],
    "Build and test dbt intermediate models, then trigger marts.",
)
with intermediate_dag:
    intermediate_build = _dbt_task_group(
        "intermediate_models", PIPELINES["intermediate"]["selector"]
    )
    trigger_marts = _trigger_next_dag(
        "trigger_marts", PIPELINES["intermediate"]["next_dag"]
    )
    intermediate_build >> trigger_marts


marts_dag = _new_dag(
    PIPELINES["marts"],
    "Build and test dbt dimensions followed by facts.",
)
with marts_dag:
    dimensions_build = _dbt_task_group(
        "dimension_models", PIPELINES["marts"]["dimensions_selector"]
    )
    facts_build = _dbt_task_group(
        "fact_models", PIPELINES["marts"]["facts_selector"]
    )
    dimensions_build >> facts_build
