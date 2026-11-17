import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from airflow import DAG
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
MODEL_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_config():
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    if not isinstance(config, dict):
        raise ValueError(f"{CONFIG_PATH} must contain a YAML mapping")

    defaults = config.get("defaults")
    dbt = config.get("dbt")
    pipelines = config.get("pipelines")
    if not all(isinstance(value, dict) for value in (defaults, dbt, pipelines)):
        raise ValueError("Airflow dbt config must define defaults, dbt, and pipelines mappings")

    for key in (
        "start_date",
        "schedule",
        "catchup",
        "retries",
        "retry_delay_minutes",
        "execution_timeout_minutes",
        "dagrun_timeout_minutes",
        "max_active_runs",
    ):
        if key not in defaults:
            raise ValueError(f"Missing required default: {key}")
    start_date = datetime.fromisoformat(defaults["start_date"])
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if defaults["schedule"] is not None and not isinstance(defaults["schedule"], str):
        raise ValueError("defaults.schedule must be a cron/timetable string or null")
    if not isinstance(defaults["catchup"], bool):
        raise ValueError("defaults.catchup must be a boolean")
    for key in (
        "retries",
        "retry_delay_minutes",
        "execution_timeout_minutes",
        "dagrun_timeout_minutes",
        "max_active_runs",
    ):
        minimum = 0 if key == "retries" else 1
        value = defaults[key]
        if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
            raise ValueError(f"defaults.{key} must be an integer >= {minimum}")

    required_dbt_settings = (
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
    )
    for key in required_dbt_settings:
        if not isinstance(dbt.get(key), str) or not dbt[key].strip():
            raise ValueError(f"dbt.{key} must be a non-empty string")
    if not isinstance(dbt.get("install_deps"), bool):
        raise ValueError("dbt.install_deps must be a boolean")
    for key, allowed in (
        ("execution_mode", {mode.value for mode in ExecutionMode}),
        ("invocation_mode", {mode.value for mode in InvocationMode}),
        ("load_method", {mode.value for mode in LoadMode}),
        ("test_behavior", {mode.value for mode in TestBehavior}),
        ("test_indirect_selection", {mode.value for mode in TestIndirectSelection}),
    ):
        if dbt[key] not in allowed:
            raise ValueError(f"dbt.{key} must be one of: {', '.join(sorted(allowed))}")

    required_pipelines = {"staging", "intermediate", "marts", "single_model"}
    if set(pipelines) != required_pipelines:
        raise ValueError(f"pipelines must define exactly: {', '.join(sorted(required_pipelines))}")
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
        if pipeline.get("next_dag") not in dag_ids:
            raise ValueError(f"pipelines.{name}.next_dag must reference a configured dag_id")

    for key in ("dimensions_selector", "facts_selector"):
        selector = pipelines["marts"].get(key)
        if not isinstance(selector, str) or not selector.strip():
            raise ValueError(f"pipelines.marts.{key} must be a non-empty string")

    single_model = pipelines["single_model"]
    default_model = single_model.get("default_model")
    if not isinstance(default_model, str) or not MODEL_NAME_PATTERN.fullmatch(default_model):
        raise ValueError("pipelines.single_model.default_model must be a valid dbt model name")

    default_args = {
        "owner": defaults.get("owner", "airflow"),
        "retries": defaults["retries"],
        "retry_delay": timedelta(minutes=defaults["retry_delay_minutes"]),
        "execution_timeout": timedelta(minutes=defaults["execution_timeout_minutes"]),
    }
    return {
        "start_date": start_date,
        "schedule": defaults["schedule"],
        "catchup": defaults["catchup"],
        "max_active_runs": defaults["max_active_runs"],
        "dagrun_timeout": timedelta(minutes=defaults["dagrun_timeout_minutes"]),
        "default_args": default_args,
        "dbt": dbt,
        "pipelines": pipelines,
    }


def build_dbt_task_group(group_id, selector, dbt, default_args):
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(
            dbt_project_path=dbt["project_dir"],
            install_dbt_deps=dbt["install_deps"],
        ),
        profile_config=ProfileConfig(
            profile_name=dbt["profile_name"],
            target_name=dbt["target"],
            profiles_yml_filepath=str(Path(dbt["profiles_dir"]) / "profiles.yml"),
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode(dbt["execution_mode"]),
            invocation_mode=InvocationMode(dbt["invocation_mode"]),
            dbt_executable_path=dbt["executable_path"],
            test_indirect_selection=TestIndirectSelection(dbt["test_indirect_selection"]),
        ),
        render_config=RenderConfig(
            select=[selector],
            load_method=LoadMode(dbt["load_method"]),
            test_behavior=TestBehavior(dbt["test_behavior"]),
            dbt_executable_path=dbt["executable_path"],
            enable_mock_profile=False,
            dbt_deps=dbt["install_deps"],
        ),
        operator_args={"install_deps": dbt["install_deps"]},
        default_args=default_args,
    )


def build_dag(pipeline, description, config):
    return DAG(
        dag_id=pipeline["dag_id"],
        description=description,
        start_date=config["start_date"],
        schedule=config["schedule"],
        catchup=config["catchup"],
        max_active_runs=config["max_active_runs"],
        dagrun_timeout=config["dagrun_timeout"],
        default_args=config["default_args"],
        tags=["dbt", "glue"],
    )


def run_single_model(dbt_config, **context):
    model_name = context["params"]["model_name"]
    if not isinstance(model_name, str) or not MODEL_NAME_PATTERN.fullmatch(model_name):
        raise ValueError("model_name must be a valid dbt model name")

    dbt_options = [
        "--project-dir",
        dbt_config["project_dir"],
        "--profiles-dir",
        dbt_config["profiles_dir"],
        "--target",
        dbt_config["target"],
    ]
    selected_models = subprocess.run(
        [
            dbt_config["executable_path"],
            "ls",
            "--quiet",
            *dbt_options,
            "--resource-type",
            "model",
            "--select",
            model_name,
            "--output",
            "name",
        ],
        cwd=dbt_config["project_dir"],
        env=os.environ.copy(),
        check=True,
        capture_output=True,
        text=True,
    )
    model_names = [
        line.strip()
        for line in selected_models.stdout.splitlines()
        if line.strip()
    ]
    if model_names != [model_name]:
        raise ValueError(
            f"Expected exactly one dbt model named {model_name!r}; found {model_names!r}"
        )

    build_command = [
        dbt_config["executable_path"],
        "build",
        *dbt_options,
        "--select",
        f"resource_type:model,{model_name}",
        "--indirect-selection",
        dbt_config["test_indirect_selection"],
    ]
    subprocess.run(
        build_command,
        cwd=dbt_config["project_dir"],
        env=os.environ.copy(),
        check=True,
    )
