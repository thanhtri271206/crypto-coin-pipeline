import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from utils.alerting import airflow_task_failure_callback, airflow_task_retry_callback

DBT_PROJECT_DIR = "/opt/airflow/dbt/crypto_dwh"
DBT_BIN = "/opt/dbt_venv/bin/dbt"

_DBT_BASE_FLAGS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"

_TASK_DEFAULTS = {
    "on_failure_callback": airflow_task_failure_callback,
    "on_retry_callback": airflow_task_retry_callback,
}

with DAG(
    dag_id="transform_dag",
    description="Run dbt transform pipeline: clean → deps → source freshness → build (includes tests)",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["transform", "phase-2"],
) as dag:
    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command=f"{DBT_BIN} clean {_DBT_BASE_FLAGS}",
        **_TASK_DEFAULTS,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BIN} deps {_DBT_BASE_FLAGS}",
        **_TASK_DEFAULTS,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_BIN} source freshness {_DBT_BASE_FLAGS}",
        # Freshness failures are WARNING level — do not block the build
        # but still send an alert so the team is notified.
        on_failure_callback=airflow_task_failure_callback,
        on_retry_callback=airflow_task_retry_callback,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT_BIN} build {_DBT_BASE_FLAGS}",
        **_TASK_DEFAULTS,
    )

    # `dbt build` = seed + snapshot + run + test (per node, theo dependency order).
    # Tests đã được chạy nội tại trong dbt_build — không cần `dbt test` riêng.
    dbt_clean >> dbt_deps >> dbt_source_freshness >> dbt_build
