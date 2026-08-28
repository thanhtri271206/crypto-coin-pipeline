import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR="/opt/airflow/dbt/crypto_dwh"
DBT_BIN="/opt/dbt_venv/bin/dbt"

with DAG(
    dag_id="transform_dag",
    description="Run DBT transform for the warehouse",
    start_date=pendulum.datetime(2026,1,1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["transform", "phase-2"]
) as dag:
    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command=f"{DBT_BIN} clean --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BIN} deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT_BIN} build --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        )
    )
    dbt_clean >> dbt_deps >> dbt_build