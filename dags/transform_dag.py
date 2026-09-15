import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.decorators import task
from utils.alerting import airflow_task_failure_callback, airflow_task_retry_callback

DBT_PROJECT_DIR = "/opt/airflow/dbt/crypto_dwh"
DBT_BIN = "/opt/dbt_venv/bin/dbt"

_DBT_BASE_FLAGS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --use-colors"

_TASK_DEFAULTS = {
    "on_failure_callback": airflow_task_failure_callback,
    "on_retry_callback": airflow_task_retry_callback,
}

with DAG(
    dag_id="transform_dag",
    description=(
        "Run dbt transform pipeline: clean → deps → source freshness → build (includes tests). "
        "Hỗ trợ selective reprocessing qua dag_run.conf: "
        "{\"dbt_selector\": \"stg_market_chart+\"} để chỉ build subset model."
    ),
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

    @task(**_TASK_DEFAULTS)
    def resolve_dbt_build_command() -> str:
        """Xây dựng lệnh dbt build dựa trên dag_run.conf.

        Selective reprocessing: truyền conf {"dbt_selector": "<selector>"} khi trigger
        để chỉ rebuild một subset model thay vì toàn bộ pipeline.

        Ví dụ hợp lệ cho dbt_selector:
          - "stg_market_chart+"        → stg_market_chart và tất cả downstream
          - "tag:staging"              → tất cả models có tag staging
          - "stg_market_chart stg_coins_markets" → chạy 2 model cụ thể
          - (không truyền)             → build toàn bộ (behaviour mặc định)

        Docs: https://docs.getdbt.com/reference/node-selection/syntax
        """
        context = get_current_context()
        conf = (context["dag_run"].conf or {})
        dbt_selector = conf.get("dbt_selector", "").strip()

        if dbt_selector:
            cmd = f"{DBT_BIN} build {_DBT_BASE_FLAGS} --select {dbt_selector}"
            # Ghi log rõ ràng để audit trail trong Airflow task logs
            import logging
            logging.getLogger(__name__).info(
                f"[SELECTIVE BUILD] dbt selector='{dbt_selector}' — chỉ build subset model."
            )
        else:
            cmd = f"{DBT_BIN} build {_DBT_BASE_FLAGS}"
            import logging
            logging.getLogger(__name__).info(
                "[FULL BUILD] Không có dbt_selector trong conf — build toàn bộ pipeline."
            )

        return cmd

    dbt_build_cmd = resolve_dbt_build_command()

    dbt_build = BashOperator(
        task_id="dbt_build",
        # Đọc lệnh được resolve từ task trước — hỗ trợ cả full build và selective
        bash_command="{{ ti.xcom_pull(task_ids='resolve_dbt_build_command') }}",
        **_TASK_DEFAULTS,
    )

    # `dbt build` = seed + snapshot + run + test (per node, theo dependency order).
    # Tests đã được chạy nội tại trong dbt_build — không cần `dbt test` riêng.
    dbt_clean >> dbt_deps >> dbt_source_freshness >> dbt_build_cmd >> dbt_build
