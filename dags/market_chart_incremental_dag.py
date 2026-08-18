import asyncio
from datetime import timedelta
from typing import cast
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from ingestion import schemas
from ingestion.coingecko_client import CoinGeckoClient
from ingestion.config import COIN_IDS
from ingestion.s3_writer import S3Writer

DEFAULT_TASK_KWARGS = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

# Endpoint: https://api.coingecko.com/api/v3/coins/{id}/market_chart
with DAG(
    dag_id="market_chart_incremental",
    description="Incremental daily fetch for market chart data (days=7) for all coins.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,  # = "@daily",  # Chạy định kỳ mỗi ngày 1 lần
    catchup=False,
    max_active_tasks=10,  # 1 slot/coin, tránh task starvation khi 30 tasks tranh slot
    tags=["ingestion", "phase-1"],
):

    @task(**DEFAULT_TASK_KWARGS)
    def fetch_market_chart_raw(coin_id: str) -> dict:
        """Task 1: Fetch incremental market chart data (days=7) for a single coin."""

        async def _fetch():
            async with CoinGeckoClient() as client:
                raw_data = await client.fetch_coin_market_chart_raw(id=coin_id, days="7")
                return {"coin_id": coin_id, "raw_data": raw_data}

        return asyncio.run(_fetch())

    @task(**DEFAULT_TASK_KWARGS)
    def upload_market_chart_to_s3(payload: dict) -> str:
        """Task 2: Upload incremental market chart raw JSON to S3 under endpoint coins/{coin_id}/market_chart."""
        coin_id = payload["coin_id"]
        raw_data = payload["raw_data"]

        context = get_current_context()
        fetched_at = context.get("logical_date") or pendulum.now("UTC")

        writer = S3Writer()
        s3_key = writer.upload_raw_json(
            endpoint=f"coins/{coin_id}/market_chart",
            raw_data=raw_data,
            fetched_at=fetched_at,
        )
        return s3_key

    @task(**DEFAULT_TASK_KWARGS)
    def validate_market_chart(s3_key: str) -> dict:
        """Task 3: Validate raw market chart data from S3 against Pydantic schema."""
        writer = S3Writer()
        raw_data = cast(dict, writer.read_raw_json(s3_key))
        validated = CoinGeckoClient.validate_market_chart(raw_data)
        # Chỉ return status summary gọn nhẹ để bảo vệ Airflow XCom DB & tránh lỗi Structured XCom với None/arrays
        return {
            "s3_key": s3_key,
            "prices_count": len(validated.prices),
            "status": "valid",
        }

    # Dynamic Task Mapping: fetch -> upload -> validate
    chart_payloads = fetch_market_chart_raw.expand(coin_id=COIN_IDS)
    s3_keys = upload_market_chart_to_s3.expand(payload=chart_payloads)
    validate_market_chart.expand(s3_key=s3_keys)

