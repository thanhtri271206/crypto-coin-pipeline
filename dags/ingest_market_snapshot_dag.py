import asyncio
from datetime import timedelta
from typing import cast
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from ingestion import schemas
from ingestion.coingecko_client import CoinGeckoClient
from ingestion.config import COIN_IDS_STR
from ingestion.s3_writer import S3Writer

DEFAULT_TASK_KWARGS = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="ingest_market_snapshot",
    description="Ingest coins/markets (Top 10) + global market data — cùng hourly cadence",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,  # "@hourly",
    catchup=False,
    tags=["ingestion", "phase-1"],
):
    # ------------------------------------------------------------------
    # Nhánh 1: coins/markets — Top 10 coin snapshot
    # ------------------------------------------------------------------

    @task(**DEFAULT_TASK_KWARGS)
    def fetch_markets_raw() -> list | dict:
        """Task 1: Fetch raw market snapshot from CoinGecko API."""

        async def _fetch():
            async with CoinGeckoClient() as client:
                return await client.fetch_markets_raw(ids=COIN_IDS_STR)

        return asyncio.run(_fetch())

    @task(**DEFAULT_TASK_KWARGS)
    def upload_markets_to_s3(raw_data: list | dict) -> str:
        """Task 2: Upload raw JSON data to S3 using logical_date from context for Idempotency."""
        context = get_current_context()
        fetched_at = context.get("logical_date") or pendulum.now("UTC")

        writer = S3Writer()
        s3_key = writer.upload_raw_json(
            endpoint="coins/markets",
            raw_data=raw_data,
            fetched_at=fetched_at,
        )
        return s3_key

    @task(**DEFAULT_TASK_KWARGS)
    def validate_market_data(s3_key: str) -> dict:
        """Task 3: Validate stored market snapshot from S3 against Pydantic schema."""
        writer = S3Writer()
        raw_data = writer.read_raw_json(s3_key)
        validated = CoinGeckoClient.validate_markets(raw_data)
        return {"s3_key": s3_key, "coins_count": len(validated), "status": "valid"}

    # ------------------------------------------------------------------
    # Nhánh 2: global — market overview KPI (BTC dominance, total market cap...)
    # ------------------------------------------------------------------

    @task(**DEFAULT_TASK_KWARGS)
    def fetch_global_raw() -> dict:
        async def _fetch():
            async with CoinGeckoClient() as client:
                return await client.fetch_global_market_data_raw()

        return asyncio.run(_fetch())

    @task(**DEFAULT_TASK_KWARGS)
    def upload_global_to_s3(raw_data: dict) -> str:
        context = get_current_context()
        fetched_at = context.get("logical_date") or pendulum.now("UTC")

        writer = S3Writer()
        return writer.upload_raw_json(
            endpoint="global",
            raw_data=raw_data,
            fetched_at=fetched_at,
        )

    @task(**DEFAULT_TASK_KWARGS)
    def validate_global_data(s3_key: str) -> dict:
        """Task 6: Validate stored global market data from S3 against Pydantic schema."""
        writer = S3Writer()
        raw_data = cast(dict, writer.read_raw_json(s3_key))
        CoinGeckoClient.validate_global_market_data(raw_data)
        return {"s3_key": s3_key, "status": "valid"}

    # Flow 1: coins/markets (fetch -> upload -> validate)
    markets_data = fetch_markets_raw()
    markets_s3_key = upload_markets_to_s3(raw_data=markets_data)
    validate_market_data(s3_key=markets_s3_key)

    # Flow 2: global (fetch -> upload -> validate)
    global_data = fetch_global_raw()
    global_s3_key = upload_global_to_s3(raw_data=global_data)
    validate_global_data(s3_key=global_s3_key)

