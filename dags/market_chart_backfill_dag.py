import asyncio
from datetime import timedelta
import logging
import pendulum
from pydantic import ValidationError

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from ingestion import schemas
from ingestion.coingecko_client import CoinGeckoClient
from ingestion.config import COIN_IDS
from ingestion.s3_writer import S3Writer

logger = logging.getLogger(__name__)

DEFAULT_TASK_KWARGS = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

# Endpoint: https://api.coingecko.com/api/v3/coins/{id}/market_chart
with DAG(
    dag_id="market_chart_backfill",
    description="One-time manual backfill for historical market chart data (days=max) for all coins.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,  # Manual trigger execution only
    catchup=False,
    max_active_tasks=2,  # Giới hạn số task chạy song song để tránh dính Rate Limit (HTTP 429)
    tags=["ingestion", "phase-1", "backfill"],
):

    @task(**DEFAULT_TASK_KWARGS)
    def fetch_validate_and_upload_market_chart(coin_id: str) -> dict:
        """Fetch raw historical market chart, upload to S3, and validate schema."""
        async def _fetch():
            async with CoinGeckoClient() as client:
                return await client.fetch_coin_market_chart_raw(id=coin_id, days="max")

        raw_data = asyncio.run(_fetch())

        # 1. Upload lên S3
        context = get_current_context()
        fetched_at = context.get("logical_date") or pendulum.now("UTC")

        writer = S3Writer()
        s3_key = writer.upload_raw_json(
            endpoint=f"coins/{coin_id}/market_chart",
            raw_data=raw_data,
            fetched_at=fetched_at,
        )

        # 2. Validate với Pydantic schema
        try:
            CoinGeckoClient.validate_market_chart(raw_data)
        except ValidationError as e:
            logger.error(f"Validation failed for coin {coin_id}: {e}")
            raise

        # Trả về metadata gọn (s3_key, coin_id), KHÔNG return raw_data để bảo vệ Airflow XCom DB
        return {"coin_id": coin_id, "s3_key": s3_key, "status": "validated_and_uploaded"}

    # Dynamic Task Mapping (1 task duy nhất cho mỗi coin)
    fetch_validate_and_upload_market_chart.expand(coin_id=COIN_IDS)
