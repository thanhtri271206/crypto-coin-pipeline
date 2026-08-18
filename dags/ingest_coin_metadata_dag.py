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

with DAG(
    dag_id="ingest_coin_metadata",
    description="Ingest coin metadata for all coins listed on CoinGecko — weekly cadence",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,  # "@weekly",
    catchup=False,
    # max_active_tasks=10 (thay vì 3) để tránh task starvation:
    # 10 coins × 3 tasks (fetch/upload/validate) = 30 tasks tranh 3 slot
    # → các tasks sau cùng chờ quá lâu trong queue → "requeue exceeded max → FAIL".
    # CoinGecko rate-limit vẫn được xử lý bởi retry mechanism (retries=3,
    # wait_exponential) khi API trả 429 — không cần giới hạn cứng ở đây.
    max_active_tasks=10,
    tags=["ingestion", "phase-1"],
):

    @task(**DEFAULT_TASK_KWARGS)
    def fetch_coin_metadata_raw(coin_id: str) -> dict:
        """Task 1: Fetch raw metadata for a single coin and wrap payload with coin_id."""
        async def _fetch():
            async with CoinGeckoClient() as client:
                raw_data = await client.fetch_coin_metadata_raw(id=coin_id)
                return {"coin_id": coin_id, "raw_data": raw_data}

        return asyncio.run(_fetch())

    @task(**DEFAULT_TASK_KWARGS)
    def upload_coin_metadata_to_s3(payload: dict) -> str:
        """Task 2: Upload raw metadata JSON to S3."""
        coin_id = payload["coin_id"]
        raw_data = payload["raw_data"]

        context = get_current_context()
        fetched_at = context.get("logical_date") or pendulum.now("UTC")

        writer = S3Writer()
        s3_key = writer.upload_raw_json(
            endpoint=f"coins_metadata/{coin_id}",
            raw_data=raw_data,
            fetched_at=fetched_at,
        )
        return s3_key

    @task(**DEFAULT_TASK_KWARGS)
    def validate_coin_metadata(s3_key: str) -> dict:
        """Task 3: Validate stored metadata from S3 against Pydantic schema.

        Đọc trực tiếp từ S3 thay vì nhận raw_data qua XCom vì 2 lý do:
        1. Airflow 3.x dual-consumer XCom bug: cả upload lẫn validate đều
           expand từ cùng metadata_payloads XCom source — gây race condition
           ở API server khi nhiều task đọc XCom đồng thời.
        2. Validate đọc data đã persist = đảm bảo data trên S3 không bị corrupt
           trong quá trình upload (validate-what-you-store, not what-you-fetched).

        Return value: flat dict nhỏ (KHÔNG dùng model_dump() toàn bộ) vì
        Airflow 3.x decompose dict return thành từng XCom entry riêng per-key.
        Các field Optional (genesis_date=None, market_cap_rank=None) gây HTTP 422
        khi push XCom vì API server yêu cầu body non-null.
        """
        writer = S3Writer()
        raw_data = cast(dict, writer.read_raw_json(s3_key))  # coins/{id} luôn trả dict
        validated = CoinGeckoClient.validate_coin_metadata(raw_data)
        # Chỉ trả về minimal summary — data đầy đủ đã có trên S3
        return {
            "coin_id": validated.id,
            "symbol": validated.symbol,
            "status": "valid",
        }

    # Dynamic Task Mapping
    # fetch → upload (payload XCom, coin_id + raw_data)
    # upload → validate (s3_key string XCom — nhỏ, không gây API server overload)
    metadata_payloads = fetch_coin_metadata_raw.expand(coin_id=COIN_IDS)
    s3_keys = upload_coin_metadata_to_s3.expand(payload=metadata_payloads)
    validate_coin_metadata.expand(s3_key=s3_keys)