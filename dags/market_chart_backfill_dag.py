import asyncio
import logging
import time
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from pydantic import ValidationError

from ingestion.coingecko_client import CoinGeckoClient
from ingestion.config import COIN_IDS
from ingestion.s3_writer import S3Writer, generate_s3_key

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cấu hình mặc định — có thể override toàn bộ qua dag_run.conf khi trigger:
#
#   {
#     "days": "90",           # số ngày lịch sử cần fetch (default: "365")
#     "skip_existing": true,  # bỏ qua coin đã có file trên S3 (default: true)
#     "throttle_delay": 8     # giây delay giữa các API call (default: 6)
#   }
#
# CoinGecko Free Tier: ~10-30 req/phút tuỳ thời điểm.
# throttle_delay=6s → max 10 req/phút → safe margin cho Free Tier.
# max_active_tasks=2: tối đa 2 coin chạy song song —
#   đủ parallelism nhưng không flood API khi mỗi request là heavy (days=365).
# ---------------------------------------------------------------------------

_BACKFILL_DEFAULTS = {
    "days": "365",
    "skip_existing": True,
    "throttle_delay": 6,  # seconds
}

DEFAULT_TASK_KWARGS = {
    "retries": 3,
    # Tăng từ 30s lên 60s: Free Tier rate limit window thường là 60s,
    # 30s quá ngắn → retry vẫn hit 429 trong cùng window.
    "retry_delay": timedelta(seconds=60),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

# Endpoint: https://api.coingecko.com/api/v3/coins/{id}/market_chart
with DAG(
    dag_id="market_chart_backfill",
    description=(
        "Configurable backfill for historical market chart data. "
        "Supports dag_run.conf: days (default 365), skip_existing (default true), "
        "throttle_delay (default 6s). Trigger manually với conf JSON để customise."
    ),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,  # Manual trigger only
    # Giảm từ 5 → 2: mỗi request market_chart?days=365 là heavy call —
    # 5 concurrent quá cao cho Free Tier ngay cả khi không bị 429 ngay lập tức.
    max_active_tasks=2,
    tags=["ingestion", "phase-1", "backfill"],
):

    @task(**DEFAULT_TASK_KWARGS)
    def fetch_validate_and_upload_market_chart(coin_id: str) -> dict:
        """Fetch raw historical market chart, upload to S3, và validate schema.

        Behaviour được điều khiển hoàn toàn qua dag_run.conf:
        - days:           số ngày cần backfill (default "365")
        - skip_existing:  True → skip nếu S3 key đã tồn tại (default True)
        - throttle_delay: giây delay sau khi fetch để tránh rate limit (default 6)
        """
        context = get_current_context()
        conf = context["dag_run"].conf or {}

        # Đọc config từ conf với fallback về defaults
        days: str = str(conf.get("days", _BACKFILL_DEFAULTS["days"]))
        skip_existing: bool = bool(conf.get("skip_existing", _BACKFILL_DEFAULTS["skip_existing"]))
        throttle_delay: float = float(conf.get("throttle_delay", _BACKFILL_DEFAULTS["throttle_delay"]))

        fetched_at = context.get("logical_date") or pendulum.now("UTC")
        writer = S3Writer()

        # 1. Skip-if-exists: kiểm tra S3 trước khi gọi API
        #    → tránh lãng phí quota khi chạy lại backfill DAG nhiều lần
        if skip_existing:
            s3_key_candidate = generate_s3_key(f"coins/{coin_id}/market_chart", fetched_at)
            if writer.object_exists(s3_key_candidate):
                logger.info(
                    f"[SKIP] {coin_id}: S3 object đã tồn tại tại {s3_key_candidate} "
                    f"(skip_existing=True). Bỏ qua re-fetch API."
                )
                return {
                    "coin_id": coin_id,
                    "s3_key": s3_key_candidate,
                    "status": "skipped_existing",
                }

        logger.info(f"[FETCH] {coin_id}: Fetching market_chart?days={days} từ CoinGecko...")

        # 2. Fetch từ CoinGecko API
        async def _fetch():
            async with CoinGeckoClient() as client:
                return await client.fetch_coin_market_chart_raw(id=coin_id, days=days)

        raw_data = asyncio.run(_fetch())

        # 3. Throttle delay — chạy SAU khi fetch xong để giảm throughput
        #    khi nhiều task chạy song song (max_active_tasks=2).
        if throttle_delay > 0:
            logger.info(f"[THROTTLE] {coin_id}: Sleeping {throttle_delay}s để tránh rate limit...")
            time.sleep(throttle_delay)

        # 4. Upload lên S3
        s3_key = writer.upload_raw_json(
            endpoint=f"coins/{coin_id}/market_chart",
            raw_data=raw_data,
            fetched_at=fetched_at,
        )

        # 5. Validate với Pydantic schema
        try:
            CoinGeckoClient.validate_market_chart(raw_data)
        except ValidationError as e:
            logger.error(f"Validation failed for coin {coin_id}: {e}")
            raise

        # Trả về metadata gọn (s3_key, coin_id), KHÔNG return raw_data để bảo vệ Airflow XCom DB
        return {"coin_id": coin_id, "s3_key": s3_key, "status": "fetched_and_validated"}

    # Dynamic Task Mapping (1 task duy nhất cho mỗi coin)
    fetch_validate_and_upload_market_chart.expand(coin_id=COIN_IDS)
