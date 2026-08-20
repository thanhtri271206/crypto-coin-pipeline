import asyncio
import logging
import os

import httpx
from dotenv import load_dotenv
from pydantic import RootModel, ValidationError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential
from ingestion import schemas

load_dotenv()
logger = logging.getLogger(__name__)

API_KEY = os.getenv("COINGECKO_API_KEY")
DEFAULT_HEADERS = {"x-cg-demo-api-key": API_KEY} if API_KEY else {}


# Nếu cần hỗ trợ đa currency, làm ở tầng transform/dbt — KHÔNG đổi default ở đây,
# tránh mismatch với các layer phía sau (vd GlobalMarketDataSchema.total_market_cap["usd"]).
DEFAULT_VS_CURRENCY = "usd"


def is_transient_error(exception: Exception) -> bool:
    """Chỉ retry lỗi tạm thời (network, 429, 5xx). Lỗi 4xx khác (vd 404 coin
    không tồn tại) hoặc ValidationError sẽ KHÔNG được retry"""
    if isinstance(exception, httpx.RequestError):
        return True
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code == 429 or exception.response.status_code >= 500
    return False


# Response của coins/markets là 1 mảng JSON thuần -> dùng RootModel để validate
# nguyên list, không phải object đơn lẻ.
CoinMarketListSchema = RootModel[list[schemas.CoinMarketSchema]]


class CoinGeckoClient:
    def __init__(
        self,
        base_url: str = "https://api.coingecko.com/api/v3",
        max_concurrency: int = 5,
    ):
        """
        max_concurrency: giới hạn số call đồng thời qua asyncio.Semaphore.
        Free/demo tier CoinGecko có rate limit khá thấp (thay đổi tuỳ thời
        điểm) — nếu gọi asyncio.gather() cho 10 coin cùng lúc (vd backfill
        market_chart) mà không giới hạn, rất dễ dính 429 hàng loạt. Đây chỉ
        là guard đơn giản (không phải rate limiter chuẩn theo request/phút),
        đủ dùng cho scope MVP.
        """
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=30.0, headers=DEFAULT_HEADERS)
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def __aenter__(self) -> "CoinGeckoClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def close(self) -> None:
        # httpx.AsyncClient dùng aclose() (async), KHÔNG có method close() —
        # gọi close() sẽ raise AttributeError.
        await self.client.aclose()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(is_transient_error),
        reraise=True,
    )
    async def _get_with_retry(self, endpoint: str, params: dict) -> dict | list:
        url = f"{self.base_url}/{endpoint}"

        async with self._semaphore:
            print(f"[API Request] GET {url} | params: {params}")
            logger.info(f"GET {url} with params: {params}")
            response = await self.client.get(url, params=params)
            logger.debug(
                f"Response {response.status_code} in "
                f"{response.elapsed.total_seconds():.2f}s | headers: {dict(response.headers)}"
            )
            response.raise_for_status()
            return response.json()

    # ---------------------------------------------------------------------------
    # RAW FETCH METHODS (I/O + Retry, KHÔNG phụ thuộc validation logic)
    # ---------------------------------------------------------------------------

    async def fetch_markets_raw(
        self,
        ids: str,
        vs_currency: str = DEFAULT_VS_CURRENCY,
        per_page: int = 100,
    ) -> list | dict:
        """ids: chuỗi coin id cách nhau bởi dấu phẩy, vd 'bitcoin,ethereum'."""
        endpoint = "coins/markets"
        params = {
            "ids": ids,
            "vs_currency": vs_currency,
            "per_page": per_page,
            "order": "market_cap_desc",
            "price_change_percentage": "1h,24h,7d",
        }
        return await self._get_with_retry(endpoint, params)

    async def fetch_coin_metadata_raw(self, id: str) -> dict:
        endpoint = f"coins/{id}"
        params = {
            "localization": "false",
            "tickers": "false",
            "market_data": "false",
            "community_data": "false",
            "developer_data": "false",
        }
        return await self._get_with_retry(endpoint, params)

    async def fetch_coin_market_chart_raw(
        self, id: str, vs_currency: str = DEFAULT_VS_CURRENCY, days: str = "max"
    ) -> dict:
        """days='max': CHỈ dùng cho full backfill 1 lần duy nhất.
        Cho incremental fetch định kỳ, truyền days nhỏ (vd '7') để tránh
        re-fetch toàn bộ lịch sử mỗi lần chạy."""
        endpoint = f"coins/{id}/market_chart"
        params = {"vs_currency": vs_currency, "days": days}
        return await self._get_with_retry(endpoint, params)

    async def fetch_global_market_data_raw(self) -> dict:
        # Endpoint /global KHÔNG nhận param vs_currency — API luôn trả sẵn
        # mọi currency trong response (total_market_cap.usd, .vnd, ...).
        endpoint = "global"
        return await self._get_with_retry(endpoint, params={})

    async def fetch_coin_ohlc_raw(
        self, id: str, vs_currency: str = DEFAULT_VS_CURRENCY, days: str = "30"
    ) -> list | dict:
        endpoint = f"coins/{id}/ohlc"
        params = {"vs_currency": vs_currency, "days": days}
        return await self._get_with_retry(endpoint, params)

    # ---------------------------------------------------------------------------
    # VALIDATION METHODS (Pure Functions, nhận Raw -> trả Pydantic Model)
    # ---------------------------------------------------------------------------

    @staticmethod
    def validate_markets(raw_data: list | dict) -> list[schemas.CoinMarketSchema]:
        try:
            validated_data = CoinMarketListSchema.model_validate(raw_data)
            return validated_data.root
        except ValidationError as e:
            logger.error(f"Validation error [coins/markets]: {e}")
            raise

    @staticmethod
    def validate_coin_metadata(raw_data: dict) -> schemas.CoinMetadataSchema:
        try:
            return schemas.CoinMetadataSchema.model_validate(raw_data)
        except ValidationError as e:
            logger.error(f"Validation error [coins/metadata]: {e}")
            raise

    @staticmethod
    def validate_market_chart(raw_data: dict) -> schemas.CoinMarketChartSchema:
        try:
            return schemas.CoinMarketChartSchema.model_validate(raw_data)
        except ValidationError as e:
            logger.error(f"Validation error [coins/market_chart]: {e}")
            raise

    @staticmethod
    def validate_global_market_data(raw_data: dict) -> schemas.GlobalMarketDataSchema:
        try:
            # QUAN TRỌNG: response có wrapper {"data": {...}} -> phải validate
            # qua GlobalDataSchema (wrapper) rồi lấy .data
            validated_wrapper = schemas.GlobalDataSchema.model_validate(raw_data)
            return validated_wrapper.data
        except ValidationError as e:
            logger.error(f"Validation error [global]: {e}")
            raise

    @staticmethod
    def validate_coin_ohlc(raw_data: list | dict) -> list[list[float]]:
        try:
            validated_data = schemas.CoinOHLCSchema.model_validate(raw_data)
            return validated_data.root
        except ValidationError as e:
            logger.error(f"Validation error [coins/ohlc]: {e}")
            raise
