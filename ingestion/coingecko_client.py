import asyncio
import logging
import os

import httpx
from dotenv import load_dotenv
from pydantic import RootModel, ValidationError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential
try:
    from . import schemas
except ImportError:
    import schemas

load_dotenv()
logger = logging.getLogger(__name__)

API_KEY = os.getenv("COINGECKO_API_KEY")
DEFAULT_HEADERS = {"x-cg-demo-api-key": API_KEY}

# QUAN TRỌNG: toàn bộ project (schema, dashboard, mart) giả định USD.
# Nếu cần hỗ trợ đa currency, làm ở tầng transform/dbt — KHÔNG đổi default ở đây,
# tránh mismatch âm thầm với các layer phía sau (vd GlobalMarketDataSchema.total_market_cap["usd"]).
DEFAULT_VS_CURRENCY = "usd"


def is_transient_error(exception: Exception) -> bool:
    """Chỉ retry lỗi tạm thời (network, 429, 5xx). Lỗi 4xx khác (vd 404 coin
    không tồn tại) hoặc ValidationError sẽ KHÔNG được retry — retry những lỗi
    này chỉ tốn quota vô ích vì kết quả sẽ luôn giống nhau."""
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
            logger.info(f"GET {url} with params: {params}")
            response = await self.client.get(url, params=params)
            logger.debug(
                f"Response {response.status_code} in "
                f"{response.elapsed.total_seconds():.2f}s | headers: {dict(response.headers)}"
            )
            response.raise_for_status()
            return response.json()

    async def ingest_markets(
        self,
        ids: str,
        vs_currency: str = DEFAULT_VS_CURRENCY,
        per_page: int = 100,
    ) -> list[schemas.CoinMarketSchema]:
        """ids: chuỗi coin id cách nhau bởi dấu phẩy, vd 'bitcoin,ethereum'."""
        endpoint = "coins/markets"
        params = {
            "ids": ids,
            "vs_currency": vs_currency,
            "per_page": per_page,
            "order": "market_cap_desc",
            "price_change_percentage": "1h,24h,7d",
        }
        try:
            raw_data = await self._get_with_retry(endpoint, params)
            validated_data = CoinMarketListSchema.model_validate(raw_data)
            return validated_data.root
        except ValidationError as e:
            logger.error(f"Validation error [coins/markets]: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error [coins/markets]: {e}")
            raise

    async def ingest_coin_metadata(self, id: str) -> schemas.CoinMetadataSchema:
        endpoint = f"coins/{id}"
        params = {
            "localization": "false",
            "tickers": "false",
            "market_data": "false",
            "community_data": "false",
            "developer_data": "false",
        }
        try:
            raw_data = await self._get_with_retry(endpoint, params)
            return schemas.CoinMetadataSchema.model_validate(raw_data)
        except ValidationError as e:
            logger.error(f"Validation error [coins/{id}]: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error [coins/{id}]: {e}")
            raise

    async def ingest_coin_market_chart(
        self, id: str, vs_currency: str = DEFAULT_VS_CURRENCY, days: str = "max"
    ) -> schemas.CoinMarketChartSchema:
        """days='max': CHỈ dùng cho full backfill 1 lần duy nhất (theo plan).
        Cho incremental fetch định kỳ, truyền days nhỏ (vd '7') để tránh
        re-fetch toàn bộ lịch sử mỗi lần chạy."""
        endpoint = f"coins/{id}/market_chart"
        params = {"vs_currency": vs_currency, "days": days}
        try:
            raw_data = await self._get_with_retry(endpoint, params)
            return schemas.CoinMarketChartSchema.model_validate(raw_data)
        except ValidationError as e:
            logger.error(f"Validation error [market_chart/{id}]: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error [market_chart/{id}]: {e}")
            raise

    async def ingest_global_market_data(self) -> schemas.GlobalMarketDataSchema:
        # Endpoint /global KHÔNG nhận param vs_currency — API luôn trả sẵn
        # mọi currency trong response (total_market_cap.usd, .vnd, ...).
        endpoint = "global"
        try:
            raw_data = await self._get_with_retry(endpoint, params={})
            # QUAN TRỌNG: response có wrapper {"data": {...}} -> phải validate
            # qua GlobalDataSchema (wrapper) rồi lấy .data, KHÔNG validate
            # thẳng vào GlobalMarketDataSchema — nếu làm vậy, do mọi field
            # đều Optional + extra="ignore", Pydantic sẽ KHÔNG báo lỗi mà
            # âm thầm trả về object toàn field None/rỗng.
            validated_wrapper = schemas.GlobalDataSchema.model_validate(raw_data)
            return validated_wrapper.data
        except ValidationError as e:
            logger.error(f"Validation error [global]: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error [global]: {e}")
            raise

    async def ingest_coin_ohlc(
        self, id: str, vs_currency: str = DEFAULT_VS_CURRENCY, days: str = "30"
    ) -> list[list[float]]:
        endpoint = f"coins/{id}/ohlc"
        params = {"vs_currency": vs_currency, "days": days}
        try:
            raw_data = await self._get_with_retry(endpoint, params)
            # Response là mảng phẳng -> CoinOHLCSchema là RootModel, validate
            # rồi trả .root (không phải .candles — field đó không tồn tại).
            validated_data = schemas.CoinOHLCSchema.model_validate(raw_data)
            return validated_data.root
        except ValidationError as e:
            logger.error(f"Validation error [ohlc/{id}]: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error [ohlc/{id}]: {e}")
            raise