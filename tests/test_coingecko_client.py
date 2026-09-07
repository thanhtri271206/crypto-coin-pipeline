import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from ingestion.coingecko_client import CoinGeckoClient, is_transient_error


class TestCoinGeckoClient(unittest.IsolatedAsyncioTestCase):
    def test_is_transient_error(self):
        # 429 Rate limit should be retryable
        mock_response_429 = MagicMock()
        mock_response_429.status_code = 429
        err_429 = httpx.HTTPStatusError(
            "Rate limited", request=MagicMock(), response=mock_response_429
        )
        self.assertTrue(is_transient_error(err_429))

        # 500 Internal server error should be retryable
        mock_response_500 = MagicMock()
        mock_response_500.status_code = 500
        err_500 = httpx.HTTPStatusError(
            "Server error", request=MagicMock(), response=mock_response_500
        )
        self.assertTrue(is_transient_error(err_500))

        # 404 Not found should NOT be retryable
        mock_response_404 = MagicMock()
        mock_response_404.status_code = 404
        err_404 = httpx.HTTPStatusError(
            "Not found", request=MagicMock(), response=mock_response_404
        )
        self.assertFalse(is_transient_error(err_404))

        # Network request error should be retryable
        net_err = httpx.ConnectTimeout("Connection timeout")
        self.assertTrue(is_transient_error(net_err))

        # Generic ValueError should NOT be retryable
        self.assertFalse(is_transient_error(ValueError("Invalid data")))

    async def test_fetch_markets_raw_calls_get_with_retry(self):
        async with CoinGeckoClient() as client:
            mock_data = [{"id": "bitcoin", "current_price": 60000}]
            with patch.object(
                client, "_get_with_retry", new=AsyncMock(return_value=mock_data)
            ) as mock_get:
                result = await client.fetch_markets_raw(
                    ids="bitcoin", vs_currency="usd", per_page=10
                )
                self.assertEqual(result, mock_data)
                mock_get.assert_awaited_once_with(
                    "coins/markets",
                    {
                        "ids": "bitcoin",
                        "vs_currency": "usd",
                        "per_page": 10,
                        "order": "market_cap_desc",
                        "price_change_percentage": "1h,24h,7d",
                    },
                )

    async def test_fetch_coin_metadata_raw_calls_correct_endpoint(self):
        async with CoinGeckoClient() as client:
            mock_data = {"id": "bitcoin", "name": "Bitcoin"}
            with patch.object(
                client, "_get_with_retry", new=AsyncMock(return_value=mock_data)
            ) as mock_get:
                result = await client.fetch_coin_metadata_raw(id="bitcoin")
                self.assertEqual(result, mock_data)
                mock_get.assert_awaited_once_with(
                    "coins/bitcoin",
                    {
                        "localization": "false",
                        "tickers": "false",
                        "market_data": "false",
                        "community_data": "false",
                        "developer_data": "false",
                    },
                )


if __name__ == "__main__":
    unittest.main()
