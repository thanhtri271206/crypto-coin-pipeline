import unittest
from datetime import datetime
from ingestion.coingecko_client import CoinGeckoClient
from ingestion import schemas


class TestCoinGeckoValidation(unittest.TestCase):
    def test_validate_markets(self):
        raw_data = [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "last_updated": "2026-08-15T00:00:00.000Z",
                "current_price": 60000.0,
                "market_cap": 1200000000000.0,
            }
        ]
        validated = CoinGeckoClient.validate_markets(raw_data)
        self.assertEqual(len(validated), 1)
        self.assertEqual(validated[0].id, "bitcoin")
        self.assertEqual(validated[0].symbol, "btc")
        self.assertEqual(validated[0].current_price, 60000.0)

    def test_validate_coin_metadata(self):
        raw_data = {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "categories": ["Smart Contract Platform"],
            "description": {"en": "Bitcoin is a cryptocurrency."},
            "links": {"homepage": ["https://bitcoin.org"], "blockchain_site": []},
            "platforms": {},
        }
        validated = CoinGeckoClient.validate_coin_metadata(raw_data)
        self.assertIsInstance(validated, schemas.CoinMetadataSchema)
        self.assertEqual(validated.id, "bitcoin")
        self.assertEqual(validated.description.en, "Bitcoin is a cryptocurrency.")

    def test_validate_coin_market_chart(self):
        raw_data = {
            "prices": [[1700000000000, 50000.0]],
            "market_caps": [[1700000000000, 1000000000.0]],
            "total_volumes": [[1700000000000, 5000000.0]],
        }
        validated = CoinGeckoClient.validate_market_chart(raw_data)
        self.assertIsInstance(validated, schemas.CoinMarketChartSchema)
        self.assertEqual(len(validated.prices), 1)
        self.assertEqual(validated.prices[0], [1700000000000, 50000.0])

    def test_validate_global_market_data(self):
        raw_data = {
            "data": {
                "active_cryptocurrencies": 10000,
                "markets": 800,
                "total_market_cap": {"usd": 2500000000000.0},
                "total_volume": {"usd": 100000000000.0},
                "market_cap_percentage": {"btc": 55.0},
                "updated_at": 1700000000,
            }
        }
        validated = CoinGeckoClient.validate_global_market_data(raw_data)
        self.assertIsInstance(validated, schemas.GlobalMarketDataSchema)
        self.assertEqual(validated.active_cryptocurrencies, 10000)
        self.assertEqual(validated.total_market_cap["usd"], 2500000000000.0)

    def test_validate_coin_ohlc(self):
        raw_data = [
            [1700000000000, 50000.0, 51000.0, 49500.0, 50500.0]
        ]
        validated = CoinGeckoClient.validate_coin_ohlc(raw_data)
        self.assertEqual(len(validated), 1)
        self.assertEqual(validated[0], [1700000000000, 50000.0, 51000.0, 49500.0, 50500.0])


if __name__ == "__main__":
    unittest.main()
