import unittest

from pydantic import ValidationError

from ingestion import schemas


class TestSchemas(unittest.TestCase):
    def test_coin_market_schema_valid(self):
        payload = {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 65000.5,
            "market_cap": 1280000000000,
            "market_cap_rank": 1,
            "total_volume": 35000000000,
            "high_24h": 66000.0,
            "low_24h": 64000.0,
            "price_change_24h": 1000.5,
            "price_change_percentage_24h": 1.55,
            "circulating_supply": 19700000.0,
            "total_supply": 21000000.0,
            "max_supply": 21000000.0,
            "last_updated": "2026-09-08T00:00:00.000Z",
        }
        item = schemas.CoinMarketSchema.model_validate(payload)
        self.assertEqual(item.id, "bitcoin")
        self.assertEqual(item.current_price, 65000.5)

    def test_coin_market_schema_invalid_missing_required(self):
        # Missing 'symbol', 'name'
        payload = {"id": "bitcoin"}
        with self.assertRaises(ValidationError):
            schemas.CoinMarketSchema.model_validate(payload)

    def test_coin_ohlc_schema_validation(self):
        # Valid: 5 floats per candle [timestamp, open, high, low, close]
        valid_ohlc = [
            [1700000000000.0, 60000.0, 61000.0, 59500.0, 60500.0],
            [1700003600000.0, 60500.0, 62000.0, 60200.0, 61800.0],
        ]
        parsed = schemas.CoinOHLCSchema.model_validate(valid_ohlc)
        self.assertEqual(len(parsed.root), 2)

        # Invalid: candle has only 4 elements
        invalid_ohlc = [[1700000000000.0, 60000.0, 61000.0, 59500.0]]
        with self.assertRaises(ValidationError):
            schemas.CoinOHLCSchema.model_validate(invalid_ohlc)


if __name__ == "__main__":
    unittest.main()
