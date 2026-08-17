import asyncio
import sys
from pathlib import Path

# Đảm bảo thư mục gốc dự án nằm trong sys.path khi chạy trực tiếp file test
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.coingecko_client import CoinGeckoClient


async def main():
    async with CoinGeckoClient() as client:
        raw_data = await client.fetch_markets_raw(ids="bitcoin,ethereum")
        print(f"Fetched raw market data with {len(raw_data)} items.")
        
        validated_data = CoinGeckoClient.validate_markets(raw_data)
        print(f"Validated {len(validated_data)} market records:")
        for item in validated_data:
            print(f"- {item.name} ({item.symbol.upper()}): ${item.current_price}")



if __name__ == "__main__":
    asyncio.run(main())