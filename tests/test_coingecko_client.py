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
        data = await client.ingest_markets(ids="bitcoin,ethereum")
        print(f"Ingested {len(data)} market records:")
        for item in data:
            print(f"- {item.name} ({item.symbol.upper()}): ${item.current_price}")


if __name__ == "__main__":
    asyncio.run(main())