from datetime import datetime
from typing import Dict, List, Optional
 
from pydantic import BaseModel, ConfigDict, RootModel, field_validator
 
 
# ---------------------------------------------------------------------------
# 1. coins/markets — snapshot giá + market data cho Top 10 coin
#    Nguồn chính cho fact_market_snapshot_hourly
# ---------------------------------------------------------------------------
class CoinMarketSchema(BaseModel):
    """Schema cho endpoint coins/markets.
 
    Params gọi API tương ứng: vs_currency=usd, ids=<10 coin id>,
    order=market_cap_desc, price_change_percentage=1h,24h,7d
    """
    model_config = ConfigDict(extra="ignore")
 
    # Bắt buộc — luôn có trong mọi response hợp lệ
    id: str
    symbol: str
    name: str
    last_updated: datetime
 
    # Optional — số liệu thị trường, có thể null tuỳ coin/thời điểm
    current_price: Optional[float] = None
    market_cap: Optional[float] = None
    market_cap_rank: Optional[int] = None
    total_volume: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    price_change_24h: Optional[float] = None
    price_change_percentage_24h: Optional[float] = None
    circulating_supply: Optional[float] = None
    total_supply: Optional[float] = None
    max_supply: Optional[float] = None  # ETH, DOGE... luôn null field này
    ath: Optional[float] = None
    ath_change_percentage: Optional[float] = None
    ath_date: Optional[datetime] = None
    atl: Optional[float] = None
    atl_change_percentage: Optional[float] = None
    atl_date: Optional[datetime] = None
 
    # Chỉ có nếu gọi API kèm param price_change_percentage=1h,24h,7d
    price_change_percentage_1h_in_currency: Optional[float] = None
    price_change_percentage_7d_in_currency: Optional[float] = None
 
 
# ---------------------------------------------------------------------------
# 2. coins/{id}/market_chart — backfill time-series lịch sử
#    Nguồn cho fact_market_snapshot_daily + Time-series Mart
# ---------------------------------------------------------------------------
class CoinMarketChartSchema(BaseModel):
    """Schema cho coins/{id}/market_chart.
 
    Mục đích: Backfill historical data cho Top 10 coin.
    Gọi full backfill (days=max) 1 lần duy nhất, sau đó incremental chỉ
    lấy window gần nhất (vd last 7 days hourly) — không re-fetch full history.
    """
    model_config = ConfigDict(extra="ignore")
 
    prices: List[List[float]]         # [[timestamp_ms, price], ...]
    market_caps: List[List[float]]    # [[timestamp_ms, market_cap], ...]
    total_volumes: List[List[float]]  # [[timestamp_ms, volume], ...]
 
    @field_validator("prices", "market_caps", "total_volumes")
    @classmethod
    def validate_pairs(cls, v: List[List[float]]) -> List[List[float]]:
        """Đảm bảo mỗi phần tử đúng dạng [timestamp, value] (length = 2).
        Tránh lỗi âm thầm nếu CoinGecko đổi format response."""
        for item in v:
            if len(item) != 2:
                raise ValueError(f"Mỗi phần tử phải là [timestamp, value], nhận được: {item}")
        return v
 
 
# ---------------------------------------------------------------------------
# 3. coins/{id} — metadata, dùng để enrich dim_coin
#    Gọi 1 lần/tuần, KHÔNG lấy lại market_data (đã có từ coins/markets)
# ---------------------------------------------------------------------------
class CoinDescriptionSchema(BaseModel):
    """Nested object 'description' trong response coins/{id}.
    CoinGecko trả description theo nhiều ngôn ngữ — chỉ lấy bản tiếng Anh."""
    model_config = ConfigDict(extra="ignore")
 
    en: Optional[str] = None
 
 
class CoinLinksSchema(BaseModel):
    """Nested object 'links' trong response coins/{id}.
    Chỉ giữ lại các link thật sự dùng cho dim_coin — bỏ qua các link
    mạng xã hội ít giá trị phân tích (chat, forum, announcement...)."""
    model_config = ConfigDict(extra="ignore")
 
    homepage: Optional[List[str]] = []
    blockchain_site: Optional[List[str]] = []
 
    @field_validator("homepage", "blockchain_site")
    @classmethod
    def drop_empty_strings(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """CoinGecko hay trả về mảng có nhiều phần tử rỗng ("") — lọc bỏ
        ngay ở bước validate để dbt không phải xử lý lại."""
        # return [url for url in v if url]
        if v is None:
            return []
        if isinstance(v, list):
            return [url for url in v if url]
        return v
 
 
class CoinMetadataSchema(BaseModel):
    """Schema cho endpoint coins/{id}.
 
    Params gọi API nên tắt bớt phần nặng không cần: community_data=false,
    developer_data=false, localization=false, tickers=false, market_data=false
    (market_data đã lấy đủ từ coins/markets, không lấy trùng ở đây).
    """
    model_config = ConfigDict(extra="ignore")
 
    id: str
    symbol: str
    name: str
 
    categories: List[Optional[str]] = []  # CoinGecko đôi khi trả None trong mảng category
    description: Optional[CoinDescriptionSchema] = None
    links: Optional[CoinLinksSchema] = None
    genesis_date: Optional[str] = None    # dạng "YYYY-MM-DD" hoặc null, không phải ISO datetime đầy đủ
    market_cap_rank: Optional[int] = None
 
    # platforms: dict {tên_chain: contract_address}, vd {"ethereum": "0xabc..."}
    # Native coin (BTC) trả về {"": ""}. Coin đa chuỗi (SOL, ADA...) có thêm các
    # chain entry với value null (vd {"solana": null, "ethereum": "0x..."}) —
    # phải dùng Optional[str] để không fail validation.
    # Lọc null/empty value để xử lý ở tầng dbt/transform, không làm ở schema level.
    platforms: Dict[str, Optional[str]] = {}
 
 
# ---------------------------------------------------------------------------
# 4. global — KPI tổng quan thị trường, nguồn cho Market Health Mart
#    Lưu ý: response thật nằm trong wrapper {"data": {...}}
# ---------------------------------------------------------------------------
class GlobalMarketDataSchema(BaseModel):
    """Nested object 'data' trong response /global."""
    model_config = ConfigDict(extra="ignore")
 
    active_cryptocurrencies: Optional[int] = None
    markets: Optional[int] = None
 
    # Các field dạng dict theo currency — chỉ cần lấy .get("usd")
    # ở bước transform/parsing, không tách riêng field usd tại schema level
    # để giữ đúng cấu trúc gốc phòng khi cần mở rộng đa currency sau này.
    total_market_cap: Dict[str, float] = {}
    total_volume: Dict[str, float] = {}
    market_cap_percentage: Dict[str, float] = {}  # vd {"btc": 52.3, "eth": 14.1, ...}
 
    market_cap_change_percentage_24h_usd: Optional[float] = None
    updated_at: Optional[int] = None  # unix timestamp (giây) — không phải ISO string
 
 
class GlobalDataSchema(BaseModel):
    """Schema cho toàn bộ response endpoint /global (có wrapper 'data')."""
    model_config = ConfigDict(extra="ignore")
 
    data: GlobalMarketDataSchema
 
 
# ---------------------------------------------------------------------------
# 5. coins/{id}/ohlc — OPTIONAL, chỉ dùng nếu làm candlestick chart
# ---------------------------------------------------------------------------
class CoinOHLCSchema(RootModel[List[List[float]]]):
    """Schema cho coins/{id}/ohlc.
 
    QUAN TRỌNG: response API là 1 mảng phẳng [[timestamp, open, high, low,
    close], ...] ở NGAY top-level, không có wrapper key nào cả — giống hệt
    coins/markets. Vì vậy phải dùng RootModel (giống CoinMarketListSchema ở
    coingecko_client.py), KHÔNG dùng BaseModel với field đặt tên tuỳ ý —
    nếu dùng BaseModel, model_validate() sẽ raise lỗi ngay vì input là list
    chứ không phải dict.
    """
 
    @field_validator("root")
    @classmethod
    def validate_ohlc_rows(cls, v: List[List[float]]) -> List[List[float]]:
        for item in v:
            if len(item) != 5:
                raise ValueError(
                    f"Mỗi phần tử OHLC phải là [timestamp, open, high, low, close], nhận được: {item}"
                )
        return v