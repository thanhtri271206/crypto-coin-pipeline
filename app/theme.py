"""
Design tokens cho toàn bộ dashboard.
Dark-mode professional financial/crypto color system.

Nguyên tắc:
- Xanh lá (POSITIVE_GREEN) = giá tăng, return dương
- Đỏ (NEGATIVE_RED)        = giá giảm, return âm
- Cam (WARN_AMBER)         = cảnh báo, drawdown
- Tím nhạt (ACCENT_*)      = series phụ, background accent
- Các màu coin được chọn để phân biệt rõ trên dark background
"""

# ─── Background & Surface ──────────────────────────────────────────────────
BG_DARK = "#0E1117"  # Streamlit default dark
SURFACE_1 = "#161B22"  # Card / panel background
SURFACE_2 = "#1C2333"  # Hover state / divider
BORDER = "#30363D"  # Subtle border

# ─── Semantic Colors ───────────────────────────────────────────────────────
POSITIVE_GREEN = "#26A69A"  # Teal-green — dễ đọc trên dark, không quá neon
NEGATIVE_RED = "#EF5350"  # Red — nhưng không alarm-red quá
NEUTRAL_GRAY = "#8B949E"  # Text phụ, label trục
WARN_AMBER = "#FFA726"  # Drawdown, warning
ACCENT_BLUE = "#58A6FF"  # Accent / highlight chính
ACCENT_PURPLE = "#7C4DFF"  # Secondary accent

# ─── Text ──────────────────────────────────────────────────────────────────
TEXT_PRIMARY = "#E6EDF3"
TEXT_SECONDARY = "#8B949E"
TEXT_MUTED = "#6E7681"

# ─── Per-Coin Color Palette (10 coins) ────────────────────────────────────
# Phân biệt rõ ràng trên dark background
COIN_COLORS = {
    "bitcoin": "#F7931A",  # BTC Orange (canonical)
    "ethereum": "#627EEA",  # ETH Blue-Purple (canonical)
    "tether": "#26A17B",  # USDT Green (canonical)
    "usd-coin": "#2775CA",  # USDC Blue (canonical)
    "binancecoin": "#F0B90B",  # BNB Yellow (canonical)
    "solana": "#9945FF",  # SOL Purple (canonical)
    "ripple": "#00AAE4",  # XRP Cyan (canonical)
    "dogecoin": "#C2A633",  # DOGE Gold
    "cardano": "#0D9488",  # ADA Teal
    "shiba-inu": "#FF5733",  # SHIB Orange-Red
}

# Ordered list (for charts without explicit coin mapping)
COIN_COLOR_LIST = list(COIN_COLORS.values())

# ─── Stablecoin filter ─────────────────────────────────────────────────────
STABLECOINS = {"tether", "usd-coin"}

# ─── Chart Layout Defaults ────────────────────────────────────────────────
CHART_TEMPLATE = "plotly_dark"
CHART_PAPER_BG = "rgba(0,0,0,0)"  # Transparent — merge vào Streamlit bg
CHART_PLOT_BG = "rgba(0,0,0,0)"
CHART_GRID_COLOR = BORDER
CHART_FONT_COLOR = TEXT_SECONDARY
CHART_MARGIN = dict(l=10, r=10, t=40, b=10)

# ─── Coin display name mapping ─────────────────────────────────────────────
COIN_NAMES = {
    "bitcoin": "Bitcoin (BTC)",
    "ethereum": "Ethereum (ETH)",
    "tether": "Tether (USDT)",
    "usd-coin": "USD Coin (USDC)",
    "binancecoin": "BNB (BNB)",
    "solana": "Solana (SOL)",
    "ripple": "XRP (XRP)",
    "dogecoin": "Dogecoin (DOGE)",
    "cardano": "Cardano (ADA)",
    "shiba-inu": "Shiba Inu (SHIB)",
}

COIN_SYMBOLS = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "tether": "USDT",
    "usd-coin": "USDC",
    "binancecoin": "BNB",
    "solana": "SOL",
    "ripple": "XRP",
    "dogecoin": "DOGE",
    "cardano": "ADA",
    "shiba-inu": "SHIB",
}

# ─── Vietnamese Coin Descriptions ──────────────────────────────────────────
COIN_DESCRIPTIONS_VI = {
    "bitcoin": (
        "**Bitcoin (BTC)** là đồng tiền mã hóa phi tập trung đầu tiên trên thế giới, "
        "được tạo ra bởi Satoshi Nakamoto vào năm 2009. Hoạt động trên cơ chế đồng thuận "
        "Proof-of-Work (PoW) với tổng cung cố định 21 triệu coin, Bitcoin thường được coi là "
        "'vàng kỹ thuật số' và là tài sản lưu trữ giá trị chủ chốt của thị trường crypto."
    ),
    "ethereum": (
        "**Ethereum (ETH)** là nền tảng blockchain mã nguồn mở tiên phong hỗ trợ hợp đồng thông minh "
        "(Smart Contracts) và các ứng dụng phi tập trung (DApps). Chuyển sang cơ chế Proof-of-Stake (PoS) "
        "từ bản nâng cấp The Merge, Ethereum là xương sống của hầu hết hệ sinh thái Tài chính phi tập trung (DeFi) "
        "và Web3 toàn cầu."
    ),
    "tether": (
        "**Tether (USDT)** là stablecoin lớn nhất thế giới, được neo giá trị theo tỷ lệ 1:1 với đồng Đô la Mỹ (USD). "
        "Được phát hành bởi Tether Limited, USDT đóng vai trò là cầu nối thanh khoản chính giữa tiền pháp định (fiat) "
        "và thị trường tiền mã hóa, giúp nhà đầu tư giảm thiểu rủi ro biến động giá."
    ),
    "usd-coin": (
        "**USD Coin (USDC)** là stablecoin được quản lý bởi Centre Consortium (thành lập bởi Circle và Coinbase). "
        "Tương tự USDT, USDC được neo 1:1 với USD và được bảo chứng 100% bằng tiền mặt cùng tín phiếu kho bạc Mỹ, "
        "được đánh giá cao về tính minh bạch và tuân thủ pháp lý."
    ),
    "binancecoin": (
        "**BNB (BNB)** ban đầu là token tiện ích của sàn giao dịch Binance, hiện là native token vận hành "
        "hệ sinh thái blockchain BNB Chain. BNB được sử dụng để thanh toán phí giao dịch, tham gia launchpad, "
        "vận hành các ứng dụng DeFi và có cơ chế đốt token (burn) định kỳ để giảm nguồn cung."
    ),
    "solana": (
        "**Solana (SOL)** là nền tảng blockchain Layer 1 hiệu năng cao, nổi bật với khả năng xử lý hàng chục nghìn "
        "giao dịch mỗi giây (TPS) cùng chi phí cực thấp. Sử dụng cơ chế kết hợp Proof-of-History (PoH) và Proof-of-Stake, "
        "Solana là môi trường lý tưởng cho DeFi, NFT và các ứng dụng thanh toán thời gian thực."
    ),
    "ripple": (
        "**XRP (XRP)** là token vận hành sổ cái phi tập trung XRP Ledger, được phát triển bởi Ripple Labs. "
        "Mục tiêu chính của XRP là cách mạng hóa mạng lưới chuyển tiền và thanh toán xuyên biên giới cho các định chế "
        "tài chính và ngân hàng với tốc độ xử lý trong vài giây và chi phí gần như bằng không."
    ),
    "dogecoin": (
        "**Dogecoin (DOGE)** ra đời vào năm 2013 như một trò đùa dựa trên meme chú chó Shiba Inu, nhưng đã nhanh chóng "
        "phát triển thành một đồng tiền mã hóa thanh toán ngang hàng phổ biến với cộng đồng hùng hậu. Dogecoin sử dụng thuật toán "
        "Scrypt và có nguồn cung không giới hạn."
    ),
    "cardano": (
        "**Cardano (ADA)** là nền tảng blockchain Proof-of-Stake thế hệ thứ ba, được phát triển dựa trên phương pháp "
        "nghiên cứu khoa học và phản biện học thuật (peer-reviewed). Cardano hướng tới việc cung cấp cơ sở hạ tầng bền vững, "
        "bảo mật cao và có khả năng mở rộng cho các hợp đồng thông minh và hệ thống doanh nghiệp."
    ),
    "shiba-inu": (
        "**Shiba Inu (SHIB)** xuất phát điểm là một token meme phi tập trung trên mạng Ethereum, sau đó đã mở rộng thành một "
        "hệ sinh thái toàn diện gồm sàn giao dịch phi tập trung ShibaSwap, giải pháp Layer 2 Shibarium và dự án Metaverse."
    ),
}

# ─── Centralized CSS Styling ───────────────────────────────────────────────
CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* KPI metric cards */
[data-testid="metric-container"] {
    background-color: #161B22;
    border: 1px solid #30363D;
    border-radius: 10px;
    padding: 12px 16px;
}
[data-testid="metric-container"] label {
    font-size: 11px !important;
    color: #8B949E !important;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 16px !important;
    font-weight: 700;
    color: #E6EDF3 !important;
}
[data-testid="stMetricDelta"] {
    font-size: 12px !important;
    font-weight: 600;
}

/* Sidebar header */
section[data-testid="stSidebar"] {
    background-color: #161B22;
    border-right: 1px solid #30363D;
}

/* Dividers */
hr {
    border-color: #30363D;
    margin: 12px 0;
}

/* Streamlit plotly charts */
.js-plotly-plot {
    border-radius: 8px;
}

/* Alert boxes */
.stAlert {
    border-radius: 8px;
    border: 1px solid #30363D;
}
</style>
"""


def apply_custom_css() -> None:
    """Inject centralized dark-mode styles across dashboard pages."""
    import streamlit as st

    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
