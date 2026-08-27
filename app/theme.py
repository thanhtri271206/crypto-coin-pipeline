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
BG_DARK      = "#0E1117"   # Streamlit default dark
SURFACE_1    = "#161B22"   # Card / panel background
SURFACE_2    = "#1C2333"   # Hover state / divider
BORDER       = "#30363D"   # Subtle border

# ─── Semantic Colors ───────────────────────────────────────────────────────
POSITIVE_GREEN  = "#26A69A"   # Teal-green — dễ đọc trên dark, không quá neon
NEGATIVE_RED    = "#EF5350"   # Red — nhưng không alarm-red quá
NEUTRAL_GRAY    = "#8B949E"   # Text phụ, label trục
WARN_AMBER      = "#FFA726"   # Drawdown, warning
ACCENT_BLUE     = "#58A6FF"   # Accent / highlight chính
ACCENT_PURPLE   = "#7C4DFF"   # Secondary accent

# ─── Text ──────────────────────────────────────────────────────────────────
TEXT_PRIMARY    = "#E6EDF3"
TEXT_SECONDARY  = "#8B949E"
TEXT_MUTED      = "#6E7681"

# ─── Per-Coin Color Palette (10 coins) ────────────────────────────────────
# Phân biệt rõ ràng trên dark background
COIN_COLORS = {
    "bitcoin":      "#F7931A",   # BTC Orange (canonical)
    "ethereum":     "#627EEA",   # ETH Blue-Purple (canonical)
    "tether":       "#26A17B",   # USDT Green (canonical)
    "usd-coin":     "#2775CA",   # USDC Blue (canonical)
    "binancecoin":  "#F0B90B",   # BNB Yellow (canonical)
    "solana":       "#9945FF",   # SOL Purple (canonical)
    "ripple":       "#00AAE4",   # XRP Cyan (canonical)
    "dogecoin":     "#C2A633",   # DOGE Gold
    "cardano":      "#0D9488",   # ADA Teal
    "shiba-inu":    "#FF5733",   # SHIB Orange-Red
}

# Ordered list (for charts without explicit coin mapping)
COIN_COLOR_LIST = list(COIN_COLORS.values())

# ─── Stablecoin filter ─────────────────────────────────────────────────────
STABLECOINS = {"tether", "usd-coin"}

# ─── Chart Layout Defaults ────────────────────────────────────────────────
CHART_TEMPLATE = "plotly_dark"
CHART_PAPER_BG = "rgba(0,0,0,0)"   # Transparent — merge vào Streamlit bg
CHART_PLOT_BG  = "rgba(0,0,0,0)"
CHART_GRID_COLOR = BORDER
CHART_FONT_COLOR = TEXT_SECONDARY
CHART_MARGIN = dict(l=10, r=10, t=40, b=10)

# ─── Coin display name mapping ─────────────────────────────────────────────
COIN_NAMES = {
    "bitcoin":      "Bitcoin (BTC)",
    "ethereum":     "Ethereum (ETH)",
    "tether":       "Tether (USDT)",
    "usd-coin":     "USD Coin (USDC)",
    "binancecoin":  "BNB (BNB)",
    "solana":       "Solana (SOL)",
    "ripple":       "XRP (XRP)",
    "dogecoin":     "Dogecoin (DOGE)",
    "cardano":      "Cardano (ADA)",
    "shiba-inu":    "Shiba Inu (SHIB)",
}

COIN_SYMBOLS = {
    "bitcoin":      "BTC",
    "ethereum":     "ETH",
    "tether":       "USDT",
    "usd-coin":     "USDC",
    "binancecoin":  "BNB",
    "solana":       "SOL",
    "ripple":       "XRP",
    "dogecoin":     "DOGE",
    "cardano":      "ADA",
    "shiba-inu":    "SHIB",
}
