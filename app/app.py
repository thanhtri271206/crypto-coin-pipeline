"""
Entry point chính của Crypto Dashboard.
Dùng st.navigation() (Streamlit ≥1.37) để kiểm soát hoàn toàn:
  - Page icon (emoji)
  - Page label trong sidebar
  - Thứ tự hiển thị

Cách chạy: streamlit run app/app.py
"""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import importlib

import streamlit as st

# Ensure core app modules are fresh on rerun even in long-running processes
for _mod in ("app.theme", "app.queries", "app.charts"):
    if _mod in sys.modules:
        importlib.reload(sys.modules[_mod])

st.set_page_config(
    page_title="Crypto Market Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = st.navigation(
    [
        st.Page("main.py", title="Market Overview", icon=":material/dashboard:"),
        st.Page("pages/1_top_movers.py", title="Top Movers", icon=":material/trending_up:"),
        st.Page("pages/2_coin_deep_dive.py", title="Coin Deep Dive", icon=":material/analytics:"),
        st.Page("pages/3_comparison.py", title="Comparison", icon=":material/compare_arrows:"),
        st.Page(
            "pages/4_market_intelligence.py",
            title="Market Intelligence",
            icon=":material/insights:",
        ),
    ],
    position="sidebar",
)
pages.run()
