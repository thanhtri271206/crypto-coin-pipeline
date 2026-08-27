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

import streamlit as st

st.set_page_config(
    page_title="Crypto Market Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = st.navigation(
    [
        st.Page("main.py",                        title="Market Overview",     icon="🏠"),
        st.Page("pages/1_top_movers.py",          title="Top Movers",          icon="🚀"),
        st.Page("pages/2_coin_deep_dive.py",      title="Coin Deep Dive",      icon="🔍"),
        st.Page("pages/3_comparison.py",          title="Comparison",          icon="⚖️"),
        st.Page("pages/4_market_intelligence.py", title="Market Intelligence", icon="🧠"),
    ],
    position="sidebar",
)
pages.run()
