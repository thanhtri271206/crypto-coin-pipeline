"""
DuckDB connection — singleton via st.cache_resource.
Read-only: dashboard chỉ đọc data, không ghi.
"""

import os
from pathlib import Path

import duckdb
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB = str(PROJECT_ROOT / "warehouse" / "crypto.duckdb")

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", _DEFAULT_DB)


@st.cache_resource
def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a shared read-only DuckDB connection."""
    if MOTHERDUCK_TOKEN:
        return duckdb.connect(f"md:crypto_dwh?motherduck_token={MOTHERDUCK_TOKEN}", read_only=True)
    return duckdb.connect(DUCKDB_PATH, read_only=True)
