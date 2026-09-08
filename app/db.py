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

def _get_config_val(key: str, default=None):
    """Retrieve config value from os.environ first, falling back to st.secrets."""
    val = os.getenv(key)
    if val:
        return val
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return default


@st.cache_resource
def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a shared read-only DuckDB connection."""
    motherduck_token = _get_config_val("MOTHERDUCK_TOKEN")
    if motherduck_token:
        return duckdb.connect(f"md:crypto_dwh?motherduck_token={motherduck_token}", read_only=True)

    db_path = _get_config_val("DUCKDB_PATH", _DEFAULT_DB)
    return duckdb.connect(db_path, read_only=True)
