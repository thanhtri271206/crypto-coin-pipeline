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
    """Retrieve config value from st.secrets first, falling back to os.environ."""
    val = None
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            val = str(st.secrets[key])
    except Exception:
        pass
    if not val:
        val = os.getenv(key)

    if val:
        return val.strip().strip('"').strip("'")
    return default


@st.cache_resource
def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a shared DuckDB connection (MotherDuck cloud or local read-only)."""
    motherduck_token = _get_config_val("MOTHERDUCK_TOKEN")
    if motherduck_token:
        # Validate JWT token structure
        if motherduck_token.count(".") != 2:
            raise ValueError(
                f"MOTHERDUCK_TOKEN không hợp lệ (có {motherduck_token.count('.')} dấu chấm thay vì 2 dấu chấm).\n"
                "Token MotherDuck đầy đủ phải có dạng: 'Header.Payload.Signature'.\n"
                "Hãy sao chép toàn bộ token từ file .env hoặc MotherDuck Console vào Streamlit Secrets."
            )
        os.environ["motherduck_token"] = motherduck_token
        md_database = _get_config_val("MOTHERDUCK_DATABASE", "crypto_dwh")
        return duckdb.connect(f"md:{md_database}?motherduck_token={motherduck_token}")

    db_path = _get_config_val("DUCKDB_PATH", _DEFAULT_DB)
    return duckdb.connect(db_path, read_only=True)
