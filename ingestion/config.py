from pathlib import Path
import yaml

# Đường dẫn tới thư mục gốc dự án và file config
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "coins.yaml"


def load_coin_ids(config_path: Path = CONFIG_PATH) -> list[str]:
    """Parse file config/coins.yaml và trả về danh sách danh mục coin ID."""

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    coins = data.get("coins", [])
    return [coin["id"] for coin in coins if isinstance(coin, dict) and "id" in coin]


# Biến cấu hình dùng chung cho các DAGs
COIN_IDS: list[str] = load_coin_ids()
COIN_IDS_STR: str = ",".join(COIN_IDS)
