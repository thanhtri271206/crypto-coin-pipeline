import glob
import json
import sys
from pathlib import Path

# Đảm bảo import được ingestion
sys.path.insert(0, ".")

from pydantic import ValidationError

from ingestion.schemas import CoinMetadataSchema


def validate_local_raw_metadata():
    raw_dir = Path("crypto_raw_data") if Path("crypto_raw_data").exists() else Path("raw_data")
    if not raw_dir.exists():
        print(f"No local raw data directory found ({raw_dir}). Skipping manual check.")
        return

    for coin_dir in sorted(raw_dir.iterdir()):
        if not coin_dir.is_dir():
            continue
        for fpath in sorted(glob.glob(str(coin_dir / "**" / "*.json"), recursive=True)):
            with open(fpath, encoding="utf-8") as f:
                data = json.load(f)
            try:
                CoinMetadataSchema.model_validate(data)
                print(f"PASS  {coin_dir.name} | {Path(fpath).name}")
            except ValidationError as e:
                print(f"FAIL  {coin_dir.name} | {Path(fpath).name}")
                for err in e.errors():
                    loc = " -> ".join(str(x) for x in err["loc"])
                    print(f"   field: {loc} | {err['msg']} ({err['type']})")


if __name__ == "__main__":
    validate_local_raw_metadata()
