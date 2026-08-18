import json, sys, glob
from pathlib import Path
sys.path.insert(0, '.')
from ingestion.schemas import CoinMetadataSchema
from pydantic import ValidationError

for coin_dir in sorted(Path('raw_data').iterdir()):
    for fpath in sorted(glob.glob(str(coin_dir / '**' / '*.json'), recursive=True)):
        with open(fpath, encoding='utf-8') as f:
            data = json.load(f)
        try:
            CoinMetadataSchema.model_validate(data)
            print(f"PASS  {coin_dir.name} | {Path(fpath).name}")
        except ValidationError as e:
            print(f"FAIL  {coin_dir.name} | {Path(fpath).name}")
            for err in e.errors():
                loc = ' -> '.join(str(x) for x in err['loc'])
                print(f"   field: {loc} | {err['msg']} ({err['type']})")
