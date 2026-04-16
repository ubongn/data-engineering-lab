import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def write_parquet(records: list[dict], output_path: str) -> int:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(records)
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    logger.info(f"{output_path} — {len(df)} rows")
    return len(df)