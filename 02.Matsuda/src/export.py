# 02.Matsuda/src/export.py
from pathlib import Path
import pandas as pd

from .sanitize import sanitize_public

def export_results(df: pd.DataFrame, out_path: str | Path, public: bool = True) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out = sanitize_public(df) if public else df
    out.to_csv(out_path, index=False)
