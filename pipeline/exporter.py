from pathlib import Path

def export_results(df, path="data/synthetic_results.csv"):
    path = Path(path)
    df.to_csv(path, index=False)
    return path
