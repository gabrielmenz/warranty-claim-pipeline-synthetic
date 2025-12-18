import pandas as pd

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Feature engineering logic for demonstration."""
    df = df.copy()
    
    df["usage_intensity"] = df["mileage_km"] / (df["vehicle_age_months"] + 1)
    df["is_high_usage"] = df["usage_intensity"] > df["usage_intensity"].median()

    return df
