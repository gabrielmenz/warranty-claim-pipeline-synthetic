def clean_data(df):
    """Basic cleaning logic for synthetic dataset."""
    df = df.copy()
    df["mileage_km"] = df["mileage_km"].clip(lower=0)
    df["vehicle_age_months"] = df["vehicle_age_months"].clip(lower=0)
    return df
