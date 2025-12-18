def classify(df):
    """Very simple synthetic rule-based model."""
    df = df.copy()

    df["prediction"] = (
        (df["symptom_code"] < 5) &
        (df["is_high_usage"] == False)
    ).astype(int)

    return df

