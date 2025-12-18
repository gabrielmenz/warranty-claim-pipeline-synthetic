import pandas as pd

def load_synthetic_data(n: int = 200) -> pd.DataFrame:
    """Generate synthetic warranty claim data for demonstration."""
    import numpy as np

    np.random.seed(42)

    df = pd.DataFrame({
        "claim_id": range(1, n+1),
        "vehicle_age_months": np.random.randint(0, 60, n),
        "mileage_km": np.random.randint(0, 200000, n),
        "component": np.random.choice(["ModuleX", "ModuleY", "ModuleZ"], n),
        "symptom_code": np.random.randint(1, 10, n),
    })

    return df
