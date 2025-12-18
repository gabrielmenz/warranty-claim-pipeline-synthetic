import numpy as np
import pandas as pd


def generate_synthetic_warranty_data(n_rows: int = 5000, random_state: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(random_state)

    claim_ids = [f"CLM_{i:06d}" for i in range(1, n_rows + 1)]

    base_date = np.datetime64("2022-01-01")
    days_offset = rng.integers(0, 3 * 365, size=n_rows)
    claim_dates = base_date + days_offset

    reg_offset_days = rng.integers(100, 3000, size=n_rows)
    vehicle_reg_dates = claim_dates - reg_offset_days

    fail_offset_days = rng.integers(1, 60, size=n_rows)
    vehicle_fail_dates = claim_dates - fail_offset_days

    vehicle_mfd_year = vehicle_reg_dates.astype("datetime64[Y]").astype(int) + 1970

    use_years = (claim_dates - vehicle_reg_dates).astype("timedelta64[D]").astype(float) / 365
    mileage = use_years * rng.normal(15000, 5000, size=n_rows)
    mileage = np.clip(mileage, 0, None)

    part_groups = rng.choice(["Engine", "Brakes", "Electronics", "Body", "Chassis"], size=n_rows)
    subparts = []
    for pg in part_groups:
        if pg == "Engine":
            subparts.append(rng.choice(["ENG_A", "ENG_B", "ENG_C"]))
        elif pg == "Brakes":
            subparts.append(rng.choice(["BRK_A", "BRK_B"]))
        elif pg == "Electronics":
            subparts.append(rng.choice(["ELEC_A", "ELEC_B", "ELEC_C"]))
        elif pg == "Body":
            subparts.append(rng.choice(["BODY_A", "BODY_B"]))
        else:
            subparts.append(rng.choice(["CHS_A", "CHS_B"]))

    failure_modes = rng.choice(["Leak", "Noise", "No_Start", "Vibration", "Electrical_Issue"], size=n_rows)
    customer_type = rng.choice(["Retail", "Fleet"], size=n_rows, p=[0.7, 0.3])
    region = rng.choice(["Region_1", "Region_2", "Region_3"], size=n_rows)

    labor_cost = rng.gamma(shape=2.0, scale=80.0, size=n_rows)
    material_cost = rng.gamma(shape=2.0, scale=120.0, size=n_rows)
    total_cost = labor_cost + material_cost

    burden_ratio = rng.uniform(0.2, 1.0, size=n_rows)

    p_approve = (
        0.75
        - 0.04 * use_years
        - 0.000002 * mileage
        - 0.15 * (total_cost > 1800)
    )
    p_approve = np.clip(p_approve, 0.05, 0.95)

    decision_random = rng.random(size=n_rows)
    final_decision = np.where(decision_random < p_approve, "Approve", "Reject")

    p_dpr = np.clip(p_approve - 0.1, 0.05, 0.95)
    dpr_decision = np.where(rng.random(size=n_rows) < p_dpr, "Approve", "Reject")

    df = pd.DataFrame(
        {
            "Claim_ID": claim_ids,
            "Claim_Date": claim_dates,
            "Vehicle_Registration_Date": vehicle_reg_dates,
            "Vehicle_Failure_Date": vehicle_fail_dates,
            "Vehicle_MFD_Year": vehicle_mfd_year,
            "Mileage_km": mileage,
            "Part_Group": part_groups,
            "Subpart_Code": subparts,
            "Failure_Mode": failure_modes,
            "Customer_Type": customer_type,
            "Region": region,
            "Labor_Cost": labor_cost,
            "Material_Cost": material_cost,
            "Total_Cost": total_cost,
            "Burden_Ratio": burden_ratio,
            "Final_Claim_Decision": final_decision,
            "Final_DPR_Decision": dpr_decision,
        }
    )

    return df


def save_synthetic_warranty_data(
    output_path: str = "data/raw/warranty_claims_synthetic.csv",
    n_rows: int = 5000,
    random_state: int = 42,
) -> None:
    df = generate_synthetic_warranty_data(n_rows=n_rows, random_state=random_state)
    df.to_csv(output_path, index=False)
