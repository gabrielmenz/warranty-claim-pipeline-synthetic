# config/paths_nissan.py

from pathlib import Path

# 1) Your code base root (where your pipeline/, notebooks/, config/ live)
BASE_CODE = Path(
    r"\\bosch.com\DfsRB\DfsJP\DIV\PS\QMC\All\01.QMC11"
    r"\05_General\06_internship\20251016_Gabriel_Alves"
    r"\AI_Projects\warranty-judge\01. Nissan"
)
# TODO: replace 20251016_Gabriel_Alves with your exact folder name.


# 2) Production data root (QMM shared collabo path you just gave)
BASE_DATA = Path(
    r"\\bosch.com\DfsRB\DfsJP\DIV\PS\z_Collabo"
    r"\0215_QMM_JP3_Share\Claim_WBS\4.Warranty_Info\11.Nissan_異議"
)

# 3) Folder with AI results that Power BI uses
#    Adjust 'AI_Results' if your actual subfolder name is different.
AI_RESULTS_DIR = BASE_DATA / "AI_Results"

# 4) Historical validated claims file
AI_CLAIMS_PATH = AI_RESULTS_DIR / "AI_validated_claims.xlsx"

# 5) Your personal temp folder (safe, not used by PBI)
TEMP_DIR = BASE_CODE / "temp"
