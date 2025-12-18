# %% [markdown]
# # Nissan Warranty Judge — Rule-based Model
# # Version: 1.0 (2025-10) 
# # Author: Gabriel Alves (PS/QMC 11-JP)
# # Status: Production, replaces old working_dev notebook

# %%
# ============================================================
# 0. IMPORTS
# ============================================================

from datetime import datetime, timedelta
import os
import re
import string

import numpy as np
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from rapidfuzz import fuzz


# %%
# ============================================================
# 1. CONFIGURATION
# ============================================================

# Monthly claim date (format: yyyy/mm/dd)
CLAIM_DATE = "2025/11/01"

# Sheet name containing monthly Nissan PS data
SHEET_PS = "For_sap_C"


# Datetime version of claim date (used across pipeline)
claim_date_ts = pd.to_datetime(CLAIM_DATE)

# ============================================================
# PART FILTER LISTS (Behavior-preserving)
# These lists define subparts that should be auto-accepted or 
# filtered in objection logic. Business rules unchanged.
# ============================================================

PART_GROUPS = {
    "GLOW_PLUG": [
        '101023XN4A', '140355X00A', '140365X00A', '144113XN3E', '144155X00A',
        '144455X00A', 'NIMEXU03Q1', '120', 'BUHIN0029100', '147306RC0A',
        'ATM01F0009', '320106US1A', '290Y63NF0B', '144116UA0A', '552226RN1A',
        '56280JP00A', '144986UA2A', '144616RC0B', '658406UA0A', '1518940P00',
        '668926UA0A', 'KE90299975', '2144088M00', '147496RC1A', '200A06RA2C',
        '166356RC0A', '147356RC0B', '166186RC0B', '166126RC0A', 'B08A26RX0A',
        '166186RC0D', 'KE90090033', '450', '12309EN20A', '442', 'KE90999945',
        '150666RC0A', '140944KV0A', '130496RC0A', '331144BA0B', '0142200Q0C',
        '37120JD01A', '37120JD00B', '331424BA0C', '164397S02A', '371717S00A',
        '924776RA1A', '250853JA0A', '110261CA0A', '383423WX0C', '1643956S1A',
        '12315ED010', '92473N823A', '24009F991J', '147106RC0B', '223416UL0A',
        '391006RC4D', '201006RU2C', '223206UA0A', 'KE90200075', 'KE90090041',
        '14094JG30A', '14064JG30A', 'KE90090043', '112986RN0D', '14064JG30B',
        '166356RC0B', '166186RC0C', 'BUHIN0024000', '224486RC0C', '2277000Q0D',
        '201006RU1C', '01125E6071', '144606RC2A', '140416RC1A', '140355CA0A',
        '166351LA0B', '140325CA0A', '166005CA0A', '226935CA0A', '224015CA1D',
        '161755CA0A', '3100921X00', '144115CA3D', '144645CA1A', '37121JK20B',
        '0122500062', '151925CA1A', '144645CB0A', '151895CA0B', '144155CA0A',
        '144505CA1A', '130495CA0A', '12315D0201', '402622Y00A', '144115CA3C',
        '151925CA1B', '213045CB1A', '37120AH00A', '144455CB0A', '37171AL60A',
        '144506GP0A', '383430P013', '37120AL60A', '140949BA0A', '151895CA0A',
        '101026HN3A', '210144HK0A', '20692JK00B', '999MPCV0NS3', '1233016A0A',
        '210495CA0B', '210495CA0A', '130505CA0A', '110625CA2A', '110625CA1A',
        '089183401A', '166183VA0A', '22636N4200', '11026AD200', '151965CB0A',
        '15196MB40B', '135335CA0A', 'B08026HL0B', '226A05CA0A', 'B08026HL0A',
        '243617990A', '2060241G00', '210141KC0A', '13050EN20B', '22131EN205',
        '206921HA0A', '140361VA0A', '101026MAHA', '21430AX30A', '166001VA0C',
        'A44015CA0A', '54588JK00A', '111405CA0B', '170404HK0A', '37120CG10A',
        '166121LA1A', '383430P001', '999BK00W20SS', '175206GP0A', '208B25CA0A',
        '2162632U00', '101026HN4A', '144505CA1C', '215147990B', '175225CA0A',
        '175215CA0A', '166385CA0A', '151975CA1A', '144995CA0B', '144995CA0A',
        '144985CA0B', '144985CA0A', 'CP1H', '16175MA70A', '16683MA70A',
        '14722EC00A', '16684MA70A', '16700MA70D', '14722AD200', '16680MA70A',
        '14719EC00A', 'NLLCWS0021', '1471943G02', '16682MA70A', '14035MA70A',
        '16681MA70A'
    ],

    "HIGH_PRESSURE_PUMP": [
        '17520HY00A', '11026JA00A', '166181KC0A', '1520865F0E', '140351KC0B',
        '16630HY00A', '140E26MR0A', '132311KC6E', '626606MR0A', '480014BA0C',
        '233004BB0B', '31935X420D', '3191829X0C', '165006MA2B', '165766MA1B',
        '149306FM0A', '21503DF40A', '140014BT0A', '999MPNS300P', '241106MA0A',
        '65616MA0A', '66830DF30A', '161194BB2A', '391014BB8E', '260606MR0A',
        '663184CC0A', '226805RB0A', 'D60104BA5A', '226931PM0A', '319351XF0C',
        '240236MR1B', '112204BB0A', '112544BB1A', '349356MA0A', '240126MR1C',
        '658406MR0A', '165546MA2B', '623106MR5A', '999M147C650', '240834MS1A',
        '166303JY0A', '499', '401', '166186RC0A', '166306RC0A', '170406MA0A',
        '173429TA0A', '565', '173436FK0A', '166384BB0A', '166331KC0A', '144656RC0B',
        '140356RC0A', '2148189900', '161756RC0A', '175206RC0B', '166306RC1A',
        '118106RC0A', '118126N200', '999MPL25500P', '1520865F1B', '999BK00W20Q',
        '210495NA3A'
    ],

    "IGNITION_COIL": ['224481HC0A'],

    "CONTROL_UNIT": [
        '23703HG00F', '237037JA1A', '170406HA0A', '17342CE800'
    ],

    "SENSOR_ASSEMBLY": ['250606FK0A', '250606FK5A'],

    "FUEL_PUMP_MOUNTING_UNIT": [
        '170406FK0A', '170407FV1A', '173423VA0A', '173433VA0A', '252307990A',
        '166351LA0A', 'K88206GG0A', 'LSU4.2', '22693CD700'
    ],

    "RADIAL_PISTON_PUMP": ['A6600MA70B'],

    "INJECTION_VALVE": [
        '144156RC0B', '402624GA0D', '101026RCAA', '210496RC2A', '144456RC0A',
        '144646RC0A', '206928H30A', '206925NA0A', '147226RC0B', '147226CA0A',
        '147196RC0A', '1518969F00', '150661KC0A', '151936RC0A', '150666RC0B',
        '291A96XK0A', 'XBGA16XK0B', '290Y63NA1A', '101026UAAE', '166006RC1A',
        'A44016RC0B', '290T56UM0A', '110264N200', '224016RC1E', '140016RC0A',
        '119A06UA0A', '150661HS0C', 'KE90090144', '295G36LS0A', '295B06UM9A',
        'KE90899933', 'KE90200045', '402624GA0C', '200A06UM0C', 'B08A26UM2A',
        '152089F60A', 'KE90100035', 'KE90090133', '132706RC2A', '22131EN215',
        '210496RC0A', '213045CB0A', '213045CB0B', '01225A0111', 'KE90090143',
        '131', '237036UA0A', '999BK00W20N0', '166006RC0A', '01223A2031',
        '01225A2011', '54588EN00A', 'B08A26UM0A', '39752ET02B', '383428E000',
        '92472N823A', '92474N823A', '14069JD00A', 'KE90090134', 'KE90399932',
        '101036UAAE', '110561KC0B', '1320700Q0A', '132075NA0C', '161756UM0A',
        '144656UM0A', '110446RC0A', '132706RC0B', '132706RC0A', '150662Y510',
        '135104BA0A', '150256RC0A', '166386RC1A', '175216RC0B', '140701LA0A',
        '11026EA20A', '150539HS0A', '121396RC0A', '01223A0121', '150661CA0A',
        '101', '123106UA0B', '12315EE000', '151926RC0A', '210106UA0A',
        '11022AD200', '30223ET00A', '391006UM2D', 'KE90299935', '01125A1051',
        '290Y66UM0A', '223406UL0A', '210496RC1A', '226A06UA0B', '226406UA0C',
        '164324KV0A', '2422889974', '1102601M02', '0155800411', '20825HV70A',
        '1643900Q1C', 'KE90090174', 'KE90299945', '166185CA0A', '166006TA0A',
        '140', '201', '199', '0122300Q0A', '40178JA000', '166386RC0A',
        '151976RC0A', '144116UA1A', '181', '1520865F0A', '295B06RA9A',
        '878446RR5B', '550446RA0B', 'AYBGDL2000JP', '92471N823A', '226936UA0A',
        '402626RA0A', '200A06RU0A', '400730L700', '226456RC1A', 'NLLCWS0024',
        'AY14140718', '166006MR0B', '132316RC5E', '150664W000'
    ]
}

# Optional: quick access variables (behavior unchanged)
GLOW_PLUG = PART_GROUPS["GLOW_PLUG"]
HIGH_PRESSURE_PUMP = PART_GROUPS["HIGH_PRESSURE_PUMP"]
IGNITION_COIL = PART_GROUPS["IGNITION_COIL"]
CONTROL_UNIT = PART_GROUPS["CONTROL_UNIT"]
SENSOR_ASSEMBLY = PART_GROUPS["SENSOR_ASSEMBLY"]
FUEL_PUMP_MOUNTING_UNIT = PART_GROUPS["FUEL_PUMP_MOUNTING_UNIT"]
RADIAL_PISTON_PUMP = PART_GROUPS["RADIAL_PISTON_PUMP"]
INJECTION_VALVE = PART_GROUPS["INJECTION_VALVE"]

# ============================================================
# DERIVED CONSTANTS
# ============================================================

# Parts where color-based logic should be ignored
CODES_IGNORE_COLOR = set(
    GLOW_PLUG
    + HIGH_PRESSURE_PUMP
    + IGNITION_COIL
    + CONTROL_UNIT
    + SENSOR_ASSEMBLY
    + FUEL_PUMP_MOUNTING_UNIT
    + INJECTION_VALVE
)

# Automatic sorting depending on the beginning of part number
PART_GROUP_PATTERNS = {
    "A6600": "INJECTOR",
    "13276": "INJECTOR",
    "13270": "INJECTOR",
    "14710": "INJECTOR",
    "16672": "INJECTOR",
    "14035": "Injection Valve",
    "21049": "Injection Valve",
    "16600": "Injection Valve",
    "14465": "Injection Valve",
    "16175": "Injection Valve",
    "16630": "High Pressure Pump",
    "17520": "High Pressure Pump",
    "16072": "Dosing module",
    "208S4": "Dosing module",
    "17040": "Fuel Pump Mounting Unit",
    "17342": "Fuel Pump Mounting Unit",
    "17343": "Fuel Pump Mounting Unit",
    "11065": "GLOW PLUG",
    "24009": "GLOW PLUG",
    "11067": "GLOW PLUG",
    "22790": "NOx sensor",
    "16618": "O-Ring",
    "16635": "O-Ring",
    "17521": "Supporting Disc",
    "17520": "Supporting Disc",
    "16612": "Supporting Disc",
    "25060": "Sensor Assembly",
    "23703": "CONTROL UNIT",
    "14722": "RAIL",
    "14735": "RAIL",
    "B08D0": "RAIL",
    "16683": "RAIL",
}

# Normalize free-text expected group names to standard form
EXPECTED_GROUP_NORMALIZATION = {
    "fuel pump assy": "fuel pump assembly",
    "fuel pump mount unit": "fuel pump mounting unit",
    "sensor assy": "sensor assembly",
    "o2 sensor": "oxygen sensor",
    "abs hydraulic": "hydraulic unit / abs",
    "control unit": "control unit",
    "controle unit": "control unit",
    "camshaft sensor": "camshaft position sensor",
    "brake master cyl": "brake master cylinder",
    "wheel speed": "wheel speed sensor",
}

# Reference No. month-letter → month-code mapping
MONTH_LETTER_TO_CODE = {
    "L": "Jan",  # 1
    "A": "Feb",  # 2
    "B": "Mar",
    "C": "Apr",
    "D": "May",
    "E": "Jun",
    "F": "Jul",
    "G": "Aug",
    "H": "Sep",
    "I": "Oct",
    "J": "Nov",
    "K": "Dec",
}



# %%
# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

def align_to_template(
    df: pd.DataFrame,
    template_path: str,
    column_mapping: dict | None = None,
    default_value=np.nan,
) -> pd.DataFrame:
    """
    Ensure df has at least all columns from template_path.
    - If template col exists in df: keep as is.
    - Else if mapping is provided and mapped source col exists in df: copy.
    - Else: create col with default_value.
    Returns df with columns ordered like the template.
    """
    template_header = pd.read_excel(template_path, nrows=0)
    template_cols = list(template_header.columns)

    column_mapping = column_mapping or {}

    for col in template_cols:
        if col in df.columns:
            continue

        # If we have an explicit mapping, use it
        src = column_mapping.get(col)
        if src and src in df.columns:
            df[col] = df[src]
        else:
            df[col] = default_value

    # Reorder columns to match template first; keep extra cols at the end
    ordered_existing = [c for c in template_cols if c in df.columns]
    extra_cols = [c for c in df.columns if c not in ordered_existing]
    return df[ordered_existing + extra_cols]


def normalize_bosch_part_no(pn):
    """
    Normalize Bosch part numbers:
    - Convert to string
    - Strip spaces
    - Remove trailing '.0' from Excel float artifacts.
    """
    if pd.isna(pn):
        return None
    s = str(pn).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.replace(" ", "")


def translate(df_main: pd.DataFrame,
              df_translation: pd.DataFrame,
              column1: str,
              column2: str) -> pd.DataFrame:
    """
    Rename columns in df_main based on a translation table.

    df_translation[column1] = current column names
    df_translation[column2] = new column names
    """
    current_columns = list(df_translation[column1])
    new_columns = list(df_translation[column2])

    df_main.rename(columns=dict(zip(current_columns, new_columns)), inplace=True)
    return df_main


def normalize_nissan_bosch_pn(pn):
    """
    Normalize Nissan/Bosch part numbers:
    - Remove spaces, hyphens, dots and non-alphanumerics
    - Keep only leading 8–12 chars (drop suffixes like KB, T00, etc.).
    """
    if pd.isna(pn):
        return None

    s = str(pn).replace(" ", "").replace("-", "").replace(".", "")
    s = re.sub(r"[^0-9A-Za-z]", "", s)

    match = re.match(r"([0-9A-Za-z]{8,12})", s)
    if match:
        return match.group(1)
    return s


def get_most_common_ezkl(df: pd.DataFrame, parts_no_prefix: str):
    """
    From EZKL mapping table, get the most common EZKL Name for a given prefix.
    """
    matching_rows = df[df["Bosch Parts No. Prefix"] == parts_no_prefix]
    if not matching_rows.empty:
        return matching_rows["EZKL Name"].mode()[0]
    return None


def get_letter_from_claim_date(claim_date: str) -> str:
    """
    Map claim date (string) to Nissan claim letter based on month.
    """
    month_to_letter = {
        1: "L", 2: "A", 3: "B", 4: "C", 5: "D", 6: "E",
        7: "F", 8: "G", 9: "H", 10: "I", 11: "J", 12: "K"
    }

    claim_date = pd.to_datetime(claim_date, errors="coerce")
    return month_to_letter[claim_date.month]


def get_OEM_date_month(objection_id: str) -> int:
    """
    Decode OEM month from the 3rd character of the objection_id.
    A = 1, B = 2, ..., Z = 26
    """
    alphabet_dict = {letter: index for index, letter in enumerate(string.ascii_uppercase, start=1)}
    third_character = objection_id[2].upper()
    return alphabet_dict.get(third_character, None)


def convert_to_date(value):
    """
    Convert various date encodings to pandas.Timestamp:
    - Excel serial numbers (int/float or numeric string)
    - 'yyyy/mm' strings
    - otherwise return NaT
    """
    try:
        # Excel serial number (int, float, or numeric string)
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
            base_date = datetime(1899, 12, 30)  # Excel's epoch
            days = int(value)
            return base_date + timedelta(days=days)

        # yyyy/mm formatted string
        if isinstance(value, str):
            return pd.to_datetime(value, format="%Y/%m", errors="coerce")

        return pd.NaT
    except Exception:
        return pd.NaT


def clean_vehicle_mfd(val):
    """
    Normalize Vehicle MFD:
    - NaN → NaT
    - 4-digit year (int/float/str) → yyyy-01-01
    - 'yyyy/mm' → first of that month
    - other strings → parsed by pandas
    """
    try:
        if pd.isna(val):
            return pd.NaT

        # numeric year like 2018 or 2018.0
        if isinstance(val, (int, float)):
            return pd.to_datetime(f"{int(val)}-01-01")

        val_str = str(val).strip()

        # 4-digit year
        if re.match(r"^\d{4}$", val_str):
            return pd.to_datetime(f"{val_str}-01-01")

        # 'yyyy/mm'
        if re.match(r"^\d{4}/\d{1,2}$", val_str):
            return pd.to_datetime(val_str, format="%Y/%m", errors="coerce")

        # fallback
        return pd.to_datetime(val_str, errors="coerce")

    except Exception:
        return pd.NaT


# ============================================================
# BUSINESS RULE FUNCTIONS
# ============================================================

def check_burden_ratio(row: pd.Series) -> int:
    """
    Check burden ratio correctness based on EZKL, MFD, contract dates, and special HDEV5 logic.
    Returns:
        0 if correct
        1 if incorrect
    Note: this function assumes row has columns:
          'Vehicle MFD', 'EZKL Name', 'Customer Parts No.', 'Burden Ratio',
          'Standard Burden Ratio', 'Current Burden Ratio',
          'New BR Date', 'SAP Date', '類別区分'
    """
    # Ensure "Vehicle MFD" is converted to a datetime object (yyyy/mm)
    mfd = pd.to_datetime(row["Vehicle MFD"], format="%Y/%m")

    if row["EZKL Name"] == "HDEV5":
        # Special cases based on Customer Parts No. and 類別区分
        if (
            "166001VA0A" in row["Customer Parts No."]
            or "166001VA0B" in row["Customer Parts No."]
            or "166001VA0C" in row["Customer Parts No."]
        ):
            if re.match(r"^H", row["類別区分"]):
                # Vehicle MFD date ranges for H-type
                if mfd <= datetime(2021, 6, 30):
                    return 0 if 2.4 <= row["Burden Ratio"] <= 3.4 else 1
                elif mfd >= datetime(2021, 7, 1):
                    return 0 if 49.5 <= row["Burden Ratio"] <= 50.5 else 1
                else:
                    return 1
            else:
                # Non-H 類別区分
                return 0 if 5 <= row["Burden Ratio"] <= 6 else 1

        elif "166005CA0A" in row["Customer Parts No."]:
            return 0 if 5 <= row["Burden Ratio"] <= 6 else 1

        elif (
            "166006MR0B" in row["Customer Parts No."]
            or "166006MR0C" in row["Customer Parts No."]
        ):
            return 0 if 5 <= row["Burden Ratio"] <= 6 else 1

        else:
            # Assign 0 but flag irregular case in "Irregular case BR"
            # (Mutation is kept for behavior compatibility, even if apply() won't persist it.)
            row["Irregular case BR"] = 1
            return 0

    if row["EZKL Name"] == "LUFT":
        return 0

    if pd.isna(row["EZKL Name"]):
        return 0

    # Default logic for non-HDEV5 / non-LUFT
    new_br_date = pd.to_datetime(row["New BR Date"], errors="coerce")
    sap_date = pd.to_datetime(row["SAP Date"], errors="coerce")

    if pd.isna(new_br_date):
        return 0 if row["Burden Ratio"] == row["Standard Burden Ratio"] else 1

    if sap_date < new_br_date:
        return 0 if row["Burden Ratio"] == row["Standard Burden Ratio"] else 1

    return 0 if row["Burden Ratio"] == row["Current Burden Ratio"] else 1


def generate_claim(row: pd.Series) -> int:
    """
    Claim decision (main logic).

    Returns 1 (claim) when any critical flag is raised and Right_Month / High Denied Paid Ratio are not blocking.
    """
    # Automatically return 0 if Right_Month is 1
    if row["Right_Month"] == 1:
        return 0

    # Automatically return 0 if High Denied Paid Ratio is 1
    if row["High Denied Paid Ratio"] == 1:
        return 0

    # Check the other conditions only if the above are false
    if (
        row["TCA Outlier EZKL"] == 1
        or row["BR Contract"] == 1
        or row["Outside_warranty_period"] == 1
        or row["HDEV6_countermeasure"] == 1
        or row["HDEV6_over_120000"] == 1
    ):
        return 1

    # Return 0 if none of the conditions are met
    return 0


def generate_claim_DPR(row: pd.Series) -> int:
    """
    Claim decision variation for DPR logic.

    Same as generate_claim, but does NOT consider 'High Denied Paid Ratio'.
    """
    # Automatically return 0 if Right_Month is 1
    if row["Right_Month"] == 1:
        return 0

    # Check the other conditions only if the above are false
    if (
        row["TCA Outlier EZKL"] == 1
        or row["BR Contract"] == 1
        or row["Outside_warranty_period"] == 1
        or row["HDEV6_countermeasure"] == 1
        or row["HDEV6_over_120000"] == 1
    ):
        return 1

    # Return 0 if none of the conditions are met
    return 0


def assign_group(part_number: str) -> str:
    """
    Assign a group based on the prefix of the part number.
    Uses PART_GROUP_PATTERNS, checking longest prefixes first.
    """
    if not isinstance(part_number, str):
        part_number = str(part_number)

    for prefix in sorted(PART_GROUP_PATTERNS, key=len, reverse=True):
        if part_number.startswith(prefix):
            return PART_GROUP_PATTERNS[prefix]
    return "Unknown"


def normalize_expected(value):
    """
    Normalize 'Expected group' free-text into a standard label.
    - Lowercases / strips
    - Cuts off after ';', ',' or ':' if present
    - Maps through EXPECTED_GROUP_NORMALIZATION
    """
    if not value:
        return "unassigned"

    val = str(value).strip().lower()

    # Remove known suffixes after delimiters (e.g., ';', ',', ':')
    for delimiter in [";", ",", ":"]:
        if delimiter in val:
            val = val.split(delimiter)[0].strip()
            break

    # Normalize using dictionary
    return EXPECTED_GROUP_NORMALIZATION.get(val, val)


def is_similar(a: str, b: str, threshold: int = 90) -> bool:
    """
    Fuzzy similarity between two strings using rapidfuzz.ratio.
    Returns True if similarity >= threshold.
    """
    return fuzz.ratio(a.lower(), b.lower()) >= threshold


def refno_to_month_code(ref_no):
    """
    Extract 3rd char of Reference No. and map to month code (Jan, Feb, ...).
    Uses MONTH_LETTER_TO_CODE.
    """
    if pd.isna(ref_no):
        return None

    s = str(ref_no).strip()
    if len(s) < 3:
        return None

    letter = s[2].upper()
    return MONTH_LETTER_TO_CODE.get(letter)



# %%
# ============================================================
# 3. PATHS & FILE LOCATIONS
# ============================================================

# Convert claim date to compact yymm format (input: yyyy/mm/dd)
date_obj = datetime.strptime(CLAIM_DATE, "%Y/%m/%d")
DATE_YYMM = date_obj.strftime("%y%m")     # e.g., 2025/10/01 → "2510"
DATE_YYYYMM = date_obj.strftime("%Y%m")   # sometimes needed


# Base SharePoint directory
ROOT_DIR = r"\\bosch.com\DfsRB\DfsJP\DIV\PS\z_Collabo\0215_QMM_JP3_Share\Claim_WBS\4.Warranty_Info\11.Nissan_異議"


# Path to unlabeled Nissan objection Excel file
file_path = fr"{ROOT_DIR}\20{DATE_YYMM}\nissan_{DATE_YYMM}_GB.xlsx"

# Path to save predicted labels / outputs
result_file_path = fr"{ROOT_DIR}\20{DATE_YYMM}"


# Replacement dictionary for product names and EZKL corrections
REPLACEMENTS = {
    "HDEV": "HDEV5",             # Unspecified HDEV assumed to be HDEV5
    "EKP/T": "EKPT",
    "EGT-PC": "EGT-PC(DM3.4)",   # (or EGT-PC(MIXER) depending on rule; unchanged here)
    "EV(Do)": "EV",
}

# ============================================================
# 3.B POWER BI TEMPLATE / SCHEMA CONFIG
# ============================================================

# Template used only for column/schema alignment
AI_TEMPLATE_PATH = fr"{ROOT_DIR}\AI_validated_claims_template.xlsx"

# Historical clean file (non-aggregated)
AI_CLAIMS_CLEAN_PATH = fr"{ROOT_DIR}\AI_validated_claims_clean.xlsx"

# Aggregated file consumed by Power BI
AI_CLAIMS_AGG_PATH = fr"{ROOT_DIR}\AI_validated_claims.xlsx"

# Mapping from old Power BI column names → new refactor column names
# Extend this dict if you find more legacy Japanese columns later.
COLUMN_MAPPING = {
    "判定.1": "claim",   # old column used by the model → new "claim"
}


# %%
# ============================================================
# 4. DATA LOADING
# ============================================================

# ------------------------------------------------------------
# 4.1 PS DATA (GLOBAL, SLOW TO LOAD)
# ------------------------------------------------------------
# Note:
# PS data takes a long time to load (~7 min). When iterating on
# logic below, you can comment this block out *after* it is in
# memory, as long as you don't restart the kernel.

df_ps = pd.read_excel(
    r"\\bosch.com\dfsrb\DfsJP\DIV\PS\QMC\All\06.QMM_QMD\60.Data_Base\2.Warranty_data\PS_Database.xlsm",
    sheet_name="PS_Data",
    header=1,
)

# Translation sheet for PS columns
df_ps_translation = pd.read_excel(
    r"\\bosch.com\dfsrb\DfsJP\DIV\PS\QMC\All\06.QMM_QMD\60.Data_Base\2.Warranty_data\PS_Database.xlsm",
    sheet_name="Translation",
    header=0,
)
df_ps = translate(df_ps, df_ps_translation, column1="PS_Data Columns", column2="Translated Version")

# --- Master EZKL lookup from full PS database (no Nissan filter) ---

# Normalize Bosch part numbers
df_ps["Bosch Parts No. norm"] = df_ps["Bosch Parts No."].apply(normalize_bosch_part_no)
df_ps["Bosch Prefix 10"] = df_ps["Bosch Parts No. norm"].str[:10]

# Build mapping: prefix -> most common EZKL Name
ezkl_lookup = (
    df_ps
    .dropna(subset=["Bosch Prefix 10", "EZKL Name"])
    .groupby("Bosch Prefix 10")["EZKL Name"]
    .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0])
    .reset_index()
    .rename(columns={"EZKL Name": "EZKL_from_PS"})
)

# Ensure SAP Date is datetime before any filtering
df_ps["SAP Date"] = pd.to_datetime(df_ps["SAP Date"], errors="coerce")

# Filter for Nissan-related data
df_ps_nissan = df_ps[df_ps["OEM Name"] == "日産"]
df_ps_nissan = df_ps_nissan[df_ps_nissan["Key No."] != "M"]

cutoff = pd.Timestamp("2021-01-01")
df_ps_nissan = df_ps_nissan[df_ps_nissan["SAP Date"] >= cutoff]

df_ps_nissan["Objection ID"] = df_ps_nissan["Reference No."].str[:8]
df_ps_nissan["Bosch Parts No. Prefix"] = df_ps_nissan["Bosch Parts No."].str[:10]

# Exclude irrelevant cases
df_ps_nissan = df_ps_nissan[
    ~df_ps_nissan["Bosch Parts Name"].isin(["CP1H recall", "新負担割合による遡及精算分", "ECM　キャンペーン費用"])
]
df_ps_nissan = df_ps_nissan[~df_ps_nissan["EZKL Name"].str.contains(r"\(S\)")]

# Replace EZKL Names based on replacement dictionary (from config)
df_ps_nissan["EZKL Name"] = df_ps_nissan["EZKL Name"].replace(REPLACEMENTS)

# Drop unnecessary columns
df_ps_nissan = df_ps_nissan.drop(
    columns=["Product Code(DS)", "Product Code", "Sequence No.", "c3", "Division"]
)

# Drop duplicate Reference No., keeping most recent SAP Date
df_sorted = df_ps_nissan.sort_values(by=["Reference No.", "SAP Date"], ascending=[True, False])
df_ps_nissan = df_sorted.drop_duplicates(subset="Reference No.", keep="first")

# Filter to exclude the current claim month from PS database
df_ps_nissan["SAP Date"] = pd.to_datetime(df_ps_nissan["SAP Date"], errors="coerce")
df_ps_nissan = df_ps_nissan.loc[df_ps_nissan["SAP Date"] < claim_date_ts]

# Convert installation date to datetime
df_ps_nissan["Parts Warranty Installation Date"] = df_ps_nissan[
    "Parts Warranty Installation Date"
].apply(convert_to_date)


# ------------------------------------------------------------
# 4.2 UNTRAINED (NEW) NISSAN DATA
# ------------------------------------------------------------
df_new = pd.read_excel(file_path, sheet_name=SHEET_PS)

# Translation sheet for new Nissan objection file
df_new_translation = pd.read_excel(
    r"\\bosch.com\DfsRB\DfsJP\DIV\PS\z_Collabo\0215_QMM_JP3_Share\Claim_WBS\4.Warranty_Info\11.Nissan_異議\Nissan_異議申請リスト_translated_forAI.xlsx"
)
df_new = translate(df_new, df_new_translation, column1="Nissan Columns", column2="Translated Version")

# Filter relevant divisions
# df_new = df_new.loc[df_new["Parts Distinction"] == 1]
df_new = df_new.loc[df_new["Division"].isin(["PS(GS)", "PS(DS)", "P"])]
# "P" is actually an error resulting from the macros, this may be fixed in the near future

# Extract key columns
df_new["Objection ID"] = df_new["Reference No."].str[:8]
df_new["Bosch Parts No. Prefix"] = df_new["Bosch Parts No."].str[:10]
df_new["EZKL Name"] = df_new["Bosch Parts No. Prefix"].apply(
    lambda x: get_most_common_ezkl(df_ps_nissan, x)
)

# Normalize Bosch Parts Name to lowercase
df_new["Bosch Parts Name"] = df_new["Bosch Parts Name"].fillna("").str.lower()

# Standardize SAP Date name/type
df_new["SAP Date"] = df_new["EDP Date"]
df_new["SAP Date"] = pd.to_datetime(df_new["SAP Date"], errors="coerce")

# Exclude irrelevant cases
df_new = df_new[
    ~df_new["Bosch Parts Name"].isin(["CP1H recall", "新負担割合による遡及精算分", "ECM　キャンペーン費用"])
]


# ------------------------------------------------------------
# 4.3 BURDEN RATIO CONTRACT DATA
# ------------------------------------------------------------
df_burden = pd.read_excel(
    r"\\BOSCH.COM\DfsRB\DfsJP\DIV\PS\z_Collabo\0173_PSQMC_123\PSQMC_Share\2_General\Quality_data\Q_Reporting\01 GS-JP External defect cost\2 Customer別 要求事項\顧客別負担割合一覧表.xlsx",
    sheet_name="2021",
    header=4,
)

# Translate columns
df_burden.rename(
    columns={
        "製品名\n（EZKL名称）": "EZKL Name",
        "製品コード\n(EZKL)": "EZKL (Product Class)",
        "基準負担率\nBosch": "Standard Burden Ratio",
        "現状負担率\nBosch": "Current Burden Ratio",
        "適用開始日": "New BR Date",
        "変更後負担率有効期限": "New BR Expiry Date",
        "備考1": "Remarks 1",
        "備考2": "Remarks 2",
        "最終更新日/確認日": "Last Updated Date",
    },
    inplace=True,
)

# Nissan-only rows
df_burden_nissan = df_burden.loc[df_burden["メーカー"] == "NISSAN"]

# Drop unnecessary columns
df_burden_nissan.drop(
    columns=["Unnamed: 13", "メーカー", "代表品番", "負担率決定合意書保存先リンク"],
    inplace=True,
)

# Exclude irrelevant cases for BR logic
df_burden_nissan = df_burden_nissan[
    ~(
        (df_burden_nissan["EZKL Name"] == "LS")
        & (df_burden_nissan["Current Burden Ratio"] == 1.5)
    )
]
df_burden_nissan = df_burden_nissan[
    ~(
        (df_burden_nissan["EZKL Name"] == "HDEV5")
        & ~(df_burden_nissan["Current Burden Ratio"] == "5.5\n(一部50%)")
    )
]


# ------------------------------------------------------------
# 4.4 OBJECTION DATA (HISTORICAL)
# ------------------------------------------------------------
# Kept for reference and possible future ML usage.
df_obj_nissan = pd.read_excel(
    r"\\BOSCH.COM\DfsRB\DfsJP\DIV\PS\z_Collabo\0215_QMM_JP3_Share\Claim_WBS\4.Warranty_Info\異議申請状況確認リスト_PC.xlsx",
    sheet_name="Nissan",
    header=1,
)

df_obj_translation = pd.read_excel(
    r"\\BOSCH.COM\DfsRB\DfsJP\DIV\PS\z_Collabo\0215_QMM_JP3_Share\Claim_WBS\4.Warranty_Info\異議申請状況確認リスト_PC.xlsx",
    sheet_name="Translation",
    header=0,
)
df_obj_nissan = translate(df_obj_nissan, df_obj_translation, column1="Nissan Columns", column2="Translated Version")

df_obj_nissan.rename(
    columns={"Return Amount": "Saved Amount", "Return Amount1": "Saved Amount1"},
    inplace=True,
)

df_excluded_nissan = df_obj_nissan[df_obj_nissan["Status"] == "申請中"]
df_obj_nissan = df_obj_nissan[df_obj_nissan["Status"].isin(["却下", "受理"])]

df_obj_nissan["Objection ID"] = df_obj_nissan["Reference No."].str[:8]


# %%
# ============================================================
# 5. CONTROL UNIT NORMALIZATION + MERGES
# ============================================================

# Normalize Bosch Parts Name to lower case for matching
name_col = df_ps_nissan["Bosch Parts Name"].fillna("").str.lower()

# Ensure EZKL column exists (safety, even though it should already be present)
if "EZKL Name" not in df_ps_nissan.columns:
    df_ps_nissan["EZKL Name"] = None

# Assign EZKL "Control Unit" to any PS rows whose name contains "control unit"
df_ps_nissan.loc[name_col.str.contains("control unit"), "EZKL Name"] = "Control Unit"

# Merge PS Nissan data with Burden Ratio table (no Control Unit row yet → preserves original behavior)
df_ps_nissan = df_ps_nissan.merge(
    df_burden_nissan[["EZKL Name", "Standard Burden Ratio", "Current Burden Ratio", "New BR Date"]],
    on="EZKL Name",
    how="left",
)

# Add Control Unit row to burden table if not already present
if "Control Unit" not in df_burden_nissan["EZKL Name"].values:
    control_unit_row = pd.DataFrame(
        [
            {
                "EZKL Name": "Control Unit",
                "Standard Burden Ratio": 0.5,
                "Current Burden Ratio": 0.5,
                "New BR Date": pd.to_datetime("2021-01-01"),
            }
        ]
    )
    df_burden_nissan = pd.concat([df_burden_nissan, control_unit_row], ignore_index=True)

# Merge objection status into PS Nissan data
df_ps_nissan = df_ps_nissan.merge(
    df_obj_nissan[["Objection ID", "Total Claimed Amount", "Status"]],
    on=["Objection ID", "Total Claimed Amount"],
    how="left",
)

# Merge Burden Ratio into new (untrained) Nissan data
df_new = df_new.merge(
    df_burden_nissan[["EZKL Name", "Standard Burden Ratio", "Current Burden Ratio", "New BR Date"]],
    on="EZKL Name",
    how="left",
)


# %%
# ============================================================
# 6. EXTRA DATA CURATION
#    - Duplicate resolution
#    - Missing values treatment
# ============================================================

# ------------------------------------------------------------
# 6.1 Merging Duplicates Issue
# ------------------------------------------------------------

# Translate Status values (JP → EN)
df_ps_nissan["Status"] = df_ps_nissan["Status"].map({"却下": "Rejected", "受理": "Accepted"})

# Count occurrences of each Objection ID
obj_id_counts = df_ps_nissan["Objection ID"].value_counts()

# Temporary status to distinguish NaN rows
df_ps_nissan["Status_temp"] = df_ps_nissan.apply(
    lambda row: f"NaN_{row.name}" if pd.isna(row["Status"]) else row["Status"],
    axis=1,
)

# Objection IDs that appear exactly twice
obj_ids_twice = obj_id_counts[obj_id_counts == 2].index
obj_no_2 = df_ps_nissan[df_ps_nissan["Objection ID"].isin(obj_ids_twice)]

# Among those, IDs with more than one distinct Status_temp (i.e., conflicting statuses)
status_counts = obj_no_2.groupby("Objection ID")["Status_temp"].nunique()
conflict_ids = status_counts[status_counts > 1].index

# For conflicting IDs, get the earliest SAP Date row
OBJ_SAP = df_ps_nissan[df_ps_nissan["Objection ID"].isin(conflict_ids)].sort_values(
    by=["Objection ID", "SAP Date"],
    ascending=True,
)

OBJ_SAP_sorted = OBJ_SAP.sort_values(by=["Objection ID", "SAP Date"], ascending=True)
OBJ_SAP_order = OBJ_SAP_sorted.drop_duplicates(subset=["Objection ID"], keep="first")

# Attach earliest SAP Date per conflicting Objection ID
earliest = OBJ_SAP_order[["Objection ID", "SAP Date"]].rename(
    columns={"SAP Date": "Earliest SAP Date"}
)
df_ps_nissan = df_ps_nissan.merge(earliest, on="Objection ID", how="left")

# Drop later SAP Date rows for those conflicting IDs
mask_drop = (
    df_ps_nissan["Earliest SAP Date"].notna()
    & (df_ps_nissan["SAP Date"] > df_ps_nissan["Earliest SAP Date"])
)
df_ps_nissan = df_ps_nissan[~mask_drop].drop(columns=["Earliest SAP Date"])

# Final dedupe: keep last record per (Objection ID, Total Claimed Amount)
df_nissan_sorted = df_ps_nissan.sort_values(
    by=["Objection ID", "Total Claimed Amount", "SAP Date"],
    ascending=True,
)
df_ps_nissan = df_nissan_sorted.drop_duplicates(
    subset=["Objection ID", "Total Claimed Amount"],
    keep="last",
)

# Clean up temp column
df_ps_nissan.drop(columns=["Status_temp"], inplace=True)


# ------------------------------------------------------------
# 6.2 Treating Missing Values
# ------------------------------------------------------------

# Mean registration-to-failure time (for both PS and new data fallback)
mean_reg_fal_time = (
    df_ps_nissan["Vehicle Failure Date"].mean()
    - df_ps_nissan["Vehicle Registration Date"].mean()
)

# Fill missing Vehicle MFD in PS data
for index, row in df_ps_nissan[df_ps_nissan["Vehicle MFD"].isna()].iterrows():
    if pd.notna(row["Vehicle Registration Date"]):
        df_ps_nissan.at[index, "Vehicle MFD"] = row["Vehicle Registration Date"]
    else:
        df_ps_nissan.at[index, "Vehicle MFD"] = row["Vehicle Failure Date"] - mean_reg_fal_time

# Fill missing Vehicle MFD in new data
# 1st rule: use Vehicle Registration Date year when available
df_new.loc[df_new["Vehicle MFD"].isna(), "Vehicle MFD"] = df_new["Vehicle Registration Date"].dt.year

# 2nd rule: for remaining NaN, approximate with Failure Date minus mean lag
df_new.loc[df_new["Vehicle MFD"].isna(), "Vehicle MFD"] = (
    df_new.loc[df_new["Vehicle MFD"].isna(), "Vehicle Failure Date"] - mean_reg_fal_time
)

# Normalize Vehicle MFD in df_new to proper datetime
df_new["Vehicle MFD"] = df_new["Vehicle MFD"].apply(clean_vehicle_mfd)
df_new["Vehicle MFD"] = pd.to_datetime(df_new["Vehicle MFD"])

# Fill missing Passed Month in PS data
mean_passed_month = df_ps_nissan["Passed Month"].mean()
df_ps_nissan["Passed Month"].fillna(mean_passed_month, inplace=True)


# %%
# ============================================================
# 7. FEATURE ENGINEERING & SELECTION
# ============================================================

# ------------------------------------------------------------
# 7.1 Fixing Data Types
# ------------------------------------------------------------

df_ps_nissan["SAP Date"] = pd.to_datetime(df_ps_nissan["SAP Date"], format="%Y-%m-%d")
df_ps_nissan["New BR Date"] = pd.to_datetime(df_ps_nissan["New BR Date"])
df_ps_nissan["Parts Warranty Installation Date"] = pd.to_datetime(
    df_ps_nissan["Parts Warranty Installation Date"]
)

df_burden_nissan["New BR Date"] = pd.to_datetime(df_burden_nissan["New BR Date"])

df_new["Vehicle MFD"] = pd.to_datetime(df_new["Vehicle MFD"])
df_new["SAP Date"] = pd.to_datetime(df_new["SAP Date"])
df_new["New BR Date"] = pd.to_datetime(df_new["New BR Date"])
df_new["Download Date"] = pd.to_datetime(df_new["Download Date"])
df_new["Parts Warranty Installation Date"] = pd.to_datetime(
    df_new["Parts Warranty Installation Date"]
)

# (CLAIM_DATE already used to define claim_date_ts earlier; no need to redefine)


# ------------------------------------------------------------
# 7.2 Control-unit EZKL patch (blank ECU → ECU-PC/GS)
# ------------------------------------------------------------

mask_blank_control_unit = (
    df_new["EZKL Name"].isna()
    & df_new["Bosch Parts Name"].str.lower().str.contains("control unit", na=False)
)
df_new.loc[mask_blank_control_unit, "EZKL Name"] = "ECU-PC/GS"
df_new.loc[mask_blank_control_unit, "Original_EZKL_Name"] = "ECU-PC/GS"


# ------------------------------------------------------------
# 7.3 Global stats, Claim Status, and DPR (Denied Paid Ratio)
# ------------------------------------------------------------

# Overall TCA stats
std_amount = df_ps_nissan["Total Claimed Amount"].std()
mean_amount = df_ps_nissan["Total Claimed Amount"].mean()
sigma_1_5_above = mean_amount + std_amount * 1.5
sigma_1_above = mean_amount + std_amount

# Domestic / Overseas stats
std_amount_dom = df_ps_nissan.loc[
    df_ps_nissan["Domestic/Overseas"] == "1", "Total Claimed Amount"
].std()
mean_amount_dom = df_ps_nissan.loc[
    df_ps_nissan["Domestic/Overseas"] == "1", "Total Claimed Amount"
].mean()
sigma_1_above_dom = mean_amount_dom + std_amount_dom

std_amount_over = df_ps_nissan.loc[
    df_ps_nissan["Domestic/Overseas"] == "2", "Total Claimed Amount"
].std()
mean_amount_over = df_ps_nissan.loc[
    df_ps_nissan["Domestic/Overseas"] == "2", "Total Claimed Amount"
].mean()
sigma_1_above_over = mean_amount_over + std_amount_over

# 12-month PS window (currently unused, kept for potential TS)
temp_df = df_ps_nissan.loc[
    (df_ps_nissan["SAP Date"] >= claim_date_ts - pd.DateOffset(months=12))
    & (df_ps_nissan["SAP Date"] <= claim_date_ts)
]

# Monthly stats in new data (not used downstream, but kept)
df_new["Year_SAP"] = df_new["SAP Date"].dt.year
df_new["Month_SAP"] = df_new["SAP Date"].dt.month
std_monthly_EZKL_new = df_new.groupby(
    ["EZKL Name", "Year_SAP", "Month_SAP"]
)["Total Claimed Amount"].std()
mean_monthly_EZKL_new = df_new.groupby(
    ["EZKL Name", "Year_SAP", "Month_SAP"]
)["Total Claimed Amount"].mean()
sigma_1_above_monthly_EZKL_new = mean_monthly_EZKL_new + std_monthly_EZKL_new

# Claim Status mapping
df_ps_nissan.loc[:, "Claim Status"] = df_ps_nissan["Status"].replace(
    {"Accepted": "Denied Claim", "Rejected": "Denied Paid Claim"}
)
df_ps_nissan.loc[:, "Claim Status"] = df_ps_nissan["Claim Status"].fillna("Paid Claim")

denied_paid_counts = (
    df_ps_nissan.loc[
        df_ps_nissan["Claim Status"] == "Denied Paid Claim",
        ["EZKL Name", "Claim Status"],
    ]
    .groupby("EZKL Name")["Claim Status"]
    .count()
    .reset_index(name="Denied Paid Count")
)

denied_counts = (
    df_ps_nissan.loc[
        df_ps_nissan["Claim Status"] == "Denied Claim",
        ["EZKL Name", "Claim Status"],
    ]
    .groupby("EZKL Name")["Claim Status"]
    .count()
    .reset_index(name="Denied Count")
)

ratio_df = pd.merge(denied_paid_counts, denied_counts, on="EZKL Name", how="outer").fillna(0)
ratio_df["Denied Paid Ratio"] = np.where(
    (ratio_df["Denied Count"] == 0) & (ratio_df["Denied Paid Count"] == 0),
    0,
    ratio_df["Denied Paid Count"]
    / (ratio_df["Denied Count"] + ratio_df["Denied Paid Count"]),
)

# Month-letter for current claim date
current_letter = get_letter_from_claim_date(claim_date_ts)


# ------------------------------------------------------------
# 7.4 Special handling for new HDEV6 part (Customer P/N 166006RC1C)
# ------------------------------------------------------------

df_new["Standard Burden Ratio"] = np.where(
    (df_new["Customer Parts No."] == "166006RC1C") & (pd.isna(df_new["EZKL Name"])),
    df_new.loc[df_new["EZKL Name"] == "HDEV6", "Standard Burden Ratio"].iloc[:1],
    df_new["Standard Burden Ratio"],
)

df_new["Current Burden Ratio"] = np.where(
    (df_new["Customer Parts No."] == "166006RC1C") & (pd.isna(df_new["EZKL Name"])),
    df_new.loc[df_new["EZKL Name"] == "HDEV6", "Current Burden Ratio"].iloc[:1],
    df_new["Current Burden Ratio"],
)

df_new["New BR Date"] = np.where(
    (df_new["Customer Parts No."] == "166006RC1C") & (pd.isna(df_new["EZKL Name"])),
    df_new.loc[df_new["EZKL Name"] == "HDEV6", "New BR Date"].iloc[:1],
    df_new["New BR Date"],
)

df_new["EZKL Name"] = np.where(
    (df_new["Customer Parts No."] == "166006RC1C") & (pd.isna(df_new["EZKL Name"])),
    "HDEV6",
    df_new["EZKL Name"],
)


# ------------------------------------------------------------
# 7.5 FALLBACK: Fill remaining EZKL from full PS database
# ------------------------------------------------------------

# 1) Normalize Bosch P/N in df_new
df_new["Bosch Parts No. norm"] = df_new["Bosch Parts No."].apply(normalize_nissan_bosch_pn)
df_new["Bosch Prefix 10"] = df_new["Bosch Parts No. norm"].str[:10]

# ezkl_lookup already built in PS loading section; reuse it here.

# 2) Fallback merge
df_new = df_new.merge(ezkl_lookup, how="left", on="Bosch Prefix 10")

# 3) Only fill where EZKL is still NaN
df_new["EZKL Name"] = df_new["EZKL Name"].fillna(df_new["EZKL_from_PS"])

# 4) Cleanup helper column
df_new.drop(columns=["EZKL_from_PS"], inplace=True)

print("Remaining EZKL NaN after fallback:", df_new["EZKL Name"].isna().sum())


# ------------------------------------------------------------
# 7.6 Outlier flags and date-based features
# ------------------------------------------------------------

# Outlier flags on TCA
df_new["TCA Outlier15"] = np.where(df_new["Total Claimed Amount"] > sigma_1_5_above, 1, 0)
df_new["TCA Outlier1"] = np.where(df_new["Total Claimed Amount"] > sigma_1_above, 1, 0)

df_new["TCA Outlier_dom"] = np.where(
    (df_new["Domestic/Overseas"] == "2")
    & (df_new["Total Claimed Amount"] > sigma_1_above_dom),
    1,
    0,
)

df_new["TCA Outlier_over"] = np.where(
    (df_new["Domestic/Overseas"] == "2")
    & (df_new["Total Claimed Amount"] > sigma_1_above_over),
    1,
    0,
)

# Time deltas and OEM month decoding
df_new["Days MFD SAP"] = (df_new["SAP Date"] - df_new["Vehicle MFD"]).dt.days
df_new["Days MFD Failure"] = (df_new["Vehicle Failure Date"] - df_new["Vehicle MFD"]).dt.days
df_new["MFD Year"] = df_new["Vehicle MFD"].dt.year
df_new["OEM Date Month"] = df_new["Objection ID"].apply(get_OEM_date_month)


# ------------------------------------------------------------
# 7.7 Burden Ratio contract check (BR Contract)
# ------------------------------------------------------------

def safe_check_burden_ratio(row):
    try:
        return check_burden_ratio(row)
    except TypeError:
        return 0


df_new["Irregular case BR"] = 0
df_new["BR Contract"] = df_new.apply(safe_check_burden_ratio, axis=1)

# Preserve EZKL at this stage for later hybrid flags
df_new["Original_EZKL_Name"] = df_new["EZKL Name"]

# Hybrid EZKL label
df_new["EZKL_H"] = df_new.apply(
    lambda row: f"{row['Original_EZKL_Name']} (H)"
    if isinstance(row["類別区分"], str) and re.match(r"^H", row["類別区分"])
    else row["Original_EZKL_Name"],
    axis=1,
)


# ------------------------------------------------------------
# 7.8 EZKL statistics for TCA Outlier EZKL
# ------------------------------------------------------------

# Cleanup any old stats columns if notebook re-run
stats_cols_to_drop = [
    "Mean_TCA",
    "Std_TCA",
    "Mean_Plus_Std",
    "Mean_TCA_x",
    "Std_TCA_x",
    "Mean_Plus_Std_x",
    "Mean_TCA_y",
    "Std_TCA_y",
    "Mean_Plus_Std_y",
]
df_new = df_new.drop(
    columns=[c for c in stats_cols_to_drop if c in df_new.columns],
    errors="ignore",
)

# Mean/std per EZKL
std_summary = (
    df_new.groupby("EZKL Name")["Total Claimed Amount"]
    .agg(Mean_TCA="mean", Std_TCA="std")
    .reset_index()
)
std_summary["Mean_Plus_Std"] = std_summary["Mean_TCA"] + std_summary["Std_TCA"]

print("std_summary columns:", std_summary.columns.tolist())

# Merge stats into df_new
df_new = df_new.merge(std_summary, on="EZKL Name", how="left")
print("Has Mean_Plus_Std in df_new?:", "Mean_Plus_Std" in df_new.columns)

df_new["TCA Outlier EZKL"] = np.where(
    df_new["Total Claimed Amount"] > df_new["Mean_Plus_Std"], 1, 0
)


# ------------------------------------------------------------
# 7.9 HDEV6-specific flags and main-part exclusion
# ------------------------------------------------------------

HDEV6_MAIN_PART_EXCLUDED = ["166007JA1A"]
mask_hdev6_main_excl = df_new["Customer Parts No."].isin(HDEV6_MAIN_PART_EXCLUDED)

df_new["HDEV6_CM"] = np.where(
    (df_new["EZKL Name"] == "HDEV6")
    & (df_new["Vehicle MFD"] >= pd.to_datetime("2023-04-01")),
    1,
    0,
)

df_new["HDEV6_countermeasure"] = np.where(
    (df_new["EZKL Name"] == "HDEV6")
    & (df_new["Vehicle MFD"] >= pd.to_datetime("2023-04-01"))
    & (df_new["TCA Outlier EZKL"] == 1),
    1,
    0,
)

df_new["HDEV6_over_120000"] = np.where(
    (df_new["EZKL Name"] == "HDEV6")
    & (df_new["Total Claimed Amount"] >= 120000)
    & (df_new["Total Claimed Amount"] <= 200000),
    1,
    0,
)

# Apply main-part exclusion (HDEV6 flags disabled for these parts)
df_new.loc[mask_hdev6_main_excl, ["HDEV6_countermeasure", "HDEV6_over_120000"]] = 0

# BR Contract logic for excluded main parts (must be exactly 50%)
df_new.loc[mask_hdev6_main_excl, "BR Contract"] = 0
br_ok_mask = df_new["Burden Ratio"] == 50
df_new.loc[mask_hdev6_main_excl & (~br_ok_mask), "BR Contract"] = 1


# ------------------------------------------------------------
# 7.10 High Denied Paid Ratio (EZKL-level)
# ------------------------------------------------------------

deny_cols_to_drop = [
    "Denied Paid Ratio",
    "Denied Count",
    "Denied Paid Count",
    "Denied Paid Ratio_x",
    "Denied Count_x",
    "Denied Paid Count_x",
    "Denied Paid Ratio_y",
    "Denied Count_y",
    "Denied Paid Count_y",
]
df_new = df_new.drop(
    columns=[c for c in deny_cols_to_drop if c in df_new.columns],
    errors="ignore",
)

df_new = df_new.merge(
    ratio_df[["EZKL Name", "Denied Paid Ratio", "Denied Count", "Denied Paid Count"]],
    on="EZKL Name",
    how="left",
)

df_new["Denied Paid Ratio"] = df_new["Denied Paid Ratio"].fillna(0)
df_new["Num Objected"] = df_new["Denied Count"] + df_new["Denied Paid Count"]

df_new["High Denied Paid Ratio"] = np.where(
    df_new["Num Objected"] >= 10,
    np.where(df_new["Denied Paid Ratio"] >= 0.90, 1, 0),
    0,
)


# ------------------------------------------------------------
# 7.11 Warranty period and month checks
# ------------------------------------------------------------

df_new["期間"] = pd.to_numeric(df_new["期間"], errors="coerce").fillna(100)

df_new["period_m_difference"] = (
    (df_new["Vehicle Failure Date"].dt.year - df_new["Parts Warranty Installation Date"].dt.year) * 12
    + (df_new["Vehicle Failure Date"].dt.month - df_new["Parts Warranty Installation Date"].dt.month)
)

df_new["Outside_warranty_period"] = np.where(
    pd.isna(df_new["Parts Warranty Installation Date"]),
    0,
    np.where(df_new["period_m_difference"] > df_new["期間"], 1, 0),
)

df_new["Right_Month"] = df_new["Reference No."].astype(str).apply(
    lambda row: 0 if row[2] == current_letter else 1
)

df_new["Irr. Month"] = df_new["Reference No."].apply(refno_to_month_code)
print(df_new[["Reference No.", "Irr. Month"]].head(20))


# ------------------------------------------------------------
# 7.12 Hybrid label for Power BI (duplicate, kept intentionally)
# ------------------------------------------------------------

df_new["Hybrid_specification_EZKL"] = np.where(
    (df_new["Original_EZKL_Name"] == "HDEV5")
    & (df_new["類別区分"].str[:1] == "H"),
    "HDEV5 (H)",
    df_new["Original_EZKL_Name"],
)


# ------------------------------------------------------------
# 7.13 Irregular-case flag and EZKL group count
# ------------------------------------------------------------

df_new["Irregular_case"] = np.where(
    (df_new["Irregular case BR"] == 1) | (df_new["Right_Month"] == 1),
    1,
    0,
)

if "Group Count" in df_new.columns:
    df_new = df_new.drop(columns=["Group Count"])

parts_count = df_new["EZKL Name"].value_counts().reset_index()
parts_count.columns = ["EZKL Name", "Group Count"]

df_new = df_new.join(parts_count.set_index("EZKL Name"), on="EZKL Name", how="left")


# %%
# ============================================================
# 8. APPLY RULE-BASED MODEL & SAVE RESULTS
# ============================================================

# ------------------------------------------------------------
# 8.1 Apply claim logic
# ------------------------------------------------------------

df_new["claim"] = df_new.apply(generate_claim, axis=1)
df_new["claim_DPR"] = df_new.apply(generate_claim_DPR, axis=1)

# Claim date for Power BI filtering
df_new["AI_DATE"] = claim_date_ts  # from CONFIG section


# Convert burden ratio into decimal (0–1)
df_new["Burden Ratio Decimal"] = df_new["Burden Ratio"] / 100.0


# ------------------------------------------------------------
# 8.2 Subpart validation filter
# ------------------------------------------------------------

def apply_subpart_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Subpart consistency check:
    - For Parts Distinction = 2 (subparts), compare Bosch Parts Name (normalized)
      vs part-number-based group (assign_group).
    - If mismatch → 'To object?', else 'OK'.
    - Propagate 'To object?' to Distinction = 1 rows sharing the same Reference No.
    """
    df = df.copy()

    # Step 1: work on subparts only
    df2 = df[df["Parts Distinction"] == 2].copy()

    status_list = []
    for _, row in df2.iterrows():
        part_number = str(row.get("Customer Parts No.", "")).strip()
        raw_expected = row.get("Bosch Parts Name", "")
        normalized_expected = normalize_expected(raw_expected)
        computed_group = assign_group(part_number).lower()

        similar = is_similar(normalized_expected, computed_group)

        if similar:
            status_list.append("OK")
        else:
            status_list.append("To object?")

    # Step 2: assign Subpart for Distinction = 2 rows
    df2["Subpart"] = status_list

    # Keep unique Subpart per Reference No. + Customer Parts No.
    df2_unique = df2[["Reference No.", "Customer Parts No.", "Subpart"]].drop_duplicates()

    # Step 3: merge back into full df
    df = df.drop(columns=["Subpart"], errors="ignore")
    df = df.merge(
        df2_unique,
        on=["Reference No.", "Customer Parts No."],
        how="left",
    )

    # Step 4: propagate "To object?" to Distinction = 1 rows
    refs_with_bad_parts = df.loc[
        (df["Parts Distinction"] == 2) & (df["Subpart"] == "To object?"),
        "Reference No.",
    ].unique()

    df.loc[
        (df["Parts Distinction"] == 1) & (df["Reference No."].isin(refs_with_bad_parts)),
        "Subpart",
    ] = "To object?"

    # Step 5: remaining NaN → "OK"
    df["Subpart"] = df["Subpart"].fillna("OK")

    return df


# Apply subpart filter
results = apply_subpart_filter(df_new)

# ------------------------------------------------------------
# 8.3 Recompute Irr. Month on final results table (robust)
# ------------------------------------------------------------

results["Irr. Month"] = results["Reference No."].apply(refno_to_month_code)

# Optional sanity check
print(results[["Reference No.", "Irr. Month"]].head(20))


# ------------------------------------------------------------
# 8.4 Save results (refactor output, non-destructive)
# ------------------------------------------------------------

# Ensure legacy 判定.1 column exists for Power BI compatibility
if "判定.1" not in results.columns:
    if "claim" in results.columns:
        results["判定.1"] = results["claim"]
    else:
        # Fallback if claim doesn't exist (shouldn't happen)
        results["判定.1"] = 0

REF_RESULTS_PATH = (
    r"\\bosch.com\DfsRB\DfsJP\DIV\PS\QMC\All\01.QMC11\05_General\06_internship"
    r"\20240901_Julia_Antonioli\AI_Projects\warranty-judge\01. Nissan\AI_Results"
    fr"\results_refactor_20{DATE_YYMM}.xlsx"
)

results.to_excel(REF_RESULTS_PATH, index=False)
print("Refactor results saved to:", REF_RESULTS_PATH)
print("Refactor columns:", list(results.columns))

# ============================================================
# TEMPLATE / POWER BI SCHEMA CONFIG (GLOBAL)
# ============================================================

# Base directory already defined earlier:
# ROOT_DIR = r"...\11.Nissan_異議"

# Template used to force Power BI schema consistency
AI_TEMPLATE_PATH = fr"{ROOT_DIR}\AI_validated_claims_template.xlsx"

# Historical clean file (non-aggregated)
AI_CLAIMS_CLEAN_PATH = fr"{ROOT_DIR}\AI_validated_claims_clean.xlsx"

# Aggregated file consumed by Power BI
AI_CLAIMS_AGG_PATH = fr"{ROOT_DIR}\AI_validated_claims.xlsx"

# Mapping from old Power BI column names → new refactor column names
COLUMN_MAPPING = {
    "判定.1": "claim",   # old PBI column "判定.1" should now read from "claim"
    # add more mappings here if needed later
}

results = align_to_template(
    results,
    AI_TEMPLATE_PATH,
    column_mapping=COLUMN_MAPPING,
)

results.to_excel(REF_RESULTS_PATH, index=False)
print("Refactor results saved to:", REF_RESULTS_PATH)



# %%
# ============================================================
# 9. APPEND MONTHLY RESULTS TO POWER BI AGGREGATE FILE
# ============================================================

# Try to load existing aggregate file; if missing, start empty
try:
    all_claims = pd.read_excel(AI_CLAIMS_AGG_PATH)
    # Ensure AI_DATE is datetime
    if "AI_DATE" in all_claims.columns:
        all_claims["AI_DATE"] = pd.to_datetime(
            all_claims["AI_DATE"], format="%Y-%m-%d", errors="coerce"
        )
    else:
        # If for some reason no AI_DATE column, create it
        all_claims["AI_DATE"] = pd.NaT

except FileNotFoundError:
    # First run: no historical file yet
    all_claims = pd.DataFrame(columns=results.columns)
    all_claims["AI_DATE"] = pd.to_datetime(all_claims.get("AI_DATE", pd.Series([], dtype="datetime64[ns]")))

# Drop any existing rows for this month's AI_DATE
dates_to_replace = results["AI_DATE"].unique()
all_claims = all_claims[~all_claims["AI_DATE"].isin(dates_to_replace)]

# Append current results
all_claims = pd.concat([all_claims, results], ignore_index=True)

# Align schema to the template so Power BI never complains about missing columns
all_claims = align_to_template(
    all_claims,
    AI_TEMPLATE_PATH,
    column_mapping=COLUMN_MAPPING,
)

# Sort for Power BI readability
all_claims = (
    all_claims
    .sort_values(by=["AI_DATE", "EZKL Name", "Total Claimed Amount"], ascending=False)
    .reset_index(drop=True)
)

# Save aggregate file for Power BI
all_claims.to_excel(AI_CLAIMS_AGG_PATH, index=False)

print("Updated aggregate file saved to:", AI_CLAIMS_AGG_PATH)
print(all_claims["AI_DATE"].value_counts())



