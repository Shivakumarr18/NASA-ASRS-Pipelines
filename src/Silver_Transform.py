"""
silver_transform.py
NASA ASRS — Silver Layer Transformation
Fixes: hardcoding, silent failures, no validation, no logging, no config
"""

import pandas as pd
import logging
import os
import sys

# ─────────────────────────────────────────────────────────────────────────────
# FIX 1 — LOGGING (replaces all print statements)
# Logs go to console AND to a file for debugging later
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("silver_transform.log")
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# FIX 2 — CONFIG (replaces all hardcoded values)
# One place to change anything. No hardcoded paths or column names anywhere.
# ─────────────────────────────────────────────────────────────────────────────
BASE_PATH   = r"C:\Users\shivn\NASA-ASRS-Pipelines"
BRONZE_PATH = os.path.join(BASE_PATH, "data", "bronze", "ASRS_DBOnline.csv")
SILVER_PATH = os.path.join(BASE_PATH, "data", "silver", "ASRS_SILVER_FINAL.csv")

CONFIG = {
    "null_fill_value": "UNKNOWN",
    "narrative_fill":  "NO NARRATIVE PROVIDED",
    "synopsis_fill":   "NO SYNOPSIS PROVIDED",

    # These are CRITICAL columns — if any are missing the pipeline must STOP
    "critical_columns": [
        "_ACN",
        "Report_1_Narrative",
        "Events_Anomaly",
        "Assessments_Primary_Problem",
    ],

    # Full Silver contract — all 17 columns we want
    "silver_columns": [
        "_ACN",
        "Time_Date",
        "Place_State_Reference",
        "Place_Altitude.MSL.Single_Value",
        "Environment_Flight_Conditions",
        "Environment_Light",
        "Aircraft_1_Make_Model_Name",
        "Aircraft_1_Aircraft_Operator",
        "Aircraft_1_Mission",
        "Aircraft_1_Flight_Phase",
        "Aircraft_1_Operating_Under_FAR_Part",
        "Component_Aircraft_Component",
        "Events_Anomaly",
        "Assessments_Primary_Problem",
        "Assessments_Contributing_Factors_/_Situations",
        "Report_1_Narrative",
        "Report_1_Synopsis",
    ],

    # Columns where nulls get filled with UNKNOWN
    "sparse_columns": [
        "Place_State_Reference",
        "Place_Altitude.MSL.Single_Value",
        "Environment_Flight_Conditions",
        "Environment_Light",
        "Aircraft_1_Aircraft_Operator",
        "Aircraft_1_Mission",
        "Aircraft_1_Flight_Phase",
        "Aircraft_1_Operating_Under_FAR_Part",
        "Component_Aircraft_Component",
    ],

    # String columns to strip whitespace from
    "string_columns": [
        "Place_State_Reference",
        "Environment_Flight_Conditions",
        "Environment_Light",
        "Aircraft_1_Make_Model_Name",
        "Aircraft_1_Aircraft_Operator",
        "Aircraft_1_Mission",
        "Aircraft_1_Flight_Phase",
        "Aircraft_1_Operating_Under_FAR_Part",
        "Component_Aircraft_Component",
        "Events_Anomaly",
        "Assessments_Primary_Problem",
        "Assessments_Contributing_Factors_/_Situations",
    ],

    # Valid date range for this dataset
    "date_min": "2020-01-01",
    "date_max": "2026-12-31",

    # Minimum rows we expect after cleaning — if below this something is wrong
    "min_expected_rows": 4000,
}


# ─────────────────────────────────────────────────────────────────────────────
# FIX 3 — MODULAR FUNCTIONS (replaces one giant block of code)
# Each function does ONE thing. Easy to test, easy to debug.
# ─────────────────────────────────────────────────────────────────────────────

def load_bronze(path):
    """Load raw Bronze CSV with MultiIndex headers and flatten column names."""
    log.info(f"Loading Bronze file from: {path}")

    # FIX 4 — FAIL FAST: stop immediately if file does not exist
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Bronze file not found at: {path}\n"
            f"Check your data/bronze/ folder."
        )

    df = pd.read_csv(path, low_memory=False, header=[0, 1])

    # Flatten MultiIndex columns
    df.columns = [
        f"{str(cat).strip()}_{str(col).strip()}".replace(" ", "_")
        if not str(col).startswith("Unnamed")
        else str(cat).strip().replace(" ", "_")
        for cat, col in df.columns
    ]
    df.columns = [c.replace("__", "_") for c in df.columns]

    log.info(f"Bronze loaded: {df.shape[0]} rows, {df.shape[1]} columns")
    return df


def select_columns(df, silver_columns, critical_columns):
    """Select only the Silver columns. Fail fast if critical columns are missing."""
    available = [col for col in silver_columns if col in df.columns]
    missing   = [col for col in silver_columns if col not in df.columns]

    # FIX 4 — FAIL FAST: stop if any CRITICAL column is missing
    missing_critical = [col for col in critical_columns if col not in df.columns]
    if missing_critical:
        raise ValueError(
            f"PIPELINE STOPPED — Critical columns missing: {missing_critical}\n"
            f"These columns are required for the risk engine. Check your Bronze file."
        )

    # Non-critical missing columns — log a warning but continue
    non_critical_missing = [col for col in missing if col not in critical_columns]
    if non_critical_missing:
        log.warning(f"Non-critical columns not found (skipped): {non_critical_missing}")

    df = df[available].copy()
    log.info(f"Selected {len(available)} columns")
    return df


def remove_ghost_row(df):
    """Remove the ghost row — the category header that leaked into data."""
    rows_before = len(df)
    df = df.dropna(subset=["_ACN"])
    removed = rows_before - len(df)
    log.info(f"Ghost rows removed: {removed} | Rows remaining: {len(df)}")
    return df


def handle_nulls(df, sparse_columns, null_fill, narrative_fill, synopsis_fill):
    """Fill nulls with UNKNOWN for sparse columns. Never drop rows."""
    for col in sparse_columns:
        if col in df.columns:
            count = df[col].isna().sum()
            if count > 0:
                df[col] = df[col].fillna(null_fill)
                log.info(f"  Filled {count} nulls in '{col}' with '{null_fill}'")

    df["Report_1_Narrative"] = df["Report_1_Narrative"].fillna(narrative_fill)
    df["Report_1_Synopsis"]  = df["Report_1_Synopsis"].fillna(synopsis_fill)
    log.info("Null handling complete")
    return df


def remove_duplicates(df):
    """Remove duplicate ACNs. Each incident must appear exactly once."""
    rows_before = len(df)
    df = df.drop_duplicates(subset=["_ACN"])
    removed = rows_before - len(df)
    if removed > 0:
        log.warning(f"Removed {removed} duplicate ACN(s)")
    else:
        log.info(f"No duplicates found — all {len(df)} ACNs are unique")
    return df


def convert_types(df, string_columns):
    """Convert columns to correct data types."""

    # ACN — integer primary key
    df["_ACN"] = pd.to_numeric(df["_ACN"], errors="coerce").astype("Int64")
    log.info("  _ACN → Int64")

    # Time_Date — format is YYYYMM, convert to datetime
    df["Time_Date"] = pd.to_datetime(
        df["Time_Date"].astype(str).str[:6],
        format="%Y%m",
        errors="coerce"
    )
    log.info("  Time_Date → datetime")

    # Altitude — numeric
    df["Place_Altitude.MSL.Single_Value"] = pd.to_numeric(
        df["Place_Altitude.MSL.Single_Value"], errors="coerce"
    )
    log.info("  Place_Altitude.MSL.Single_Value → float")

    # String columns — strip whitespace
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    log.info("  All string columns stripped of whitespace")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# FIX 5 — VALIDATION LAYER
# After cleaning, enforce rules. If rules fail, stop the pipeline.
# This is the assert pattern — fail loudly, never silently.
# ─────────────────────────────────────────────────────────────────────────────

def validate(df, date_min, date_max, min_expected_rows):
    """Validate the Silver DataFrame before saving. Raise on any failure."""
    log.info("Running validation checks...")
    errors = []

    # Rule 1 — ACN must never be null
    acn_nulls = df["_ACN"].isna().sum()
    if acn_nulls > 0:
        errors.append(f"ACN contains {acn_nulls} null values — primary key cannot be null")

    # Rule 2 — ACN must be unique
    acn_dupes = df["_ACN"].duplicated().sum()
    if acn_dupes > 0:
        errors.append(f"ACN contains {acn_dupes} duplicate values — primary key must be unique")

    # Rule 3 — Date range must be valid
    invalid_dates = df["Time_Date"].isna().sum()
    if invalid_dates > 0:
        errors.append(f"Time_Date has {invalid_dates} unparseable values")

    out_of_range = df[
        (df["Time_Date"] < date_min) | (df["Time_Date"] > date_max)
    ]["Time_Date"].dropna()
    if len(out_of_range) > 0:
        errors.append(f"Time_Date has {len(out_of_range)} values outside expected range")

    # Rule 4 — Narrative must never be empty string
    empty_narrative = (df["Report_1_Narrative"].str.strip() == "").sum()
    if empty_narrative > 0:
        errors.append(f"Report_1_Narrative has {empty_narrative} empty strings")

    # Rule 5 — Row count must be above minimum threshold
    if len(df) < min_expected_rows:
        errors.append(
            f"Row count {len(df)} is below minimum expected {min_expected_rows} — "
            f"possible data loss during transformation"
        )

    # If ANY rule failed — stop the pipeline
    if errors:
        log.error("VALIDATION FAILED:")
        for e in errors:
            log.error(f"  ✗ {e}")
        raise AssertionError(
            f"Silver validation failed with {len(errors)} error(s). "
            f"Fix before saving. See logs above."
        )

    log.info("All validation checks passed")


def save_silver(df, path):
    """Save the Silver DataFrame to CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    size_kb = os.path.getsize(path) / 1024
    log.info(f"Saved to: {path}")
    log.info(f"File size: {size_kb:.1f} KB")


def final_audit(df):
    """Print a final audit summary of the Silver layer."""
    log.info("=" * 55)
    log.info("SILVER LAYER — FINAL AUDIT")
    log.info("=" * 55)
    log.info(f"  Total rows     : {df.shape[0]}")
    log.info(f"  Total columns  : {df.shape[1]}")
    log.info(f"  Duplicate ACNs : {df['_ACN'].duplicated().sum()}")
    log.info(f"  Null in ACN    : {df['_ACN'].isna().sum()}")
    log.info(f"  Date range     : {df['Time_Date'].min()} → {df['Time_Date'].max()}")

    remaining_nulls = df.isnull().sum()
    remaining_nulls = remaining_nulls[remaining_nulls > 0]
    if len(remaining_nulls) > 0:
        log.info("  Remaining nulls (expected — altitude only):")
        for col, count in remaining_nulls.items():
            log.info(f"    {col}: {count}")
    else:
        log.info("  No unexpected nulls")

    log.info("=" * 55)
    log.info("Silver layer is locked and ready for Gold.")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def run_silver_pipeline():
    log.info("=" * 55)
    log.info("SILVER LAYER TRANSFORMATION — START")
    log.info("=" * 55)

    # Step 0 — Load
    df = load_bronze(BRONZE_PATH)

    # Step 1 — Select columns
    log.info("[Step 1] Selecting relevant columns...")
    df = select_columns(df, CONFIG["silver_columns"], CONFIG["critical_columns"])

    # Step 2 — Remove ghost row
    log.info("[Step 2] Removing ghost row...")
    df = remove_ghost_row(df)

    # Step 3 — Handle nulls
    log.info("[Step 3] Handling null values...")
    df = handle_nulls(
        df,
        CONFIG["sparse_columns"],
        CONFIG["null_fill_value"],
        CONFIG["narrative_fill"],
        CONFIG["synopsis_fill"]
    )

    # Step 4 — Remove duplicates
    log.info("[Step 4] Checking for duplicates...")
    df = remove_duplicates(df)

    # Step 5 — Convert types
    log.info("[Step 5] Converting data types...")
    df = convert_types(df, CONFIG["string_columns"])

    # Validate before saving — fail fast if anything is wrong
    validate(df, CONFIG["date_min"], CONFIG["date_max"], CONFIG["min_expected_rows"])

    # Step 6 — Save
    log.info("[Step 6] Saving Silver layer...")
    save_silver(df, SILVER_PATH)

    # Final audit
    final_audit(df)


# Entry point — works both as a script and as a notebook cell
run_silver_pipeline()