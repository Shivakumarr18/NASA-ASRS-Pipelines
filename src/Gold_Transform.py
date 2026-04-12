"""
Gold Layer Transformation — NASA ASRS Aviation Risk Triage Platform
===================================================================
Reads the validated Silver CSV and produces a Star Schema:
  - dim_time
  - dim_aircraft
  - dim_environment
  - dim_component
  - dim_narrative
  - fact_incidents

Grain: One row per incident (ACN).
Output: 6 CSV files in data/gold/

Author: Shiva Kumar Goud
"""

import os
import logging
import pandas as pd
import numpy as np

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_PATH, ".."))

SILVER_PATH = os.path.join(PROJECT_ROOT, "data", "silver", "ASRS_SILVER_FINAL.csv")
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")
LOG_PATH = os.path.join(PROJECT_ROOT, "gold_transform.log")

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
logger = logging.getLogger("gold_transform")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")
stream_handler = logging.StreamHandler()

formatter = logging.Formatter("%(asctime)s | %(levelname)-5s | %(message)s")
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


# ─────────────────────────────────────────────
# STEP 0 — LOAD SILVER
# ─────────────────────────────────────────────
def load_silver(path):
    """Load the validated Silver CSV."""
    logger.info("=" * 60)
    logger.info("GOLD LAYER TRANSFORMATION — START")
    logger.info("=" * 60)
    logger.info(f"Loading Silver file from: {path}")

    if not os.path.exists(path):
        raise FileNotFoundError(f"Silver file not found at: {path}")

    df = pd.read_csv(path, parse_dates=["Time_Date"])
    logger.info(f"Silver loaded: {df.shape[0]} rows, {df.shape[1]} columns")

    # Fail-fast: verify expected column count
    expected_cols = 17
    if df.shape[1] != expected_cols:
        raise ValueError(f"Expected {expected_cols} columns, got {df.shape[1]}")

    # Fail-fast: verify ACN uniqueness
    if df["_ACN"].nunique() != df.shape[0]:
        raise ValueError("ACN is not unique — Silver data may be corrupted")

    logger.info("Silver validation passed — 17 columns, ACN is unique")
    return df


# ─────────────────────────────────────────────
# STEP 1 — BUILD dim_time
# ─────────────────────────────────────────────
def build_dim_time(df):
    """Extract date components from Time_Date into a time dimension."""
    logger.info("[Step 1] Building dim_time...")

    time_df = df[["Time_Date"]].drop_duplicates().copy()
    time_df = time_df.sort_values("Time_Date").reset_index(drop=True)

    time_df["time_id"] = time_df.index + 1
    time_df["full_date"] = time_df["Time_Date"]
    time_df["year"] = time_df["Time_Date"].dt.year
    time_df["month"] = time_df["Time_Date"].dt.month
    time_df["quarter"] = time_df["Time_Date"].dt.quarter
    time_df["day_of_week"] = time_df["Time_Date"].dt.day_name()
    time_df["is_weekend"] = time_df["Time_Date"].dt.dayofweek.isin([5, 6]).astype(int)

    dim_time = time_df[["time_id", "full_date", "year", "month", "quarter", "day_of_week", "is_weekend"]]

    logger.info(f"  dim_time: {dim_time.shape[0]} unique dates")
    return dim_time


# ─────────────────────────────────────────────
# STEP 2 — BUILD dim_aircraft
# ─────────────────────────────────────────────
def build_dim_aircraft(df):
    """Extract unique aircraft combinations into an aircraft dimension."""
    logger.info("[Step 2] Building dim_aircraft...")

    aircraft_cols = [
        "Aircraft_1_Make_Model_Name",
        "Aircraft_1_Aircraft_Operator",
        "Aircraft_1_Mission",
        "Aircraft_1_Flight_Phase",
        "Aircraft_1_Operating_Under_FAR_Part"
    ]

    aircraft_df = df[aircraft_cols].drop_duplicates().copy()
    aircraft_df = aircraft_df.sort_values(aircraft_cols).reset_index(drop=True)
    aircraft_df["aircraft_id"] = aircraft_df.index + 1

    aircraft_df = aircraft_df.rename(columns={
        "Aircraft_1_Make_Model_Name": "make_model_name",
        "Aircraft_1_Aircraft_Operator": "operator",
        "Aircraft_1_Mission": "mission",
        "Aircraft_1_Flight_Phase": "flight_phase",
        "Aircraft_1_Operating_Under_FAR_Part": "far_part"
    })

    dim_aircraft = aircraft_df[["aircraft_id", "make_model_name", "operator", "mission", "flight_phase", "far_part"]]

    logger.info(f"  dim_aircraft: {dim_aircraft.shape[0]} unique aircraft combinations")
    return dim_aircraft


# ─────────────────────────────────────────────
# STEP 3 — BUILD dim_environment
# ─────────────────────────────────────────────
def build_dim_environment(df):
    """Extract unique environment combinations into an environment dimension."""
    logger.info("[Step 3] Building dim_environment...")

    env_cols = ["Environment_Flight_Conditions", "Environment_Light"]

    env_df = df[env_cols].drop_duplicates().copy()
    env_df = env_df.sort_values(env_cols).reset_index(drop=True)
    env_df["environment_id"] = env_df.index + 1

    env_df = env_df.rename(columns={
        "Environment_Flight_Conditions": "flight_conditions",
        "Environment_Light": "light"
    })

    dim_environment = env_df[["environment_id", "flight_conditions", "light"]]

    logger.info(f"  dim_environment: {dim_environment.shape[0]} unique environment combinations")
    return dim_environment


# ─────────────────────────────────────────────
# STEP 4 — BUILD dim_component
# ─────────────────────────────────────────────
def build_dim_component(df):
    """Extract unique components into a component dimension."""
    logger.info("[Step 4] Building dim_component...")

    comp_df = df[["Component_Aircraft_Component"]].drop_duplicates().copy()
    comp_df = comp_df.sort_values("Component_Aircraft_Component").reset_index(drop=True)
    comp_df["component_id"] = comp_df.index + 1

    comp_df = comp_df.rename(columns={
        "Component_Aircraft_Component": "component_name"
    })

    dim_component = comp_df[["component_id", "component_name"]]

    logger.info(f"  dim_component: {dim_component.shape[0]} unique components")
    return dim_component


# ─────────────────────────────────────────────
# STEP 5 — BUILD dim_narrative
# ─────────────────────────────────────────────
def build_dim_narrative(df):
    """Store narratives and synopses in a separate dimension for on-demand detail."""
    logger.info("[Step 5] Building dim_narrative...")

    narr_df = df[["_ACN", "Report_1_Narrative", "Report_1_Synopsis"]].copy()
    narr_df["narrative_id"] = narr_df.index + 1

    narr_df = narr_df.rename(columns={
        "Report_1_Narrative": "narrative",
        "Report_1_Synopsis": "synopsis"
    })

    dim_narrative = narr_df[["narrative_id", "_ACN", "narrative", "synopsis"]]

    logger.info(f"  dim_narrative: {dim_narrative.shape[0]} narratives stored")
    return dim_narrative


# ─────────────────────────────────────────────
# STEP 6 — BUILD fact_incidents
# ─────────────────────────────────────────────
def build_fact_incidents(df, dim_time, dim_aircraft, dim_environment, dim_component, dim_narrative):
    """Build the fact table by replacing descriptive columns with foreign keys."""
    logger.info("[Step 6] Building fact_incidents...")

    fact = df.copy()

    # --- Merge time_id ---
    fact = fact.merge(
        dim_time[["time_id", "full_date"]],
        left_on="Time_Date",
        right_on="full_date",
        how="left"
    )

    # --- Merge aircraft_id ---
    aircraft_merge_cols = {
        "Aircraft_1_Make_Model_Name": "make_model_name",
        "Aircraft_1_Aircraft_Operator": "operator",
        "Aircraft_1_Mission": "mission",
        "Aircraft_1_Flight_Phase": "flight_phase",
        "Aircraft_1_Operating_Under_FAR_Part": "far_part"
    }
    dim_aircraft_merge = dim_aircraft.copy()
    fact = fact.merge(
        dim_aircraft_merge,
        left_on=list(aircraft_merge_cols.keys()),
        right_on=list(aircraft_merge_cols.values()),
        how="left"
    )

    # --- Merge environment_id ---
    env_merge_cols = {
        "Environment_Flight_Conditions": "flight_conditions",
        "Environment_Light": "light"
    }
    dim_env_merge = dim_environment.copy()
    fact = fact.merge(
        dim_env_merge,
        left_on=list(env_merge_cols.keys()),
        right_on=list(env_merge_cols.values()),
        how="left"
    )

    # --- Merge component_id ---
    dim_comp_merge = dim_component.copy()
    fact = fact.merge(
        dim_comp_merge,
        left_on="Component_Aircraft_Component",
        right_on="component_name",
        how="left"
    )

    # --- Merge narrative_id ---
    fact = fact.merge(
        dim_narrative[["narrative_id", "_ACN"]],
        on="_ACN",
        how="left"
    )

    # --- Select and rename final fact columns ---
    fact_incidents = fact[[
        "_ACN",
        "time_id",
        "aircraft_id",
        "environment_id",
        "component_id",
        "narrative_id",
        "Place_Altitude.MSL.Single_Value",
        "Place_State_Reference",
        "Events_Anomaly",
        "Assessments_Primary_Problem",
        "Assessments_Contributing_Factors_/_Situations"
    ]].copy()

    fact_incidents = fact_incidents.rename(columns={
        "_ACN": "incident_id",
        "Place_Altitude.MSL.Single_Value": "altitude",
        "Place_State_Reference": "state",
        "Events_Anomaly": "anomaly",
        "Assessments_Primary_Problem": "primary_problem",
        "Assessments_Contributing_Factors_/_Situations": "contributing_factors"
    })

    logger.info(f"  fact_incidents: {fact_incidents.shape[0]} rows, {fact_incidents.shape[1]} columns")

    # --- Validate no rows were lost or duplicated ---
    if fact_incidents.shape[0] != df.shape[0]:
        raise ValueError(
            f"Row count mismatch! Silver had {df.shape[0]} rows, "
            f"fact has {fact_incidents.shape[0]} rows. "
            "Check for fan-out in joins."
        )

    # --- Validate no null foreign keys ---
    fk_cols = ["time_id", "aircraft_id", "environment_id", "component_id", "narrative_id"]
    for col in fk_cols:
        null_count = fact_incidents[col].isnull().sum()
        if null_count > 0:
            logger.warning(f"  WARNING: {null_count} null values in {col} — unmatched dimension records")

    logger.info("  Fact table validation passed — row count matches Silver, foreign keys checked")
    return fact_incidents


# ─────────────────────────────────────────────
# STEP 7 — SAVE ALL GOLD TABLES
# ─────────────────────────────────────────────
def save_gold_tables(dim_time, dim_aircraft, dim_environment, dim_component, dim_narrative, fact_incidents):
    """Save all Gold layer tables as CSVs."""
    logger.info("[Step 7] Saving Gold layer tables...")

    os.makedirs(GOLD_DIR, exist_ok=True)

    tables = {
        "dim_time": dim_time,
        "dim_aircraft": dim_aircraft,
        "dim_environment": dim_environment,
        "dim_component": dim_component,
        "dim_narrative": dim_narrative,
        "fact_incidents": fact_incidents
    }

    for name, table in tables.items():
        path = os.path.join(GOLD_DIR, f"{name}.csv")
        table.to_csv(path, index=False)
        logger.info(f"  Saved {name}.csv — {table.shape[0]} rows, {table.shape[1]} columns")

    logger.info("=" * 60)
    logger.info("GOLD LAYER TRANSFORMATION — COMPLETE")
    logger.info("=" * 60)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    try:
        # Load
        df = load_silver(SILVER_PATH)

        # Build dimensions
        dim_time = build_dim_time(df)
        dim_aircraft = build_dim_aircraft(df)
        dim_environment = build_dim_environment(df)
        dim_component = build_dim_component(df)
        dim_narrative = build_dim_narrative(df)

        # Build fact
        fact_incidents = build_fact_incidents(
            df, dim_time, dim_aircraft, dim_environment, dim_component, dim_narrative
        )

        # Save
        save_gold_tables(dim_time, dim_aircraft, dim_environment, dim_component, dim_narrative, fact_incidents)

        # Final summary
        logger.info("")
        logger.info("GOLD LAYER SUMMARY:")
        logger.info(f"  dim_time:        {dim_time.shape[0]} rows x {dim_time.shape[1]} cols")
        logger.info(f"  dim_aircraft:    {dim_aircraft.shape[0]} rows x {dim_aircraft.shape[1]} cols")
        logger.info(f"  dim_environment: {dim_environment.shape[0]} rows x {dim_environment.shape[1]} cols")
        logger.info(f"  dim_component:   {dim_component.shape[0]} rows x {dim_component.shape[1]} cols")
        logger.info(f"  dim_narrative:   {dim_narrative.shape[0]} rows x {dim_narrative.shape[1]} cols")
        logger.info(f"  fact_incidents:  {fact_incidents.shape[0]} rows x {fact_incidents.shape[1]} cols")

    except Exception as e:
        logger.error(f"GOLD TRANSFORMATION FAILED: {e}")
        raise


if __name__ == "__main__":
    main()