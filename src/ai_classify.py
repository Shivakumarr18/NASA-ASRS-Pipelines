"""
AI Classification Layer — NASA ASRS Aviation Risk Triage Platform
==================================================================
Reads narratives from dim_narrative, calls Google Gemini API to classify
each incident into a risk tier with reasoning, and saves results back
to the Gold layer.

Risk Tiers:
  - CRITICAL : Immediate action required (safety-critical events)
  - HIGH     : Urgent review needed (significant risk indicators)
  - MEDIUM   : Monitor closely (procedural/minor anomalies)
  - LOW      : Routine operations (informational reports)

Output: data/gold/dim_narrative_classified.csv
        (with risk_tier, risk_reasoning, key_factors columns added)

Design Principles:
  - Idempotent: Re-running skips already-classified incidents
  - Fail-safe: API failures logged, processing continues for other rows
  - Cost-aware: Free tier rate limits respected (15 RPM, 1500/day)
  - Auditable: Every classification logged with timestamp
  - Explainable: Each tier comes with reasoning and key factors

Author: Shiva Kumar Goud
"""

import os
import sys
import json
import time
import logging
import pandas as pd
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_PATH, ".."))

GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")
NARRATIVE_INPUT = os.path.join(GOLD_DIR, "dim_narrative.csv")
NARRATIVE_OUTPUT = os.path.join(GOLD_DIR, "dim_narrative_classified.csv")
LOG_PATH = os.path.join(PROJECT_ROOT, "ai_classify.log")
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")

# Free tier rate limits (Gemini 2.0 Flash)
REQUESTS_PER_MINUTE = 15
DAILY_LIMIT = 1500
DELAY_BETWEEN_REQUESTS = 4.5  # seconds (gives ~13 RPM, safe under 15 RPM cap)

# Process in batches and save progress (recovery from crashes)
SAVE_EVERY_N = 50

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
logger = logging.getLogger("ai_classify")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")
stream_handler = logging.StreamHandler()

formatter = logging.Formatter("%(asctime)s | %(levelname)-5s | %(message)s")
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


# ─────────────────────────────────────────────
# STEP 0 — LOAD ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────
def load_api_key():
    """Load the Gemini API key from .env file. Fail fast if missing."""
    if not os.path.exists(ENV_PATH):
        raise FileNotFoundError(
            f".env file not found at {ENV_PATH}\n"
            "Create a .env file in the project root with: GEMINI_API_KEY=your_key_here"
        )

    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("GEMINI_API_KEY="):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
                if not key:
                    raise ValueError("GEMINI_API_KEY is empty in .env file")
                return key

    raise ValueError("GEMINI_API_KEY not found in .env file")


# ─────────────────────────────────────────────
# STEP 1 — INITIALIZE GEMINI CLIENT
# ─────────────────────────────────────────────
def init_gemini_client(api_key):
    """Initialize the Gemini API client. Fail fast on import or auth issues."""
    try:
        import google.generativeai as genai
    except ImportError:
        raise ImportError(
            "google-generativeai package not installed.\n"
            "Run: pip install google-generativeai"
        )

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.0-flash-exp")

    # Smoke test — verify the key actually works before processing 4500 rows
    try:
        test_response = model.generate_content("Reply with just the word: OK")
        if "OK" not in test_response.text.upper():
            logger.warning(f"Smoke test returned unexpected response: {test_response.text}")
        else:
            logger.info("Gemini API smoke test passed")
    except Exception as e:
        raise RuntimeError(f"Gemini API authentication failed: {e}")

    return model


# ─────────────────────────────────────────────
# STEP 2 — LOAD NARRATIVES
# ─────────────────────────────────────────────
def load_narratives():
    """Load dim_narrative. Resume from existing classified file if present."""
    logger.info("=" * 60)
    logger.info("AI CLASSIFICATION LAYER — START")
    logger.info("=" * 60)

    if not os.path.exists(NARRATIVE_INPUT):
        raise FileNotFoundError(f"Input file not found: {NARRATIVE_INPUT}")

    df_input = pd.read_csv(NARRATIVE_INPUT)
    logger.info(f"Loaded dim_narrative: {df_input.shape[0]} rows, {df_input.shape[1]} columns")

    # If a classified file already exists, resume from where we stopped
    if os.path.exists(NARRATIVE_OUTPUT):
        df_existing = pd.read_csv(NARRATIVE_OUTPUT)
        logger.info(f"Found existing output: {df_existing.shape[0]} rows already classified")

        # Merge: keep existing classifications, add new rows
        already_classified = set(df_existing["narrative_id"])
        df_pending = df_input[~df_input["narrative_id"].isin(already_classified)].copy()
        logger.info(f"Pending classification: {df_pending.shape[0]} rows")
        return df_input, df_existing, df_pending

    # First run — nothing classified yet
    df_pending = df_input.copy()
    df_existing = pd.DataFrame(columns=list(df_input.columns) + ["risk_tier", "risk_reasoning", "key_factors"])
    return df_input, df_existing, df_pending


# ─────────────────────────────────────────────
# STEP 3 — BUILD CLASSIFICATION PROMPT
# ─────────────────────────────────────────────
def build_prompt(narrative, synopsis):
    """Build the classification prompt. Same prompt for every row — keeps results consistent."""
    narrative_text = str(narrative)[:3000] if narrative else "NO NARRATIVE PROVIDED"
    synopsis_text = str(synopsis)[:500] if synopsis else "NO SYNOPSIS PROVIDED"

    prompt = f"""You are an aviation safety analyst classifying NASA ASRS incident reports.

Classify the following incident into ONE of these risk tiers:
- CRITICAL: Immediate safety threat. Loss of control, near-collision, fire, structural damage, severe injury risk.
- HIGH: Significant safety concern. System failures, procedural violations with safety impact, major equipment malfunction.
- MEDIUM: Notable concern requiring attention. Minor system issues, recoverable procedural lapses, training gaps.
- LOW: Routine or informational report. Minor anomalies with no immediate safety impact, observations.

INCIDENT NARRATIVE:
{narrative_text}

SYNOPSIS:
{synopsis_text}

Respond ONLY with valid JSON in this exact format (no markdown, no extra text):
{{
  "risk_tier": "CRITICAL|HIGH|MEDIUM|LOW",
  "reasoning": "One or two sentences explaining the classification.",
  "key_factors": ["factor 1", "factor 2", "factor 3"]
}}"""
    return prompt


# ─────────────────────────────────────────────
# STEP 4 — CALL GEMINI API
# ─────────────────────────────────────────────
def classify_incident(model, narrative, synopsis):
    """Call Gemini API for one incident. Returns dict with tier, reasoning, factors."""
    prompt = build_prompt(narrative, synopsis)

    try:
        response = model.generate_content(prompt)
        raw_text = response.text.strip()

        # Strip markdown code fences if present
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
            raw_text = raw_text.strip()

        result = json.loads(raw_text)

        # Validate response structure
        if "risk_tier" not in result or result["risk_tier"] not in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            raise ValueError(f"Invalid risk_tier in response: {result}")

        return {
            "risk_tier": result.get("risk_tier"),
            "risk_reasoning": result.get("reasoning", ""),
            "key_factors": "|".join(result.get("key_factors", [])) if isinstance(result.get("key_factors"), list) else str(result.get("key_factors", ""))
        }

    except json.JSONDecodeError as e:
        logger.warning(f"JSON parse error: {e} | Raw response: {raw_text[:200]}")
        return {"risk_tier": "ERROR_PARSE", "risk_reasoning": f"Parse error: {str(e)[:100]}", "key_factors": ""}

    except Exception as e:
        logger.warning(f"API call failed: {str(e)[:200]}")
        return {"risk_tier": "ERROR_API", "risk_reasoning": f"API error: {str(e)[:100]}", "key_factors": ""}


# ─────────────────────────────────────────────
# STEP 5 — SAVE PROGRESS
# ─────────────────────────────────────────────
def save_progress(df_classified, output_path):
    """Save current progress to disk. Called periodically during processing."""
    df_classified.to_csv(output_path, index=False)
    logger.info(f"  Progress saved: {df_classified.shape[0]} rows classified")


# ─────────────────────────────────────────────
# STEP 6 — MAIN PROCESSING LOOP
# ─────────────────────────────────────────────
def process_narratives(model, df_existing, df_pending):
    """Classify each pending narrative, saving progress periodically."""
    logger.info(f"[Processing] {df_pending.shape[0]} narratives to classify")
    logger.info(f"  Estimated time: {df_pending.shape[0] * DELAY_BETWEEN_REQUESTS / 60:.1f} minutes")

    classified_rows = []
    total = df_pending.shape[0]
    error_count = 0

    for idx, (_, row) in enumerate(df_pending.iterrows(), start=1):
        # Daily limit safety check
        if idx > DAILY_LIMIT:
            logger.warning(f"  Reached daily limit ({DAILY_LIMIT}). Stopping. Resume tomorrow.")
            break

        result = classify_incident(model, row.get("narrative", ""), row.get("synopsis", ""))

        new_row = row.to_dict()
        new_row.update(result)
        classified_rows.append(new_row)

        if result["risk_tier"].startswith("ERROR"):
            error_count += 1

        # Log progress every 10 rows
        if idx % 10 == 0:
            logger.info(f"  Progress: {idx}/{total} ({idx*100//total}%) | Errors: {error_count}")

        # Save progress every N rows (crash recovery)
        if idx % SAVE_EVERY_N == 0:
            df_partial = pd.concat([df_existing, pd.DataFrame(classified_rows)], ignore_index=True)
            save_progress(df_partial, NARRATIVE_OUTPUT)

        # Rate limit delay
        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Final save
    df_final = pd.concat([df_existing, pd.DataFrame(classified_rows)], ignore_index=True)
    save_progress(df_final, NARRATIVE_OUTPUT)

    logger.info(f"[Processing complete] Total classified: {len(classified_rows)} | Errors: {error_count}")
    return df_final


# ─────────────────────────────────────────────
# STEP 7 — FINAL AUDIT
# ─────────────────────────────────────────────
def final_audit(df_final):
    """Print summary of classifications."""
    logger.info("=" * 60)
    logger.info("CLASSIFICATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total rows: {df_final.shape[0]}")

    if "risk_tier" in df_final.columns:
        tier_counts = df_final["risk_tier"].value_counts()
        for tier, count in tier_counts.items():
            pct = count * 100 / df_final.shape[0]
            logger.info(f"  {tier:15s}: {count:5d} ({pct:.1f}%)")

    logger.info(f"\nOutput saved to: {NARRATIVE_OUTPUT}")
    logger.info("=" * 60)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    try:
        # Load API key from .env (fail fast if missing)
        api_key = load_api_key()
        logger.info("API key loaded from .env")

        # Initialize Gemini client (fail fast if auth fails)
        model = init_gemini_client(api_key)

        # Load data (resumes from existing output if present)
        df_input, df_existing, df_pending = load_narratives()

        if df_pending.shape[0] == 0:
            logger.info("All narratives already classified. Nothing to do.")
            final_audit(df_existing)
            return

        # Process
        df_final = process_narratives(model, df_existing, df_pending)

        # Audit
        final_audit(df_final)

    except Exception as e:
        logger.error(f"AI CLASSIFICATION FAILED: {e}")
        raise


if __name__ == "__main__":
    main()