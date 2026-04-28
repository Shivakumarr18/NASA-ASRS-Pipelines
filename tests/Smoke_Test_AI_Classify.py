"""
AI Classification Smoke Test
=============================
Validates that the Gemini API integration works end-to-end on a small sample
BEFORE running the full classification on 4,500 incidents.

What this verifies:
  1. .env file is found and API key loads correctly
  2. Gemini API authentication succeeds
  3. Sample narratives produce valid risk classifications
  4. JSON parsing works correctly
  5. All four risk tiers are reachable in principle
  6. Output structure matches expected schema

This script does NOT modify any production data.
It runs against an in-memory sample and prints results to console.

Run: python tests/smoke_test_ai_classify.py

Author: Shiva Kumar Goud
"""

import os
import sys
import json
import time
import logging
from datetime import datetime

# Add src/ to path so we can import functions from ai_classify.py
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_PATH, ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
sys.path.insert(0, SRC_PATH)

# Import the production functions — reuse, don't duplicate
from ai_classify import load_api_key, init_gemini_client, build_prompt, classify_incident

# ─────────────────────────────────────────────
# LOGGING SETUP (console only — no file writes)
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("smoke_test")


# ─────────────────────────────────────────────
# TEST DATA — 5 sample narratives covering different risk levels
# ─────────────────────────────────────────────
TEST_INCIDENTS = [
    {
        "id": 1,
        "expected_tier_hint": "CRITICAL",
        "narrative": (
            "During takeoff at 1,500 feet AGL, the left engine experienced sudden complete "
            "power loss with smoke visible from the engine compartment. Aircraft yawed "
            "sharply left. Captain declared emergency, returned to airport, executed "
            "single-engine landing successfully. Post-flight inspection revealed catastrophic "
            "engine failure with internal component fragmentation."
        ),
        "synopsis": "Engine failure during takeoff with smoke. Emergency single-engine landing."
    },
    {
        "id": 2,
        "expected_tier_hint": "HIGH",
        "narrative": (
            "While on approach to runway 27, hydraulic system pressure dropped from 3000 PSI "
            "to 1200 PSI. Backup hydraulics engaged. Landing gear extension required manual "
            "backup procedure. Aircraft landed safely. Maintenance discovered leaking "
            "hydraulic line near landing gear actuator."
        ),
        "synopsis": "Hydraulic pressure loss on approach. Manual backup gear extension."
    },
    {
        "id": 3,
        "expected_tier_hint": "MEDIUM",
        "narrative": (
            "Cabin pressurization fluctuated during cruise at FL350. Pressure differential "
            "varied between 8.0 and 8.5 PSI for approximately 10 minutes. Pressurization "
            "stabilized after pilot cycled the outflow valve. No oxygen masks deployed. "
            "Maintenance found minor outflow valve calibration issue."
        ),
        "synopsis": "Pressurization fluctuation at cruise altitude. Resolved by valve cycling."
    },
    {
        "id": 4,
        "expected_tier_hint": "LOW",
        "narrative": (
            "Minor anomaly noted in cabin announcement system during pre-flight check. "
            "Public address system briefly produced static when activated. Issue resolved "
            "by power-cycling the audio system. Flight departed on schedule with no "
            "passenger impact."
        ),
        "synopsis": "PA system static during preflight. Resolved by power cycle."
    },
    {
        "id": 5,
        "expected_tier_hint": "HIGH or CRITICAL",
        "narrative": (
            "Near mid-air collision with VFR traffic during descent. TCAS issued resolution "
            "advisory commanding immediate climb. Crew complied. Estimated separation at "
            "closest point was approximately 300 feet vertical, less than 0.5 nautical miles "
            "horizontal. ATC was notified of the conflict."
        ),
        "synopsis": "Near mid-air collision during descent. TCAS RA executed."
    },
]


# ─────────────────────────────────────────────
# TEST FUNCTIONS
# ─────────────────────────────────────────────
def test_env_loading():
    """Test 1: .env file loads and API key is present."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 1: Environment Configuration")
    logger.info("=" * 60)
    try:
        key = load_api_key()
        if not key.startswith("AIza"):
            raise ValueError(f"Key doesn't start with 'AIza' — got: {key[:10]}...")
        logger.info(f"  PASS: API key loaded ({len(key)} chars, starts with {key[:6]}...)")
        return key
    except Exception as e:
        logger.error(f"  FAIL: {e}")
        raise


def test_api_authentication(api_key):
    """Test 2: Gemini API client initializes and smoke test passes."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 2: Gemini API Authentication")
    logger.info("=" * 60)
    try:
        model = init_gemini_client(api_key)
        logger.info("  PASS: Gemini client initialized and smoke test succeeded")
        return model
    except Exception as e:
        logger.error(f"  FAIL: {e}")
        raise


def test_classification_quality(model):
    """Test 3: Run classification on 5 sample incidents and inspect quality."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 3: Classification Quality (5 sample incidents)")
    logger.info("=" * 60)

    valid_tiers = {"CRITICAL", "HIGH", "MEDIUM", "LOW"}
    results = []
    failures = 0

    for incident in TEST_INCIDENTS:
        logger.info(f"\n  Incident {incident['id']} (expected: {incident['expected_tier_hint']}):")
        logger.info(f"    Synopsis: {incident['synopsis']}")

        result = classify_incident(model, incident["narrative"], incident["synopsis"])

        # Validation
        tier = result.get("risk_tier", "")
        reasoning = result.get("risk_reasoning", "")
        factors = result.get("key_factors", "")

        if tier not in valid_tiers:
            logger.error(f"    FAIL: Invalid tier returned: {tier}")
            failures += 1
        elif not reasoning or len(reasoning) < 10:
            logger.warning(f"    WARN: Reasoning too short or empty")
            logger.info(f"    Tier: {tier}")
        elif not factors:
            logger.warning(f"    WARN: No key factors returned")
            logger.info(f"    Tier: {tier}")
            logger.info(f"    Reasoning: {reasoning}")
        else:
            logger.info(f"    PASS:")
            logger.info(f"      Tier: {tier}")
            logger.info(f"      Reasoning: {reasoning}")
            logger.info(f"      Factors: {factors}")

        results.append({"id": incident["id"], "tier": tier, "expected": incident["expected_tier_hint"]})

        # Respect rate limits
        time.sleep(4.5)

    return results, failures


def print_summary(results, failures):
    """Print final summary of all tests."""
    logger.info("\n" + "=" * 60)
    logger.info("SMOKE TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total incidents tested: {len(results)}")
    logger.info(f"Failures: {failures}")

    logger.info("\nTier Distribution:")
    tier_counts = {}
    for r in results:
        tier_counts[r["tier"]] = tier_counts.get(r["tier"], 0) + 1
    for tier in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        count = tier_counts.get(tier, 0)
        logger.info(f"  {tier:10s}: {count}")

    # Other unexpected tiers
    for tier, count in tier_counts.items():
        if tier not in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            logger.warning(f"  {tier:10s}: {count} (unexpected)")

    logger.info("\nTier vs Expected:")
    for r in results:
        match_indicator = "OK" if r["tier"] in r["expected"] else "REVIEW"
        logger.info(f"  Incident {r['id']}: got '{r['tier']}', expected hint '{r['expected']}' [{match_indicator}]")

    logger.info("=" * 60)
    if failures == 0:
        logger.info("RESULT: All tests passed. Safe to run full classification.")
    else:
        logger.error(f"RESULT: {failures} failures detected. Fix issues before full run.")
    logger.info("=" * 60)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("AI CLASSIFICATION — SMOKE TEST")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    try:
        # Test 1: Env
        api_key = test_env_loading()

        # Test 2: Auth
        model = test_api_authentication(api_key)

        # Test 3: Classification
        results, failures = test_classification_quality(model)

        # Summary
        print_summary(results, failures)

        if failures > 0:
            sys.exit(1)

    except Exception as e:
        logger.error(f"\nSMOKE TEST FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()