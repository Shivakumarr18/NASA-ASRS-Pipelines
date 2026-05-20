# AI Classification Audit — Findings

## Methodology

A stratified random sample of 30 classifications was drawn from the AI-labelled dataset of 4,500 ASRS incident reports. The sample includes representation from each risk tier (CRITICAL, HIGH, MEDIUM, LOW) and one `ERROR_API` row to verify error handling.

For each row, the narrative, AI-assigned tier, AI reasoning, and key factors were reviewed manually. A human verdict was recorded in the `your_verdict` column of `audit_sample_30.csv`.

## Results

| Verdict | Count | Share |
|---|---|---|
| Agree with AI | 19 | 63% |
| Disagree — would rate more severe | 8 | 27% |
| Disagree — would rate less severe | 2 | 7% |
| ERROR_API (not auditable) | 1 | 3% |
| **Total** | **30** | **100%** |

## Key Observations

**1. Agreement rate of 63% on a stratified sample.**
The AI's risk-tier assignments align with human judgement on roughly two-thirds of cases. This is consistent with what would be expected from a general-purpose LLM applied to a specialised safety domain without fine-tuning.

**2. The AI tends to under-classify borderline incidents.**
Of the 10 disagreements, 8 were cases where I would have assigned a higher tier than the AI — typically pushing `HIGH` to `CRITICAL`. These were generally incidents where the outcome was safely contained but the safety margin was thin (e.g. rejected takeoffs with control issues, near-collisions handled by ATC, tire failures with potential loss of control on landing).

Only 2 disagreements went the other way — cases where the AI assigned `HIGH` but the incident felt closer to `MEDIUM` after reading the full narrative.

**3. ERROR_API rows are correctly tagged.**
The one error row in the sample was clearly labelled as an API failure rather than being silently assigned a tier. This confirms the error-handling design is working as intended.

## Implications for Production Use

The audit suggests two areas of improvement before this system could be used for real safety triage at scale:

- **Prompt tuning:** The classification prompt could weight "potential for escalation" more heavily, so that incidents with thin safety margins are pushed to `CRITICAL` rather than `HIGH`.
- **Human-in-the-loop on borderline cases:** Rather than relying solely on the AI for `HIGH` vs `CRITICAL` boundary cases, a production system should route ambiguous classifications to a human reviewer. The audit confirms that this is the boundary where the AI is weakest.

A 30-row sample is sufficient to surface directional findings but is not large enough to draw statistical conclusions. A production deployment would warrant a 500+ stratified sample with inter-rater reliability metrics.

## Honest Limitations

- Sample size of 30 is illustrative, not statistically robust.
- The audit was performed by a single reviewer (the project author); a second independent reviewer would strengthen the methodology.
- The `your_verdict` column uses natural-language values; a future version should use a standardised vocabulary (`AGREE` / `DISAGREE_HIGHER` / `DISAGREE_LOWER` / `ERROR`) for easier programmatic analysis.

## Files

- `audit_sample_30.csv` — the audited sample with human verdicts in column H
- `audit_classification.py` — the script that drew the stratified sample
