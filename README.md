# NASA ASRS Aviation Risk Triage Pipeline

End-to-end data engineering project on NASA's Aviation Safety Reporting System (ASRS) dataset. Ingests 4,500 pilot-submitted incident reports, applies medallion architecture for refinement, classifies each incident into risk tiers using AI, and surfaces patterns through a Power BI dashboard.

---

## What this project does

- Ingests raw NASA ASRS incident data (4,502 rows × 125 columns) with documented schema handling
- Cleans and validates through a Silver layer (17 high-value columns, 5-rule validation gate)
- Models a star schema in MySQL (Gold layer): one fact table, five dimension tables
- Runs analytical SQL queries on the Gold layer covering aircraft, components, time, and risk combinations
- Classifies each incident into risk tiers (CRITICAL / HIGH / MEDIUM / LOW) using OpenAI's GPT-4o-mini
- Manually audits a stratified random sample of 30 classifications to validate calibration
- Visualizes findings through a Power BI dashboard

---

## Architecture

Bronze (raw, locked)
↓
Silver (cleaned, 17 columns, validated)
↓
Gold (star schema in MySQL)
↓
SQL Analytics + AI Classification (OpenAI GPT-4o-mini)
↓

---

## The raw data challenge

The NASA ASRS data dictionary describes ~250 conceptual fields, but pandas loads only **125 physical columns** from the CSV. Why the gap?

The CSV uses a **mirrored schema**: each incident can involve two aircraft and two reporters. Aircraft 1 and Aircraft 2 share 35 identical column names; Person 1 and Person 2 share 11. This produces **48 duplicate column names** across **77 unique field names** in 125 total physical columns.

When pandas reads a duplicate name, the later column silently overwrites the earlier one in the DataFrame index. The Bronze layer documents this explicitly and preserves the raw file untouched. The Silver layer resolves the collision by selecting from Aircraft 1 / Person 1 only — appropriate for this project's risk-triage focus on primary aircraft. The design decision is documented in `notebooks/Bronze_Layer.ipynb`.

---

## Layer details

**Bronze** — Raw ingestion, append-only, never modified. 4,502 rows × 125 columns. Includes 1 ghost row (category header leak) which is documented and removed in Silver.

**Silver** — Cleaned and validated. 4,500 rows × 17 columns. Production-grade Python transform with:
- Modular functions and structured logging
- 5-rule validation gate (zero null ACNs, unique ACNs, parseable dates, valid date range, minimum row count)
- Fail-fast on validation breach
- Numeric nulls preserved as NaN; string nulls filled with `UNKNOWN` (conservative bias — never fabricate values for safety-critical data)

**Gold** — Star schema in MySQL. One fact table (`fact_incidents`, 4,500 rows × 11 columns) joined to five dimensions: `dim_aircraft`, `dim_component`, `dim_environment`, `dim_time`, `dim_narrative`. Foreign key validation and row count checks after joins to catch fan-out bugs.

**SQL Analytics** — 6 analytical queries on the Gold layer:
1. Top 10 aircraft models by incident count
2. Top 10 components by failure frequency
3. Quarterly incident trends
4. Highest-risk flight phases
5. Day vs night incidents
6. Top aircraft + component combinations (highest combination: Skyhawk 172 + Engine = 84 incidents)

**AI Classification** — Each narrative is classified into one of four risk tiers (CRITICAL / HIGH / MEDIUM / LOW) with model-generated reasoning and key factors per incident. Multi-provider design:
- Gemini gemini-2.5-flash-lite for free-tier architecture validation
- OpenAI gpt-4o-mini for production classification on the full dataset

A stratified random audit of 30 classifications validates calibration. Cost-controlled via OpenAI prepaid credits with hard cap.

**Power BI Dashboard** — Four-visual single-page report covering scale, risk-tier breakdown, top aircraft, and top aircraft + component combinations. Located in `BI Dashboard/`.

---

## Design principles

The project demonstrates four patterns of trustworthy AI / data systems:

- **Governance** — Bronze layer is append-only and read-only. Raw data is never modified. Every downstream output traces back to source.
- **Guardrails** — Silver layer fails loudly on bad input via a 5-rule validation gate. Silent failures sink systems; loud failures protect them.
- **Transparency** — Gold layer star schema includes foreign key validation and post-join row count checks. AI classification outputs include reasoning and key factors per incident, not just the label.
- **Observability** — A stratified manual audit of 30 AI classifications produces a human verdict file (`tests/audit_sample_30.csv`) as a permanent evidence artifact.

---

## Key design decisions

- **17 columns from 125 physical** — selective cleaning beats exhaustive processing. Aircraft 1 / Person 1 fields retained; Aircraft 2 / Person 2 dropped after documenting the mirrored-schema rationale.
- **pandas over Spark** — 4,500 rows doesn't justify distributed compute overhead. Spark would be appropriate at 4.5M+ rows.
- **MySQL over PostgreSQL** — available locally; sufficient for this dataset size.
- **Star schema over snowflake** — query patterns here are aggregation, not deep hierarchy traversal.
- **`dim_narrative` as a separate dimension** — every question should be answerable, including "what actually happened in this incident."
- **Multi-provider AI architecture** — vendor-agnostic, with Gemini for validation and OpenAI for production.
- **Numeric nulls preserved as NaN** — never fabricate values for safety-critical data.
- **Manual audit in addition to smoke testing** — automated tests verify the pipeline runs; human audit validates the AI is calibrated.

---

## Known limitations

- 4 of 4,500 incidents (0.09%) failed OpenAI classification due to transient API errors. These are tagged `ERROR_API` and excluded from analytics; preserved in source for traceability.
- The `component_name` field has a high proportion of `UNKNOWN` values from NASA's source data. Preserved as-is rather than dropped, to maintain auditability. ~25% of the top aircraft+component combinations involve UNKNOWN components — flagged as a data quality issue rather than hidden.
- The audit sample is proportional-stratified, so the LOW tier (7 incidents in the full dataset) is under-represented. A second equal-N stratified audit could be run to validate calibration on rare tiers.
- The pipeline runs locally on pandas + MySQL. Production scale (millions of rows) would require migration to S3 + Redshift, PySpark for Silver/Gold transforms, and Airflow for orchestration.

---

## Project structure

NASA-ASRS-Pipelines/
├── BI Dashboard/
│   └── Aviation_Risk_Dashboard.pbix          # Power BI report
├── notebooks/
│   └── Bronze_Layer.ipynb            # Raw data analysis & schema documentation
├── src/
│   ├── Silver_Transform.py           # Silver layer transformation
│   ├── Gold_Transform.py             # Gold star schema build
│   ├── ai_classify_gemini.py         # Gemini classification (free-tier)
│   ├── ai_classify_openai.py         # OpenAI classification (production)
│   ├── Columns_list.txt              # Documented column reference
│   └── sql/
│       └── gold_analytics.sql        # 6 analytical queries
├── tests/
│   ├── Smoke_Test_AI_Classify.py     # 5-incident smoke test
│   └── audit_classification.py       # Stratified audit sampler
├── docs/
│   └── architecture_decisions.md
├── .gitignore
└── README.md

> **Data files** are excluded from the repo. See [Data Setup](#data-setup) to run locally.

---
## Tech stack

| Category | Used |
|----------|------|
| Language | Python (pandas, numpy) |
| Database | MySQL |
| AI | OpenAI gpt-4o-mini (production), Google Gemini gemini-2.5-flash-lite (validation) |
| Visualization | Power BI Desktop |
| Version control | Git / GitHub |

---
## Data setup

1. Download the NASA ASRS dataset from [NASA ASRS Database Online](https://asrs.arc.nasa.gov/search/database.html)
2. Place the raw CSV at `data/bronze/ASRS_DBOnline.csv`
3. Set up a MySQL database named `nasa_asrs`
4. Create a `.env` file at the project root with:

OPENAI_API_KEY= AI_Key
MYSQL_USER= root
MYSQL_PASSWORD= *******

5. Run the pipeline in order:
   
python src/Silver_Transform.py
python src/Gold_Transform.py
python src/ai_classify_openai.py

6. Open `BI Dashboard/Final_Dashboard.pbix` in Power BI Desktop to view the dashboard.
---
## Status

| Layer | Status |
|-------|--------|
| Bronze | ✅ Complete |
| Silver | ✅ Complete |
| Gold (Star Schema) | ✅ Complete |
| SQL Analytics | ✅ Complete |
| AI Classification | ✅ Complete |
| Manual Audit | ✅ Complete |
| Power BI Dashboard | ✅ Complete |
---
## Author

Shiva Kumar — [GitHub](https://github.com/Shivakumarr18)
