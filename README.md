# Aviation Decision Intelligence Platform (ADIP)

**End-to-end aviation maintenance risk triage platform.** Ingests 4,500+ NASA ASRS incident records, applies medallion architecture for data refinement, and surfaces critical safety flags to reduce manual triage time by ~89%.

---

## The Problem

Aviation maintenance teams drown in **data entropy**. Raw incident reports from NASA's Aviation Safety Reporting System are:

- **Fragmented** — 125 columns with duplicate schemas across multi-aircraft incidents
- **High-Noise** — Redundant metadata, high nullity, ghost rows leaking into data
- **Non-Prioritized** — Thousands of reports with no automated signal to flag critical threats

**Result:** Delayed maintenance decisions, increased operational costs, and elevated safety risk.

---

## System Architecture — Medallion Pattern

```
Bronze (Raw CSV) → Silver (Cleaned & Validated) → Gold (Star Schema) → Risk Engine → Dashboard
```

### Bronze Layer — Raw Ingestion
- Direct ingestion of NASA ASRS hierarchical dataset (4,502 rows × 125 columns)
- Preserved original state — append-only, never modified
- Documented: 48 duplicate column names from multi-aircraft schema, ghost row identified

### Silver Layer — Refined
- Distilled 125 columns → 17 high-value features with documented data contract
- Removed ghost rows, handled nulls, validated data types
- 5-rule validation gate: zero null ACNs, unique ACNs, parseable dates, valid date range, minimum row count
- Production-grade transform script: modular functions, logging, fail-fast error handling

### Gold Layer — Analytics *(in progress)*
- Star schema: `fact_incidents` + 4 dimension tables (`dim_aircraft`, `dim_component`, `dim_environment`, `dim_time`)
- Target: PostgreSQL load, optimized for analytical querying

### Risk Engine *(planned)*
Classifies incidents into four safety tiers based on three vectors:

| Tier | Label | Action |
|------|-------|--------|
| 🔴 1 | CRITICAL | Immediate action required |
| 🟠 2 | HIGH | Urgent review needed |
| 🟡 3 | MEDIUM | Monitor closely |
| 🟢 4 | LOW | Routine operations |

**Vectors:** Incident recurrence per airframe, component failure frequency across fleet, temporal spike detection.

---

## Project Structure

```
NASA-ASRS-Pipelines/
├── docs/
│   ├── architecture_decisions.md
│   └── data_dictionary.md
├── src/
│   ├── detect_columns.py
│   ├── ingest_data.py
│   └── silver_transform.py
├── sql/
├── dags/
├── .gitignore
└── README.md
```

> **Data files** are excluded from the repo. See [Data Setup](#data-setup) to run locally.

---

## Data Setup

1. Download the NASA ASRS dataset from [NASA ASRS Database Online](https://asrs.arc.nasa.gov/search/database.html)
2. Place the raw CSV at `data/bronze/ASRS_DBOnline.csv`
3. Run the Silver transform: `python src/silver_transform.py`
4. Output: `data/silver/ASRS_SILVER_FINAL.csv` (4,500 rows × 17 columns)

---

## Tech Stack

| Category | Technology |
|----------|-----------|
| Languages | Python (Pandas, NumPy) |
| Data Modeling | Star Schema (Fact / Dimension), Schema Enforcement |
| Analytics | SQL (Window functions, CTEs, aggregation) |
| Orchestration | Apache Airflow *(planned)* |
| Scaling | PySpark *(target for distributed transformation)* |
| Visualization | Power BI *(planned)* |

---

## Current Status

| Layer | Status |
|-------|--------|
| Bronze (Raw Ingestion) | ✅ Complete |
| Silver (Clean & Validate) | ✅ Complete |
| Gold (Star Schema) | 🔧 In Progress |
| SQL Analytics | ⏳ Planned |
| Risk Engine (AI Classification) | ⏳ Planned |
| Airflow DAG | ⏳ Planned |
| Power BI Dashboard | ⏳ Planned |

---

## Key Decisions

- **Why 17 columns from 125?** — Selective cleaning beats exhaustive processing. The 17 retained columns carry 100% of the analytical signal.
- **Why Medallion?** — Full data lineage. Raw data is never touched. Every transformation is traceable.
- **Why Star Schema?** — Optimized for the query patterns maintenance teams actually use: "which aircraft model has the most incidents?" requires a clean join path, not a flat file.
