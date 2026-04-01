# ✈️ Aircraft Maintenance Risk Triage Platform

## 🎯 Project Overview
Most maintenance data tells you **what** broke. [cite_start]This system tells you **how to prioritize what to fix — and why**. [cite: 88]

[cite_start]This is an end-to-end data engineering platform that ingests aviation safety reports (NASA ASRS), leverages AI classification to assign risk tiers with documented reasoning, and surfaces high-priority flags to operations teams. [cite: 88, 90]

### 🚀 Business Impact
* [cite_start]**Efficiency:** Reduces daily manual triage effort from ~83 hours to under 30 minutes. [cite: 20, 70]
* [cite_start]**Operational Gain:** Achieves an estimated **89% reduction** in manual review time by surfacing only HIGH and CRITICAL risk events. [cite: 71, 90]
* [cite_start]**Transparency:** Includes a mandatory `ai_reason` field for every classification to ensure a human-in-the-loop audit trail. [cite: 55, 90]

---

## 🏗️ Architecture (Medallion Pattern)
[cite_start]The platform follows the industry-standard **Medallion Architecture** used in high-scale environments: [cite: 38, 39]

1. [cite_start]**Bronze (Raw):** Untouched NASA ASRS CSV data stored in **AWS S3**. [cite: 41, 75]
2. [cite_start]**Silver (Cleaned):** **PySpark**-processed Parquet files featuring deduplication, normalized component names, and 30-day recurrence window calculations. [cite: 41, 79]
3. [cite_start]**AI Layer:** **OpenAI API** integration for automated risk tiering (CRITICAL to LOW) based on incident narratives. [cite: 48, 51]
4. [cite_start]**Gold (Warehouse):** Optimized Star Schema in **PostgreSQL** featuring Fact and Dimension tables for sub-second analytical queries. [cite: 41, 60]
5. [cite_start]**Orchestration:** Fully automated end-to-end pipeline via **Apache Airflow** DAG. [cite: 41, 79]

---

## 🛠️ Tech Stack
* [cite_start]**Languages:** Python (Ingestion/API), SQL (Analytics/Modeling), PySpark (Transformation). [cite: 75]
* [cite_start]**Infrastructure:** AWS S3, PostgreSQL. [cite: 75]
* [cite_start]**AI/ML:** OpenAI GPT-4o (Structured Risk Classification). [cite: 75]
* [cite_start]**Orchestration:** Apache Airflow. [cite: 75]
* [cite_start]**Interface:** Power BI (Operational Dashboard) & FastAPI (MCP-style Natural Language Query Layer). [cite: 75, 100]

---

## 🚧 Production Extensions (Scope Guard)
[cite_start]*Note: The following features are documented as design decisions for future versions to maintain build discipline:* [cite: 33, 34, 80]
* [cite_start]**Override Tracking:** Schema for human operators to flag and correct AI misclassifications. [cite: 14, 36]
* [cite_start]**Advanced Versioning:** Full prompt versioning tables and re-classification history. [cite: 36]
* [cite_start]**CI/CD Pipeline:** Automated unit testing for Spark transformations and dbt integration. [cite: 36]

---
[cite_start]**Build Period:** April 15 – May 30, 2026 [cite: 101]
[cite_start]**Target:** AI Data Platform Engineer (Aviation Domain) [cite: 97]
