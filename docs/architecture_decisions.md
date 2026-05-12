Architecture Decision Record (ADR)
Subject: Ingestion Strategy & Schema Mapping

Status: Phase 1 (Ingestion Prototype) Complete

1. Current Implementation: Positional Index Mapping
   The initial ingestion module utilizes Integer-Location (iloc) Mapping instead of standard Header-Name mapping.

Reasoning: The NASA ASRS source dataset contains non-unique/duplicate headers and a multi-row metadata structure that causes standard pd.read_csv name-mapping to fail or return null values.

Outcome: By identifying the physical index of the 19 core columns, we achieved 100% data integrity for the Silver Layer.

2. Data Quality Tiering
   We have implemented a Tiered Governance Model to categorize the reliability of our data:

Tier 1 (Production Ready): 95%+ completeness. (Used for Core AI Training)..

Tier 2 (Analytical Grade): 70-94% completeness. (Used for Trend Analysis).

Tier 3 (Observation Only): <70% completeness. (Flagged for potential data gaps).

3. Future State: PySpark Dynamic Schema Discovery
   A future version will transition from this schema-specific script to a Distributed PySpark Pipeline.

Objective: Implement automatic schema discovery and data profiling using Spark column statistics.

Design Goal: Decouple the mapping logic into a JSON-based configuration layer. This will enable the pipeline to process any aviation maintenance dataset (ASRS, SDR, or private logs) regardless of the source schema.
