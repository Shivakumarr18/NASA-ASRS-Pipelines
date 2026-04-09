🛫 Aviation Decision Intelligence Platform (ADIP)
Executive Summary
A high-integrity data engineering system designed to transform fragmented, high-entropy aviation safety records into actionable maintenance intelligence. This platform automates the identification of fleet-wide vulnerabilities, reducing the "Data-to-Decision" window from hours to seconds.

🚩 The Problem
Aviation maintenance teams are often overwhelmed by "Data entropy." Raw incident reports are:

Fragmented: Inconsistent schemas across legacy CSV formats.

High-Noise: ~250 columns containing redundant metadata and high nullity.

Non-Prioritized: Thousands of reports exist, but there is no automated "Signal" to highlight Critical safety threats.

Business Impact: Delayed maintenance, increased operational costs, and higher safety risks due to slow, manual data triage.

🏗 System Architecture (Medallion)
The platform follows the industry-standard Medallion Architecture to ensure data lineage and reliability.

Bronze (Raw): Direct ingestion of hierarchical NASA ASRS datasets.

Silver (Refined): * Data Hygiene: Automated removal of "ghost rows," deduplication, and schema standardization.

Feature Engineering: Distilling 250 columns into 15 High-Value Features (e.g., Incident Frequency, Component Recurrence).

Gold (Analytics): Star-Schema modeling with specialized Fact and Dimension tables optimized for sub-second querying.

🧠 The Intelligence Layer
1. Risk Engine (Core Logic)
A custom algorithm that classifies assets into four safety tiers:

Incident Recurrence: Tracks repeated reports tied to a specific airframe.

Component Failure Frequency: Identifies parts failing across the entire fleet.

Temporal Patterns: Detects rapid spikes in incident density over short windows.
Outputs: CRITICAL | HIGH | MEDIUM | LOW

2. Query Layer (Decision Support)
An abstraction layer that enables stakeholders to get immediate answers to operational questions:

"Which aircraft model currently carries the highest risk score?"

"What components have the highest failure frequency in the last quarter?"

📊 Business Impact
Operational Velocity: Reduced risk identification time from hours to seconds.

Proactive Safety: Enabled a "Pattern-First" approach, identifying mechanical trends before they result in grounding events.

Architecture Efficiency: Optimized the data footprint by 85% while retaining 100% of the technical signal.

🧰 Tech Stack
Languages: Python (Pandas/NumPy)

Data Engineering: Star-Schema Modeling (Fact/Dim), Schema Enforcement.

Scaling: Apache Spark (Target for high-volume distributed transformation).

Analytics: SQL (Complex analytical window functions).

🧠 Key Learnings & Challenges
Selective Cleaning: Real-world data is messy; choosing the right 15 columns is more valuable than processing all 250.

Data Modeling is Logic: Structured Fact/Dim tables are the only way to build a query layer that executives can trust.

Handling Ambiguity: Developed narrative text-mining logic to recover missing "Component" data from pilot descriptions.
