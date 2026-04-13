-- ============================================
-- NASA ASRS Gold Layer Analytics Queries
-- Database: nasa_asrs (MySQL)
-- ============================================

#Query 1 — Top 10 aircraft models by incident count:
SELECT a.make_model_name, COUNT(*) AS incident_count FROM fact_incidents f
JOIN dim_aircraft a ON f.aircraft_id = a.aircraft_id
GROUP BY a.make_model_name
ORDER BY incident_count DESC
LIMIT 10;

#Query 2 — Top 10 most failing components:
SELECT c.component_name, COUNT(*) AS failure_count FROM fact_incidents f
JOIN dim_component c ON f.component_id = c.component_id
WHERE c.component_name != 'UNKNOWN'
GROUP BY c.component_name
ORDER BY failure_count DESC
LIMIT 10;

#Query 3 — Incidents by quarter (trend detection):
SELECT t.year, t.quarter, COUNT(*) AS incident_count FROM fact_incidents f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.year, t.quarter
ORDER BY t.year, t.quarter;

#Query 4 — Most dangerous flight phases:
SELECT a.flight_phase, COUNT(*) AS incident_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_total
FROM fact_incidents f
JOIN dim_aircraft a ON f.aircraft_id = a.aircraft_id
WHERE a.flight_phase != 'UNKNOWN'
GROUP BY a.flight_phase
ORDER BY incident_count DESC;

#Query 5 — Night vs Day incidents:
SELECT e.light, COUNT(*) AS incident_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_total
FROM fact_incidents f
JOIN dim_environment e ON f.environment_id = e.environment_id
WHERE e.light != 'UNKNOWN'
GROUP BY e.light
ORDER BY incident_count DESC;

#Query 6 — Top 10 highest-risk aircraft + component combinations:
SELECT a.make_model_name, c.component_name, COUNT(*) AS incident_count
FROM fact_incidents f
JOIN dim_aircraft a ON f.aircraft_id = a.aircraft_id
JOIN dim_component c ON f.component_id = c.component_id
WHERE c.component_name != 'UNKNOWN'
GROUP BY a.make_model_name, c.component_name
ORDER BY incident_count DESC
LIMIT 10;