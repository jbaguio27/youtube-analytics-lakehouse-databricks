-- Placeholder Lakeflow Declarative Pipeline SQL.
-- Bronze -> Silver transformations will be added in subsequent iterations.

CREATE OR REFRESH MATERIALIZED VIEW silver.placeholder_silver_healthcheck
AS
SELECT
  current_timestamp() AS created_at_utc,
  'placeholder' AS status;
