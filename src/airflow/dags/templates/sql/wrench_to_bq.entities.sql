
SELECT DISTINCT
  e.entity_id
FROM
  wrench.entities e
  LEFT JOIN {table} insight ON e.entity_id = insight.entity_id
WHERE
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), e.ingestion_timestamp, HOUR) <= {hours}
  AND insight.entity_id IS NULL