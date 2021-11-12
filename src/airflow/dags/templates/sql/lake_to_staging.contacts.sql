WITH ids AS (
  SELECT *
  FROM lake.contacts
  WHERE ingestion_timestamp BETWEEN "{first_ingestion_timestamp}" AND "{last_ingestion_timestamp}"
)

SELECT
    c.partition_id AS client_partition_id,
    l.icentris_client,
    id,
    first_name,
    last_name,
    email,
    email2,
    email3,
    phone,
    phone2,
    phone3,
    twitter,
    city,
    state,
    zip,
    country,
    current_timestamp() as ingestion_timestamp
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY icentris_client, id
      ORDER BY ingestion_timestamp DESC
    ) AS rn
  FROM ids
) l
INNER JOIN system.clients c ON l.icentris_client = c.icentris_client
WHERE
  rn = 1
