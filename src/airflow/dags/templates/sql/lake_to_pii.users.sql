SELECT
    c.partition_id AS client_partition_id,
    c.wrench_id AS client_wrench_id,
    c.icentris_client,
    tu.leo_eid,
    CURRENT_TIMESTAMP() AS ingestion_timestamp,
    tu.id AS tree_user_id,
    first_name,
    last_name,
    company_name,
    CONCAT(address1,
        IF(address2 IS NOT NULL AND address2 <> '', ' ', ''),
        IF(address2 IS NOT NULL AND address2 <> '', address2, '')
    ) AS street,
    city,
    state,
    country,
    email,
    phone,
    mobile_phone,
    CASE
        WHEN gender IN ('F',
        'female') THEN 'female'
        WHEN gender IN ('M',
        'male') THEN 'male'
        ELSE 'none'
    END AS gender,
    birth_date,
    created_date as created
FROM (
  SELECT icentris_client, id, sponsor_id, user_type_id, rank_id, first_name, last_name, company_name, address1, address2, city, state, country, email, phone,
    mobile_phone, gender, birth_date, leo_eid, created_date,
    ROW_NUMBER() OVER (PARTITION BY icentris_client, id ORDER BY leo_eid DESC, ingestion_timestamp DESC) AS rn
  FROM lake.tree_users
  WHERE (ingestion_timestamp > "{first_ingestion_timestamp}" AND ingestion_timestamp <= "{last_ingestion_timestamp}")
) tu
INNER JOIN system.clients c ON tu.icentris_client = c.icentris_client
WHERE tu.rn = 1 AND tu.sponsor_id IS NOT NULL AND tu.user_type_id IS NOT NULL AND tu.rank_id IS NOT NULL
