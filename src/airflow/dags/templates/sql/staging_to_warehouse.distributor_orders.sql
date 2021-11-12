with staging_orders as (
  select icentris_client, order_id, tree_user_id, type, total, order_date from (
    select 
      icentris_client, order_id, tree_user_id, type, total, order_date,
      row_number() over (
        PARTITION BY order_id
        ORDER BY leo_eid DESC, ingestion_timestamp DESC
      ) as rn,
    from staging.orders o
    ) s
  where rn=1
),
staging_orders_12_months as (
  select * from staging_orders so
  where so.order_date >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 YEAR)
),
ids as (
  select icentris_client, tree_user_id, created, client_status, type as client_type from (
    select 
      icentris_client, tree_user_id, created, client_status, type,
      row_number() over (
        PARTITION BY tree_user_id
        ORDER BY leo_eid DESC, ingestion_timestamp DESC
      ) as rn,
    from staging.users
    ) s
  where rn=1
),
lifetime_autoship_orders as (
  select so.tree_user_id, 
  count(so.total) as lifetime_total_autoship_count,
  sum(so.total) as lifetime_total_autoship_amount,
  sum(so.total)/count(*) as lifetime_avg_autoship_amount
  from staging_orders so
  where so.type='Autoship'
  group by so.tree_user_id
),
rolling_12_month_autoship_orders as (
  select so.tree_user_id, 
  count(so.total) as rolling_12_month_total_autoship_count,
  sum(so.total) as rolling_12_month_total_autoship_amount,
  sum(so.total)/count(*) as rolling_12_month_avg_autoship_amount
  from staging_orders_12_months so
  where so.type='Autoship'
  group by so.tree_user_id
),
lifetime_wholesale_orders as (
  select so.tree_user_id, 
  count(*) as lifetime_total_wholesale_count,
  sum(so.total) as lifetime_total_wholesale_amount,
  sum(so.total)/count(*) as lifetime_avg_wholesale_amount
  from staging_orders so
  where so.type='Wholesale'
  group by so.tree_user_id
),
rolling_12_month_wholesale_orders as (
  select so.tree_user_id, 
  count(so.total) as rolling_12_month_total_wholesale_count,
  sum(so.total) as rolling_12_month_total_wholesale_amount,
  sum(so.total)/count(*) as rolling_12_month_avg_wholesale_amount
  from staging_orders_12_months so
  where so.type='Wholesale'
  group by so.tree_user_id
),
lifetime_retail_orders as (
  select so.tree_user_id, 
  count(*) as lifetime_total_retail_count,
  sum(so.total) as lifetime_total_retail_amount,
  sum(so.total)/count(*) as lifetime_avg_retail_amount
  from staging_orders so
  where so.type='retail'
  group by so.tree_user_id
),
rolling_12_month_retail_orders as (
  select so.tree_user_id, 
  count(so.total) as rolling_12_month_total_retail_count,
  sum(so.total) as rolling_12_month_total_retail_amount,
  sum(so.total)/count(*) as rolling_12_month_avg_retail_amount
  from staging_orders_12_months so
  where so.type='retail'
  group by so.tree_user_id
),
first_wholesale_order as (
  select tree_user_id, order_date, rn from (
    select 
      tree_user_id, order_date,
      ROW_NUMBER() OVER (
        PARTITION BY tree_user_id
        ORDER BY order_date
      ) as rn,
    from staging_orders so
    where so.type='Wholesale') s
    where s.rn=1
),
first_autoship_order as (
  select tree_user_id, order_date, rn from (
    select 
      tree_user_id, order_date,
      ROW_NUMBER() OVER (
        PARTITION BY tree_user_id
        ORDER BY order_date
      ) as rn,
    from staging_orders so
    where so.type='Autoship') s
    where s.rn=1
),
first_retail_order as (
  select tree_user_id, order_date, rn from (
    select 
      tree_user_id, order_date,
      ROW_NUMBER() OVER (
        PARTITION BY tree_user_id
        ORDER BY order_date
      ) as rn,
    from staging_orders so
    where so.type='retail') s
    where s.rn=1
),
last_wholesale_order as (
  select tree_user_id, order_date, rn from (
    select 
      tree_user_id, order_date,
      ROW_NUMBER() OVER (
        PARTITION BY tree_user_id
        ORDER BY order_date DESC
      ) as rn,
    from staging_orders so
    where so.type='Wholesale') s
    where s.rn=1
),
last_autoship_order as (
  select tree_user_id, order_date, rn from (
    select 
      tree_user_id, order_date,
      ROW_NUMBER() OVER (
        PARTITION BY tree_user_id
        ORDER BY order_date DESC
      ) as rn,
    from staging_orders so
    where so.type='Autoship') s
    where s.rn=1
),
last_retail_order as (
  select tree_user_id, order_date, rn from (
    select 
      tree_user_id, order_date,
      ROW_NUMBER() OVER (
        PARTITION BY tree_user_id
        ORDER BY order_date DESC
      ) as rn,
    from staging_orders so
    where so.type='retail') s
    where s.rn=1
)

select 
  'z/0' as leo_eid,
  c.partition_id AS client_partition_id,
  c.wrench_id AS client_wrench_id,
  c.icentris_client,
  ids.tree_user_id, 
  ids.client_status,
  ids.client_type,
  wo.lifetime_total_wholesale_count, 
  wo.lifetime_total_wholesale_amount,
  wo.lifetime_avg_wholesale_amount,
  wo_12.rolling_12_month_total_wholesale_count,
  wo_12.rolling_12_month_total_wholesale_amount,
  wo_12.rolling_12_month_avg_wholesale_amount,
  DATETIME_DIFF(fwo.order_date, ids.created, DAY) as days_to_first_wholesale_order,
  DATETIME_DIFF(CURRENT_DATETIME(), lwo.order_date, DAY) as days_since_last_wholesale_order,
  ao.lifetime_total_autoship_count, 
  ao.lifetime_total_autoship_amount,
  ao.lifetime_avg_autoship_amount,
  ao_12.rolling_12_month_total_autoship_count,
  ao_12.rolling_12_month_total_autoship_amount,
  ao_12.rolling_12_month_avg_autoship_amount,
  DATETIME_DIFF(fao.order_date, ids.created, DAY) as days_to_first_autoship_order,
  DATETIME_DIFF(CURRENT_DATETIME(), lao.order_date, DAY) as days_since_last_autoship_order,
  ro.lifetime_total_retail_count, 
  ro.lifetime_total_retail_amount,
  ro.lifetime_avg_retail_amount,
  ro_12.rolling_12_month_total_retail_count,
  ro_12.rolling_12_month_total_retail_amount,
  ro_12.rolling_12_month_avg_retail_amount,
  DATETIME_DIFF(fro.order_date, ids.created, DAY) as days_to_first_retail_order,
  DATETIME_DIFF(CURRENT_DATETIME(), lro.order_date, DAY) as days_since_last_retail_order,

from ids
INNER JOIN system.clients c ON c.icentris_client=ids.icentris_client 

left outer join lifetime_autoship_orders ao on ao.tree_user_id = ids.tree_user_id
left outer join rolling_12_month_autoship_orders ao_12 on ao_12.tree_user_id = ids.tree_user_id

left outer join lifetime_wholesale_orders wo on wo.tree_user_id = ids.tree_user_id
left outer join rolling_12_month_wholesale_orders wo_12 on wo_12.tree_user_id = ids.tree_user_id

left outer join lifetime_retail_orders ro on ro.tree_user_id = ids.tree_user_id
left outer join rolling_12_month_retail_orders ro_12 on ro_12.tree_user_id = ids.tree_user_id

left outer join first_wholesale_order fwo on fwo.tree_user_id = ids.tree_user_id
left outer join first_autoship_order fao on fao.tree_user_id = ids.tree_user_id
left outer join first_retail_order fro on fro.tree_user_id = ids.tree_user_id

left outer join last_wholesale_order lwo on lwo.tree_user_id = ids.tree_user_id
left outer join last_autoship_order lao on lao.tree_user_id = ids.tree_user_id
left outer join last_retail_order lro on lro.tree_user_id = ids.tree_user_id