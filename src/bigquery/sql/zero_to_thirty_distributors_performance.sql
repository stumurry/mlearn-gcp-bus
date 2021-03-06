WITH sales AS (
  SELECT
    cu.tree_user_id AS commission_user_id,
    cu.created AS signup_date,
    COUNT(s.order_id) AS sales_count,
    ARRAY_AGG(STRUCT(s.order_id, s.order_date, s.total) ORDER BY s.order_date ASC lIMIT 1)[OFFSET (0)] AS first_sale,
    ARRAY_AGG(STRUCT(s.order_id, s.order_date, s.total) ORDER BY s.order_date DESC lIMIT 1)[OFFSET (0)] AS last_sale,
    COUNT(s.order_id) AS `active_sales_count`,
    SUM(IFNULL(s.total, 0)) AS `active_sales_total`
  FROM staging.users cu
  INNER JOIN (
    SELECT DISTINCT commission_user_id, tree_user_id, order_id, type, status, total, order_date
    FROM staging.orders
    WHERE `type` = 'Retail'
    AND `status` <> 'Cancelled'
  ) s ON cu.tree_user_id = s.commission_user_id
  WHERE cu.created BETWEEN '2014-06-01' AND '2018-05-01'
  AND DATE_DIFF(DATE(s.order_date), DATE(cu.created), DAY) BETWEEN 0 AND 502
  GROUP BY cu.tree_user_id, cu.created
), wholesale AS (
  SELECT
      o.commission_user_id,
      COUNT(o.order_id) AS `active_wholesale_count`,
      SUM(IFNULL(o.total,0)) AS `active_wholesale_total`
  FROM staging.users cu
  INNER JOIN (
    SELECT DISTINCT commission_user_id, tree_user_id, order_id, type, status, total, order_date
    FROM staging.orders
    WHERE `type` = 'Wholesale'
    AND `status` <> 'Cancelled'
  ) o ON cu.tree_user_id = o.commission_user_id
  WHERE cu.created BETWEEN '2014-06-01' AND '2018-05-01'
  AND DATE_DIFF(DATE(o.order_date), DATE(cu.created), DAY) BETWEEN 0 AND 502
  GROUP BY o.commission_user_id
)
SELECT
  s.commission_user_id,
  DATE_DIFF(DATE(first_sale.order_date), DATE(signup_date), DAY) AS days_to_first_sale,
  DATE_DIFF(DATE(last_sale.order_date), DATE(signup_date), DAY) AS days_to_last_sale,
  active_sales_count,
  active_wholesale_count,
  active_sales_total,
  active_wholesale_total
FROM sales s
JOIN wholesale w ON s.commission_user_id = w.commission_user_id
WHERE DATE_DIFF(DATE(first_sale.order_date), DATE(s.signup_date), DAY) BETWEEN 0 AND 30
ORDER BY active_sales_total DESC, active_wholesale_count DESC, days_to_last_sale DESC, days_to_first_sale ASC
