-- Run inside Trino:
--   docker compose exec trino trino --catalog iceberg --execute "$(cat sql/examples.sql)"
-- or interactively:
--   docker compose exec trino trino --catalog iceberg

SHOW SCHEMAS;
SHOW TABLES FROM silver;

SELECT * FROM silver.orders ORDER BY order_date, order_id LIMIT 20;

SELECT
  region,
  ROUND(SUM(revenue), 2) AS revenue,
  SUM(order_count) AS orders
FROM gold.daily_revenue
GROUP BY region
ORDER BY revenue DESC;

SELECT
  order_date,
  SUM(revenue) AS revenue
FROM gold.daily_revenue
GROUP BY order_date
ORDER BY order_date;
