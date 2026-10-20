-- Run inside Trino:
--   docker compose exec trino trino --catalog iceberg --execute "$(cat sql/examples.sql)"
-- or interactively:
--   docker compose exec trino trino --catalog iceberg

SHOW SCHEMAS;
SHOW TABLES FROM silver;
SHOW TABLES FROM gold;

SELECT * FROM gold.data_quality_summary ORDER BY dataset, metric;
SELECT * FROM gold.pipeline_monitoring ORDER BY status, dataset;
SELECT * FROM gold.data_governance_summary ORDER BY layer, table_name;
SELECT * FROM silver.inventory_adjustments ORDER BY event_id;
SELECT * FROM gold.inventory_availability ORDER BY warehouse, product_id;

SELECT
  committed_at,
  snapshot_id,
  parent_id,
  operation
FROM silver."inventory$snapshots"
ORDER BY committed_at DESC;

-- Replace the snapshot ID with the opening-stock snapshot to compare before MERGE.
-- SELECT * FROM silver.inventory FOR VERSION AS OF 1234567890123456789;

SELECT * FROM silver.orders ORDER BY order_date, order_id LIMIT 20;
SELECT * FROM silver.customers ORDER BY customer_id LIMIT 20;
SELECT * FROM silver.products ORDER BY product_id LIMIT 20;
SELECT * FROM silver.payments ORDER BY payment_id LIMIT 20;
SELECT * FROM silver.shipments ORDER BY shipment_id LIMIT 20;

SELECT
  segment,
  COUNT(*) AS customers,
  ROUND(AVG(CAST(DATE_DIFF('day', signup_date, CURRENT_DATE) AS DOUBLE)) / 30.0, 1) AS avg_days_since_signup
FROM silver.customers
GROUP BY segment
ORDER BY customers DESC;

SELECT
  product_name,
  category,
  unit_price,
  region
FROM silver.products
ORDER BY unit_price DESC;

SELECT
  payment_method,
  COUNT(*) AS payments,
  ROUND(SUM(amount), 2) AS total_amount
FROM silver.payments
GROUP BY payment_method
ORDER BY total_amount DESC;

SELECT
  delivery_status,
  COUNT(*) AS shipments
FROM silver.shipments
GROUP BY delivery_status
ORDER BY shipments DESC;

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

SELECT
  month_start,
  region,
  revenue,
  order_count,
  customer_count
FROM gold.monthly_revenue
ORDER BY month_start, region;

SELECT
  product_name,
  category,
  region,
  revenue,
  order_count,
  customer_count,
  avg_order_value
FROM gold.product_performance
ORDER BY revenue DESC;

SELECT
  customer_name,
  country,
  SUM(amount) AS total_order_value,
  MAX(shipment_status) AS latest_shipment_status
FROM gold.customer_order_summary
GROUP BY customer_name, country
ORDER BY total_order_value DESC
LIMIT 10;

SELECT
  c.segment,
  COUNT(DISTINCT c.customer_id) AS customers,
  ROUND(SUM(o.amount), 2) AS total_revenue,
  ROUND(AVG(o.amount), 2) AS avg_order_value
FROM silver.customers c
LEFT JOIN silver.orders o
  ON LOWER(TRIM(o.customer)) = LOWER(TRIM(c.customer_name))
GROUP BY c.segment
ORDER BY total_revenue DESC;

SELECT
  product_name,
  category,
  COUNT(*) AS orders,
  ROUND(SUM(amount), 2) AS revenue,
  ROUND(AVG(amount), 2) AS avg_order_value
FROM silver.orders
GROUP BY product_name, category
ORDER BY revenue DESC;

SELECT
  payment_method,
  COUNT(*) AS payments,
  ROUND(SUM(amount), 2) AS total_amount,
  ROUND(AVG(amount), 2) AS avg_payment
FROM silver.payments
GROUP BY payment_method
ORDER BY total_amount DESC;

SELECT
  region,
  COUNT(*) AS shipments,
  SUM(CASE WHEN delivery_status = 'delivered' THEN 1 ELSE 0 END) AS delivered,
  SUM(CASE WHEN delivery_status = 'in_transit' THEN 1 ELSE 0 END) AS in_transit
FROM silver.shipments
GROUP BY region
ORDER BY shipments DESC;
