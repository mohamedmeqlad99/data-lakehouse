-- Run inside Trino:
--   docker compose exec trino trino --catalog iceberg --execute "$(cat sql/examples.sql)"
-- or interactively:
--   docker compose exec trino trino --catalog iceberg

SHOW SCHEMAS;
SHOW TABLES FROM silver;
SHOW TABLES FROM gold;

SELECT * FROM silver.orders ORDER BY order_date, order_id LIMIT 20;
SELECT * FROM silver.customers ORDER BY customer_id LIMIT 20;
SELECT * FROM silver.products ORDER BY product_id LIMIT 20;
SELECT * FROM silver.payments ORDER BY payment_id LIMIT 20;
SELECT * FROM silver.shipments ORDER BY shipment_id LIMIT 20;

SELECT
  segment,
  COUNT(*) AS customers,
  ROUND(AVG(CAST(DATEDIFF(CURRENT_DATE, signup_date) AS DOUBLE)) / 30.0, 1) AS avg_days_since_signup
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
  customer_name,
  country,
  SUM(amount) AS total_order_value,
  MAX(shipment_status) AS latest_shipment_status
FROM gold.customer_order_summary
GROUP BY customer_name, country
ORDER BY total_order_value DESC
LIMIT 10;
