"""Load sample orders into Iceberg bronze / silver / gold tables."""

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("lakehouse-bootstrap").getOrCreate()

    for namespace in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    raw = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("/home/iceberg/data/orders.csv")
    )
    raw.createOrReplaceTempView("orders_csv")

    spark.sql(
        """
        CREATE OR REPLACE TABLE bronze.orders_raw
        USING iceberg
        AS SELECT * FROM orders_csv
        """
    )

    customers = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("/home/iceberg/data/customers.csv")
    )
    customers.createOrReplaceTempView("customers_csv")

    products = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("/home/iceberg/data/products.csv")
    )
    products.createOrReplaceTempView("products_csv")

    payments = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("/home/iceberg/data/payments.csv")
    )
    payments.createOrReplaceTempView("payments_csv")

    shipments = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("/home/iceberg/data/shipments.csv")
    )
    shipments.createOrReplaceTempView("shipments_csv")

    inventory = (
      spark.read.option("header", True)
      .option("inferSchema", True)
      .csv("/home/iceberg/data/inventory.csv")
    )
    inventory.createOrReplaceTempView("inventory_csv")

    inventory_adjustments = spark.read.json(
      "/home/iceberg/data/inventory_adjustments.jsonl"
    )
    inventory_adjustments.createOrReplaceTempView("inventory_adjustments_json")

    spark.sql(
        """
        CREATE OR REPLACE TABLE bronze.customers_raw
        USING iceberg
        AS SELECT * FROM customers_csv
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE bronze.products_raw
        USING iceberg
        AS SELECT * FROM products_csv
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE bronze.payments_raw
        USING iceberg
        AS SELECT * FROM payments_csv
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE bronze.shipments_raw
        USING iceberg
        AS SELECT * FROM shipments_csv
        """
    )

    spark.sql(
      """
      CREATE OR REPLACE TABLE bronze.inventory_raw
      USING iceberg
      AS SELECT * FROM inventory_csv
      """
    )

    spark.sql(
      """
      CREATE OR REPLACE TABLE bronze.inventory_adjustments_raw
      USING iceberg
      AS SELECT * FROM inventory_adjustments_json
      """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE silver.orders
        USING iceberg
        AS
        SELECT
          CAST(order_id AS BIGINT) AS order_id,
          TRIM(customer) AS customer,
          region,
          product,
          CAST(amount AS DECIMAL(10, 2)) AS amount,
          CAST(order_date AS DATE) AS order_date
        FROM bronze.orders_raw
        WHERE order_id IS NOT NULL
          AND customer IS NOT NULL
          AND amount IS NOT NULL
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE silver.customers
        USING iceberg
        AS
        SELECT
          CAST(customer_id AS BIGINT) AS customer_id,
          TRIM(customer_name) AS customer_name,
          LOWER(TRIM(email)) AS email,
          TRIM(city) AS city,
          TRIM(country) AS country,
          CAST(signup_date AS DATE) AS signup_date,
          TRIM(segment) AS segment
        FROM bronze.customers_raw
        WHERE customer_id IS NOT NULL
          AND customer_name IS NOT NULL
          AND email IS NOT NULL
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE silver.products
        USING iceberg
        AS
        SELECT
          CAST(product_id AS BIGINT) AS product_id,
          TRIM(product_name) AS product_name,
          TRIM(category) AS category,
          CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
          TRIM(region) AS region
        FROM bronze.products_raw
        WHERE product_id IS NOT NULL
          AND product_name IS NOT NULL
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE silver.payments
        USING iceberg
        AS
        SELECT
          CAST(payment_id AS BIGINT) AS payment_id,
          CAST(order_id AS BIGINT) AS order_id,
          TRIM(payment_method) AS payment_method,
          CAST(amount AS DECIMAL(10, 2)) AS amount,
          TRIM(payment_status) AS payment_status,
          CAST(payment_date AS DATE) AS payment_date
        FROM bronze.payments_raw
        WHERE payment_id IS NOT NULL
          AND order_id IS NOT NULL
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE silver.shipments
        USING iceberg
        AS
        SELECT
          CAST(shipment_id AS BIGINT) AS shipment_id,
          CAST(order_id AS BIGINT) AS order_id,
          TRIM(carrier) AS carrier,
          CAST(ship_date AS DATE) AS ship_date,
          TRIM(delivery_status) AS delivery_status,
          TRIM(region) AS region
        FROM bronze.shipments_raw
        WHERE shipment_id IS NOT NULL
          AND order_id IS NOT NULL
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE silver.inventory
        USING iceberg
        AS
        SELECT
          CAST(inventory_id AS BIGINT) AS inventory_id,
          CAST(product_id AS BIGINT) AS product_id,
          TRIM(warehouse) AS warehouse,
          CAST(quantity AS BIGINT) AS quantity,
          CAST(last_event_id AS BIGINT) AS last_event_id,
          CAST(updated_at AS TIMESTAMP) AS updated_at
        FROM bronze.inventory_raw
        WHERE inventory_id IS NOT NULL
          AND product_id IS NOT NULL
          AND quantity IS NOT NULL
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE silver.inventory_adjustments
        USING iceberg
        AS
        SELECT
          CAST(event_id AS BIGINT) AS event_id,
          CAST(inventory_id AS BIGINT) AS inventory_id,
          CAST(quantity_delta AS BIGINT) AS quantity_delta,
          CAST(event_time AS TIMESTAMP) AS event_time,
          TRIM(reason) AS reason
        FROM bronze.inventory_adjustments_raw
        WHERE event_id IS NOT NULL
          AND inventory_id IS NOT NULL
          AND quantity_delta IS NOT NULL
        """
    )

    spark.sql(
        """
        MERGE INTO silver.inventory AS target
        USING silver.inventory_adjustments AS source
          ON target.inventory_id = source.inventory_id
        WHEN MATCHED AND source.event_id > target.last_event_id THEN
          UPDATE SET
            quantity = target.quantity + source.quantity_delta,
            last_event_id = source.event_id,
            updated_at = source.event_time
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE gold.daily_revenue
        USING iceberg
        AS
        SELECT
          order_date,
          region,
          ROUND(SUM(amount), 2) AS revenue,
          COUNT(*) AS order_count
        FROM silver.orders
        GROUP BY order_date, region
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE gold.monthly_revenue
        USING iceberg
        AS
        SELECT
          CAST(DATE_TRUNC('MONTH', order_date) AS DATE) AS month_start,
          region,
          ROUND(SUM(amount), 2) AS revenue,
          COUNT(*) AS order_count,
          COUNT(DISTINCT customer) AS customer_count
        FROM silver.orders
        GROUP BY CAST(DATE_TRUNC('MONTH', order_date) AS DATE), region
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE gold.product_performance
        USING iceberg
        AS
        SELECT
          p.product_id,
          COALESCE(p.product_name, o.product) AS product_name,
          COALESCE(p.category, 'unknown') AS category,
          o.region,
          ROUND(SUM(o.amount), 2) AS revenue,
          COUNT(*) AS order_count,
          COUNT(DISTINCT o.customer) AS customer_count,
          ROUND(AVG(o.amount), 2) AS avg_order_value
        FROM silver.orders o
        LEFT JOIN silver.products p
          ON LOWER(TRIM(o.product)) = LOWER(TRIM(p.product_name))
        GROUP BY
          p.product_id,
          COALESCE(p.product_name, o.product),
          COALESCE(p.category, 'unknown'),
          o.region
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE gold.inventory_availability
        USING iceberg
        AS
        SELECT
          i.inventory_id,
          i.product_id,
          p.product_name,
          p.category,
          i.warehouse,
          i.quantity,
          i.updated_at,
          CASE
            WHEN i.quantity <= 0 THEN 'out_of_stock'
            WHEN i.quantity <= 10 THEN 'low_stock'
            ELSE 'in_stock'
          END AS stock_status
        FROM silver.inventory i
        LEFT JOIN silver.products p
          ON p.product_id = i.product_id
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE gold.customer_order_summary
        USING iceberg
        AS
        WITH payment_totals AS (
          SELECT order_id, ROUND(SUM(amount), 2) AS total_paid
          FROM silver.payments
          GROUP BY order_id
        ),
        latest_shipment AS (
          SELECT order_id, MAX(delivery_status) AS shipment_status
          FROM silver.shipments
          GROUP BY order_id
        )
        SELECT
          c.customer_id,
          c.customer_name,
          c.country,
          o.order_id,
          o.order_date,
          o.region,
          o.product,
          o.amount,
          COALESCE(pt.total_paid, 0) AS total_paid,
          COALESCE(ls.shipment_status, 'pending') AS shipment_status
        FROM silver.orders o
        LEFT JOIN silver.customers c
          ON LOWER(TRIM(o.customer)) = LOWER(TRIM(c.customer_name))
        LEFT JOIN payment_totals pt
          ON pt.order_id = o.order_id
        LEFT JOIN latest_shipment ls
          ON ls.order_id = o.order_id
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE gold.data_quality_summary
        USING iceberg
        AS
        WITH checks AS (
          SELECT 'orders' AS dataset, 'missing_order_id' AS metric, COUNT(*) AS value
          FROM bronze.orders_raw
          WHERE order_id IS NULL

          UNION ALL
          SELECT 'orders', 'missing_customer', COUNT(*)
          FROM bronze.orders_raw
          WHERE customer IS NULL

          UNION ALL
          SELECT 'orders', 'missing_amount', COUNT(*)
          FROM bronze.orders_raw
          WHERE amount IS NULL

          UNION ALL
          SELECT 'orders', 'invalid_order_date', COUNT(*)
          FROM bronze.orders_raw
          WHERE order_date IS NULL

          UNION ALL
          SELECT 'customers', 'missing_email', COUNT(*)
          FROM bronze.customers_raw
          WHERE email IS NULL OR TRIM(email) = ''

          UNION ALL
          SELECT 'customers', 'duplicate_email', COUNT(*)
          FROM (
            SELECT LOWER(TRIM(email)) AS email
            FROM bronze.customers_raw
            WHERE email IS NOT NULL
            GROUP BY LOWER(TRIM(email))
            HAVING COUNT(*) > 1
          )

          UNION ALL
          SELECT 'payments', 'missing_payment_status', COUNT(*)
          FROM bronze.payments_raw
          WHERE payment_status IS NULL OR TRIM(payment_status) = ''

          UNION ALL
          SELECT 'shipments', 'missing_delivery_status', COUNT(*)
          FROM bronze.shipments_raw
          WHERE delivery_status IS NULL OR TRIM(delivery_status) = ''

          UNION ALL
          SELECT 'inventory', 'negative_quantity', COUNT(*)
          FROM silver.inventory
          WHERE quantity < 0
        )
        SELECT
          dataset,
          metric,
          CAST(value AS BIGINT) AS issue_count,
          CURRENT_DATE AS checked_on
        FROM checks
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE gold.pipeline_monitoring
        USING iceberg
        AS
        WITH freshness AS (
          SELECT 'orders' AS dataset, MAX(order_date) AS latest_record_date, COUNT(*) AS row_count
          FROM silver.orders

          UNION ALL
          SELECT 'customers', MAX(signup_date), COUNT(*)
          FROM silver.customers

          UNION ALL
          SELECT 'payments', MAX(payment_date), COUNT(*)
          FROM silver.payments

          UNION ALL
          SELECT 'shipments', MAX(ship_date), COUNT(*)
          FROM silver.shipments

          UNION ALL
          SELECT 'inventory', CAST(MAX(updated_at) AS DATE), COUNT(*)
          FROM silver.inventory
        ),
        quality AS (
          SELECT dataset, SUM(issue_count) AS open_issues
          FROM gold.data_quality_summary
          GROUP BY dataset
        )
        SELECT
          f.dataset,
          f.latest_record_date,
          CAST(f.row_count AS BIGINT) AS row_count,
          COALESCE(q.open_issues, 0) AS open_issues,
          CASE
            WHEN COALESCE(q.open_issues, 0) = 0 THEN 'healthy'
            ELSE 'needs_attention'
          END AS status,
          CURRENT_DATE AS checked_on
        FROM freshness f
        LEFT JOIN quality q
          ON q.dataset = f.dataset
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE gold.data_governance_summary
        USING iceberg
        AS
        SELECT * FROM (
          VALUES
            ('bronze.orders_raw', 'orders', 'bronze', 'sales_ops', 'raw order feed', 'CSV source to bronze ingestion'),
            ('bronze.customers_raw', 'customers', 'bronze', 'crm_team', 'customer feed', 'CSV source to bronze ingestion'),
            ('bronze.products_raw', 'products', 'bronze', 'catalog_team', 'product feed', 'CSV source to bronze ingestion'),
            ('bronze.payments_raw', 'payments', 'bronze', 'finance_ops', 'payment feed', 'CSV source to bronze ingestion'),
            ('bronze.shipments_raw', 'shipments', 'bronze', 'ops_team', 'shipment feed', 'CSV source to bronze ingestion'),
            ('bronze.inventory_raw', 'inventory', 'bronze', 'warehouse_ops', 'opening inventory', 'CSV source to bronze ingestion'),
            ('bronze.inventory_adjustments_raw', 'inventory', 'bronze', 'warehouse_ops', 'inventory adjustments', 'JSON Lines source to bronze ingestion'),
            ('silver.orders', 'orders', 'silver', 'sales_ops', 'cleaned orders', 'bronze to silver transformation'),
            ('silver.customers', 'customers', 'silver', 'crm_team', 'cleaned customers', 'bronze to silver transformation'),
            ('silver.products', 'products', 'silver', 'catalog_team', 'cleaned products', 'bronze to silver transformation'),
            ('silver.payments', 'payments', 'silver', 'finance_ops', 'cleaned payments', 'bronze to silver transformation'),
            ('silver.shipments', 'shipments', 'silver', 'ops_team', 'cleaned shipments', 'bronze to silver transformation'),
            ('silver.inventory', 'inventory', 'silver', 'warehouse_ops', 'current inventory', 'CSV opening stock updated by JSON adjustment events'),
            ('silver.inventory_adjustments', 'inventory', 'silver', 'warehouse_ops', 'cleaned inventory events', 'bronze JSON Lines to typed Iceberg table'),
            ('gold.daily_revenue', 'revenue', 'gold', 'analytics_team', 'daily revenue', 'silver aggregate for reporting'),
            ('gold.monthly_revenue', 'revenue', 'gold', 'analytics_team', 'monthly revenue', 'silver aggregate for monthly reporting'),
            ('gold.product_performance', 'products', 'gold', 'analytics_team', 'product performance', 'silver product metrics for analysis'),
            ('gold.customer_order_summary', 'customers', 'gold', 'sales_ops', 'customer order view', 'joined customer and order data'),
            ('gold.data_quality_summary', 'quality', 'gold', 'data_engineering', 'quality metrics', 'checks on bronze sources'),
            ('gold.pipeline_monitoring', 'operations', 'gold', 'data_engineering', 'pipeline freshness', 'monitoring over silver and quality tables'),
            ('gold.inventory_availability', 'inventory', 'gold', 'warehouse_ops', 'inventory availability', 'silver inventory enriched with product details')
        ) AS governance(
          table_name,
          dataset,
          layer,
          owner,
          business_name,
          lineage
        )
        """
    )

    print("Lakehouse tables:")
    for row in spark.sql("SHOW TABLES IN bronze").collect():
        print(f"  bronze.{row.tableName}")
    for row in spark.sql("SHOW TABLES IN silver").collect():
        print(f"  silver.{row.tableName}")
    for row in spark.sql("SHOW TABLES IN gold").collect():
        print(f"  gold.{row.tableName}")

    spark.sql("SELECT * FROM gold.daily_revenue ORDER BY order_date, region").show(50, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

