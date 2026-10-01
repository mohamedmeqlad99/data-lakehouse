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
