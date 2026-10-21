# data-lakehouse

Local **data lakehouse**: object storage (MinIO / S3), an Iceberg REST catalog, Spark for writes, and Trino for SQL.

```text
CSV sample ──► Spark ──► Iceberg tables on MinIO
                              ▲
                              │
                         REST catalog
                              │
                         Trino (SQL)
```

Medallion layers after bootstrap:

| Layer | Table | What it is |
| --- | --- | --- |
| bronze | `bronze.orders_raw` | Raw CSV as ingested |
| silver | `silver.orders` | Typed, nulls dropped |
| gold | `gold.daily_revenue` | Revenue by day and region |
| gold | `gold.monthly_revenue` | Revenue, orders, and customers by month and region |
| gold | `gold.product_performance` | Product revenue and order metrics by region |
| gold | `gold.data_quality_summary` | Checks for missing or invalid records per dataset |
| gold | `gold.pipeline_monitoring` | Data freshness and issue load per pipeline stage |
| gold | `gold.data_governance_summary` | Ownership, lineage, and business definition for each table |
| gold | `gold.inventory_availability` | Current stock levels and availability by warehouse |

## Data governance and lineage

The lakehouse now includes a simple governance view to make ownership and lineage explicit:

- `gold.data_governance_summary` lists each table, dataset, layer, owner, business name, and lineage description.
- Bronze tables are owned by the source team that provides the raw feed.
- Silver tables are owned by the teams that define the cleaned business model.
- Gold tables are owned by analytics or data engineering depending on the downstream use case.

This creates a straightforward map from raw source feed to business-ready reporting.

## Multiple formats and ACID writes

The sample pipeline ingests CSV sources and JSON Lines inventory adjustment events, normalizes them into Iceberg tables, and stores the Iceberg data files as Parquet. This demonstrates how the same lakehouse can combine different source formats behind one table and query layer.

During bootstrap, Spark applies the JSON adjustment events to `silver.inventory` with an Iceberg `MERGE`. The event ID guard makes rerunning the merge safe for the same event, and each successful merge is committed as a new Iceberg snapshot. Query current stock and inspect the snapshots in Trino:

```sql
SELECT * FROM gold.inventory_availability ORDER BY warehouse, product_id;

SELECT committed_at, snapshot_id, parent_id, operation
FROM silver."inventory$snapshots"
ORDER BY committed_at DESC;
```

This demonstrates Iceberg ACID behavior:

- **Atomicity:** the inventory merge is committed as one table snapshot, not as partially visible row writes.
- **Consistency:** typed quantities and the event ID guard preserve the sample inventory update rules.
- **Isolation:** readers see a consistent snapshot while a write is being committed.
- **Durability:** committed table data and metadata are stored in MinIO and tracked by Iceberg.

Use a snapshot ID from the history to inspect the opening stock before the merge:

```sql
SELECT *
FROM silver.inventory FOR VERSION AS OF 1234567890123456789
ORDER BY inventory_id;
```

The example data is in `sample-data/inventory.csv` and `sample-data/inventory_adjustments.jsonl`.

## Prerequisites

- Docker and Docker Compose v2
- About 6 GB RAM free for Spark + Trino

On Ubuntu, if `docker` is missing:

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-v2
sudo usermod -aG docker "$USER"
# log out and back in, then:
docker compose up -d
```

## Start

Use the Compose command available on your machine. The project supports both Docker Compose variants:

```bash
cp .env.example .env   # optional; defaults already match compose

# Docker Compose v2
 docker compose up -d

# Legacy Compose binary
 docker-compose up -d
```

Wait until `docker compose ps` or `docker-compose ps` shows Spark and Trino running, then load the sample:

```bash
make bootstrap
# or:
docker compose exec spark python /home/iceberg/jobs/bootstrap.py
# or:
docker-compose exec spark python /home/iceberg/jobs/bootstrap.py
```

## Dashboard

A Streamlit dashboard is available for viewing live lakehouse metrics. Start the dashboard container with:

```bash
make dashboard
```

Then open:

```text
http://localhost:8501
```

The dashboard reads directly from Trino over the Compose network and shows revenue and order KPIs, inventory by warehouse, data quality issues, and recent Iceberg inventory snapshots. Use **Refresh live data** to bypass the 15-second cache. Streamlit and the Trino Python client are installed in the dashboard image.

## Query

Trino on [http://localhost:8088](http://localhost:8088) (no auth). CLI:

```bash
docker compose exec trino trino --catalog iceberg
# or:
docker-compose exec trino trino --catalog iceberg
```

Then:

```sql
SHOW SCHEMAS;
SELECT * FROM gold.daily_revenue ORDER BY order_date, region;
```

One-shot:

```bash
make query
```

## Iceberg snapshots and time travel

Inspect the recent snapshots, table history, and current data files for `gold.daily_revenue`:

```bash
make iceberg-history
```

To query a prior version, copy a `snapshot_id` from the snapshots output and use it in Trino:

```sql
SELECT *
FROM gold.daily_revenue FOR VERSION AS OF 1234567890123456789
ORDER BY order_date, region;
```

The example SQL is in `sql/iceberg_operations.sql`. The version query is commented out there until you replace the sample ID with a real snapshot ID. These examples are read-only; snapshot rollback and expiration are intentionally left as explicit operator actions.

Spark SQL:

```bash
docker compose exec spark spark-sql
```

Jupyter in the Spark image: [http://localhost:8888](http://localhost:8888)  
Spark UI: [http://localhost:8080](http://localhost:8080)  
MinIO console: [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin`)  
Iceberg REST: [http://localhost:8181](http://localhost:8181)

Credentials are **local-only** defaults. Do not reuse them outside this machine.

## Layout

```text
docker-compose.yml
spark/conf/spark-defaults.conf   # Iceberg REST + S3A
spark/jobs/bootstrap.py          # bronze / silver / gold load
airflow/dags/lakehouse_daily_pipeline.py  # scheduled orchestration definition
sample-data/orders.csv
sample-data/inventory.csv
sample-data/inventory_adjustments.jsonl
trino/catalog/iceberg.properties
sql/examples.sql
```

## Orchestration

The project now includes an Airflow-style DAG definition for the daily lakehouse refresh workflow:

```python
from airflow.dags.lakehouse_daily_pipeline import build_dag

dag = build_dag()
```

The DAG runs in this order:

1. `bootstrap_lakehouse`
2. `quality_check`
3. `analytics_snapshot`

You can validate the definition with:

```bash
make dag-check
```

## Stop

```bash
docker compose down
```

MinIO data lives in the `minio-data` volume. Remove it with `docker compose down -v` if you want a clean warehouse.
