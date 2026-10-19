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

## Data governance and lineage

The lakehouse now includes a simple governance view to make ownership and lineage explicit:

- `gold.data_governance_summary` lists each table, dataset, layer, owner, business name, and lineage description.
- Bronze tables are owned by the source team that provides the raw feed.
- Silver tables are owned by the teams that define the cleaned business model.
- Gold tables are owned by analytics or data engineering depending on the downstream use case.

This creates a straightforward map from raw source feed to business-ready reporting.

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
sample-data/orders.csv
trino/catalog/iceberg.properties
sql/examples.sql
```

## Stop

```bash
docker compose down
```

MinIO data lives in the `minio-data` volume. Remove it with `docker compose down -v` if you want a clean warehouse.
