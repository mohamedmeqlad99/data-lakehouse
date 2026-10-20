.PHONY: up down logs bootstrap query spark-sql ps dag-check iceberg-history

COMPOSE_CMD := $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; elif command -v docker-compose >/dev/null 2>&1; then echo "docker-compose"; else echo "docker compose"; fi)

up:
	$(COMPOSE_CMD) up -d

down:
	$(COMPOSE_CMD) down

logs:
	$(COMPOSE_CMD) logs -f --tail=100

ps:
	$(COMPOSE_CMD) ps

bootstrap:
	$(COMPOSE_CMD) exec spark python /home/iceberg/jobs/bootstrap.py

query:
	$(COMPOSE_CMD) exec trino trino --catalog iceberg --execute "SELECT region, ROUND(SUM(revenue), 2) AS revenue, SUM(order_count) AS orders FROM gold.daily_revenue GROUP BY region ORDER BY revenue DESC"

spark-sql:
	$(COMPOSE_CMD) exec spark spark-sql --catalog demo

dag-check:
	python3 -c "from airflow.dags.lakehouse_daily_pipeline import build_dag; print(build_dag().task_ids)"

iceberg-history:
	$(COMPOSE_CMD) exec trino trino --catalog iceberg --file /home/iceberg/sql/iceberg_operations.sql
