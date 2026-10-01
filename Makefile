.PHONY: up down logs bootstrap query spark-sql ps

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

bootstrap:
	docker compose exec spark python /home/iceberg/jobs/bootstrap.py

query:
	docker compose exec trino trino --catalog iceberg --execute "SELECT region, ROUND(SUM(revenue), 2) AS revenue, SUM(order_count) AS orders FROM gold.daily_revenue GROUP BY region ORDER BY revenue DESC"

spark-sql:
	docker compose exec spark spark-sql --catalog demo
