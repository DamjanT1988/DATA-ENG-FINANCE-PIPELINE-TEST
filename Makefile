.PHONY: up down logs run dbt dbt-compile test

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f

run:
	docker compose --profile tools run --rm pipeline-runner

dbt:
	docker compose --profile tools run --rm dbt run --project-dir /app/dbt

dbt-compile:
	docker compose --profile tools run --rm dbt deps --project-dir /app/dbt
	docker compose --profile tools run --rm dbt compile --project-dir /app/dbt

test:
	docker compose --profile tools run --rm pipeline-runner pytest -q


