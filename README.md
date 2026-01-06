## Data Engineering Pipeline (portfolio project)

Production-style, Dockerized **end-to-end data engineering pipeline**:

- **Extract**: snapshot raw CSV
- **Validate**: schema + business-rule checks with threshold-based failure
- **Transform**: clean, standardize, dedupe
- **Load**: PostgreSQL warehouse (raw + staging schemas)
- **Model**: dbt models to create analytics-ready star schema (analytics schema)

### Architecture overview

- **Input**: `data/raw/financial_transactions.csv`
- **Pipeline (Python)**: `src/pipeline.py`
  - writes artifacts to `data/processed/`
  - loads to Postgres schemas: `raw`, `staging`, `analytics`
- **Warehouse DDL**: `sql/schema.sql`
- **Transformations (dbt)**: `dbt/` (sources from `staging`)

### Local setup (from scratch)

1) Start Postgres:

```bash
cd de-finance-pipeline
docker compose up -d --build
```

2) Run the full pipeline (one command):

```bash
make run
```

Or without Make:

```bash
docker compose --profile tools run --rm pipeline-runner
```

### Commands

- **Start DB**: `make up`
- **Run pipeline**: `make run`
- **Run dbt**: `make dbt`
- **Run tests**: `make test`
- **Stop and wipe volumes**: `make down`

If `make` is not available (common on Windows), use:

```bash
docker compose up -d --build
docker compose --profile tools run --rm pipeline-runner
docker compose --profile tools run --rm dbt run --project-dir /app/dbt
docker compose --profile tools run --rm pipeline-runner pytest -q
```

### Outputs and where to look

- **Raw snapshot**: `data/processed/raw_snapshot_<ts>.csv`
- **Validation report**: `data/processed/validation_report_<ts>.json`
- **Clean output**: `data/processed/clean_transactions_<ts>.csv`
- **Warehouse tables**:
  - `raw.financial_transactions_raw` (append-only, includes `ingestion_ts`)
  - `staging.financial_transactions` (cleaned, deduped)
  - `analytics.*` (dbt models)

### Data model (analytics star schema)

dbt builds:

- **dim_accounts**: one row per `account_id`, with first seen timestamp and location attributes
- **dim_merchants**: one row per `merchant_id`, merchant attributes and canonical category
- **fct_transactions**: one row per `transaction_id` (grain), amount/currency/status/date fields + dimension keys

### Environment variables (12-factor)

All connection details are controlled via env vars (compose uses safe defaults). Copy `.env.example` to `.env` if you want to override:

```bash
copy .env.example .env
```

Supported:
- `DATABASE_URL` (optional, takes precedence if set)
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `LOG_LEVEL`

### Cloud-ready notes (generic + Azure template)

The pipeline is designed to run as a container with env vars (no hardcoded credentials/paths).

- **Container runtime**:
  - Build `docker/pipeline.Dockerfile`
  - Provide Postgres connection via `DATABASE_URL` or discrete `POSTGRES_*`
  - Mount or bake in the input dataset (or swap the extractor to read from blob storage)
- **State/artifacts**:
  - For cloud runs, write `data/processed/` outputs to durable storage (Blob/S3/GCS) instead of local disk.

#### Azure Container Apps (template steps)

- Build and push images to ACR
- Create a Container App for the pipeline runner
- Set secrets/variables:
  - `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` (or `DATABASE_URL`)
- Optionally schedule runs with:
  - Azure Container Apps Jobs, or
  - Azure Functions / Logic Apps triggering the job

### How to extend

- **Orchestration**: wrap `python -m src.pipeline` as an Airflow DAG / Dagster job / Prefect flow
- **Incremental loads**: store watermark (e.g., max `posting_date` or ingestion timestamp) and only ingest new rows
- **Stronger DQ**: swap `src/validate.py` for Great Expectations suites and data docs
- **More marts**: add daily aggregates (e.g., spend by category/country) and SLA monitoring

### Checklist (quick start)

- **Run locally**: `docker compose up -d --build` then `make run`
- **Outputs**: check `data/processed/` for snapshots/reports/clean CSV
- **Run dbt models**: `make dbt`


