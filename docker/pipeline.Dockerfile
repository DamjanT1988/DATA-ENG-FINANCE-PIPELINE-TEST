FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

RUN apt-get update \
  && apt-get install -y --no-install-recommends curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir \
      pandas==2.2.3 \
      psycopg2-binary==2.9.9 \
      python-dateutil==2.9.0.post0 \
      dbt-postgres==1.8.2 \
      pytest==8.3.4

COPY src /app/src
COPY sql /app/sql
COPY dbt /app/dbt

CMD ["python", "-m", "src.pipeline"]


