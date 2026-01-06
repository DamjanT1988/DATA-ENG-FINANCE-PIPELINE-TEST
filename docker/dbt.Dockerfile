FROM python:3.11-slim

WORKDIR /app/dbt

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir dbt-postgres==1.8.2

# In local dev, docker-compose mounts `dbt/profiles.yml.example` to `/root/.dbt/profiles.yml`.
# In CI and cloud runtimes, provide `/root/.dbt/profiles.yml` via secret/volume or bake it into the image.
ENTRYPOINT ["dbt"]
CMD ["--version"]


