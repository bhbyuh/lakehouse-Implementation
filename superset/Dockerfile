FROM apache/superset:6.1.0

USER root

# psycopg2 -> Postgres metadata DB, trino -> query Iceberg tables via Trino,
# redis -> cache backend. Superset runs from /app/.venv, so install into it.
RUN . /app/.venv/bin/activate && \
    uv pip install --no-cache \
      psycopg2-binary \
      trino \
      redis

USER superset