ARG SUPERSET_VERSION=6.0.0
FROM apache/superset:${SUPERSET_VERSION}

USER root

# Superset lean images do not bundle analytics database drivers.
# Trino's Python client provides both DB-API and SQLAlchemy support.
RUN . /app/.venv/bin/activate && \
    uv pip install "trino[sqlalchemy]"

COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap.sh /app/docker/bootstrap-customer360.sh
COPY write_trino_datasource.py /app/docker/write_trino_datasource.py
COPY create_customer360_dashboard.py /app/docker/create_customer360_dashboard.py
COPY check_customer360_queries.py /app/docker/check_customer360_queries.py

RUN chmod +x /app/docker/bootstrap-customer360.sh

USER superset

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

CMD ["/app/docker/bootstrap-customer360.sh"]
