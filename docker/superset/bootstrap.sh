#!/usr/bin/env bash
set -euo pipefail

require_value() {
  local name="$1"
  local value="${2:-}"
  if [ -z "$value" ] || [ "$value" = "CHANGE_ME" ]; then
    echo "ERROR: $name must be configured before starting Superset." >&2
    exit 1
  fi
}

export SUPERSET_HOME="${SUPERSET_HOME:-/app/superset_home}"
mkdir -p "$SUPERSET_HOME"

require_value SUPERSET_SECRET_KEY "${SUPERSET_SECRET_KEY:-}"
require_value SUPERSET_TRINO_PASSWORD "${SUPERSET_TRINO_PASSWORD:-}"

echo "Initializing Superset metadata database..."
superset db upgrade

echo "Ensuring Superset admin user exists..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER:-admin}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Admin}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME:-User}" \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@lakehouse.local}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

superset init

echo "Importing Trino Sandbox datasource..."
datasource_file="$(python /app/docker/write_trino_datasource.py)"
if ! superset import_datasources -p "$datasource_file" -u "${SUPERSET_ADMIN_USER:-admin}"; then
  echo "WARN: datasource import failed. Superset will still start; create the Trino connection manually from the UI using docker/superset/README.md." >&2
fi

echo "Starting Superset on 0.0.0.0:8088..."
exec superset run -h 0.0.0.0 -p 8088 --with-threads
