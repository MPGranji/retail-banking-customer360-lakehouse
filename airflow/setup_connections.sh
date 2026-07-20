#!/bin/bash
# =============================================================================
# Setup Airflow connections — chạy sau khi airflow-webserver healthy
# Usage: docker exec airflow-webserver bash /opt/project/airflow/setup_connections.sh
# =============================================================================

set -e

# Secrets are supplied by docker/.env through the Airflow service env_file.
# Fail before creating connections if any required secret is missing.
: "${ORACLE_PASSWORD:?ORACLE_PASSWORD is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
: "${PII_HASH_SALT:?PII_HASH_SALT is required}"

ORACLE_USER="${ORACLE_USER:-core_banking}"
POSTGRES_USER="${POSTGRES_USER:-card_crm}"
POSTGRES_DB="${POSTGRES_DB:-card_crm}"

add_or_replace() {
  airflow connections delete "$1" 2>/dev/null || true
  airflow connections add "$@"
  echo "  OK: $1"
}

echo "=== Setting up Airflow connections ==="

# Oracle Core Banking
# conn-type must be 'jdbc' so that jdbc_jinja_args() gets the full URL from conn.host
add_or_replace 'oracle-core' \
  --conn-type 'jdbc' \
  --conn-host 'jdbc:oracle:thin:@//oracle:1521/XEPDB1' \
  --conn-login "${ORACLE_USER}" \
  --conn-password "${ORACLE_PASSWORD}"

# PostgreSQL — dùng cho flag_job_etl sensor và card/CRM source
add_or_replace 'postgres-etl' \
  --conn-type 'postgres' \
  --conn-host 'postgres' \
  --conn-port 5432 \
  --conn-schema "${POSTGRES_DB}" \
  --conn-login "${POSTGRES_USER}" \
  --conn-password "${POSTGRES_PASSWORD}"

# PostgreSQL Card/CRM source (Bronze card_crm DAG)
# conn-type must be 'jdbc' so that jdbc_jinja_args() gets the full URL from conn.host
add_or_replace 'postgres-card-crm' \
  --conn-type 'jdbc' \
  --conn-host "jdbc:postgresql://postgres:5432/${POSTGRES_DB}" \
  --conn-login "${POSTGRES_USER}" \
  --conn-password "${POSTGRES_PASSWORD}"

# Spark — SparkSubmitOperator dùng spark_default
add_or_replace 'spark_default' \
  --conn-type 'spark' \
  --conn-host 'spark://spark-master' \
  --conn-port 7077

echo ""
echo "=== Setting up Airflow variables ==="

airflow variables set pii_hash_salt "${PII_HASH_SALT}"
echo "  OK: pii_hash_salt"

echo ""
echo "=== Done. Connections list ==="
airflow connections list
