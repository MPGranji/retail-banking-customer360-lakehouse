-- =============================================================================
-- Init PostgreSQL databases for this project without shell scripts.
-- This file runs under /docker-entrypoint-initdb.d and creates required DBs
-- before downstream services like Airflow and Iceberg catalog connect.
-- =============================================================================

\echo "Creating airflow database if missing"
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

\echo "Creating iceberg_catalog database if missing"
SELECT 'CREATE DATABASE iceberg_catalog'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'iceberg_catalog')\gexec

\echo "Database initialization complete"
