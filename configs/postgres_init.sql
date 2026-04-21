-- =============================================================================
-- postgres_init.sql
-- Runs once on first boot of external_postgres_db.
-- Creates the airflow and mlflow databases alongside the default 'external' DB.
-- The default user is 'external' with password 'external' (from the compose file).
-- =============================================================================

-- Airflow metadata database
CREATE DATABASE airflow
    WITH OWNER = external
    ENCODING = 'UTF8';

-- MLflow backend store database
CREATE DATABASE mlflow
    WITH OWNER = external
    ENCODING = 'UTF8';