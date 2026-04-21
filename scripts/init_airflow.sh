#!/usr/bin/env bash
# =============================================================================
# init_airflow.sh  — Step 1 of 3: initialise Airflow metadata DB
#
# Run inside namenode:
#   docker exec -it namenode bash /home/scripts/init_airflow.sh
#
# After this completes, run step 2:
#   docker exec -it namenode bash /home/scripts/init_airflow_step2.sh
# =============================================================================

set -euo pipefail

CONDA_INIT="/root/anaconda/etc/profile.d/conda.sh"
ENV_NAME="healthcare-rec"

[ -f "$CONDA_INIT" ] && . "$CONDA_INIT" && conda activate "$ENV_NAME" 2>/dev/null || true
export AIRFLOW_HOME=/root/airflow

echo "================================================================"
echo "  Airflow Init — Step 1: DB migration"
echo "================================================================"

# Verify Postgres connection first
python -c "
import psycopg2, sys
try:
    c = psycopg2.connect(host='external_postgres_db', user='external',
                         password='external', dbname='airflow', connect_timeout=5)
    c.close()
    print('[OK] Postgres reachable')
except Exception as e:
    print(f'[ERROR] Cannot reach Postgres: {e}')
    sys.exit(1)
"

echo "Running: airflow db init"
airflow db init

echo ""
echo "[DONE] Step 1 complete."
echo "Next: docker exec -it namenode bash /home/scripts/init_airflow_step2.sh"