#!/usr/bin/env bash
# =============================================================================
# init_airflow_step2.sh  — Step 2 of 3: create Airflow admin user
#
# Run inside namenode AFTER init_airflow.sh completes:
#   docker exec -it namenode bash /home/scripts/init_airflow_step2.sh
#
# After this completes, run step 3:
#   docker exec -it namenode bash /home/scripts/init_airflow_step3.sh
# =============================================================================

set -euo pipefail

CONDA_INIT="/root/anaconda/etc/profile.d/conda.sh"
ENV_NAME="healthcare-rec"

[ -f "$CONDA_INIT" ] && . "$CONDA_INIT" && conda activate "$ENV_NAME" 2>/dev/null || true
export AIRFLOW_HOME=/root/airflow

echo "================================================================"
echo "  Airflow Init — Step 2: Create admin user"
echo "================================================================"

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@healthcare.local 2>&1 | grep -v "already exist" || true

echo ""
echo "[DONE] Step 2 complete. Login: admin / admin"
echo "Next: docker exec -it namenode bash /home/scripts/init_airflow_step3.sh"