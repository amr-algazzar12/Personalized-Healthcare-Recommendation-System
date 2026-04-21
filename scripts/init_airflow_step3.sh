#!/usr/bin/env bash
# =============================================================================
# init_airflow_step3.sh  — Step 3 of 3: register SSH connection + start daemons
#
# Run inside namenode AFTER init_airflow_step2.sh completes:
#   docker exec -it namenode bash /home/scripts/init_airflow_step3.sh
# =============================================================================

set -euo pipefail

CONDA_INIT="/root/anaconda/etc/profile.d/conda.sh"
ENV_NAME="healthcare-rec"

[ -f "$CONDA_INIT" ] && . "$CONDA_INIT" && conda activate "$ENV_NAME" 2>/dev/null || true
export AIRFLOW_HOME=/root/airflow

echo "================================================================"
echo "  Airflow Init — Step 3: SSH connection + start daemons"
echo "================================================================"

# Register SSH connection to hive-server
echo "Registering hive_server_ssh connection..."
airflow connections delete hive_server_ssh 2>/dev/null || true
airflow connections add hive_server_ssh \
    --conn-type ssh \
    --conn-host hive-server \
    --conn-login root \
    --conn-password "UPDATE_THIS_PASSWORD" \
    --conn-port 22

echo ""
echo "========================================================"
echo "  ACTION REQUIRED — update SSH password:"
echo "  1. Find hive-server root password:"
echo "       docker exec -it hive-server cat /etc/shadow | grep ^root"
echo "     (if shadow is unreadable, try the mrugankray README)"
echo "  2. Update in Airflow UI:"
echo "       http://localhost:3000"
echo "       Admin → Connections → hive_server_ssh → Edit"
echo "========================================================"
echo ""

# Start Airflow scheduler
echo "Starting Airflow scheduler..."
airflow scheduler --daemon --pid /tmp/airflow-scheduler.pid

# Give scheduler a moment to start
sleep 3

# Start Airflow webserver
echo "Starting Airflow webserver on port 3000..."
airflow webserver --port 3000 --daemon --pid /tmp/airflow-webserver.pid

sleep 3

# Verify webserver is up
if curl -sf http://localhost:3000/health > /dev/null 2>&1; then
    echo "[OK] Airflow webserver is up at http://localhost:3000"
else
    echo "[WARN] Webserver may still be starting. Check in 30 seconds."
fi

echo ""
echo "================================================================"
echo "  Airflow fully initialised."
echo "  UI:    http://localhost:3000"
echo "  Login: admin / admin"
echo "================================================================"