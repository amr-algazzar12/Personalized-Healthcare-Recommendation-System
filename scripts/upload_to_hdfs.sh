#!/usr/bin/env bash
# =============================================================================
# upload_to_hdfs.sh
# Runs validate.py then load_hdfs.py inside the namenode container.
# Called by: make upload-hdfs
#
# Usage (from host project root):
#   bash scripts/upload_to_hdfs.sh
# =============================================================================

set -euo pipefail

CONDA_ACTIVATE="source /root/anaconda/etc/profile.d/conda.sh && conda activate healthcare-rec"

echo "================================================================"
echo "  Step 1: Validate CSV files"
echo "================================================================"
docker exec -it namenode bash -c "
    $CONDA_ACTIVATE &&
    cd /home &&
    python -m src.ingestion.validate /home/data/raw
"

echo ""
echo "================================================================"
echo "  Step 2: Upload to HDFS"
echo "================================================================"
docker exec -it namenode bash -c "
    $CONDA_ACTIVATE &&
    cd /home &&
    python -m src.ingestion.load_hdfs /home/data/raw
"

echo ""
echo "================================================================"
echo "  Step 3: Verify HDFS directories"
echo "================================================================"
docker exec -it namenode bash -c "
    hdfs dfs -ls hdfs://namenode:9000/data/raw/
"

echo ""
echo "[DONE] CSV files are in HDFS. Run 'make create-hive-tables' next."