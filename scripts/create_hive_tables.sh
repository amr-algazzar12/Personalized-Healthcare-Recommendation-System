#!/usr/bin/env bash
# =============================================================================
# create_hive_tables.sh
# Copies create_tables.hql into the hive-server container and executes
# it via beeline against HiveServer2.
# Called by: make create-hive-tables
#
# Usage (from host project root):
#   bash scripts/create_hive_tables.sh
# =============================================================================

set -euo pipefail

HQL_SRC="src/hive/create_tables.hql"
HQL_DEST="/tmp/create_tables.hql"
HIVE_CONTAINER="hive-server"
HIVE_JDBC="jdbc:hive2://localhost:10000"

echo "================================================================"
echo "  Creating Hive tables"
echo "================================================================"

# Copy HQL file into hive-server container
echo "Copying $HQL_SRC → $HIVE_CONTAINER:$HQL_DEST ..."
docker cp "$HQL_SRC" "$HIVE_CONTAINER:$HQL_DEST"

# Execute via beeline
echo "Running beeline..."
docker exec -it "$HIVE_CONTAINER" bash -c "
    beeline -u '$HIVE_JDBC' \
            -n root \
            --hiveconf hive.cli.print.header=true \
            --silent=false \
            -f '$HQL_DEST'
"

EXIT_CODE=$?

if [ "$EXIT_CODE" -eq 0 ]; then
    echo ""
    echo "================================================================"
    echo "  Hive tables created successfully."
    echo "  Verify via beeline:"
    echo "    docker exec -it hive-server beeline -u '$HIVE_JDBC' -n root"
    echo "    USE healthcare; SHOW TABLES;"
    echo "================================================================"
else
    echo ""
    echo "[ERROR] beeline exited with code $EXIT_CODE."
    echo "  Check that hive-metastore is healthy:"
    echo "    docker ps | grep hive"
    exit "$EXIT_CODE"
fi