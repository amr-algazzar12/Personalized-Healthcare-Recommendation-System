#!/usr/bin/env bash
# =============================================================================
# create_hive_tables.sh
# Ensures Hive metastore schema is initialized, then runs create_tables.hql
# via beeline inside the hive-server container.
# Called by: make create-hive-tables
# =============================================================================

set -euo pipefail

HQL_SRC="src/hive/create_tables.hql"
HIVE_CONTAINER="hive-server"
METASTORE_CONTAINER="hive-metastore"
HIVE_JDBC="jdbc:hive2://localhost:10000"

echo "================================================================"
echo "  Step 1: Verify hive-metastore is healthy"
echo "================================================================"

# Poll hive-metastore port 9083 for up to 2 minutes
READY=false
for i in $(seq 1 24); do
    if docker exec "$METASTORE_CONTAINER" bash -c \
        "echo > /dev/tcp/localhost/9083" 2>/dev/null; then
        echo "[OK] hive-metastore is up on port 9083"
        READY=true
        break
    fi
    echo "  Waiting for hive-metastore... ($i/24, 5s interval)"
    sleep 5
done

if [ "$READY" = false ]; then
    echo ""
    echo "[ERROR] hive-metastore did not come up after 2 minutes."
    echo ""
    echo "  Most likely cause: Hive metastore schema not initialized."
    echo "  Run this fix manually, then retry:"
    echo ""
    echo "    docker exec -it hive-metastore /opt/hive/bin/schematool -dbType postgres -initSchema"
    echo "    docker restart hive-metastore"
    echo "    docker logs hive-metastore -f"
    echo "    (wait for: 'Starting Hive Metastore Server')"
    echo ""
    exit 1
fi

echo ""
echo "================================================================"
echo "  Step 2: Copy HQL file into hive-server"
echo "================================================================"
docker cp "$HQL_SRC" "$HIVE_CONTAINER:/tmp/create_tables.hql"
echo "[OK] Copied $HQL_SRC → $HIVE_CONTAINER:/tmp/create_tables.hql"

echo ""
echo "================================================================"
echo "  Step 3: Run create_tables.hql via beeline"
echo "================================================================"
docker exec -it "$HIVE_CONTAINER" bash -c "
    beeline -u '$HIVE_JDBC' \
            -n root \
            --hiveconf hive.cli.print.header=true \
            --silent=false \
            -f /tmp/create_tables.hql
"

EXIT_CODE=$?

if [ "$EXIT_CODE" -eq 0 ]; then
    echo ""
    echo "================================================================"
    echo "  Hive tables created successfully."
    echo "  Verify with:"
    echo "    docker exec -it hive-server beeline -u '$HIVE_JDBC' -n root"
    echo "    USE healthcare; SHOW TABLES;"
    echo "================================================================"
else
    echo ""
    echo "[ERROR] beeline exited with code $EXIT_CODE."
    echo "  Check logs: docker logs hive-metastore --tail 50"
    exit "$EXIT_CODE"
fi