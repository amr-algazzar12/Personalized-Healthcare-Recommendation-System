#!/usr/bin/env bash
# Quick Hive schema init (fixed params)
set -euo pipefail

echo ">>> Initialising Hive metastore schema..."
docker exec -it hive-metastore /opt/hive/bin/schematool -dbType postgres -connUrl 'jdbc:postgresql://hive-metastore-postgresql:5432/metastore' -userName hive -passWord hive -initSchema

echo "Schema initialized."
echo "Restarting hive-metastore for safety..."
docker restart hive-metastore
sleep 20
docker logs hive-metastore --tail 15

echo "Done. Run: make verify"

