#!/usr/bin/env bash
# =============================================================================
# verify_versions.sh
# Verifies installed versions and connectivity inside the namenode container.
#
# Usage (from host):
#   docker exec -it namenode bash /home/scripts/verify_versions.sh
#
# Exit codes:
#   0 — all checks passed or only warnings
#   1 — a critical connectivity check failed (HDFS, Postgres)
# =============================================================================

WARN=0
FAIL=0

pass()  { echo "  [PASS] $1"; }
warn()  { echo "  [WARN] $1"; WARN=$((WARN + 1)); }
fail()  { echo "  [FAIL] $1"; FAIL=$((FAIL + 1)); }
note()  { echo "         $2"; }

echo "================================================================"
echo "  Version Verification — namenode container"
echo "================================================================"
echo ""

# ---------------------------------------------------------------------------
# Java
# The namenode image ships with Java 8 (openjdk 1.8.x).
# Java 8 is fully supported by Hadoop 3.2.1 and Spark 3.2.2.
# ---------------------------------------------------------------------------
echo "--- Core ---"
JAVA_VER=$(java -version 2>&1 | head -1)
if echo "$JAVA_VER" | grep -qE "1\.8\.|11\.|17\."; then
    pass "Java: $JAVA_VER"
else
    warn "Java: $JAVA_VER"
    note "" "Expected Java 8, 11, or 17. Verify Hadoop/Spark compatibility."
fi

# ---------------------------------------------------------------------------
# Python
# ---------------------------------------------------------------------------
PYTHON_VER=$(python --version 2>&1)
if echo "$PYTHON_VER" | grep -q "3\.9"; then
    pass "Python: $PYTHON_VER"
else
    warn "Python: $PYTHON_VER"
    note "" "Expected Python 3.9. Update environment.yml if different."
fi

# ---------------------------------------------------------------------------
# Conda
# Conda changed versioning from 4.x to YY.M.patch after version 4.14.
# 23.x, 24.x are all valid modern conda versions.
# ---------------------------------------------------------------------------
CONDA_VER=$(conda --version 2>&1)
if echo "$CONDA_VER" | grep -qE "conda (4\.|23\.|24\.|25\.)"; then
    pass "Conda: $CONDA_VER"
else
    warn "Conda: $CONDA_VER"
    note "" "Could not detect a known conda version. Check manually."
fi

# ---------------------------------------------------------------------------
# Hadoop
# ---------------------------------------------------------------------------
HADOOP_VER=$(hadoop version 2>/dev/null | head -1)
if echo "$HADOOP_VER" | grep -q "3\.2\.1"; then
    pass "Hadoop: $HADOOP_VER"
else
    warn "Hadoop: $HADOOP_VER"
    note "" "Expected Hadoop 3.2.1."
fi

# ---------------------------------------------------------------------------
# Spark
# spark-submit --version outputs a multi-line banner. The version is on the
# line that starts with 'version', not line 1.
# ---------------------------------------------------------------------------
SPARK_VER=$(spark-submit --version 2>&1 | grep -i "version" | grep -v "Welcome\|Using\|Branch\|Compiled\|Revision\|Url\|Type" | head -1)
if [ -z "$SPARK_VER" ]; then
    # Fallback: grab any line containing a version number pattern
    SPARK_VER=$(spark-submit --version 2>&1 | grep -oE "[0-9]+\.[0-9]+\.[0-9]+" | head -1)
    SPARK_VER="version $SPARK_VER"
fi
if echo "$SPARK_VER" | grep -qE "3\.[0-9]"; then
    pass "Spark: $SPARK_VER"
else
    warn "Spark: $SPARK_VER (raw output — check manually)"
    note "" "Run: spark-submit --version 2>&1 | grep -i version"
fi

# ---------------------------------------------------------------------------
# Airflow
# ---------------------------------------------------------------------------
AIRFLOW_VER=$(airflow version 2>/dev/null || echo "not found")
if echo "$AIRFLOW_VER" | grep -qE "2\.[0-9]"; then
    pass "Airflow: $AIRFLOW_VER"
else
    warn "Airflow: $AIRFLOW_VER"
    note "" "Expected Airflow 2.x. Check if healthcare-rec conda env is active."
fi

echo ""
echo "--- Python packages (active conda env) ---"

# ---------------------------------------------------------------------------
# PySpark
# ---------------------------------------------------------------------------
PYSPARK_VER=$(python -c "import pyspark; print(pyspark.__version__)" 2>/dev/null || echo "not found")
if echo "$PYSPARK_VER" | grep -qE "3\.[0-9]"; then
    pass "PySpark: $PYSPARK_VER"
else
    warn "PySpark: $PYSPARK_VER"
    note "" "Run 'make install-conda' to install the healthcare-rec environment."
fi

# ---------------------------------------------------------------------------
# MLflow
# ---------------------------------------------------------------------------
MLFLOW_VER=$(python -c "import mlflow; print(mlflow.__version__)" 2>/dev/null || echo "not found")
if echo "$MLFLOW_VER" | grep -qE "2\.[0-9]"; then
    pass "MLflow: $MLFLOW_VER"
else
    warn "MLflow: $MLFLOW_VER"
fi

# ---------------------------------------------------------------------------
# XGBoost
# ---------------------------------------------------------------------------
XGB_VER=$(python -c "import xgboost; print(xgboost.__version__)" 2>/dev/null || echo "not found")
if echo "$XGB_VER" | grep -qE "1\.[5-9]\.|2\."; then
    pass "XGBoost: $XGB_VER"
else
    warn "XGBoost: $XGB_VER"
    note "" "Expected xgboost 1.7.x. Run 'make install-conda'."
fi

# ---------------------------------------------------------------------------
# Pandas
# ---------------------------------------------------------------------------
PANDAS_VER=$(python -c "import pandas; print(pandas.__version__)" 2>/dev/null || echo "not found")
if echo "$PANDAS_VER" | grep -qE "1\.[0-9]\.|2\."; then
    pass "Pandas: $PANDAS_VER"
else
    warn "Pandas: $PANDAS_VER"
fi

echo ""
echo "--- Connectivity (critical — failures here are blockers) ---"

# ---------------------------------------------------------------------------
# HDFS — critical check
# ---------------------------------------------------------------------------
HDFS_CHECK=$(hdfs dfs -ls / 2>&1 | head -1)
if echo "$HDFS_CHECK" | grep -qE "Found|drwx"; then
    pass "HDFS: accessible (hdfs dfs -ls /)"
else
    fail "HDFS: NOT accessible — '$HDFS_CHECK'"
    note "" "Check namenode and datanode containers are healthy."
    note "" "Verify HDFS is out of safe mode: hdfs dfsadmin -safemode get"
fi

# ---------------------------------------------------------------------------
# Hive metastore port — critical check
# ---------------------------------------------------------------------------
HIVE_CHECK=$(python -c "
import socket, sys
s = socket.socket()
s.settimeout(5)
try:
    s.connect(('hive-metastore', 9083))
    print('OK')
except Exception as e:
    print(f'FAIL: {e}')
finally:
    s.close()
" 2>/dev/null)
if echo "$HIVE_CHECK" | grep -q "^OK"; then
    pass "Hive metastore: reachable on hive-metastore:9083"
else
    fail "Hive metastore: NOT reachable — $HIVE_CHECK"
    note "" "Check hive-metastore container is healthy."
fi

# ---------------------------------------------------------------------------
# Postgres (external_postgres_db) — critical check
# Credentials: external / external (from docker-compose.yaml)
# ---------------------------------------------------------------------------
PG_CHECK=$(python -c "
import sys
try:
    import psycopg2
    c = psycopg2.connect(
        host='external_postgres_db',
        user='external',
        password='external',
        dbname='external',
        connect_timeout=5
    )
    c.close()
    print('OK')
except ImportError:
    print('SKIP: psycopg2 not installed yet — run make install-conda first')
except Exception as e:
    print(f'FAIL: {e}')
" 2>/dev/null)
if echo "$PG_CHECK" | grep -q "^OK"; then
    pass "Postgres: connected to external_postgres_db (external/external)"
elif echo "$PG_CHECK" | grep -q "^SKIP"; then
    warn "Postgres: $PG_CHECK"
else
    fail "Postgres: NOT reachable — $PG_CHECK"
    note "" "Check external_postgres_db container is healthy."
    note "" "Credentials: user=external password=external db=external"
fi

# ---------------------------------------------------------------------------
# MLflow service — warning only (may not be up yet on first boot)
# ---------------------------------------------------------------------------
MLFLOW_CHECK=$(python -c "
import urllib.request, sys
try:
    urllib.request.urlopen('http://mlflow:5001/health', timeout=5)
    print('OK')
except Exception as e:
    print(f'WARN: {e}')
" 2>/dev/null)
if echo "$MLFLOW_CHECK" | grep -q "^OK"; then
    pass "MLflow: reachable on mlflow:5001"
else
    warn "MLflow: $MLFLOW_CHECK"
    note "" "MLflow container may still be starting. Check: docker ps | grep mlflow"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "================================================================"
echo "  Summary: $WARN warning(s), $FAIL critical failure(s)"
echo ""

if [ "$FAIL" -gt 0 ]; then
    echo "  CRITICAL FAILURES detected — resolve before proceeding."
    echo "  Check the [FAIL] lines above."
    echo "================================================================"
    exit 1
elif [ "$WARN" -gt 0 ]; then
    echo "  Warnings present — review [WARN] lines above."
    echo "  Warnings do not block progress if connectivity checks passed."
    echo "  If Python packages show 'not found', run: make install-conda"
    echo "================================================================"
    exit 0
else
    echo "  All checks passed. Safe to proceed with Milestone 1."
    echo "================================================================"
    exit 0
fi