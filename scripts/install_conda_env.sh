#!/usr/bin/env bash
# =============================================================================
# install_conda_env.sh
# Installs the healthcare-rec conda environment inside the namenode.
# Installs in stages to avoid OOM kill (exit code 137) which happens
# when conda tries to solve + download everything in one shot.
#
# Usage (from host):
#   docker exec -it namenode bash /home/scripts/install_conda_env.sh
# =============================================================================

set -euo pipefail

ENV_NAME="healthcare-rec"
CONDA_INIT="/root/anaconda/etc/profile.d/conda.sh"

echo "================================================================"
echo "  Installing conda environment: $ENV_NAME"
echo "  (staged install to avoid OOM kill)"
echo "================================================================"

if [ ! -f "$CONDA_INIT" ]; then
    echo "[ERROR] Conda not found at $CONDA_INIT"
    exit 1
fi
. "$CONDA_INIT"

# ---------------------------------------------------------------------------
# Remove existing env if present
# ---------------------------------------------------------------------------
if conda env list | grep -q "^$ENV_NAME"; then
    echo "Removing existing environment: $ENV_NAME"
    conda env remove -n "$ENV_NAME" -y
fi

# ---------------------------------------------------------------------------
# Stage 1: Create the base env with Python only
# This is fast and avoids the heavy solve step failing due to memory.
# ---------------------------------------------------------------------------
echo ""
echo "--- Stage 1: Create base Python 3.9 environment ---"
conda create -n "$ENV_NAME" python=3.9.16 pip=23.1.2 -y

# Activate for all subsequent stages
conda activate "$ENV_NAME"

# ---------------------------------------------------------------------------
# Stage 2: Core data packages via conda (faster than pip for numpy/pandas)
# ---------------------------------------------------------------------------
echo ""
echo "--- Stage 2: Core data packages (numpy, pandas, pyarrow) ---"
conda install -n "$ENV_NAME" -y \
    numpy=1.23.5 \
    pandas=1.5.3 \
    pyarrow=11.0.0 \
    -c conda-forge

# ---------------------------------------------------------------------------
# Stage 3: PySpark — match the version already in the namenode image
# ---------------------------------------------------------------------------
echo ""
echo "--- Stage 3: PySpark 3.3.1 ---"
conda install -n "$ENV_NAME" -y pyspark=3.3.1 -c conda-forge

# ---------------------------------------------------------------------------
# Stage 4: ML packages via pip (smaller footprint than conda for these)
# ---------------------------------------------------------------------------
echo ""
echo "--- Stage 4: ML packages (xgboost, scikit-learn) ---"
pip install --no-cache-dir \
    xgboost==1.7.6 \
    scikit-learn==1.2.2

# ---------------------------------------------------------------------------
# Stage 5: MLflow
# ---------------------------------------------------------------------------
echo ""
echo "--- Stage 5: MLflow ---"
pip install --no-cache-dir \
    mlflow==2.3.2 \
    "mlflow[spark]==2.3.2"

# ---------------------------------------------------------------------------
# Stage 6: Airflow providers
# Airflow itself is pre-installed in the image — only install the providers
# ---------------------------------------------------------------------------
echo ""
echo "--- Stage 6: Airflow providers ---"
pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.0.0 \
    apache-airflow-providers-ssh==3.0.0

# ---------------------------------------------------------------------------
# Stage 7: API, dashboard, and utility packages
# ---------------------------------------------------------------------------
echo ""
echo "--- Stage 7: API, dashboard, and utilities ---"
pip install --no-cache-dir \
    flask==2.2.5 \
    flask-cors==3.0.10 \
    streamlit==1.22.0 \
    "pydantic==1.10.9" \
    psycopg2-binary==2.9.6 \
    python-dotenv==1.0.0 \
    requests==2.31.0

# ---------------------------------------------------------------------------
# Stage 8: Jupyter for notebooks
# ---------------------------------------------------------------------------
echo ""
echo "--- Stage 8: Jupyter ---"
conda install -n "$ENV_NAME" -y \
    jupyterlab=3.6.3 \
    ipykernel=6.22.0 \
    pytest=7.3.1 \
    -c conda-forge

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
echo ""
echo "--- Verifying installation ---"
python -c "
import sys
results = {}

packages = {
    'pyspark':    ('pyspark',    '3.3'),
    'mlflow':     ('mlflow',     '2.3'),
    'xgboost':    ('xgboost',    '1.7'),
    'pandas':     ('pandas',     '1.5'),
    'numpy':      ('numpy',      '1.23'),
    'sklearn':    ('sklearn',    '1.2'),
    'flask':      ('flask',      '2.2'),
    'psycopg2':   ('psycopg2',   '2.9'),
}

all_ok = True
for name, (module, expected) in packages.items():
    try:
        mod = __import__(module)
        ver = getattr(mod, '__version__', 'unknown')
        status = 'OK' if expected in ver else 'WARN'
        print(f'  [{status}] {name}: {ver}')
    except ImportError:
        print(f'  [FAIL] {name}: not importable')
        all_ok = False

if all_ok:
    print()
    print('All packages installed successfully.')
else:
    sys.exit(1)
"

echo ""
echo "================================================================"
echo "  Environment '$ENV_NAME' installed successfully."
echo "================================================================"