#!/usr/bin/env bash
# =============================================================================
# setup_project.sh
# Creates the full directory and placeholder file structure for the
# Personalized Healthcare Recommendation System project.
#
# Usage:
#   chmod +x setup_project.sh
#   ./setup_project.sh
#
# Run this from the parent of your intended project root, or adjust ROOT below.
# =============================================================================

set -euo pipefail

ROOT="/home/amrelgazzar/Projects/Personalized-Healthcare-Recommendation-System"

echo "================================================================"
echo "  Creating project structure at:"
echo "  $ROOT"
echo "================================================================"

# ---------------------------------------------------------------------------
# Helper: create a file only if it does not already exist
# ---------------------------------------------------------------------------
mkfile() {
  local path="$1"
  mkdir -p "$(dirname "$path")"
  if [ ! -f "$path" ]; then
    touch "$path"
    echo "  [+] $path"
  fi
}

# ---------------------------------------------------------------------------
# Root-level files
# ---------------------------------------------------------------------------
mkfile "$ROOT/.env"
mkfile "$ROOT/.gitignore"
mkfile "$ROOT/Makefile"
mkfile "$ROOT/README.md"
mkfile "$ROOT/hadoop.env"
mkfile "$ROOT/hive-sqoop-postgres-cassandra-docker-compose.yaml"
mkfile "$ROOT/docker-compose.override.yml"

# ---------------------------------------------------------------------------
# configs/
# ---------------------------------------------------------------------------
mkfile "$ROOT/configs/namenode_airflow.cfg"
mkfile "$ROOT/configs/namenode_bashrc.txt"

# ---------------------------------------------------------------------------
# conda/
# ---------------------------------------------------------------------------
mkfile "$ROOT/conda/environment.yml"

# ---------------------------------------------------------------------------
# dags/
# ---------------------------------------------------------------------------
mkfile "$ROOT/dags/dag_ingest_to_hdfs.py"
mkfile "$ROOT/dags/dag_create_hive_tables.py"
mkfile "$ROOT/dags/dag_spark_processing.py"
mkfile "$ROOT/dags/dag_train_models.py"
mkfile "$ROOT/dags/dag_evaluate_and_register.py"
mkfile "$ROOT/dags/dag_deploy_and_serve.py"

# ---------------------------------------------------------------------------
# docker/app/
# ---------------------------------------------------------------------------
mkfile "$ROOT/docker/app/Dockerfile"

# ---------------------------------------------------------------------------
# docker/mlflow/
# ---------------------------------------------------------------------------
mkfile "$ROOT/docker/mlflow/Dockerfile"

# ---------------------------------------------------------------------------
# data/
# ---------------------------------------------------------------------------
mkdir -p "$ROOT/data/raw"
mkdir -p "$ROOT/data/processed"
mkdir -p "$ROOT/data/features"
mkfile "$ROOT/data/synthea/generate_data.sh"
mkfile "$ROOT/data/synthea/synthea.properties"
echo "  [+] $ROOT/data/raw/           (directory)"
echo "  [+] $ROOT/data/processed/     (directory)"
echo "  [+] $ROOT/data/features/      (directory)"

# ---------------------------------------------------------------------------
# src/ingestion/
# ---------------------------------------------------------------------------
mkfile "$ROOT/src/ingestion/__init__.py"
mkfile "$ROOT/src/ingestion/load_hdfs.py"
mkfile "$ROOT/src/ingestion/validate.py"

# ---------------------------------------------------------------------------
# src/hive/
# ---------------------------------------------------------------------------
mkfile "$ROOT/src/hive/create_tables.hql"
mkfile "$ROOT/src/hive/queries.hql"

# ---------------------------------------------------------------------------
# src/processing/
# ---------------------------------------------------------------------------
mkfile "$ROOT/src/processing/__init__.py"
mkfile "$ROOT/src/processing/clean.py"
mkfile "$ROOT/src/processing/feature_engineering.py"

# ---------------------------------------------------------------------------
# src/models/
# ---------------------------------------------------------------------------
mkfile "$ROOT/src/models/__init__.py"
mkfile "$ROOT/src/models/collaborative_filtering.py"
mkfile "$ROOT/src/models/content_based.py"
mkfile "$ROOT/src/models/hybrid_model.py"
mkfile "$ROOT/src/models/evaluate.py"

# ---------------------------------------------------------------------------
# src/api/
# ---------------------------------------------------------------------------
mkfile "$ROOT/src/api/__init__.py"
mkfile "$ROOT/src/api/app.py"
mkfile "$ROOT/src/api/routes.py"
mkfile "$ROOT/src/api/schemas.py"

# ---------------------------------------------------------------------------
# src/recommender/
# ---------------------------------------------------------------------------
mkfile "$ROOT/src/recommender/__init__.py"
mkfile "$ROOT/src/recommender/engine.py"
mkfile "$ROOT/src/recommender/ranking.py"
mkfile "$ROOT/src/recommender/explain.py"

# ---------------------------------------------------------------------------
# src/dashboard/
# ---------------------------------------------------------------------------
mkfile "$ROOT/src/dashboard/__init__.py"
mkfile "$ROOT/src/dashboard/app.py"

# ---------------------------------------------------------------------------
# src/utils/
# ---------------------------------------------------------------------------
mkfile "$ROOT/src/utils/__init__.py"
mkfile "$ROOT/src/utils/spark_session.py"
mkfile "$ROOT/src/utils/config.py"

# ---------------------------------------------------------------------------
# notebooks/
# ---------------------------------------------------------------------------
mkfile "$ROOT/notebooks/01_data_exploration.ipynb"
mkfile "$ROOT/notebooks/02_feature_engineering.ipynb"
mkfile "$ROOT/notebooks/03_model_training.ipynb"

# ---------------------------------------------------------------------------
# models/   (saved trained model artifacts — gitignored)
# ---------------------------------------------------------------------------
mkdir -p "$ROOT/models"
echo "  [+] $ROOT/models/             (directory)"

# ---------------------------------------------------------------------------
# tests/
# ---------------------------------------------------------------------------
mkfile "$ROOT/tests/__init__.py"
mkfile "$ROOT/tests/test_validate.py"
mkfile "$ROOT/tests/test_clean.py"
mkfile "$ROOT/tests/test_feature_engineering.py"
mkfile "$ROOT/tests/test_ranking.py"
mkfile "$ROOT/tests/test_schemas.py"
mkfile "$ROOT/tests/test_pipeline.py"
mkfile "$ROOT/tests/test_api.py"
mkfile "$ROOT/tests/conftest.py"

# ---------------------------------------------------------------------------
# mlflow/   (artifact store — mounted into mlflow container)
# ---------------------------------------------------------------------------
mkdir -p "$ROOT/mlflow"
echo "  [+] $ROOT/mlflow/             (directory)"

# ---------------------------------------------------------------------------
# scripts/  (operational helper scripts)
# ---------------------------------------------------------------------------
mkfile "$ROOT/scripts/verify_versions.sh"
mkfile "$ROOT/scripts/init_airflow.sh"
mkfile "$ROOT/scripts/upload_to_hdfs.sh"
mkfile "$ROOT/scripts/create_hive_tables.sh"
mkfile "$ROOT/scripts/install_conda_env.sh"

# ---------------------------------------------------------------------------
# .gitignore seed content
# ---------------------------------------------------------------------------
cat > "$ROOT/.gitignore" << 'EOF'
# Data (generated — never commit raw patient data)
data/raw/
data/processed/
data/features/

# Saved models (large binary files)
models/

# MLflow artifacts
mlflow/

# Python
__pycache__/
*.py[cod]
*.egg-info/
.eggs/
dist/
build/
.env
*.env

# Jupyter checkpoints
.ipynb_checkpoints/

# Conda
*.tar.gz

# OS
.DS_Store
Thumbs.db

# IDE
.vscode/
.idea/
EOF
echo "  [✓] .gitignore populated"

echo ""
echo "================================================================"
echo "  Structure created successfully."
echo "  Next: fill in the files using the milestone 1 implementation."
echo "================================================================"