"""
config.py
Centralised configuration for the healthcare recommendation pipeline.
All HDFS paths, Hive settings, and service URLs live here.
"""

import os

# =============================================================================
# HDFS paths — always use the full hdfs:// URI, never local container paths
# =============================================================================
HDFS_BASE               = "hdfs://namenode:9000"
HDFS_RAW_DIR            = f"{HDFS_BASE}/data/raw"
HDFS_PROCESSED_DIR      = f"{HDFS_BASE}/data/processed"
HDFS_FEATURES_DIR       = f"{HDFS_BASE}/data/features"
HDFS_MODELS_DIR         = f"{HDFS_BASE}/models"

HDFS_RAW_PATIENTS       = f"{HDFS_RAW_DIR}/patients"
HDFS_RAW_CONDITIONS     = f"{HDFS_RAW_DIR}/conditions"
HDFS_RAW_MEDICATIONS    = f"{HDFS_RAW_DIR}/medications"
HDFS_RAW_OBSERVATIONS   = f"{HDFS_RAW_DIR}/observations"
HDFS_RAW_ENCOUNTERS     = f"{HDFS_RAW_DIR}/encounters"
HDFS_RAW_PROCEDURES     = f"{HDFS_RAW_DIR}/procedures"
HDFS_PATIENT_FEATURES   = f"{HDFS_FEATURES_DIR}/patient_features"

# =============================================================================
# Local container paths (volume-mounted from host)
# =============================================================================
LOCAL_SRC_DIR    = "/home/src"
LOCAL_DATA_DIR   = "/home/data"
LOCAL_RAW_DIR    = "/home/data/raw"
LOCAL_MODELS_DIR = "/home/models"
LOCAL_MLFLOW_DIR = "/home/mlflow_artifacts"

# =============================================================================
# Hive
# =============================================================================
HIVE_METASTORE_URI       = "thrift://hive-metastore:9083"
HIVE_DATABASE            = "healthcare"
HIVE_TABLE_PATIENTS      = f"{HIVE_DATABASE}.patients"
HIVE_TABLE_CONDITIONS    = f"{HIVE_DATABASE}.conditions"
HIVE_TABLE_MEDICATIONS   = f"{HIVE_DATABASE}.medications"
HIVE_TABLE_OBSERVATIONS  = f"{HIVE_DATABASE}.observations"
HIVE_TABLE_ENCOUNTERS    = f"{HIVE_DATABASE}.encounters"
HIVE_TABLE_PROCEDURES    = f"{HIVE_DATABASE}.procedures"
HIVE_TABLE_FEATURES      = f"{HIVE_DATABASE}.patient_features"

# =============================================================================
# Spark / YARN
# =============================================================================
SPARK_MASTER          = "yarn"
SPARK_DEPLOY_MODE     = "cluster"
SPARK_APP_NAME        = "healthcare-rec"
SPARK_EXECUTOR_MEMORY = "2g"
SPARK_DRIVER_MEMORY   = "2g"
SPARK_EXECUTOR_CORES  = "2"
PYSPARK_PYTHON        = "/root/anaconda/envs/healthcare-rec/bin/python"

# =============================================================================
# MLflow
# =============================================================================
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5001")
MLFLOW_EXPERIMENT   = "healthcare-recommendation"

# =============================================================================
# PostgreSQL — credentials MUST match docker-compose.yaml exactly
# external_postgres_db uses: user=external, password=external, db=external
# =============================================================================
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "external_postgres_db")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "external")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "external")
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "external")

# =============================================================================
# Synthea
# =============================================================================
SYNTHEA_POPULATION = 10000
SYNTHEA_SEED       = 42
SYNTHEA_OUTPUT_DIR = LOCAL_RAW_DIR

# =============================================================================
# Model settings
# =============================================================================
TRAIN_RATIO = 0.70
VAL_RATIO   = 0.15
TEST_RATIO  = 0.15
TOP_K       = 10


def assert_hdfs_path(path: str) -> None:
    """
    Raises ValueError if a path does not start with the HDFS URI.
    Call at the top of every PySpark script to catch accidental local-path usage.
    """
    if not path.startswith(HDFS_BASE):
        raise ValueError(
            f"[config] Path must start with '{HDFS_BASE}'.\n"
            f"Got: '{path}'\n"
            f"Never use local container paths in Spark jobs."
        )