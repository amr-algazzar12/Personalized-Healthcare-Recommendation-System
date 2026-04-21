"""
spark_session.py
SparkSession factory for the healthcare recommendation pipeline.
Always configures for YARN execution with Hive support.
Never creates a local or standalone Spark session in production scripts.
"""

from pyspark.sql import SparkSession
from src.utils.config import (
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_EXECUTOR_MEMORY,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_CORES,
    HIVE_METASTORE_URI,
    PYSPARK_PYTHON,
)


def get_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    """
    Build and return a SparkSession configured for YARN + Hive.

    Parameters
    ----------
    app_name : str
        The Spark application name shown in the YARN UI and Job History server.

    Returns
    -------
    SparkSession

    Notes
    -----
    - master("yarn") is non-negotiable in this cluster.
    - Hive support is enabled so DataFrames can read/write Hive tables directly.
    - spark.sql.hive.metastore.version must match the Hive version in the cluster.
    - All memory/core settings are pulled from config.py — change them there.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        # Hive integration
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", HIVE_METASTORE_URI)
        .config("spark.sql.hive.metastore.version", "2.3")
        .config("spark.sql.hive.metastore.jars", "maven")
        # Resource settings
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
        .config("spark.yarn.executor.memoryOverhead", "512m")
        # Python interpreter — must point to the conda env inside YARN containers
        .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", PYSPARK_PYTHON)
        .config("spark.executorEnv.PYSPARK_PYTHON", PYSPARK_PYTHON)
        # Parquet optimisations
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        # Log level — INFO is enough for debugging; change to WARN for production runs
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")
    return spark


def get_local_spark_session(app_name: str = "healthcare-rec-local") -> SparkSession:
    """
    Build a local SparkSession for unit testing and notebook exploration only.
    NEVER call this from a script that will be submitted to YARN.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark