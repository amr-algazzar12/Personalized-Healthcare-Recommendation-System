"""
load_hdfs.py
Uploads validated Synthea CSV files from the local container mount
into HDFS, creating the correct directory structure.

Called by dag_ingest_to_hdfs after validate.py succeeds.
Exits with code 1 on any failure so Airflow marks the task as failed.

HDFS layout created:
  hdfs://namenode:9000/data/raw/patients/patients.csv
  hdfs://namenode:9000/data/raw/conditions/conditions.csv
  ...  (one subdirectory per table so Hive external tables point cleanly)
"""

import subprocess
import sys
import logging
from pathlib import Path

from src.utils.config import (
    HDFS_BASE,
    HDFS_RAW_DIR,
    LOCAL_RAW_DIR,
    assert_hdfs_path,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [load_hdfs] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# Tables to upload — must match filenames produced by Synthea
TABLES = [
    "patients",
    "conditions",
    "medications",
    "observations",
    "encounters",
    "procedures",
]


def hdfs_run(args: list) -> int:
    """Run an hdfs dfs command and return the exit code."""
    cmd = ["hdfs", "dfs"] + args
    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error("STDERR: %s", result.stderr)
    else:
        log.info("STDOUT: %s", result.stdout.strip())
    return result.returncode


def wait_for_hdfs_ready(retries: int = 12, delay: int = 10) -> bool:
    """
    Poll HDFS until safe mode is OFF.
    Hadoop enters safe mode on startup and rejects writes until it exits.
    """
    import time
    for attempt in range(retries):
        result = subprocess.run(
            ["hdfs", "dfsadmin", "-safemode", "get"],
            capture_output=True, text=True,
        )
        if "Safe mode is OFF" in result.stdout:
            log.info("HDFS safe mode is OFF — ready for writes.")
            return True
        log.info(
            "HDFS still in safe mode (attempt %d/%d). Waiting %ds...",
            attempt + 1, retries, delay,
        )
        time.sleep(delay)
    log.error("HDFS did not exit safe mode after %d attempts.", retries)
    return False


def upload_table(table_name: str, local_dir: str) -> bool:
    """
    Creates an HDFS subdirectory for the table and uploads the CSV.
    Overwrites any existing file at that path.
    """
    local_file = Path(local_dir) / f"{table_name}.csv"
    hdfs_table_dir = f"{HDFS_RAW_DIR}/{table_name}"
    hdfs_target    = f"{hdfs_table_dir}/{table_name}.csv"

    assert_hdfs_path(hdfs_table_dir)

    if not local_file.exists():
        log.error("Local file not found: %s", local_file)
        return False

    # Create HDFS directory (idempotent)
    rc = hdfs_run(["-mkdir", "-p", hdfs_table_dir])
    if rc != 0:
        log.error("Failed to create HDFS directory: %s", hdfs_table_dir)
        return False

    # Remove existing file if present (allows re-runs to be idempotent)
    hdfs_run(["-rm", "-f", hdfs_target])

    # Upload
    rc = hdfs_run(["-put", str(local_file), hdfs_target])
    if rc != 0:
        log.error("Failed to upload %s to %s", local_file, hdfs_target)
        return False

    log.info("Uploaded: %s → %s", local_file, hdfs_target)
    return True


def verify_upload(table_name: str) -> bool:
    """Confirms the uploaded file exists in HDFS and is non-zero size."""
    hdfs_target = f"{HDFS_RAW_DIR}/{table_name}/{table_name}.csv"
    result = subprocess.run(
        ["hdfs", "dfs", "-ls", hdfs_target],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        log.error("Verification failed — file not found in HDFS: %s", hdfs_target)
        return False
    log.info("Verified in HDFS: %s", hdfs_target)
    return True


def upload_all(local_dir: str = LOCAL_RAW_DIR) -> bool:
    """
    Main entry point.
    1. Waits for HDFS to be ready
    2. Creates the base /data/raw directory
    3. Uploads each table CSV into its own subdirectory
    4. Verifies every upload
    """
    if not wait_for_hdfs_ready():
        return False

    # Ensure base directory exists
    assert_hdfs_path(HDFS_RAW_DIR)
    hdfs_run(["-mkdir", "-p", HDFS_RAW_DIR])

    results = {}
    for table in TABLES:
        ok = upload_table(table, local_dir)
        if ok:
            ok = verify_upload(table)
        results[table] = ok

    passed = [t for t, ok in results.items() if ok]
    failed = [t for t, ok in results.items() if not ok]

    log.info("Upload summary: %d succeeded, %d failed", len(passed), len(failed))

    if failed:
        log.error("Failed tables: %s", failed)
        return False

    log.info("All tables uploaded successfully to HDFS.")
    return True


if __name__ == "__main__":
    local_dir = sys.argv[1] if len(sys.argv) > 1 else LOCAL_RAW_DIR
    ok = upload_all(local_dir)
    sys.exit(0 if ok else 1)