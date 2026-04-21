"""
dag_ingest_to_hdfs.py
Validates Synthea CSV files then uploads them to HDFS.
On success, triggers dag_create_hive_tables automatically.

Schedule: manual trigger only (data generation is a one-time host step)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ---------------------------------------------------------------------------
# Shared conda activation prefix
# Every BashOperator task that needs Python must start with this.
# ---------------------------------------------------------------------------
CONDA_ACTIVATE = (
    "source /root/anaconda/etc/profile.d/conda.sh && "
    "conda activate healthcare-rec"
)

default_args = {
    "owner": "healthcare",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_ingest_to_hdfs",
    default_args=default_args,
    description="Validate Synthea CSVs and upload to HDFS",
    schedule_interval=None,          # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["milestone-1", "ingestion"],
) as dag:

    # -------------------------------------------------------------------------
    # Task 1: Verify HDFS is out of safe mode before attempting any writes
    # -------------------------------------------------------------------------
    wait_for_hdfs = BashOperator(
        task_id="wait_for_hdfs_ready",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            "for i in $(seq 1 12); do "
            "  STATUS=$(hdfs dfsadmin -safemode get 2>&1); "
            "  echo \"Safe mode check $i: $STATUS\"; "
            "  if echo \"$STATUS\" | grep -q 'Safe mode is OFF'; then "
            "    echo 'HDFS ready.'; exit 0; "
            "  fi; "
            "  sleep 10; "
            "done; "
            "echo 'ERROR: HDFS did not exit safe mode.'; exit 1"
        ),
    )

    # -------------------------------------------------------------------------
    # Task 2: Validate all six CSV files
    # -------------------------------------------------------------------------
    validate_csvs = BashOperator(
        task_id="validate_csv_files",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            "cd /home && "
            "python -m src.ingestion.validate /home/data/raw"
        ),
    )

    # -------------------------------------------------------------------------
    # Task 3: Create HDFS base directory structure
    # -------------------------------------------------------------------------
    create_hdfs_dirs = BashOperator(
        task_id="create_hdfs_directories",
        bash_command=(
            "hdfs dfs -mkdir -p hdfs://namenode:9000/data/raw && "
            "hdfs dfs -mkdir -p hdfs://namenode:9000/data/processed && "
            "hdfs dfs -mkdir -p hdfs://namenode:9000/data/features && "
            "hdfs dfs -mkdir -p hdfs://namenode:9000/models && "
            "echo 'HDFS directories created.'"
        ),
    )

    # -------------------------------------------------------------------------
    # Task 4: Upload all CSVs to HDFS
    # -------------------------------------------------------------------------
    upload_to_hdfs = BashOperator(
        task_id="upload_csvs_to_hdfs",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            "cd /home && "
            "python -m src.ingestion.load_hdfs /home/data/raw"
        ),
    )

    # -------------------------------------------------------------------------
    # Task 5: Verify uploads
    # -------------------------------------------------------------------------
    verify_hdfs = BashOperator(
        task_id="verify_hdfs_uploads",
        bash_command=(
            "echo '--- HDFS /data/raw listing ---' && "
            "hdfs dfs -ls hdfs://namenode:9000/data/raw/ && "
            "for TABLE in patients conditions medications observations encounters procedures; do "
            "  COUNT=$(hdfs dfs -cat hdfs://namenode:9000/data/raw/$TABLE/$TABLE.csv "
            "           2>/dev/null | wc -l); "
            "  echo \"$TABLE: $COUNT lines (including header)\"; "
            "  if [ \"$COUNT\" -lt 101 ]; then "
            "    echo \"ERROR: $TABLE has too few lines.\"; exit 1; "
            "  fi; "
            "done; "
            "echo 'All tables verified in HDFS.'"
        ),
    )

    # -------------------------------------------------------------------------
    # Task 6: Trigger next DAG
    # -------------------------------------------------------------------------
    trigger_create_hive = TriggerDagRunOperator(
        task_id="trigger_dag_create_hive_tables",
        trigger_dag_id="dag_create_hive_tables",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # ---------------------------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------------------------
    (
        wait_for_hdfs
        >> validate_csvs
        >> create_hdfs_dirs
        >> upload_to_hdfs
        >> verify_hdfs
        >> trigger_create_hive
    )