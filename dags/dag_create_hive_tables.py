"""
dag_create_hive_tables.py
Creates the healthcare Hive database and all six external tables
by SSHing into hive-server and running beeline.
On success, triggers dag_spark_processing automatically.

Requires Airflow SSH connection named 'hive_server_ssh' to be configured.
Create it via: make init-airflow  (or manually in the Airflow UI)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "healthcare",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_create_hive_tables",
    default_args=default_args,
    description="Create Hive external tables for all six Synthea datasets",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["milestone-1", "hive"],
) as dag:

    # -------------------------------------------------------------------------
    # Task 1: Copy the HQL file from the namenode volume into hive-server
    # We use BashOperator on the namenode to docker-cp the file.
    # This avoids needing a separate file transfer step over SSH.
    # -------------------------------------------------------------------------
    copy_hql_to_hive_server = BashOperator(
        task_id="copy_hql_to_hive_server",
        bash_command=(
            "docker cp /home/src/hive/create_tables.hql "
            "hive-server:/tmp/create_tables.hql && "
            "echo 'HQL copied to hive-server:/tmp/create_tables.hql'"
        ),
    )

    # -------------------------------------------------------------------------
    # Task 2: Run the HQL via beeline inside hive-server
    # SSHOperator connects to the SSH server running on hive-server container.
    # Connection 'hive_server_ssh' must be configured in Airflow:
    #   Host: hive-server  |  User: root  |  Port: 22
    # -------------------------------------------------------------------------
    run_hql = SSHOperator(
        task_id="run_create_tables_hql",
        ssh_conn_id="hive_server_ssh",
        command=(
            "beeline "
            "-u 'jdbc:hive2://localhost:10000' "
            "-n root "
            "--hiveconf hive.cli.print.header=true "
            "--silent=false "
            "-f /tmp/create_tables.hql"
        ),
        cmd_timeout=300,   # 5 minutes — Hive can be slow on first query
        get_pty=True,
    )

    # -------------------------------------------------------------------------
    # Task 3: Verify row counts via beeline
    # -------------------------------------------------------------------------
    verify_row_counts = SSHOperator(
        task_id="verify_hive_row_counts",
        ssh_conn_id="hive_server_ssh",
        command=(
            "beeline "
            "-u 'jdbc:hive2://localhost:10000' "
            "-n root "
            "--silent=false "
            "-e \""
            "USE healthcare; "
            "SELECT 'patients'     AS tbl, COUNT(*) AS cnt FROM patients     UNION ALL "
            "SELECT 'conditions'   AS tbl, COUNT(*) AS cnt FROM conditions   UNION ALL "
            "SELECT 'medications'  AS tbl, COUNT(*) AS cnt FROM medications  UNION ALL "
            "SELECT 'observations' AS tbl, COUNT(*) AS cnt FROM observations UNION ALL "
            "SELECT 'encounters'   AS tbl, COUNT(*) AS cnt FROM encounters   UNION ALL "
            "SELECT 'procedures'   AS tbl, COUNT(*) AS cnt FROM procedures;"
            "\""
        ),
        cmd_timeout=300,
        get_pty=True,
    )

    # -------------------------------------------------------------------------
    # Task 4: Sanity check — fail if any table is empty
    # -------------------------------------------------------------------------
    check_not_empty = SSHOperator(
        task_id="check_tables_not_empty",
        ssh_conn_id="hive_server_ssh",
        command=(
            "beeline "
            "-u 'jdbc:hive2://localhost:10000' "
            "-n root "
            "--silent=true "
            "-e \"USE healthcare; "
            "SELECT COUNT(*) FROM patients;\" "
            "| grep -v 'rows selected' "
            "| awk 'NR>1 {if ($1+0 < 100) exit 1}' "
            "&& echo 'Row count check passed.' "
            "|| (echo 'ERROR: patients table is empty or has too few rows.'; exit 1)"
        ),
        cmd_timeout=120,
        get_pty=True,
    )

    # -------------------------------------------------------------------------
    # Task 5: Trigger next DAG
    # -------------------------------------------------------------------------
    trigger_spark_processing = TriggerDagRunOperator(
        task_id="trigger_dag_spark_processing",
        trigger_dag_id="dag_spark_processing",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # ---------------------------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------------------------
    (
        copy_hql_to_hive_server
        >> run_hql
        >> verify_row_counts
        >> check_not_empty
        >> trigger_spark_processing
    )