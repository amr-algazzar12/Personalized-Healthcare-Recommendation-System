"""
dag_spark_processing.py
Submits PySpark cleaning and feature engineering jobs to YARN.
On success, triggers dag_train_models automatically.

STATUS: Scaffold — task bodies completed in Milestone 2.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

CONDA_ACTIVATE = (
    "source /root/anaconda/etc/profile.d/conda.sh && "
    "conda activate healthcare-rec"
)

# spark-submit base command — all jobs use YARN cluster mode
SPARK_SUBMIT = (
    "spark-submit "
    "--master yarn "
    "--deploy-mode cluster "
    "--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="
    "/root/anaconda/envs/healthcare-rec/bin/python "
    "--conf spark.executorEnv.PYSPARK_PYTHON="
    "/root/anaconda/envs/healthcare-rec/bin/python "
    "--executor-memory 2g "
    "--driver-memory 2g "
    "--executor-cores 2 "
    "--conf spark.yarn.executor.memoryOverhead=512m "
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
    dag_id="dag_spark_processing",
    default_args=default_args,
    description="PySpark data cleaning and feature engineering on YARN",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["milestone-2", "spark", "processing"],
) as dag:

    # TODO (Milestone 2): implement clean.py
    run_cleaning = BashOperator(
        task_id="run_data_cleaning",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            f"{SPARK_SUBMIT} /home/src/processing/clean.py"
        ),
    )

    # TODO (Milestone 2): implement feature_engineering.py
    run_feature_engineering = BashOperator(
        task_id="run_feature_engineering",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            f"{SPARK_SUBMIT} /home/src/processing/feature_engineering.py"
        ),
    )

    # TODO (Milestone 2): register patient_features as Hive external table
    register_hive_table = BashOperator(
        task_id="register_patient_features_hive_table",
        bash_command=(
            "docker cp /home/src/hive/queries.hql hive-server:/tmp/queries.hql && "
            "docker exec hive-server beeline "
            "-u 'jdbc:hive2://localhost:10000' -n root "
            "-f /tmp/queries.hql"
        ),
    )

    trigger_train = TriggerDagRunOperator(
        task_id="trigger_dag_train_models",
        trigger_dag_id="dag_train_models",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    run_cleaning >> run_feature_engineering >> register_hive_table >> trigger_train