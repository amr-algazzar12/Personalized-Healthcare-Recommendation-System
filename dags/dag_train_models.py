"""
dag_train_models.py
Submits three model training jobs to YARN sequentially.
On success, triggers dag_evaluate_and_register automatically.

STATUS: Scaffold — task bodies completed in Milestone 3.
LocalExecutor runs tasks sequentially, so all three training jobs
execute one after another, not in parallel.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

CONDA_ACTIVATE = (
    "source /root/anaconda/etc/profile.d/conda.sh && "
    "conda activate healthcare-rec"
)

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
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_train_models",
    default_args=default_args,
    description="Train collaborative filtering, content-based, and hybrid models",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["milestone-3", "training"],
) as dag:

    # TODO (Milestone 3): implement collaborative_filtering.py
    train_collaborative = BashOperator(
        task_id="train_collaborative_filtering",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            f"{SPARK_SUBMIT} /home/src/models/collaborative_filtering.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    # TODO (Milestone 3): implement content_based.py
    train_content_based = BashOperator(
        task_id="train_content_based",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            f"{SPARK_SUBMIT} /home/src/models/content_based.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    # TODO (Milestone 3): implement hybrid_model.py
    train_hybrid = BashOperator(
        task_id="train_hybrid_model",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            f"{SPARK_SUBMIT} /home/src/models/hybrid_model.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    trigger_evaluate = TriggerDagRunOperator(
        task_id="trigger_dag_evaluate_and_register",
        trigger_dag_id="dag_evaluate_and_register",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Sequential — LocalExecutor does not support true parallelism
    train_collaborative >> train_content_based >> train_hybrid >> trigger_evaluate