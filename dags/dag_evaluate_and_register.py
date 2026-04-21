"""
dag_evaluate_and_register.py
Evaluates all three models, logs metrics to MLflow,
and promotes the best-performing model to Production.
On success, triggers dag_deploy_and_serve automatically.

STATUS: Scaffold — task bodies completed in Milestone 3.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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
)

default_args = {
    "owner": "healthcare",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_evaluate_and_register",
    default_args=default_args,
    description="Evaluate models, log to MLflow, promote best model to Production",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["milestone-3", "evaluation", "mlflow"],
) as dag:

    # TODO (Milestone 3): implement evaluate.py
    run_evaluation = BashOperator(
        task_id="run_model_evaluation",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            f"{SPARK_SUBMIT} /home/src/models/evaluate.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    # TODO (Milestone 3): implement MLflow promotion logic
    promote_best_model = BashOperator(
        task_id="promote_best_model_to_production",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            "cd /home && "
            "MLFLOW_TRACKING_URI=http://mlflow:5001 "
            "python -c \""
            "# TODO: query MLflow for best run by AUC-ROC and promote to Production"
            "print('Placeholder: implement in Milestone 3')"
            "\""
        ),
    )

    trigger_deploy = TriggerDagRunOperator(
        task_id="trigger_dag_deploy_and_serve",
        trigger_dag_id="dag_deploy_and_serve",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    run_evaluation >> promote_best_model >> trigger_deploy