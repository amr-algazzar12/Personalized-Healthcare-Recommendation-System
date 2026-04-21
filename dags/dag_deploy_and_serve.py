"""
dag_deploy_and_serve.py
Restarts the Flask API and Streamlit dashboard containers,
pointing them at the Production model in MLflow.

STATUS: Scaffold — task bodies completed in Milestone 4.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

CONDA_ACTIVATE = (
    "source /root/anaconda/etc/profile.d/conda.sh && "
    "conda activate healthcare-rec"
)

default_args = {
    "owner": "healthcare",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="dag_deploy_and_serve",
    default_args=default_args,
    description="Restart Flask API and Streamlit pointing at latest Production model",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["milestone-4", "serving"],
) as dag:

    # TODO (Milestone 4): implement full restart logic
    verify_production_model = BashOperator(
        task_id="verify_production_model_exists",
        bash_command=(
            f"{CONDA_ACTIVATE} && "
            "MLFLOW_TRACKING_URI=http://mlflow:5001 "
            "python -c \""
            "# TODO: verify a Production model exists in MLflow registry"
            "print('Placeholder: implement in Milestone 4')"
            "\""
        ),
    )

    restart_app = BashOperator(
        task_id="restart_app_container",
        bash_command=(
            "docker restart app && "
            "echo 'App container restarted.' && "
            "sleep 10 && "
            "curl -sf http://localhost:5050/health || "
            "(echo 'ERROR: Flask API health check failed.'; exit 1)"
        ),
    )

    verify_production_model >> restart_app