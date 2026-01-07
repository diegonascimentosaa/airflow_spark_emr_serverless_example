import os
import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from base_dag import DEFAULT_ARGS

DAG_ID = "emr_serverless_test"
SPARK_IMAGE = "projeto-spark-custom:latest"

HOST_SCRIPTS = os.getenv("HOST_SCRIPTS_PATH")
HOST_DATA = os.getenv("HOST_DATA_PATH")

if not HOST_SCRIPTS or not HOST_DATA:
    raise ValueError("Directory not found.")

PIPELINE_STEPS = ["ingestion", "raw", "trusted", "refined"]


def get_spark_kwargs():
    return {
        "image": SPARK_IMAGE,
        "api_version": "auto",
        "auto_remove": "force",
        "docker_url": "unix://var/run/docker.sock",
        "network_mode": "bridge",
        "mount_tmp_dir": False,
        "mounts": [
            Mount(source=HOST_SCRIPTS, target="/app/scripts", type="bind"),
            Mount(source=HOST_DATA, target="/app/data", type="bind"),
        ],
        "user": "root",
    }


def create_spark_task(step_name, index):

    cmd = f"/opt/spark/bin/spark-submit /app/scripts/main.py --mode incremental --exec_date {{{{ ds }}}} --step {step_name}"

    return DockerOperator(
        task_id=f"step_{index}_{step_name}",
        command=cmd,
        **get_spark_kwargs(),
    )


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="@daily",
    catchup=False,
    tags=["spark", "docker", "etl"],
) as dag:

    task_anterior = None

    for i, step_name in enumerate(PIPELINE_STEPS):
        task_atual = create_spark_task(step_name, index=i + 1)
        if task_anterior:
            task_anterior >> task_atual
        task_anterior = task_atual
