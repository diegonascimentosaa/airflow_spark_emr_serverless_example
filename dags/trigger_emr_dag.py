import os
import pendulum  # Manipulação avançada de datas e fuso horário
from airflow import DAG  # Classe principal para definição de pipelines
from airflow.providers.docker.operators.docker import (
    DockerOperator,
)  # Operador para executar containers
from docker.types import Mount  # Classe auxiliar para montagem de volumes
from base_dag import DEFAULT_ARGS  # Importa args padrão (com alertas Telegram)

DAG_ID = "emr_serverless_test"
SPARK_IMAGE = "projeto-spark-custom:latest"

HOST_SCRIPTS = os.getenv("HOST_SCRIPTS_PATH")
HOST_DATA = os.getenv("HOST_DATA_PATH")

# Valida se as variáveis de ambiente necessárias existem
if not HOST_SCRIPTS or not HOST_DATA:
    raise ValueError("Directory not found.")

PIPELINE_STEPS = ["ingestion", "raw", "trusted", "refined"]


# Define configurações reutilizáveis do container (volumes e rede)
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


# Monta o comando Spark e cria a task do Docker
def create_spark_task(step_name, index):

    # Realiza o escape da f-string para preservar a sintaxe do Airflow
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

    # Gera tasks dinamicamente e encadeia a dependência (anterior >> atual)
    for i, step_name in enumerate(PIPELINE_STEPS):
        task_atual = create_spark_task(step_name, index=i + 1)
        if task_anterior:
            task_anterior >> task_atual
        task_anterior = task_atual
