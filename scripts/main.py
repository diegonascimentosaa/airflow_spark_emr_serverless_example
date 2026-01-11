import argparse  # Parser de argumentos e opções da linha de comando (CLI)
import sys
import os

# Garante que módulos locais possam ser importados
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession  # Lib externa para processamento distribuído
from etl.steps import IngestionStep, RawStep, TrustedStep, RefinedStep


def main():
    # Define e lê os argumentos passados via linha de comando
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, required=True)
    parser.add_argument("--exec_date", type=str, required=True)
    parser.add_argument(
        "--step",
        type=str,
        required=True,
        choices=["ingestion", "raw", "trusted", "refined"],
    )
    args = parser.parse_args()

    print(f"--- STARTING JOB SPARK [Step: {args.step.upper()}] ---")

    spark = SparkSession.builder.appName(f"ETL_{args.step}").getOrCreate()

    # Mapeia o nome da etapa para a classe correspondente (Strategy Pattern)
    strategies = {
        "ingestion": IngestionStep,
        "raw": RawStep,
        "trusted": TrustedStep,
        "refined": RefinedStep,
    }

    StepClass = strategies.get(args.step)

    if StepClass:
        # Instancia a classe selecionada e roda o processo
        step_instance = StepClass(spark, args)
        step_instance.execute()
    else:
        raise ValueError(f"Step unknown: {args.step}")

    spark.stop()


if __name__ == "__main__":
    main()
