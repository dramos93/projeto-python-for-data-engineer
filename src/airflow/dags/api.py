from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Definir DAG
with DAG(
    dag_id="meu_projeto_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="59 * * * *",
    catchup=False,
    tags=["projeto"],
) as dag:

    etapa1 = BashOperator(
        task_id="etapa1",
        bash_command="python src/etl/raw/api/extract_api_exchange_rates.py",
        cwd="/opt/airflow",
    )

    etapa2 = BashOperator(
        task_id="etapa2",
        bash_command="python src/etl/silver/finance/transform_exchange_rates.py",
        cwd="/opt/airflow",
    )


    # Orquestração (ordem de execução)
    etapa1 >> etapa2
