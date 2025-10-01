from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
with DAG(
    dag_id='exchange_rates_etl',  # More descriptive name
    default_args=default_args,
    description='ETL pipeline for exchange rates data',
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['finance', 'exchange_rates'],
) as dag:
    
    # Tasks with better error handling and documentation
    bronze = BashOperator(
        task_id='extract_to_bronze',
        bash_command='python src/etl/raw/api/extract_api_exchange_rates.py',
        cwd='/opt/airflow',
        retries=5,  # Specific retries for this task
        retry_delay=timedelta(minutes=5),
    )

    silver = BashOperator(
        task_id='transform_to_silver',
        bash_command='python src/etl/silver/finance/transform_exchange_rates.py',
        cwd='/opt/airflow',
    )

    gold = BashOperator(
        task_id='load_to_gold',
        bash_command='python src/etl/gold/finance/currency_heatmap.py',
        cwd='/opt/airflow',
    )

    # Task dependencies
    bronze >> silver >> gold

# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime

# # Definir DAG
# with DAG(
#     dag_id="meu_projeto_dag",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval="59 * * * *",
#     catchup=False,
#     tags=["projeto"],
# ) as dag:
#     etapa1 = BashOperator(
#         task_id="etapa1",
#         bash_command="python src/etl/raw/api/extract_api_exchange_rates.py",
#         cwd="/opt/airflow",
#     )

#     etapa2 = BashOperator(
#         task_id="etapa2",
#         bash_command="python src/etl/silver/finance/transform_exchange_rates.py",
#         cwd="/opt/airflow",
#     )

#     # Orquestração (ordem de execução)
    # etapa1 >> etapa2
