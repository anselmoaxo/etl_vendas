from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


import sys
import os

# Adiciona o caminho do diretório 'src' ao sys.path para que o Airflow encontre o módulo
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

# Agora o import funcionará corretamente
from extract import extract_data


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG("export_postgres_to_csv", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    export_task = PythonOperator(
        task_id="export_to_csv",
        python_callable=extract_data
    )

    export_task
