from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


import sys
import os

# Adiciona o caminho do diretório 'src' ao sys.path para que o Airflow encontre o módulo
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

# Agora o import funcionará corretamente
from extract import extract_data, path_file, fetch_and_save
from load import inseri_data


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG("export_postgres_to_csv", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    cria_diretorio = PythonOperator(
        task_id="cria_diretorio",
        python_callable=path_file
    )

    export_task = PythonOperator(
        task_id="export_to_csv",
        python_callable=extract_data
    )
    
    inseri_bd = PythonOperator(
        task_id="inseri_bd",
        python_callable=inseri_data
    )

    cria_diretorio >> export_task >> inseri_bd
