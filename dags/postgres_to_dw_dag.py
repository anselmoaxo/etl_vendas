import sys
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(BASE_DIR)


from airflow.decorators import dag, task
from datetime import datetime, timedelta
from extract.extract_ids_max import get_max_id_for_all_tables, get_dw_connection
from load.load_incremental_data import transfer_data_incremental, get_origem_connection

table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 
               'vendedores', 'clientes', 'vendas']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='incremental_postgres_to_dw',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['postgres', 'dw', 'incremental']
)
def incremental_pipeline():

    @task()
    def run_incremental():
        origem_conn = get_origem_connection()
        destino_conn = get_dw_connection()
        ids_max = get_max_id_for_all_tables()

        for tabela in table_names:
            last_id = ids_max.get(tabela, 0)
            transfer_data_incremental(origem_conn, destino_conn, tabela, last_id)

    run_incremental()

dag_instance = incremental_pipeline()
