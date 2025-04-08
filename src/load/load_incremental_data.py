import sys
import os

# Caminho para o diretório raiz do projeto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.extract.extract_ids_max import get_max_id_for_all_tables, get_dw_connection
from airflow.providers.postgres.hooks.postgres import PostgresHook


table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 
               'vendedores', 'clientes', 'vendas']


def get_origem_connection():
    """Conecta ao PostgreSQL via Airflow, executa um SELECT e salva o resultado em CSV."""
    postgres_hook = PostgresHook(postgres_conn_id="postgres_novadrive")
    conn = postgres_hook.get_conn()
    return conn


def get_ids_atuais():
    """Retorna um dicionário com o maior ID de cada tabela."""
    return get_max_id_for_all_tables()

def transfer_data_incremental(origem_db, destino_db, table_name, last_id):
    with origem_db.cursor() as origem_cursor, destino_db.cursor() as destino_cursor:
        # Pega nomes das colunas
        origem_cursor.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        )
        columns = [row[0] for row in origem_cursor.fetchall()]
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        # Busca registros novos
        origem_cursor.execute(
            f"SELECT {columns_str} FROM {table_name} WHERE id_{table_name} > %s",
            (last_id,)
        )
        novos_dados = origem_cursor.fetchall()

        if novos_dados:
            destino_cursor.executemany(
                f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})",
                novos_dados
            )
            destino_db.commit()
            print(f"{len(novos_dados)} registros inseridos na tabela {table_name}.")
        else:
            print(f"Nenhum novo registro para a tabela {table_name}.")

