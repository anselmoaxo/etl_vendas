import os
import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 
               'vendedores', 'clientes', 'vendas']

def conect_dw():
    """Conecta ao PostgreSQL via Airflow e retorna um SQLAlchemy engine."""
    postgres_hook = PostgresHook(postgres_conn_id="postgres_dw")  
    conn = postgres_hook.get_uri()  # Pega a URI da conexão
    engine = create_engine(conn)  # Cria o SQLAlchemy engine
    return engine

def inseri_data():
    """Lê os arquivos CSV e insere os dados no banco PostgreSQL via SQLAlchemy."""
    engine = conect_dw()  # Obtém o SQLAlchemy engine

    for tabela in table_names:
        url = f'/home/anselmo/etl_vendas/data/{tabela}.csv'
        
        # Verifica se o arquivo existe antes de tentar carregar
        if os.path.exists(url):
            df = pd.read_csv(url)
            df.to_sql(tabela, engine, if_exists="append", index=False)
            print(f"Dados inseridos na tabela {tabela} com sucesso!")
        else:
            print(f"Arquivo {url} não encontrado.")

# Chamar a função para testar (no Airflow, seria usada dentro de uma DAG)
# inseri_data()
