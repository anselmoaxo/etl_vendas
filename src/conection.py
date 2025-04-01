import os
from dotenv import load_dotenv
from pathlib import Path
import psycopg2
import logging

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

dotenv_path = Path(r"/home/anselmo/etl_vendas/.env")
load_dotenv(dotenv_path=dotenv_path)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def conexao_postgresql():
    """Conecta ao PostgreSQL usando psycopg2."""
    try:
        
        # Estabelecer conexão
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("✅ Conexão com o PostgreSQL estabelecida com sucesso!")
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao PostgreSQL: {e}")
        return None

