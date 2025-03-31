import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine



dotenv_path = Path(r"C:\Users\csouz\dataflow_airflow\.env")  # O 'r' antes da string trata como raw string

# Carregar variáveis do .env
load_dotenv(dotenv_path=dotenv_path)

# Testar carregamento das variáveis
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def conexao_postgresql():
    """Conecta ao PostgreSQL."""
    try:
        # Criar a string de conexão corretamente
        postgresql_engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        print("✅ Conexão com o PostgreSQL estabelecida com sucesso!")
        return postgresql_engine
    except Exception as e:
        print(f"❌ Erro ao conectar ao PostgreSQL: {e}")
        return None

