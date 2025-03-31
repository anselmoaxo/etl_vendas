from conection import conexao_postgresql
import pandas as pd
from sqlalchemy import text

def extract_data():
    """Extrai dados do banco de dados PostgreSQL."""
    engine = conexao_postgresql()

    if engine is not None:
        try:
            with engine.raw_connection() as conn:  # Usa a conexão pura
                df = pd.read_sql_query(text("SELECT * FROM public.clientes;"), conn)
                print("✅ Dados extraídos com sucesso!")
                return df
        except Exception as e:
            print(f"❌ Erro ao extrair dados: {e}")
            return None
    else:
        print("❌ Não foi possível conectar ao banco de dados.")
        return None

# Executar a extração de dados
clientes = extract_data()

# Exibir os primeiros registros, se houver dados
if clientes is not None:
    print(clientes.head())
