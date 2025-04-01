from conection import conexao_postgresql
import pandas as pd
import os

def extract_data():
    """Extrai os 10 primeiros registros de cada tabela e salva em CSV no diretório ../data."""
    conn = conexao_postgresql()
    if conn is None:
        raise ValueError("Falha ao criar a conexão com o banco.")
    
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 
                   'vendedores', 'clientes', 'vendas']
    
    # Definir o diretório de saída relativo ao script
    output_dir = "/home/anselmo/etl_vendas/data"
    # Criar o diretório se não existir
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        with conn.cursor() as cursor:
            for table_name in table_names:
                query = f"SELECT * FROM {table_name} "
                cursor.execute(query)
                # Ler os dados diretamente em um DataFrame
                df = pd.read_sql_query(query, conn)
                # Caminho completo para o arquivo CSV
                output_file = os.path.join(output_dir, f"{table_name}.csv")
                # Salvar o DataFrame como CSV
                df.to_csv(output_file, index=False)
        conn.commit()  
    except Exception as e:
        print(f"❌ Erro durante a extração: {e}")
    finally:
        conn.close()  # Garante que a conexão seja fechada
        

if __name__ == "__main__":
    extract_data()