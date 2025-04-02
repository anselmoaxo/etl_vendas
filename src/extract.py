from conection import conexao_postgresql
import pandas as pd
import os

# Lista de tabelas no banco de dados
table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 
               'vendedores', 'clientes', 'vendas']

def path_file():
    """Retorna o caminho do diretório de saída e cria-o se não existir."""
    output_dir = "/home/anselmo/etl_vendas/data"
    os.makedirs(output_dir, exist_ok=True)  # Criar diretório se não existir
    return output_dir

def conectar():
    """Cria a conexão com o banco de dados PostgreSQL."""
    conn = conexao_postgresql()
    if conn is None:
        raise ValueError("Falha ao criar a conexão com o banco.")
    return conn

def get_max(cursor, table_name):
    """Obtém o maior ID de uma tabela específica."""
    query = f"SELECT MAX(id_{table_name}) FROM {table_name}"
    cursor.execute(query)
    result = cursor.fetchone()  # Retorna uma tupla com o valor
    return result[0] if result and result[0] is not None else 0  # Retorna 0 se não houver registros

def extract_data():
    """Extrai os dados mais recentes de cada tabela e salva em CSV."""
    conn = conectar()
    
    with conn.cursor() as cursor:
        for table_name in table_names:
            max_id = get_max(cursor, table_name)  # Pega o maior ID de cada tabela
            query = f"SELECT * FROM {table_name} WHERE id_{table_name} = {max_id}"
            cursor.execute(query)
            
            # Salva os dados em CSV
            save_data(query, conn, table_name)
    
    conn.close()

def save_data(query, conn, tabela):
    """Salva os dados extraídos de uma tabela em um arquivo CSV."""
    output_dir = path_file()
    
    # Lendo dados da query com pandas
    df = pd.read_sql_query(query, conn)
    
    if not df.empty:  # Só salvar se houver dados
        output_file = os.path.join(output_dir, f"{tabela}.csv")
        df.to_csv(output_file, index=False)
        print(f"Arquivo salvo: {output_file}")
    else:
        print(f"Nenhum dado encontrado para {tabela}.")

if __name__ == "__main__":
    extract_data()
