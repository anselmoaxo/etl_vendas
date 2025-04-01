from conection import conexao_postgresql

def extract_data():
    """Extrai dados do banco de dados PostgreSQL."""
    conn = conexao_postgresql()
    if conn is None:
        raise ValueError("Falha ao criar a conexão com o banco.")
    
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 
                   'vendedores', 'clientes', 'vendas']
    max_ids = {}
    
    try:
        # Criar um cursor para executar as queries
        with conn.cursor() as cursor:
            for table_name in table_names:
                try:
                    query = f"SELECT MAX(id_{table_name}) FROM {table_name}"
                    cursor.execute(query)
                    result = cursor.fetchone()[0]  # Pega o primeiro valor da linha
                    max_ids[table_name] = result if result is not None else 0
                except Exception as e:
                    print(f"Erro ao processar {table_name}: {e}")
                    max_ids[table_name] = 0
        conn.commit()  # Não necessário aqui, mas boa prática
    finally:
        conn.close()  # Garante que a conexão seja fechada
    
    return max_ids  

if __name__ == "__main__":
    result = extract_data()
    print(f"Resultado final: {result}")