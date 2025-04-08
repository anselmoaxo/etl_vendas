from airflow.providers.postgres.hooks.postgres import PostgresHook


# Lista de tabelas no banco de dados
table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 
               'vendedores', 'clientes', 'vendas']


def get_dw_connection():
    """Conecta ao PostgreSQL via Airflow, executa um SELECT e salva o resultado em CSV."""
    postgres_hook = PostgresHook(postgres_conn_id="postgres_dw")  # Usa a conexão do Airflow
    conn = postgres_hook.get_conn()
    return conn


def get_max_id_for_all_tables():
    conn = get_dw_connection()
    max_ids = {}
    with conn.cursor() as cursor:
        for table in table_names:
            primary_key = f"id_{table}"
            cursor.execute(f"SELECT MAX({primary_key}) FROM {table}")
            result = cursor.fetchone()
            max_ids[table] = result[0] if result[0] is not None else 0
    return max_ids 



