# db_config.py - Versão definitiva
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import atexit

load_dotenv()

# Configuração do banco
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'sistema_maconico'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# Pool de conexões (opcional)
connection_pool = None

def init_pool(min_conn=1, max_conn=10):
    """Inicializa o pool de conexões (opcional)"""
    global connection_pool
    try:
        from psycopg2 import pool
        connection_pool = pool.SimpleConnectionPool(
            min_conn,
            max_conn,
            **DB_CONFIG
        )
        print(f"✅ Pool de conexões criado (min={min_conn}, max={max_conn})")
        return True
    except Exception as e:
        print(f"❌ Erro ao criar pool: {e}")
        return False

def get_connection():
    """Obtém uma conexão (do pool se disponível, ou direta)"""
    global connection_pool
    
    # Se tiver pool, usa ele
    if connection_pool:
        try:
            return connection_pool.getconn()
        except:
            pass
    
    # Fallback: conexão direta
    return psycopg2.connect(**DB_CONFIG)

def return_connection(conn):
    """Retorna a conexão (para o pool ou fecha)"""
    global connection_pool
    
    if connection_pool:
        try:
            connection_pool.putconn(conn)
        except:
            conn.close()
    else:
        if conn:
            conn.close()

def get_db():
    """Retorna cursor e conexão para uso no Flask"""
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return cursor, conn

def close_all_connections():
    """Fecha todas as conexões"""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()
        print("✅ Pool de conexões fechado")

# Registrar fechamento automático
atexit.register(close_all_connections)

# Testar conexão na importação
try:
    cursor, conn = get_db()
    cursor.execute("SELECT 1")
    conn.commit()
    return_connection(conn)
    print("✅ Configuração do banco de dados OK")
except Exception as e:
    print(f"⚠️  Banco de dados não disponível: {e}")