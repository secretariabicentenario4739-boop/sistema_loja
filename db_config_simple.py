# db_config_simple.py
import psycopg2
from psycopg2.extras import RealDictCursor
from config import Config

def get_db():
    """Retorna uma conexão direta com o PostgreSQL"""
    try:
        conn = psycopg2.connect(**Config.DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        return cursor, conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao PostgreSQL: {e}")
        raise

def init_db():
    """Testa a conexão com o banco"""
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        print(f"✅ Conectado ao PostgreSQL: {version['version'][:50]}...")
        return_connection(conn)
        return True
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        return False

def return_connection(conn):
    """Fecha a conexão"""
    if conn:
        conn.close()