# db_config.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import atexit
import urllib.parse

load_dotenv()

def get_db():
    """Retorna uma conexão com o PostgreSQL usando DATABASE_URL"""
    try:
        # Usar a URL completa do banco (recomendado)
        database_url = os.getenv('DATABASE_URL')
        
        if database_url:
            # A URL já contém todos os parâmetros, incluindo sslmode
            conn = psycopg2.connect(database_url)
        else:
            # Fallback para variáveis individuais
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                dbname=os.getenv('DB_NAME', 'sistema_maconico'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', ''),
                sslmode='require'
            )
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        return cursor, conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao PostgreSQL: {e}")
        raise

def return_connection(conn):
    """Fecha a conexão"""
    if conn:
        conn.close()

def init_db():
    """Testa a conexão com o banco"""
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        print(f"✅ PostgreSQL conectado: {version['version'][:50]}...")
        return_connection(conn)
        return True
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return False