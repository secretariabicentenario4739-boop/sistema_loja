# db_config.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Tenta obter a URL do banco
DATABASE_URL = os.getenv('DATABASE_URL')

# Se não tiver URL, constrói com as variáveis individuais
if not DATABASE_URL:
    print("⚠️  DATABASE_URL não encontrada, usando variáveis individuais")
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'sistema_maconico')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    print(f"   URL construída: {DATABASE_URL[:50]}...")

def get_db():
    """Retorna uma conexão com o PostgreSQL"""
    try:
        print(f"🔄 Conectando ao banco...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        return cursor, conn
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        raise

def return_connection(conn):
    """Fecha a conexão"""
    if conn:
        conn.close()

def init_db():
    """Testa a conexão"""
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT 1")
        print("✅ Conexão com PostgreSQL estabelecida!")
        return_connection(conn)
        return True
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False