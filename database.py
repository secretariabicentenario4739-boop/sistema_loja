# database.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

def get_db():
    """Cria uma conexão direta com o banco de dados"""
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    try:
        if DATABASE_URL:
            # Corrigir URL se necessário
            if DATABASE_URL.startswith('postgres://'):
                DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
            
            conn = psycopg2.connect(
                DATABASE_URL,
                cursor_factory=RealDictCursor,
                connect_timeout=30
            )
        else:
            # Conexão local
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                dbname=os.getenv('DB_NAME', 'sistema_maconico'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', 'postgres'),
                cursor_factory=RealDictCursor,
                connect_timeout=30
            )
        
        cursor = conn.cursor()
        print("✅ Conexão com o banco estabelecida")
        return cursor, conn
        
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        raise

def return_connection(conn):
    """Fecha a conexão"""
    if conn:
        conn.close()
        print("🔌 Conexão fechada")

@contextmanager
def get_db_connection():
    """Context manager para usar com 'with'"""
    cursor, conn = get_db()
    try:
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        return_connection(conn)