# database.py
import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_db_pool = None

def init_db_pool():
    """Inicializa o pool de conexões para PostgreSQL"""
    global _db_pool
    
    if _db_pool is not None:
        return _db_pool
    
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    if not DATABASE_URL:
        # Configuração local
        DATABASE_URL = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'postgres')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'sistema_maconico')}"
    
    logger.info(f"🚀 Inicializando pool de conexões...")
    
    try:
        # Configurações para o Render
        _db_pool = psycopg2.pool.SimpleConnectionPool(
            1,                      # mínimo de conexões
            10,                     # máximo de conexões (reduzido para evitar sobrecarga)
            dsn=DATABASE_URL,
            cursor_factory=RealDictCursor,
            connect_timeout=10,     # timeout de conexão
            keepalives=1,           # mantém conexão ativa
            keepalives_idle=30,     # envia keepalive a cada 30 segundos
            keepalives_interval=5,  # intervalo entre keepalives
            keepalives_count=3,     # tentativas antes de considerar morta
            sslmode='require'       # requer SSL para o Render
        )
        
        logger.info(f"✅ Pool de conexões inicializado com sucesso!")
        return _db_pool
        
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar pool: {e}")
        raise

def get_db():
    """Obtém uma conexão do pool"""
    pool = init_db_pool()
    
    try:
        conn = pool.getconn()
        # Testar se a conexão está viva
        with conn.cursor() as test_cursor:
            test_cursor.execute("SELECT 1")
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        logger.debug(f"🔌 Conexão obtida do pool (ativas: {pool._used})")
        return cursor, conn
    except Exception as e:
        logger.error(f"❌ Erro ao obter conexão: {e}")
        # Se a conexão falhou, tentar recriar o pool
        _db_pool = None
        pool = init_db_pool()
        conn = pool.getconn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        return cursor, conn

def return_connection(conn):
    """Retorna a conexão para o pool"""
    pool = init_db_pool()
    if conn and pool:
        pool.putconn(conn)
        logger.debug(f"🔌 Conexão retornada ao pool (ativas: {pool._used})")

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