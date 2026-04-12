# database.py
import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging
from functools import wraps
import time
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabasePool:
    """Singleton para gerenciar pool de conexões PostgreSQL"""
    
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabasePool, cls).__new__(cls)
            cls._instance._initialize_pool()
        return cls._instance
    
    def _initialize_pool(self):
        """Inicializa o pool de conexões PostgreSQL"""
        try:
            DATABASE_URL = os.getenv('DATABASE_URL')
            
            if DATABASE_URL:
                # Render - usar URL
                logger.info("🔧 Configurando pool para o banco do Render")
                self._pool = psycopg2.pool.SimpleConnectionPool(
                    1, 20,
                    dsn=DATABASE_URL,
                    cursor_factory=RealDictCursor,
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5
                )
            else:
                # Local - PostgreSQL
                logger.info("🔧 Configurando pool para o banco local (PostgreSQL)")
                self._pool = psycopg2.pool.SimpleConnectionPool(
                    1, 20,
                    host=os.getenv('DB_HOST', 'localhost'),
                    port=os.getenv('DB_PORT', '5432'),
                    dbname=os.getenv('DB_NAME', 'sistema_maconico'),
                    user=os.getenv('DB_USER', 'postgres'),
                    password=os.getenv('DB_PASSWORD', 'postgres'),
                    cursor_factory=RealDictCursor,
                    connect_timeout=10
                )
            
            logger.info(f"✅ Pool de conexões PostgreSQL criado com sucesso!")
            
        except Exception as e:
            logger.error(f"❌ Erro ao criar pool de conexões: {e}")
            raise
    
    def get_connection(self):
        """Obtém uma conexão do pool"""
        if self._pool is None:
            self._initialize_pool()
        
        try:
            conn = self._pool.getconn()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            logger.debug(f"🔌 Conexão obtida do pool (ativas: {self._pool._used})")
            return cursor, conn
        except Exception as e:
            logger.error(f"❌ Erro ao obter conexão: {e}")
            raise
    
    def return_connection(self, conn):
        """Retorna a conexão para o pool"""
        if self._pool and conn:
            self._pool.putconn(conn)
            logger.debug(f"🔌 Conexão retornada ao pool (ativas: {self._pool._used})")
    
    def close_all_connections(self):
        """Fecha todas as conexões do pool"""
        if self._pool:
            self._pool.closeall()
            logger.info("🔌 Todas as conexões foram fechadas")

# Instância global do pool
db_pool = DatabasePool()

# Funções para compatibilidade
def get_db():
    """Obtém cursor e conexão do pool"""
    return db_pool.get_connection()

def return_connection(conn):
    """Retorna a conexão para o pool"""
    db_pool.return_connection(conn)

# Context manager
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

# Decorador para gerenciar conexões automaticamente
def with_db_connection(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        cursor, conn = get_db()
        try:
            result = func(cursor, *args, **kwargs)
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            return_connection(conn)
    return wrapper