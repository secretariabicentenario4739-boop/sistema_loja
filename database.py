# database.py
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import os
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
            # Tentar obter a URL do banco do ambiente (Render)
            DATABASE_URL = os.getenv('DATABASE_URL')
            
            if DATABASE_URL:
                # Está no Render - usar URL
                logger.info("🔧 Configurando pool para o banco do Render")
                self._pool = psycopg2.pool.SimpleConnectionPool(
                    1, 20,  # mínimo 1, máximo 20 conexões
                    dsn=DATABASE_URL,
                    cursor_factory=RealDictCursor,
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5
                )
            else:
                # Está localmente - usar parâmetros diretos
                logger.info("🔧 Configurando pool para o banco local")
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

# Funções simplificadas para compatibilidade com código existente
def get_db():
    """Obtém cursor e conexão do pool (compatível com código antigo)"""
    return db_pool.get_connection()

def return_connection(conn):
    """Retorna a conexão para o pool (compatível com código antigo)"""
    db_pool.return_connection(conn)

# Decorador para gerenciar conexões automaticamente
def with_db_connection(func):
    """Decorador que gerencia automaticamente a conexão com o banco"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        cursor = None
        conn = None
        try:
            cursor, conn = get_db()
            result = func(cursor, *args, **kwargs)
            conn.commit()
            return result
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Erro na função {func.__name__}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                return_connection(conn)
    
    return wrapper

# Context manager para usar com 'with'
class DatabaseConnection:
    """Context manager para conexões com o banco"""
    
    def __enter__(self):
        self.cursor, self.conn = get_db()
        return self.cursor
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        
        return_connection(self.conn)

# Context manager simplificado
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