# database.py
import mysql.connector
from mysql.connector import pooling
import logging
from functools import wraps
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabasePool:
    """Singleton para gerenciar pool de conexões"""
    
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabasePool, cls).__new__(cls)
            cls._instance._initialize_pool()
        return cls._instance
    
    def _initialize_pool(self):
        """Inicializa o pool de conexões"""
        try:
            config = {
                'host': 'localhost',
                'user': 'seu_usuario',
                'password': 'sua_senha',
                'database': 'seu_banco',
                'pool_name': 'mypool',
                'pool_size': 10,  # Número máximo de conexões simultâneas
                'pool_reset_session': True,
                'autocommit': False,
                'use_pure': True
            }
            
            self._pool = mysql.connector.pooling.MySQLConnectionPool(**config)
            logger.info(f"✅ Pool de conexões criado com sucesso (tamanho: {config['pool_size']})")
            
        except Exception as e:
            logger.error(f"❌ Erro ao criar pool de conexões: {e}")
            raise
    
    def get_connection(self):
        """Obtém uma conexão do pool"""
        if self._pool is None:
            self._initialize_pool()
        
        try:
            connection = self._pool.get_connection()
            logger.debug("🔌 Conexão obtida do pool")
            return connection
        except Exception as e:
            logger.error(f"❌ Erro ao obter conexão: {e}")
            raise
    
    def close_all_connections(self):
        """Fecha todas as conexões do pool"""
        if self._pool:
            self._pool._remove_connections()
            logger.info("🔌 Todas as conexões foram fechadas")

# Instância global do pool
db_pool = DatabasePool()

# Decorador para gerenciar conexões automaticamente
def with_db_connection(func):
    """Decorador que gerencia automaticamente a conexão com o banco"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        connection = None
        cursor = None
        try:
            connection = db_pool.get_connection()
            cursor = connection.cursor(dictionary=True)
            result = func(cursor, *args, **kwargs)
            connection.commit()
            return result
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Erro na função {func.__name__}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                logger.debug("🔌 Conexão retornada ao pool")
    
    return wrapper

# Context manager para usar com 'with'
class DatabaseConnection:
    """Context manager para conexões com o banco"""
    
    def __enter__(self):
        self.connection = db_pool.get_connection()
        self.cursor = self.connection.cursor(dictionary=True)
        return self.cursor
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.connection.rollback()
        else:
            self.connection.commit()
        
        self.cursor.close()
        self.connection.close()