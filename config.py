import os
from dotenv import load_dotenv
import cloudinary
import cloudinary.uploader
import cloudinary.api
import psycopg2
from psycopg2 import pool
import urllib.parse

load_dotenv()

class Config:
    # Configuração do Banco de Dados
    DATABASE_URL = os.environ.get('DATABASE_URL', '')
    
    # Configuração do Cloudinary
    CLOUDINARY_CLOUD_NAME = os.environ.get('CLOUDINARY_CLOUD_NAME', '')
    CLOUDINARY_API_KEY = os.environ.get('CLOUDINARY_API_KEY', '')
    CLOUDINARY_API_SECRET = os.environ.get('CLOUDINARY_API_SECRET', '')
    
    # Configuração do Flask
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.environ.get('DEBUG', 'False') == 'True'
    
    # Configuração do Pool de Conexões
    DB_POOL_MIN = int(os.environ.get('DB_POOL_MIN', 1))
    DB_POOL_MAX = int(os.environ.get('DB_POOL_MAX', 10))

# Configurar Cloudinary
cloudinary.config(
    cloud_name=Config.CLOUDINARY_CLOUD_NAME,
    api_key=Config.CLOUDINARY_API_KEY,
    api_secret=Config.CLOUDINARY_API_SECRET,
    secure=True
)

# Pool de conexões global
db_pool = None

def init_db_pool():
    """Inicializa o pool de conexões com o banco de dados"""
    global db_pool
    try:
        if db_pool is None:
            database_url = Config.DATABASE_URL
            
            # Tratar URL do Render
            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql://', 1)
            
            print(f"🔧 Conectando ao banco: {database_url.split('@')[0]}@***")
            
            db_pool = psycopg2.pool.SimpleConnectionPool(
                Config.DB_POOL_MIN,
                Config.DB_POOL_MAX,
                database_url,
                sslmode='require'
            )
            print("✅ Pool de conexões inicializado com sucesso!")
        return db_pool
    except Exception as e:
        print(f"❌ Erro ao inicializar pool: {e}")
        raise

def get_db_connection():
    """Obtém uma conexão do pool"""
    global db_pool
    if db_pool is None:
        db_pool = init_db_pool()
    return db_pool.getconn()

def return_db_connection(conn):
    """Retorna a conexão para o pool"""
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)

def close_all_connections():
    """Fecha todas as conexões do pool"""
    global db_pool
    if db_pool:
        db_pool.closeall()
        db_pool = None
        print("🔌 Todas as conexões foram fechadas")