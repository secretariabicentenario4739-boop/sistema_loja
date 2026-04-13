# config.py
import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Configurações do Flask
    SECRET_KEY = os.getenv('SECRET_KEY', os.urandom(24))
    
    # Configurações de upload
    UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'uploads/documentos')
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'png', 'gif', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'zip', 'rar'}
    
    # Configurações do banco de dados - VERSÃO PARA RENDER
    DATABASE_URL = os.getenv('DATABASE_URL', '')
    
    @classmethod
    def get_db_config(cls):
        """Retorna a configuração do banco de dados"""
        # Se tiver DATABASE_URL (Render), usa ela
        if cls.DATABASE_URL:
            return {'database_url': cls.DATABASE_URL}
        
        # Fallback para configuração local
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'dbname': os.getenv('DB_NAME', 'sistema_maconico'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }