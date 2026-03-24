# config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Configurações do Flask
    SECRET_KEY = os.getenv('SECRET_KEY', os.urandom(24))
    
    # Configurações de upload
    UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'uploads/documentos')
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'png', 'gif', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'zip', 'rar'}
    
    # Configurações do banco de dados
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('DB_NAME', 'sistema_maconico'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }