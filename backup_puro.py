# backup_puro.py
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import zipfile
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'sistema_maconico'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

def backup_via_python():
    """Faz backup do banco via Python puro"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        dbname = DB_CONFIG['dbname']
        filename = f"backup_{dbname}_{timestamp}.sql"
        
        # Criar diretório de backup
        backup_dir = os.path.join(os.path.dirname(__file__), 'backups')
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        filepath = os.path.join(backup_dir, filename)
        
        print(f"\n🔄 Criando backup em: {filepath}")
        
        # Obter lista de tabelas
        cursor.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            ORDER BY tablename
        """)
        tabelas = cursor.fetchall()
        
        with open(filepath, 'w', encoding='utf-8') as f:
            # Escrever cabeçalho
            f.write(f"-- Backup do banco {dbname} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write("-- Gerado via Python\n\n")
            f.write("SET statement_timeout = 0;\n")
            f.write("SET lock_timeout = 0;\n")
            f.write("SET idle_in_transaction_session_timeout = 0;\n")
            f.write("SET client_encoding = 'UTF8';\n")
            f.write("SET standard_conforming_strings = on;\n")
            f.write("SELECT pg_catalog.set_config('search_path', '', false);\n")
            f.write("SET check_function_bodies = false;\n")
            f.write("SET xmloption = content;\n")
            f.write("SET client_min_messages = warning;\n")
            f.write("SET row_security = off;\n\n")
            
            # Para cada tabela, exportar dados
            for tabela in tabelas:
                nome_tabela = tabela[0]
                print(f"   Exportando tabela: {nome_tabela}")
                
                # Obter estrutura da tabela
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{nome_tabela}'
                    ORDER BY ordinal_position
                """)
                colunas = cursor.fetchall()
                
                # Escrever comando DELETE
                f.write(f"-- Dados da tabela: {nome_tabela}\n")
                f.write(f"DELETE FROM {nome_tabela};\n\n")
                
                # Obter dados
                cursor.execute(f"SELECT * FROM {nome_tabela}")
                dados = cursor.fetchall()
                
                if dados:
                    # Construir INSERT
                    colunas_nomes = [c[0] for c in colunas]
                    colunas_str = ', '.join(colunas_nomes)
                    
                    for row in dados:
                        valores = []
                        for i, val in enumerate(row):
                            if val is None:
                                valores.append('NULL')
                            elif isinstance(val, str):
                                # Escapar aspas simples
                                val_escapado = val.replace("'", "''")
                                valores.append(f"'{val_escapado}'")
                            elif isinstance(val, datetime):
                                valores.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                            else:
                                valores.append(str(val))
                        
                        valores_str = ', '.join(valores)
                        f.write(f"INSERT INTO {nome_tabela} ({colunas_str}) VALUES ({valores_str});\n")
                    f.write("\n")
            
            f.write("\n-- Fim do backup\n")
        
        # Compactar
        zip_filename = filepath + '.zip'
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(filepath, os.path.basename(filepath))
        
        # Remover arquivo SQL
        os.remove(filepath)
        
        tamanho = os.path.getsize(zip_filename) / (1024 * 1024)
        
        print(f"\n✅ Backup concluído!")
        print(f"   Arquivo: {os.path.basename(zip_filename)}")
        print(f"   Tamanho: {tamanho:.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

if __name__ == "__main__":
    backup_via_python()