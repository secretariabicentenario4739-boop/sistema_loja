# backup_system.py - Sistema completo de backup e restauração
# -*- coding: utf-8 -*-

import os
import json
import zipfile
import shutil
import threading
import time
import logging
from datetime import datetime
from flask import jsonify, send_file, render_template, flash, redirect, request, session
import psycopg2

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================
# CONFIGURAÇÕES DE BACKUP
# =============================

class BackupConfig:
    """Configurações do sistema de backup"""
    
    BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
    TEMP_DIR = os.path.join(BACKUP_DIR, 'temp')
    MAX_BACKUPS = 30
    MAX_BACKUP_AGE_DAYS = 90
    AUTO_BACKUP_HOUR = 2
    AUTO_BACKUP_ENABLED = False  # Desabilitado por padrão para não interferir
    COMPRESSION_LEVEL = 9
    
    @classmethod
    def ensure_directories(cls):
        """Garante que os diretórios necessários existem"""
        os.makedirs(cls.BACKUP_DIR, exist_ok=True)
        os.makedirs(cls.TEMP_DIR, exist_ok=True)
    
    @classmethod
    def get_backup_path(cls, backup_type='full', timestamp=None):
        """Gera o caminho do arquivo de backup"""
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        db_name = os.getenv('DB_NAME', 'sistema_maconico')
        filename = f"backup_{db_name}_{backup_type}_{timestamp}.zip"
        return os.path.join(cls.BACKUP_DIR, filename)
    
    @classmethod
    def get_backup_info_path(cls, backup_file):
        """Gera o caminho do arquivo de informações do backup"""
        return backup_file.replace('.zip', '.json')


# =============================
# GERENCIADOR DE BACKUP
# =============================

class BackupManager:
    """Gerencia todas as operações de backup e restauração"""
    
    def __init__(self, db_url):
        self.db_url = db_url
        self.config = BackupConfig
        self.config.ensure_directories()
        logger.info("BackupManager inicializado")
    
    def create_backup(self, backup_type='full', tables=None):
        """Cria um backup do banco de dados"""
        try:
            logger.info(f"Iniciando backup tipo: {backup_type}")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = self.config.get_backup_path(backup_type, timestamp)
            temp_sql_file = os.path.join(self.config.TEMP_DIR, f'backup_{timestamp}.sql')
            
            # Conectar ao banco
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = False
            cursor = conn.cursor()
            
            # Obter lista de tabelas
            cursor.execute("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'public' 
                ORDER BY tablename
            """)
            all_tables = [row[0] for row in cursor.fetchall()]
            
            if tables:
                backup_tables = [t for t in all_tables if t in tables]
            else:
                backup_tables = all_tables
            
            # Criar arquivo SQL
            with open(temp_sql_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Backup do Sistema Maçônico\n")
                f.write(f"-- Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                f.write(f"-- Tipo: {backup_type.upper()}\n\n")
                
                f.write("BEGIN;\n\n")
                
                # Exportar estrutura
                if backup_type in ['full', 'structure']:
                    for table in backup_tables:
                        f.write(f"-- Estrutura da tabela: {table}\n")
                        cursor.execute(f"""
                            SELECT column_name, data_type, is_nullable, column_default
                            FROM information_schema.columns 
                            WHERE table_name = '{table}'
                            ORDER BY ordinal_position
                        """)
                        columns = cursor.fetchall()
                        
                        f.write(f"DROP TABLE IF EXISTS {table} CASCADE;\n")
                        f.write(f"CREATE TABLE {table} (\n")
                        col_defs = []
                        for col in columns:
                            col_def = f"    {col[0]} {col[1]}"
                            if col[2] == 'NO':
                                col_def += " NOT NULL"
                            if col[3]:
                                col_def += f" DEFAULT {col[3]}"
                            col_defs.append(col_def)
                        f.write(",\n".join(col_defs))
                        f.write("\n);\n\n")
                
                # Exportar dados
                if backup_type in ['full', 'data']:
                    for table in backup_tables:
                        f.write(f"-- Dados da tabela: {table}\n")
                        f.write(f"DELETE FROM {table};\n\n")
                        
                        cursor.execute(f"SELECT * FROM {table}")
                        rows = cursor.fetchall()
                        
                        if rows:
                            cursor.execute(f"""
                                SELECT column_name 
                                FROM information_schema.columns 
                                WHERE table_name = '{table}'
                                ORDER BY ordinal_position
                            """)
                            col_names = [row[0] for row in cursor.fetchall()]
                            col_names_str = ', '.join(col_names)
                            
                            for row in rows:
                                values = []
                                for val in row:
                                    if val is None:
                                        values.append('NULL')
                                    elif isinstance(val, str):
                                        escaped = val.replace("'", "''")
                                        values.append(f"'{escaped}'")
                                    elif isinstance(val, datetime):
                                        values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                                    elif isinstance(val, (int, float)):
                                        values.append(str(val))
                                    else:
                                        values.append(f"'{str(val)}'")
                                
                                f.write(f"INSERT INTO {table} ({col_names_str}) VALUES ({', '.join(values)});\n")
                            f.write("\n")
                
                f.write("COMMIT;\n")
            
            cursor.close()
            conn.close()
            
            # Compactar
            with zipfile.ZipFile(backup_file, 'w', zipfile.ZIP_DEFLATED, compresslevel=self.config.COMPRESSION_LEVEL) as zf:
                zf.write(temp_sql_file, os.path.basename(temp_sql_file))
            
            backup_size = os.path.getsize(backup_file)
            os.remove(temp_sql_file)
            
            # Metadados
            metadata = {
                'filename': os.path.basename(backup_file),
                'type': backup_type,
                'created_at': datetime.now().isoformat(),
                'size_bytes': backup_size,
                'size_mb': round(backup_size / (1024 * 1024), 2),
                'tables_count': len(backup_tables)
            }
            
            metadata_file = self.config.get_backup_info_path(backup_file)
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            self.cleanup_old_backups()
            
            return metadata
            
        except Exception as e:
            logger.error(f"Erro ao criar backup: {e}")
            raise
    
    def restore_backup(self, backup_file, dry_run=False):
        """Restaura um backup"""
        try:
            if not os.path.exists(backup_file):
                raise FileNotFoundError(f"Arquivo não encontrado: {backup_file}")
            
            if dry_run:
                return self._validate_backup(backup_file)
            
            temp_dir = os.path.join(self.config.TEMP_DIR, f'restore_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
            os.makedirs(temp_dir, exist_ok=True)
            
            try:
                with zipfile.ZipFile(backup_file, 'r') as zf:
                    zf.extractall(temp_dir)
                
                sql_files = [f for f in os.listdir(temp_dir) if f.endswith('.sql')]
                if not sql_files:
                    raise Exception("Arquivo SQL não encontrado")
                
                sql_file = os.path.join(temp_dir, sql_files[0])
                
                conn = psycopg2.connect(self.db_url)
                conn.autocommit = False
                cursor = conn.cursor()
                
                with open(sql_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                commands_executed = 0
                for command in sql_content.split(';'):
                    command = command.strip()
                    if command and not command.startswith('--'):
                        try:
                            cursor.execute(command)
                            commands_executed += 1
                        except Exception as e:
                            logger.warning(f"Erro: {command[:100]}... - {e}")
                
                conn.commit()
                cursor.close()
                conn.close()
                
                return {
                    'success': True,
                    'commands_executed': commands_executed
                }
                
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)
                
        except Exception as e:
            logger.error(f"Erro ao restaurar: {e}")
            raise
    
    def _validate_backup(self, backup_file):
        """Valida um backup"""
        try:
            with zipfile.ZipFile(backup_file, 'r') as zf:
                bad_files = zf.testzip()
                if bad_files:
                    return {'valid': False, 'error': f'Arquivo corrompido: {bad_files}'}
                
                sql_files = [f for f in zf.namelist() if f.endswith('.sql')]
                if not sql_files:
                    return {'valid': False, 'error': 'Backup não contém arquivo SQL'}
                
                return {
                    'valid': True,
                    'files': len(zf.namelist()),
                    'size_mb': round(os.path.getsize(backup_file) / (1024 * 1024), 2)
                }
        except Exception as e:
            return {'valid': False, 'error': str(e)}
    
    def list_backups(self):
        """Lista backups disponíveis"""
        backups = []
        
        if not os.path.exists(self.config.BACKUP_DIR):
            return backups
        
        for file in os.listdir(self.config.BACKUP_DIR):
            if file.endswith('.zip'):
                filepath = os.path.join(self.config.BACKUP_DIR, file)
                mtime = os.path.getmtime(filepath)
                size = os.path.getsize(filepath) / (1024 * 1024)
                
                metadata_file = filepath.replace('.zip', '.json')
                metadata = None
                if os.path.exists(metadata_file):
                    try:
                        with open(metadata_file, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                    except:
                        pass
                
                backups.append({
                    'name': file,
                    'path': filepath,
                    'date': datetime.fromtimestamp(mtime),
                    'date_str': datetime.fromtimestamp(mtime).strftime('%d/%m/%Y %H:%M:%S'),
                    'size_mb': round(size, 2),
                    'metadata': metadata
                })
        
        backups.sort(key=lambda x: x['date'], reverse=True)
        return backups
    
    def cleanup_old_backups(self):
        """Remove backups antigos"""
        try:
            backups = self.list_backups()
            now = datetime.now()
            deleted = 0
            
            for i, backup in enumerate(backups):
                age_days = (now - backup['date']).days
                
                if i >= self.config.MAX_BACKUPS or age_days > self.config.MAX_BACKUP_AGE_DAYS:
                    if os.path.exists(backup['path']):
                        os.remove(backup['path'])
                    
                    metadata_file = backup['path'].replace('.zip', '.json')
                    if os.path.exists(metadata_file):
                        os.remove(metadata_file)
                    
                    deleted += 1
            
            if deleted > 0:
                logger.info(f"Limpeza: {deleted} backup(s) removido(s)")
            
        except Exception as e:
            logger.error(f"Erro na limpeza: {e}")
    
    def get_backup_stats(self):
        """Estatísticas dos backups"""
        backups = self.list_backups()
        
        if not backups:
            return {
                'total': 0,
                'total_size_mb': 0,
                'newest': None,
                'by_type': {}
            }
        
        total_size = sum(b['size_mb'] for b in backups)
        
        stats = {
            'total': len(backups),
            'total_size_mb': round(total_size, 2),
            'newest': backups[0]['date_str'] if backups else None,
            'by_type': {}
        }
        
        for backup in backups:
            if backup['metadata']:
                backup_type = backup['metadata'].get('type', 'unknown')
                stats['by_type'][backup_type] = stats['by_type'].get(backup_type, 0) + 1
        
        return stats


# =============================
# FUNÇÃO DE INICIALIZAÇÃO DAS ROTAS
# =============================

def init_backup_routes(app, backup_manager):
    """Inicializa as rotas de backup no Flask"""
    
    @app.route("/admin/backup")
    def backup_page():
        """Página de gerenciamento de backups"""
        if session.get("tipo") != "admin":
            flash("Acesso restrito a administradores", "danger")
            return redirect("/dashboard")
        
        stats = backup_manager.get_backup_stats()
        backups = backup_manager.list_backups()
        return render_template("admin/backup.html", 
                              stats=stats, 
                              backups=backups,
                              config=BackupConfig)
    
    @app.route("/api/backup/list")
    def api_list_backups():
        """API para listar backups"""
        try:
            backups = backup_manager.list_backups()
            return jsonify({
                'success': True,
                'backups': [{
                    'name': b['name'],
                    'date': b['date_str'],
                    'size_mb': b['size_mb'],
                    'type': b['metadata'].get('type', 'full') if b['metadata'] else 'full'
                } for b in backups]
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route("/api/backup/create", methods=["POST"])
    def api_create_backup():
        """API para criar backup"""
        if session.get("tipo") != "admin":
            return jsonify({'success': False, 'error': 'Acesso negado'}), 403
        
        try:
            data = request.get_json() or {}
            backup_type = data.get('type', 'full')
            
            backup = backup_manager.create_backup(backup_type)
            return jsonify({
                'success': True,
                'backup': backup,
                'message': f"Backup criado com sucesso: {backup['filename']}"
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route("/api/backup/restore/<filename>", methods=["POST"])
    def api_restore_backup(filename):
        """API para restaurar backup"""
        if session.get("tipo") != "admin":
            return jsonify({'success': False, 'error': 'Acesso negado'}), 403
        
        try:
            backup_path = os.path.join(BackupConfig.BACKUP_DIR, filename)
            if not os.path.exists(backup_path):
                return jsonify({'success': False, 'error': 'Arquivo não encontrado'}), 404
            
            data = request.get_json() or {}
            dry_run = data.get('dry_run', False)
            
            result = backup_manager.restore_backup(backup_path, dry_run)
            
            if dry_run:
                return jsonify(result)
            else:
                return jsonify({
                    'success': True,
                    'result': result,
                    'message': f"Backup {filename} restaurado com sucesso!"
                })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route("/api/backup/download/<filename>")
    def api_download_backup(filename):
        """API para baixar backup"""
        if session.get("tipo") != "admin":
            flash("Acesso restrito a administradores", "danger")
            return redirect("/dashboard")
        
        try:
            backup_path = os.path.join(BackupConfig.BACKUP_DIR, filename)
            if not os.path.exists(backup_path):
                flash("Arquivo não encontrado", "danger")
                return redirect("/admin/backup")
            
            return send_file(
                backup_path,
                as_attachment=True,
                download_name=filename,
                mimetype='application/zip'
            )
        except Exception as e:
            flash(f"Erro ao baixar: {str(e)}", "danger")
            return redirect("/admin/backup")
    
    @app.route("/api/backup/delete/<filename>", methods=["DELETE"])
    def api_delete_backup(filename):
        """API para excluir backup"""
        if session.get("tipo") != "admin":
            return jsonify({'success': False, 'error': 'Acesso negado'}), 403
        
        try:
            backup_path = os.path.join(BackupConfig.BACKUP_DIR, filename)
            if not os.path.exists(backup_path):
                return jsonify({'success': False, 'error': 'Arquivo não encontrado'}), 404
            
            os.remove(backup_path)
            
            metadata_path = backup_path.replace('.zip', '.json')
            if os.path.exists(metadata_path):
                os.remove(metadata_path)
            
            return jsonify({'success': True, 'message': 'Backup excluído com sucesso'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route("/api/backup/cleanup", methods=["POST"])
    def api_cleanup_backups():
        """API para limpar backups antigos"""
        if session.get("tipo") != "admin":
            return jsonify({'success': False, 'error': 'Acesso negado'}), 403
        
        try:
            backup_manager.cleanup_old_backups()
            return jsonify({'success': True, 'message': 'Limpeza realizada com sucesso'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route("/api/backup/stats")
    def api_backup_stats():
        """API para estatísticas de backup"""
        try:
            stats = backup_manager.get_backup_stats()
            return jsonify({'success': True, 'stats': stats})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route("/api/backup/validate/<filename>")
    def api_validate_backup(filename):
        """API para validar um backup"""
        if session.get("tipo") != "admin":
            return jsonify({'success': False, 'error': 'Acesso negado'}), 403
        
        try:
            backup_path = os.path.join(BackupConfig.BACKUP_DIR, filename)
            if not os.path.exists(backup_path):
                return jsonify({'success': False, 'error': 'Arquivo não encontrado'}), 404
            
            result = backup_manager._validate_backup(backup_path)
            return jsonify({'success': True, 'validation': result})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500


def init_backup_system(app, db_url):
    """Inicializa o sistema completo de backup"""
    backup_manager = BackupManager(db_url)
    init_backup_routes(app, backup_manager)
    print("✅ Sistema de backup inicializado com sucesso!")
    return backup_manager, None