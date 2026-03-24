#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para backup automático do banco de dados PostgreSQL
Execute: python backup_automatico.py --now (para backup imediato)
       python backup_automatico.py --schedule (para executar em segundo plano)
"""

import os
import sys
import subprocess
import datetime
import time
import schedule
import zipfile
import shutil
from dotenv import load_dotenv
from db_config import get_db, return_connection

# Carregar variáveis de ambiente
load_dotenv()

# Configurações
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'sistema_maconico'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# Configurações de backup
BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
BACKUP_RETENTION_DAYS = 30  # Manter backups dos últimos 30 dias
MAX_BACKUPS = 20  # Máximo de backups a manter

def criar_diretorio_backup():
    """Cria o diretório de backups se não existir"""
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
        print(f"✅ Diretório de backups criado: {BACKUP_DIR}")

def get_backup_filename():
    """Gera nome do arquivo de backup"""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"backup_{DB_CONFIG['dbname']}_{timestamp}.sql"

def fazer_backup():
    """Faz o backup do banco de dados"""
    try:
        criar_diretorio_backup()
        
        filename = get_backup_filename()
        filepath = os.path.join(BACKUP_DIR, filename)
        
        print(f"\n🔄 Iniciando backup do banco {DB_CONFIG['dbname']}...")
        print(f"   Arquivo: {filename}")
        
        # Comando pg_dump
        cmd = [
            'pg_dump',
            '-h', DB_CONFIG['host'],
            '-p', DB_CONFIG['port'],
            '-U', DB_CONFIG['user'],
            '-d', DB_CONFIG['dbname'],
            '-f', filepath,
            '--clean',
            '--if-exists',
            '--no-owner',
            '--no-privileges'
        ]
        
        # Executar backup
        result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PGPASSWORD': DB_CONFIG['password']})
        
        if result.returncode == 0:
            # Compactar arquivo
            zip_filename = filepath + '.zip'
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(filepath, os.path.basename(filepath))
            
            # Remover arquivo SQL original
            os.remove(filepath)
            
            # Calcular tamanho
            tamanho = os.path.getsize(zip_filename) / (1024 * 1024)  # MB
            
            print(f"✅ Backup concluído com sucesso!")
            print(f"   Arquivo: {os.path.basename(zip_filename)}")
            print(f"   Tamanho: {tamanho:.2f} MB")
            
            # Registrar no log do sistema
            registrar_log_backup(filename, tamanho, 'sucesso')
            
            # Limpar backups antigos
            limpar_backups_antigos()
            
            return True
        else:
            print(f"❌ Erro no backup: {result.stderr}")
            registrar_log_backup(filename, 0, 'erro', result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Erro ao fazer backup: {e}")
        registrar_log_backup('erro', 0, 'erro', str(e))
        return False

def limpar_backups_antigos():
    """Remove backups antigos mantendo apenas os últimos MAX_BACKUPS e os de até BACKUP_RETENTION_DAYS dias"""
    try:
        # Listar arquivos de backup
        backups = []
        for file in os.listdir(BACKUP_DIR):
            if file.startswith('backup_') and file.endswith('.zip'):
                filepath = os.path.join(BACKUP_DIR, file)
                mtime = os.path.getmtime(filepath)
                backups.append((mtime, filepath))
        
        # Ordenar por data (mais recentes primeiro)
        backups.sort(reverse=True)
        
        # Calcular data limite
        data_limite = datetime.datetime.now() - datetime.timedelta(days=BACKUP_RETENTION_DAYS)
        
        removidos = 0
        for i, (mtime, filepath) in enumerate(backups):
            data_arquivo = datetime.datetime.fromtimestamp(mtime)
            
            # Remover se exceder o número máximo ou for mais antigo que a data limite
            if i >= MAX_BACKUPS or data_arquivo < data_limite:
                os.remove(filepath)
                removidos += 1
        
        if removidos > 0:
            print(f"🧹 {removidos} backup(s) antigo(s) removido(s)")
            
    except Exception as e:
        print(f"⚠️ Erro ao limpar backups antigos: {e}")

def registrar_log_backup(arquivo, tamanho, status, erro=None):
    """Registra o backup no log do sistema"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            INSERT INTO notificacoes_log 
            (destinatario, assunto, corpo, tipo, status, erro, data_envio)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            'sistema',
            f'Backup - {arquivo}',
            f'Arquivo: {arquivo}\nTamanho: {tamanho:.2f} MB' if tamanho > 0 else '',
            'backup',
            status,
            erro,
            datetime.datetime.now()
        ))
        conn.commit()
        return_connection(conn)
    except Exception as e:
        print(f"Erro ao registrar log: {e}")

def listar_backups():
    """Lista os backups disponíveis"""
    criar_diretorio_backup()
    
    backups = []
    for file in os.listdir(BACKUP_DIR):
        if file.startswith('backup_') and file.endswith('.zip'):
            filepath = os.path.join(BACKUP_DIR, file)
            mtime = os.path.getmtime(filepath)
            size = os.path.getsize(filepath) / (1024 * 1024)
            backups.append({
                'nome': file,
                'data': datetime.datetime.fromtimestamp(mtime),
                'tamanho': size,
                'caminho': filepath
            })
    
    # Ordenar por data (mais recentes primeiro)
    backups.sort(key=lambda x: x['data'], reverse=True)
    
    return backups

def restaurar_backup(arquivo):
    """Restaura um backup"""
    try:
        filepath = os.path.join(BACKUP_DIR, arquivo)
        
        if not os.path.exists(filepath):
            print(f"❌ Arquivo não encontrado: {arquivo}")
            return False
        
        print(f"\n🔄 Restaurando backup: {arquivo}")
        
        # Descompactar se for zip
        if filepath.endswith('.zip'):
            with zipfile.ZipFile(filepath, 'r') as zipf:
                sql_file = zipf.namelist()[0]
                zipf.extractall(BACKUP_DIR)
                sql_path = os.path.join(BACKUP_DIR, sql_file)
        else:
            sql_path = filepath
        
        # Comando psql para restaurar
        cmd = [
            'psql',
            '-h', DB_CONFIG['host'],
            '-p', DB_CONFIG['port'],
            '-U', DB_CONFIG['user'],
            '-d', DB_CONFIG['dbname'],
            '-f', sql_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, 'PGPASSWORD': DB_CONFIG['password']})
        
        # Remover arquivo SQL temporário
        if filepath.endswith('.zip') and os.path.exists(sql_path):
            os.remove(sql_path)
        
        if result.returncode == 0:
            print("✅ Backup restaurado com sucesso!")
            registrar_log_backup(arquivo, 0, 'restaurado')
            return True
        else:
            print(f"❌ Erro ao restaurar: {result.stderr}")
            registrar_log_backup(arquivo, 0, 'erro_restauracao', result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ Erro ao restaurar: {e}")
        return False

def executar_agora():
    """Executa backup imediatamente"""
    print("\n" + "="*60)
    print("🚀 INICIANDO BACKUP MANUAL")
    print("="*60)
    fazer_backup()

def executar_agendado():
    """Executa backup agendado"""
    print("\n" + "="*60)
    print("⏰ EXECUTANDO BACKUP AGENDADO")
    print("="*60)
    fazer_backup()

def iniciar_agendamento():
    """Inicia o agendamento de backups"""
    print("\n" + "="*60)
    print("📅 INICIANDO SERVIÇO DE BACKUP AUTOMÁTICO")
    print("="*60)
    print(f"📁 Diretório de backups: {BACKUP_DIR}")
    print(f"💾 Reter backups dos últimos {BACKUP_RETENTION_DAYS} dias")
    print(f"📦 Máximo de {MAX_BACKUPS} backups armazenados")
    print("\n⏰ Agendamentos:")
    print("   • 02:00 - Backup diário")
    print("   • 12:00 - Backup diário (segundo horário)")
    print("   • 08:00 - Backup no início do expediente")
    print("   • 18:00 - Backup no fim do expediente")
    print("\n📌 Pressione Ctrl+C para parar")
    print("="*60)
    
    # Agendar backups
    schedule.every().day.at("02:00").do(executar_agendado)
    schedule.every().day.at("12:00").do(executar_agendado)
    schedule.every().day.at("08:00").do(executar_agendado)
    schedule.every().day.at("18:00").do(executar_agendado)
    
    # Executar um backup ao iniciar
    executar_agendado()
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Verificar a cada minuto
    except KeyboardInterrupt:
        print("\n\n🛑 Serviço de backup interrompido")

def mostrar_menu():
    """Mostra menu interativo"""
    print("\n" + "="*60)
    print("📦 SISTEMA DE BACKUP AUTOMÁTICO")
    print("="*60)
    print("\n1 - Fazer backup agora")
    print("2 - Listar backups disponíveis")
    print("3 - Restaurar backup")
    print("4 - Iniciar serviço automático (agendado)")
    print("5 - Sair")
    
    opcao = input("\nEscolha uma opção (1-5): ").strip()
    
    if opcao == "1":
        executar_agora()
    elif opcao == "2":
        backups = listar_backups()
        if backups:
            print("\n📋 BACKUPS DISPONÍVEIS:\n")
            for i, b in enumerate(backups, 1):
                print(f"  {i}. {b['nome']}")
                print(f"     Data: {b['data'].strftime('%d/%m/%Y %H:%M:%S')}")
                print(f"     Tamanho: {b['tamanho']:.2f} MB")
                print()
        else:
            print("\n📭 Nenhum backup encontrado")
    elif opcao == "3":
        backups = listar_backups()
        if backups:
            print("\n📋 BACKUPS DISPONÍVEIS:\n")
            for i, b in enumerate(backups, 1):
                print(f"  {i}. {b['nome']} - {b['data'].strftime('%d/%m/%Y %H:%M:%S')}")
            
            try:
                escolha = int(input("\nDigite o número do backup para restaurar: ")) - 1
                if 0 <= escolha < len(backups):
                    confirmar = input(f"Restaurar backup {backups[escolha]['nome']}? (s/n): ")
                    if confirmar.lower() == 's':
                        restaurar_backup(backups[escolha]['nome'])
                else:
                    print("Opção inválida!")
            except ValueError:
                print("Opção inválida!")
        else:
            print("\n📭 Nenhum backup encontrado")
    elif opcao == "4":
        iniciar_agendamento()
    elif opcao == "5":
        print("Saindo...")
        sys.exit(0)
    else:
        print("Opção inválida!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Sistema de Backup Automático')
    parser.add_argument('--now', action='store_true', help='Fazer backup agora')
    parser.add_argument('--schedule', action='store_true', help='Iniciar serviço agendado')
    parser.add_argument('--list', action='store_true', help='Listar backups')
    parser.add_argument('--menu', action='store_true', help='Abrir menu interativo')
    
    args = parser.parse_args()
    
    if args.now:
        executar_agora()
    elif args.schedule:
        iniciar_agendamento()
    elif args.list:
        backups = listar_backups()
        if backups:
            print("\n📋 BACKUPS DISPONÍVEIS:\n")
            for b in backups:
                print(f"  • {b['nome']}")
                print(f"    Data: {b['data'].strftime('%d/%m/%Y %H:%M:%S')}")
                print(f"    Tamanho: {b['tamanho']:.2f} MB\n")
        else:
            print("\n📭 Nenhum backup encontrado")
    else:
        mostrar_menu()