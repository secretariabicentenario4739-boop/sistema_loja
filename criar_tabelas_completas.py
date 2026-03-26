#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para criar a tabela de notificações
Execute: python criar_tabela_notificacoes.py
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import sys

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do banco de dados
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'sistema_maconico'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

def conectar_banco():
    """Estabelece conexão com o PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao PostgreSQL: {e}")
        print("\n📝 Verifique:")
        print("  1. O PostgreSQL está rodando?")
        print("  2. As credenciais no .env estão corretas?")
        print("  3. O banco de dados 'sistema_maconico' existe?")
        sys.exit(1)

def criar_tabela_notificacoes(cursor):
    """Cria a tabela de notificações"""
    print("\n📋 Criando tabela: notificacoes")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS notificacoes (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            tipo TEXT NOT NULL,
            titulo TEXT NOT NULL,
            mensagem TEXT NOT NULL,
            link TEXT,
            lida INTEGER DEFAULT 0,
            data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_leitura TIMESTAMP
        )
    """)
    print("✅ Tabela notificacoes criada/verificada")

def criar_indices(cursor):
    """Cria os índices para otimização"""
    print("\n🔍 Criando índices...")
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_notificacoes_usuario 
        ON notificacoes(usuario_id)
    """)
    print("✅ Índice idx_notificacoes_usuario criado")
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_notificacoes_lida 
        ON notificacoes(lida)
    """)
    print("✅ Índice idx_notificacoes_lida criado")
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_notificacoes_data 
        ON notificacoes(data_criacao)
    """)
    print("✅ Índice idx_notificacoes_data criado")

def verificar_tabela(cursor):
    """Verifica se a tabela foi criada corretamente"""
    print("\n📊 Verificando estrutura da tabela...")
    
    cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'notificacoes'
        ORDER BY ordinal_position
    """)
    
    colunas = cursor.fetchall()
    
    print("\n📋 Estrutura da tabela notificacoes:")
    for col in colunas:
        print(f"  • {col[0]}: {col[1]} (Nullable: {col[2]})")
    
    # Verificar índices
    cursor.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'notificacoes'
    """)
    
    indices = cursor.fetchall()
    
    print("\n🔍 Índices criados:")
    for idx in indices:
        print(f"  • {idx[0]}")

def main():
    """Função principal"""
    print("\n" + "="*60)
    print("🚀 CRIANDO TABELA DE NOTIFICAÇÕES")
    print("="*60)
    
    conn = conectar_banco()
    cursor = conn.cursor()
    
    try:
        # 1. Criar tabela
        criar_tabela_notificacoes(cursor)
        
        # 2. Criar índices
        criar_indices(cursor)
        
        # 3. Commit das alterações
        conn.commit()
        
        # 4. Verificar estrutura
        verificar_tabela(cursor)
        
        print("\n" + "="*60)
        print("✅ TABELA DE NOTIFICAÇÕES CRIADA COM SUCESSO!")
        print("="*60)
        print("\n📌 Funcionalidades:")
        print("   • Armazenamento de notificações por usuário")
        print("   • Suporte a diferentes tipos (reuniao, sindicancia, etc.)")
        print("   • Controle de leitura (lida/não lida)")
        print("   • Links para redirecionamento")
        print("   • Índices otimizados para consultas")
        
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()