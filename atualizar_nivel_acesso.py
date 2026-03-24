#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para adicionar campo de nível de acesso e atualizar usuários
Execute: python atualizar_nivel_acesso.py
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import sys

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
        sys.exit(1)

def verificar_coluna_existe(cursor, tabela, coluna):
    """Verifica se uma coluna existe na tabela"""
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s AND column_name = %s
    """, (tabela, coluna))
    return cursor.fetchone() is not None

def adicionar_coluna_nivel_acesso(cursor):
    """Adiciona a coluna nivel_acesso se não existir"""
    print("\n📋 Verificando coluna nivel_acesso...")
    
    if verificar_coluna_existe(cursor, "usuarios", "nivel_acesso"):
        print("  ⚠️ Coluna 'nivel_acesso' já existe")
        return True
    
    try:
        cursor.execute("""
            ALTER TABLE usuarios 
            ADD COLUMN nivel_acesso INTEGER DEFAULT 1
        """)
        print("  ✅ Coluna 'nivel_acesso' adicionada com sucesso!")
        return True
    except Exception as e:
        print(f"  ❌ Erro ao adicionar coluna: {e}")
        return False

def atualizar_niveis_por_grau(cursor):
    """Atualiza níveis baseados no grau atual"""
    print("\n📊 Atualizando níveis baseados no grau atual...")
    
    # Aprendiz, Companheiro, Mestre
    cursor.execute("""
        UPDATE usuarios 
        SET nivel_acesso = grau_atual 
        WHERE grau_atual IN (1, 2, 3) 
          AND tipo != 'admin'
    """)
    qtd = cursor.rowcount
    print(f"  ✅ {qtd} usuário(s) atualizado(s) baseado no grau")
    
    # Administradores
    cursor.execute("""
        UPDATE usuarios 
        SET nivel_acesso = 4 
        WHERE tipo = 'admin'
    """)
    qtd_admin = cursor.rowcount
    print(f"  ✅ {qtd_admin} administrador(es) atualizado(s) para nível 4")
    
    return qtd + qtd_admin

def atualizar_usuarios_sem_nivel(cursor):
    """Atualiza usuários sem nível definido"""
    print("\n🔧 Atualizando usuários sem nível definido...")
    
    cursor.execute("""
        UPDATE usuarios 
        SET nivel_acesso = 1 
        WHERE nivel_acesso IS NULL 
          AND tipo != 'admin'
    """)
    qtd = cursor.rowcount
    if qtd > 0:
        print(f"  ✅ {qtd} usuário(s) atualizado(s) para nível 1 (Aprendiz)")
    else:
        print("  ℹ️ Nenhum usuário sem nível encontrado")
    
    return qtd

def mostrar_estatisticas(cursor):
    """Mostra estatísticas dos níveis de acesso"""
    print("\n" + "="*60)
    print("📊 ESTATÍSTICAS DOS NÍVEIS DE ACESSO")
    print("="*60)
    
    cursor.execute("""
        SELECT 
            nivel_acesso,
            COUNT(*) as total,
            string_agg(usuario, ', ' ORDER BY usuario) as usuarios
        FROM usuarios 
        GROUP BY nivel_acesso
        ORDER BY nivel_acesso
    """)
    
    niveis = cursor.fetchall()
    
    for nivel in niveis:
        descricao = {
            1: "👤 Aprendiz (acesso básico)",
            2: "⭐ Companheiro (acesso intermediário)",
            3: "🌟 Mestre (acesso avançado)",
            4: "👑 Administrador (acesso total)",
            None: "❓ Não definido"
        }.get(nivel[0], f"🎯 Nível {nivel[0]}")
        
        print(f"\n{descricao}: {nivel[1]} usuário(s)")
        if nivel[2]:
            usuarios = nivel[2].split(', ')
            if len(usuarios) <= 5:
                print(f"   Usuários: {', '.join(usuarios)}")
            else:
                print(f"   Usuários: {', '.join(usuarios[:5])} e mais {len(usuarios)-5}")

def listar_usuarios_com_niveis(cursor):
    """Lista todos os usuários com seus níveis"""
    print("\n" + "="*60)
    print("📋 LISTA DE USUÁRIOS COM NÍVEIS DE ACESSO")
    print("="*60)
    
    cursor.execute("""
        SELECT 
            id,
            usuario,
            nome_completo,
            tipo,
            grau_atual,
            nivel_acesso,
            CASE nivel_acesso
                WHEN 1 THEN 'Aprendiz'
                WHEN 2 THEN 'Companheiro'
                WHEN 3 THEN 'Mestre'
                WHEN 4 THEN 'Administrador'
                ELSE 'Não definido'
            END as nivel_descricao
        FROM usuarios
        ORDER BY nivel_acesso, usuario
    """)
    
    usuarios = cursor.fetchall()
    
    print(f"\n{'ID':<5} {'Usuário':<20} {'Nome':<25} {'Tipo':<12} {'Grau':<6} {'Nível':<12} {'Descrição':<15}")
    print("-" * 95)
    
    for u in usuarios:
        print(f"{u[0]:<5} {u[1]:<20} {(u[2] or '-')[:25]:<25} {u[3]:<12} {u[4] or '-':<6} {u[5]:<12} {u[6]:<15}")

def main():
    """Função principal"""
    print("\n" + "="*60)
    print("🚀 ATUALIZANDO NÍVEIS DE ACESSO DOS USUÁRIOS")
    print("="*60)
    print("\nNíveis de acesso:")
    print("  1 = Aprendiz - Acesso apenas a conteúdo de Aprendiz")
    print("  2 = Companheiro - Acesso a conteúdo de Aprendiz e Companheiro")
    print("  3 = Mestre - Acesso a todo conteúdo")
    print("  4 = Administrador - Acesso total ao sistema")
    
    conn = conectar_banco()
    cursor = conn.cursor()
    
    try:
        # 1. Adicionar coluna se não existir
        adicionar_coluna_nivel_acesso(cursor)
        
        # 2. Atualizar níveis baseados no grau
        atualizar_niveis_por_grau(cursor)
        
        # 3. Atualizar usuários sem nível
        atualizar_usuarios_sem_nivel(cursor)
        
        # Commit das alterações
        conn.commit()
        
        # 4. Mostrar estatísticas
        mostrar_estatisticas(cursor)
        
        # 5. Listar todos os usuários
        listar_usuarios_com_niveis(cursor)
        
        print("\n" + "="*60)
        print("✅ ATUALIZAÇÃO CONCLUÍDA COM SUCESSO!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()