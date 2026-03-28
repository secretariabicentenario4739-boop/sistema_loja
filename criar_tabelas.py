#!/usr/bin/env python
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Usar a DATABASE_URL do Render
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    # Se estiver rodando localmente, use a URL do Render
    DATABASE_URL = "postgresql://admin_sistema:1kmSlZWYS4IywIdCGskYDkAnFF4mzyTL@dpg-d704e30gjchc73d0hv10-a.oregon-postgres.render.com:5432/sistema_maconico"

def criar_tabelas():
    """Cria todas as tabelas no banco do Render"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        print("🚀 Criando tabelas no banco do Render...")
        
        # Criar tabela assinaturas_ata
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.assinaturas_ata (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER,
                tipo VARCHAR(100),
                data_assinatura DATE,
                status VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Tabela assinaturas_ata criada")
        
        # Criar tabela logs_auditoria
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.logs_auditoria (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER,
                usuario_nome VARCHAR(255),
                acao VARCHAR(100),
                entidade VARCHAR(100),
                entidade_id INTEGER,
                dados_anteriores TEXT,
                dados_novos TEXT,
                ip VARCHAR(45),
                user_agent TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Tabela logs_auditoria criada")
        
        # Criar índices
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_logs_usuario_id 
            ON logs_auditoria(usuario_id)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_logs_created_at 
            ON logs_auditoria(created_at)
        """)
        print("✅ Índices criados")
        
        conn.commit()
        
        # Verificar
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        print(f"\n📋 Tabelas existentes: {[t[0] for t in tables]}")
        
        cursor.close()
        conn.close()
        
        print("\n✅ Todas as tabelas criadas com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        raise

if __name__ == "__main__":
    criar_tabelas()