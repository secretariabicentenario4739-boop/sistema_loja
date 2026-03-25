#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para criar/atualizar as tabelas de candidatos e filhos
Execute: python criar_tabelas_candidatos.py
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

def verificar_coluna_existe(cursor, tabela, coluna):
    """Verifica se uma coluna existe na tabela"""
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s AND column_name = %s
    """, (tabela, coluna))
    return cursor.fetchone() is not None

def adicionar_coluna(cursor, tabela, coluna, tipo, default=None):
    """Adiciona uma coluna à tabela se não existir"""
    if verificar_coluna_existe(cursor, tabela, coluna):
        print(f"  ⚠️ Coluna '{coluna}' já existe")
        return False
    
    try:
        sql = f"ALTER TABLE {tabela} ADD COLUMN {coluna} {tipo}"
        if default is not None:
            sql += f" DEFAULT {default}"
        cursor.execute(sql)
        print(f"  ✅ Coluna '{coluna}' ({tipo}) adicionada")
        return True
    except Exception as e:
        print(f"  ❌ Erro ao adicionar coluna '{coluna}': {e}")
        return False

def criar_tabela_filhos(cursor):
    """Cria a tabela de filhos do candidato"""
    print("\n📋 Criando tabela: filhos_candidato")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS filhos_candidato (
            id SERIAL PRIMARY KEY,
            candidato_id INTEGER NOT NULL REFERENCES candidatos(id) ON DELETE CASCADE,
            nome TEXT NOT NULL,
            data_nascimento DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✅ Tabela filhos_candidato criada/verificada")
    
    # Criar índices
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_filhos_candidato 
        ON filhos_candidato(candidato_id)
    """)
    print("✅ Índice idx_filhos_candidato criado")

def atualizar_tabela_candidatos(cursor):
    """Adiciona todas as colunas necessárias na tabela candidatos"""
    print("\n🔍 Verificando/Atualizando tabela candidatos...")
    
    colunas = [
        # Dados da Loja
        ("loja_nome", "TEXT", None),
        ("loja_numero", "TEXT", None),
        
        # Dados Pessoais
        ("data_nascimento", "DATE", None),
        ("naturalidade", "TEXT", None),
        ("uf_naturalidade", "TEXT", None),
        ("nacionalidade", "TEXT", "'Brasileiro'"),
        ("cpf", "TEXT", None),
        ("rg", "TEXT", None),
        ("orgao_expedidor", "TEXT", None),
        ("telefone_fixo", "TEXT", None),
        ("celular", "TEXT", None),
        ("email", "TEXT", None),
        ("grau_instrucao", "TEXT", None),
        
        # Endereço Residencial
        ("endereco_residencial", "TEXT", None),
        ("numero_residencial", "TEXT", None),
        ("bairro", "TEXT", None),
        ("cidade", "TEXT", None),
        ("uf_residencial", "TEXT", None),
        ("cep", "TEXT", None),
        
        # Saúde
        ("tipo_sanguineo", "TEXT", None),
        
        # Família
        ("nome_pai", "TEXT", None),
        ("nome_mae", "TEXT", None),
        ("estado_civil", "TEXT", None),
        ("data_casamento", "DATE", None),
        ("nome_conjuge", "TEXT", None),
        ("data_nascimento_conjuge", "DATE", None),
        
        # Profissional
        ("profissao", "TEXT", None),
        ("empregador", "TEXT", None),
        ("endereco_profissional", "TEXT", None),
        ("bairro_profissional", "TEXT", None),
        ("cidade_profissional", "TEXT", None),
        ("uf_profissional", "TEXT", None),
        ("cep_profissional", "TEXT", None),
        ("telefone_comercial", "TEXT", None),
        
        # Outros
        ("observacoes", "TEXT", None),
        ("data_atualizacao", "TIMESTAMP", "CURRENT_TIMESTAMP")
    ]
    
    colunas_adicionadas = 0
    for nome, tipo, default in colunas:
        if adicionar_coluna(cursor, "candidatos", nome, tipo, default):
            colunas_adicionadas += 1
    
    print(f"\n✅ {colunas_adicionadas} novas colunas adicionadas à tabela candidatos")

def verificar_tabelas_existentes(cursor):
    """Lista todas as tabelas relacionadas a candidatos"""
    print("\n" + "="*60)
    print("📊 TABELAS DE CANDIDATOS")
    print("="*60)
    
    # Verificar tabela candidatos
    cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'candidatos'
        ORDER BY ordinal_position
    """)
    
    colunas_candidatos = cursor.fetchall()
    
    print(f"\n📋 Tabela candidatos: {len(colunas_candidatos)} colunas")
    for col in colunas_candidatos[:10]:  # Mostra as 10 primeiras
        print(f"  • {col[0]}: {col[1]}")
    if len(colunas_candidatos) > 10:
        print(f"  ... e mais {len(colunas_candidatos) - 10} colunas")
    
    # Verificar tabela filhos
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'filhos_candidato'
        ORDER BY ordinal_position
    """)
    
    colunas_filhos = cursor.fetchall()
    
    print(f"\n👶 Tabela filhos_candidato: {len(colunas_filhos)} colunas")
    for col in colunas_filhos:
        print(f"  • {col[0]}: {col[1]}")
    
    # Contar registros
    cursor.execute("SELECT COUNT(*) as total FROM candidatos")
    total_candidatos = cursor.fetchone()[0]
    print(f"\n📈 Total de candidatos cadastrados: {total_candidatos}")
    
    cursor.execute("SELECT COUNT(*) as total FROM filhos_candidato")
    total_filhos = cursor.fetchone()[0]
    print(f"👶 Total de filhos cadastrados: {total_filhos}")

def inserir_dados_teste(cursor):
    """Insere dados de teste (opcional)"""
    print("\n" + "="*60)
    print("📝 INSERINDO DADOS DE TESTE (OPCIONAL)")
    print("="*60)
    
    resposta = input("Deseja inserir um candidato de teste? (s/n): ").strip().lower()
    
    if resposta == 's':
        # Inserir candidato de teste
        cursor.execute("""
            INSERT INTO candidatos (nome, data_criacao, loja_nome, loja_numero, 
                                   data_nascimento, naturalidade, nacionalidade,
                                   cpf, rg, celular, email, grau_instrucao,
                                   endereco_residencial, cidade, uf_residencial,
                                   nome_pai, nome_mae, estado_civil, profissao)
            VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            "João da Silva Teste",
            "ARLS Estrela do Oriente",
            "123",
            "1980-05-15",
            "São Paulo",
            "Brasileiro",
            "123.456.789-00",
            "12.345.678-9",
            "(11) 99999-1234",
            "joao.teste@email.com",
            "Ensino Superior",
            "Rua das Acácias, 123",
            "São Paulo",
            "SP",
            "Antônio da Silva",
            "Maria da Silva",
            "Casado",
            "Engenheiro"
        ))
        
        candidato_id = cursor.fetchone()[0]
        print(f"✅ Candidato de teste inserido com ID: {candidato_id}")
        
        # Inserir filhos
        filhos = [
            ("Pedro Silva", "2010-03-10"),
            ("Ana Silva", "2012-07-22")
        ]
        
        for filho in filhos:
            cursor.execute("""
                INSERT INTO filhos_candidato (candidato_id, nome, data_nascimento)
                VALUES (%s, %s, %s)
            """, (candidato_id, filho[0], filho[1]))
        
        print(f"✅ {len(filhos)} filhos inseridos para o candidato")
        
        # Inserir cônjuge
        cursor.execute("""
            UPDATE candidatos 
            SET nome_conjuge = %s, data_nascimento_conjuge = %s
            WHERE id = %s
        """, ("Maria Oliveira", "1982-08-20", candidato_id))
        
        print("✅ Cônjuge adicionado")
        
        print(f"\n🎉 Dados de teste inseridos com sucesso!")
        print(f"   ID do candidato: {candidato_id}")
        print(f"   Nome: João da Silva Teste")
        print(f"   Filhos: Pedro Silva e Ana Silva")
        
        return candidato_id
    
    return None

def mostrar_estrutura_completa(cursor):
    """Mostra a estrutura completa das tabelas"""
    print("\n" + "="*60)
    print("📋 ESTRUTURA COMPLETA DA TABELA CANDIDATOS")
    print("="*60)
    
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'candidatos'
        ORDER BY ordinal_position
    """)
    
    colunas = cursor.fetchall()
    
    for col in colunas:
        print(f"  • {col[0]:<25} {col[1]:<15} Nullable: {col[2]:<5} Default: {col[3] or 'None'}")

def main():
    """Função principal"""
    print("\n" + "="*60)
    print("🚀 SCRIPT DE CRIAÇÃO/ATUALIZAÇÃO DE TABELAS DE CANDIDATOS")
    print("="*60)
    
    conn = conectar_banco()
    cursor = conn.cursor()
    
    try:
        # 1. Atualizar tabela candidatos com novas colunas
        atualizar_tabela_candidatos(cursor)
        
        # 2. Criar tabela de filhos
        criar_tabela_filhos(cursor)
        
        # 3. Commit das alterações
        conn.commit()
        
        # 4. Mostrar estrutura das tabelas
        verificar_tabelas_existentes(cursor)
        
        # 5. Opção para inserir dados de teste
        inserir_dados_teste(cursor)
        
        # 6. Commit final
        conn.commit()
        
        print("\n" + "="*60)
        print("✅ TABELAS CRIADAS/ATUALIZADAS COM SUCESSO!")
        print("="*60)
        print("\n📌 Resumo:")
        print("   • Tabela candidatos: colunas expandidas")
        print("   • Tabela filhos_candidato: criada (relacionamento 1:N)")
        print("   • Índices criados para otimização")
        print("\n📌 Próximos passos:")
        print("   1. Acesse: /candidatos")
        print("   2. Clique no botão 'Formulário' para preencher os dados")
        print("   3. Os filhos podem ser adicionados dinamicamente no formulário")
        
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
    
    
    # =============================
# ROTA TEMPORARIA PARA CRIAR AS TABELAS DO FORMULARIO DOS CANDIDATOS
# ============================= 

@app.route("/migrar-candidatos")
def migrar_candidatos():
    """Rota temporária para migrar as tabelas de candidatos"""
    try:
        cursor, conn = get_db()
        
        print("🔄 Iniciando migração das tabelas de candidatos...")
        
        # ==================== ATUALIZAR TABELA CANDIDATOS ====================
        
        # Lista de colunas a adicionar
        colunas = [
            ('loja_nome', 'TEXT'),
            ('loja_numero', 'TEXT'),
            ('data_nascimento', 'DATE'),
            ('naturalidade', 'TEXT'),
            ('uf_naturalidade', 'TEXT'),
            ('nacionalidade', 'TEXT'),
            ('cpf', 'TEXT'),
            ('rg', 'TEXT'),
            ('orgao_expedidor', 'TEXT'),
            ('telefone_fixo', 'TEXT'),
            ('celular', 'TEXT'),
            ('email', 'TEXT'),
            ('grau_instrucao', 'TEXT'),
            ('endereco_residencial', 'TEXT'),
            ('numero_residencial', 'TEXT'),
            ('bairro', 'TEXT'),
            ('cidade', 'TEXT'),
            ('uf_residencial', 'TEXT'),
            ('cep', 'TEXT'),
            ('tipo_sanguineo', 'TEXT'),
            ('nome_pai', 'TEXT'),
            ('nome_mae', 'TEXT'),
            ('estado_civil', 'TEXT'),
            ('data_casamento', 'DATE'),
            ('nome_conjuge', 'TEXT'),
            ('data_nascimento_conjuge', 'DATE'),
            ('profissao', 'TEXT'),
            ('empregador', 'TEXT'),
            ('endereco_profissional', 'TEXT'),
            ('bairro_profissional', 'TEXT'),
            ('cidade_profissional', 'TEXT'),
            ('uf_profissional', 'TEXT'),
            ('cep_profissional', 'TEXT'),
            ('telefone_comercial', 'TEXT'),
            ('observacoes', 'TEXT'),
            ('data_atualizacao', 'TIMESTAMP')
        ]
        
        colunas_adicionadas = 0
        
        for nome_coluna, tipo in colunas:
            try:
                cursor.execute(f"""
                    ALTER TABLE candidatos ADD COLUMN IF NOT EXISTS {nome_coluna} {tipo}
                """)
                colunas_adicionadas += 1
                print(f"  ✅ Coluna {nome_coluna} adicionada")
            except Exception as e:
                print(f"  ⚠️ Coluna {nome_coluna} já existe ou erro: {e}")
        
        # ==================== CRIAR TABELA FILHOS ====================
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS filhos_candidato (
                id SERIAL PRIMARY KEY,
                candidato_id INTEGER NOT NULL REFERENCES candidatos(id) ON DELETE CASCADE,
                nome TEXT NOT NULL,
                data_nascimento DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Tabela filhos_candidato criada/verificada")
        
        # Criar índice
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_filhos_candidato 
            ON filhos_candidato(candidato_id)
        """)
        print("✅ Índice idx_filhos_candidato criado")
        
        # ==================== ATUALIZAR CAMPOS EXISTENTES ====================
        
        # Definir valores padrão para campos existentes
        cursor.execute("""
            UPDATE candidatos 
            SET nacionalidade = COALESCE(nacionalidade, 'Brasileiro'),
                data_atualizacao = CURRENT_TIMESTAMP
            WHERE nacionalidade IS NULL
        """)
        
        conn.commit()
        return_connection(conn)
        
        # Contar registros
        cursor2, conn2 = get_db()
        cursor2.execute("SELECT COUNT(*) as total FROM candidatos")
        total_candidatos = cursor2.fetchone()['total']
        
        cursor2.execute("SELECT COUNT(*) as total FROM filhos_candidato")
        total_filhos = cursor2.fetchone()['total']
        return_connection(conn2)
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Migração Concluída</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    padding: 20px;
                }}
                .container {{
                    max-width: 800px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 20px;
                    padding: 40px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                }}
                h1 {{
                    color: #28a745;
                    text-align: center;
                    margin-bottom: 30px;
                }}
                .success {{
                    color: #28a745;
                }}
                .info {{
                    background: #e8f4fd;
                    padding: 15px;
                    border-radius: 10px;
                    margin: 20px 0;
                }}
                .stats {{
                    background: #f8f9fa;
                    padding: 15px;
                    border-radius: 10px;
                    margin: 20px 0;
                    text-align: center;
                }}
                .stats .number {{
                    font-size: 2rem;
                    font-weight: bold;
                    color: #007bff;
                }}
                .btn {{
                    display: inline-block;
                    padding: 10px 20px;
                    background: #007bff;
                    color: white;
                    text-decoration: none;
                    border-radius: 8px;
                    margin: 5px;
                    transition: all 0.3s;
                }}
                .btn:hover {{
                    background: #0056b3;
                    transform: translateY(-2px);
                }}
                .btn-success {{
                    background: #28a745;
                }}
                .btn-success:hover {{
                    background: #1e7e34;
                }}
                .btn-warning {{
                    background: #ffc107;
                    color: #333;
                }}
                hr {{
                    margin: 30px 0;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>✅ Migração Concluída com Sucesso!</h1>
                
                <div class="info">
                    <strong>📋 Resumo das operações:</strong>
                    <ul>
                        <li>✅ {colunas_adicionadas} novas colunas adicionadas à tabela candidatos</li>
                        <li>✅ Tabela filhos_candidato criada</li>
                        <li>✅ Índices criados para otimização</li>
                        <li>✅ Valores padrão definidos</li>
                    </ul>
                </div>
                
                <div class="stats">
                    <div class="number">{total_candidatos}</div>
                    <div>Candidatos cadastrados</div>
                    <div class="number mt-3">{total_filhos}</div>
                    <div>Filhos cadastrados</div>
                </div>
                
                <div class="info">
                    <strong>📝 Campos adicionados:</strong>
                    <ul style="columns: 2;">
                        <li>Loja Maçônica</li><li>Nº da Loja</li>
                        <li>Data de Nascimento</li><li>Naturalidade</li>
                        <li>CPF</li><li>RG</li>
                        <li>Telefone Fixo</li><li>Celular</li>
                        <li>E-mail</li><li>Grau de Instrução</li>
                        <li>Endereço Residencial</li><li>Bairro/Cidade/UF/CEP</li>
                        <li>Nome do Pai</li><li>Nome da Mãe</li>
                        <li>Estado Civil</li><li>Nome do Cônjuge</li>
                        <li>Profissão</li><li>Empregador</li>
                        <li>Endereço Profissional</li><li>Telefone Comercial</li>
                        <li>Filhos (tabela separada)</li>
                    </ul>
                </div>
                
                <div style="text-align: center; margin-top: 30px;">
                    <a href="/candidatos" class="btn btn-success">Ir para Candidatos</a>
                    <a href="/dashboard" class="btn btn-primary">Voltar ao Dashboard</a>
                </div>
                
                <hr>
                
                <div class="alert alert-warning" style="background: #fff3cd; padding: 15px; border-radius: 10px;">
                    <strong>⚠️ Importante:</strong> Esta é uma rota temporária de migração.
                    Após confirmar que tudo está funcionando, remova a rota <code>/migrar-candidatos</code> do código.
                </div>
            </div>
        </body>
        </html>
        """
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Erro na Migração</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    background: #f8d7da;
                    min-height: 100vh;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    padding: 20px;
                }}
                .container {{
                    max-width: 800px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 20px;
                    padding: 40px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                }}
                h1 {{
                    color: #dc3545;
                    text-align: center;
                }}
                pre {{
                    background: #f4f4f4;
                    padding: 15px;
                    border-radius: 8px;
                    overflow-x: auto;
                    font-size: 12px;
                }}
                .btn {{
                    display: inline-block;
                    padding: 10px 20px;
                    background: #007bff;
                    color: white;
                    text-decoration: none;
                    border-radius: 8px;
                    margin: 5px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>❌ Erro na Migração</h1>
                <pre>{error_details}</pre>
                <div style="text-align: center;">
                    <a href="/dashboard" class="btn">Voltar ao Dashboard</a>
                </div>
            </div>
        </body>
        </html>
        """