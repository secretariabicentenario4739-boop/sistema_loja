import sqlite3
import json
from datetime import datetime

def atualizar_banco_atas():
    conn = sqlite3.connect("banco.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("🔄 Atualizando banco de dados para módulo de atas...")
    
    # Verificar colunas existentes na tabela atas
    cursor.execute("PRAGMA table_info(atas)")
    colunas = [col[1] for col in cursor.fetchall()]
    
    novas_colunas = [
        ("numero_ata", "INTEGER"),
        ("ano_ata", "INTEGER"),
        ("tipo_ata", "TEXT DEFAULT 'Ordinária'"),
        ("redator_nome", "TEXT"),
        ("secretario_id", "INTEGER"),
        ("aprovada_em", "DATE"),
        ("aprovada_por", "INTEGER"),
        ("observacoes_aprovacao", "TEXT"),
        ("modelo_ata", "TEXT"),
        ("assinaturas", "TEXT"),
        ("hash_documento", "TEXT"),
        ("data_impressao", "TIMESTAMP"),
        ("impresso_por", "INTEGER")
    ]
    
    for coluna, tipo in novas_colunas:
        if coluna not in colunas:
            cursor.execute(f"ALTER TABLE atas ADD COLUMN {coluna} {tipo}")
            print(f"✅ Coluna '{coluna}' adicionada à tabela atas")
    
    # Criar tabela de modelos de ata
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS modelos_ata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        descricao TEXT,
        tipo TEXT DEFAULT 'Ordinária',
        estrutura TEXT NOT NULL,
        campos_personalizados TEXT,
        ativo INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by INTEGER,
        FOREIGN KEY (created_by) REFERENCES usuarios (id)
    )
    """)
    print("✅ Tabela 'modelos_ata' criada/verificada")
    
    # Inserir modelos padrão (usando aspas simples para evitar problemas com #)
    cursor.execute("SELECT COUNT(*) as total FROM modelos_ata")
    if cursor.fetchone()["total"] == 0:
        modelos = [
            ("Ata Ordinária", "Modelo padrão para reuniões ordinárias", "Ordinária", 
             json.dumps({
                 "cabecalho": ["Abertura", "Verificacao de quorum", "Leitura da ata anterior"],
                 "expediente": ["Comunicacoes", "Propostas", "Informacoes"],
                 "ordem_do_dia": ["Assuntos em pauta", "Votacoes", "Deliberacoes"],
                 "encerramento": ["Palavra final", "Marcacao proxima reuniao", "Encerramento"]
             })),
            ("Ata Magna", "Modelo para reunioes magnas", "Magna",
             json.dumps({
                 "cabecalho": ["Abertura solene", "Composicao da mesa", "Hino"],
                 "expediente": ["Comunicacoes oficiais", "Propostas especiais"],
                 "ordem_do_dia": ["Rituais", "Iniciacoes", "Elevacoes", "Exaltacoes"],
                 "encerramento": ["Palavra do Veneravel", "Encerramento solene"]
             })),
            ("Ata Administrativa", "Modelo para reunioes administrativas", "Administrativa",
             json.dumps({
                 "cabecalho": ["Abertura", "Presentes", "Pauta"],
                 "expediente": ["Prestacao de contas", "Assuntos financeiros"],
                 "ordem_do_dia": ["Deliberacoes", "Votacoes", "Planejamento"],
                 "encerramento": ["Encaminhamentos", "Proxima reuniao", "Encerramento"]
             }))
        ]
        
        for modelo in modelos:
            cursor.execute("""
                INSERT INTO modelos_ata (nome, descricao, tipo, estrutura)
                VALUES (?, ?, ?, ?)
            """, modelo)
        print("✅ Modelos de ata padrão inseridos")
    
    # Criar tabela de assinaturas_ata
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS assinaturas_ata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ata_id INTEGER NOT NULL,
        obreiro_id INTEGER NOT NULL,
        cargo_id INTEGER,
        assinatura TEXT,
        data_assinatura TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ip_assinatura TEXT,
        hash_assinatura TEXT,
        validada INTEGER DEFAULT 0,
        FOREIGN KEY (ata_id) REFERENCES atas (id) ON DELETE CASCADE,
        FOREIGN KEY (obreiro_id) REFERENCES usuarios (id),
        FOREIGN KEY (cargo_id) REFERENCES cargos (id),
        UNIQUE(ata_id, obreiro_id)
    )
    """)
    print("✅ Tabela 'assinaturas_ata' criada/verificada")
    
    # Criar tabela de anexos_ata
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS anexos_ata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ata_id INTEGER NOT NULL,
        nome_arquivo TEXT NOT NULL,
        caminho_arquivo TEXT NOT NULL,
        tipo_arquivo TEXT,
        tamanho INTEGER,
        data_upload TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        uploaded_by INTEGER,
        FOREIGN KEY (ata_id) REFERENCES atas (id) ON DELETE CASCADE,
        FOREIGN KEY (uploaded_by) REFERENCES usuarios (id)
    )
    """)
    print("✅ Tabela 'anexos_ata' criada/verificada")
    
    conn.commit()
    conn.close()
    print("✅ Banco de dados atualizado com sucesso!")

def verificar_estrutura():
    conn = sqlite3.connect("banco.db")
    cursor = conn.cursor()
    
    print("\n🔍 VERIFICANDO ESTRUTURA DO BANCO")
    print("=" * 50)
    
    tabelas = ["atas", "modelos_ata", "assinaturas_ata", "anexos_ata"]
    
    for tabela in tabelas:
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tabela}'")
        if cursor.fetchone():
            print(f"✅ Tabela '{tabela}' existe")
            
            cursor.execute(f"PRAGMA table_info({tabela})")
            colunas = cursor.fetchall()
            print(f"   Colunas: {', '.join([col[1] for col in colunas])}")
        else:
            print(f"❌ Tabela '{tabela}' NÃO existe")
    
    conn.close()

if __name__ == "__main__":
    print("🚀 MIGRAÇÃO PARA MÓDULO DE ATAS")
    print("=" * 50)
    
    verificar_estrutura()
    
    resposta = input("\nDeseja criar as novas tabelas e colunas? (s/n): ")
    if resposta.lower() == 's':
        atualizar_banco_atas()
    
    resposta = input("\nDeseja verificar a estrutura após a migração? (s/n): ")
    if resposta.lower() == 's':
        verificar_estrutura()