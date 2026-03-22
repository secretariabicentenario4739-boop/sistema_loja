import sqlite3

conn = sqlite3.connect("banco.db")
cursor = conn.cursor()

# Cria as tabelas faltantes
cursor.execute("""
CREATE TABLE IF NOT EXISTS categorias_documentos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL UNIQUE,
    descricao TEXT,
    icone TEXT DEFAULT 'fa-file',
    cor TEXT DEFAULT '#6c757d',
    ativo INTEGER DEFAULT 1,
    ordem INTEGER DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS documentos_obreiro (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    obreiro_id INTEGER NOT NULL,
    titulo TEXT NOT NULL,
    descricao TEXT,
    categoria TEXT DEFAULT 'outros',
    tipo_arquivo TEXT NOT NULL,
    nome_arquivo TEXT NOT NULL,
    caminho_arquivo TEXT NOT NULL,
    tamanho INTEGER,
    data_upload TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    uploaded_by INTEGER NOT NULL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS categorias_sugestoes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL UNIQUE,
    descricao TEXT,
    icone TEXT DEFAULT 'fa-lightbulb',
    cor TEXT DEFAULT '#ffc107',
    ativo INTEGER DEFAULT 1
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS sugestoes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    titulo TEXT NOT NULL,
    descricao TEXT NOT NULL,
    categoria TEXT NOT NULL,
    prioridade TEXT DEFAULT 'media',
    status TEXT DEFAULT 'pendente',
    votos INTEGER DEFAULT 0,
    autor_id INTEGER NOT NULL,
    implementada INTEGER DEFAULT 0,
    implementado_por INTEGER,
    data_implementacao TIMESTAMP,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS comentarios_sugestao (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sugestao_id INTEGER NOT NULL,
    autor_id INTEGER NOT NULL,
    comentario TEXT NOT NULL,
    data_comentario TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS whatsapp_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    notificar_ausencia INTEGER DEFAULT 1,
    notificar_nova_reuniao INTEGER DEFAULT 1,
    notificar_comunicado INTEGER DEFAULT 1,
    lembrete_reuniao INTEGER DEFAULT 1,
    grupo_id TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# Inserir categorias padrão se não existirem
cursor.execute("SELECT COUNT(*) as count FROM categorias_documentos")
if cursor.fetchone()[0] == 0:
    categorias = [
        ("Identificação", "Documentos de identificação pessoal", "fa-id-card", "#3498db", 1),
        ("Comprovantes", "Comprovantes de residência, etc", "fa-file-alt", "#2ecc71", 2),
        ("Certificados", "Certificados de cursos e graus", "fa-certificate", "#f39c12", 3),
        ("Maçônicos", "Documentos da loja e maçonaria", "fa-masonic", "#9b59b6", 4),
        ("Financeiros", "Documentos financeiros e comprovantes", "fa-money-bill-wave", "#e74c3c", 5),
        ("Outros", "Outros documentos", "fa-folder", "#95a5a6", 6)
    ]
    for cat in categorias:
        cursor.execute("""
            INSERT INTO categorias_documentos (nome, descricao, icone, cor, ordem)
            VALUES (?, ?, ?, ?, ?)
        """, cat)

cursor.execute("SELECT COUNT(*) as count FROM categorias_sugestoes")
if cursor.fetchone()[0] == 0:
    categorias_sug = [
        ("Funcionalidade", "Sugestões de novas funcionalidades", "fa-plus-circle", "#3498db"),
        ("Melhoria", "Melhorias em funcionalidades existentes", "fa-chart-line", "#2ecc71"),
        ("Interface", "Melhorias na interface do usuário", "fa-paint-brush", "#f39c12"),
        ("Relatório", "Novos relatórios e exportações", "fa-chart-bar", "#9b59b6"),
        ("Segurança", "Sugestões de segurança", "fa-shield-alt", "#e74c3c"),
        ("Outros", "Outras sugestões", "fa-comment", "#95a5a6")
    ]
    for cat in categorias_sug:
        cursor.execute("""
            INSERT INTO categorias_sugestoes (nome, descricao, icone, cor)
            VALUES (?, ?, ?, ?)
        """, cat)

cursor.execute("SELECT COUNT(*) as count FROM whatsapp_config")
if cursor.fetchone()[0] == 0:
    cursor.execute("""
        INSERT INTO whatsapp_config (notificar_ausencia, notificar_nova_reuniao, notificar_comunicado, lembrete_reuniao)
        VALUES (1, 1, 1, 1)
    """)

conn.commit()
conn.close()

print("✅ Tabelas criadas com sucesso!")