import sqlite3

def criar_tabelas():
    conn = sqlite3.connect("banco.db")
    cursor = conn.cursor()
    
    # Tabela de categorias
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categorias_documentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        descricao TEXT,
        icone TEXT DEFAULT 'bi-file-earmark',
        ativo INTEGER DEFAULT 1
    )
    """)
    print("✅ Tabela 'categorias_documentos' criada/verificada")
    
    # Inserir categorias padrão
    cursor.execute("SELECT COUNT(*) as total FROM categorias_documentos")
    if cursor.fetchone()[0] == 0:
        categorias = [
            ("Iniciação", "Documentos relacionados à iniciação", "bi-star"),
            ("Elevação", "Documentos de elevação ao grau de Companheiro", "bi-arrow-up-circle"),
            ("Exaltação", "Documentos de exaltação ao grau de Mestre", "bi-crown"),
            ("Cargos", "Diplomas e nomeações para cargos", "bi-award"),
            ("Diplomas", "Certificados e diplomas", "bi-trophy"),
            ("Declarações", "Declarações e atestados", "bi-card-text"),
            ("Correspondências", "Correspondências oficiais", "bi-envelope"),
            ("Outros", "Documentos diversos", "bi-folder")
        ]
        for cat in categorias:
            cursor.execute("INSERT INTO categorias_documentos (nome, descricao, icone) VALUES (?, ?, ?)", cat)
        print("✅ Categorias padrão inseridas")
    
    # Tabela de documentos
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
        uploaded_by INTEGER,
        FOREIGN KEY (obreiro_id) REFERENCES usuarios (id) ON DELETE CASCADE,
        FOREIGN KEY (uploaded_by) REFERENCES usuarios (id)
    )
    """)
    print("✅ Tabela 'documentos_obreiro' criada/verificada")
    
    conn.commit()
    conn.close()
    print("✅ Migração concluída!")

if __name__ == "__main__":
    criar_tabelas()