import sqlite3

def criar_tabelas():
    conn = sqlite3.connect("banco.db")
    cursor = conn.cursor()
    
    # Tabela de comunicados
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comunicados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            titulo TEXT NOT NULL,
            conteudo TEXT NOT NULL,
            tipo TEXT DEFAULT 'informativo',
            prioridade TEXT DEFAULT 'normal',
            data_inicio DATE NOT NULL,
            data_fim DATE,
            ativo INTEGER DEFAULT 1,
            criado_por INTEGER NOT NULL,
            data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (criado_por) REFERENCES usuarios (id)
        )
    """)
    print("✅ Tabela 'comunicados' verificada/criada")
    
    # Tabela de visualizações de comunicados
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS visualizacoes_comunicado (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comunicado_id INTEGER NOT NULL,
            obreiro_id INTEGER NOT NULL,
            data_visualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (comunicado_id) REFERENCES comunicados (id) ON DELETE CASCADE,
            FOREIGN KEY (obreiro_id) REFERENCES usuarios (id) ON DELETE CASCADE,
            UNIQUE(comunicado_id, obreiro_id)
        )
    """)
    print("✅ Tabela 'visualizacoes_comunicado' verificada/criada")
    
    # Tabela de configurações de e-mail (opcional, para futuro)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server TEXT NOT NULL,
            port INTEGER NOT NULL,
            use_tls INTEGER DEFAULT 1,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            sender TEXT NOT NULL,
            sender_name TEXT,
            active INTEGER DEFAULT 1
        )
    """)
    print("✅ Tabela 'email_settings' verificada/criada")
    
    # Tabela de log de notificações (opcional)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS notificacoes_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            destinatario TEXT NOT NULL,
            assunto TEXT NOT NULL,
            corpo TEXT NOT NULL,
            tipo TEXT,
            status TEXT,
            data_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            erro TEXT
        )
    """)
    print("✅ Tabela 'notificacoes_log' verificada/criada")
    
    # Tabela de preferências de notificação
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS preferencias_notificacao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            obreiro_id INTEGER NOT NULL,
            lembrete_reuniao INTEGER DEFAULT 1,
            alerta_ausencia INTEGER DEFAULT 1,
            email TEXT,
            FOREIGN KEY (obreiro_id) REFERENCES usuarios (id)
        )
    """)
    print("✅ Tabela 'preferencias_notificacao' verificada/criada")
    
    conn.commit()
    conn.close()
    print("\n✅ Todas as tabelas foram criadas/verificadas com sucesso!")

if __name__ == "__main__":
    criar_tabelas()