@app.route("/debug")
def debug_info():
    """Rota para diagnosticar problemas"""
    import traceback
    import os
    
    try:
        from db_config import get_db, return_connection
        
        # Testar conexão com banco
        cursor, conn = get_db()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        
        # Testar tabelas
        cursor.execute("SELECT COUNT(*) as total FROM usuarios")
        total_usuarios = cursor.fetchone()['total']
        
        return_connection(conn)
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Debug - Sistema Maçônico</title>
            <style>
                body {{ font-family: monospace; padding: 20px; background: #f5f5f5; }}
                .info {{ background: white; padding: 15px; border-radius: 8px; margin: 10px 0; }}
                .success {{ color: green; }}
                .error {{ color: red; }}
                pre {{ background: #eee; padding: 10px; overflow-x: auto; }}
            </style>
        </head>
        <body>
            <h1>🔍 Debug Info</h1>
            
            <div class="info">
                <h2>✅ Status Geral</h2>
                <p><strong>Status:</strong> <span class="success">OK - Sistema funcionando</span></p>
                <p><strong>Database:</strong> Conectado</p>
                <p><strong>PostgreSQL Version:</strong> {version['version'][:80]}</p>
                <p><strong>Total Usuários:</strong> {total_usuarios}</p>
            </div>
            
            <div class="info">
                <h2>📋 Variáveis de Ambiente</h2>
                <p><strong>FLASK_ENV:</strong> {os.getenv('FLASK_ENV', 'development')}</p>
                <p><strong>DATABASE_URL:</strong> {os.getenv('DATABASE_URL', 'NÃO DEFINIDA')[:60] if os.getenv('DATABASE_URL') else 'NÃO DEFINIDA'}...</p>
                <p><strong>SECRET_KEY:</strong> {'DEFINIDA' if os.getenv('SECRET_KEY') else 'NÃO DEFINIDA'}</p>
            </div>
            
            <div class="info">
                <h2>🔌 Rotas Disponíveis</h2>
                <ul>
                    <li><a href="/">/</a> - Login</li>
                    <li><a href="/debug">/debug</a> - Esta página</li>
                    <li><a href="/dashboard">/dashboard</a> - Dashboard</li>
                    <li><a href="/logout">/logout</a> - Logout</li>
                </ul>
            </div>
            
            <div class="info">
                <h2>👤 Teste de Login</h2>
                <form action="/" method="POST">
                    <input type="text" name="usuario" placeholder="Usuário" required><br><br>
                    <input type="password" name="senha" placeholder="Senha" required><br><br>
                    <button type="submit">Entrar</button>
                </form>
            </div>
            
            <hr>
            <a href="/">Voltar ao login</a>
        </body>
        </html>
        """
    except Exception as e:
        return f"""
        <!DOCTYPE html>
        <html>
        <head><title>Erro - Debug</title></head>
        <body>
            <h1>❌ Erro no Debug</h1>
            <pre>{traceback.format_exc()}</pre>
            <a href="/">Voltar</a>
        </body>
        </html>
        """
        
        

@app.route("/create-admin-user")
def create_admin_user():
    """Rota temporária para criar usuário admin"""
    try:
        from werkzeug.security import generate_password_hash
        cursor, conn = get_db()
        
        senha_hash = generate_password_hash("admin123")
        cursor.execute("""
            INSERT INTO usuarios (usuario, senha_hash, tipo, data_cadastro, ativo, nome_completo)
            VALUES ('admin', %s, 'admin', NOW(), 1, 'Administrador')
            ON CONFLICT (usuario) DO NOTHING
        """, (senha_hash,))
        conn.commit()
        return_connection(conn)
        
        return """
        <html>
        <head><title>Admin Criado</title></head>
        <body style="font-family: Arial; text-align: center; padding: 50px;">
            <h1 style="color: green;">✅ Usuário Admin criado com sucesso!</h1>
            <p><strong>Usuário:</strong> admin</p>
            <p><strong>Senha:</strong> admin123</p>
            <a href="/" style="background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Fazer Login</a>
        </body>
        </html>
        """
        
    except Exception as e:
        return f"❌ Erro: {e}"
