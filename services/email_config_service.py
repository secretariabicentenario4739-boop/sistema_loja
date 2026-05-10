import os
from datetime import datetime


def get_email_config(get_db, return_connection):
    """Busca a configuração de e-mail ativa do banco."""
    try:
        cursor, conn = get_db()
        cursor.execute(
            """
            SELECT sender, sender_name, active
            FROM email_config
            WHERE active = 1
            LIMIT 1
            """
        )
        config = cursor.fetchone()
        return_connection(conn)

        if config:
            return {
                "sender": config["sender"],
                "sender_name": config["sender_name"] or "Sistema Maçônico",
            }
        return {"sender": "contato@juramelo.com.br", "sender_name": "Sistema Maçônico"}
    except Exception as e:
        print(f"Erro ao buscar config de e-mail: {e}")
        return {"sender": "contato@juramelo.com.br", "sender_name": "Sistema Maçônico"}


def diagnostico_email_data(get_db, return_connection):
    """Monta payload de diagnóstico de e-mail."""
    resultados = {}
    resultados["resend_api_key"] = "✅ Configurada" if os.environ.get("RESEND_API_KEY") else "❌ NÃO CONFIGURADA"
    resultados["resend_key_length"] = len(os.environ.get("RESEND_API_KEY", ""))

    try:
        cursor, conn = get_db()
        cursor.execute("SELECT * FROM email_config WHERE active = 1")
        config = cursor.fetchone()
        if config:
            resultados["email_config"] = {
                "sender": config.get("sender"),
                "sender_name": config.get("sender_name"),
                "active": config.get("active"),
            }
        else:
            resultados["email_config"] = "❌ Nenhuma configuração ativa no banco"
        return_connection(conn)
    except Exception as e:
        resultados["email_config"] = f"Erro: {str(e)}"

    try:
        cursor, conn = get_db()
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'password_reset_tokens'
            ) AS exists
            """
        )
        tabela_tokens = cursor.fetchone()
        resultados["tabela_password_reset_tokens"] = (
            "✅ Existe" if tabela_tokens and tabela_tokens.get("exists") else "❌ NÃO EXISTE"
        )

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'email_logs'
            ) AS exists
            """
        )
        tabela_logs = cursor.fetchone()
        resultados["tabela_email_logs"] = (
            "✅ Existe" if tabela_logs and tabela_logs.get("exists") else "⚠️ Não existe (opcional)"
        )
        return_connection(conn)
    except Exception as e:
        resultados["tabelas"] = f"Erro: {str(e)}"

    try:
        resultados["resend_config"] = "✅ Configuração OK" if os.environ.get("RESEND_API_KEY") else "❌ Erro: API key ausente"
    except Exception as e:
        resultados["resend_config"] = f"❌ Erro: {str(e)}"

    return resultados


def salvar_config_email(get_db, return_connection, form, email_from_default):
    """Salva configuração de e-mail e retorna mensagem flash."""
    sender = form.get("sender", email_from_default)
    if not sender:
        return {"ok": False, "message": "Preencha o e-mail remetente", "category": "danger"}

    cursor, conn = get_db()
    try:
        server = form.get("server", "")
        port = form.get("port", "")
        use_tls = 1 if form.get("use_tls") else 0
        username = form.get("username", "")
        password = form.get("password", "")
        sender_name = form.get("sender_name", "Sistema Maçônico")
        active = 1 if form.get("active") else 0

        if active:
            cursor.execute("UPDATE email_settings SET active = 0")

        cursor.execute(
            """
            INSERT INTO email_settings
            (server, port, use_tls, username, password, sender, sender_name, active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (server, port, use_tls, username, password, sender, sender_name, active),
        )
        conn.commit()
        return {"ok": True, "message": "Configuração de e-mail salva com sucesso! (Usando Resend)", "category": "success"}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "message": f"Erro ao salvar configuração: {str(e)}", "category": "danger"}
    finally:
        return_connection(conn)


def carregar_config_email_ativa(get_db, return_connection):
    cursor, conn = get_db()
    try:
        cursor.execute("SELECT * FROM email_settings WHERE active = 1 ORDER BY id DESC LIMIT 1")
        return cursor.fetchone()
    finally:
        return_connection(conn)


def montar_email_teste_html(render_template_fn):
    dados_template = {
        "nome": "Irmão",
        "remetente": "contato@juramelo.com.br",
        "nome_remetente": "ARLS Bicentenário",
        "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "ano": datetime.now().year,
    }
    try:
        return render_template_fn("email/teste.html", **dados_template)
    except Exception:
        return f"""
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"></head>
        <body>
            <h2>✅ Teste de E-mail</h2>
            <p>Olá, esta é uma mensagem de teste do Sistema Maçônico.</p>
            <p>Data: {dados_template['data_hora']}</p>
        </body>
        </html>
        """
