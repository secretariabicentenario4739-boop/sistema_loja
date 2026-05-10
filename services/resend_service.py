import os

import requests
from flask import render_template


def enviar_email_resend(destinatario, assunto, conteudo_html, conteudo_texto=None):
    """Envia e-mail usando a API do Resend."""
    api_key = os.environ.get("RESEND_API_KEY", "")
    if not api_key:
        print("❌ RESEND_API_KEY não configurada")
        return {"success": False, "message": "API key não configurada"}

    if not conteudo_html:
        conteudo_html = "<p>Conteúdo do e-mail não disponível.</p>"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    from_email = "ARLS Bicentenário <contato@juramelo.com.br>"
    data = {
        "from": from_email,
        "to": [destinatario],
        "subject": assunto,
        "html": conteudo_html,
    }
    if conteudo_texto:
        data["text"] = conteudo_texto

    print(f"📧 Enviando e-mail para: {destinatario}")
    print(f"📧 Assunto: {assunto}")
    print(f"📧 From: {from_email}")

    try:
        response = requests.post("https://api.resend.com/emails", headers=headers, json=data)
        print(f"📧 Resposta Status: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            return {"success": True, "message": "E-mail enviado com sucesso", "id": result.get("id")}
        return {"success": False, "message": f"Erro {response.status_code}: {response.text}"}
    except Exception as e:
        print(f"❌ Exceção: {e}")
        return {"success": False, "message": str(e)}


def gerar_html_fallback(nome_destinatario, dados_reuniao):
    """Fallback em caso de erro no template."""
    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body>
        <h2>Olá {nome_destinatario},</h2>
        <p>Você foi convidado para uma reunião:</p>
        <p><strong>{dados_reuniao.get('titulo')}</strong></p>
        <p>📅 Data: {dados_reuniao.get('data')}</p>
        <p>⏰ Horário: {dados_reuniao.get('horario_formatado')}</p>
        <p>📍 Local: {dados_reuniao.get('local')}</p>
        <p>🔗 Link: <a href="{dados_reuniao.get('link_reuniao')}">Ver detalhes</a></p>
        <p>Atenciosamente,<br>Secretaria do Sistema Maçônico</p>
    </body>
    </html>
    """


def enviar_email_reuniao(destinatario, nome_destinatario, dados_reuniao):
    """Envia e-mail de convocação para reunião via Resend usando templates."""
    reuniao_id = dados_reuniao.get("id", "")
    assunto = f"📅 Convite: {dados_reuniao.get('titulo', 'Nova Reunião')} - ARLS Bicentenário"

    hora_termino = dados_reuniao.get("hora_termino")
    horario = dados_reuniao.get("hora_inicio")
    if hora_termino:
        horario = f"{dados_reuniao.get('hora_inicio')} às {hora_termino}"

    dados_reuniao["horario_formatado"] = horario
    dados_reuniao["link_reuniao"] = (
        f"https://www.juramelo.com.br/reunioes/{reuniao_id}" if reuniao_id else "#"
    )

    try:
        html_content = render_template(
            "email/reuniao_agendada.html", nome=nome_destinatario, reuniao=dados_reuniao
        )
    except Exception as e:
        print(f"Erro ao carregar template HTML: {e}")
        html_content = gerar_html_fallback(nome_destinatario, dados_reuniao)

    return enviar_email_resend(destinatario, assunto, html_content)
