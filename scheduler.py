# scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, date
import os

def verificar_e_disparar_lembretes():
    """Função principal que verifica reuniões e aniversários"""
    from app import app, get_db, return_connection
    from datetime import datetime, date
    
    print("=" * 60)
    print(f"🕐 INICIANDO VERIFICAÇÃO DE LEMBRETES - {datetime.now()}")
    print("=" * 60)
    
    with app.app_context():
        cursor, conn = get_db()
        
        hoje = date.today()
        agora = datetime.now()
        
        print(f"📅 Data atual: {hoje}")
        print(f"⏰ Hora atual: {agora.time()}")
        
        # ==========================================
        # 1. Verificar reuniões de hoje
        # ==========================================
        print("\n📋 VERIFICANDO REUNIÕES DE HOJE...")
        
        cursor.execute("""
            SELECT id, titulo, data, hora_inicio, local, tipo, grau
            FROM reunioes 
            WHERE data = %s 
              AND status = 'agendada'
        """, (hoje,))
        
        reunioes_hoje = cursor.fetchall()
        print(f"📊 Encontradas {len(reunioes_hoje)} reuniões agendadas para hoje")
        
        for reuniao in reunioes_hoje:
            print(f"\n📌 Reunião: {reuniao['titulo']} às {reuniao['hora_inicio']}")
            
            # Buscar participantes
            cursor.execute("""
                SELECT id, nome_completo, email
                FROM usuarios
                WHERE ativo = 1 
                  AND email IS NOT NULL 
                  AND email != ''
            """)
            
            participantes = cursor.fetchall()
            print(f"   👥 Total de participantes elegíveis: {len(participantes)}")
            
            for participante in participantes:
                print(f"   📧 Enviando e-mail para: {participante['email']}")
                try:
                    enviar_email_reuniao(
                        destinatario=participante['email'],
                        nome_destinatario=participante['nome_completo'],
                        dados_reuniao={
                            'titulo': reuniao['titulo'],
                            'data': reuniao['data'].strftime('%d/%m/%Y'),
                            'hora_inicio': reuniao['hora_inicio'].strftime('%H:%M'),
                            'local': reuniao['local'] or 'Templo Maçônico',
                            'tipo': reuniao['tipo']
                        }
                    )
                    print(f"   ✅ E-mail enviado com sucesso para {participante['email']}")
                except Exception as e:
                    print(f"   ❌ Erro ao enviar para {participante['email']}: {e}")
        
        # ==========================================
        # 2. Verificar aniversários de obreiros
        # ==========================================
        print("\n🎂 VERIFICANDO ANIVERSÁRIOS DE OBREIROS...")
        
        cursor.execute("""
            SELECT id, nome_completo, email, data_nascimento
            FROM usuarios
            WHERE ativo = 1
              AND email IS NOT NULL
              AND email != ''
              AND data_nascimento IS NOT NULL
              AND EXTRACT(MONTH FROM data_nascimento) = %s
              AND EXTRACT(DAY FROM data_nascimento) = %s
        """, (hoje.month, hoje.day))
        
        aniversariantes = cursor.fetchall()
        print(f"📊 Obreiros aniversariantes hoje: {len(aniversariantes)}")
        
        for obreiro in aniversariantes:
            print(f"   🎂 {obreiro['nome_completo']} - {obreiro['email']}")
            try:
                enviar_email_aniversario(
                    destinatario=obreiro['email'],
                    nome_destinatario=obreiro['nome_completo']
                )
                print(f"   ✅ E-mail de aniversário enviado")
            except Exception as e:
                print(f"   ❌ Erro: {e}")
        
        # ==========================================
        # 3. Verificar aniversários de familiares
        # ==========================================
        print("\n🎂 VERIFICANDO ANIVERSÁRIOS DE FAMILIARES...")
        
        cursor.execute("""
            SELECT f.nome, f.parentesco, f.data_nascimento, u.nome_completo as obreiro_nome, u.email
            FROM familiares f
            JOIN usuarios u ON f.obreiro_id = u.id
            WHERE u.ativo = 1
              AND u.email IS NOT NULL
              AND u.email != ''
              AND f.data_nascimento IS NOT NULL
              AND EXTRACT(MONTH FROM f.data_nascimento) = %s
              AND EXTRACT(DAY FROM f.data_nascimento) = %s
        """, (hoje.month, hoje.day))
        
        aniversariantes_familia = cursor.fetchall()
        print(f"📊 Familiares aniversariantes hoje: {len(aniversariantes_familia)}")
        
        for familiar in aniversariantes_familia:
            print(f"   🎂 {familiar['nome']} ({familiar['parentesco']}) - {familiar['email']}")
            try:
                enviar_email_aniversario_familiar(
                    destinatario=familiar['email'],
                    nome_familiar=familiar['nome'],
                    parentesco=familiar['parentesco'],
                    nome_obreiro=familiar['obreiro_nome']
                )
                print(f"   ✅ E-mail enviado")
            except Exception as e:
                print(f"   ❌ Erro: {e}")
        
        return_connection(conn)
        
        print("\n" + "=" * 60)
        print(f"✅ VERIFICAÇÃO CONCLUÍDA EM {datetime.now()}")
        print("=" * 60)


def enviar_email_reuniao(destinatario, nome_destinatario, dados_reuniao):
    """Envia e-mail de lembrete de reunião"""
    import resend
    
    resend.api_key = os.environ.get("RESEND_API_KEY")
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px; background: #f5f0e8;">
            <div style="text-align: center; padding: 20px; background: #4A0E2E; color: white;">
                <h2>📢 Lembrete de Reunião</h2>
            </div>
            <div style="padding: 20px; background: white;">
                <p>Prezado(a) <strong>{nome_destinatario}</strong>,</p>
                <p>Lembramos que hoje às <strong>{dados_reuniao['hora_inicio']}h</strong> teremos a reunião:</p>
                <div style="background: #f5f0e8; padding: 15px; margin: 15px 0;">
                    <p><strong>📌 {dados_reuniao['titulo']}</strong></p>
                    <p>📍 Local: {dados_reuniao['local']}</p>
                    <p>📅 Data: {dados_reuniao['data']}</p>
                    <p>⏰ Horário: {dados_reuniao['hora_inicio']}h</p>
                </div>
                <p>Contamos com sua presença!</p>
                <hr style="margin: 20px 0;">
                <p style="text-align: center; font-size: 12px; color: #666;">
                    ARLS Bicentenário Nº 4739 - Oriente de Ceilândia - DF
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    try:
        resend.Emails.send(
            from_="Loja Bicentenário <onboarding@resend.dev>",
            to=[destinatario],
            subject=f"📢 Lembrete: {dados_reuniao['titulo']} - Loja Bicentenário",
            html=html_content
        )
        return {'success': True}
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")
        return {'success': False, 'error': str(e)}


def enviar_email_aniversario(destinatario, nome_destinatario):
    """Envia e-mail de aniversário para o obreiro"""
    import resend
    
    resend.api_key = os.environ.get("RESEND_API_KEY")
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px; background: #f5f0e8;">
            <div style="text-align: center; padding: 20px; background: #4A0E2E; color: white;">
                <h2>🎂 Feliz Aniversário!</h2>
            </div>
            <div style="padding: 20px; background: white;">
                <p>Prezado(a) <strong>{nome_destinatario}</strong>,</p>
                <p>A Loja Bicentenário: Ceilândia deseja a você um feliz aniversário!</p>
                <p>Que este dia seja repleto de alegria, saúde e prosperidade.</p>
                <hr style="margin: 20px 0;">
                <p style="text-align: center; font-size: 12px; color: #666;">
                    ARLS Bicentenário Nº 4739 - Oriente de Ceilândia - DF
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    try:
        resend.Emails.send(
            from_="Loja Bicentenário <onboarding@resend.dev>",
            to=[destinatario],
            subject=f"🎂 Feliz Aniversário - Loja Bicentenário",
            html=html_content
        )
        return {'success': True}
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")
        return {'success': False, 'error': str(e)}


def enviar_email_aniversario_familiar(destinatario, nome_familiar, parentesco, nome_obreiro):
    """Envia e-mail de aniversário para o familiar"""
    import resend
    
    resend.api_key = os.environ.get("RESEND_API_KEY")
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px; background: #f5f0e8;">
            <div style="text-align: center; padding: 20px; background: #4A0E2E; color: white;">
                <h2>🎂 Feliz Aniversário, {nome_familiar}!</h2>
            </div>
            <div style="padding: 20px; background: white;">
                <p>Prezado(a) <strong>{nome_obreiro}</strong>,</p>
                <p>Parabéns pelo aniversário do(a) seu(sua) <strong>{parentesco}</strong> <strong>{nome_familiar}</strong>!</p>
                <p>A Loja Bicentenário: Ceilândia se junta a você para celebrar esta data especial.</p>
                <p>Que venham muitos e muitos anos pela frente!</p>
                <hr style="margin: 20px 0;">
                <p style="text-align: center; font-size: 12px; color: #666;">
                    ARLS Bicentenário Nº 4739 - Oriente de Ceilândia - DF
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    try:
        resend.Emails.send(
            from_="Loja Bicentenário <onboarding@resend.dev>",
            to=[destinatario],
            subject=f"🎂 Aniversário - {nome_familiar}",
            html=html_content
        )
        return {'success': True}
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")
        return {'success': False, 'error': str(e)}