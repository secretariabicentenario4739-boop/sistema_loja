# scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, date
import os

def verificar_e_disparar_lembretes():
    """Função principal que verifica reuniões e aniversários"""
    from app import app, get_db, enviar_email_reuniao, enviar_email_aniversario
    
    with app.app_context():
        cursor, conn = get_db()
        
        hoje = date.today()
        agora = datetime.now()
        
        print(f"🔍 Executando verificação em {agora}")
        
        # 1. Verificar reuniões de hoje
        cursor.execute("""
            SELECT id, titulo, data, hora_inicio, local, tipo, grau
            FROM reunioes 
            WHERE data = %s 
              AND status = 'agendada'
              AND hora_inicio > %s
        """, (hoje, agora.time()))
        
        reunioes_hoje = cursor.fetchall()
        
        for reuniao in reunioes_hoje:
            # Buscar participantes
            cursor.execute("""
                SELECT id, nome_completo, email
                FROM usuarios
                WHERE ativo = 1 
                  AND email IS NOT NULL 
                  AND email != ''
                  AND grau_atual >= %s
            """, (reuniao['grau'] or 1,))
            
            participantes = cursor.fetchall()
            
            for participante in participantes:
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
                print(f"📧 Lembrete enviado para {participante['email']}")
        
        # 2. Verificar aniversários de obreiros
        cursor.execute("""
            SELECT id, nome_completo, email, data_nascimento
            FROM usuarios
            WHERE ativo = 1
              AND email IS NOT NULL
              AND email != ''
              AND EXTRACT(MONTH FROM data_nascimento) = %s
              AND EXTRACT(DAY FROM data_nascimento) = %s
        """, (hoje.month, hoje.day))
        
        aniversariantes = cursor.fetchall()
        
        for obreiro in aniversariantes:
            # Enviar e-mail de aniversário
            enviar_email_aniversario(
                destinatario=obreiro['email'],
                nome_destinatario=obreiro['nome_completo']
            )
            print(f"🎂 E-mail de aniversário enviado para {obreiro['email']}")
        
        # 3. Verificar aniversários de familiares
        cursor.execute("""
            SELECT f.nome, f.parentesco, f.data_nascimento, u.nome_completo as obreiro_nome, u.email
            FROM familiares f
            JOIN usuarios u ON f.obreiro_id = u.id
            WHERE u.ativo = 1
              AND u.email IS NOT NULL
              AND u.email != ''
              AND EXTRACT(MONTH FROM f.data_nascimento) = %s
              AND EXTRACT(DAY FROM f.data_nascimento) = %s
        """, (hoje.month, hoje.day))
        
        aniversariantes_familia = cursor.fetchall()
        
        for familiar in aniversariantes_familia:
            enviar_email_aniversario_familiar(
                destinatario=familiar['email'],
                nome_familiar=familiar['nome'],
                parentesco=familiar['parentesco'],
                nome_obreiro=familiar['obreiro_nome']
            )
            print(f"🎂 E-mail de aniversário do familiar enviado para {familiar['email']}")
        
        conn.commit()
        return_connection(conn)
        
        print(f"✅ Verificação concluída em {datetime.now()}")