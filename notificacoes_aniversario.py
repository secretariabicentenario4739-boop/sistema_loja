#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para verificar aniversariantes e enviar notificações por e-mail
Execute: python notificacoes_aniversario.py --agora
Para executar em segundo plano: python notificacoes_aniversario.py --daemon
"""

import time
from datetime import datetime
import os
import sys
from dotenv import load_dotenv

# Adicionar o diretório atual ao path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Carregar variáveis de ambiente
load_dotenv()

# Importar funções do banco
from db_config import get_db, return_connection
from email_service import enviar_notificacao_aniversario, email_service

def verificar_aniversariantes():
    """Verifica aniversariantes do dia e envia notificações por e-mail"""
    try:
        cursor, conn = get_db()
        
        hoje = datetime.now()
        mes = hoje.month
        dia = hoje.day
        
        print(f"\n{'='*60}")
        print(f"🔍 VERIFICANDO ANIVERSARIANTES - {hoje.strftime('%d/%m/%Y')}")
        print(f"{'='*60}")
        
        # Buscar aniversariantes do dia
        cursor.execute("""
            SELECT f.*, 
                   u.nome_completo as obreiro_nome, 
                   u.id as obreiro_id, 
                   u.telefone as obreiro_telefone,
                   u.email as obreiro_email
            FROM familiares f
            JOIN usuarios u ON f.obreiro_id = u.id
            WHERE f.receber_notificacoes = 1
              AND EXTRACT(MONTH FROM f.data_nascimento) = %s
              AND EXTRACT(DAY FROM f.data_nascimento) = %s
              AND u.ativo = 1
        """, (mes, dia))
        
        aniversariantes = cursor.fetchall()
        
        if aniversariantes:
            print(f"\n🎉 Encontrados {len(aniversariantes)} aniversariante(s) hoje!")
            print("-" * 60)
            
            enviados = 0
            falhas = 0
            sem_email = 0
            
            for a in aniversariantes:
                # Calcular idade
                idade = hoje.year - a['data_nascimento'].year
                
                print(f"\n📌 Familiar: {a['nome']} ({a['parentesco'].title()})")
                print(f"   Idade: {idade} anos")
                print(f"   Obreiro: {a['obreiro_nome']}")
                
                # Preparar dados do familiar
                familiar = {
                    'nome': a['nome'],
                    'parentesco': a['parentesco'],
                    'data_nascimento': a['data_nascimento']
                }
                
                # Preparar dados do obreiro
                obreiro = {
                    'nome_completo': a['obreiro_nome'],
                    'email': a['obreiro_email'],
                    'telefone': a['obreiro_telefone']
                }
                
                # Enviar e-mail
                if obreiro['email']:
                    print(f"   📧 Enviando e-mail para: {obreiro['email']}")
                    sucesso = enviar_notificacao_aniversario(familiar, obreiro)
                    
                    if sucesso:
                        print(f"   ✅ E-mail enviado com sucesso!")
                        enviados += 1
                    else:
                        print(f"   ❌ Falha ao enviar e-mail")
                        falhas += 1
                else:
                    print(f"   ⚠️ Obreiro não tem e-mail cadastrado")
                    sem_email += 1
            
            print(f"\n{'='*60}")
            print(f"📊 RESUMO DO ENVIO:")
            print(f"   ✅ Enviados: {enviados}")
            print(f"   ❌ Falhas: {falhas}")
            print(f"   ⚠️ Sem e-mail: {sem_email}")
            print(f"{'='*60}")
            
        else:
            print(f"\n📭 Nenhum aniversariante encontrado hoje.")
        
        conn.commit()
        return_connection(conn)
        
    except Exception as e:
        print(f"\n❌ Erro ao verificar aniversariantes: {e}")
        import traceback
        traceback.print_exc()

def listar_aniversariantes_mes():
    """Lista todos os aniversariantes do mês atual"""
    try:
        cursor, conn = get_db()
        
        hoje = datetime.now()
        mes = hoje.month
        
        cursor.execute("""
            SELECT f.*, 
                   u.nome_completo as obreiro_nome,
                   u.id as obreiro_id,
                   u.email as obreiro_email
            FROM familiares f
            JOIN usuarios u ON f.obreiro_id = u.id
            WHERE f.receber_notificacoes = 1
              AND EXTRACT(MONTH FROM f.data_nascimento) = %s
              AND u.ativo = 1
            ORDER BY EXTRACT(DAY FROM f.data_nascimento)
        """, (mes,))
        
        aniversariantes = cursor.fetchall()
        
        nome_mes = hoje.strftime('%B').capitalize()
        print(f"\n{'='*60}")
        print(f"📅 ANIVERSARIANTES DO MÊS DE {nome_mes.upper()}")
        print(f"{'='*60}")
        
        if aniversariantes:
            for a in aniversariantes:
                dia = a['data_nascimento'].day
                idade_este_ano = hoje.year - a['data_nascimento'].year
                email_info = f" (Email: {a['obreiro_email']})" if a['obreiro_email'] else " (Sem email)"
                
                print(f"\n  📌 {dia:2d} - {a['nome']} ({a['parentesco'].title()})")
                print(f"      Idade: {idade_este_ano} anos")
                print(f"      Obreiro: {a['obreiro_nome']}{email_info}")
        else:
            print("\n  Nenhum aniversariante este mês")
        
        return_connection(conn)
        
    except Exception as e:
        print(f"❌ Erro ao listar aniversariantes: {e}")

def listar_todos_aniversariantes():
    """Lista todos os aniversariantes cadastrados"""
    try:
        cursor, conn = get_db()
        
        hoje = datetime.now()
        
        cursor.execute("""
            SELECT f.*, 
                   u.nome_completo as obreiro_nome,
                   u.id as obreiro_id,
                   u.email as obreiro_email
            FROM familiares f
            JOIN usuarios u ON f.obreiro_id = u.id
            WHERE f.receber_notificacoes = 1
              AND f.data_nascimento IS NOT NULL
              AND u.ativo = 1
            ORDER BY EXTRACT(MONTH FROM f.data_nascimento), 
                     EXTRACT(DAY FROM f.data_nascimento)
        """)
        
        aniversariantes = cursor.fetchall()
        
        print(f"\n{'='*60}")
        print(f"📅 TODOS OS ANIVERSARIANTES CADASTRADOS")
        print(f"{'='*60}")
        
        if aniversariantes:
            mes_atual = None
            for a in aniversariantes:
                mes = a['data_nascimento'].month
                dia = a['data_nascimento'].day
                idade = hoje.year - a['data_nascimento'].year
                
                if mes != mes_atual:
                    mes_atual = mes
                    nome_mes = a['data_nascimento'].strftime('%B').capitalize()
                    print(f"\n📌 {nome_mes}:")
                    print("-" * 50)
                
                email_info = f" ({a['obreiro_email']})" if a['obreiro_email'] else ""
                print(f"  • {dia:2d} - {a['nome']} ({a['parentesco'].title()}) - {idade} anos")
                print(f"      Obreiro: {a['obreiro_nome']}{email_info}")
        else:
            print("\n  Nenhum aniversariante cadastrado")
        
        return_connection(conn)
        
    except Exception as e:
        print(f"❌ Erro ao listar aniversariantes: {e}")

def verificar_configuracao_email():
    """Verifica se o e-mail está configurado"""
    if email_service.config:
        print(f"\n✅ E-mail configurado:")
        print(f"   Servidor: {email_service.config['server']}")
        print(f"   Porta: {email_service.config['port']}")
        print(f"   Remetente: {email_service.config['sender']}")
        print(f"   Nome: {email_service.config['sender_name']}")
        return True
    else:
        print(f"\n⚠️ E-mail NÃO configurado!")
        print("   Configure o e-mail em: http://localhost:5000/config/email")
        return False

def executar_agora():
    """Executa a verificação imediatamente"""
    print("\n🚀 EXECUTANDO VERIFICAÇÃO DE ANIVERSARIANTES")
    print("=" * 60)
    
    # Verificar configuração de e-mail
    verificar_configuracao_email()
    
    # Executar verificação
    verificar_aniversariantes()
    
    # Listar próximos aniversários
    listar_aniversariantes_mes()

def executar_loop():
    """Executa em loop verificando a cada hora"""
    print("\n🚀 INICIANDO SERVIÇO DE NOTIFICAÇÕES DE ANIVERSÁRIO")
    print("=" * 60)
    
    # Verificar configuração inicial
    if not verificar_configuracao_email():
        print("\n⚠️ ATENÇÃO: Configure o e-mail antes de iniciar o serviço!")
        print("   Acesse: http://localhost:5000/config/email")
        print("\nPressione Ctrl+C para sair...")
    
    print("\n⏰ O sistema verificará aniversariantes a cada hora")
    print("📅 Também fará uma verificação especial às 08:00")
    print("📌 Pressione Ctrl+C para parar")
    print("=" * 60)
    
    # Executar uma vez ao iniciar
    executar_agora()
    
    ultima_verificacao = datetime.now().date()
    ultima_verificacao_hora = datetime.now().hour
    
    try:
        while True:
            agora = datetime.now()
            hoje = agora.date()
            hora_atual = agora.hour
            
            # Verificar uma vez por dia às 08:00 (se mudou a hora)
            if hora_atual == 8 and ultima_verificacao_hora != hora_atual:
                print(f"\n📅 Verificação diária das 08:00 - {agora.strftime('%d/%m/%Y')}")
                executar_agora()
                ultima_verificacao_hora = hora_atual
                ultima_verificacao = hoje
            
            # Verificar também se mudou o dia (executar uma vez por dia)
            elif hoje != ultima_verificacao:
                print(f"\n📅 Nova data: {agora.strftime('%d/%m/%Y')}")
                executar_agora()
                ultima_verificacao = hoje
                ultima_verificacao_hora = hora_atual
            
            # Aguardar 1 hora antes de verificar novamente
            time.sleep(3600)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Serviço de notificações interrompido pelo usuário")

def mostrar_ajuda():
    """Mostra ajuda do script"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║        SISTEMA DE NOTIFICAÇÕES DE ANIVERSÁRIO                    ║
╚══════════════════════════════════════════════════════════════════╝

📌 COMANDOS DISPONÍVEIS:

  python notificacoes_aniversario.py --agora    
      → Verifica aniversariantes do dia e envia e-mails AGORA
  
  python notificacoes_aniversario.py --listar   
      → Lista os aniversariantes do MÊS ATUAL
  
  python notificacoes_aniversario.py --todos    
      → Lista TODOS os aniversariantes cadastrados
  
  python notificacoes_aniversario.py --daemon   
      → Executa em SEGUNDO PLANO (verifica a cada hora)
  
  python notificacoes_aniversario.py --help     
      → Mostra esta ajuda

📧 CONFIGURAÇÃO DE E-MAIL:
  Antes de usar, configure o e-mail em:
  http://localhost:5000/config/email

📅 AGENDAMENTO AUTOMÁTICO:
  O serviço --daemon verifica aniversariantes:
  • Imediatamente ao iniciar
  • Diariamente às 08:00
  • A cada hora para garantir

🔔 NOTIFICAÇÕES:
  • Envia e-mail para o obreiro no dia do aniversário
  • Inclui nome, parentesco e idade do familiar
  • Registra todos os envios no log do sistema

╔══════════════════════════════════════════════════════════════════╗
║  Exemplo: python notificacoes_aniversario.py --agora            ║
╚══════════════════════════════════════════════════════════════════╝
    """)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Sistema de notificações de aniversário')
    parser.add_argument('--agora', action='store_true', help='Executar verificação agora')
    parser.add_argument('--listar', action='store_true', help='Listar aniversariantes do mês')
    parser.add_argument('--todos', action='store_true', help='Listar todos os aniversariantes')
    parser.add_argument('--daemon', action='store_true', help='Executar em segundo plano (verificação horária)')
    parser.add_argument('--help', action='store_true', help='Mostrar ajuda')
    
    args = parser.parse_args()
    
    if args.help:
        mostrar_ajuda()
    elif args.agora:
        executar_agora()
    elif args.listar:
        listar_aniversariantes_mes()
    elif args.todos:
        listar_todos_aniversariantes()
    elif args.daemon:
        executar_loop()
    else:
        mostrar_ajuda()