# email_service.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import os
from dotenv import load_dotenv
from db_config import get_db, return_connection

load_dotenv()

class EmailService:
    def __init__(self):
        self.config = self._load_config()
    
    def _load_config(self):
        """Carrega configurações de e-mail do banco"""
        try:
            cursor, conn = get_db()
            cursor.execute("""
                SELECT * FROM email_settings 
                WHERE active = 1 
                ORDER BY id DESC LIMIT 1
            """)
            config = cursor.fetchone()
            return_connection(conn)
            
            if config:
                print(f"✅ Configuração de e-mail carregada:")
                print(f"   Servidor: {config['server']}")
                print(f"   Porta: {config['port']}")
                print(f"   Usuário: {config['username']}")
                return {
                    'server': config['server'],
                    'port': config['port'],
                    'use_ssl': config.get('use_ssl', 1),  # Usar SSL por padrão
                    'username': config['username'],
                    'password': config['password'],
                    'sender': config['sender'],
                    'sender_name': config['sender_name'] or 'Sistema Maçônico'
                }
            return None
        except Exception as e:
            print(f"❌ Erro ao carregar configurações: {e}")
            return None
    
    def enviar_email(self, destinatario, assunto, corpo_html, corpo_texto=None):
        """Envia um e-mail com logs detalhados"""
        if not self.config:
            print("❌ Configuração de e-mail não encontrada")
            print("   Acesse: http://localhost:5000/config/email")
            return False
        
        print(f"\n📧 Tentando enviar e-mail para: {destinatario}")
        print(f"   Assunto: {assunto}")
        print(f"   Servidor: {self.config['server']}:{self.config['port']}")
        
        try:
            # Criar mensagem
            msg = MIMEMultipart('alternative')
            msg['From'] = f"{self.config['sender_name']} <{self.config['sender']}>"
            msg['To'] = destinatario
            msg['Subject'] = assunto
            
            # Adicionar versão texto
            if corpo_texto:
                parte_texto = MIMEText(corpo_texto, 'plain', 'utf-8')
                msg.attach(parte_texto)
            
            # Adicionar versão HTML
            parte_html = MIMEText(corpo_html, 'html', 'utf-8')
            msg.attach(parte_html)
            
            # Conectar ao servidor SMTP
            print(f"🔄 Conectando ao servidor {self.config['server']}:{self.config['port']}...")
            
            # Usar SSL se a porta for 465 ou configurado
            if self.config['port'] == 465 or self.config.get('use_ssl'):
                print("   Usando SSL direto...")
                server = smtplib.SMTP_SSL(self.config['server'], self.config['port'], timeout=30)
            else:
                print("   Usando TLS...")
                server = smtplib.SMTP(self.config['server'], self.config['port'], timeout=30)
                server.starttls()
            
            print(f"🔐 Fazendo login como {self.config['username']}...")
            server.login(self.config['username'], self.config['password'])
            
            print(f"📤 Enviando mensagem...")
            server.send_message(msg)
            server.quit()
            
            print(f"✅ E-mail enviado com sucesso para {destinatario}")
            
            # Registrar envio
            self._registrar_log(destinatario, assunto, 'enviado')
            return True
            
        except smtplib.SMTPAuthenticationError as e:
            print(f"❌ Erro de autenticação: {e}")
            print("   Verifique usuário e senha")
            print("   Se for Gmail, use senha de aplicativo")
            self._registrar_log(destinatario, assunto, 'erro', str(e))
            return False
            
        except smtplib.SMTPConnectError as e:
            print(f"❌ Erro de conexão: {e}")
            print(f"   Verifique servidor e porta: {self.config['server']}:{self.config['port']}")
            self._registrar_log(destinatario, assunto, 'erro', str(e))
            return False
            
        except Exception as e:
            print(f"❌ Erro inesperado: {e}")
            import traceback
            traceback.print_exc()
            self._registrar_log(destinatario, assunto, 'erro', str(e))
            return False
    
    def _registrar_log(self, destinatario, assunto, status, erro=None):
        """Registra o envio no log"""
        try:
            cursor, conn = get_db()
            cursor.execute("""
                INSERT INTO notificacoes_log 
                (destinatario, assunto, corpo, tipo, status, erro, data_envio)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (destinatario, assunto, '', 'email', status, erro, datetime.now()))
            conn.commit()
            return_connection(conn)
        except Exception as e:
            print(f"Erro ao registrar log: {e}")

# Instância global
email_service = EmailService()