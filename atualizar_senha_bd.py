# atualizar_senha.py
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'sistema_maconico'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

print("="*60)
print("🔧 ATUALIZAR SENHA DE E-MAIL")
print("="*60)

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Mostrar configuração atual
    cursor.execute("""
        SELECT id, server, port, username, active 
        FROM email_settings 
        WHERE active = 1
    """)
    config = cursor.fetchone()
    
    if config:
        print(f"\n📧 Configuração ativa (ID: {config[0]}):")
        print(f"   Servidor: {config[1]}:{config[2]}")
        print(f"   Usuário: {config[3]}")
        
        print("\n" + "-"*60)
        print("⚠️  IMPORTANTE: Para Gmail, use SENHA DE APLICATIVO!")
        print("   Gerar em: https://myaccount.google.com/apppasswords")
        print("-"*60)
        
        nova_senha = input("\n🔑 Digite a senha de aplicativo: ").strip()
        
        if nova_senha:
            cursor.execute("""
                UPDATE email_settings 
                SET password = %s
                WHERE id = %s
            """, (nova_senha, config[0]))
            conn.commit()
            print(f"\n✅ Senha atualizada com sucesso!")
            
            # Testar conexão
            print("\n🔍 Testando conexão...")
            import smtplib
            
            try:
                server = smtplib.SMTP(config[1], config[2], timeout=30)
                server.starttls()
                server.login(config[3], nova_senha)
                server.quit()
                print("✅ Conexão com servidor SMTP OK!")
            except Exception as e:
                print(f"⚠️  Teste de conexão falhou: {e}")
                print("   Verifique se a senha está correta")
        else:
            print("❌ Nenhuma senha informada.")
    else:
        print("❌ Nenhuma configuração ativa encontrada!")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Erro: {e}")