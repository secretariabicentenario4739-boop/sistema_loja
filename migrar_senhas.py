# migrar_senhas.py - VERSÃO PARA SEU .ENV
# -*- coding: utf-8 -*-
import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from werkzeug.security import generate_password_hash
from dotenv import load_dotenv

# Carregar .env
load_dotenv()

print("=" * 60)
print("MIGRADOR DE SENHAS - SISTEMA MAÇÔNICO")
print("=" * 60)

# Pegar credenciais do .env (sem caracteres especiais)
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'sistema_maconico')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

print(f"\n📋 Configuração do banco:")
print(f"   Host: {DB_HOST}")
print(f"   Porta: {DB_PORT}")
print(f"   Banco: {DB_NAME}")
print(f"   Usuário: {DB_USER}")
print(f"   Senha: {'*' * len(DB_PASSWORD) if DB_PASSWORD else 'NÃO DEFINIDA'}")

# Tentar conexão
try:
    print("\n🔌 Conectando ao PostgreSQL...")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        client_encoding='UTF8'
    )

    cursor = conn.cursor(cursor_factory=RealDictCursor)
    print("✅ Conectado com sucesso!")

except Exception as e:
    print(f"❌ Erro de conexão: {e}")
    print("\nPossíveis soluções:")
    print("1. Verifique se o PostgreSQL está rodando")
    print("2. Confirme se o banco 'sistema_maconico' existe")
    print("3. Teste a senha no pgAdmin ou psql")
    sys.exit(1)

# Buscar usuários
try:
    cursor.execute("SELECT id, usuario, senha_hash FROM usuarios")
    usuarios = cursor.fetchall()
    print(f"\n📊 Encontrados {len(usuarios)} usuários")
except Exception as e:
    print(f"❌ Erro ao buscar usuários: {e}")
    print("   A tabela 'usuarios' pode não existir ainda")
    sys.exit(1)

print("=" * 60)

modificados = 0
resetados = []

for usuario in usuarios:
    user_id = usuario['id']
    username = usuario['usuario']
    senha_hash_atual = usuario['senha_hash']

    print(f"\n👤 Usuário: {username}")

    # Verificar se já é hash Werkzeug
    if senha_hash_atual and str(senha_hash_atual).startswith(('scrypt:', 'pbkdf2:', 'bcrypt:')):
        print(f"   ✅ Já está no formato correto")
        continue

    # Resetar para senha padrão
    nova_senha = '123456'
    novo_hash = generate_password_hash(nova_senha)

    # Atualizar
    cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (novo_hash, user_id))
    modificados += 1
    resetados.append((username, nova_senha))
    print(f"   🔄 Senha resetada para: {nova_senha}")

# Commit das alterações
conn.commit()

print("\n" + "=" * 60)
print(f"✅ {modificados} usuários resetados!")

if resetados:
    print("\n📋 NOVAS CREDENCIAIS PARA TESTE:")
    print("-" * 40)
    for username, senha in resetados:
        print(f"   Usuário: {username}")
        print(f"   Senha: {senha}")
        print()

cursor.close()
conn.close()

print("🔐 Agora tente fazer login no sistema!")
print("=" * 60)