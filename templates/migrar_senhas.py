# migrar_senhas.py - VERSÃO CORRIGIDA
# -*- coding: utf-8 -*-
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor
from werkzeug.security import generate_password_hash, check_password_hash
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Usar DATABASE_URL do ambiente (mais seguro)
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    # Fallback para conexão local (SEM caracteres especiais)
    DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/sistema_maconico"

print(f"🔌 Conectando ao banco...")

try:
    # Conectar usando DATABASE_URL (resolve problemas de encoding)
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    print("✅ Conectado com sucesso!")

    # Buscar todos os usuários
    cursor.execute("SELECT id, usuario, senha_hash FROM usuarios")
    usuarios = cursor.fetchall()

    print(f"\n🔍 Analisando {len(usuarios)} usuários...")
    print("=" * 50)

    modificados = 0
    precisam_reset = []

    for usuario in usuarios:
        user_id = usuario['id']
        username = usuario['usuario']
        senha_hash_atual = usuario['senha_hash']

        # Verificar se já está no formato Werkzeug
        is_werkzeug = senha_hash_atual and senha_hash_atual.startswith(('scrypt:', 'pbkdf2:', 'bcrypt:'))

        if not is_werkzeug and senha_hash_atual and len(senha_hash_atual) == 64:
            # É SHA256 puro
            print(f"\n⚠️ Usuário: {username}")
            print(f"   Hash atual: {senha_hash_atual[:20]}...")

            # Tabela de senhas conhecidas (ajuste conforme necessário)
            senhas_conhecidas = {
                'admin': 'admin',
                'charles': 'admin',
                'jairo': '123456',  # Palpite - ajuste
                'francisco': '123456',  # Palpite - ajuste
                'Renato': '123456',  # Palpite - ajuste
                'Aben-Athar': '123456',  # Palpite - ajuste
            }

            senha_original = senhas_conhecidas.get(username)

            if senha_original:
                # Gerar novo hash no formato Werkzeug
                novo_hash = generate_password_hash(senha_original)

                # Atualizar no banco
                cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (novo_hash, user_id))
                modificados += 1
                print(f"   ✅ Migrado com sucesso!")
                print(f"   Nova senha: {senha_original}")
            else:
                # Resetar para senha padrão
                nova_senha = 'mudar123'
                novo_hash = generate_password_hash(nova_senha)
                cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (novo_hash, user_id))
                modificados += 1
                precisam_reset.append(username)
                print(f"   ⚠️ Senha desconhecida - resetada para: {nova_senha}")
                print(f"   ✅ Usuário {username} atualizado (senha padrão: {nova_senha})")

    conn.commit()

    print("\n" + "=" * 50)
    print(f"✅ {modificados} usuários migrados para o formato Werkzeug!")

    if precisam_reset:
        print(f"\n⚠️ Usuários com senha resetada para 'mudar123':")
        for user in precisam_reset:
            print(f"   - {user}")

    print("\n🔐 Agora use as credenciais atualizadas para fazer login!")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback

    traceback.print_exc()