# limpar_tabela.py
import subprocess
import sys
import os

def executar_psql(comando, capturar_saida=False):
    """Executa comando psql"""
    env = os.environ.copy()
    env['PGPASSWORD'] = '1kmSlZWYS4IywIdCGskYDkAnFF4mzyTL'
    
    cmd = [
        'psql',
        '-h', 'localhost',
        '-p', '5432',
        '-U', 'postgres',
        '-d', 'sistema_maconico',
        '-c', comando
    ]
    
    try:
        if capturar_saida:
            result = subprocess.run(cmd, capture_output=True, text=True, env=env, encoding='utf-8', errors='ignore')
            return result.returncode == 0, result.stdout
        else:
            result = subprocess.run(cmd, env=env)
            return result.returncode == 0, ""
    except Exception as e:
        return False, str(e)

def main():
    print("=" * 60)
    print("LIMPAR TABELA assinaturas_ata")
    print("=" * 60)
    
    # Testa conexao
    print("\n[1/4] Testando conexao...")
    sucesso, _ = executar_psql("SELECT 1")
    if not sucesso:
        print("   ❌ Erro ao conectar ao PostgreSQL!")
        print("      Verifique se o servidor esta rodando.")
        input("\nPressione ENTER para sair...")
        return
    print("   ✅ Conexao OK")
    
    # Mostra registros atuais
    print("\n[2/4] Registros atuais na tabela:")
    sucesso, saida = executar_psql("SELECT id, usuario_id, status FROM assinaturas_ata ORDER BY id;", True)
    if sucesso and saida:
        print(saida)
    else:
        print("   Nenhum registro encontrado")
    
    # Confirma limpeza
    print("\n" + "=" * 60)
    print("⚠️  ATENCAO: Isso vai APAGAR TODOS os registros!")
    print("=" * 60)
    confirm = input("\nDigite 'SIM' para confirmar a limpeza: ")
    
    if confirm != "SIM":
        print("\n❌ Operacao cancelada!")
        return
    
    # Limpa a tabela
    print("\n[3/4] Limpando tabela...")
    sucesso, _ = executar_psql("TRUNCATE TABLE assinaturas_ata RESTART IDENTITY CASCADE;")
    if sucesso:
        print("   ✅ Tabela limpa com sucesso!")
    else:
        print("   ❌ Erro ao limpar tabela!")
        return
    
    # Reseta sequencia
    print("\n[4/4] Resetando sequencia...")
    sucesso, _ = executar_psql("SELECT setval('assinaturas_ata_id_seq', 1, false);")
    if sucesso:
        print("   ✅ Sequencia resetada!")
    
    # Verifica resultado
    print("\n" + "=" * 60)
    print("✅ TABELA LIMPA COM SUCESSO!")
    print("=" * 60)
    
    sucesso, saida = executar_psql("SELECT COUNT(*) as total FROM assinaturas_ata;", True)
    if sucesso:
        print(f"\n📊 Total de registros agora: {saida.strip()}")
    
    print("\nAgora voce pode restaurar o backup sem erros de duplicacao!")
    input("\nPressione ENTER para sair...")

if __name__ == "__main__":
    main()