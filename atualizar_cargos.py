import sqlite3

def adicionar_coluna_ativo():
    conn = sqlite3.connect("banco.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("🔄 Verificando estrutura da tabela 'cargos'...")
    
    # Verificar colunas existentes
    cursor.execute("PRAGMA table_info(cargos)")
    colunas = [col[1] for col in cursor.fetchall()]
    
    print(f"📋 Colunas atuais: {', '.join(colunas)}")
    
    # Adicionar coluna 'ativo' se não existir
    if "ativo" not in colunas:
        print("➕ Adicionando coluna 'ativo'...")
        cursor.execute("ALTER TABLE cargos ADD COLUMN ativo INTEGER DEFAULT 1")
        print("✅ Coluna 'ativo' adicionada com sucesso!")
    else:
        print("✅ Coluna 'ativo' já existe")
    
    # Atualizar registros existentes para ativo = 1
    cursor.execute("UPDATE cargos SET ativo = 1 WHERE ativo IS NULL")
    print("✅ Registros existentes atualizados")
    
    conn.commit()
    
    # Mostrar estrutura final
    cursor.execute("PRAGMA table_info(cargos)")
    colunas_final = cursor.fetchall()
    print("\n📊 Estrutura final da tabela 'cargos':")
    for col in colunas_final:
        print(f"   • {col['name']} ({col['type']})")
    
    conn.close()
    print("\n✅ Migração concluída!")

def verificar_cargos():
    conn = sqlite3.connect("banco.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("\n🔍 VERIFICANDO CARGOS CADASTRADOS")
    print("=" * 40)
    
    cursor.execute("SELECT * FROM cargos ORDER BY ordem")
    cargos = cursor.fetchall()
    
    if cargos:
        print(f"📋 Total de cargos: {len(cargos)}")
        for cargo in cargos:
            ativo = cargo['ativo'] if 'ativo' in cargo.keys() else 1
            ativo_status = "✅ Ativo" if ativo == 1 else "❌ Inativo"
            print(f"   • {cargo['ordem']}. {cargo['nome']} ({cargo['sigla']}) - {ativo_status}")
    else:
        print("📭 Nenhum cargo cadastrado")
    
    conn.close()

def listar_tabelas():
    conn = sqlite3.connect("banco.db")
    cursor = conn.cursor()
    
    print("\n📋 TABELAS NO BANCO DE DADOS")
    print("=" * 40)
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tabelas = cursor.fetchall()
    
    for tabela in tabelas:
        print(f"   • {tabela[0]}")
    
    conn.close()

if __name__ == "__main__":
    print("🚀 SCRIPT DE ATUALIZAÇÃO DA TABELA CARGOS")
    print("=" * 50)
    
    # Mostrar tabelas existentes
    listar_tabelas()
    
    # Perguntar se quer verificar antes
    resposta = input("\nDeseja verificar os cargos existentes? (s/n): ")
    if resposta.lower() == 's':
        verificar_cargos()
    
    # Perguntar se quer adicionar a coluna
    resposta = input("\nDeseja adicionar a coluna 'ativo' na tabela cargos? (s/n): ")
    if resposta.lower() == 's':
        adicionar_coluna_ativo()
    else:
        print("❌ Operação cancelada.")
    
    # Verificar novamente
    resposta = input("\nDeseja verificar os cargos após a atualização? (s/n): ")
    if resposta.lower() == 's':
        verificar_cargos()
    
    print("\n✅ Script finalizado!")