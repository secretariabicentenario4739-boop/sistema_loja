import sqlite3

conn = sqlite3.connect("banco.db")
cursor = conn.cursor()

# Atualizar todas as atas que não têm numero_ata/ano_ata
# Primeiro, agrupar por ano e renumera
cursor.execute("""
    SELECT id, strftime('%Y', data_criacao) as ano
    FROM atas
    WHERE numero_ata IS NULL OR ano_ata IS NULL
    ORDER BY data_criacao
""")
atas = cursor.fetchall()

for ata_id, ano in atas:
    # Contar quantas atas já existem no mesmo ano
    cursor.execute("SELECT COUNT(*) as total FROM atas WHERE ano_ata = ?", (ano,))
    total = cursor.fetchone()[0]
    novo_numero = total + 1
    cursor.execute("""
        UPDATE atas
        SET numero_ata = ?, ano_ata = ?
        WHERE id = ?
    """, (novo_numero, ano, ata_id))

conn.commit()
conn.close()
print("Atas corrigidas!")