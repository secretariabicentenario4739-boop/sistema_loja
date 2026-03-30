def gerar_presenca_reuniao(db, reuniao_id, loja_id):
    obreiros = db.execute("""
        SELECT id FROM obreiros WHERE loja_id = %s
    """, (loja_id,)).fetchall()

    for o in obreiros:
        db.execute("""
            INSERT INTO presencas (reuniao_id, obreiro_id, presente)
            VALUES (%s, %s, NULL)
            ON CONFLICT (reuniao_id, obreiro_id) DO NOTHING
        """, (reuniao_id, o["id"]))

    db.commit()