from services.presenca_service import gerar_presenca_reuniao

@reunioes_bp.route("/reunioes/nova", methods=["POST"])
def criar_reuniao():
    db = get_db()

    loja_id = request.form["loja_id"]

    # 1. cria reunião
    reuniao_id = db.execute("""
        INSERT INTO reunioes (titulo, loja_id)
        VALUES (%s, %s)
        RETURNING id
    """, (request.form["titulo"], loja_id)).fetchone()["id"]

    db.commit()

    # 2. gera presença automática
    gerar_presenca_reuniao(db, reuniao_id, loja_id)

    flash("Reunião criada com presença gerada!", "success")
    return redirect("/reunioes")