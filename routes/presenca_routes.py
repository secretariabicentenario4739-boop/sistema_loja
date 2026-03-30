from flask import Blueprint, request, redirect, flash

presenca_bp = Blueprint("presenca", __name__)

@presenca_bp.route("/presenca/update/<int:presenca_id>", methods=["POST"])
def atualizar_presenca(presenca_id):
    presente = request.form.get("presente")
    tipo_ausencia = request.form.get("tipo_ausencia")
    justificativa = request.form.get("justificativa")

    db = get_db()

    db.execute("""
        UPDATE presencas
        SET presente = %s,
            tipo_ausencia = %s,
            justificativa = %s
        WHERE id = %s
    """, (presente, tipo_ausencia, justificativa, presenca_id))

    db.commit()

    flash("Presença atualizada com sucesso!", "success")
    return redirect(request.referrer)