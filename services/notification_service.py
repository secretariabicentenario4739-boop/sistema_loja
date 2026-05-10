from datetime import datetime, timedelta


def verificar_reunioes_e_enviar_notificacoes(
    get_db,
    return_connection,
    enviar_notificacao_reuniao_lembrete,
):
    """Verifica reuniões do dia seguinte e envia notificações."""
    cursor, conn = get_db()
    try:
        hoje = datetime.now().date()
        amanha = hoje + timedelta(days=1)

        cursor.execute(
            """
            SELECT r.*, l.nome as loja_nome
            FROM reunioes r
            LEFT JOIN lojas l ON r.loja_id = l.id
            WHERE r.status = 'agendada'
            AND r.data = %s
            ORDER BY r.hora_inicio
            """,
            (amanha,),
        )
        reunioes_amanha = cursor.fetchall()

        total_notificados = 0
        for reuniao in reunioes_amanha:
            cursor.execute(
                """
                SELECT u.id, u.nome_completo, u.email,
                       COALESCE(nc.dias_antecedencia_reuniao, 1) as dias_antecedencia
                FROM usuarios u
                LEFT JOIN notificacoes_config nc ON u.id = nc.usuario_id
                WHERE u.ativo = 1
                AND u.email IS NOT NULL
                AND u.email != ''
                """
            )
            participantes = cursor.fetchall()
            for participante in participantes:
                dias_antecedencia = participante.get("dias_antecedencia", 1)
                if dias_antecedencia >= 1:
                    enviar_notificacao_reuniao_lembrete(participante, reuniao)
                    total_notificados += 1

        return_connection(conn)
        return {
            "success": True,
            "reunioes_amanha": len(reunioes_amanha),
            "participantes_notificados": total_notificados,
        }
    except Exception as e:
        print(f"Erro ao verificar reuniões: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            return_connection(conn)
        return {"success": False, "error": str(e)}


def verificar_aniversarios_e_enviar_notificacoes(
    get_db,
    return_connection,
    registrar_notificacao_sistema,
    enviar_email_aniversario_obreiro,
    enviar_email_aniversario_familiar,
):
    """Verifica aniversários do dia e envia notificações."""
    cursor, conn = get_db()
    try:
        hoje = datetime.now().date()

        cursor.execute(
            """
            SELECT u.id, u.nome_completo, u.email, u.telefone,
                   nc.notificar_aniversario_obreiro
            FROM usuarios u
            LEFT JOIN notificacoes_config nc ON u.id = nc.usuario_id
            WHERE EXTRACT(MONTH FROM u.data_nascimento) = %s
              AND EXTRACT(DAY FROM u.data_nascimento) = %s
              AND u.ativo = 1
              AND u.data_nascimento IS NOT NULL
            """,
            (hoje.month, hoje.day),
        )
        obreiros_aniversariantes = cursor.fetchall()

        cursor.execute(
            """
            SELECT f.id, f.nome, f.obreiro_id, f.grau_parentesco,
                   u.nome_completo as obreiro_nome, u.email as obreiro_email,
                   nc.notificar_aniversario_familiar
            FROM familiares f
            JOIN usuarios u ON f.obreiro_id = u.id
            LEFT JOIN notificacoes_config nc ON u.id = nc.usuario_id
            WHERE EXTRACT(MONTH FROM f.data_nascimento) = %s
              AND EXTRACT(DAY FROM f.data_nascimento) = %s
              AND f.ativo = 1
              AND f.data_nascimento IS NOT NULL
            """,
            (hoje.month, hoje.day),
        )
        familiares_aniversariantes = cursor.fetchall()

        for obreiro in obreiros_aniversariantes:
            if obreiro.get("notificar_aniversario_obreiro", 1):
                titulo = f"🎂 Feliz Aniversário, {obreiro['nome_completo']}!"
                mensagem = (
                    "Neste dia especial, toda a Loja Maçônica celebra sua vida. "
                    "Que a Sabedoria, Força e Beleza continuem guiando seus passos."
                )
                registrar_notificacao_sistema(
                    obreiro["id"],
                    titulo,
                    mensagem,
                    "aniversario_obreiro",
                    "/obreiros/perfil",
                )
                if obreiro.get("email"):
                    enviar_email_aniversario_obreiro(obreiro["email"], obreiro["nome_completo"])

        for familiar in familiares_aniversariantes:
            if familiar.get("notificar_aniversario_familiar", 1):
                titulo = f"🎂 Aniversário do Familiar: {familiar['nome']}"
                mensagem = (
                    f"Hoje é aniversário de {familiar['nome']} ({familiar['grau_parentesco']}). "
                    "Que este dia seja repleto de alegria para sua família."
                )
                registrar_notificacao_sistema(
                    familiar["obreiro_id"],
                    titulo,
                    mensagem,
                    "aniversario_familiar",
                    "/familiares",
                )
                if familiar.get("obreiro_email"):
                    enviar_email_aniversario_familiar(
                        familiar["obreiro_email"],
                        familiar["obreiro_nome"],
                        familiar["nome"],
                        familiar["grau_parentesco"],
                    )

        return_connection(conn)
        return {
            "success": True,
            "obreiro_aniversariantes": len(obreiros_aniversariantes),
            "familiar_aniversariantes": len(familiares_aniversariantes),
        }
    except Exception as e:
        print(f"Erro ao verificar aniversários: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.rollback()
        return_connection(conn)
        return {"success": False, "error": str(e)}
