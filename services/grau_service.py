def get_grau_principal(grau_nivel):
    if grau_nivel == 1:
        return "Aprendiz"
    if grau_nivel == 2:
        return "Companheiro"
    if grau_nivel >= 3:
        return "Mestre"
    return "Mestre"


def get_grau_detalhado(grau_nivel):
    grau_map = {
        1: "Aprendiz",
        2: "Companheiro",
        3: "Mestre",
        4: "Mestre Instalado",
        5: "Arquiteto Real",
        6: "Soberano Grande Inspetor Geral",
        7: "Mestre Perfeito",
        8: "Eleito dos Nove",
        9: "Mestre da Maçonaria Real",
        10: "Cavaleiro Rosa-Cruz",
        11: "Cavaleiro Kadosch",
        12: "Grande Escocês",
    }
    return grau_map.get(grau_nivel, f"Grau Superior {grau_nivel}")


def get_grau_descricao(grau):
    if grau == 1:
        return "Aprendiz"
    if grau == 2:
        return "Companheiro"
    if grau == 3:
        return "Mestre"
    if grau == 4:
        return "Mestre Instalado"
    if grau == 5:
        return "Mestre Inst. (5°)"
    if grau == 6:
        return "Grau 6 - Superior"
    if grau >= 7:
        return f"Grau Superior {grau}"
    return "Mestre"


def get_grau_badge_class(grau_nivel):
    if grau_nivel == 1:
        return "bg-secondary"
    if grau_nivel == 2:
        return "bg-primary"
    if grau_nivel == 3:
        return "bg-warning text-dark"
    if grau_nivel >= 4:
        return "bg-info"
    return "bg-secondary"


def get_grau_icon(grau_nivel):
    if grau_nivel == 1 or grau_nivel == 2:
        return "bi bi-star"
    if grau_nivel >= 3:
        return "bi bi-star-fill"
    return "bi bi-star"
