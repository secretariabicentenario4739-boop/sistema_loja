import time
from functools import wraps

from flask import flash, redirect, session, url_for

from database import get_db, return_connection


_cache_grau = {}
_cache_timeout = 60


def _get_grau_usuario(usuario_id):
    cache_key = f"grau_{usuario_id}"
    cache_entry = _cache_grau.get(cache_key)
    if cache_entry and (time.time() - cache_entry["timestamp"]) < _cache_timeout:
        return cache_entry["grau"]

    cursor, conn = None, None
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT grau_atual FROM usuarios WHERE id = %s", (usuario_id,))
        usuario = cursor.fetchone()
        grau = usuario["grau_atual"] if usuario else 1
        _cache_grau[cache_key] = {"grau": grau, "timestamp": time.time()}
        return grau
    except Exception:
        return 1
    finally:
        if conn:
            return_connection(conn)


def _get_ata_grau(ata_id):
    cache_key = f"ata_grau_{ata_id}"
    cache_entry = _cache_grau.get(cache_key)
    if cache_entry and (time.time() - cache_entry["timestamp"]) < _cache_timeout:
        return cache_entry.get("grau", 1)

    cursor, conn = None, None
    try:
        cursor, conn = get_db()
        cursor.execute(
            """
            SELECT r.grau as reuniao_grau
            FROM atas a
            JOIN reunioes r ON a.reuniao_id = r.id
            WHERE a.id = %s
            """,
            (ata_id,),
        )
        ata = cursor.fetchone()
        grau = ata["reuniao_grau"] if ata else 1
        _cache_grau[cache_key] = {"grau": grau, "timestamp": time.time()}
        return grau
    except Exception:
        return 1
    finally:
        if conn:
            return_connection(conn)


def require_grau(min_grau):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            usuario_id = session.get("user_id") or session.get("usuario_id")
            if not usuario_id:
                flash("Faça login para acessar esta página", "warning")
                return redirect(url_for("login"))
            if session.get("tipo") == "admin":
                return f(*args, **kwargs)
            grau_usuario = _get_grau_usuario(usuario_id)
            if grau_usuario < min_grau:
                flash("Você não tem permissão para acessar este conteúdo", "danger")
                return redirect(url_for("biblioteca.listar_materiais"))
            return f(*args, **kwargs)

        return decorated_function

    return decorator


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("user_id"):
            flash("Faça login para acessar esta página", "warning")
            return redirect("/")
        return f(*args, **kwargs)

    return decorated_function


def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("user_id") or session.get("tipo") != "admin":
            flash("Acesso restrito a administradores", "danger")
            return redirect("/dashboard")
        return f(*args, **kwargs)

    return decorated_function


def sindicante_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("user_id") or session.get("tipo") != "sindicante":
            flash("Acesso restrito a sindicantes", "danger")
            return redirect("/dashboard")
        return f(*args, **kwargs)

    return decorated_function


def nivel_required(nivel_minimo):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get("user_id"):
                flash("Faça login para acessar esta página", "warning")
                return redirect("/")
            if session.get("tipo") == "admin":
                return f(*args, **kwargs)
            nivel_usuario = session.get("nivel_acesso", 1)
            if nivel_usuario >= nivel_minimo:
                return f(*args, **kwargs)
            flash("Você não tem permissão para acessar esta página", "danger")
            return redirect("/dashboard")

        return decorated_function

    return decorator


def nivel_ata_required():
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get("user_id"):
                flash("Faça login para acessar esta página", "warning")
                return redirect("/")
            if session.get("tipo") == "admin":
                return f(*args, **kwargs)

            ata_id = kwargs.get("id")
            if ata_id:
                reuniao_grau = _get_ata_grau(ata_id)
                nivel_usuario = session.get("nivel_acesso", 1)
                if nivel_usuario == 1 and reuniao_grau == 1:
                    return f(*args, **kwargs)
                if nivel_usuario == 2 and reuniao_grau <= 2:
                    return f(*args, **kwargs)
                if nivel_usuario >= 3:
                    return f(*args, **kwargs)
                flash("Você não tem permissão para visualizar esta ata", "danger")
                return redirect("/dashboard")
            return f(*args, **kwargs)

        return decorated_function

    return decorator


def verificar_permissao(usuario_id, permissao_chave):
    cursor, conn = get_db()
    try:
        cursor.execute("SELECT grau_atual, tipo FROM usuarios WHERE id = %s", (usuario_id,))
        usuario = cursor.fetchone()
        if not usuario:
            return False
        if usuario["tipo"] == "admin":
            return True

        grau_original = usuario["grau_atual"]
        grau_efetivo = 3 if grau_original >= 3 else grau_original

        cursor.execute("SELECT id FROM permissoes WHERE chave = %s", (permissao_chave,))
        permissao = cursor.fetchone()
        if not permissao:
            return False
        permissao_id = permissao["id"]

        try:
            cursor.execute(
                """
                SELECT permitido FROM permissoes_usuario
                WHERE usuario_id = %s AND permissao_id = %s
                """,
                (usuario_id, permissao_id),
            )
            bloqueio = cursor.fetchone()
            if bloqueio and bloqueio["permitido"] == 0:
                return False
        except Exception:
            try:
                cursor.execute(
                    """
                    SELECT tipo FROM permissoes_usuario
                    WHERE usuario_id = %s AND permissao_id = %s
                    """,
                    (usuario_id, permissao_id),
                )
                bloqueio = cursor.fetchone()
                if bloqueio and bloqueio["tipo"] == 0:
                    return False
            except Exception:
                pass

        cursor.execute(
            """
            SELECT 1 FROM permissoes_grau
            WHERE grau_id = %s AND permissao_id = %s
            """,
            (grau_efetivo, permissao_id),
        )
        possui_permissao = cursor.fetchone() is not None

        if not possui_permissao:
            try:
                cursor.execute(
                    """
                    SELECT 1 FROM permissoes_usuario
                    WHERE usuario_id = %s AND permissao_id = %s AND permitido = 1
                    """,
                    (usuario_id, permissao_id),
                )
                possui_permissao = cursor.fetchone() is not None
            except Exception:
                try:
                    cursor.execute(
                        """
                        SELECT 1 FROM permissoes_usuario
                        WHERE usuario_id = %s AND permissao_id = %s AND tipo = 1
                        """,
                        (usuario_id, permissao_id),
                    )
                    possui_permissao = cursor.fetchone() is not None
                except Exception:
                    pass

        return possui_permissao
    except Exception:
        return False
    finally:
        if conn:
            return_connection(conn)


def tem_permissao(permissao_chave):
    if not session.get("user_id"):
        return False
    if session.get("tipo") == "admin":
        return True
    if permissao_chave == "obreiro.view" and session.get("grau_atual", 0) >= 3:
        return True
    return verificar_permissao(session["user_id"], permissao_chave)


def permissao_required(permissao_chave):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get("user_id"):
                flash("Você precisa estar logado para acessar esta página.", "danger")
                return redirect(url_for("login"))
            if tem_permissao(permissao_chave):
                return f(*args, **kwargs)
            flash("Você não tem permissão para acessar esta página.", "danger")
            return redirect(url_for("dashboard"))

        return decorated_function

    return decorator
