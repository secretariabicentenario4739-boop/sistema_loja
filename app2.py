from flask import Flask, render_template, request, redirect, session, flash, jsonify, send_file, after_this_request, make_response, Response
import sqlite3
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import os
import json
import openpyxl
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter
import shutil
import tempfile
from werkzeug.utils import secure_filename
import pywhatkit as kit
import time
import threading
import gc
import traceback

# Tentar importar psycopg2, se não existir, usar SQLite
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

app = Flask(__name__)
app.secret_key = os.urandom(24)

# =============================
# CONFIGURACAO DO BANCO DE DADOS
# =============================
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///banco.db')
IS_POSTGRES = DATABASE_URL.startswith('postgres') and HAS_PSYCOPG2

def get_db():
    if IS_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        conn.cursor_factory = RealDictCursor
        return conn
    else:
        conn = sqlite3.connect("banco.db")
        conn.row_factory = sqlite3.Row
        return conn

# =============================
# CONFIGURACOES DE UPLOAD
# =============================
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads', 'documentos')
BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'png', 'gif', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'zip', 'rar'}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def allowed_file_backup(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'db', 'sqlite', 'sqlite3'}

# =============================
# CONTEXTO GLOBAL PARA TEMPLATES
# =============================
@app.context_processor
def inject_global():
    return {'datetime': datetime, 'now': datetime.now()}

# =============================
# DECORATORS DE AUTENTICACAO
# =============================
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "usuario" not in session:
            flash("Faca login para acessar esta pagina", "warning")
            return redirect("/")
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "usuario" not in session or session.get("tipo") != "admin":
            flash("Acesso restrito a administradores", "danger")
            return redirect("/dashboard")
        return f(*args, **kwargs)
    return decorated_function

def sindicante_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "usuario" not in session or session.get("tipo") != "sindicante":
            flash("Acesso restrito a sindicantes", "danger")
            return redirect("/dashboard")
        return f(*args, **kwargs)
    return decorated_function

# =============================
# FUNCAO DE AUDITORIA
# =============================
def registrar_log(acao, entidade=None, entidade_id=None, dados_anteriores=None, dados_novos=None):
    if "user_id" not in session:
        return
    try:
        conn = get_db()
        cursor = conn.cursor()
        if dados_anteriores and isinstance(dados_anteriores, dict):
            dados_anteriores = json.dumps(dados_anteriores, ensure_ascii=False, default=str)
        if dados_novos and isinstance(dados_novos, dict):
            dados_novos = json.dumps(dados_novos, ensure_ascii=False, default=str)
        
        if IS_POSTGRES:
            cursor.execute("""
                INSERT INTO logs_auditoria 
                (usuario_id, usuario_nome, acao, entidade, entidade_id, dados_anteriores, dados_novos, ip, user_agent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                session["user_id"],
                session.get("nome_completo", session["usuario"]),
                acao,
                entidade,
                entidade_id,
                dados_anteriores,
                dados_novos,
                request.remote_addr,
                request.headers.get('User-Agent', '')[:500]
            ))
        else:
            cursor.execute("""
                INSERT INTO logs_auditoria 
                (usuario_id, usuario_nome, acao, entidade, entidade_id, dados_anteriores, dados_novos, ip, user_agent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session["user_id"],
                session.get("nome_completo", session["usuario"]),
                acao,
                entidade,
                entidade_id,
                dados_anteriores,
                dados_novos,
                request.remote_addr,
                request.headers.get('User-Agent', '')[:500]
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Erro ao registrar log: {e}")

# =============================
# FUNCOES WHATSAPP
# =============================
def enviar_whatsapp(numero, mensagem, hora=None, minuto=None):
    try:
        import urllib.parse
        import webbrowser
        numero = ''.join(filter(str.isdigit, numero))
        if not numero.startswith('55'):
            numero = '55' + numero
        mensagem_codificada = urllib.parse.quote(mensagem)
        url = f"https://web.whatsapp.com/send?phone={numero}&text={mensagem_codificada}"
        webbrowser.open(url)
        return True
    except Exception as e:
        print(f"Erro ao enviar WhatsApp: {e}")
        return False

def notificar_ausencia_whatsapp(obreiro_id, reuniao_titulo, data_reuniao):
    conn = get_db()
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("SELECT telefone, nome_completo FROM usuarios WHERE id = %s", (obreiro_id,))
    else:
        cursor.execute("SELECT telefone, nome_completo FROM usuarios WHERE id = ?", (obreiro_id,))
    obreiro = cursor.fetchone()
    conn.close()
    if obreiro and obreiro["telefone"]:
        numero = obreiro["telefone"]
        mensagem = f"""NOTIFICACAO DE AUSENCIA

Ola {obreiro["nome_completo"].split()[0]},

Voce foi marcado como AUSENTE na reuniao:
Data: {data_reuniao}
Reuniao: {reuniao_titulo}

Por favor, acesse o sistema para justificar sua ausencia:
http://localhost:5000/reunioes

Atenciosamente,
Sistema Maconico"""
        thread = threading.Thread(target=enviar_whatsapp, args=(numero, mensagem))
        thread.start()
        return True
    return False

def notificar_nova_reuniao_whatsapp(reuniao_id, titulo, data, hora):
    conn = get_db()
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("SELECT id, telefone, nome_completo FROM usuarios WHERE ativo = 1")
    else:
        cursor.execute("SELECT id, telefone, nome_completo FROM usuarios WHERE ativo = 1")
    obreiros = cursor.fetchall()
    conn.close()
    for obreiro in obreiros:
        if obreiro["telefone"]:
            numero = obreiro["telefone"]
            mensagem = f"""NOVA REUNIAO AGENDADA

Ola {obreiro["nome_completo"].split()[0]},

Uma nova reuniao foi agendada:
Titulo: {titulo}
Data: {data}
Horario: {hora}

Confirme sua presenca no sistema:
http://localhost:5000/reunioes/{reuniao_id}

Atenciosamente,
Sistema Maconico"""
            thread = threading.Thread(target=enviar_whatsapp, args=(numero, mensagem))
            thread.start()
            time.sleep(1)

def notificar_comunicado_whatsapp(comunicado_id, titulo, conteudo):
    conn = get_db()
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("SELECT id, telefone, nome_completo FROM usuarios WHERE ativo = 1")
    else:
        cursor.execute("SELECT id, telefone, nome_completo FROM usuarios WHERE ativo = 1")
    obreiros = cursor.fetchall()
    conn.close()
    for obreiro in obreiros:
        if obreiro["telefone"]:
            numero = obreiro["telefone"]
            mensagem = f"""NOVO COMUNICADO

Ola {obreiro["nome_completo"].split()[0]},

{titulo}

{conteudo[:200]}...

Acesse para visualizar completo:
http://localhost:5000/comunicados/{comunicado_id}

Atenciosamente,
Sistema Maconico"""
            thread = threading.Thread(target=enviar_whatsapp, args=(numero, mensagem))
            thread.start()
            time.sleep(1)

# =============================
# INICIALIZACAO DO BANCO (SIMPLIFICADA)
# =============================
def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    # Criar tabelas se não existirem
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario TEXT UNIQUE NOT NULL,
        senha_hash TEXT NOT NULL,
        tipo TEXT NOT NULL,
        data_cadastro TEXT NOT NULL,
        nome_completo TEXT,
        nome_maconico TEXT,
        cim_numero TEXT,
        grau_atual INTEGER DEFAULT 1,
        data_iniciacao DATE,
        data_elevacao DATE,
        data_exaltacao DATE,
        telefone TEXT,
        email TEXT,
        endereco TEXT,
        loja_nome TEXT,
        loja_numero TEXT,
        loja_orient TEXT,
        ativo INTEGER DEFAULT 1
    )
    """)
    
    # Criar admin padrão
    try:
        if IS_POSTGRES:
            cursor.execute("SELECT * FROM usuarios WHERE usuario = %s", ("admin",))
        else:
            cursor.execute("SELECT * FROM usuarios WHERE usuario = ?", ("admin",))
        if not cursor.fetchone():
            senha_hash = generate_password_hash("admin123")
            hoje = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if IS_POSTGRES:
                cursor.execute("""
                    INSERT INTO usuarios (usuario, senha_hash, tipo, data_cadastro, ativo, nome_completo, grau_atual) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, ("admin", senha_hash, "admin", hoje, 1, "Administrador", 3))
            else:
                cursor.execute("""
                    INSERT INTO usuarios (usuario, senha_hash, tipo, data_cadastro, ativo, nome_completo, grau_atual) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, ("admin", senha_hash, "admin", hoje, 1, "Administrador", 3))
    except Exception as e:
        print(f"Erro ao criar admin: {e}")
    
    conn.commit()
    conn.close()
    print("Banco de dados inicializado com sucesso!")

init_db()

# =============================
# ROTA DE LOGIN
# =============================
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form["usuario"]
        senha = request.form["senha"]
        conn = get_db()
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute("SELECT * FROM usuarios WHERE usuario = %s AND ativo = 1", (usuario,))
        else:
            cursor.execute("SELECT * FROM usuarios WHERE usuario = ? AND ativo = 1", (usuario,))
        user = cursor.fetchone()
        conn.close()
        if user and check_password_hash(user["senha_hash"], senha):
            session["usuario"] = user["usuario"]
            session["tipo"] = user["tipo"]
            session["user_id"] = user["id"]
            session["nome_completo"] = user["nome_completo"] or ""
            session["cim_numero"] = user["cim_numero"] or ""
            session["loja_nome"] = user["loja_nome"] or ""
            session["loja_numero"] = user["loja_numero"] or ""
            session["loja_orient"] = user["loja_orient"] or ""
            session["grau_atual"] = user["grau_atual"] or 1
            registrar_log("login", "usuarios", user["id"], dados_novos={"usuario": usuario})
            flash(f"Bem-vindo, {user['nome_completo'] or user['usuario']}!", "success")
            return redirect("/dashboard")
        else:
            flash("Usuario ou senha invalidos", "danger")
    return render_template("login.html")

# =============================
# ROTA DE LOGOUT
# =============================
@app.route("/logout")
def logout():
    registrar_log("logout", "usuarios", session.get("user_id"))
    session.clear()
    flash("Logout realizado com sucesso", "info")
    return redirect("/")

# =============================
# ROTA DO DASHBOARD
# =============================
@app.route("/dashboard")
@login_required
def dashboard():
    conn = get_db()
    cursor = conn.cursor()
    
    # Candidatos
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM candidatos ORDER BY data_criacao DESC")
    else:
        cursor.execute("SELECT * FROM candidatos ORDER BY data_criacao DESC")
    candidatos = cursor.fetchall()
    
    # Sindicantes
    if IS_POSTGRES:
        cursor.execute("""
            SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, ativo 
            FROM usuarios WHERE tipo = 'sindicante' AND ativo = 1 ORDER BY nome_completo
        """)
    else:
        cursor.execute("""
            SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, ativo 
            FROM usuarios WHERE tipo = 'sindicante' AND ativo = 1 ORDER BY nome_completo
        """)
    sindicantes = cursor.fetchall()
    
    # Pareceres conclusivos
    pareceres_conclusivos = []
    try:
        if IS_POSTGRES:
            cursor.execute("""
                SELECT pc.*, c.nome as candidato_nome, u.nome_completo as sindicante_nome
                FROM pareceres_conclusivos pc
                JOIN candidatos c ON pc.candidato_id = c.id
                JOIN usuarios u ON pc.sindicante = u.usuario
                ORDER BY pc.data_envio DESC LIMIT 10
            """)
        else:
            cursor.execute("""
                SELECT pc.*, c.nome as candidato_nome, u.nome_completo as sindicante_nome
                FROM pareceres_conclusivos pc
                JOIN candidatos c ON pc.candidato_id = c.id
                JOIN usuarios u ON pc.sindicante = u.usuario
                ORDER BY pc.data_envio DESC LIMIT 10
            """)
        pareceres_conclusivos = cursor.fetchall()
    except:
        pass
    
    total_sindicantes_ativos = len(sindicantes)
    total_candidatos = len(candidatos)
    
    if session["tipo"] == "admin":
        em_analise = sum(1 for c in candidatos if c["status"] == "Em analise" and not c["fechado"])
        aprovados = sum(1 for c in candidatos if c["status"] == "Aprovado")
        reprovados = sum(1 for c in candidatos if c["status"] == "Reprovado")
        
        pendentes = []
        for c in candidatos:
            if not c["fechado"]:
                if IS_POSTGRES:
                    cursor.execute("SELECT sindicante FROM sindicancias WHERE candidato_id = %s", (c["id"],))
                else:
                    cursor.execute("SELECT sindicante FROM sindicancias WHERE candidato_id = ?", (c["id"],))
                enviados = [r["sindicante"] for r in cursor.fetchall()]
                faltam = [s["usuario"] for s in sindicantes if s["usuario"] not in enviados]
                if faltam:
                    pendentes.append({"candidato": dict(c), "faltam": faltam})
        
        prazo_vencido = []
        for c in candidatos:
            if not c["fechado"] and c["status"] == "Em analise" and c["data_criacao"]:
                try:
                    data_criacao = datetime.strptime(c["data_criacao"], "%Y-%m-%d %H:%M:%S")
                    dias = (datetime.now() - data_criacao).days
                    if dias > 7:
                        prazo_vencido.append(dict(c))
                except:
                    pass
        
        if IS_POSTGRES:
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo IN ('admin', 'sindicante', 'obreiro') AND ativo = 1")
        else:
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo IN ('admin', 'sindicante', 'obreiro') AND ativo = 1")
        total_obreiros = cursor.fetchone()["total"]
        
        if IS_POSTGRES:
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 3 AND ativo = 1")
        else:
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 3 AND ativo = 1")
        mestres = cursor.fetchone()["total"]
        
        if IS_POSTGRES:
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 2 AND ativo = 1")
        else:
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 2 AND ativo = 1")
        companheiros = cursor.fetchone()["total"]
        
        if IS_POSTGRES:
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 1 AND ativo = 1")
        else:
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 1 AND ativo = 1")
        aprendizes = cursor.fetchone()["total"]
        
        if IS_POSTGRES:
            cursor.execute("SELECT COUNT(*) as total FROM reunioes")
        else:
            cursor.execute("SELECT COUNT(*) as total FROM reunioes")
        total_reunioes = cursor.fetchone()["total"]
        
        if IS_POSTGRES:
            cursor.execute("SELECT COUNT(*) as total FROM reunioes WHERE status = 'realizada'")
        else:
            cursor.execute("SELECT COUNT(*) as total FROM reunioes WHERE status = 'realizada'")
        reunioes_realizadas = cursor.fetchone()["total"]
        
        if IS_POSTGRES:
            cursor.execute("SELECT COUNT(*) as total FROM reunioes WHERE status = 'agendada'")
        else:
            cursor.execute("SELECT COUNT(*) as total FROM reunioes WHERE status = 'agendada'")
        reunioes_agendadas = cursor.fetchone()["total"]
        
        if IS_POSTGRES:
            cursor.execute("""
                SELECT id, titulo, data, hora_inicio FROM reunioes 
                WHERE status = 'agendada' AND data >= CURRENT_DATE
                ORDER BY data ASC, hora_inicio ASC LIMIT 5
            """)
        else:
            cursor.execute("""
                SELECT id, titulo, data, hora_inicio FROM reunioes 
                WHERE status = 'agendada' AND data >= date('now')
                ORDER BY data ASC, hora_inicio ASC LIMIT 5
            """)
        proximas_reunioes = cursor.fetchall()
        proxima_reuniao = proximas_reunioes[0] if proximas_reunioes else None
        
    else:
        em_analise = 0
        aprovados = 0
        reprovados = 0
        pendentes = []
        prazo_vencido = []
        total_obreiros = mestres = companheiros = aprendizes = 0
        total_reunioes = reunioes_realizadas = reunioes_agendadas = 0
        proximas_reunioes = []
        proxima_reuniao = None
        
        for c in candidatos:
            if IS_POSTGRES:
                cursor.execute("SELECT parecer FROM sindicancias WHERE candidato_id = %s AND sindicante = %s", (c["id"], session["usuario"]))
            else:
                cursor.execute("SELECT parecer FROM sindicancias WHERE candidato_id = ? AND sindicante = ?", (c["id"], session["usuario"]))
            parecer = cursor.fetchone()
            if parecer:
                if parecer["parecer"] == "positivo":
                    aprovados += 1
                else:
                    reprovados += 1
            elif not c["fechado"]:
                em_analise += 1
    
    conn.close()
    now = datetime.now()
    
    return render_template(
        "dashboard.html", tipo=session["tipo"], total_candidatos=total_candidatos,
        total_sindicantes=total_sindicantes_ativos, total_obreiros=total_obreiros,
        mestres=mestres, companheiros=companheiros, aprendizes=aprendizes,
        total_reunioes=total_reunioes, reunioes_realizadas=reunioes_realizadas,
        reunioes_agendadas=reunioes_agendadas, proximas_reunioes=proximas_reunioes,
        proxima_reuniao=proxima_reuniao, em_analise=em_analise, aprovados=aprovados,
        reprovados=reprovados, pendentes=pendentes, prazo_vencido=prazo_vencido,
        sindicantes=sindicantes, pareceres_conclusivos=pareceres_conclusivos, now=now
    )

# =============================
# ROTA DE PERFIL
# =============================
@app.route("/perfil", methods=["GET", "POST"])
@login_required
def perfil():
    conn = get_db()
    cursor = conn.cursor()
    if request.method == "POST":
        nome_completo = request.form.get("nome_completo", "")
        cim_numero = request.form.get("cim_numero", "")
        loja_nome = request.form.get("loja_nome", "")
        loja_numero = request.form.get("loja_numero", "")
        loja_orient = request.form.get("loja_orient", "")
        
        if IS_POSTGRES:
            cursor.execute("SELECT * FROM usuarios WHERE id = %s", (session["user_id"],))
        else:
            cursor.execute("SELECT * FROM usuarios WHERE id = ?", (session["user_id"],))
        dados_antigos = dict(cursor.fetchone())
        
        if IS_POSTGRES:
            cursor.execute("""
                UPDATE usuarios SET nome_completo = %s, cim_numero = %s, loja_nome = %s, loja_numero = %s, loja_orient = %s
                WHERE id = %s
            """, (nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, session["user_id"]))
        else:
            cursor.execute("""
                UPDATE usuarios SET nome_completo = ?, cim_numero = ?, loja_nome = ?, loja_numero = ?, loja_orient = ?
                WHERE id = ?
            """, (nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, session["user_id"]))
        
        conn.commit()
        session["nome_completo"] = nome_completo
        session["cim_numero"] = cim_numero
        session["loja_nome"] = loja_nome
        session["loja_numero"] = loja_numero
        session["loja_orient"] = loja_orient
        registrar_log("editar", "perfil", session["user_id"], dados_anteriores=dados_antigos, dados_novos={"nome_completo": nome_completo})
        flash("Perfil atualizado com sucesso!", "success")
    
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM usuarios WHERE id = %s", (session["user_id"],))
    else:
        cursor.execute("SELECT * FROM usuarios WHERE id = ?", (session["user_id"],))
    usuario = cursor.fetchone()
    
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM lojas")
    else:
        cursor.execute("SELECT * FROM lojas")
    lojas = cursor.fetchall()
    
    conn.close()
    return render_template("perfil.html", usuario=usuario, lojas=lojas)

# =============================
# ROTA DE OBREIROS
# =============================
@app.route("/obreiros")
@login_required
def listar_obreiros():
    conn = get_db()
    cursor = conn.cursor()
    
    if IS_POSTGRES:
        cursor.execute("SELECT grau_atual FROM usuarios WHERE id = %s", (session["user_id"],))
    else:
        cursor.execute("SELECT grau_atual FROM usuarios WHERE id = ?", (session["user_id"],))
    usuario_logado = cursor.fetchone()
    grau_usuario = usuario_logado["grau_atual"] if usuario_logado else 1
    
    nome = request.args.get('nome', '').strip()
    grau = request.args.get('grau', '')
    cargo = request.args.get('cargo', '')
    loja = request.args.get('loja', '')
    status = request.args.get('status', '')
    
    query = """
        SELECT u.*, l.nome as loja_nome, 
               CASE 
                   WHEN u.grau_atual = 1 THEN 'Aprendiz'
                   WHEN u.grau_atual = 2 THEN 'Companheiro'
                   WHEN u.grau_atual = 3 THEN 'Mestre'
                   ELSE 'Nao informado'
               END as grau_descricao,
               (SELECT COUNT(*) FROM ocupacao_cargos oc WHERE oc.obreiro_id = u.id AND oc.ativo = 1) as total_cargos
        FROM usuarios u
        LEFT JOIN lojas l ON u.loja_nome = l.nome
        WHERE 1=1
    """
    params = []
    
    if nome:
        if IS_POSTGRES:
            query += " AND (u.nome_completo LIKE %s OR u.usuario LIKE %s)"
            params.extend([f"%{nome}%", f"%{nome}%"])
        else:
            query += " AND (u.nome_completo LIKE ? OR u.usuario LIKE ?)"
            params.extend([f"%{nome}%", f"%{nome}%"])
    if grau:
        if IS_POSTGRES:
            query += " AND u.grau_atual = %s"
        else:
            query += " AND u.grau_atual = ?"
        params.append(grau)
    if cargo:
        if IS_POSTGRES:
            query += " AND EXISTS (SELECT 1 FROM ocupacao_cargos oc WHERE oc.obreiro_id = u.id AND oc.cargo_id = %s AND oc.ativo = 1)"
        else:
            query += " AND EXISTS (SELECT 1 FROM ocupacao_cargos oc WHERE oc.obreiro_id = u.id AND oc.cargo_id = ? AND oc.ativo = 1)"
        params.append(cargo)
    if loja:
        if IS_POSTGRES:
            query += " AND u.loja_nome = %s"
        else:
            query += " AND u.loja_nome = ?"
        params.append(loja)
    
    if session["tipo"] == "admin":
        if status:
            if IS_POSTGRES:
                query += " AND u.ativo = %s"
            else:
                query += " AND u.ativo = ?"
            params.append(status)
        else:
            query += " AND u.ativo = 1"
    elif grau_usuario == 3:
        query += " AND u.ativo = 1"
    else:
        if IS_POSTGRES:
            query += " AND u.id = %s"
        else:
            query += " AND u.id = ?"
        params.append(session["user_id"])
    
    query += " ORDER BY u.nome_completo"
    
    if IS_POSTGRES:
        query = query.replace('?', '%s')
    
    cursor.execute(query, params)
    obreiros = cursor.fetchall()
    
    if IS_POSTGRES:
        cursor.execute("SELECT DISTINCT grau_atual FROM usuarios WHERE grau_atual IS NOT NULL ORDER BY grau_atual")
    else:
        cursor.execute("SELECT DISTINCT grau_atual FROM usuarios WHERE grau_atual IS NOT NULL ORDER BY grau_atual")
    graus = cursor.fetchall()
    
    if IS_POSTGRES:
        cursor.execute("SELECT id, nome FROM cargos WHERE ativo = 1 ORDER BY ordem")
    else:
        cursor.execute("SELECT id, nome FROM cargos WHERE ativo = 1 ORDER BY ordem")
    cargos_list = cursor.fetchall()
    
    if IS_POSTGRES:
        cursor.execute("SELECT DISTINCT loja_nome FROM usuarios WHERE loja_nome IS NOT NULL ORDER BY loja_nome")
    else:
        cursor.execute("SELECT DISTINCT loja_nome FROM usuarios WHERE loja_nome IS NOT NULL ORDER BY loja_nome")
    lojas = cursor.fetchall()
    
    conn.close()
    return render_template("obreiros/lista.html", 
                          obreiros=obreiros,
                          graus=graus,
                          cargos=cargos_list,
                          lojas=lojas,
                          filtros={'nome': nome, 'grau': grau, 'cargo': cargo, 'loja': loja, 'status': status},
                          grau_usuario=grau_usuario)

# =============================
# ROTAS DE PRESENÇA E ALERTAS
# =============================
@app.route("/presenca/alertas")
@admin_required
def listar_alertas():
    conn = get_db()
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("""
            SELECT a.*, u.nome_completo, u.grau_atual, ru.nome_completo as resolvido_por_nome
            FROM alertas_presenca a 
            JOIN usuarios u ON a.obreiro_id = u.id 
            LEFT JOIN usuarios ru ON a.resolvido_por = ru.id
            WHERE a.resolvido = 0 
            ORDER BY a.data_gerado DESC
        """)
    else:
        cursor.execute("""
            SELECT a.*, u.nome_completo, u.grau_atual, ru.nome_completo as resolvido_por_nome
            FROM alertas_presenca a 
            JOIN usuarios u ON a.obreiro_id = u.id 
            LEFT JOIN usuarios ru ON a.resolvido_por = ru.id
            WHERE a.resolvido = 0 
            ORDER BY a.data_gerado DESC
        """)
    alertas = cursor.fetchall()
    conn.close()
    return render_template("presenca/alertas.html", alertas=alertas)

@app.route("/presenca/estatisticas")
@login_required
def estatisticas_presenca():
    conn = get_db()
    cursor = conn.cursor()
    ano = request.args.get('ano', datetime.now().year)
    
    if IS_POSTGRES:
        cursor.execute("""
            SELECT u.id, u.nome_completo, u.grau_atual, COUNT(r.id) as total_reunioes,
                   SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presencas,
                   SUM(CASE WHEN p.presente = 0 AND p.tipo_ausencia IS NOT NULL THEN 1 ELSE 0 END) as ausencias_justificadas,
                   SUM(CASE WHEN p.presente = 0 AND p.tipo_ausencia IS NULL THEN 1 ELSE 0 END) as ausencias_injustificadas
            FROM usuarios u 
            LEFT JOIN presenca p ON u.id = p.obreiro_id 
            LEFT JOIN reunioes r ON p.reuniao_id = r.id AND EXTRACT(YEAR FROM r.data) = %s
            WHERE u.ativo = 1 
            GROUP BY u.id 
            ORDER BY u.grau_atual DESC, u.nome_completo
        """, (str(ano),))
    else:
        cursor.execute("""
            SELECT u.id, u.nome_completo, u.grau_atual, COUNT(r.id) as total_reunioes,
                   SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presencas,
                   SUM(CASE WHEN p.presente = 0 AND p.tipo_ausencia IS NOT NULL THEN 1 ELSE 0 END) as ausencias_justificadas,
                   SUM(CASE WHEN p.presente = 0 AND p.tipo_ausencia IS NULL THEN 1 ELSE 0 END) as ausencias_injustificadas
            FROM usuarios u 
            LEFT JOIN presenca p ON u.id = p.obreiro_id 
            LEFT JOIN reunioes r ON p.reuniao_id = r.id AND strftime('%Y', r.data) = ?
            WHERE u.ativo = 1 
            GROUP BY u.id 
            ORDER BY u.grau_atual DESC, u.nome_completo
        """, (str(ano),))
    rows = cursor.fetchall()
    estatisticas = [dict(row) for row in rows]
    
    if IS_POSTGRES:
        cursor.execute("""
            SELECT EXTRACT(MONTH FROM r.data) as mes, COUNT(*) as total_reunioes, 
                   SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presencas
            FROM reunioes r 
            LEFT JOIN presenca p ON r.id = p.reuniao_id
            WHERE EXTRACT(YEAR FROM r.data) = ? AND r.status = 'realizada' 
            GROUP BY mes 
            ORDER BY mes
        """, (str(ano),))
    else:
        cursor.execute("""
            SELECT strftime('%m', r.data) as mes, COUNT(*) as total_reunioes, 
                   SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presencas
            FROM reunioes r 
            LEFT JOIN presenca p ON r.id = p.reuniao_id
            WHERE strftime('%Y', r.data) = ? AND r.status = 'realizada' 
            GROUP BY mes 
            ORDER BY mes
        """, (str(ano),))
    mensal_rows = cursor.fetchall()
    mensal = [dict(row) for row in mensal_rows]
    
    conn.close()
    anos = range(2020, datetime.now().year + 1)
    return render_template("presenca/estatisticas.html", 
                          estatisticas=estatisticas, 
                          mensal=mensal, 
                          ano=ano, 
                          anos=anos)

@app.route("/presenca/justificar/<int:id>", methods=["GET", "POST"])
@login_required
def justificar_ausencia(id):
    conn = get_db()
    cursor = conn.cursor()
    
    if request.method == "POST":
        tipo_ausencia = request.form.get("tipo_ausencia")
        justificativa = request.form.get("justificativa")
        
        if IS_POSTGRES:
            cursor.execute("""
                UPDATE presenca SET tipo_ausencia = %s, justificativa = %s, data_registro = CURRENT_TIMESTAMP 
                WHERE id = %s
            """, (tipo_ausencia, justificativa, id))
        else:
            cursor.execute("""
                UPDATE presenca SET tipo_ausencia = ?, justificativa = ?, data_registro = CURRENT_TIMESTAMP 
                WHERE id = ?
            """, (tipo_ausencia, justificativa, id))
        conn.commit()
        
        if IS_POSTGRES:
            cursor.execute("SELECT reuniao_id FROM presenca WHERE id = %s", (id,))
        else:
            cursor.execute("SELECT reuniao_id FROM presenca WHERE id = ?", (id,))
        presenca = cursor.fetchone()
        reuniao_id = presenca["reuniao_id"] if presenca else None
        registrar_log("justificar_ausencia", "presenca", id, dados_novos={"tipo_ausencia": tipo_ausencia})
        conn.close()
        flash("Ausencia justificada com sucesso!", "success")
        return redirect(f"/reunioes/{reuniao_id}")
    
    if IS_POSTGRES:
        cursor.execute("""
            SELECT p.*, r.titulo, r.data as reuniao_data, r.hora_inicio, u.nome_completo, u.id as obreiro_id
            FROM presenca p 
            JOIN reunioes r ON p.reuniao_id = r.id 
            JOIN usuarios u ON p.obreiro_id = u.id 
            WHERE p.id = %s
        """, (id,))
    else:
        cursor.execute("""
            SELECT p.*, r.titulo, r.data as reuniao_data, r.hora_inicio, u.nome_completo, u.id as obreiro_id
            FROM presenca p 
            JOIN reunioes r ON p.reuniao_id = r.id 
            JOIN usuarios u ON p.obreiro_id = u.id 
            WHERE p.id = ?
        """, (id,))
    presenca = cursor.fetchone()
    if not presenca:
        flash("Registro de presenca nao encontrado", "danger")
        return redirect("/reunioes")
    
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM tipos_ausencia WHERE ativo = 1")
    else:
        cursor.execute("SELECT * FROM tipos_ausencia WHERE ativo = 1")
    tipos_ausencia = cursor.fetchall()
    conn.close()
    return render_template("presenca/justificar.html", 
                          presenca=presenca, 
                          tipos_ausencia=tipos_ausencia)

@app.route("/presenca/validar/<int:id>", methods=["POST"])
@admin_required
def validar_ausencia(id):
    conn = get_db()
    cursor = conn.cursor()
    validar = request.form.get("validar") == "true"
    observacao = request.form.get("observacao", "")
    
    if validar:
        if IS_POSTGRES:
            cursor.execute("""
                UPDATE presenca 
                SET validado_por = %s, data_validacao = CURRENT_TIMESTAMP, observacao_validacao = %s 
                WHERE id = %s
            """, (session["user_id"], observacao, id))
        else:
            cursor.execute("""
                UPDATE presenca 
                SET validado_por = ?, data_validacao = CURRENT_TIMESTAMP, observacao_validacao = ? 
                WHERE id = ?
            """, (session["user_id"], observacao, id))
        registrar_log("validar_ausencia", "presenca", id, dados_novos={"validado": True})
        flash("Ausencia validada com sucesso!", "success")
    else:
        if IS_POSTGRES:
            cursor.execute("""
                UPDATE presenca 
                SET tipo_ausencia = NULL, justificativa = NULL, validado_por = NULL, 
                    data_validacao = NULL, observacao_validacao = %s 
                WHERE id = %s
            """, (observacao, id))
        else:
            cursor.execute("""
                UPDATE presenca 
                SET tipo_ausencia = NULL, justificativa = NULL, validado_por = NULL, 
                    data_validacao = NULL, observacao_validacao = ? 
                WHERE id = ?
            """, (observacao, id))
        registrar_log("rejeitar_ausencia", "presenca", id)
        flash("Validacao removida!", "success")
    
    conn.commit()
    conn.close()
    return redirect(request.referrer or "/reunioes")

@app.route("/presenca/alerta/<int:id>/resolver", methods=["POST"])
@admin_required
def resolver_alerta(id):
    conn = get_db()
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("""
            UPDATE alertas_presenca 
            SET resolvido = 1, data_resolucao = CURRENT_TIMESTAMP, resolvido_por = %s 
            WHERE id = %s
        """, (session["user_id"], id))
    else:
        cursor.execute("""
            UPDATE alertas_presenca 
            SET resolvido = 1, data_resolucao = CURRENT_TIMESTAMP, resolvido_por = ? 
            WHERE id = ?
        """, (session["user_id"], id))
    conn.commit()
    registrar_log("resolver_alerta", "alerta", id)
    conn.close()
    flash("Alerta marcado como resolvido", "success")
    return redirect("/presenca/alertas")

@app.route("/api/gerar_alertas")
@admin_required
def gerar_alertas():
    conn = get_db()
    cursor = conn.cursor()
    mes_atual = datetime.now().strftime('%Y-%m')
    ano_atual = datetime.now().year
    
    if IS_POSTGRES:
        cursor.execute("""
            SELECT p.obreiro_id, u.nome_completo, COUNT(*) as ausencias, %s as mes
            FROM presenca p 
            JOIN usuarios u ON p.obreiro_id = u.id 
            JOIN reunioes r ON p.reuniao_id = r.id
            WHERE p.presente = 0 AND p.tipo_ausencia IS NULL AND r.status = 'realizada' 
              AND TO_CHAR(r.data, 'YYYY-MM') = %s
            GROUP BY p.obreiro_id 
            HAVING COUNT(*) >= 3
        """, (mes_atual, mes_atual))
    else:
        cursor.execute("""
            SELECT p.obreiro_id, u.nome_completo, COUNT(*) as ausencias, ? as mes
            FROM presenca p 
            JOIN usuarios u ON p.obreiro_id = u.id 
            JOIN reunioes r ON p.reuniao_id = r.id
            WHERE p.presente = 0 AND p.tipo_ausencia IS NULL AND r.status = 'realizada' 
              AND strftime('%Y-%m', r.data) = ?
            GROUP BY p.obreiro_id 
            HAVING COUNT(*) >= 3
        """, (mes_atual, mes_atual))
    alertas_ausencias = cursor.fetchall()
    
    for a in alertas_ausencias:
        if IS_POSTGRES:
            cursor.execute("""
                INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                VALUES (%s, %s, %s)
            """, (a["obreiro_id"], "limite_atingido", f"{a['nome_completo']} possui {a['ausencias']} ausencias injustificadas em {a['mes']}"))
        else:
            cursor.execute("""
                INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                VALUES (?, ?, ?)
            """, (a["obreiro_id"], "limite_atingido", f"{a['nome_completo']} possui {a['ausencias']} ausencias injustificadas em {a['mes']}"))
    
    if IS_POSTGRES:
        cursor.execute("""
            SELECT u.id, u.nome_completo, COUNT(r.id) as total_reunioes, 
                   SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presencas
            FROM usuarios u 
            LEFT JOIN presenca p ON u.id = p.obreiro_id 
            LEFT JOIN reunioes r ON p.reuniao_id = r.id AND EXTRACT(YEAR FROM r.data) = %s
            WHERE u.ativo = 1 
            GROUP BY u.id 
            HAVING COUNT(r.id) > 0
        """, (str(ano_atual),))
    else:
        cursor.execute("""
            SELECT u.id, u.nome_completo, COUNT(r.id) as total_reunioes, 
                   SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presencas
            FROM usuarios u 
            LEFT JOIN presenca p ON u.id = p.obreiro_id 
            LEFT JOIN reunioes r ON p.reuniao_id = r.id AND strftime('%Y', r.data) = ?
            WHERE u.ativo = 1 
            GROUP BY u.id 
            HAVING COUNT(r.id) > 0
        """, (str(ano_atual),))
    estatisticas = cursor.fetchall()
    
    for e in estatisticas:
        total = e["total_reunioes"]
        presencas = e["presencas"] or 0
        percentual = (presencas / total) * 100
        
        if percentual < 50:
            if IS_POSTGRES:
                cursor.execute("""
                    INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                    VALUES (%s, %s, %s)
                """, (e["id"], "presenca_critica", f"{e['nome_completo']} tem apenas {percentual:.1f}% de presenca no ano {ano_atual} (CRITICO)"))
            else:
                cursor.execute("""
                    INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                    VALUES (?, ?, ?)
                """, (e["id"], "presenca_critica", f"{e['nome_completo']} tem apenas {percentual:.1f}% de presenca no ano {ano_atual} (CRITICO)"))
        elif percentual < 75:
            if IS_POSTGRES:
                cursor.execute("""
                    INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                    VALUES (%s, %s, %s)
                """, (e["id"], "presenca_atencao", f"{e['nome_completo']} tem {percentual:.1f}% de presenca no ano {ano_atual} (ATENCAO)"))
            else:
                cursor.execute("""
                    INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                    VALUES (?, ?, ?)
                """, (e["id"], "presenca_atencao", f"{e['nome_completo']} tem {percentual:.1f}% de presenca no ano {ano_atual} (ATENCAO)"))
    
    conn.commit()
    registrar_log("gerar_alertas", "alertas", None, dados_novos={"quantidade": len(alertas_ausencias)})
    conn.close()
    flash(f"Alertas gerados! ({len(alertas_ausencias)} por ausencias + alertas de presenca)", "success")
    return redirect("/presenca/alertas")

# =============================
# ROTAS DE TIPOS DE AUSENCIA
# =============================
@app.route("/tipos_ausencia")
@admin_required
def listar_tipos_ausencia():
    conn = get_db()
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM tipos_ausencia ORDER BY nome")
    else:
        cursor.execute("SELECT * FROM tipos_ausencia ORDER BY nome")
    tipos = cursor.fetchall()
    conn.close()
    return render_template("presenca/tipos_ausencia.html", tipos=tipos)

@app.route("/tipos_ausencia/novo", methods=["GET", "POST"])
@admin_required
def novo_tipo_ausencia():
    if request.method == "POST":
        nome = request.form.get("nome")
        descricao = request.form.get("descricao")
        requer_comprovante = 1 if request.form.get("requer_comprovante") else 0
        cor = request.form.get("cor", "#6c757d")
        if not nome:
            flash("Nome e obrigatorio", "danger")
        else:
            conn = get_db()
            cursor = conn.cursor()
            if IS_POSTGRES:
                cursor.execute("""
                    INSERT INTO tipos_ausencia (nome, descricao, requer_comprovante, cor, ativo) 
                    VALUES (%s, %s, %s, %s, %s)
                """, (nome, descricao, requer_comprovante, cor, 1))
            else:
                cursor.execute("""
                    INSERT INTO tipos_ausencia (nome, descricao, requer_comprovante, cor, ativo) 
                    VALUES (?, ?, ?, ?, 1)
                """, (nome, descricao, requer_comprovante, cor))
            conn.commit()
            tipo_id = cursor.lastrowid
            registrar_log("criar", "tipo_ausencia", tipo_id, dados_novos={"nome": nome})
            conn.close()
            flash(f"Tipo de ausencia '{nome}' adicionado com sucesso!", "success")
            return redirect("/tipos_ausencia")
    return render_template("presenca/tipo_ausencia_form.html")

@app.route("/tipos_ausencia/editar/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_tipo_ausencia(id):
    conn = get_db()
    cursor = conn.cursor()
    if request.method == "POST":
        nome = request.form.get("nome")
        descricao = request.form.get("descricao")
        requer_comprovante = 1 if request.form.get("requer_comprovante") else 0
        cor = request.form.get("cor", "#6c757d")
        ativo = 1 if request.form.get("ativo") else 0
        if IS_POSTGRES:
            cursor.execute("SELECT * FROM tipos_ausencia WHERE id = %s", (id,))
        else:
            cursor.execute("SELECT * FROM tipos_ausencia WHERE id = ?", (id,))
        dados_antigos = dict(cursor.fetchone())
        if IS_POSTGRES:
            cursor.execute("""
                UPDATE tipos_ausencia SET nome = %s, descricao = %s, requer_comprovante = %s, cor = %s, ativo = %s 
                WHERE id = %s
            """, (nome, descricao, requer_comprovante, cor, ativo, id))
        else:
            cursor.execute("""
                UPDATE tipos_ausencia SET nome = ?, descricao = ?, requer_comprovante = ?, cor = ?, ativo = ? 
                WHERE id = ?
            """, (nome, descricao, requer_comprovante, cor, ativo, id))
        conn.commit()
        registrar_log("editar", "tipo_ausencia", id, dados_anteriores=dados_antigos, dados_novos={"nome": nome})
        flash("Tipo de ausencia atualizado com sucesso!", "success")
        return redirect("/tipos_ausencia")
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM tipos_ausencia WHERE id = %s", (id,))
    else:
        cursor.execute("SELECT * FROM tipos_ausencia WHERE id = ?", (id,))
    tipo = cursor.fetchone()
    conn.close()
    if not tipo:
        flash("Tipo de ausencia nao encontrado", "danger")
        return redirect("/tipos_ausencia")
    return render_template("presenca/tipo_ausencia_form.html", tipo=tipo)

@app.route("/tipos_ausencia/excluir/<int:id>")
@admin_required
def excluir_tipo_ausencia(id):
    conn = get_db()
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM tipos_ausencia WHERE id = %s", (id,))
    else:
        cursor.execute("SELECT * FROM tipos_ausencia WHERE id = ?", (id,))
    dados = dict(cursor.fetchone()) if cursor.fetchone() else None
    if IS_POSTGRES:
        cursor.execute("SELECT COUNT(*) as total FROM presenca WHERE tipo_ausencia = (SELECT nome FROM tipos_ausencia WHERE id = %s)", (id,))
    else:
        cursor.execute("SELECT COUNT(*) as total FROM presenca WHERE tipo_ausencia = (SELECT nome FROM tipos_ausencia WHERE id = ?)", (id,))
    resultado = cursor.fetchone()
    if resultado and resultado["total"] > 0:
        if IS_POSTGRES:
            cursor.execute("UPDATE tipos_ausencia SET ativo = 0 WHERE id = %s", (id,))
        else:
            cursor.execute("UPDATE tipos_ausencia SET ativo = 0 WHERE id = ?", (id,))
        flash("Tipo de ausencia desativado pois esta em uso.", "warning")
    else:
        if IS_POSTGRES:
            cursor.execute("DELETE FROM tipos_ausencia WHERE id = %s", (id,))
        else:
            cursor.execute("DELETE FROM tipos_ausencia WHERE id = ?", (id,))
        registrar_log("excluir", "tipo_ausencia", id, dados_anteriores=dados)
        flash("Tipo de ausencia excluido com sucesso!", "success")
    conn.commit()
    conn.close()
    return redirect("/tipos_ausencia")

# =============================
# ROTAS DE BACKUP E RESTAURACAO
# =============================
@app.route("/backup")
@admin_required
def backup_banco():
    db_path = "banco.db"
    if not os.path.exists(db_path):
        flash("Arquivo do banco de dados nao encontrado.", "danger")
        return redirect("/dashboard")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"backup_banco_{timestamp}.db"
    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, backup_filename)
    shutil.copy2(db_path, temp_path)
    registrar_log("backup", "banco", None, dados_novos={"arquivo": backup_filename})
    response = send_file(temp_path, as_attachment=True, download_name=backup_filename, mimetype="application/x-sqlite3")
    def remove_file():
        try:
            os.remove(temp_path)
        except:
            pass
    response.call_on_close(remove_file)
    return response

@app.route("/restaurar")
@admin_required
def restaurar_page():
    return render_template("backup/restaurar.html")

@app.route("/restaurar", methods=["POST"])
@admin_required
def restaurar_banco():
    if 'backup_file' not in request.files:
        flash("Nenhum arquivo selecionado", "danger")
        return redirect("/restaurar")
    file = request.files['backup_file']
    if file.filename == '':
        flash("Nenhum arquivo selecionado", "danger")
        return redirect("/restaurar")
    if not allowed_file_backup(file.filename):
        flash("Formato de arquivo nao permitido. Use .db, .sqlite ou .sqlite3", "danger")
        return redirect("/restaurar")
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_atual = f"backup_antes_restauracao_{timestamp}.db"
        backup_path = os.path.join(BACKUP_DIR, backup_atual)
        if os.path.exists("banco.db"):
            shutil.copy2("banco.db", backup_path)
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), f"restore_temp_{timestamp}.db")
        file.save(temp_path)
        try:
            test_conn = sqlite3.connect(temp_path)
            test_conn.cursor().execute("SELECT name FROM sqlite_master WHERE type='table'")
            test_conn.close()
        except sqlite3.DatabaseError:
            os.remove(temp_path)
            flash("Arquivo invalido: nao e um banco de dados SQLite valido", "danger")
            return redirect("/restaurar")
        shutil.copy2(temp_path, "banco.db")
        os.remove(temp_path)
        registrar_log("restaurar", "banco", None, dados_novos={"arquivo": file.filename, "backup_anterior": backup_atual})
        flash(f"Banco de dados restaurado com sucesso! Backup do banco anterior salvo como: {backup_atual}", "success")
        return redirect("/dashboard")
    except Exception as e:
        flash(f"Erro ao restaurar banco de dados: {str(e)}", "danger")
        return redirect("/restaurar")

# =============================
# ROTAS WHATSAPP
# =============================
@app.route("/config/whatsapp", methods=["GET", "POST"])
@admin_required
def config_whatsapp():
    conn = get_db()
    cursor = conn.cursor()
    if request.method == "POST":
        notificar_ausencia = 1 if request.form.get("notificar_ausencia") else 0
        notificar_nova_reuniao = 1 if request.form.get("notificar_nova_reuniao") else 0
        notificar_comunicado = 1 if request.form.get("notificar_comunicado") else 0
        lembrete_reuniao = 1 if request.form.get("lembrete_reuniao") else 0
        grupo_id = request.form.get("grupo_id", "")
        if IS_POSTGRES:
            cursor.execute("""
                UPDATE whatsapp_config SET notificar_ausencia = %s, notificar_nova_reuniao = %s, notificar_comunicado = %s, 
                lembrete_reuniao = %s, grupo_id = %s, updated_at = CURRENT_TIMESTAMP WHERE id = 1
            """, (notificar_ausencia, notificar_nova_reuniao, notificar_comunicado, lembrete_reuniao, grupo_id))
        else:
            cursor.execute("""
                UPDATE whatsapp_config SET notificar_ausencia = ?, notificar_nova_reuniao = ?, notificar_comunicado = ?, 
                lembrete_reuniao = ?, grupo_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = 1
            """, (notificar_ausencia, notificar_nova_reuniao, notificar_comunicado, lembrete_reuniao, grupo_id))
        conn.commit()
        registrar_log("configurar_whatsapp", "config", 1, dados_novos={"notificacoes": "atualizadas", "grupo": grupo_id})
        flash("Configuracoes do WhatsApp salvas com sucesso!", "success")
        return redirect("/config/whatsapp")
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM whatsapp_config WHERE id = 1")
    else:
        cursor.execute("SELECT * FROM whatsapp_config WHERE id = 1")
    config = cursor.fetchone()
    conn.close()
    return render_template("config/whatsapp.html", config=config)

@app.route("/testar_whatsapp", methods=["POST"])
@admin_required
def testar_whatsapp():
    numero = request.form.get("numero")
    mensagem = request.form.get("mensagem")
    if not numero or not mensagem:
        flash("Numero e mensagem sao obrigatorios", "danger")
        return redirect("/config/whatsapp")
    if enviar_whatsapp(numero, mensagem):
        flash("Mensagem enviada com sucesso! Verifique seu WhatsApp.", "success")
        registrar_log("testar_whatsapp", "whatsapp", None, dados_novos={"numero": numero})
    else:
        flash("Erro ao enviar mensagem. Certifique-se de que o WhatsApp Web esta aberto.", "danger")
    return redirect("/config/whatsapp")

# =============================
# ROTAS DE COMUNICADOS
# =============================
@app.route("/comunicados")
@login_required
def listar_comunicados():
    conn = get_db()
    cursor = conn.cursor()
    tipo = request.args.get('tipo', '')
    prioridade = request.args.get('prioridade', '')
    data_ini = request.args.get('data_ini', '')
    data_fim = request.args.get('data_fim', '')
    ativo = request.args.get('ativo', '')
    hoje = datetime.now().strftime("%Y-%m-%d")
    
    query = """
        SELECT c.*, u.nome_completo as autor_nome, 
               (SELECT COUNT(*) FROM visualizacoes_comunicado WHERE comunicado_id = c.id AND obreiro_id = ?) as ja_visto
        FROM comunicados c 
        JOIN usuarios u ON c.criado_por = u.id 
        WHERE 1=1
    """
    params = [session["user_id"]]
    
    if tipo:
        if IS_POSTGRES:
            query += " AND c.tipo = %s"
        else:
            query += " AND c.tipo = ?"
        params.append(tipo)
    if prioridade:
        if IS_POSTGRES:
            query += " AND c.prioridade = %s"
        else:
            query += " AND c.prioridade = ?"
        params.append(prioridade)
    if data_ini:
        if IS_POSTGRES:
            query += " AND c.data_inicio >= %s"
        else:
            query += " AND c.data_inicio >= ?"
        params.append(data_ini)
    if data_fim:
        if IS_POSTGRES:
            query += " AND c.data_fim <= %s"
        else:
            query += " AND c.data_fim <= ?"
        params.append(data_fim)
    if ativo != '':
        if IS_POSTGRES:
            query += " AND c.ativo = %s"
        else:
            query += " AND c.ativo = ?"
        params.append(ativo)
    else:
        if IS_POSTGRES:
            query += " AND c.ativo = 1 AND c.data_inicio <= %s AND (c.data_fim IS NULL OR c.data_fim >= %s)"
        else:
            query += " AND c.ativo = 1 AND c.data_inicio <= ? AND (c.data_fim IS NULL OR c.data_fim >= ?)"
        params.extend([hoje, hoje])
    
    if IS_POSTGRES:
        query = query.replace('?', '%s')
    query += " ORDER BY c.prioridade = 'urgente' DESC, c.data_criacao DESC"
    
    cursor.execute(query, params)
    comunicados = cursor.fetchall()
    
    if IS_POSTGRES:
        cursor.execute("SELECT DISTINCT tipo FROM comunicados ORDER BY tipo")
    else:
        cursor.execute("SELECT DISTINCT tipo FROM comunicados ORDER BY tipo")
    tipos = cursor.fetchall()
    
    if IS_POSTGRES:
        cursor.execute("SELECT DISTINCT prioridade FROM comunicados ORDER BY prioridade")
    else:
        cursor.execute("SELECT DISTINCT prioridade FROM comunicados ORDER BY prioridade")
    prioridades = cursor.fetchall()
    
    conn.close()
    return render_template("comunicados/lista.html", 
                          comunicados=comunicados, 
                          tipos=tipos, 
                          prioridades=prioridades,
                          filtros={'tipo': tipo, 'prioridade': prioridade, 'data_ini': data_ini, 'data_fim': data_fim, 'ativo': ativo})

@app.route("/comunicados/novo", methods=["GET", "POST"])
@admin_required
def novo_comunicado():
    if request.method == "POST":
        titulo = request.form.get("titulo")
        conteudo = request.form.get("conteudo")
        tipo = request.form.get("tipo")
        prioridade = request.form.get("prioridade")
        data_inicio = request.form.get("data_inicio")
        data_fim = request.form.get("data_fim") or None
        conn = get_db()
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute("""
                INSERT INTO comunicados (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, criado_por)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, session["user_id"]))
        else:
            cursor.execute("""
                INSERT INTO comunicados (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, criado_por)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, session["user_id"]))
        conn.commit()
        comunicado_id = cursor.lastrowid
        registrar_log("criar", "comunicado", comunicado_id, dados_novos={"titulo": titulo, "prioridade": prioridade})
        
        conn2 = get_db()
        cur2 = conn2.cursor()
        if IS_POSTGRES:
            cur2.execute("SELECT notificar_comunicado FROM whatsapp_config WHERE id = 1")
        else:
            cur2.execute("SELECT notificar_comunicado FROM whatsapp_config WHERE id = 1")
        config = cur2.fetchone()
        conn2.close()
        if config and config["notificar_comunicado"] == 1:
            notificar_comunicado_whatsapp(comunicado_id, titulo, conteudo)
        
        flash("Comunicado publicado com sucesso!", "success")
        return redirect("/comunicados")
    hoje = datetime.now().strftime("%Y-%m-%d")
    return render_template("comunicados/novo.html", hoje=hoje)

@app.route("/comunicados/<int:id>/visualizar")
@login_required
def visualizar_comunicado(id):
    conn = get_db()
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("""
            INSERT OR IGNORE INTO visualizacoes_comunicado (comunicado_id, obreiro_id) 
            VALUES (%s, %s)
        """, (id, session["user_id"]))
    else:
        cursor.execute("""
            INSERT OR IGNORE INTO visualizacoes_comunicado (comunicado_id, obreiro_id) 
            VALUES (?, ?)
        """, (id, session["user_id"]))
    conn.commit()
    
    if IS_POSTGRES:
        cursor.execute("""
            SELECT c.*, u.nome_completo as autor_nome 
            FROM comunicados c 
            JOIN usuarios u ON c.criado_por = u.id 
            WHERE c.id = %s
        """, (id,))
    else:
        cursor.execute("""
            SELECT c.*, u.nome_completo as autor_nome 
            FROM comunicados c 
            JOIN usuarios u ON c.criado_por = u.id 
            WHERE c.id = ?
        """, (id,))
    comunicado = cursor.fetchone()
    conn.close()
    if not comunicado:
        flash("Comunicado nao encontrado", "danger")
        return redirect("/comunicados")
    return render_template("comunicados/detalhes.html", comunicado=comunicado)

@app.route("/comunicados/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_comunicado(id):
    conn = get_db()
    cursor = conn.cursor()
    if request.method == "POST":
        titulo = request.form.get("titulo")
        conteudo = request.form.get("conteudo")
        tipo = request.form.get("tipo")
        prioridade = request.form.get("prioridade")
        data_inicio = request.form.get("data_inicio")
        data_fim = request.form.get("data_fim") or None
        ativo = 1 if request.form.get("ativo") else 0
        if IS_POSTGRES:
            cursor.execute("SELECT * FROM comunicados WHERE id = %s", (id,))
        else:
            cursor.execute("SELECT * FROM comunicados WHERE id = ?", (id,))
        dados_antigos = dict(cursor.fetchone())
        if IS_POSTGRES:
            cursor.execute("""
                UPDATE comunicados SET titulo=%s, conteudo=%s, tipo=%s, prioridade=%s, 
                data_inicio=%s, data_fim=%s, ativo=%s WHERE id=%s
            """, (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, ativo, id))
        else:
            cursor.execute("""
                UPDATE comunicados SET titulo=?, conteudo=?, tipo=?, prioridade=?, 
                data_inicio=?, data_fim=?, ativo=? WHERE id=?
            """, (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, ativo, id))
        conn.commit()
        registrar_log("editar", "comunicado", id, dados_anteriores=dados_antigos, dados_novos={"titulo": titulo})
        flash("Comunicado atualizado com sucesso!", "success")
        return redirect("/comunicados")
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM comunicados WHERE id = %s", (id,))
    else:
        cursor.execute("SELECT * FROM comunicados WHERE id = ?", (id,))
    comunicado = cursor.fetchone()
    conn.close()
    return render_template("comunicados/editar.html", comunicado=comunicado)

@app.route("/comunicados/<int:id>/excluir")
@admin_required
def excluir_comunicado(id):
    conn = get_db()
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM comunicados WHERE id = %s", (id,))
    else:
        cursor.execute("SELECT * FROM comunicados WHERE id = ?", (id,))
    dados = dict(cursor.fetchone()) if cursor.fetchone() else None
    if IS_POSTGRES:
        cursor.execute("DELETE FROM comunicados WHERE id = %s", (id,))
    else:
        cursor.execute("DELETE FROM comunicados WHERE id = ?", (id,))
    conn.commit()
    registrar_log("excluir", "comunicado", id, dados_anteriores=dados)
    flash("Comunicado excluido com sucesso!", "success")
    conn.close()
    return redirect("/comunicados")

# =============================
# ROTAS DE SUGESTOES
# =============================
@app.route("/sugestoes")
@login_required
def listar_sugestoes():
    conn = get_db()
    cursor = conn.cursor()
    
    categoria = request.args.get('categoria', '')
    status = request.args.get('status', '')
    prioridade = request.args.get('prioridade', '')
    
    query = """
        SELECT s.*, u.nome_completo as autor_nome,
               (SELECT COUNT(*) FROM comentarios_sugestao WHERE sugestao_id = s.id) as total_comentarios
        FROM sugestoes s
        JOIN usuarios u ON s.autor_id = u.id
        WHERE 1=1
    """
    params = []
    
    if categoria:
        if IS_POSTGRES:
            query += " AND s.categoria = %s"
        else:
            query += " AND s.categoria = ?"
        params.append(categoria)
    if status:
        if IS_POSTGRES:
            query += " AND s.status = %s"
        else:
            query += " AND s.status = ?"
        params.append(status)
    if prioridade:
        if IS_POSTGRES:
            query += " AND s.prioridade = %s"
        else:
            query += " AND s.prioridade = ?"
        params.append(prioridade)
    
    if IS_POSTGRES:
        query = query.replace('?', '%s')
    query += " ORDER BY s.prioridade = 'alta' DESC, s.votos DESC, s.data_criacao DESC"
    
    cursor.execute(query, params)
    sugestoes = cursor.fetchall()
    
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM categorias_sugestoes WHERE ativo = 1 ORDER BY nome")
    else:
        cursor.execute("SELECT * FROM categorias_sugestoes WHERE ativo = 1 ORDER BY nome")
    categorias = cursor.fetchall()
    
    conn.close()
    
    return render_template("sugestoes/lista.html", 
                          sugestoes=sugestoes, 
                          categorias=categorias,
                          filtros={'categoria': categoria, 'status': status, 'prioridade': prioridade})

# =============================
# ROTAS DE REUNIOES
# =============================
@app.route("/reunioes")
@login_required
def listar_reunioes():
    conn = get_db()
    cursor = conn.cursor()
    
    data_ini = request.args.get('data_ini', '')
    data_fim = request.args.get('data_fim', '')
    tipo = request.args.get('tipo', '')
    status = request.args.get('status', '')
    grau = request.args.get('grau', '')
    local = request.args.get('local', '')
    
    query = """
        SELECT r.*, l.nome as loja_nome, t.cor,
               COUNT(p.id) as total_presentes,
               (SELECT COUNT(*) FROM presenca WHERE reuniao_id = r.id AND presente = 1) as presentes_confirmados
        FROM reunioes r
        LEFT JOIN lojas l ON r.loja_id = l.id
        LEFT JOIN tipos_reuniao t ON r.tipo = t.nome
        LEFT JOIN presenca p ON r.id = p.reuniao_id
        WHERE 1=1
    """
    params = []
    
    if data_ini:
        if IS_POSTGRES:
            query += " AND r.data >= %s"
        else:
            query += " AND r.data >= ?"
        params.append(data_ini)
    if data_fim:
        if IS_POSTGRES:
            query += " AND r.data <= %s"
        else:
            query += " AND r.data <= ?"
        params.append(data_fim)
    if tipo:
        if IS_POSTGRES:
            query += " AND r.tipo = %s"
        else:
            query += " AND r.tipo = ?"
        params.append(tipo)
    if status:
        if IS_POSTGRES:
            query += " AND r.status = %s"
        else:
            query += " AND r.status = ?"
        params.append(status)
    if grau:
        if IS_POSTGRES:
            query += " AND r.grau = %s"
        else:
            query += " AND r.grau = ?"
        params.append(grau)
    if local:
        if IS_POSTGRES:
            query += " AND r.local LIKE %s"
        else:
            query += " AND r.local LIKE ?"
        params.append(f"%{local}%")
    
    if IS_POSTGRES:
        query = query.replace('?', '%s')
    query += " GROUP BY r.id ORDER BY r.data DESC, r.hora_inicio DESC"
    
    cursor.execute(query, params)
    reunioes = cursor.fetchall()
    
    if IS_POSTGRES:
        cursor.execute("SELECT DISTINCT tipo FROM reunioes ORDER BY tipo")
    else:
        cursor.execute("SELECT DISTINCT tipo FROM reunioes ORDER BY tipo")
    tipos = cursor.fetchall()
    
    if IS_POSTGRES:
        cursor.execute("SELECT DISTINCT status FROM reunioes ORDER BY status")
    else:
        cursor.execute("SELECT DISTINCT status FROM reunioes ORDER BY status")
    status_list = cursor.fetchall()
    
    if IS_POSTGRES:
        cursor.execute("SELECT DISTINCT grau FROM reunioes WHERE grau IS NOT NULL ORDER BY grau")
    else:
        cursor.execute("SELECT DISTINCT grau FROM reunioes WHERE grau IS NOT NULL ORDER BY grau")
    graus = cursor.fetchall()
    
    conn.close()
    return render_template("reunioes/lista.html", reunioes=reunioes, tipos=tipos, status_list=status_list, graus=graus, filtros={'data_ini': data_ini, 'data_fim': data_fim, 'tipo': tipo, 'status': status, 'grau': grau, 'local': local})

@app.route("/reunioes/<int:id>")
@login_required
def detalhes_reuniao(id):
    conn = get_db()
    cursor = conn.cursor()
    if IS_POSTGRES:
        cursor.execute("""
            SELECT r.*, l.nome as loja_nome, t.cor as tipo_cor, u.nome_completo as criado_por_nome
            FROM reunioes r 
            LEFT JOIN lojas l ON r.loja_id = l.id 
            LEFT JOIN tipos_reuniao t ON r.tipo = t.nome 
            LEFT JOIN usuarios u ON r.criado_por = u.id 
            WHERE r.id = %s
        """, (id,))
    else:
        cursor.execute("""
            SELECT r.*, l.nome as loja_nome, t.cor as tipo_cor, u.nome_completo as criado_por_nome
            FROM reunioes r 
            LEFT JOIN lojas l ON r.loja_id = l.id 
            LEFT JOIN tipos_reuniao t ON r.tipo = t.nome 
            LEFT JOIN usuarios u ON r.criado_por = u.id 
            WHERE r.id = ?
        """, (id,))
    reuniao = cursor.fetchone()
    if not reuniao:
        flash("Reuniao nao encontrada", "danger")
        return redirect("/reunioes")
    
    if IS_POSTGRES:
        cursor.execute("SELECT id, aprovada, numero_ata, ano_ata FROM atas WHERE reuniao_id = %s", (id,))
    else:
        cursor.execute("SELECT id, aprovada, numero_ata, ano_ata FROM atas WHERE reuniao_id = ?", (id,))
    ata_row = cursor.fetchone()
    ata_id = ata_row["id"] if ata_row else None
    ata_aprovada = ata_row["aprovada"] if ata_row else None
    ata_numero = ata_row["numero_ata"] if ata_row else None
    ata_ano = ata_row["ano_ata"] if ata_row else None
    
    if IS_POSTGRES:
        cursor.execute("""
            SELECT u.id, u.nome_completo, u.grau_atual, p.id as presenca_id, p.presente, p.tipo_ausencia, 
                   p.justificativa, p.validado_por, p.data_registro, p.comprovante
            FROM usuarios u 
            LEFT JOIN presenca p ON u.id = p.obreiro_id AND p.reuniao_id = %s 
            WHERE u.ativo = 1 
            ORDER BY u.grau_atual DESC, u.nome_completo
        """, (id,))
    else:
        cursor.execute("""
            SELECT u.id, u.nome_completo, u.grau_atual, p.id as presenca_id, p.presente, p.tipo_ausencia, 
                   p.justificativa, p.validado_por, p.data_registro, p.comprovante
            FROM usuarios u 
            LEFT JOIN presenca p ON u.id = p.obreiro_id AND p.reuniao_id = ? 
            WHERE u.ativo = 1 
            ORDER BY u.grau_atual DESC, u.nome_completo
        """, (id,))
    presenca = cursor.fetchall()
    total_obreiros = len(presenca)
    presentes = sum(1 for p in presenca if p["presente"] == 1)
    ausentes = total_obreiros - presentes
    
    conn.close()
    return render_template("reunioes/detalhes.html", reuniao=reuniao, presenca=presenca, total_obreiros=total_obreiros, presentes=presentes, ausentes=ausentes, ata_id=ata_id, ata_aprovada=ata_aprovada, ata_numero=ata_numero, ata_ano=ata_ano)

# =============================
# ROTAS DE CANDIDATOS
# =============================
@app.route("/candidatos", methods=["GET", "POST"])
@admin_required
def gerenciar_candidatos():
    conn = get_db()
    cursor = conn.cursor()
    if request.method == "POST":
        nome = request.form["nome"].strip()
        if nome:
            agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if IS_POSTGRES:
                cursor.execute("INSERT INTO candidatos (nome, data_criacao) VALUES (%s, %s)", (nome, agora))
            else:
                cursor.execute("INSERT INTO candidatos (nome, data_criacao) VALUES (?, ?)", (nome, agora))
            conn.commit()
            candidato_id = cursor.lastrowid
            registrar_log("criar", "candidato", candidato_id, dados_novos={"nome": nome})
            flash(f"Candidato '{nome}' adicionado com sucesso!", "success")
        else:
            flash("Nome do candidato nao pode estar vazio", "danger")
    if IS_POSTGRES:
        cursor.execute("SELECT * FROM candidatos ORDER BY data_criacao DESC")
    else:
        cursor.execute("SELECT * FROM candidatos ORDER BY data_criacao DESC")
    candidatos = cursor.fetchall()
    conn.close()
    return render_template("candidatos.html", candidatos=candidatos, tipo=session["tipo"])

# =============================
# INICIALIZACAO DO SERVIDOR
# =============================
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)