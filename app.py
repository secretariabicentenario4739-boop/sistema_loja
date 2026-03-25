# app.py - Sistema Maçônico com PostgreSQL
# -*- coding: utf-8 -*-

from flask import Flask, render_template, request, redirect, session, flash, jsonify, send_file, after_this_request, Response
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import datetime
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
import webbrowser
from urllib.parse import quote
import threading
import time
from dotenv import load_dotenv
import traceback
import markdown

print("=" * 50)
print("🚀 INICIANDO APLICAÇÃO")
print("=" * 50)
print(f"🔧 DATABASE_URL presente: {'Sim' if os.getenv('DATABASE_URL') else 'Não'}")
print(f"🔧 FLASK_ENV: {os.getenv('FLASK_ENV', 'development')}")
print("=" * 50)

def tratar_valor_nulo(valor, tipo='string'):
    """
    Converte strings vazias para None (NULL no PostgreSQL)
    tipo pode ser: 'string', 'int', 'float', 'date', 'time'
    """
    if valor is None:
        return None
    
    # Se for string e estiver vazia
    if isinstance(valor, str) and valor.strip() == '':
        return None
    
    # Se for string não vazia
    if isinstance(valor, str):
        if tipo == 'int':
            try:
                return int(valor)
            except ValueError:
                return None
        elif tipo == 'float':
            try:
                return float(valor)
            except ValueError:
                return None
        elif tipo == 'date':
            try:
                return datetime.strptime(valor, '%Y-%m-%d').date()
            except ValueError:
                return None
        elif tipo == 'time':
            try:
                return datetime.strptime(valor, '%H:%M').time()
            except ValueError:
                return None
        else:
            return valor.strip()
    
    return valor
def tratar_data_para_sql(data_str):
    """
    Converte string de data para o formato aceito pelo PostgreSQL
    - Strings vazias ou None -> retorna None (NULL no banco)
    - Strings com data válida -> retorna a data no formato YYYY-MM-DD
    """
    if not data_str:
        return None
    data_limpa = str(data_str).strip()
    if not data_limpa or data_limpa == '':
        return None
    return data_limpa
# Carregar variáveis de ambiente
load_dotenv()
# =============================
# DECORATORS DE NÍVEIS DE ACESSO
# =============================

def nivel_required(nivel_minimo):
    """Verifica se o usuário tem o nível mínimo de acesso"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if "usuario" not in session:
                flash("Faça login para acessar esta página", "warning")
                return redirect("/")
            
            nivel_usuario = session.get("nivel_acesso", 1)
            tipo_usuario = session.get("tipo", "obreiro")
            
            # Administradores têm acesso total
            if tipo_usuario == "admin":
                return f(*args, **kwargs)
            
            if nivel_usuario >= nivel_minimo:
                return f(*args, **kwargs)
            else:
                flash("Você não tem permissão para acessar esta página", "danger")
                return redirect("/dashboard")
        return decorated_function
    return decorator

def nivel_ata_required():
    """Verifica se o usuário pode visualizar a ata baseado no grau"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if "usuario" not in session:
                flash("Faça login para acessar esta página", "warning")
                return redirect("/")
            
            ata_id = kwargs.get('id')
            if ata_id:
                cursor, conn = get_db()
                cursor.execute("""
                    SELECT a.*, r.grau as reuniao_grau
                    FROM atas a
                    JOIN reunioes r ON a.reuniao_id = r.id
                    WHERE a.id = %s
                """, (ata_id,))
                ata = cursor.fetchone()
                return_connection(conn)
                
                if ata:
                    reuniao_grau = ata.get('reuniao_grau') or 1
                    nivel_usuario = session.get("nivel_acesso", 1)
                    tipo_usuario = session.get("tipo", "obreiro")
                    
                    # Administradores veem tudo
                    if tipo_usuario == "admin":
                        return f(*args, **kwargs)
                    
                    # Aprendiz (nivel 1) só vê atas de reuniões de aprendiz
                    if nivel_usuario == 1 and reuniao_grau == 1:
                        return f(*args, **kwargs)
                    # Companheiro (nivel 2) vê atas de aprendiz e companheiro
                    elif nivel_usuario == 2 and reuniao_grau <= 2:
                        return f(*args, **kwargs)
                    # Mestre (nivel 3) vê todas as atas
                    elif nivel_usuario >= 3:
                        return f(*args, **kwargs)
                    else:
                        flash("Você não tem permissão para visualizar esta ata", "danger")
                        return redirect("/dashboard")
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# =============================
# CONFIGURAÇÕES
# =============================


# Configuração de uploads
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads', 'documentos')
ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'png', 'gif', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'zip', 'rar'}

# Criar pasta se não existir
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# =============================
# CONEXÃO COM BANCO DE DADOS
# =============================

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# Configuração de conexão
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    # Fallback para desenvolvimento local
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'sistema_maconico')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print(f"⚠️  Usando conexão local: {DB_HOST}:{DB_PORT}/{DB_NAME}")

print(f"🔗 Conectando ao banco...")

def get_db():
    """Retorna uma conexão com o PostgreSQL"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        return cursor, conn
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        raise

def return_connection(conn):
    """Fecha a conexão"""
    if conn:
        conn.close()

def init_db():
    """Testa a conexão com o banco"""
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT 1")
        print("✅ Conexão com PostgreSQL estabelecida!")
        return_connection(conn)
        return True
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return False

# Testar conexão
init_db()

# =============================
# CONTEXTO GLOBAL PARA TEMPLATES
# =============================
@app.context_processor
def inject_global():
    return {'datetime': datetime, 'now': datetime.now()}
    
@app.template_filter('markdown')
def render_markdown(text):
    """Converte markdown para HTML"""
    if not text:
        return ''
    try:
        html = markdown.markdown(
            text, 
            extensions=['extra', 'codehilite', 'tables', 'fenced_code', 'nl2br']
        )
        return html
    except Exception as e:
        print(f"Erro ao converter markdown: {e}")
        return text.replace('\n', '<br>')
# =============================
# SISTEMA DE PERMISSÕES
# =============================

def tem_permissao(permissao_codigo):
    """Verifica se o usuário tem uma permissão específica"""
    if 'user_id' not in session:
        return False
    
    # Admin tem todas as permissões
    if session.get('tipo') == 'admin':
        return True
    
    cursor, conn = get_db()
    
    try:
        # Verificar permissão por grau
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM usuarios u
            JOIN permissoes_grau pg ON u.grau_atual = pg.grau_id
            JOIN permissoes p ON pg.permissao_id = p.id
            WHERE u.id = %s AND p.codigo = %s
        """, (session['user_id'], permissao_codigo))
        
        result = cursor.fetchone()
        
        if result and result['total'] > 0:
            return_connection(conn)
            return True
        
        # Verificar permissão especial por usuário (sobrescreve)
        cursor.execute("""
            SELECT permitido
            FROM permissoes_usuario pu
            JOIN permissoes p ON pu.permissao_id = p.id
            WHERE pu.usuario_id = %s AND p.codigo = %s
        """, (session['user_id'], permissao_codigo))
        
        result = cursor.fetchone()
        
        if result:
            return_connection(conn)
            return result['permitido'] == 1
        
    except Exception as e:
        print(f"Erro ao verificar permissão: {e}")
    
    return_connection(conn)
    return False

def permissao_required(permissao_codigo):
    """Decorator para verificar permissão"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not tem_permissao(permissao_codigo):
                flash("Você não tem permissão para acessar esta página", "danger")
                return redirect("/dashboard")
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# =============================
# DECORATORS DE AUTENTICAÇÃO
# =============================
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "usuario" not in session:
            flash("Faça login para acessar esta página", "warning")
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
# FUNÇÃO DE PERMISSÃO PARA TEMPLATES
# =============================

@app.context_processor
def inject_permissions():
    """Injeta a função de verificação de permissão nos templates"""
    def tem_permissao(codigo):
        if 'user_id' not in session:
            return False
        # Admin tem todas as permissões
        if session.get('tipo') == 'admin':
            return True
        return _verificar_permissao_db(codigo)
    
    return {'tem_permissao': tem_permissao}

def _verificar_permissao_db(codigo):
    """Verifica permissão no banco de dados"""
    try:
        cursor, conn = get_db()
        
        # Verificar permissões especiais do usuário (sobrescreve)
        cursor.execute("""
            SELECT permitido
            FROM permissoes_usuario pu
            JOIN permissoes p ON pu.permissao_id = p.id
            WHERE pu.usuario_id = %s AND p.codigo = %s
        """, (session['user_id'], codigo))
        
        result = cursor.fetchone()
        
        if result:
            return_connection(conn)
            return result['permitido'] == 1
        
        # Verificar por grau - usando o grau_atual da sessão
        grau_atual = session.get('grau_atual', 1)
        
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM permissoes_grau pg
            JOIN permissoes p ON pg.permissao_id = p.id
            WHERE pg.grau_id = %s AND p.codigo = %s
        """, (grau_atual, codigo))
        
        result = cursor.fetchone()
        return_connection(conn)
        return result and result['total'] > 0
        
    except Exception as e:
        print(f"Erro ao verificar permissão: {e}")
        return False

# =============================
# FUNÇÃO DE AUDITORIA
# =============================
def registrar_log(acao, entidade=None, entidade_id=None, dados_anteriores=None, dados_novos=None):
    """Registra uma ação no log de auditoria"""
    if "user_id" not in session:
        return
    
    try:
        cursor, conn = get_db()
        
        if dados_anteriores and isinstance(dados_anteriores, dict):
            dados_anteriores = json.dumps(dados_anteriores, ensure_ascii=False, default=str)
        if dados_novos and isinstance(dados_novos, dict):
            dados_novos = json.dumps(dados_novos, ensure_ascii=False, default=str)
        
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
        conn.commit()
        return_connection(conn)
    except Exception as e:
        print(f"Erro ao registrar log: {e}")

# =============================
# ROTA DE LOGIN
# =============================
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form["usuario"]
        senha = request.form["senha"]

        cursor, conn = get_db()
        cursor.execute(
            "SELECT * FROM usuarios WHERE usuario = %s AND ativo = 1",
            (usuario,)
        )
        user = cursor.fetchone()
        return_connection(conn)

        if user and check_password_hash(user["senha_hash"], senha):
            session["usuario"] = user["usuario"]
            session["tipo"] = user["tipo"]
            session["user_id"] = user["id"]
            session["nome_completo"] = user["nome_completo"] or ""
            session["cim_numero"] = user["cim_numero"] or ""
            session["loja_nome"] = user["loja_nome"] or ""
            session["loja_numero"] = user["loja_numero"] or ""
            session["loja_orient"] = user["loja_orient"] or ""
            session["grau_atual"] = user["grau_atual"] or 1  # <-- ADICIONE ESTA LINHA

            registrar_log("login", "usuarios", user["id"], dados_novos={"usuario": usuario})
            flash(f"Bem-vindo, {user['nome_completo'] or user['usuario']}!", "success")
            return redirect("/dashboard")
        else:
            flash("Usuário ou senha inválidos", "danger")

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
    try:
        cursor, conn = get_db()

        # Candidatos
        cursor.execute("SELECT * FROM candidatos ORDER BY data_criacao DESC")
        candidatos = cursor.fetchall()

        # Sindicantes ativos
        cursor.execute("""
            SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, ativo 
            FROM usuarios 
            WHERE tipo = 'sindicante' AND ativo = 1
            ORDER BY nome_completo
        """)
        sindicantes = cursor.fetchall()

        # Pareceres conclusivos recentes
        pareceres_conclusivos = []
        try:
            cursor.execute("""
                SELECT pc.*, c.nome as candidato_nome, u.nome_completo as sindicante_nome
                FROM pareceres_conclusivos pc
                JOIN candidatos c ON pc.candidato_id = c.id
                JOIN usuarios u ON pc.sindicante = u.usuario
                ORDER BY pc.data_envio DESC
                LIMIT 10
            """)
            pareceres_conclusivos = cursor.fetchall()
        except:
            pass

        total_sindicantes_ativos = len(sindicantes)
        total_candidatos = len(candidatos)

        if session["tipo"] == "admin":
            em_analise = sum(1 for c in candidatos if c["status"] == "Em análise" and not c["fechado"])
            aprovados = sum(1 for c in candidatos if c["status"] == "Aprovado")
            reprovados = sum(1 for c in candidatos if c["status"] == "Reprovado")

            # Pendências
            pendentes = []
            for c in candidatos:
                if not c["fechado"]:
                    cursor.execute("SELECT sindicante FROM sindicancias WHERE candidato_id = %s", (c["id"],))
                    enviados = [r["sindicante"] for r in cursor.fetchall()]
                    faltam = [s["usuario"] for s in sindicantes if s["usuario"] not in enviados]
                    if faltam:
                        pendentes.append({"candidato": dict(c), "faltam": faltam})

            # Prazo vencido
            prazo_vencido = []
            for c in candidatos:
                if not c["fechado"] and c["status"] == "Em análise" and c["data_criacao"]:
                    try:
                        data_criacao = c["data_criacao"]
                        dias = (datetime.now() - data_criacao).days
                        if dias > 7:
                            prazo_vencido.append(dict(c))
                    except:
                        pass

            # Estatísticas de obreiros
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo IN ('admin', 'sindicante', 'obreiro') AND ativo = 1")
            total_obreiros = cursor.fetchone()["total"]
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 3 AND ativo = 1")
            mestres = cursor.fetchone()["total"]
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 2 AND ativo = 1")
            companheiros = cursor.fetchone()["total"]
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 1 AND ativo = 1")
            aprendizes = cursor.fetchone()["total"]

            # Estatísticas de reuniões
            cursor.execute("SELECT COUNT(*) as total FROM reunioes")
            total_reunioes = cursor.fetchone()["total"]
            cursor.execute("SELECT COUNT(*) as total FROM reunioes WHERE status = 'realizada'")
            reunioes_realizadas = cursor.fetchone()["total"]
            cursor.execute("SELECT COUNT(*) as total FROM reunioes WHERE status = 'agendada'")
            reunioes_agendadas = cursor.fetchone()["total"]
            cursor.execute("""
                SELECT id, titulo, data, hora_inicio 
                FROM reunioes 
                WHERE status = 'agendada' AND data >= CURRENT_DATE
                ORDER BY data ASC, hora_inicio ASC
                LIMIT 5
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
                cursor.execute(
                    "SELECT parecer FROM sindicancias WHERE candidato_id = %s AND sindicante = %s",
                    (c["id"], session["usuario"])
                )
                parecer = cursor.fetchone()
                if parecer:
                    if parecer["parecer"] == "positivo":
                        aprovados += 1
                    else:
                        reprovados += 1
                elif not c["fechado"]:
                    em_analise += 1

        return_connection(conn)
        now = datetime.now()

        return render_template(
            "dashboard.html",
            tipo=session["tipo"],
            total_candidatos=total_candidatos,
            total_sindicantes=total_sindicantes_ativos,
            total_obreiros=total_obreiros,
            mestres=mestres,
            companheiros=companheiros,
            aprendizes=aprendizes,
            total_reunioes=total_reunioes,
            reunioes_realizadas=reunioes_realizadas,
            reunioes_agendadas=reunioes_agendadas,
            proximas_reunioes=proximas_reunioes,
            proxima_reuniao=proxima_reuniao,
            em_analise=em_analise,
            aprovados=aprovados,
            reprovados=reprovados,
            pendentes=pendentes,
            prazo_vencido=prazo_vencido,
            sindicantes=sindicantes,
            pareceres_conclusivos=pareceres_conclusivos,
            now=now
        )
        
    except Exception as e:
        print(f"Erro no dashboard: {e}")
        import traceback
        traceback.print_exc()
        flash(f"Erro ao carregar dashboard: {e}", "danger")
        return redirect("/")
# =============================
# ROTAS DE PERMISSÕES
# =============================

@app.route("/admin/permissoes")
@admin_required
def gerenciar_permissoes():
    """Página de gerenciamento de permissões"""
    cursor, conn = get_db()
    
    # Buscar apenas os 3 graus principais (Aprendiz, Companheiro, Mestre)
    cursor.execute("""
        SELECT * FROM graus 
        WHERE nivel IN (1, 2, 3) AND ativo = 1 
        ORDER BY nivel
    """)
    graus = cursor.fetchall()
    
    # Buscar todos os usuários
    cursor.execute("""
        SELECT id, usuario, nome_completo, grau_atual, tipo
        FROM usuarios
        WHERE ativo = 1
        ORDER BY nome_completo
    """)
    usuarios = cursor.fetchall()
    
    # Buscar todos os módulos com suas permissões
    cursor.execute("""
        SELECT m.id as modulo_id, m.nome as modulo_nome, m.icone as modulo_icone,
               p.id as permissao_id, p.nome as permissao_nome, p.codigo, p.descricao,
               m.ordem as modulo_ordem
        FROM modulos m
        JOIN permissoes p ON m.id = p.modulo_id
        WHERE m.ativo = 1
        ORDER BY m.ordem, p.id
    """)
    permissoes = cursor.fetchall()
    
    # Organizar permissões por módulo (para estrutura em árvore)
    permissoes_por_modulo = {}
    for p in permissoes:
        if p['modulo_nome'] not in permissoes_por_modulo:
            permissoes_por_modulo[p['modulo_nome']] = {
                'icone': p['modulo_icone'],
                'permissoes': []
            }
        permissoes_por_modulo[p['modulo_nome']]['permissoes'].append({
            'id': p['permissao_id'],
            'nome': p['permissao_nome'],
            'codigo': p['codigo'],
            'descricao': p['descricao']
        })
    
    # Buscar permissões por grau
    cursor.execute("""
        SELECT grau_id, permissao_id
        FROM permissoes_grau
        WHERE grau_id IN (1, 2, 3)
    """)
    permissoes_grau_raw = cursor.fetchall()
    permissoes_grau = [(pg['grau_id'], pg['permissao_id']) for pg in permissoes_grau_raw]
    
    # Buscar permissões especiais por usuário
    cursor.execute("""
        SELECT usuario_id, permissao_id, permitido
        FROM permissoes_usuario
    """)
    permissoes_usuario_raw = cursor.fetchall()
    permissoes_usuario = [(pu['usuario_id'], pu['permissao_id'], pu['permitido']) for pu in permissoes_usuario_raw]
    
    return_connection(conn)
    
    return render_template("admin/permissoes.html",
                          usuarios=usuarios,
                          graus=graus,
                          permissoes_por_modulo=permissoes_por_modulo,
                          permissoes_grau=permissoes_grau,
                          permissoes_usuario=permissoes_usuario)

@app.route("/admin/permissoes/grau/<int:grau_id>", methods=["POST"])
@admin_required
def salvar_permissoes_grau(grau_id):
    """Salva as permissões para um grau"""
    cursor, conn = get_db()
    
    # Remover permissões existentes
    cursor.execute("DELETE FROM permissoes_grau WHERE grau_id = %s", (grau_id,))
    
    # Adicionar novas permissões
    permissoes = request.form.getlist("permissoes")
    
    for permissao_id in permissoes:
        cursor.execute("""
            INSERT INTO permissoes_grau (grau_id, permissao_id)
            VALUES (%s, %s)
        """, (grau_id, permissao_id))
    
    conn.commit()
    return_connection(conn)
    
    flash("Permissões do grau atualizadas com sucesso!", "success")
    return redirect("/admin/permissoes")

@app.route("/admin/permissoes/usuario/<int:usuario_id>", methods=["POST"])
@admin_required
def salvar_permissoes_usuario(usuario_id):
    """Salva as permissões especiais para um usuário"""
    cursor, conn = get_db()
    
    # Remover permissões existentes
    cursor.execute("DELETE FROM permissoes_usuario WHERE usuario_id = %s", (usuario_id,))
    
    # Adicionar novas permissões
    permissoes_extra = request.form.getlist("permissoes_extra")
    permissoes_bloqueadas = request.form.getlist("permissoes_bloqueadas")
    
    for permissao_id in permissoes_extra:
        cursor.execute("""
            INSERT INTO permissoes_usuario (usuario_id, permissao_id, permitido)
            VALUES (%s, %s, 1)
        """, (usuario_id, permissao_id))
    
    for permissao_id in permissoes_bloqueadas:
        cursor.execute("""
            INSERT INTO permissoes_usuario (usuario_id, permissao_id, permitido)
            VALUES (%s, %s, 0)
        """, (usuario_id, permissao_id))
    
    conn.commit()
    return_connection(conn)
    
    flash("Permissões do usuário atualizadas com sucesso!", "success")
    return redirect("/admin/permissoes")
        
# =============================
# ROTAS DE PERFIL
# =============================
# atualizar_niveis.py
@app.route("/perfil", methods=["GET", "POST"])
@login_required
def perfil():
    cursor, conn = get_db()

    if request.method == "POST":
        # Verificar se é alteração de senha
        if request.form.get("acao") == "alterar_senha":
            senha_atual = request.form.get("senha_atual")
            nova_senha = request.form.get("nova_senha")
            confirmar_senha = request.form.get("confirmar_senha")
            
            # Validar senha atual
            cursor.execute("SELECT senha_hash FROM usuarios WHERE id = %s", (session["user_id"],))
            user = cursor.fetchone()
            
            if not check_password_hash(user["senha_hash"], senha_atual):
                flash("Senha atual incorreta!", "danger")
                return redirect("/perfil")
            
            if not nova_senha or len(nova_senha) < 6:
                flash("A nova senha deve ter pelo menos 6 caracteres!", "danger")
                return redirect("/perfil")
            
            if nova_senha != confirmar_senha:
                flash("As senhas não conferem!", "danger")
                return redirect("/perfil")
            
            # Atualizar senha
            nova_senha_hash = generate_password_hash(nova_senha)
            cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (nova_senha_hash, session["user_id"]))
            conn.commit()
            
            registrar_log("alterar_senha", "perfil", session["user_id"], 
                         dados_novos={"usuario": session["usuario"]})
            
            flash("Senha alterada com sucesso! Faça login novamente.", "success")
            return redirect("/logout")
        
        else:
            # Atualizar dados do perfil
            nome_completo = request.form.get("nome_completo", "")
            cim_numero = request.form.get("cim_numero", "")
            loja_nome = request.form.get("loja_nome", "")
            loja_numero = request.form.get("loja_numero", "")
            loja_orient = request.form.get("loja_orient", "")
            telefone = request.form.get("telefone", "")
            email = request.form.get("email", "")
            endereco = request.form.get("endereco", "")

            # Buscar dados antigos
            cursor.execute("SELECT * FROM usuarios WHERE id = %s", (session["user_id"],))
            dados_antigos = dict(cursor.fetchone())

            cursor.execute("""
                UPDATE usuarios 
                SET nome_completo = %s, cim_numero = %s, loja_nome = %s, 
                    loja_numero = %s, loja_orient = %s, telefone = %s, 
                    email = %s, endereco = %s
                WHERE id = %s
            """, (nome_completo, cim_numero, loja_nome, loja_numero, 
                  loja_orient, telefone, email, endereco, session["user_id"]))

            conn.commit()

            # Atualizar sessão
            session["nome_completo"] = nome_completo
            session["cim_numero"] = cim_numero
            session["loja_nome"] = loja_nome
            session["loja_numero"] = loja_numero
            session["loja_orient"] = loja_orient

            registrar_log("editar", "perfil", session["user_id"], 
                         dados_anteriores=dados_antigos,
                         dados_novos={"nome_completo": nome_completo, "email": email})

            flash("Perfil atualizado com sucesso!", "success")
            return redirect("/perfil")

    # GET - Carregar dados do perfil
    cursor.execute("SELECT * FROM usuarios WHERE id = %s", (session["user_id"],))
    usuario = cursor.fetchone()
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    return_connection(conn)

    return render_template("perfil.html", usuario=usuario, lojas=lojas)

import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()


def atualizar_niveis_acesso():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Adicionar coluna se não existir
        cursor.execute("""
            ALTER TABLE usuarios 
            ADD COLUMN IF NOT EXISTS nivel_acesso INTEGER DEFAULT 1
        """)
        
        # Atualizar níveis baseados no grau
        cursor.execute("""
            UPDATE usuarios 
            SET nivel_acesso = grau_atual 
            WHERE grau_atual IN (1,2,3) AND tipo != 'admin'
        """)
        
        # Administradores têm nível 4
        cursor.execute("""
            UPDATE usuarios 
            SET nivel_acesso = 4 
            WHERE tipo = 'admin'
        """)
        
        conn.commit()
        
        print("✅ Níveis de acesso atualizados com sucesso!")
        
        # Mostrar resultados
        cursor.execute("""
            SELECT tipo, grau_atual, nivel_acesso, COUNT(*) 
            FROM usuarios 
            GROUP BY tipo, grau_atual, nivel_acesso
            ORDER BY nivel_acesso
        """)
        
        print("\n📊 Distribuição de níveis:")
        for row in cursor.fetchall():
            print(f"   Tipo: {row[0]}, Grau: {row[1]}, Nível: {row[2]}, Total: {row[3]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    atualizar_niveis_acesso()

# =============================
# ROTAS DE OBREIROS
# =============================
@app.route("/obreiros")
@login_required
def listar_obreiros():
    cursor, conn = get_db()
    
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
                   ELSE 'Não informado'
               END as grau_descricao,
               (SELECT COUNT(*) FROM ocupacao_cargos oc WHERE oc.obreiro_id = u.id AND oc.ativo = 1) as total_cargos
        FROM usuarios u
        LEFT JOIN lojas l ON u.loja_nome = l.nome
        WHERE 1=1
    """
    params = []

    if nome:
        query += " AND (u.nome_completo LIKE %s OR u.usuario LIKE %s)"
        params.extend([f"%{nome}%", f"%{nome}%"])
    if grau:
        query += " AND u.grau_atual = %s"
        params.append(grau)
    if cargo:
        query += " AND EXISTS (SELECT 1 FROM ocupacao_cargos oc WHERE oc.obreiro_id = u.id AND oc.cargo_id = %s AND oc.ativo = 1)"
        params.append(cargo)
    if loja:
        query += " AND u.loja_nome = %s"
        params.append(loja)
    if status:
        query += " AND u.ativo = %s"
        params.append(status)
    else:
        query += " AND u.ativo = 1"

    query += " ORDER BY u.nome_completo"

    cursor.execute(query, params)
    obreiros = cursor.fetchall()

    cursor.execute("SELECT DISTINCT grau_atual FROM usuarios WHERE grau_atual IS NOT NULL ORDER BY grau_atual")
    graus = cursor.fetchall()
    cursor.execute("SELECT id, nome FROM cargos WHERE ativo = 1 ORDER BY ordem")
    cargos_list = cursor.fetchall()
    cursor.execute("SELECT DISTINCT loja_nome FROM usuarios WHERE loja_nome IS NOT NULL ORDER BY loja_nome")
    lojas = cursor.fetchall()

    return_connection(conn)
    return render_template("obreiros/lista.html", 
                          obreiros=obreiros,
                          graus=graus,
                          cargos=cargos_list,
                          lojas=lojas,
                          filtros={'nome': nome, 'grau': grau, 'cargo': cargo, 'loja': loja, 'status': status})

@app.route("/obreiros/novo", methods=["GET", "POST"])
@admin_required
def novo_obreiro():
    cursor, conn = get_db()

    if request.method == "POST":
        usuario = request.form.get("usuario")
        senha = request.form.get("senha")
        nome_completo = request.form.get("nome_completo")
        nome_maconico = request.form.get("nome_maconico")
        cim_numero = request.form.get("cim_numero")
        tipo = request.form.get("tipo", "obreiro")
        grau_atual = request.form.get("grau_atual", 1)
        data_iniciacao = request.form.get("data_iniciacao")
        data_elevacao = request.form.get("data_elevacao")
        data_exaltacao = request.form.get("data_exaltacao")
        telefone = request.form.get("telefone")
        email = request.form.get("email")
        endereco = request.form.get("endereco")
        loja_nome = request.form.get("loja_nome")
        loja_numero = request.form.get("loja_numero")
        loja_orient = request.form.get("loja_orient")

        # Converter strings vazias para None
        data_iniciacao = data_iniciacao if data_iniciacao and data_iniciacao.strip() else None
        data_elevacao = data_elevacao if data_elevacao and data_elevacao.strip() else None
        data_exaltacao = data_exaltacao if data_exaltacao and data_exaltacao.strip() else None

        # CONVERTER GRAU: se for grau superior (nivel > 3), mantém como 3
        try:
            grau_atual_int = int(grau_atual)
            if grau_atual_int > 3:
                # Grau superior (Mestre Instalado, etc.) - guarda no histórico
                grau_superior = grau_atual_int
                grau_atual = 3  # Mestre como grau base
            else:
                grau_superior = None
                grau_atual = grau_atual_int
        except:
            grau_atual = 1
            grau_superior = None

        if not usuario or not senha or not nome_completo:
            flash("Preencha os campos obrigatórios", "danger")
        else:
            try:
                senha_hash = generate_password_hash(senha)
                agora = datetime.now()

                # Inserir usuário
                cursor.execute("""
                    INSERT INTO usuarios 
                    (usuario, senha_hash, tipo, data_cadastro, ativo, 
                     nome_completo, nome_maconico, cim_numero, grau_atual,
                     data_iniciacao, data_elevacao, data_exaltacao,
                     telefone, email, endereco,
                     loja_nome, loja_numero, loja_orient) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (usuario, senha_hash, tipo, agora, 1,
                      nome_completo, nome_maconico, cim_numero, grau_atual,
                      data_iniciacao, data_elevacao, data_exaltacao,
                      telefone, email, endereco,
                      loja_nome, loja_numero, loja_orient))

                conn.commit()
                obreiro_id = cursor.lastrowid

                # Registrar no histórico de graus
                if data_iniciacao:
                    cursor.execute("""
                        INSERT INTO historico_graus (obreiro_id, grau, data, observacao)
                        VALUES (%s, %s, %s, %s)
                    """, (obreiro_id, 1, data_iniciacao, "Iniciação"))

                # Se for grau superior, registrar no histórico
                if grau_superior:
                    # Buscar nome do grau superior
                    cursor.execute("SELECT nome FROM graus WHERE nivel = %s", (grau_superior,))
                    grau_info = cursor.fetchone()
                    nome_grau = grau_info['nome'] if grau_info else f"Grau {grau_superior}"
                    
                    cursor.execute("""
                        INSERT INTO historico_graus (obreiro_id, grau, data, observacao)
                        VALUES (%s, %s, %s, %s)
                    """, (obreiro_id, grau_superior, datetime.now().date(), 
                          f"Registro de {nome_grau}"))

                conn.commit()
                
                registrar_log("criar", "obreiro", obreiro_id, 
                             dados_novos={"nome": nome_completo, "usuario": usuario})
                flash(f"Obreiro '{nome_completo}' adicionado com sucesso!", "success")
                return_connection(conn)
                return redirect("/obreiros")

            except psycopg2.IntegrityError as e:
                flash(f"Erro: Usuário ou CIM já existe - {e}", "danger")
                conn.rollback()

    # Buscar lojas
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    
    # Buscar todos os graus ativos para o select
    cursor.execute("SELECT * FROM graus WHERE ativo = 1 ORDER BY nivel, ordem")
    graus = cursor.fetchall()
    
    return_connection(conn)
    return render_template("obreiros/novo.html", lojas=lojas, graus=graus)

@app.route("/obreiros/<int:id>")
@login_required
def visualizar_obreiro(id):
    """Visualiza os detalhes de um obreiro"""
    cursor, conn = get_db()
    
    try:
        # Buscar dados do obreiro
        cursor.execute("""
            SELECT u.*, l.nome as loja_nome_completo
            FROM usuarios u
            LEFT JOIN lojas l ON u.loja_nome = l.nome
            WHERE u.id = %s
        """, (id,))
        obreiro = cursor.fetchone()

        if not obreiro:
            flash("Obreiro não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")

        # Verificar permissão
        if session["tipo"] != "admin" and session["user_id"] != id:
            flash("Você não tem permissão para visualizar este obreiro", "danger")
            return_connection(conn)
            return redirect("/obreiros")

        # Buscar cargos ocupados
        cursor.execute("""
            SELECT oc.*, c.nome as cargo_nome, c.sigla
            FROM ocupacao_cargos oc
            JOIN cargos c ON oc.cargo_id = c.id
            WHERE oc.obreiro_id = %s AND oc.ativo = 1
            ORDER BY oc.data_inicio DESC
        """, (id,))
        cargos = cursor.fetchall()

        # Buscar histórico de graus com nomes dos graus
        cursor.execute("""
            SELECT h.*, g.nome as grau_nome
            FROM historico_graus h
            LEFT JOIN graus g ON h.grau_id = g.id
            WHERE h.obreiro_id = %s
            ORDER BY h.data DESC
        """, (id,))
        historico_graus = cursor.fetchall()

        # Buscar o nome do grau atual
        cursor.execute("SELECT nome FROM graus WHERE nivel = %s", (obreiro['grau_atual'],))
        grau_atual_info = cursor.fetchone()
        nome_grau_atual = grau_atual_info['nome'] if grau_atual_info else None

        # Contar familiares
        cursor.execute("SELECT COUNT(*) as total FROM familiares WHERE obreiro_id = %s", (id,))
        familiares_count = cursor.fetchone()["total"]

        # Contar condecorações
        cursor.execute("SELECT COUNT(*) as total FROM condecoracoes_obreiro WHERE obreiro_id = %s", (id,))
        condecoracoes_count = cursor.fetchone()["total"]

        # Buscar cargos disponíveis (para admin)
        cargos_disponiveis = []
        if session["tipo"] == "admin":
            cursor.execute("SELECT * FROM cargos WHERE ativo = 1 ORDER BY ordem")
            cargos_disponiveis = cursor.fetchall()
        
        # Buscar graus disponíveis (para admin)
        graus_disponiveis = []
        if session["tipo"] == "admin":
            cursor.execute("SELECT * FROM graus WHERE ativo = 1 ORDER BY nivel, ordem")
            graus_disponiveis = cursor.fetchall()

        # Buscar últimas condecorações
        cursor.execute("""
            SELECT c.*, t.nome as tipo_nome, t.cor, t.icone
            FROM condecoracoes_obreiro c
            JOIN tipos_condecoracoes t ON c.tipo_id = t.id
            WHERE c.obreiro_id = %s
            ORDER BY c.data_concessao DESC
            LIMIT 5
        """, (id,))
        ultimas_condecoracoes = cursor.fetchall()

        return_connection(conn)
        
        return render_template("obreiros/visualizar.html",
                              obreiro=obreiro,
                              cargos=cargos,
                              historico_graus=historico_graus,
                              cargos_disponiveis=cargos_disponiveis,
                              graus_disponiveis=graus_disponiveis,
                              familiares_count=familiares_count,
                              condecoracoes_count=condecoracoes_count,
                              ultimas_condecoracoes=ultimas_condecoracoes,
                              nome_grau_atual=nome_grau_atual,
                              pode_editar=(session["tipo"] == "admin" or session["user_id"] == id))
                              
    except Exception as e:
        print(f"Erro ao visualizar obreiro: {e}")
        if conn:
            return_connection(conn)
        flash(f"Erro ao carregar dados do obreiro: {str(e)}", "danger")
        return redirect("/obreiros")
        
@app.route("/tipos_condecoracoes/editar/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_tipo_condecoracao(id):
    """Edita um tipo de condecoração"""
    cursor, conn = get_db()
    
    if request.method == "POST":
        nome = request.form.get("nome")
        descricao = request.form.get("descricao")
        nivel = request.form.get("nivel", 1)
        cor = request.form.get("cor", "#ffc107")
        icone = request.form.get("icone", "bi-award")
        ordem = request.form.get("ordem", 0)
        ativo = 1 if request.form.get("ativo") else 0
        
        if not nome:
            flash("Nome da condecoração é obrigatório", "danger")
        else:
            try:
                cursor.execute("""
                    UPDATE tipos_condecoracoes 
                    SET nome = %s, descricao = %s, nivel = %s, 
                        cor = %s, icone = %s, ordem = %s, ativo = %s
                    WHERE id = %s
                """, (nome, descricao, nivel, cor, icone, ordem, ativo, id))
                conn.commit()
                flash(f"Tipo de condecoração '{nome}' atualizado com sucesso!", "success")
                return_connection(conn)
                return redirect("/tipos_condecoracoes")
            except Exception as e:
                flash(f"Erro ao atualizar: {str(e)}", "danger")
                conn.rollback()
    
    cursor.execute("SELECT * FROM tipos_condecoracoes WHERE id = %s", (id,))
    tipo = cursor.fetchone()
    return_connection(conn)
    
    if not tipo:
        flash("Tipo de condecoração não encontrado", "danger")
        return redirect("/tipos_condecoracoes")
    
    return render_template("admin/tipo_condecoracao_form.html", tipo=tipo)

@app.route("/tipos_condecoracoes/excluir/<int:id>")
@admin_required
def excluir_tipo_condecoracao(id):
    """Exclui um tipo de condecoração"""
    cursor, conn = get_db()
    
    try:
        # Verificar se está sendo usado
        cursor.execute("SELECT COUNT(*) as total FROM condecoracoes_obreiro WHERE tipo_id = %s", (id,))
        resultado = cursor.fetchone()
        
        if resultado and resultado["total"] > 0:
            # Se estiver sendo usado, apenas desativa
            cursor.execute("UPDATE tipos_condecoracoes SET ativo = 0 WHERE id = %s", (id,))
            conn.commit()
            flash("Tipo de condecoração desativado pois já foi utilizado.", "warning")
        else:
            cursor.execute("DELETE FROM tipos_condecoracoes WHERE id = %s", (id,))
            conn.commit()
            flash("Tipo de condecoração excluído com sucesso!", "success")
        
    except Exception as e:
        flash(f"Erro ao excluir: {str(e)}", "danger")
        conn.rollback()
    
    return_connection(conn)
    return redirect("/tipos_condecoracoes")        

@app.route("/obreiros/<int:id>/editar", methods=["GET", "POST"])
@login_required
def editar_obreiro(id):
    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para editar este obreiro", "danger")
        return redirect("/obreiros")

    cursor, conn = get_db()

    if request.method == "POST":
        nome_completo = request.form.get("nome_completo", "")
        nome_maconico = request.form.get("nome_maconico", "")
        cim_numero = request.form.get("cim_numero", "")
        telefone = request.form.get("telefone", "")
        email = request.form.get("email", "")
        endereco = request.form.get("endereco", "")
        loja_nome = request.form.get("loja_nome", "")
        loja_numero = request.form.get("loja_numero", "")
        loja_orient = request.form.get("loja_orient", "")

        # Buscar dados antigos
        cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
        dados_antigos = dict(cursor.fetchone())
        grau_antigo = dados_antigos.get("grau_atual")

        if session["tipo"] == "admin":
            tipo = request.form.get("tipo", "obreiro")
            grau_atual = request.form.get("grau_atual", 1)
            ativo = 1 if request.form.get("ativo") else 0
            
            # Tratar datas
            data_iniciacao = request.form.get("data_iniciacao", "")
            data_elevacao = request.form.get("data_elevacao", "")
            data_exaltacao = request.form.get("data_exaltacao", "")
            
            data_iniciacao = data_iniciacao if data_iniciacao and data_iniciacao.strip() else None
            data_elevacao = data_elevacao if data_elevacao and data_elevacao.strip() else None
            data_exaltacao = data_exaltacao if data_exaltacao and data_exaltacao.strip() else None

            # Converter grau_atual para inteiro
            try:
                grau_atual = int(grau_atual)
            except ValueError:
                grau_atual = 1

            cursor.execute("""
                UPDATE usuarios 
                SET nome_completo = %s, nome_maconico = %s, cim_numero = %s, tipo = %s,
                    grau_atual = %s, data_iniciacao = %s, data_elevacao = %s, data_exaltacao = %s,
                    telefone = %s, email = %s, endereco = %s,
                    loja_nome = %s, loja_numero = %s, loja_orient = %s, ativo = %s
                WHERE id = %s
            """, (nome_completo, nome_maconico, cim_numero, tipo,
                  grau_atual, data_iniciacao, data_elevacao, data_exaltacao,
                  telefone, email, endereco,
                  loja_nome, loja_numero, loja_orient, ativo, id))
            
            # Se o grau foi alterado, registrar no histórico
            if grau_atual != grau_antigo:
                # Buscar o nome do grau
                cursor.execute("SELECT nome FROM graus WHERE nivel = %s", (grau_atual,))
                grau_info = cursor.fetchone()
                nome_grau = grau_info['nome'] if grau_info else f"Grau {grau_atual}"
                
                # Registrar no histórico de graus
                cursor.execute("""
                    INSERT INTO historico_graus (obreiro_id, grau, data, observacao)
                    VALUES (%s, %s, %s, %s)
                """, (id, grau_atual, datetime.now().date(), f"Atualização de grau para {nome_grau}"))
                
                # Buscar nome do grau antigo
                cursor.execute("SELECT nome FROM graus WHERE nivel = %s", (grau_antigo,))
                grau_antigo_info = cursor.fetchone()
                nome_grau_antigo = grau_antigo_info['nome'] if grau_antigo_info else f"Grau {grau_antigo}"
                
                flash(f"Grau alterado de {nome_grau_antigo} para {nome_grau}. Registro adicionado ao histórico!", "info")
        else:
            cursor.execute("""
                UPDATE usuarios 
                SET nome_completo = %s, nome_maconico = %s, cim_numero = %s,
                    telefone = %s, email = %s, endereco = %s,
                    loja_nome = %s, loja_numero = %s, loja_orient = %s
                WHERE id = %s
            """, (nome_completo, nome_maconico, cim_numero,
                  telefone, email, endereco,
                  loja_nome, loja_numero, loja_orient, id))

        conn.commit()

        if session["user_id"] == id:
            session["nome_completo"] = nome_completo
            session["cim_numero"] = cim_numero
            session["loja_nome"] = loja_nome
            session["loja_numero"] = loja_numero
            session["loja_orient"] = loja_orient

        registrar_log("editar", "obreiro", id, 
                     dados_anteriores=dados_antigos,
                     dados_novos={"nome": nome_completo, "email": email, "telefone": telefone})

        flash("Perfil atualizado com sucesso!", "success")
        return_connection(conn)
        return redirect(f"/obreiros/{id}")

    # GET - Carregar dados para edição
    cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
    obreiro = cursor.fetchone()
    
    # Buscar lojas
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    
    # Buscar todos os graus ativos para o select
    cursor.execute("SELECT * FROM graus WHERE ativo = 1 ORDER BY nivel, ordem")
    graus = cursor.fetchall()
    
    # Buscar graus superiores registrados no histórico (nivel > 3)
    cursor.execute("""
        SELECT h.*, g.nome as grau_nome
        FROM historico_graus h
        LEFT JOIN graus g ON h.grau_id = g.id
        WHERE h.obreiro_id = %s AND h.grau > 3
        ORDER BY h.data DESC
    """, (id,))
    historico_graus_superiores = cursor.fetchall()
    
    return_connection(conn)

    return render_template("obreiros/editar.html",
                          obreiro=obreiro,
                          lojas=lojas,
                          graus=graus,
                          historico_graus_superiores=historico_graus_superiores,
                          is_admin=(session["tipo"] == "admin"),
                          is_own_profile=(session["user_id"] == id))


# =============================
# ROTAS DE CARGOS
# =============================

@app.route("/cargos")
@admin_required
def listar_cargos():
    """Lista todos os cargos"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT * FROM cargos 
            ORDER BY ordem NULLS LAST, nome
        """)
        cargos = cursor.fetchall()
        
        return_connection(conn)
        return render_template("cargos/lista.html", cargos=cargos)
        
    except Exception as e:
        print(f"Erro ao listar cargos: {e}")
        if conn:
            return_connection(conn)
        flash(f"Erro ao carregar cargos: {str(e)}", "danger")
        return redirect("/dashboard")

@app.route("/cargos/novo", methods=["GET", "POST"])
@admin_required
def novo_cargo():
    """Cadastra um novo cargo"""
    if request.method == "POST":
        nome = request.form.get("nome")
        sigla = request.form.get("sigla")
        ordem = request.form.get("ordem")
        grau_minimo = request.form.get("grau_minimo")
        descricao = request.form.get("descricao")
        
        if not nome or not sigla or not ordem:
            flash("Preencha todos os campos obrigatórios (Nome, Sigla e Ordem)", "danger")
            return redirect("/cargos/novo")
        
        try:
            cursor, conn = get_db()
            
            # Converter ordem para inteiro
            try:
                ordem = int(ordem)
            except ValueError:
                ordem = 999
            
            # Converter grau_minimo para inteiro
            try:
                grau_minimo = int(grau_minimo) if grau_minimo else 1
            except ValueError:
                grau_minimo = 1
            
            cursor.execute("""
                INSERT INTO cargos (nome, sigla, ordem, grau_minimo, descricao, ativo)
                VALUES (%s, %s, %s, %s, %s, 1)
            """, (nome, sigla, ordem, grau_minimo, descricao))
            conn.commit()
            
            cargo_id = cursor.lastrowid
            registrar_log("criar", "cargo", cargo_id, dados_novos={"nome": nome, "sigla": sigla})
            
            flash(f"Cargo '{nome}' adicionado com sucesso!", "success")
            return_connection(conn)
            return redirect("/cargos")
            
        except Exception as e:
            print(f"Erro ao criar cargo: {e}")
            if conn:
                conn.rollback()
                return_connection(conn)
            flash(f"Erro ao criar cargo: {str(e)}", "danger")
            return redirect("/cargos/novo")
    
    # GET - Mostrar formulário
    return render_template("cargos/novo.html")

@app.route("/cargos/editar/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_cargo(id):
    """Edita um cargo existente"""
    cursor, conn = get_db()
    
    if request.method == "POST":
        nome = request.form.get("nome")
        sigla = request.form.get("sigla")
        ordem = request.form.get("ordem")
        grau_minimo = request.form.get("grau_minimo")
        descricao = request.form.get("descricao")
        ativo = 1 if request.form.get("ativo") else 0
        
        if not nome or not sigla or not ordem:
            flash("Preencha todos os campos obrigatórios (Nome, Sigla e Ordem)", "danger")
            return_connection(conn)
            return redirect(f"/cargos/editar/{id}")
        
        try:
            # Buscar dados antigos para log
            cursor.execute("SELECT * FROM cargos WHERE id = %s", (id,))
            dados_antigos = dict(cursor.fetchone())
            
            # Converter ordem para inteiro
            try:
                ordem = int(ordem)
            except ValueError:
                ordem = 999
            
            # Converter grau_minimo para inteiro
            try:
                grau_minimo = int(grau_minimo) if grau_minimo else 1
            except ValueError:
                grau_minimo = 1
            
            cursor.execute("""
                UPDATE cargos 
                SET nome = %s, sigla = %s, ordem = %s, grau_minimo = %s, descricao = %s, ativo = %s
                WHERE id = %s
            """, (nome, sigla, ordem, grau_minimo, descricao, ativo, id))
            conn.commit()
            
            registrar_log("editar", "cargo", id, dados_anteriores=dados_antigos,
                         dados_novos={"nome": nome, "sigla": sigla})
            flash("Cargo atualizado com sucesso!", "success")
            return_connection(conn)
            return redirect("/cargos")
            
        except Exception as e:
            print(f"Erro ao editar cargo: {e}")
            if conn:
                conn.rollback()
                return_connection(conn)
            flash(f"Erro ao editar cargo: {str(e)}", "danger")
            return redirect(f"/cargos/editar/{id}")
    
    # GET - Carregar dados do cargo
    cursor.execute("SELECT * FROM cargos WHERE id = %s", (id,))
    cargo = cursor.fetchone()
    return_connection(conn)
    
    if not cargo:
        flash("Cargo não encontrado", "danger")
        return redirect("/cargos")
    
    return render_template("cargos/editar.html", cargo=cargo)

@app.route("/cargos/excluir/<int:id>")
@admin_required
def excluir_cargo(id):
    """Exclui um cargo (se não estiver sendo usado)"""
    cursor, conn = get_db()
    
    try:
        # Verificar se o cargo existe
        cursor.execute("SELECT * FROM cargos WHERE id = %s", (id,))
        cargo = cursor.fetchone()
        
        if not cargo:
            flash("Cargo não encontrado", "danger")
            return_connection(conn)
            return redirect("/cargos")
        
        dados = dict(cargo)
        
        # Verificar se o cargo está sendo usado
        cursor.execute("SELECT COUNT(*) as total FROM ocupacao_cargos WHERE cargo_id = %s", (id,))
        resultado = cursor.fetchone()
        
        if resultado and resultado["total"] > 0:
            # Se estiver sendo usado, apenas desativa
            cursor.execute("UPDATE cargos SET ativo = 0 WHERE id = %s", (id,))
            conn.commit()
            registrar_log("desativar", "cargo", id, dados_anteriores=dados)
            flash(f"Cargo '{cargo['nome']}' desativado pois está em uso.", "warning")
        else:
            # Se não estiver sendo usado, exclui
            cursor.execute("DELETE FROM cargos WHERE id = %s", (id,))
            conn.commit()
            registrar_log("excluir", "cargo", id, dados_anteriores=dados)
            flash(f"Cargo '{cargo['nome']}' excluído com sucesso!", "success")
        
        return_connection(conn)
        return redirect("/cargos")
        
    except Exception as e:
        print(f"Erro ao excluir cargo: {e}")
        if conn:
            conn.rollback()
            return_connection(conn)
        flash(f"Erro ao excluir cargo: {str(e)}", "danger")
        return redirect("/cargos")
# =============================
# ROTAS DE GRAUS MAÇÔNICOS
# =============================

@app.route("/graus")
@admin_required
def listar_graus():
    """Lista todos os graus maçônicos"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT g.*,
                   (SELECT COUNT(*) FROM historico_graus WHERE grau_id = g.id) as total_historicos,
                   (SELECT COUNT(*) FROM usuarios WHERE grau_atual = g.nivel) as total_obreiros
            FROM graus g
            ORDER BY g.nivel, g.ordem
        """)
        graus = cursor.fetchall()
        
        return_connection(conn)
        return render_template("graus/lista.html", graus=graus)
        
    except Exception as e:
        print(f"Erro ao listar graus: {e}")
        if conn:
            return_connection(conn)
        flash(f"Erro ao carregar graus: {str(e)}", "danger")
        return redirect("/dashboard")

@app.route("/graus/novo", methods=["GET", "POST"])
@admin_required
def novo_grau():
    """Cadastra um novo grau"""
    if request.method == "POST":
        nome = request.form.get("nome")
        descricao = request.form.get("descricao")
        nivel = request.form.get("nivel")
        ordem = request.form.get("ordem")
        
        if not nome:
            flash("Nome do grau é obrigatório", "danger")
            return redirect("/graus/novo")
        
        try:
            cursor, conn = get_db()
            
            # Converter valores
            try:
                nivel = int(nivel) if nivel else 4
            except ValueError:
                nivel = 4
            
            try:
                ordem = int(ordem) if ordem else 999
            except ValueError:
                ordem = 999
            
            cursor.execute("""
                INSERT INTO graus (nome, descricao, nivel, ordem, ativo, created_by)
                VALUES (%s, %s, %s, %s, 1, %s)
            """, (nome, descricao, nivel, ordem, session["user_id"]))
            conn.commit()
            
            grau_id = cursor.lastrowid
            registrar_log("criar", "grau", grau_id, dados_novos={"nome": nome, "nivel": nivel})
            
            flash(f"Grau '{nome}' adicionado com sucesso!", "success")
            return_connection(conn)
            return redirect("/graus")
            
        except Exception as e:
            print(f"Erro ao criar grau: {e}")
            if conn:
                conn.rollback()
                return_connection(conn)
            flash(f"Erro ao criar grau: {str(e)}", "danger")
            return redirect("/graus/novo")
    
    return render_template("graus/novo.html")

@app.route("/graus/editar/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_grau(id):
    """Edita um grau existente"""
    cursor, conn = get_db()
    
    if request.method == "POST":
        nome = request.form.get("nome")
        descricao = request.form.get("descricao")
        nivel = request.form.get("nivel")
        ordem = request.form.get("ordem")
        ativo = 1 if request.form.get("ativo") else 0
        
        if not nome:
            flash("Nome do grau é obrigatório", "danger")
            return_connection(conn)
            return redirect(f"/graus/editar/{id}")
        
        try:
            # Buscar dados antigos para log
            cursor.execute("SELECT * FROM graus WHERE id = %s", (id,))
            dados_antigos = dict(cursor.fetchone())
            
            # Converter valores
            try:
                nivel = int(nivel) if nivel else 4
            except ValueError:
                nivel = 4
            
            try:
                ordem = int(ordem) if ordem else 999
            except ValueError:
                ordem = 999
            
            cursor.execute("""
                UPDATE graus 
                SET nome = %s, descricao = %s, nivel = %s, ordem = %s, ativo = %s
                WHERE id = %s
            """, (nome, descricao, nivel, ordem, ativo, id))
            conn.commit()
            
            registrar_log("editar", "grau", id, dados_anteriores=dados_antigos,
                         dados_novos={"nome": nome, "nivel": nivel})
            flash("Grau atualizado com sucesso!", "success")
            return_connection(conn)
            return redirect("/graus")
            
        except Exception as e:
            print(f"Erro ao editar grau: {e}")
            if conn:
                conn.rollback()
                return_connection(conn)
            flash(f"Erro ao editar grau: {str(e)}", "danger")
            return redirect(f"/graus/editar/{id}")
    
    # GET - Carregar dados do grau com estatísticas
    cursor.execute("""
        SELECT g.*,
               (SELECT COUNT(*) FROM historico_graus WHERE grau_id = g.id) as total_historicos,
               (SELECT COUNT(*) FROM usuarios WHERE grau_atual = g.nivel) as total_obreiros
        FROM graus g
        WHERE g.id = %s
    """, (id,))
    grau = cursor.fetchone()
    return_connection(conn)
    
    if not grau:
        flash("Grau não encontrado", "danger")
        return redirect("/graus")
    
    return render_template("graus/editar.html", grau=grau)

@app.route("/graus/excluir/<int:id>")
@admin_required
def excluir_grau(id):
    """Exclui um grau (se não estiver sendo usado)"""
    cursor, conn = get_db()
    
    try:
        cursor.execute("SELECT * FROM graus WHERE id = %s", (id,))
        grau = cursor.fetchone()
        
        if not grau:
            flash("Grau não encontrado", "danger")
            return_connection(conn)
            return redirect("/graus")
        
        dados = dict(grau)
        
        # Verificar se o grau está sendo usado
        cursor.execute("SELECT COUNT(*) as total FROM historico_graus WHERE grau_id = %s", (id,))
        resultado = cursor.fetchone()
        
        if resultado and resultado["total"] > 0:
            # Se estiver sendo usado, apenas desativa
            cursor.execute("UPDATE graus SET ativo = 0 WHERE id = %s", (id,))
            conn.commit()
            registrar_log("desativar", "grau", id, dados_anteriores=dados)
            flash(f"Grau '{grau['nome']}' desativado pois está em uso.", "warning")
        else:
            cursor.execute("DELETE FROM graus WHERE id = %s", (id,))
            conn.commit()
            registrar_log("excluir", "grau", id, dados_anteriores=dados)
            flash(f"Grau '{grau['nome']}' excluído com sucesso!", "success")
        
        return_connection(conn)
        return redirect("/graus")
        
    except Exception as e:
        print(f"Erro ao excluir grau: {e}")
        if conn:
            conn.rollback()
            return_connection(conn)
        flash(f"Erro ao excluir grau: {str(e)}", "danger")
        return redirect("/graus")
        
@app.route("/obreiros/<int:id>/grau", methods=["POST"])
@admin_required
def registrar_grau(id):
    cursor, conn = get_db()
    grau_id = request.form.get("grau_id")
    data = request.form.get("data")
    observacao = request.form.get("observacao")
    
    if not grau_id or not data:
        flash("Grau e data são obrigatórios", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{id}")
    
    try:
        # Buscar informações do grau
        cursor.execute("SELECT id, nome, nivel FROM graus WHERE id = %s", (grau_id,))
        grau = cursor.fetchone()
        
        if not grau:
            flash("Grau não encontrado", "danger")
            return_connection(conn)
            return redirect(f"/obreiros/{id}")
        
        # Verificar se é grau superior (nivel > 3)
        if grau['nivel'] > 3:
            # Registrar no histórico com o grau_id
            cursor.execute("""
                INSERT INTO historico_graus (obreiro_id, grau, grau_id, data, observacao)
                VALUES (%s, %s, %s, %s, %s)
            """, (id, grau['nivel'], grau_id, data, observacao))
            
            # NÃO altera o grau_atual (mantém como Mestre)
            flash(f"Grau superior '{grau['nome']}' registrado no histórico!", "info")
        else:
            # Grau básico (1,2,3) - atualiza o grau atual
            cursor.execute("""
                INSERT INTO historico_graus (obreiro_id, grau, data, observacao)
                VALUES (%s, %s, %s, %s)
            """, (id, grau['nivel'], data, observacao))
            
            # Atualizar grau atual do obreiro
            cursor.execute("UPDATE usuarios SET grau_atual = %s WHERE id = %s", (grau['nivel'], id))
            flash(f"Grau '{grau['nome']}' registrado e atualizado como grau principal!", "success")
        
        conn.commit()
        
        registrar_log("registrar_grau", "obreiro", id, 
                     dados_novos={"grau": grau['nome'], "data": data})
        
    except Exception as e:
        print(f"Erro ao registrar grau: {e}")
        conn.rollback()
        flash(f"Erro ao registrar grau: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/obreiros/{id}")       
        
# =============================
# ROTAS DE REUNIÕES
# =============================
@app.route("/reunioes")
@login_required
def listar_reunioes():
    cursor, conn = get_db()
    
    data_ini = request.args.get('data_ini', '')
    data_fim = request.args.get('data_fim', '')
    tipo = request.args.get('tipo', '')
    status = request.args.get('status', '')
    grau = request.args.get('grau', '')
    local = request.args.get('local', '')
    
    nivel_usuario = session.get("nivel_acesso", 1)
    tipo_usuario = session.get("tipo", "obreiro")
    
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
    
    # Filtrar por nível de acesso
    if tipo_usuario != "admin":
        if nivel_usuario == 1:  # Aprendiz
            query += " AND (r.grau = 1 OR r.grau IS NULL)"
        elif nivel_usuario == 2:  # Companheiro
            query += " AND (r.grau IN (1, 2) OR r.grau IS NULL)"
        # Mestre (nivel 3) vê todas
    
    if data_ini:
        query += " AND r.data >= %s"
        params.append(data_ini)
    if data_fim:
        query += " AND r.data <= %s"
        params.append(data_fim)
    if tipo:
        query += " AND r.tipo = %s"
        params.append(tipo)
    if status:
        query += " AND r.status = %s"
        params.append(status)
    if grau:
        query += " AND r.grau = %s"
        params.append(grau)
    if local:
        query += " AND r.local LIKE %s"
        params.append(f"%{local}%")

    query += " GROUP BY r.id, l.nome, t.cor ORDER BY r.data DESC, r.hora_inicio DESC"

    cursor.execute(query, params)
    reunioes = cursor.fetchall()

    cursor.execute("SELECT DISTINCT tipo FROM reunioes ORDER BY tipo")
    tipos = cursor.fetchall()
    cursor.execute("SELECT DISTINCT status FROM reunioes ORDER BY status")
    status_list = cursor.fetchall()
    cursor.execute("SELECT DISTINCT grau FROM reunioes WHERE grau IS NOT NULL ORDER BY grau")
    graus = cursor.fetchall()

    return_connection(conn)
    return render_template("reunioes/lista.html", 
                          reunioes=reunioes,
                          tipos=tipos,
                          status_list=status_list,
                          graus=graus,
                          filtros={'data_ini': data_ini, 'data_fim': data_fim, 
                                  'tipo': tipo, 'status': status, 'grau': grau, 'local': local})

@app.route("/reunioes/calendario")
@login_required
def calendario_reunioes():
    return render_template("reunioes/calendario.html")

@app.route("/api/reunioes")
@login_required
def api_reunioes():
    cursor, conn = get_db()
    cursor.execute("""
        SELECT r.*, t.cor, t.nome as tipo_nome,
               COUNT(p.id) as total_obreiros,
               SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presentes
        FROM reunioes r
        LEFT JOIN tipos_reuniao t ON r.tipo = t.nome
        LEFT JOIN presenca p ON r.id = p.reuniao_id
        GROUP BY r.id, t.cor, t.nome
        ORDER BY r.data
    """)
    rows = cursor.fetchall()
    return_connection(conn)

    eventos = []
    for row in rows:
        r = dict(row)
        start = f"{r['data']}T{r['hora_inicio']}" if r.get('hora_inicio') else str(r['data'])
        end = f"{r['data']}T{r['hora_termino']}" if r.get('hora_termino') else None
        eventos.append({
            "id": r["id"],
            "title": r["titulo"],
            "start": start,
            "end": end,
            "color": r.get("cor", "#3788d8"),
            "textColor": "#ffffff",
            "url": f"/reunioes/{r['id']}",
            "extendedProps": {
                "tipo": r.get("tipo"),
                "local": r.get("local"),
                "status": r.get("status"),
                "presentes": f"{r.get('presentes', 0)}/{r.get('total_obreiros', 0)}"
            }
        })
    return {"eventos": eventos}

@app.route("/reunioes/nova", methods=["GET", "POST"])
@admin_required
def nova_reuniao():
    cursor, conn = get_db()
    
    if request.method == "POST":
        # Obter e tratar os valores do formulário
        titulo = tratar_valor_nulo(request.form.get("titulo"))
        tipo = tratar_valor_nulo(request.form.get("tipo"))
        grau = tratar_valor_nulo(request.form.get("grau"), 'int')
        data = tratar_valor_nulo(request.form.get("data"), 'date')
        hora_inicio = tratar_valor_nulo(request.form.get("hora_inicio"), 'time')
        hora_termino = tratar_valor_nulo(request.form.get("hora_termino"), 'time')
        local = tratar_valor_nulo(request.form.get("local"))
        loja_id = tratar_valor_nulo(request.form.get("loja_id"), 'int')
        pauta = tratar_valor_nulo(request.form.get("pauta"))
        observacoes = tratar_valor_nulo(request.form.get("observacoes"))
        
        # Validar campos obrigatórios
        if not titulo or not tipo or not data or not hora_inicio:
            flash("Preencha todos os campos obrigatórios (Título, Tipo, Data e Horário)", "danger")
            return_connection(conn)
            return redirect("/reunioes/nova")
        
        try:
            cursor.execute("""
                INSERT INTO reunioes 
                (titulo, tipo, grau, data, hora_inicio, hora_termino, local, loja_id, pauta, observacoes, criado_por)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (titulo, tipo, grau, data, hora_inicio, hora_termino, 
                  local, loja_id, pauta, observacoes, session["user_id"]))
            conn.commit()
            reuniao_id = cursor.lastrowid
            
            registrar_log("criar", "reuniao", reuniao_id, dados_novos={"titulo": titulo, "data": str(data), "tipo": tipo})
            flash("Reunião agendada com sucesso!", "success")
            return_connection(conn)
            return redirect(f"/reunioes/{reuniao_id}")
            
        except psycopg2.Error as e:
            print(f"ERRO PostgreSQL: {e}")
            conn.rollback()
            flash(f"Erro ao salvar reunião: {str(e)}", "danger")
            return_connection(conn)
            return redirect("/reunioes/nova")
        except Exception as e:
            print(f"ERRO inesperado: {e}")
            conn.rollback()
            flash(f"Erro ao salvar reunião: {str(e)}", "danger")
            return_connection(conn)
            return redirect("/reunioes/nova")

    # GET - Carregar dados para o formulário
    cursor.execute("SELECT * FROM tipos_reuniao ORDER BY nome")
    tipos = cursor.fetchall()
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    return_connection(conn)
    hoje = datetime.now().strftime("%Y-%m-%d")
    return render_template("reunioes/nova.html", tipos=tipos, lojas=lojas, hoje=hoje)

@app.route("/reunioes/<int:id>")
@login_required
def detalhes_reuniao(id):
    cursor, conn = get_db()
    cursor.execute("""
        SELECT r.*, l.nome as loja_nome, t.cor as tipo_cor,
               u.nome_completo as criado_por_nome
        FROM reunioes r
        LEFT JOIN lojas l ON r.loja_id = l.id
        LEFT JOIN tipos_reuniao t ON r.tipo = t.nome
        LEFT JOIN usuarios u ON r.criado_por = u.id
        WHERE r.id = %s
    """, (id,))
    reuniao = cursor.fetchone()
    
    if not reuniao:
        flash("Reunião não encontrada", "danger")
        return_connection(conn)
        return redirect("/reunioes")

    # Buscar presenças
    cursor.execute("""
        SELECT u.id, u.nome_completo, u.grau_atual, 
               p.id as presenca_id, p.presente, p.tipo_ausencia, 
               p.justificativa, p.validado_por, p.data_registro,
               p.comprovante
        FROM usuarios u
        LEFT JOIN presenca p ON u.id = p.obreiro_id AND p.reuniao_id = %s
        WHERE u.ativo = 1 
        ORDER BY u.grau_atual DESC, u.nome_completo
    """, (id,))
    presenca = cursor.fetchall()
    
    total_obreiros = len(presenca)
    presentes = sum(1 for p in presenca if p["presente"] == 1)
    ausentes = total_obreiros - presentes

    # ========== BUSCAR ATA ==========
    cursor.execute("""
        SELECT id, aprovada, numero_ata, ano_ata, conteudo, data_criacao, 
               redator_id, versao, data_aprovacao, redator_nome
        FROM atas 
        WHERE reuniao_id = %s
        ORDER BY versao DESC
        LIMIT 1
    """, (id,))
    ata = cursor.fetchone()
    
    ata_id = ata["id"] if ata else None
    ata_aprovada = ata["aprovada"] if ata else None
    ata_numero = ata["numero_ata"] if ata else None
    ata_ano = ata["ano_ata"] if ata else None
    ata_conteudo = ata["conteudo"] if ata else None
    ata_data_criacao = ata["data_criacao"] if ata else None
    ata_versao = ata["versao"] if ata else None

    # Buscar tipos de ausência
    cursor.execute("SELECT * FROM tipos_ausencia WHERE ativo = 1 ORDER BY nome")
    tipos_ausencia = cursor.fetchall()

    return_connection(conn)
    
    return render_template("reunioes/detalhes.html",
                          reuniao=reuniao,
                          presenca=presenca,
                          total_obreiros=total_obreiros,
                          presentes=presentes,
                          ausentes=ausentes,
                          ata_id=ata_id,
                          ata_aprovada=ata_aprovada,
                          ata_numero=ata_numero,
                          ata_ano=ata_ano,
                          ata_conteudo=ata_conteudo,
                          ata_data_criacao=ata_data_criacao,
                          ata_versao=ata_versao,
                          tipos_ausencia=tipos_ausencia)

@app.route("/reunioes/<int:id>/presenca", methods=["POST"])
@admin_required
def registrar_presenca(id):
    cursor, conn = get_db()
    obreiro_id = request.form.get("obreiro_id")
    presente = request.form.get("presente", 0)
    justificativa = request.form.get("justificativa", "")
    tipo_ausencia = request.form.get("tipo_ausencia", None)

    cursor.execute("""
        INSERT INTO presenca (reuniao_id, obreiro_id, presente, justificativa, registrado_por, tipo_ausencia)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (reuniao_id, obreiro_id) 
        DO UPDATE SET presente = %s, justificativa = %s, registrado_por = %s, tipo_ausencia = %s, data_registro = CURRENT_TIMESTAMP
    """, (id, obreiro_id, presente, justificativa, session["user_id"], tipo_ausencia,
          presente, justificativa, session["user_id"], tipo_ausencia))
    conn.commit()
    
    registrar_log("registrar_presenca", "presenca", id, 
                 dados_novos={"obreiro_id": obreiro_id, "presente": presente})
    return_connection(conn)
    flash("Presença registrada com sucesso!", "success")
    return redirect(f"/reunioes/{id}")

@app.route("/reunioes/<int:id>/ata", methods=["GET", "POST"])
@admin_required
def redigir_ata(id):
    cursor, conn = get_db()
    
    if request.method == "POST":
        conteudo = request.form.get("conteudo")
        aprovada = request.form.get("aprovada", 0)
        cursor.execute("""
            INSERT INTO atas (reuniao_id, conteudo, redator_id, aprovada, data_aprovacao)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (reuniao_id) 
            DO UPDATE SET conteudo = %s, redator_id = %s, aprovada = %s, 
                          data_aprovacao = %s, versao = atas.versao + 1
        """, (id, conteudo, session["user_id"], aprovada,
              datetime.now().date() if aprovada else None,
              conteudo, session["user_id"], aprovada,
              datetime.now().date() if aprovada else None))
        conn.commit()
        ata_id = cursor.lastrowid if cursor.lastrowid else id
        registrar_log("criar" if not cursor.lastrowid else "editar", "ata", ata_id)
        flash("Ata salva com sucesso!", "success")
        return_connection(conn)
        return redirect(f"/reunioes/{id}")

    cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
    reuniao = cursor.fetchone()
    cursor.execute("SELECT * FROM atas WHERE reuniao_id = %s", (id,))
    ata = cursor.fetchone()
    return_connection(conn)
    return render_template("reunioes/ata.html", reuniao=reuniao, ata=ata)

@app.route("/reunioes/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_reuniao(id):
    cursor, conn = get_db()
    
    if request.method == "POST":
        titulo = request.form.get("titulo")
        tipo = request.form.get("tipo")
        grau = request.form.get("grau")
        data = request.form.get("data")
        hora_inicio = request.form.get("hora_inicio")
        hora_termino = request.form.get("hora_termino")
        local = request.form.get("local")
        pauta = request.form.get("pauta")
        observacoes = request.form.get("observacoes")
        status = request.form.get("status")
        
        # Validar campos obrigatórios
        if not titulo or not tipo or not data or not hora_inicio:
            flash("Preencha todos os campos obrigatórios (Título, Tipo, Data e Horário)", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{id}/editar")
        
        # Tratar valores vazios
        grau = grau if grau and grau.strip() else None
        if grau:
            try:
                grau = int(grau)
            except ValueError:
                grau = None
        
        hora_termino = hora_termino if hora_termino and hora_termino.strip() else None
        local = local if local and local.strip() else None
        pauta = pauta if pauta and pauta.strip() else None
        observacoes = observacoes if observacoes and observacoes.strip() else None
        
        # Buscar dados antigos para log
        cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
        dados_antigos = dict(cursor.fetchone())
        
        # Converter data e hora
        try:
            data_obj = datetime.strptime(data, '%Y-%m-%d').date() if data else None
            hora_inicio_obj = datetime.strptime(hora_inicio, '%H:%M').time() if hora_inicio else None
            hora_termino_obj = datetime.strptime(hora_termino, '%H:%M').time() if hora_termino else None
        except ValueError as e:
            flash(f"Erro no formato da data/hora: {str(e)}", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{id}/editar")
        
        try:
            cursor.execute("""
                UPDATE reunioes 
                SET titulo = %s, tipo = %s, grau = %s, data = %s, hora_inicio = %s, 
                    hora_termino = %s, local = %s, pauta = %s, observacoes = %s, status = %s
                WHERE id = %s
            """, (titulo, tipo, grau, data_obj, hora_inicio_obj, hora_termino_obj, 
                  local, pauta, observacoes, status, id))
            conn.commit()
            
            registrar_log("editar", "reuniao", id, dados_anteriores=dados_antigos,
                         dados_novos={"titulo": titulo, "data": data, "status": status})
            flash("Reunião atualizada com sucesso!", "success")
            return_connection(conn)
            return redirect(f"/reunioes/{id}")
            
        except Exception as e:
            print(f"ERRO: {e}")
            conn.rollback()
            flash(f"Erro ao atualizar: {str(e)}", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{id}/editar")

    cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
    reuniao = cursor.fetchone()
    cursor.execute("SELECT * FROM tipos_reuniao ORDER BY nome")
    tipos = cursor.fetchall()
    return_connection(conn)
    return render_template("reunioes/editar.html", reuniao=reuniao, tipos=tipos)

@app.route("/reunioes/<int:id>/status", methods=["POST"])
@admin_required
def alterar_status_reuniao(id):
    """Altera o status de uma reunião"""
    try:
        cursor, conn = get_db()
        
        # Obter o novo status do formulário
        novo_status = request.form.get("status")
        
        if not novo_status:
            flash("Status não informado", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{id}")
        
        # Verificar se o status é válido
        status_validos = ['agendada', 'realizada', 'cancelada']
        if novo_status not in status_validos:
            flash("Status inválido", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{id}")
        
        # Buscar dados antigos para log
        cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
        reuniao_antiga = cursor.fetchone()
        
        if not reuniao_antiga:
            flash("Reunião não encontrada", "danger")
            return_connection(conn)
            return redirect("/reunioes")
        
        # Atualizar o status
        cursor.execute("""
            UPDATE reunioes 
            SET status = %s 
            WHERE id = %s
        """, (novo_status, id))
        conn.commit()
        
        # Registrar log
        registrar_log("alterar_status", "reuniao", id, 
                     dados_anteriores={"status": reuniao_antiga["status"]},
                     dados_novos={"status": novo_status})
        
        flash(f"Status da reunião alterado para: {novo_status}", "success")
        return_connection(conn)
        return redirect(f"/reunioes/{id}")
        
    except Exception as e:
        print(f"ERRO ao alterar status: {e}")
        if conn:
            conn.rollback()
            return_connection(conn)
        flash(f"Erro ao alterar status: {str(e)}", "danger")
        return redirect(f"/reunioes/{id}")

@app.route("/reunioes/<int:id>/excluir")
@admin_required
def excluir_reuniao(id):
    cursor, conn = get_db()
    
    # Buscar dados da reunião
    cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
    reuniao = cursor.fetchone()
    
    if not reuniao:
        flash("Reunião não encontrada", "danger")
        return_connection(conn)
        return redirect("/reunioes")
    
    # Verificar se tem ata
    cursor.execute("SELECT id FROM atas WHERE reuniao_id = %s", (id,))
    if cursor.fetchone():
        flash("Não é possível excluir uma reunião que já possui ata.", "danger")
    else:
        dados = dict(reuniao)
        cursor.execute("DELETE FROM reunioes WHERE id = %s", (id,))
        conn.commit()
        registrar_log("excluir", "reuniao", id, dados_anteriores=dados)
        flash("Reunião excluída com sucesso!", "success")
    
    return_connection(conn)
    return redirect("/reunioes")

# =============================
# ROTAS DE PRESENÇA E ESTATÍSTICAS
# =============================
@app.route("/presenca/estatisticas")
@login_required
def estatisticas_presenca():
    cursor, conn = get_db()
    ano = request.args.get('ano', datetime.now().year)

    cursor.execute("""
        SELECT 
            u.id, u.nome_completo, u.grau_atual,
            COUNT(r.id) as total_reunioes,
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
    rows = cursor.fetchall()
    estatisticas = [dict(row) for row in rows]

    cursor.execute("""
        SELECT 
            EXTRACT(MONTH FROM r.data) as mes,
            COUNT(*) as total_reunioes,
            SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presencas
        FROM reunioes r
        LEFT JOIN presenca p ON r.id = p.reuniao_id
        WHERE EXTRACT(YEAR FROM r.data) = %s AND r.status = 'realizada'
        GROUP BY mes
        ORDER BY mes
    """, (str(ano),))
    mensal_rows = cursor.fetchall()
    mensal = [dict(row) for row in mensal_rows]

    return_connection(conn)
    anos = range(2020, datetime.now().year + 1)
    return render_template("presenca/estatisticas.html",
                          estatisticas=estatisticas,
                          mensal=mensal,
                          ano=ano,
                          anos=anos)

@app.route("/presenca/justificar/<int:id>", methods=["GET", "POST"])
@login_required
def justificar_ausencia(id):
    cursor, conn = get_db()

    if request.method == "POST":
        tipo_ausencia = request.form.get("tipo_ausencia")
        justificativa = request.form.get("justificativa")

        cursor.execute("""
            UPDATE presenca 
            SET tipo_ausencia = %s, justificativa = %s, 
                data_registro = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (tipo_ausencia, justificativa, id))
        conn.commit()

        cursor.execute("SELECT reuniao_id FROM presenca WHERE id = %s", (id,))
        presenca = cursor.fetchone()
        reuniao_id = presenca["reuniao_id"] if presenca else None
        
        registrar_log("justificar_ausencia", "presenca", id, 
                     dados_novos={"tipo_ausencia": tipo_ausencia})
        return_connection(conn)
        flash("Ausência justificada com sucesso!", "success")
        return redirect(f"/reunioes/{reuniao_id}")

    cursor.execute("""
        SELECT p.*, r.titulo, r.data as reuniao_data, r.hora_inicio,
               u.nome_completo, u.id as obreiro_id
        FROM presenca p
        JOIN reunioes r ON p.reuniao_id = r.id
        JOIN usuarios u ON p.obreiro_id = u.id
        WHERE p.id = %s
    """, (id,))
    presenca = cursor.fetchone()
    if not presenca:
        flash("Registro de presença não encontrado", "danger")
        return_connection(conn)
        return redirect("/reunioes")

    cursor.execute("SELECT * FROM tipos_ausencia WHERE ativo = 1")
    tipos_ausencia = cursor.fetchall()
    return_connection(conn)
    return render_template("presenca/justificar.html",
                          presenca=presenca,
                          tipos_ausencia=tipos_ausencia)

@app.route("/presenca/validar/<int:id>", methods=["POST"])
@admin_required
def validar_ausencia(id):
    cursor, conn = get_db()
    validar = request.form.get("validar") == "true"
    observacao = request.form.get("observacao", "")

    if validar:
        cursor.execute("""
            UPDATE presenca 
            SET validado_por = %s, data_validacao = CURRENT_TIMESTAMP,
                observacao_validacao = %s
            WHERE id = %s
        """, (session["user_id"], observacao, id))
        registrar_log("validar_ausencia", "presenca", id, dados_novos={"validado": True})
        flash("Ausência validada com sucesso!", "success")
    else:
        cursor.execute("""
            UPDATE presenca 
            SET tipo_ausencia = NULL, justificativa = NULL,
                validado_por = NULL, data_validacao = NULL,
                observacao_validacao = %s
            WHERE id = %s
        """, (observacao, id))
        registrar_log("rejeitar_ausencia", "presenca", id)
        flash("Validação removida!", "success")

    conn.commit()
    return_connection(conn)
    return redirect(request.referrer or "/reunioes")

@app.route("/presenca/alertas")
@admin_required
def listar_alertas():
    cursor, conn = get_db()
    cursor.execute("""
        SELECT a.*, u.nome_completo, u.grau_atual,
               ru.nome_completo as resolvido_por_nome
        FROM alertas_presenca a
        JOIN usuarios u ON a.obreiro_id = u.id
        LEFT JOIN usuarios ru ON a.resolvido_por = ru.id
        WHERE a.resolvido = 0
        ORDER BY a.data_gerado DESC
    """)
    alertas = cursor.fetchall()
    return_connection(conn)
    return render_template("presenca/alertas.html", alertas=alertas)

@app.route("/presenca/alerta/<int:id>/resolver", methods=["POST"])
@admin_required
def resolver_alerta(id):
    cursor, conn = get_db()
    cursor.execute("""
        UPDATE alertas_presenca 
        SET resolvido = 1, data_resolucao = CURRENT_TIMESTAMP,
            resolvido_por = %s
        WHERE id = %s
    """, (session["user_id"], id))
    conn.commit()
    registrar_log("resolver_alerta", "alerta", id)
    return_connection(conn)
    flash("Alerta marcado como resolvido", "success")
    return redirect("/presenca/alertas")

@app.route("/api/gerar_alertas")
@admin_required
def gerar_alertas():
    cursor, conn = get_db()
    mes_atual = datetime.now().strftime('%Y-%m')
    ano_atual = datetime.now().year

    cursor.execute("""
        SELECT 
            p.obreiro_id,
            u.nome_completo,
            COUNT(*) as ausencias,
            %s as mes
        FROM presenca p
        JOIN usuarios u ON p.obreiro_id = u.id
        JOIN reunioes r ON p.reuniao_id = r.id
        WHERE p.presente = 0 
          AND p.tipo_ausencia IS NULL
          AND r.status = 'realizada'
          AND TO_CHAR(r.data, 'YYYY-MM') = %s
        GROUP BY p.obreiro_id, u.nome_completo
        HAVING COUNT(*) >= 3
    """, (mes_atual, mes_atual))
    alertas_ausencias = cursor.fetchall()

    for a in alertas_ausencias:
        cursor.execute("""
            INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
            VALUES (%s, %s, %s)
        """, (a["obreiro_id"], "limite_atingido",
               f"{a['nome_completo']} possui {a['ausencias']} ausências injustificadas em {a['mes']}"))

    cursor.execute("""
        SELECT 
            u.id,
            u.nome_completo,
            COUNT(r.id) as total_reunioes,
            SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presencas
        FROM usuarios u
        LEFT JOIN presenca p ON u.id = p.obreiro_id
        LEFT JOIN reunioes r ON p.reuniao_id = r.id AND EXTRACT(YEAR FROM r.data) = %s
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
            cursor.execute("""
                INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                VALUES (%s, %s, %s)
            """, (e["id"], "presenca_critica",
                   f"{e['nome_completo']} tem apenas {percentual:.1f}% de presença no ano {ano_atual} (CRÍTICO)"))
        elif percentual < 75:
            cursor.execute("""
                INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                VALUES (%s, %s, %s)
            """, (e["id"], "presenca_atencao",
                   f"{e['nome_completo']} tem {percentual:.1f}% de presença no ano {ano_atual} (ATENÇÃO)"))

    conn.commit()
    registrar_log("gerar_alertas", "alertas", None, dados_novos={"quantidade": len(alertas_ausencias)})
    return_connection(conn)

    flash(f"Alertas gerados! ({len(alertas_ausencias)} por ausências + alertas de presença)", "success")
    return redirect("/presenca/alertas")

# =============================
# ROTAS DE ATAS
# =============================
@app.route("/atas")
@login_required
def listar_atas():
    cursor, conn = get_db()
    
    data_ini = request.args.get('data_ini', '')
    data_fim = request.args.get('data_fim', '')
    aprovada = request.args.get('aprovada', '')
    reuniao_titulo = request.args.get('reuniao_titulo', '')
    
    nivel_usuario = session.get("nivel_acesso", 1)
    tipo_usuario = session.get("tipo", "obreiro")
    
    query = """
        SELECT a.*, 
               r.titulo as reuniao_titulo,
               r.data as reuniao_data,
               r.grau as reuniao_grau,
               u.nome_completo as redator_nome,
               (SELECT COUNT(*) FROM assinaturas_ata WHERE ata_id = a.id) as total_assinaturas
        FROM atas a
        JOIN reunioes r ON a.reuniao_id = r.id
        LEFT JOIN usuarios u ON a.redator_id = u.id
        WHERE 1=1
    """
    params = []
    
    # Filtrar por nível de acesso
    if tipo_usuario != "admin":
        if nivel_usuario == 1:  # Aprendiz
            query += " AND r.grau = 1"
        elif nivel_usuario == 2:  # Companheiro
            query += " AND r.grau IN (1, 2)"
        # Mestre (nivel 3) vê todas
    
    if data_ini:
        query += " AND r.data >= %s"
        params.append(data_ini)
    if data_fim:
        query += " AND r.data <= %s"
        params.append(data_fim)
    if aprovada != '':
        query += " AND a.aprovada = %s"
        params.append(aprovada)
    if reuniao_titulo:
        query += " AND r.titulo LIKE %s"
        params.append(f"%{reuniao_titulo}%")
    
    query += " ORDER BY a.data_criacao DESC"
    
    cursor.execute(query, params)
    atas = cursor.fetchall()
    return_connection(conn)
    
    return render_template("atas/lista.html", atas=atas, 
                          filtros={'data_ini': data_ini, 'data_fim': data_fim, 
                                  'aprovada': aprovada, 'reuniao_titulo': reuniao_titulo})

@app.route("/atas/nova/<int:reuniao_id>", methods=["GET", "POST"])
@admin_required
def nova_ata(reuniao_id):
    cursor, conn = get_db()
    
    # Buscar reunião - sem verificar status
    cursor.execute("""
        SELECT r.*, 
               (SELECT COUNT(*) FROM presenca WHERE reuniao_id = r.id AND presente = 1) as presentes
        FROM reunioes r
        WHERE r.id = %s
    """, (reuniao_id,))
    reuniao = cursor.fetchone()
    
    if not reuniao:
        flash("Reunião não encontrada", "danger")
        return_connection(conn)
        return redirect("/reunioes")

    if request.method == "POST":
        conteudo = request.form.get("conteudo")
        modelo_id = request.form.get("modelo_id")
        
        # Tratar modelo_id
        modelo_id = modelo_id if modelo_id and modelo_id.strip() else None
        if modelo_id:
            try:
                modelo_id = int(modelo_id)
            except ValueError:
                modelo_id = None
        
        ano = datetime.now().year
        cursor.execute("SELECT COUNT(*) as total FROM atas WHERE ano_ata = %s", (ano,))
        total = cursor.fetchone()["total"]
        numero_ata = total + 1
        
        try:
            cursor.execute("""
                INSERT INTO atas 
                (reuniao_id, conteudo, redator_id, numero_ata, ano_ata, tipo_ata, data_criacao)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (reuniao_id, conteudo, session["user_id"], numero_ata, ano, reuniao["tipo"]))
            ata_id = cursor.lastrowid
            conn.commit()
            
            registrar_log("criar", "ata", ata_id, dados_novos={"reuniao_id": reuniao_id, "numero": numero_ata, "ano": ano})
            flash(f"Ata nº {numero_ata}/{ano} criada com sucesso!", "success")
            return_connection(conn)
            return redirect(f"/atas/{ata_id}")
            
        except Exception as e:
            print(f"ERRO ao criar ata: {e}")
            conn.rollback()
            flash(f"Erro ao criar ata: {str(e)}", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{reuniao_id}")

    cursor.execute("SELECT * FROM modelos_ata WHERE ativo = 1")
    modelos = cursor.fetchall()
    return_connection(conn)
    return render_template("atas/nova.html", reuniao=reuniao, modelos=modelos)

@app.route("/atas/<int:id>")
@login_required
@nivel_ata_required()  # Decorator específico para atas
def visualizar_ata(id):
    cursor, conn = get_db()
    cursor.execute("""
        SELECT a.*, 
               r.titulo as reuniao_titulo,
               r.data as reuniao_data,
               r.hora_inicio,
               r.hora_termino,
               r.local,
               r.pauta,
               r.grau as reuniao_grau,
               u.nome_completo as redator_nome,
               u2.nome_completo as aprovado_por_nome
        FROM atas a
        JOIN reunioes r ON a.reuniao_id = r.id
        LEFT JOIN usuarios u ON a.redator_id = u.id
        LEFT JOIN usuarios u2 ON a.aprovada_por = u2.id
        WHERE a.id = %s
    """, (id,))
    ata = cursor.fetchone()
    
    if not ata:
        flash("Ata não encontrada", "danger")
        return_connection(conn)
        return redirect("/atas")
    
    # Verificação adicional (segurança)
    nivel_usuario = session.get("nivel_acesso", 1)
    tipo_usuario = session.get("tipo", "obreiro")
    reuniao_grau = ata.get('reuniao_grau') or 1
    
    if tipo_usuario != "admin":
        if nivel_usuario == 1 and reuniao_grau != 1:
            flash("Você não tem permissão para visualizar esta ata", "danger")
            return_connection(conn)
            return redirect("/dashboard")
        elif nivel_usuario == 2 and reuniao_grau > 2:
            flash("Você não tem permissão para visualizar esta ata", "danger")
            return_connection(conn)
            return redirect("/dashboard")
    
    # Resto do código...
    cursor.execute("""
        SELECT u.nome_completo, u.grau_atual,
               p.presente, p.tipo_ausencia,
               c.nome as cargo_nome
        FROM presenca p
        JOIN usuarios u ON p.obreiro_id = u.id
        LEFT JOIN ocupacao_cargos oc ON u.id = oc.obreiro_id AND oc.ativo = 1
        LEFT JOIN cargos c ON oc.cargo_id = c.id
        WHERE p.reuniao_id = %s
        ORDER BY 
            CASE WHEN c.ordem IS NOT NULL THEN c.ordem ELSE 999 END,
            u.grau_atual DESC,
            u.nome_completo
    """, (ata["reuniao_id"],))
    presenca = cursor.fetchall()

    cursor.execute("""
        SELECT ass.*, u.nome_completo, c.nome as cargo_nome
        FROM assinaturas_ata ass
        JOIN usuarios u ON ass.obreiro_id = u.id
        LEFT JOIN cargos c ON ass.cargo_id = c.id
        WHERE ass.ata_id = %s
        ORDER BY ass.data_assinatura
    """, (id,))
    assinaturas = cursor.fetchall()
    return_connection(conn)
    
    return render_template("atas/visualizar.html",
                          ata=ata,
                          presenca=presenca,
                          assinaturas=assinaturas)

@app.route("/atas/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_ata(id):
    cursor, conn = get_db()
    
    # Buscar ata
    cursor.execute("""
        SELECT a.*, r.titulo as reuniao_titulo, r.status as reuniao_status
        FROM atas a
        JOIN reunioes r ON a.reuniao_id = r.id
        WHERE a.id = %s
    """, (id,))
    ata = cursor.fetchone()
    
    if not ata:
        flash("Ata não encontrada", "danger")
        return_connection(conn)
        return redirect("/atas")
    
    # Verificar se já está aprovada
    if ata["aprovada"] == 1:
        flash("Ata já aprovada, não pode ser editada!", "warning")
        return_connection(conn)
        return redirect(f"/atas/{id}")

    if request.method == "POST":
        conteudo = request.form.get("conteudo")
        
        if not conteudo:
            flash("Conteúdo da ata é obrigatório", "danger")
        else:
            try:
                cursor.execute("""
                    UPDATE atas 
                    SET conteudo = %s, versao = versao + 1
                    WHERE id = %s
                """, (conteudo, id))
                conn.commit()
                
                registrar_log("editar", "ata", id, dados_novos={"versao": ata["versao"] + 1})
                flash("Ata atualizada com sucesso!", "success")
                return_connection(conn)
                return redirect(f"/atas/{id}")
                
            except Exception as e:
                flash(f"Erro ao atualizar ata: {str(e)}", "danger")
                conn.rollback()
    
    return_connection(conn)
    return render_template("atas/editar.html", ata=ata)

@app.route("/atas/<int:id>/aprovar", methods=["POST"])
@admin_required
def aprovar_ata(id):
    cursor, conn = get_db()
    cursor.execute("""
        UPDATE atas 
        SET aprovada = 1, 
            aprovada_em = CURRENT_DATE,
            aprovada_por = %s
        WHERE id = %s
    """, (session["user_id"], id))
    conn.commit()
    registrar_log("aprovar", "ata", id, dados_novos={"aprovada": 1})
    flash("Ata aprovada com sucesso!", "success")
    return_connection(conn)
    return redirect(f"/atas/{id}")

@app.route("/atas/<int:id>/assinar", methods=["POST"])
@login_required
def assinar_ata(id):
    cursor, conn = get_db()
    cursor.execute("SELECT id FROM assinaturas_ata WHERE ata_id = %s AND obreiro_id = %s", (id, session["user_id"]))
    if cursor.fetchone():
        flash("Você já assinou esta ata", "warning")
    else:
        cursor.execute("""
            SELECT cargo_id FROM ocupacao_cargos 
            WHERE obreiro_id = %s AND ativo = 1
            ORDER BY data_inicio DESC LIMIT 1
        """, (session["user_id"],))
        cargo = cursor.fetchone()
        cursor.execute("""
            INSERT INTO assinaturas_ata (ata_id, obreiro_id, cargo_id, ip_assinatura)
            VALUES (%s, %s, %s, %s)
        """, (id, session["user_id"], cargo["cargo_id"] if cargo else None, request.remote_addr))
        conn.commit()
        registrar_log("assinar", "ata", id)
        flash("Ata assinada com sucesso!", "success")
    return_connection(conn)
    return redirect(f"/atas/{id}")

@app.route("/atas/<int:id>/pdf")
@login_required
def gerar_pdf_ata(id):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from io import BytesIO

        cursor, conn = get_db()
        cursor.execute("""
            SELECT a.*, 
                   r.titulo as reuniao_titulo,
                   r.data as reuniao_data,
                   r.hora_inicio,
                   r.hora_termino,
                   r.local,
                   u.nome_completo as redator_nome
            FROM atas a
            JOIN reunioes r ON a.reuniao_id = r.id
            LEFT JOIN usuarios u ON a.redator_id = u.id
            WHERE a.id = %s
        """, (id,))
        ata = cursor.fetchone()
        if not ata:
            flash("Ata não encontrada", "danger")
            return_connection(conn)
            return redirect("/atas")

        cursor.execute("""
            SELECT u.nome_completo, 
                   CASE WHEN p.presente = 1 THEN 'Presente' ELSE 'Ausente' END as status
            FROM presenca p
            JOIN usuarios u ON p.obreiro_id = u.id
            WHERE p.reuniao_id = %s
            ORDER BY u.nome_completo
        """, (ata["reuniao_id"],))
        presenca = cursor.fetchall()

        cursor.execute("""
            SELECT u.nome_completo, c.nome as cargo
            FROM assinaturas_ata ass
            JOIN usuarios u ON ass.obreiro_id = u.id
            LEFT JOIN cargos c ON ass.cargo_id = c.id
            WHERE ass.ata_id = %s
        """, (id,))
        assinaturas = cursor.fetchall()
        return_connection(conn)

        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4,
                               rightMargin=72, leftMargin=72,
                               topMargin=72, bottomMargin=72)
        styles = getSampleStyleSheet()
        elementos = []

        styles.add(ParagraphStyle(name='CenteredTitle',
                                 parent=styles['Title'],
                                 alignment=1,
                                 spaceAfter=30))
        titulo = Paragraph(f"ATA Nº {ata['numero_ata']}/{ata['ano_ata']}", styles['CenteredTitle'])
        elementos.append(titulo)
        elementos.append(Spacer(1, 0.5*cm))

        info_data = [
            ["Reunião:", ata["reuniao_titulo"]],
            ["Data:", f"{ata['reuniao_data']} {ata['hora_inicio']} às {ata['hora_termino']}"],
            ["Local:", ata["local"] or "Não informado"],
            ["Redator:", ata["redator_nome"] or "Sistema"],
            ["Data de Criação:", ata["data_criacao"].strftime("%d/%m/%Y %H:%M") if ata["data_criacao"] else "N/A"]
        ]
        info_table = Table(info_data, colWidths=[4*cm, 12*cm])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#ecf0f1')),
        ]))
        elementos.append(info_table)
        elementos.append(Spacer(1, 0.5*cm))

        elementos.append(Paragraph("<b>ATA DA REUNIÃO</b>", styles['Heading2']))
        elementos.append(Spacer(1, 0.3*cm))
        elementos.append(Paragraph(ata["conteudo"].replace('\n', '<br/>'), styles['Normal']))
        elementos.append(Spacer(1, 0.5*cm))

        elementos.append(Paragraph("<b>LISTA DE PRESENÇA</b>", styles['Heading2']))
        elementos.append(Spacer(1, 0.3*cm))
        presenca_data = [["Obreiro", "Status"]]
        for p in presenca:
            presenca_data.append([p["nome_completo"], p["status"]])
        presenca_table = Table(presenca_data, colWidths=[12*cm, 4*cm])
        presenca_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c3e50')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ]))
        elementos.append(presenca_table)
        elementos.append(Spacer(1, 0.5*cm))

        if assinaturas:
            elementos.append(Paragraph("<b>ASSINATURAS</b>", styles['Heading2']))
            elementos.append(Spacer(1, 0.3*cm))
            for i in range(0, len(assinaturas), 2):
                linha = assinaturas[i:i+2]
                dados_linha = []
                for ass in linha:
                    dados_linha.append(f"{ass['nome_completo']}\n{ass['cargo'] or 'Obreiro'}")
                while len(dados_linha) < 2:
                    dados_linha.append("")
                linha_table = Table([dados_linha], colWidths=[8*cm, 8*cm])
                linha_table.setStyle(TableStyle([
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('LINEABOVE', (0, 0), (-1, -1), 1, colors.black),
                    ('TOPPADDING', (0, 0), (-1, -1), 20),
                ]))
                elementos.append(linha_table)
                elementos.append(Spacer(1, 0.3*cm))

        elementos.append(Spacer(1, 1*cm))
        data_emissao = datetime.now().strftime("%d/%m/%Y %H:%M")
        rodape = Paragraph(f"<i>Documento gerado em {data_emissao} - Sistema Maçônico</i>", styles['Italic'])
        elementos.append(rodape)

        doc.build(elementos)
        buffer.seek(0)

        nome_arquivo = f"ata_{ata['numero_ata']}_{ata['ano_ata']}.pdf"
        return send_file(buffer, as_attachment=True, download_name=nome_arquivo, mimetype='application/pdf')
    except ImportError:
        flash("Biblioteca reportlab não instalada. Execute: pip install reportlab", "warning")
        return redirect("/atas")
    except Exception as e:
        flash(f"Erro ao gerar PDF: {str(e)}", "danger")
        return redirect("/atas")

@app.route("/atas/modelos")
@admin_required
def listar_modelos():
    cursor, conn = get_db()
    cursor.execute("SELECT * FROM modelos_ata WHERE ativo = 1 ORDER BY nome")
    rows = cursor.fetchall()
    modelos = []
    for row in rows:
        modelo = dict(row)
        try:
            modelo["estrutura_dict"] = json.loads(modelo["estrutura"])
        except:
            modelo["estrutura_dict"] = {}
        modelos.append(modelo)
    return_connection(conn)
    return render_template("atas/modelos.html", modelos=modelos)

@app.route("/atas/modelos/novo", methods=["GET", "POST"])
@admin_required
def novo_modelo():
    if request.method == "POST":
        nome = request.form.get("nome")
        descricao = request.form.get("descricao")
        tipo = request.form.get("tipo")
        estrutura = request.form.get("estrutura")
        if not nome or not estrutura:
            flash("Nome e estrutura são obrigatórios", "danger")
        else:
            cursor, conn = get_db()
            cursor.execute("""
                INSERT INTO modelos_ata (nome, descricao, tipo, estrutura, created_by)
                VALUES (%s, %s, %s, %s, %s)
            """, (nome, descricao, tipo, estrutura, session["user_id"]))
            conn.commit()
            modelo_id = cursor.lastrowid
            registrar_log("criar", "modelo_ata", modelo_id, dados_novos={"nome": nome})
            flash("Modelo criado com sucesso!", "success")
            return_connection(conn)
            return redirect("/atas/modelos")
    return render_template("atas/modelo_form.html")

# =============================
# ROTAS DE COMUNICADOS
# =============================
@app.route("/comunicados")
@login_required
def listar_comunicados():
    cursor, conn = get_db()
    
    tipo = request.args.get('tipo', '')
    prioridade = request.args.get('prioridade', '')
    data_ini = request.args.get('data_ini', '')
    data_fim = request.args.get('data_fim', '')
    ativo = request.args.get('ativo', '')

    hoje = datetime.now().strftime("%Y-%m-%d")

    query = """
        SELECT c.*, u.nome_completo as autor_nome,
               (SELECT COUNT(*) FROM visualizacoes_comunicado WHERE comunicado_id = c.id AND obreiro_id = %s) as ja_visto
        FROM comunicados c
        JOIN usuarios u ON c.criado_por = u.id
        WHERE 1=1
    """
    params = [session["user_id"]]

    if tipo:
        query += " AND c.tipo = %s"
        params.append(tipo)
    if prioridade:
        query += " AND c.prioridade = %s"
        params.append(prioridade)
    if data_ini:
        query += " AND c.data_inicio >= %s"
        params.append(data_ini)
    if data_fim:
        query += " AND c.data_fim <= %s"
        params.append(data_fim)
    if ativo != '':
        query += " AND c.ativo = %s"
        params.append(ativo)
    else:
        query += " AND c.ativo = 1 AND c.data_inicio <= %s AND (c.data_fim IS NULL OR c.data_fim >= %s)"
        params.extend([hoje, hoje])

    query += " ORDER BY c.prioridade = 'urgente' DESC, c.data_criacao DESC"

    cursor.execute(query, params)
    comunicados = cursor.fetchall()

    cursor.execute("SELECT DISTINCT tipo FROM comunicados ORDER BY tipo")
    tipos = cursor.fetchall()
    cursor.execute("SELECT DISTINCT prioridade FROM comunicados ORDER BY prioridade")
    prioridades = cursor.fetchall()

    return_connection(conn)
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
        cursor, conn = get_db()
        cursor.execute("""
            INSERT INTO comunicados (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, criado_por)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, session["user_id"]))
        conn.commit()
        comunicado_id = cursor.lastrowid
        
        registrar_log("criar", "comunicado", comunicado_id, dados_novos={"titulo": titulo, "prioridade": prioridade})
        flash("Comunicado publicado com sucesso!", "success")
        return_connection(conn)
        return redirect("/comunicados")
    hoje = datetime.now().strftime("%Y-%m-%d")
    return render_template("comunicados/novo.html", hoje=hoje)

@app.route("/comunicados/<int:id>/visualizar")
@login_required
def visualizar_comunicado(id):
    cursor, conn = get_db()
    cursor.execute("""
        INSERT INTO visualizacoes_comunicado (comunicado_id, obreiro_id)
        VALUES (%s, %s)
        ON CONFLICT (comunicado_id, obreiro_id) DO NOTHING
    """, (id, session["user_id"]))
    conn.commit()
    cursor.execute("""
        SELECT c.*, u.nome_completo as autor_nome
        FROM comunicados c
        JOIN usuarios u ON c.criado_por = u.id
        WHERE c.id = %s
    """, (id,))
    comunicado = cursor.fetchone()
    return_connection(conn)
    if not comunicado:
        flash("Comunicado não encontrado", "danger")
        return redirect("/comunicados")
    return render_template("comunicados/detalhes.html", comunicado=comunicado)

@app.route("/comunicados/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_comunicado(id):
    cursor, conn = get_db()
    
    if request.method == "POST":
        titulo = request.form.get("titulo")
        conteudo = request.form.get("conteudo")
        tipo = request.form.get("tipo")
        prioridade = request.form.get("prioridade")
        data_inicio = request.form.get("data_inicio")
        data_fim = request.form.get("data_fim") or None
        ativo = 1 if request.form.get("ativo") else 0
        
        cursor.execute("SELECT * FROM comunicados WHERE id = %s", (id,))
        dados_antigos = dict(cursor.fetchone())
        
        cursor.execute("""
            UPDATE comunicados
            SET titulo=%s, conteudo=%s, tipo=%s, prioridade=%s, data_inicio=%s, data_fim=%s, ativo=%s
            WHERE id=%s
        """, (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, ativo, id))
        conn.commit()
        
        registrar_log("editar", "comunicado", id, dados_anteriores=dados_antigos,
                     dados_novos={"titulo": titulo, "prioridade": prioridade})
        flash("Comunicado atualizado com sucesso!", "success")
        return_connection(conn)
        return redirect("/comunicados")
    
    cursor.execute("SELECT * FROM comunicados WHERE id = %s", (id,))
    comunicado = cursor.fetchone()
    return_connection(conn)
    return render_template("comunicados/editar.html", comunicado=comunicado)

@app.route("/comunicados/<int:id>/excluir")
@admin_required
def excluir_comunicado(id):
    cursor, conn = get_db()
    
    cursor.execute("SELECT * FROM comunicados WHERE id = %s", (id,))
    comunicado = cursor.fetchone()
    
    if comunicado:
        dados = dict(comunicado)
        cursor.execute("DELETE FROM comunicados WHERE id = %s", (id,))
        conn.commit()
        registrar_log("excluir", "comunicado", id, dados_anteriores=dados)
        flash("Comunicado excluído com sucesso!", "success")
    else:
        flash("Comunicado não encontrado", "danger")
    
    return_connection(conn)
    return redirect("/comunicados")

# =============================
# ROTAS DE FORMULÁRIO DE CANDIDATOS
# =============================
@app.route("/candidato/formulario/<int:candidato_id>", methods=["GET", "POST"])
@login_required
def formulario_candidato(candidato_id):
    """Formulário completo do candidato"""
    cursor, conn = get_db()
    
    # Buscar candidato
    cursor.execute("SELECT * FROM candidatos WHERE id = %s", (candidato_id,))
    candidato = cursor.fetchone()
    
    if not candidato:
        flash("Candidato não encontrado", "danger")
        return_connection(conn)
        return redirect("/candidatos")
    
    # Buscar filhos do candidato
    cursor.execute("SELECT * FROM filhos_candidato WHERE candidato_id = %s ORDER BY data_nascimento", (candidato_id,))
    filhos = cursor.fetchall()
    
    if request.method == "POST":
        # Coletar dados do formulário
        dados = {
            'loja_nome': request.form.get('loja_nome'),
            'loja_numero': request.form.get('loja_numero'),
            'data_nascimento': request.form.get('data_nascimento') or None,
            'naturalidade': request.form.get('naturalidade'),
            'uf_naturalidade': request.form.get('uf_naturalidade'),
            'nacionalidade': request.form.get('nacionalidade'),
            'cpf': request.form.get('cpf'),
            'rg': request.form.get('rg'),
            'orgao_expedidor': request.form.get('orgao_expedidor'),
            'telefone_fixo': request.form.get('telefone_fixo'),
            'celular': request.form.get('celular'),
            'email': request.form.get('email'),
            'grau_instrucao': request.form.get('grau_instrucao'),
            'endereco_residencial': request.form.get('endereco_residencial'),
            'numero_residencial': request.form.get('numero_residencial'),
            'bairro': request.form.get('bairro'),
            'cidade': request.form.get('cidade'),
            'uf_residencial': request.form.get('uf_residencial'),
            'cep': request.form.get('cep'),
            'tipo_sanguineo': request.form.get('tipo_sanguineo'),
            'nome_pai': request.form.get('nome_pai'),
            'nome_mae': request.form.get('nome_mae'),
            'estado_civil': request.form.get('estado_civil'),
            'data_casamento': request.form.get('data_casamento') or None,
            'nome_conjuge': request.form.get('nome_conjuge'),
            'data_nascimento_conjuge': request.form.get('data_nascimento_conjuge') or None,
            'profissao': request.form.get('profissao'),
            'empregador': request.form.get('empregador'),
            'endereco_profissional': request.form.get('endereco_profissional'),
            'bairro_profissional': request.form.get('bairro_profissional'),
            'cidade_profissional': request.form.get('cidade_profissional'),
            'uf_profissional': request.form.get('uf_profissional'),
            'cep_profissional': request.form.get('cep_profissional'),
            'telefone_comercial': request.form.get('telefone_comercial')
        }
        
        # Atualizar candidato
        update_sql = """
            UPDATE candidatos SET
                loja_nome = %s, loja_numero = %s, data_nascimento = %s,
                naturalidade = %s, uf_naturalidade = %s, nacionalidade = %s,
                cpf = %s, rg = %s, orgao_expedidor = %s,
                telefone_fixo = %s, celular = %s, email = %s,
                grau_instrucao = %s, endereco_residencial = %s,
                numero_residencial = %s, bairro = %s, cidade = %s,
                uf_residencial = %s, cep = %s, tipo_sanguineo = %s,
                nome_pai = %s, nome_mae = %s, estado_civil = %s,
                data_casamento = %s, nome_conjuge = %s,
                data_nascimento_conjuge = %s, profissao = %s,
                empregador = %s, endereco_profissional = %s,
                bairro_profissional = %s, cidade_profissional = %s,
                uf_profissional = %s, cep_profissional = %s,
                telefone_comercial = %s
            WHERE id = %s
        """
        
        values = list(dados.values()) + [candidato_id]
        cursor.execute(update_sql, values)
        
        # Processar filhos
        cursor.execute("DELETE FROM filhos_candidato WHERE candidato_id = %s", (candidato_id,))
        
        filhos_nomes = request.form.getlist('filho_nome[]')
        filhos_datas = request.form.getlist('filho_data[]')
        
        for i in range(len(filhos_nomes)):
            if filhos_nomes[i] and filhos_nomes[i].strip():
                cursor.execute("""
                    INSERT INTO filhos_candidato (candidato_id, nome, data_nascimento)
                    VALUES (%s, %s, %s)
                """, (candidato_id, filhos_nomes[i], filhos_datas[i] or None))
        
        conn.commit()
        registrar_log("preencher_formulario", "candidato", candidato_id, dados_novos={"nome": candidato["nome"]})
        
        flash("Formulário do candidato salvo com sucesso!", "success")
        return_connection(conn)
        return redirect(f"/sindicancia/{candidato_id}")
    
    return_connection(conn)
    return render_template("candidatos/formulario.html", candidato=candidato, filhos=filhos)
    
# =============================
# UPLOAD DE FOTO DO OBREIRO
# =============================

import os
from werkzeug.utils import secure_filename
from PIL import Image

# Configuração de upload de fotos
UPLOAD_FOLDER_FOTOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads', 'fotos')
ALLOWED_EXTENSIONS_FOTOS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}

# Criar pasta se não existir
os.makedirs(UPLOAD_FOLDER_FOTOS, exist_ok=True)

def allowed_foto(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS_FOTOS

def redimensionar_foto(caminho_origem, tamanho=(300, 300)):
    """Redimensiona e otimiza a foto"""
    try:
        img = Image.open(caminho_origem)
        img.thumbnail(tamanho, Image.Resampling.LANCZOS)
        
        # Converter para RGB se for PNG com transparência
        if img.mode in ('RGBA', 'LA'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
            img = background
        
        img.save(caminho_origem, 'JPEG', quality=85, optimize=True)
        return True
    except Exception as e:
        print(f"Erro ao redimensionar foto: {e}")
        return False

@app.route("/obreiros/<int:id>/foto", methods=["POST"])
@login_required
def upload_foto_obreiro(id):
    """Upload da foto do obreiro"""
    # Verificar permissão
    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para alterar esta foto", "danger")
        return redirect(f"/obreiros/{id}")
    
    if 'foto' not in request.files:
        flash("Nenhum arquivo selecionado", "danger")
        return redirect(f"/obreiros/{id}/editar")
    
    file = request.files['foto']
    
    if file.filename == '':
        flash("Nenhum arquivo selecionado", "danger")
        return redirect(f"/obreiros/{id}/editar")
    
    if not allowed_foto(file.filename):
        flash("Tipo de arquivo não permitido. Use: PNG, JPG, JPEG, GIF, WEBP", "danger")
        return redirect(f"/obreiros/{id}/editar")
    
    try:
        cursor, conn = get_db()
        
        # Buscar foto antiga para deletar
        cursor.execute("SELECT foto FROM usuarios WHERE id = %s", (id,))
        foto_antiga = cursor.fetchone()
        
        # Gerar nome único para o arquivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = secure_filename(f"{id}_{timestamp}_{file.filename}")
        caminho = os.path.join(UPLOAD_FOLDER_FOTOS, filename)
        
        # Salvar arquivo
        file.save(caminho)
        
        # Redimensionar foto
        redimensionar_foto(caminho)
        
        # Atualizar banco
        cursor.execute("UPDATE usuarios SET foto = %s WHERE id = %s", (filename, id))
        conn.commit()
        
        # Deletar foto antiga
        if foto_antiga and foto_antiga['foto']:
            caminho_antigo = os.path.join(UPLOAD_FOLDER_FOTOS, foto_antiga['foto'])
            if os.path.exists(caminho_antigo):
                os.remove(caminho_antigo)
        
        registrar_log("upload_foto", "obreiro", id, dados_novos={"foto": filename})
        flash("Foto atualizada com sucesso!", "success")
        return_connection(conn)
        return redirect(f"/obreiros/{id}")
        
    except Exception as e:
        print(f"Erro ao fazer upload: {e}")
        flash(f"Erro ao fazer upload: {str(e)}", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{id}/editar")

@app.route("/obreiros/<int:id>/foto/remover")
@login_required
def remover_foto_obreiro(id):
    """Remove a foto do obreiro"""
    # Verificar permissão
    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para remover esta foto", "danger")
        return redirect(f"/obreiros/{id}")
    
    try:
        cursor, conn = get_db()
        
        cursor.execute("SELECT foto FROM usuarios WHERE id = %s", (id,))
        foto = cursor.fetchone()
        
        if foto and foto['foto']:
            caminho = os.path.join(UPLOAD_FOLDER_FOTOS, foto['foto'])
            if os.path.exists(caminho):
                os.remove(caminho)
        
        cursor.execute("UPDATE usuarios SET foto = NULL WHERE id = %s", (id,))
        conn.commit()
        
        registrar_log("remover_foto", "obreiro", id)
        flash("Foto removida com sucesso!", "success")
        return_connection(conn)
        return redirect(f"/obreiros/{id}")
        
    except Exception as e:
        print(f"Erro ao remover foto: {e}")
        flash(f"Erro ao remover foto: {str(e)}", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{id}/editar")
        
@app.route("/uploads/fotos/<filename>")
def serve_foto(filename):
    """Serve as fotos dos obreiros"""
    from flask import send_from_directory
    return send_from_directory(UPLOAD_FOLDER_FOTOS, filename)        

# =============================
# ROTAS DE CANDIDATOS E SINDICÂNCIA
# =============================
@app.route("/candidatos", methods=["GET", "POST"])
@admin_required
def gerenciar_candidatos():
    cursor, conn = get_db()
    
    if request.method == "POST":
        nome = request.form["nome"].strip()
        if nome:
            agora = datetime.now()
            cursor.execute("INSERT INTO candidatos (nome, data_criacao) VALUES (%s, %s)", (nome, agora))
            conn.commit()
            candidato_id = cursor.lastrowid
            registrar_log("criar", "candidato", candidato_id, dados_novos={"nome": nome})
            flash(f"Candidato '{nome}' adicionado com sucesso!", "success")
        else:
            flash("Nome do candidato não pode estar vazio", "danger")
    
    # Buscar candidatos com informações de sindicantes
    cursor.execute("""
        SELECT c.*,
               (SELECT COUNT(*) FROM sindicancias WHERE candidato_id = c.id) as total_votos,
               (SELECT COUNT(*) FROM sindicancias WHERE candidato_id = c.id AND parecer = 'positivo') as votos_positivos,
               (SELECT COUNT(*) FROM sindicancias WHERE candidato_id = c.id AND parecer = 'negativo') as votos_negativos
        FROM candidatos c
        ORDER BY c.data_criacao DESC
    """)
    candidatos = cursor.fetchall()
    
    # Buscar sindicantes ativos para o menu suspenso
    cursor.execute("""
        SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, ativo 
        FROM usuarios 
        WHERE tipo = 'sindicante' AND ativo = 1
        ORDER BY nome_completo
    """)
    sindicantes = cursor.fetchall()
    
    return_connection(conn)
    return render_template("candidatos.html", 
                          candidatos=candidatos, 
                          sindicantes=sindicantes,
                          tipo=session["tipo"])

@app.route("/sindicancia/<int:id>", methods=["GET", "POST"])
@login_required
def visualizar_sindicancia(id):
    if session["tipo"] == "admin":
        flash("Administradores não podem emitir pareceres", "warning")
        return redirect("/candidatos")

    cursor, conn = get_db()
    cursor.execute("SELECT * FROM candidatos WHERE id = %s", (id,))
    candidato = cursor.fetchone()
    if not candidato:
        flash("Candidato não encontrado", "danger")
        return_connection(conn)
        return redirect("/minhas_sindicancias")

    bloqueado = candidato["fechado"] == 1
    usuario = session["usuario"]

    if request.method == "POST" and not bloqueado:
        parecer = request.form["parecer"]
        agora = datetime.now()
        cursor.execute("""
            INSERT INTO sindicancias (candidato_id, sindicante, parecer, data_envio)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (candidato_id, sindicante) 
            DO UPDATE SET parecer = %s, data_envio = %s
        """, (id, usuario, parecer, agora, parecer, agora))
        conn.commit()
        registrar_log("emitir_parecer", "sindicancia", id, dados_novos={"parecer": parecer})
        flash("Parecer enviado com sucesso!", "success")
        
        cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo = 'sindicante' AND ativo = 1")
        total_sindicantes = cursor.fetchone()["total"]
        cursor.execute("SELECT COUNT(*) as votos FROM sindicancias WHERE candidato_id = %s", (id,))
        votos = cursor.fetchone()["votos"]
        if votos >= total_sindicantes and total_sindicantes > 0:
            cursor.execute("""
                SELECT 
                    SUM(CASE WHEN parecer = 'positivo' THEN 1 ELSE 0 END) as positivos,
                    SUM(CASE WHEN parecer = 'negativo' THEN 1 ELSE 0 END) as negativos
                FROM sindicancias WHERE candidato_id = %s
            """, (id,))
            res = cursor.fetchone()
            status = "Aprovado" if res["positivos"] > res["negativos"] else "Reprovado"
            agora = datetime.now()
            cursor.execute("""
                UPDATE candidatos 
                SET status = %s, fechado = 1, data_fechamento = %s, resultado_final = %s
                WHERE id = %s
            """, (status, agora, f"{res['positivos']} votos positivos, {res['negativos']} negativos", id))
            conn.commit()
            registrar_log("fechar_sindicancia", "sindicancia", id, dados_novos={"status": status})

    cursor.execute("""
        SELECT s.*, u.usuario, u.nome_completo
        FROM sindicancias s
        JOIN usuarios u ON s.sindicante = u.usuario
        WHERE s.candidato_id = %s
        ORDER BY s.data_envio DESC
    """, (id,))
    registros = cursor.fetchall()

    cursor.execute("""
        SELECT * FROM sindicancias 
        WHERE candidato_id = %s AND sindicante = %s
    """, (id, usuario))
    meu_parecer = cursor.fetchone()

    cursor.execute("""
        SELECT id FROM pareceres_conclusivos 
        WHERE candidato_id = %s AND sindicante = %s
    """, (id, usuario))
    parecer_conclusivo_existente = cursor.fetchone()

    return_connection(conn)

    total_votos = len(registros)
    votos_positivos = sum(1 for r in registros if r["parecer"] == "positivo")
    votos_negativos = total_votos - votos_positivos

    return render_template("sindicancia.html",
                          candidato=candidato,
                          registros=registros,
                          meu_parecer=meu_parecer,
                          parecer_conclusivo_existente=parecer_conclusivo_existente,
                          total_votos=total_votos,
                          votos_positivos=votos_positivos,
                          votos_negativos=votos_negativos,
                          bloqueado=bloqueado,
                          tipo=session["tipo"],
                          usuario_atual=usuario)

@app.route("/excluir_parecer/<int:candidato_id>")
@sindicante_required
def excluir_parecer(candidato_id):
    cursor, conn = get_db()
    cursor.execute("SELECT fechado FROM candidatos WHERE id = %s", (candidato_id,))
    candidato = cursor.fetchone()
    if candidato and candidato["fechado"] == 0:
        cursor.execute("DELETE FROM sindicancias WHERE candidato_id = %s AND sindicante = %s", (candidato_id, session["usuario"]))
        conn.commit()
        registrar_log("excluir_parecer", "sindicancia", candidato_id)
        flash("Parecer excluído com sucesso!", "success")
    else:
        flash("Não é possível excluir parecer de uma sindicância fechada", "danger")
    return_connection(conn)
    return redirect(f"/sindicancia/{candidato_id}")

@app.route("/fechar_sindicancia/<int:id>")
@admin_required
def fechar_sindicancia_manual(id):
    cursor, conn = get_db()
    cursor.execute("SELECT parecer FROM sindicancias WHERE candidato_id = %s", (id,))
    pareceres = cursor.fetchall()
    if not pareceres:
        flash("Não é possível fechar: nenhum parecer enviado", "warning")
        return_connection(conn)
        return redirect("/candidatos")
    
    cursor.execute("SELECT * FROM candidatos WHERE id = %s", (id,))
    dados_antigos = dict(cursor.fetchone())
    
    positivos = sum(1 for p in pareceres if p["parecer"] == "positivo")
    negativos = len(pareceres) - positivos
    status = "Aprovado" if positivos > negativos else "Reprovado"
    agora = datetime.now()
    
    cursor.execute("""
        UPDATE candidatos 
        SET status = %s, fechado = 1, data_fechamento = %s, resultado_final = %s
        WHERE id = %s
    """, (status, agora, f"{positivos} votos positivos, {negativos} negativos", id))
    conn.commit()
    
    registrar_log("fechar_sindicancia_manual", "sindicancia", id, 
                 dados_anteriores=dados_antigos,
                 dados_novos={"status": status})
    return_connection(conn)
    flash(f"Sindicância fechada! Resultado: {status}", "success")
    return redirect("/candidatos")

@app.route("/minhas_sindicancias")
@sindicante_required
def minhas_sindicancias():
    cursor, conn = get_db()
    cursor.execute("""
        SELECT c.*, 
               CASE WHEN s.parecer IS NOT NULL THEN 1 ELSE 0 END as parecer_enviado,
               s.parecer,
               s.data_envio
        FROM candidatos c
        LEFT JOIN sindicancias s ON c.id = s.candidato_id AND s.sindicante = %s
        ORDER BY c.fechado ASC, c.data_criacao DESC
    """, (session["usuario"],))
    candidatos = cursor.fetchall()
    return_connection(conn)
    return render_template("minhas_sindicancias.html", candidatos=candidatos)

@app.route("/parecer_conclusivo/<int:id>", methods=["GET"])
@login_required
def parecer_conclusivo(id):
    if session["tipo"] != "sindicante":
        flash("Acesso restrito a sindicantes", "danger")
        return redirect("/dashboard")
    cursor, conn = get_db()
    cursor.execute("SELECT * FROM candidatos WHERE id = %s", (id,))
    candidato = cursor.fetchone()
    if not candidato:
        flash("Candidato não encontrado", "danger")
        return_connection(conn)
        return redirect("/minhas_sindicancias")
    cursor.execute("""
        SELECT nome_completo, cim_numero, loja_nome, loja_numero, loja_orient 
        FROM usuarios WHERE id = %s
    """, (session["user_id"],))
    sindicante_info = cursor.fetchone()
    if sindicante_info:
        session["nome_completo"] = sindicante_info["nome_completo"] or ""
        session["cim_numero"] = sindicante_info["cim_numero"] or ""
        session["loja_nome"] = sindicante_info["loja_nome"] or ""
        session["loja_numero"] = sindicante_info["loja_numero"] or ""
        session["loja_orient"] = sindicante_info["loja_orient"] or ""
    cursor.execute("""
        SELECT * FROM pareceres_conclusivos 
        WHERE candidato_id = %s AND sindicante = %s
    """, (id, session["usuario"]))
    parecer_existente = cursor.fetchone()
    fontes_existentes = []
    if parecer_existente and parecer_existente["fontes"]:
        try:
            fontes_existentes = json.loads(parecer_existente["fontes"])
        except:
            fontes_existentes = []
    return_connection(conn)
    hoje = datetime.now().strftime("%Y-%m-%d")
    return render_template("parecer_conclusivo.html",
                          candidato=candidato,
                          parecer_existente=parecer_existente,
                          fontes_existentes=fontes_existentes,
                          hoje=hoje,
                          loja_nome=session.get("loja_nome", ""),
                          loja_numero=session.get("loja_numero", ""),
                          loja_orient=session.get("loja_orient", ""),
                          nome_completo=session.get("nome_completo", ""),
                          cim_numero=session.get("cim_numero", ""))

@app.route("/salvar_parecer_conclusivo/<int:id>", methods=["POST"])
@login_required
def salvar_parecer_conclusivo(id):
    if session["tipo"] != "sindicante":
        flash("Acesso restrito a sindicantes", "danger")
        return redirect("/dashboard")
    cursor, conn = get_db()
    parecer_texto = request.form.get("parecer_texto", "")
    conclusao = request.form.get("conclusao", "")
    observacoes = request.form.get("observacoes", "")
    cim_numero = request.form.get("cim_numero", session.get("cim_numero", ""))
    data_parecer = request.form.get("data_parecer", datetime.now().strftime("%Y-%m-%d"))
    loja_nome = request.form.get("loja_nome", session.get("loja_nome", ""))
    loja_numero = request.form.get("loja_numero", session.get("loja_numero", ""))
    loja_orient = request.form.get("loja_orient", session.get("loja_orient", ""))

    fontes = []
    i = 1
    while True:
        fonte_nome = request.form.get(f"fonte_nome_{i}")
        fonte_info = request.form.get(f"fonte_info_{i}")
        if fonte_nome and fonte_info:
            fontes.append({"nome": fonte_nome, "informacao": fonte_info})
        else:
            break
        i += 1

    fontes_json = json.dumps(fontes, ensure_ascii=False)
    agora = datetime.now()
    try:
        cursor.execute("""
            INSERT INTO pareceres_conclusivos 
            (candidato_id, sindicante, parecer_texto, conclusao, observacoes, 
             cim_numero, data_parecer, data_envio, fontes, loja_nome, loja_numero, loja_orient)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (candidato_id, sindicante) 
            DO UPDATE SET 
                parecer_texto = EXCLUDED.parecer_texto,
                conclusao = EXCLUDED.conclusao,
                observacoes = EXCLUDED.observacoes,
                cim_numero = EXCLUDED.cim_numero,
                data_parecer = EXCLUDED.data_parecer,
                data_envio = EXCLUDED.data_envio,
                fontes = EXCLUDED.fontes,
                loja_nome = EXCLUDED.loja_nome,
                loja_numero = EXCLUDED.loja_numero,
                loja_orient = EXCLUDED.loja_orient
        """, (id, session["usuario"], parecer_texto, conclusao, observacoes,
              cim_numero, data_parecer, agora, fontes_json,
              loja_nome, loja_numero, loja_orient))
        conn.commit()
        registrar_log("salvar_parecer_conclusivo", "parecer_conclusivo", id, 
                     dados_novos={"conclusao": conclusao})
        flash("Parecer conclusivo salvo com sucesso!", "success")
        
        parecer_simples = "positivo" if conclusao == "APROVADO" else "negativo"
        cursor.execute("""
            INSERT INTO sindicancias (candidato_id, sindicante, parecer, data_envio)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (candidato_id, sindicante) 
            DO UPDATE SET parecer = %s, data_envio = %s
        """, (id, session["usuario"], parecer_simples, agora, parecer_simples, agora))
        conn.commit()
    except Exception as e:
        flash(f"Erro ao salvar parecer: {str(e)}", "danger")
    return_connection(conn)
    return redirect(f"/sindicancia/{id}")

@app.route("/visualizar_parecer_conclusivo/<int:id>")
@login_required
def visualizar_parecer_conclusivo(id):
    sindicante = request.args.get("sindicante", session["usuario"])
    cursor, conn = get_db()
    cursor.execute("""
        SELECT pc.*, c.nome as candidato_nome, u.nome_completo as sindicante_nome
        FROM pareceres_conclusivos pc
        JOIN candidatos c ON pc.candidato_id = c.id
        JOIN usuarios u ON pc.sindicante = u.usuario
        WHERE pc.candidato_id = %s AND pc.sindicante = %s
    """, (id, sindicante))
    parecer = cursor.fetchone()
    return_connection(conn)
    if not parecer:
        flash("Parecer conclusivo não encontrado", "warning")
        return redirect(f"/sindicancia/{id}")
    fontes = json.loads(parecer["fontes"]) if parecer["fontes"] else []
    return render_template("visualizar_parecer.html", parecer=parecer, fontes=fontes)

@app.route("/baixar_parecer_conclusivo/<int:id>")
@admin_required
def baixar_parecer_conclusivo(id):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from io import BytesIO

        sindicante = request.args.get("sindicante")
        cursor, conn = get_db()
        cursor.execute("""
            SELECT pc.*, c.nome as candidato_nome, u.nome_completo as sindicante_nome
            FROM pareceres_conclusivos pc
            JOIN candidatos c ON pc.candidato_id = c.id
            JOIN usuarios u ON pc.sindicante = u.usuario
            WHERE pc.candidato_id = %s AND pc.sindicante = %s
        """, (id, sindicante))
        parecer = cursor.fetchone()
        if not parecer:
            flash("Parecer conclusivo não encontrado", "danger")
            return_connection(conn)
            return redirect("/dashboard")
        fontes = json.loads(parecer["fontes"]) if parecer["fontes"] else []
        return_connection(conn)

        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4,
                               rightMargin=72, leftMargin=72,
                               topMargin=72, bottomMargin=72)
        styles = getSampleStyleSheet()
        elementos = []
        styles.add(ParagraphStyle(name='CenteredTitle', parent=styles['Title'], alignment=1, spaceAfter=30))

        titulo = Paragraph("PARECER CONCLUSIVO DE SINDICÂNCIA", styles['CenteredTitle'])
        elementos.append(titulo)
        elementos.append(Spacer(1, 0.5*cm))

        elementos.append(Paragraph("<font color='red'><b>CONFIDENCIAL - VENERÁVEL MESTRE</b></font>", styles['Normal']))
        elementos.append(Spacer(1, 0.5*cm))

        info_data = [
            ["Candidato:", parecer["candidato_nome"]],
            ["Sindicante:", parecer["sindicante_nome"] or parecer["sindicante"]],
            ["Data do Parecer:", parecer["data_parecer"].strftime("%d/%m/%Y") if parecer["data_parecer"] else "N/A"],
        ]
        if parecer["cim_numero"]:
            info_data.append(["CIM Nº:", parecer["cim_numero"]])
        if parecer["loja_nome"]:
            info_data.append(["Loja:", f"{parecer['loja_nome']} - Nº {parecer['loja_numero']}"])
            info_data.append(["Oriente:", parecer["loja_orient"] or "Não informado"])

        info_table = Table(info_data, colWidths=[4*cm, 12*cm])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#ecf0f1')),
        ]))
        elementos.append(info_table)
        elementos.append(Spacer(1, 0.5*cm))

        if fontes:
            elementos.append(Paragraph("<b>FONTES DE REFERÊNCIA</b>", styles['Heading3']))
            elementos.append(Spacer(1, 0.3*cm))
            for i, fonte in enumerate(fontes, 1):
                elementos.append(Paragraph(f"<b>Fonte {i}:</b> {fonte.get('nome', '')}", styles['Normal']))
                elementos.append(Paragraph(f"<i>Informação:</i> {fonte.get('informacao', '')}", styles['Normal']))
                elementos.append(Spacer(1, 0.2*cm))
            elementos.append(Spacer(1, 0.3*cm))

        elementos.append(Paragraph("<b>PARECER DO SINDICANTE</b>", styles['Heading3']))
        elementos.append(Spacer(1, 0.3*cm))
        elementos.append(Paragraph(parecer["parecer_texto"], styles['Normal']))
        elementos.append(Spacer(1, 0.5*cm))

        if parecer["observacoes"]:
            elementos.append(Paragraph("<b>OBSERVAÇÕES ADICIONAIS</b>", styles['Heading3']))
            elementos.append(Spacer(1, 0.3*cm))
            elementos.append(Paragraph(parecer["observacoes"], styles['Normal']))
            elementos.append(Spacer(1, 0.5*cm))

        elementos.append(Paragraph("<b>CONCLUSÃO</b>", styles['Heading3']))
        elementos.append(Spacer(1, 0.3*cm))
        if parecer["conclusao"] == "APROVADO":
            conclusao_texto = "<font color='green'><b>✓ O CANDIDATO DEVERÁ INGRESSAR</b></font>"
        else:
            conclusao_texto = "<font color='red'><b>✗ O CANDIDATO NÃO DEVERÁ INGRESSAR</b></font>"
        elementos.append(Paragraph(conclusao_texto, styles['Normal']))
        elementos.append(Spacer(1, 1*cm))

        elementos.append(Paragraph("____________________________________", styles['Normal']))
        elementos.append(Paragraph(parecer["sindicante_nome"] or parecer["sindicante"], styles['Normal']))
        elementos.append(Paragraph("Sindicante", styles['Normal']))

        elementos.append(Spacer(1, 1*cm))
        data_emissao = datetime.now().strftime("%d/%m/%Y %H:%M")
        elementos.append(Paragraph(f"<i>Documento gerado em {data_emissao}</i>", styles['Italic']))

        doc.build(elementos)
        buffer.seek(0)

        nome_arquivo = f"parecer_conclusivo_{parecer['candidato_nome']}_{parecer['sindicante']}.pdf"
        nome_arquivo = nome_arquivo.replace(" ", "_").replace("/", "_")
        return send_file(buffer, as_attachment=True, download_name=nome_arquivo, mimetype='application/pdf')
    except ImportError:
        flash("Biblioteca reportlab não instalada. Execute: pip install reportlab", "warning")
        return redirect("/dashboard")
    except Exception as e:
        flash(f"Erro ao gerar PDF: {str(e)}", "danger")
        return redirect("/dashboard")

# =============================
# ROTAS DE SINDICANTES
# =============================
@app.route("/sindicantes", methods=["GET", "POST"])
@admin_required
def gerenciar_sindicantes():
    cursor, conn = get_db()
    
    if request.method == "POST":
        # Verificar se veio de um obreiro existente ou novo cadastro
        obreiro_id = request.form.get("obreiro_id")
        
        if obreiro_id and obreiro_id != "":
            # Selecionar obreiro existente
            cursor.execute("""
                SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, grau_atual
                FROM usuarios 
                WHERE id = %s AND tipo = 'obreiro' AND grau_atual = 3 AND ativo = 1
            """, (obreiro_id,))
            obreiro = cursor.fetchone()
            
            if obreiro:
                try:
                    # Atualizar o tipo do obreiro para sindicante
                    cursor.execute("""
                        UPDATE usuarios 
                        SET tipo = 'sindicante'
                        WHERE id = %s
                    """, (obreiro_id,))
                    conn.commit()
                    
                    registrar_log("promover_sindicante", "sindicante", obreiro_id, 
                                 dados_novos={"usuario": obreiro["usuario"], "nome": obreiro["nome_completo"]})
                    flash(f"Obreiro {obreiro['nome_completo']} promovido a sindicante com sucesso!", "success")
                    
                except Exception as e:
                    flash(f"Erro ao promover obreiro: {str(e)}", "danger")
                    conn.rollback()
            else:
                flash("Obreiro não encontrado ou não é um Mestre", "danger")
        else:
            # Cadastrar novo sindicante (modo antigo)
            usuario = request.form.get("usuario", "").strip()
            senha = request.form.get("senha")
            nome_completo = request.form.get("nome_completo", "")
            cim_numero = request.form.get("cim_numero", "")
            loja_nome = request.form.get("loja_nome", "")
            loja_numero = request.form.get("loja_numero", "")
            loja_orient = request.form.get("loja_orient", "")
            
            if usuario and senha:
                try:
                    senha_hash = generate_password_hash(senha)
                    agora = datetime.now()
                    cursor.execute("""
                        INSERT INTO usuarios 
                        (usuario, senha_hash, tipo, data_cadastro, ativo, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, grau_atual) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (usuario, senha_hash, "sindicante", agora, 1, nome_completo, cim_numero, 
                          loja_nome, loja_numero, loja_orient, 3))
                    conn.commit()
                    sindicante_id = cursor.lastrowid
                    registrar_log("criar", "sindicante", sindicante_id, dados_novos={"usuario": usuario})
                    flash(f"Sindicante '{usuario}' adicionado com sucesso!", "success")
                except psycopg2.IntegrityError:
                    flash("Usuário já existe", "danger")
                    conn.rollback()
            else:
                flash("Usuário e senha são obrigatórios", "danger")
    
    # Buscar obreiros mestres disponíveis para serem sindicantes
    cursor.execute("""
        SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, grau_atual
        FROM usuarios 
        WHERE tipo = 'obreiro' AND grau_atual = 3 AND ativo = 1
        ORDER BY nome_completo
    """)
    obreiros_mestres = cursor.fetchall()
    
    # Buscar sindicantes ativos
    cursor.execute("""
        SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, ativo 
        FROM usuarios 
        WHERE tipo = 'sindicante' AND ativo = 1
        ORDER BY nome_completo
    """)
    sindicantes = cursor.fetchall()
    
    cursor.execute("SELECT * FROM lojas")
    lojas = cursor.fetchall()
    return_connection(conn)
    
    return render_template("sindicantes.html", 
                          sindicantes=sindicantes, 
                          lojas=lojas,
                          obreiros_mestres=obreiros_mestres)
                          
@app.route("/reverter_sindicante/<int:id>")
@admin_required
def reverter_sindicante(id):
    cursor, conn = get_db()
    
    cursor.execute("SELECT * FROM usuarios WHERE id = %s AND tipo = 'sindicante'", (id,))
    sindicante = cursor.fetchone()
    
    if sindicante:
        cursor.execute("UPDATE usuarios SET tipo = 'obreiro' WHERE id = %s", (id,))
        conn.commit()
        registrar_log("reverter_sindicante", "sindicante", id, 
                     dados_anteriores={"tipo": "sindicante"},
                     dados_novos={"tipo": "obreiro"})
        flash(f"Sindicante {sindicante['usuario']} revertido para obreiro", "success")
    else:
        flash("Sindicante não encontrado", "danger")
    
    return_connection(conn)
    return redirect("/sindicantes")                          

@app.route("/excluir_sindicante/<int:id>")
@admin_required
def excluir_sindicante(id):
    cursor, conn = get_db()
    cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
    dados = dict(cursor.fetchone()) if cursor.fetchone() else None
    cursor.execute("SELECT tipo FROM usuarios WHERE id = %s", (id,))
    usuario = cursor.fetchone()
    if usuario and usuario["tipo"] == "sindicante":
        cursor.execute("UPDATE usuarios SET ativo = 0 WHERE id = %s", (id,))
        conn.commit()
        registrar_log("desativar", "sindicante", id, dados_anteriores=dados)
        flash("Sindicante removido com sucesso!", "success")
    else:
        flash("Usuário não encontrado ou não é sindicante", "danger")
    return_connection(conn)
    return redirect("/sindicantes")

@app.route("/reativar_sindicante/<int:id>")
@admin_required
def reativar_sindicante(id):
    cursor, conn = get_db()
    cursor.execute("SELECT tipo FROM usuarios WHERE id = %s", (id,))
    usuario = cursor.fetchone()
    if usuario and usuario["tipo"] == "sindicante":
        cursor.execute("UPDATE usuarios SET ativo = 1 WHERE id = %s", (id,))
        conn.commit()
        registrar_log("reativar", "sindicante", id)
        flash("Sindicante reativado com sucesso!", "success")
    else:
        flash("Usuário não encontrado ou não é sindicante", "danger")
    return_connection(conn)
    return redirect("/sindicantes")

@app.route("/editar_sindicante/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_sindicante(id):
    cursor, conn = get_db()
    if request.method == "POST":
        nome_completo = request.form.get("nome_completo", "")
        cim_numero = request.form.get("cim_numero", "")
        loja_nome = request.form.get("loja_nome", "")
        loja_numero = request.form.get("loja_numero", "")
        loja_orient = request.form.get("loja_orient", "")
        
        cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
        dados_antigos = dict(cursor.fetchone())
        
        cursor.execute("""
            UPDATE usuarios 
            SET nome_completo = %s, cim_numero = %s, loja_nome = %s, loja_numero = %s, loja_orient = %s
            WHERE id = %s
        """, (nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, id))
        conn.commit()
        
        registrar_log("editar", "sindicante", id, dados_anteriores=dados_antigos,
                     dados_novos={"nome_completo": nome_completo})
        flash("Sindicante atualizado!", "success")
        return_connection(conn)
        return redirect("/sindicantes")
    
    cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
    sindicante = cursor.fetchone()
    cursor.execute("SELECT * FROM lojas")
    lojas = cursor.fetchall()
    return_connection(conn)
    return render_template("editar_sindicante.html", sindicante=sindicante, lojas=lojas)

# =============================
# ROTAS DE LOJAS
# =============================

@app.route("/lojas", methods=["GET", "POST"])
@admin_required
def gerenciar_lojas():
    cursor, conn = get_db()
    
    if request.method == "POST":
        nome = request.form.get("nome", "")
        numero = request.form.get("numero", "")
        oriente = request.form.get("oriente", "")
        cidade = request.form.get("cidade", "")
        estado = request.form.get("estado", "")
        endereco = request.form.get("endereco", "")
        bairro = request.form.get("bairro", "")
        cep = request.form.get("cep", "")
        telefone = request.form.get("telefone", "")
        email = request.form.get("email", "")
        site = request.form.get("site", "")
        data_fundacao = request.form.get("data_fundacao", "")
        data_instalacao = request.form.get("data_instalacao", "")
        data_autorizacao = request.form.get("data_autorizacao", "")
        veneravel_mestre = request.form.get("veneravel_mestre", "")
        secretario = request.form.get("secretario", "")
        tesoureiro = request.form.get("tesoureiro", "")
        orador = request.form.get("orador", "")
        horario_reuniao = request.form.get("horario_reuniao", "")
        dia_reuniao = request.form.get("dia_reuniao", "")
        rito = request.form.get("rito", "")
        observacoes = request.form.get("observacoes", "")
        ativo = 1 if request.form.get("ativo") else 0
        
        # Tratar datas vazias
        data_fundacao = data_fundacao if data_fundacao and data_fundacao.strip() else None
        data_instalacao = data_instalacao if data_instalacao and data_instalacao.strip() else None
        data_autorizacao = data_autorizacao if data_autorizacao and data_autorizacao.strip() else None
        
        if nome and numero:
            try:
                cursor.execute("""
                    INSERT INTO lojas 
                    (nome, numero, oriente, cidade, estado, endereco, bairro, cep,
                     telefone, email, site, data_fundacao, data_instalacao, data_autorizacao,
                     veneravel_mestre, secretario, tesoureiro, orador, horario_reuniao,
                     dia_reuniao, rito, observacoes, ativo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (nome, numero, oriente, cidade, estado, endereco, bairro, cep,
                      telefone, email, site, data_fundacao, data_instalacao, data_autorizacao,
                      veneravel_mestre, secretario, tesoureiro, orador, horario_reuniao,
                      dia_reuniao, rito, observacoes, ativo))
                conn.commit()
                loja_id = cursor.lastrowid
                registrar_log("criar", "loja", loja_id, dados_novos={"nome": nome, "numero": numero})
                flash(f"Loja '{nome}' adicionada com sucesso!", "success")
            except Exception as e:
                print(f"Erro ao criar loja: {e}")
                conn.rollback()
                flash(f"Erro ao criar loja: {str(e)}", "danger")
        else:
            flash("Nome e número da loja são obrigatórios", "danger")
    
    # Buscar todas as lojas
    cursor.execute("""
        SELECT l.*, 
               (SELECT COUNT(*) FROM usuarios WHERE loja_nome = l.nome) as total_obreiros
        FROM lojas l
        ORDER BY l.nome
    """)
    lojas = cursor.fetchall()
    
    return_connection(conn)
    return render_template("lojas.html", lojas=lojas)

@app.route("/lojas/editar/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_loja(id):
    cursor, conn = get_db()
    
    if request.method == "POST":
        nome = request.form.get("nome", "")
        numero = request.form.get("numero", "")
        oriente = request.form.get("oriente", "")
        cidade = request.form.get("cidade", "")
        estado = request.form.get("estado", "")
        endereco = request.form.get("endereco", "")
        bairro = request.form.get("bairro", "")
        cep = request.form.get("cep", "")
        telefone = request.form.get("telefone", "")
        email = request.form.get("email", "")
        site = request.form.get("site", "")
        data_fundacao = request.form.get("data_fundacao", "")
        data_instalacao = request.form.get("data_instalacao", "")
        data_autorizacao = request.form.get("data_autorizacao", "")
        veneravel_mestre = request.form.get("veneravel_mestre", "")
        secretario = request.form.get("secretario", "")
        tesoureiro = request.form.get("tesoureiro", "")
        orador = request.form.get("orador", "")
        horario_reuniao = request.form.get("horario_reuniao", "")
        dia_reuniao = request.form.get("dia_reuniao", "")
        rito = request.form.get("rito", "")
        observacoes = request.form.get("observacoes", "")
        ativo = 1 if request.form.get("ativo") else 0
        
        # Tratar datas vazias
        data_fundacao = data_fundacao if data_fundacao and data_fundacao.strip() else None
        data_instalacao = data_instalacao if data_instalacao and data_instalacao.strip() else None
        data_autorizacao = data_autorizacao if data_autorizacao and data_autorizacao.strip() else None
        
        if nome and numero:
            try:
                # Buscar dados antigos para log
                cursor.execute("SELECT * FROM lojas WHERE id = %s", (id,))
                dados_antigos = dict(cursor.fetchone())
                
                cursor.execute("""
                    UPDATE lojas 
                    SET nome = %s, numero = %s, oriente = %s, cidade = %s, estado = %s,
                        endereco = %s, bairro = %s, cep = %s, telefone = %s, email = %s,
                        site = %s, data_fundacao = %s, data_instalacao = %s, data_autorizacao = %s,
                        veneravel_mestre = %s, secretario = %s, tesoureiro = %s, orador = %s,
                        horario_reuniao = %s, dia_reuniao = %s, rito = %s, observacoes = %s, ativo = %s
                    WHERE id = %s
                """, (nome, numero, oriente, cidade, estado, endereco, bairro, cep,
                      telefone, email, site, data_fundacao, data_instalacao, data_autorizacao,
                      veneravel_mestre, secretario, tesoureiro, orador, horario_reuniao,
                      dia_reuniao, rito, observacoes, ativo, id))
                conn.commit()
                
                registrar_log("editar", "loja", id, dados_anteriores=dados_antigos,
                             dados_novos={"nome": nome, "numero": numero})
                flash(f"Loja '{nome}' atualizada com sucesso!", "success")
                return_connection(conn)
                return redirect("/lojas")
            except Exception as e:
                print(f"Erro ao editar loja: {e}")
                conn.rollback()
                flash(f"Erro ao editar loja: {str(e)}", "danger")
        else:
            flash("Nome e número da loja são obrigatórios", "danger")
    
    cursor.execute("SELECT * FROM lojas WHERE id = %s", (id,))
    loja = cursor.fetchone()
    return_connection(conn)
    
    if not loja:
        flash("Loja não encontrada", "danger")
        return redirect("/lojas")
    
    return render_template("editar_loja.html", loja=loja)

@app.route("/lojas/excluir/<int:id>")
@admin_required
def excluir_loja(id):
    cursor, conn = get_db()
    
    cursor.execute("SELECT * FROM lojas WHERE id = %s", (id,))
    loja = cursor.fetchone()
    
    if not loja:
        flash("Loja não encontrada", "danger")
        return_connection(conn)
        return redirect("/lojas")
    
    dados = dict(loja)
    
    # Verificar se há obreiros vinculados
    cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE loja_nome = %s", (loja["nome"],))
    resultado = cursor.fetchone()
    
    if resultado and resultado["total"] > 0:
        # Se há obreiros, apenas desativa
        cursor.execute("UPDATE lojas SET ativo = 0 WHERE id = %s", (id,))
        conn.commit()
        registrar_log("desativar", "loja", id, dados_anteriores=dados)
        flash(f"Loja '{loja['nome']}' desativada pois possui obreiros vinculados.", "warning")
    else:
        cursor.execute("DELETE FROM lojas WHERE id = %s", (id,))
        conn.commit()
        registrar_log("excluir", "loja", id, dados_anteriores=dados)
        flash(f"Loja '{loja['nome']}' excluída com sucesso!", "success")
    
    return_connection(conn)
    return redirect("/lojas")

# =============================
# ROTAS DE TIPOS DE AUSÊNCIA
# =============================
@app.route("/tipos_ausencia")
@admin_required
def listar_tipos_ausencia():
    cursor, conn = get_db()
    cursor.execute("SELECT * FROM tipos_ausencia ORDER BY nome")
    tipos = cursor.fetchall()
    return_connection(conn)
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
            flash("Nome é obrigatório", "danger")
        else:
            cursor, conn = get_db()
            cursor.execute("""
                INSERT INTO tipos_ausencia (nome, descricao, requer_comprovante, cor, ativo)
                VALUES (%s, %s, %s, %s, 1)
            """, (nome, descricao, requer_comprovante, cor))
            conn.commit()
            tipo_id = cursor.lastrowid
            registrar_log("criar", "tipo_ausencia", tipo_id, dados_novos={"nome": nome})
            return_connection(conn)
            flash(f"Tipo de ausência '{nome}' adicionado com sucesso!", "success")
            return redirect("/tipos_ausencia")
    
    return render_template("presenca/tipo_ausencia_form.html")

@app.route("/tipos_ausencia/editar/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_tipo_ausencia(id):
    cursor, conn = get_db()
    
    if request.method == "POST":
        nome = request.form.get("nome")
        descricao = request.form.get("descricao")
        requer_comprovante = 1 if request.form.get("requer_comprovante") else 0
        cor = request.form.get("cor", "#6c757d")
        ativo = 1 if request.form.get("ativo") else 0
        
        cursor.execute("SELECT * FROM tipos_ausencia WHERE id = %s", (id,))
        dados_antigos = dict(cursor.fetchone())
        
        cursor.execute("""
            UPDATE tipos_ausencia 
            SET nome = %s, descricao = %s, requer_comprovante = %s, cor = %s, ativo = %s
            WHERE id = %s
        """, (nome, descricao, requer_comprovante, cor, ativo, id))
        conn.commit()
        
        registrar_log("editar", "tipo_ausencia", id, dados_anteriores=dados_antigos,
                     dados_novos={"nome": nome})
        flash("Tipo de ausência atualizado com sucesso!", "success")
        return_connection(conn)
        return redirect("/tipos_ausencia")
    
    cursor.execute("SELECT * FROM tipos_ausencia WHERE id = %s", (id,))
    tipo = cursor.fetchone()
    return_connection(conn)
    
    if not tipo:
        flash("Tipo de ausência não encontrado", "danger")
        return redirect("/tipos_ausencia")
    
    return render_template("presenca/tipo_ausencia_form.html", tipo=tipo)

@app.route("/tipos_ausencia/excluir/<int:id>")
@admin_required
def excluir_tipo_ausencia(id):
    cursor, conn = get_db()
    
    cursor.execute("SELECT * FROM tipos_ausencia WHERE id = %s", (id,))
    tipo = cursor.fetchone()
    
    if not tipo:
        flash("Tipo de ausência não encontrado", "danger")
        return_connection(conn)
        return redirect("/tipos_ausencia")
    
    dados = dict(tipo)
    
    # Verificar se está sendo usado
    cursor.execute("SELECT COUNT(*) as total FROM presenca WHERE tipo_ausencia = %s", (tipo["nome"],))
    resultado = cursor.fetchone()
    
    if resultado and resultado["total"] > 0:
        # Se estiver sendo usado, apenas desativa
        cursor.execute("UPDATE tipos_ausencia SET ativo = 0 WHERE id = %s", (id,))
        conn.commit()
        registrar_log("desativar", "tipo_ausencia", id, dados_anteriores=dados)
        flash("Tipo de ausência desativado pois está em uso.", "warning")
    else:
        cursor.execute("DELETE FROM tipos_ausencia WHERE id = %s", (id,))
        conn.commit()
        registrar_log("excluir", "tipo_ausencia", id, dados_anteriores=dados)
        flash("Tipo de ausência excluído com sucesso!", "success")
    
    return_connection(conn)
    return redirect("/tipos_ausencia")

# =============================
# ROTAS DE BACKUP E RESTAURAÇÃO
# =============================

import subprocess
import zipfile
import tempfile
from datetime import datetime
import os

@app.route("/admin/backup")
@admin_required
def backup_page():
    """Página de gerenciamento de backups"""
    backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
    db_name = os.getenv('DB_NAME', 'sistema_maconico')
    
    return render_template("admin/backup.html", 
                          backup_dir=backup_dir,
                          db_name=db_name)

@app.route("/api/backup/listar")
@admin_required
def api_listar_backups():
    """API para listar backups disponíveis"""
    backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
    
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    
    backups = []
    for file in os.listdir(backup_dir):
        if file.startswith('backup_') and file.endswith('.zip'):
            filepath = os.path.join(backup_dir, file)
            mtime = os.path.getmtime(filepath)
            size = os.path.getsize(filepath) / (1024 * 1024)  # MB
            
            backups.append({
                'nome': file,
                'data': datetime.fromtimestamp(mtime).strftime('%d/%m/%Y %H:%M:%S'),
                'tamanho': f"{size:.2f} MB",
                'caminho': filepath
            })
    
    backups.sort(key=lambda x: datetime.strptime(x['data'], '%d/%m/%Y %H:%M:%S'), reverse=True)
    
    return jsonify({'backups': backups})

@app.route("/api/backup/criar", methods=["POST"])
@admin_required
def api_criar_backup():
    """API para criar um novo backup via Python"""
    try:
        backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
        
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        dbname = os.getenv('DB_NAME', 'sistema_maconico')
        filename = f"backup_{dbname}_{timestamp}.sql"
        filepath = os.path.join(backup_dir, filename)
        
        print(f"\n🔄 Criando backup via Python...")
        
        # Conectar ao banco
        cursor, conn = get_db()
        
        with open(filepath, 'w', encoding='utf-8') as f:
            # Escrever cabeçalho
            f.write(f"-- Backup do banco {dbname} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write("-- Gerado pelo Sistema Maçônico\n\n")
            f.write("SET statement_timeout = 0;\n")
            f.write("SET lock_timeout = 0;\n")
            f.write("SET client_encoding = 'UTF8';\n\n")
            
            # Obter lista de tabelas
            cursor.execute("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'public' 
                ORDER BY tablename
            """)
            tabelas = cursor.fetchall()
            
            tabelas_exportadas = 0
            for tabela in tabelas:
                nome_tabela = tabela['tablename']
                
                # Obter estrutura da tabela
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{nome_tabela}'
                    ORDER BY ordinal_position
                """)
                colunas = cursor.fetchall()
                
                if not colunas:
                    continue
                
                # Escrever DELETE
                f.write(f"-- Dados da tabela: {nome_tabela}\n")
                f.write(f"DELETE FROM {nome_tabela};\n\n")
                
                # Obter dados
                cursor.execute(f"SELECT * FROM {nome_tabela}")
                dados = cursor.fetchall()
                
                if dados:
                    colunas_nomes = [c['column_name'] for c in colunas]
                    colunas_str = ', '.join(colunas_nomes)
                    
                    for row in dados:
                        valores = []
                        for col in colunas_nomes:
                            val = row[col]
                            if val is None:
                                valores.append('NULL')
                            elif isinstance(val, str):
                                val_escapado = val.replace("'", "''")
                                valores.append(f"'{val_escapado}'")
                            elif isinstance(val, datetime):
                                valores.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                            else:
                                valores.append(str(val))
                        
                        valores_str = ', '.join(valores)
                        f.write(f"INSERT INTO {nome_tabela} ({colunas_str}) VALUES ({valores_str});\n")
                    f.write("\n")
                    tabelas_exportadas += 1
        
        return_connection(conn)
        
        # Compactar
        zip_filename = filepath + '.zip'
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(filepath, os.path.basename(filepath))
        
        os.remove(filepath)
        tamanho = os.path.getsize(zip_filename) / (1024 * 1024)
        
        registrar_log("backup", "banco", None, dados_novos={"arquivo": filename, "tamanho": f"{tamanho:.2f} MB"})
        
        return jsonify({
            'success': True,
            'message': f'Backup criado com sucesso! {tabelas_exportadas} tabelas exportadas.',
            'arquivo': os.path.basename(zip_filename),
            'tamanho': f"{tamanho:.2f} MB"
        })
        
    except Exception as e:
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': f'Erro ao criar backup: {str(e)}'
        }), 500

@app.route("/api/backup/baixar/<nome_arquivo>")
@admin_required
def api_baixar_backup(nome_arquivo):
    """API para baixar um backup"""
    backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
    filepath = os.path.join(backup_dir, nome_arquivo)
    
    if not os.path.exists(filepath):
        flash("Arquivo não encontrado", "danger")
        return redirect("/admin/backup")
    
    registrar_log("baixar_backup", "backup", None, dados_novos={"arquivo": nome_arquivo})
    
    return send_file(
        filepath,
        as_attachment=True,
        download_name=nome_arquivo,
        mimetype="application/zip"
    )

@app.route("/api/backup/restaurar", methods=["POST"])
@admin_required
def api_restaurar_backup():
    """API para restaurar um backup via Python"""
    nome_arquivo = request.form.get("arquivo")
    
    if not nome_arquivo:
        return jsonify({'success': False, 'message': 'Arquivo não informado'}), 400
    
    try:
        backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
        filepath = os.path.join(backup_dir, nome_arquivo)
        
        if not os.path.exists(filepath):
            return jsonify({'success': False, 'message': 'Arquivo não encontrado'}), 404
        
        # Criar backup de segurança antes de restaurar
        backup_seguranca = f"backup_seguranca_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
        backup_path = os.path.join(backup_dir, backup_seguranca)
        
        cursor, conn = get_db()
        
        # Salvar estado atual
        with open(backup_path, 'w', encoding='utf-8') as f:
            cursor.execute("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'public' 
                ORDER BY tablename
            """)
            tabelas = cursor.fetchall()
            
            for tabela in tabelas:
                nome_tabela = tabela['tablename']
                cursor.execute(f"SELECT * FROM {nome_tabela}")
                dados = cursor.fetchall()
                
                if dados:
                    cursor.execute(f"""
                        SELECT column_name FROM information_schema.columns 
                        WHERE table_name = '{nome_tabela}'
                        ORDER BY ordinal_position
                    """)
                    colunas = cursor.fetchall()
                    colunas_nomes = [c['column_name'] for c in colunas]
                    colunas_str = ', '.join(colunas_nomes)
                    
                    for row in dados:
                        valores = []
                        for col in colunas_nomes:
                            val = row[col]
                            if val is None:
                                valores.append('NULL')
                            elif isinstance(val, str):
                                val_escapado = val.replace("'", "''")
                                valores.append(f"'{val_escapado}'")
                            elif isinstance(val, datetime):
                                valores.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                            else:
                                valores.append(str(val))
                        f.write(f"INSERT INTO {nome_tabela} ({colunas_str}) VALUES ({', '.join(valores)});\n")
        
        # Descompactar e restaurar
        with tempfile.TemporaryDirectory() as tmpdir:
            if filepath.endswith('.zip'):
                with zipfile.ZipFile(filepath, 'r') as zipf:
                    sql_file = zipf.namelist()[0]
                    zipf.extractall(tmpdir)
                    sql_path = os.path.join(tmpdir, sql_file)
            else:
                sql_path = filepath
            
            # Ler e executar os comandos SQL
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Executar os comandos em uma transação
            cursor.execute("BEGIN;")
            
            # Dividir os comandos por ponto e vírgula
            commands = sql_content.split(';')
            for cmd in commands:
                cmd = cmd.strip()
                if cmd and not cmd.startswith('--'):
                    try:
                        cursor.execute(cmd)
                    except Exception as e:
                        print(f"Erro ao executar: {cmd[:100]}...")
                        raise e
            
            cursor.execute("COMMIT;")
        
        return_connection(conn)
        
        registrar_log("restaurar_backup", "backup", None, 
                     dados_novos={"arquivo": nome_arquivo, "backup_seguranca": backup_seguranca})
        
        return jsonify({'success': True, 'message': 'Backup restaurado com sucesso! O sistema será reiniciado.'})
                
    except Exception as e:
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Erro ao restaurar: {str(e)}'}), 500

@app.route("/api/backup/excluir/<nome_arquivo>", methods=["DELETE"])
@admin_required
def api_excluir_backup(nome_arquivo):
    """API para excluir um backup"""
    backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
    filepath = os.path.join(backup_dir, nome_arquivo)
    
    if not os.path.exists(filepath):
        return jsonify({'success': False, 'message': 'Arquivo não encontrado'}), 404
    
    try:
        os.remove(filepath)
        registrar_log("excluir_backup", "backup", None, dados_novos={"arquivo": nome_arquivo})
        return jsonify({'success': True, 'message': 'Backup excluído com sucesso!'})
    except Exception as e:

        return jsonify({'success': False, 'message': f'Erro ao excluir: {str(e)}'}), 500
# =============================
# ROTAS DE CARGOS (OBREIROS)
# =============================

@app.route("/obreiros/<int:id>/cargo", methods=["POST"])
@admin_required
def atribuir_cargo(id):
    cursor, conn = get_db()
    cargo_id = request.form.get("cargo_id")
    data_inicio = request.form.get("data_inicio")
    gestao = request.form.get("gestao")

    if not cargo_id or not data_inicio:
        flash("Cargo e data de início são obrigatórios", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{id}")

    try:
        cursor.execute("""
            INSERT INTO ocupacao_cargos (obreiro_id, cargo_id, data_inicio, gestao, ativo)
            VALUES (%s, %s, %s, %s, 1)
        """, (id, cargo_id, data_inicio, gestao))
        conn.commit()
        
        registrar_log("atribuir_cargo", "cargo", cargo_id, dados_novos={"obreiro_id": id, "cargo_id": cargo_id})
        flash("Cargo atribuído com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao atribuir cargo: {e}")
        conn.rollback()
        flash(f"Erro ao atribuir cargo: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/obreiros/{id}")

@app.route("/obreiros/cargo/<int:id>/remover")
@admin_required
def remover_cargo(id):
    cursor, conn = get_db()
    
    try:
        # Buscar o cargo para saber o obreiro
        cursor.execute("SELECT obreiro_id, cargo_id FROM ocupacao_cargos WHERE id = %s", (id,))
        cargo = cursor.fetchone()
        
        if not cargo:
            flash("Cargo não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        obreiro_id = cargo["obreiro_id"]
        cargo_id = cargo["cargo_id"]
        
        # Remover o cargo (desativar)
        cursor.execute("UPDATE ocupacao_cargos SET ativo = 0 WHERE id = %s", (id,))
        conn.commit()
        
        registrar_log("remover_cargo", "cargo", cargo_id, dados_anteriores={"obreiro_id": obreiro_id})
        flash("Cargo removido com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao remover cargo: {e}")
        conn.rollback()
        flash(f"Erro ao remover cargo: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/obreiros/{obreiro_id}")

# =============================
# ROTAS DE AUDITORIA
# =============================
@app.route("/auditoria")
@admin_required
def listar_logs():
    cursor, conn = get_db()
    
    data_ini = request.args.get('data_ini', '')
    data_fim = request.args.get('data_fim', '')
    acao = request.args.get('acao', '')
    entidade = request.args.get('entidade', '')
    usuario = request.args.get('usuario', '')
    
    query = """
        SELECT l.*, u.usuario
        FROM logs_auditoria l
        LEFT JOIN usuarios u ON l.usuario_id = u.id
        WHERE 1=1
    """
    params = []
    
    if data_ini:
        query += " AND l.data_hora >= %s"
        params.append(data_ini)
    if data_fim:
        query += " AND l.data_hora <= %s"
        params.append(data_fim)
    if acao:
        query += " AND l.acao = %s"
        params.append(acao)
    if entidade:
        query += " AND l.entidade = %s"
        params.append(entidade)
    if usuario:
        query += " AND l.usuario_nome LIKE %s"
        params.append(f"%{usuario}%")
    
    query += " ORDER BY l.data_hora DESC LIMIT 1000"
    
    cursor.execute(query, params)
    logs = cursor.fetchall()
    
    cursor.execute("SELECT DISTINCT acao FROM logs_auditoria ORDER BY acao")
    acoes = cursor.fetchall()
    cursor.execute("SELECT DISTINCT entidade FROM logs_auditoria ORDER BY entidade")
    entidades = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("auditoria/logs.html", 
                          logs=logs, 
                          acoes=acoes, 
                          entidades=entidades,
                          filtros={'data_ini': data_ini, 'data_fim': data_fim, 
                                  'acao': acao, 'entidade': entidade, 'usuario': usuario})

@app.route("/auditoria/<int:id>")
@admin_required
def detalhes_log(id):
    cursor, conn = get_db()
    cursor.execute("""
        SELECT l.*, u.usuario, u.nome_completo
        FROM logs_auditoria l
        LEFT JOIN usuarios u ON l.usuario_id = u.id
        WHERE l.id = %s
    """, (id,))
    log = cursor.fetchone()
    return_connection(conn)
    
    if not log:
        flash("Registro não encontrado", "danger")
        return redirect("/auditoria")
    
    return render_template("auditoria/detalhes.html", log=log)

@app.route("/auditoria/exportar")
@admin_required
def exportar_logs():
    cursor, conn = get_db()
    
    data_ini = request.args.get('data_ini', '')
    data_fim = request.args.get('data_fim', '')
    
    query = """
        SELECT l.*, u.usuario
        FROM logs_auditoria l
        LEFT JOIN usuarios u ON l.usuario_id = u.id
        WHERE 1=1
    """
    params = []
    
    if data_ini:
        query += " AND l.data_hora >= %s"
        params.append(data_ini)
    if data_fim:
        query += " AND l.data_hora <= %s"
        params.append(data_fim)
    
    query += " ORDER BY l.data_hora DESC"
    
    cursor.execute(query, params)
    logs = cursor.fetchall()
    return_connection(conn)
    
    import csv
    from io import StringIO
    
    output = StringIO()
    writer = csv.writer(output, delimiter=';')
    
    # Cabeçalho
    writer.writerow(['ID', 'Data/Hora', 'Usuário', 'Ação', 'Entidade', 'ID Entidade', 'IP', 'Dados Anteriores', 'Dados Novos'])
    
    for log in logs:
        dados_anteriores = log['dados_anteriores'] if log['dados_anteriores'] is not None else ''
        dados_novos = log['dados_novos'] if log['dados_novos'] is not None else ''
        
        writer.writerow([
            log['id'],
            log['data_hora'].strftime("%d/%m/%Y %H:%M:%S") if log['data_hora'] else '',
            log['usuario_nome'],
            log['acao'],
            log['entidade'] or '',
            log['entidade_id'] or '',
            log['ip'] or '',
            dados_anteriores,
            dados_novos
        ])
    
    output.seek(0)
    
    registrar_log("exportar_logs", "auditoria", None, dados_novos={"periodo": f"{data_ini} a {data_fim}"})
    
    return Response(
        output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment;filename=logs_auditoria_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
    )

# =============================
# ROTAS DE RELATÓRIOS E EXPORTAÇÕES
# =============================
@app.route("/relatorios/consolidados", methods=["GET", "POST"])
@admin_required
def relatorios_consolidados():
    if request.method == "POST":
        tipo = request.form.get("tipo", "ano")
        ano_str = request.form.get("ano")
        mes_str = request.form.get("mes")
        data_inicio = request.form.get("data_inicio")
        data_fim = request.form.get("data_fim")

        try:
            ano = int(ano_str) if ano_str else datetime.now().year
        except ValueError:
            ano = datetime.now().year
        try:
            mes = int(mes_str) if mes_str else 1
        except ValueError:
            mes = 1

        cursor, conn = get_db()

        if tipo == "ano":
            cursor.execute("""
                SELECT * FROM reunioes 
                WHERE EXTRACT(YEAR FROM data) = %s AND status = 'realizada'
                ORDER BY data
            """, (ano,))
            reunioes = cursor.fetchall()
            periodo_desc = f"Ano {ano}"
        elif tipo == "mes":
            cursor.execute("""
                SELECT * FROM reunioes 
                WHERE EXTRACT(YEAR FROM data) = %s AND EXTRACT(MONTH FROM data) = %s AND status = 'realizada'
                ORDER BY data
            """, (ano, mes))
            reunioes = cursor.fetchall()
            periodo_desc = f"{mes:02d}/{ano}"
        elif tipo == "periodo":
            cursor.execute("""
                SELECT * FROM reunioes 
                WHERE data BETWEEN %s AND %s AND status = 'realizada'
                ORDER BY data
            """, (data_inicio, data_fim))
            reunioes = cursor.fetchall()
            periodo_desc = f"{data_inicio} a {data_fim}"
        else:
            flash("Período inválido", "danger")
            return_connection(conn)
            return redirect("/relatorios/consolidados")

        cursor.execute("""
            SELECT id, nome_completo, grau_atual 
            FROM usuarios 
            WHERE ativo = 1 
            ORDER BY grau_atual DESC, nome_completo
        """)
        obreiros = cursor.fetchall()

        stats = []
        for o in obreiros:
            total_reunioes = len(reunioes)
            if total_reunioes > 0:
                placeholders = ','.join(['%s'] * len(reunioes))
                cursor.execute(f"""
                    SELECT COUNT(*) as count
                    FROM presenca p
                    JOIN reunioes r ON p.reuniao_id = r.id
                    WHERE p.obreiro_id = %s 
                      AND p.presente = 1
                      AND r.id IN ({placeholders})
                """, [o["id"]] + [r["id"] for r in reunioes])
                presentes = cursor.fetchone()["count"]
            else:
                presentes = 0
            stats.append({
                "nome": o["nome_completo"],
                "grau": o["grau_atual"],
                "total": total_reunioes,
                "presentes": presentes,
                "ausentes": total_reunioes - presentes,
                "percentual": (presentes / total_reunioes * 100) if total_reunioes > 0 else 0
            })

        total_reunioes = len(reunioes)
        total_presencas = sum(s["presentes"] for s in stats)
        total_ausencias = sum(s["ausentes"] for s in stats)
        media_presenca = (total_presencas / (total_reunioes * len(obreiros)) * 100) if total_reunioes > 0 and obreiros else 0

        return_connection(conn)

        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import A4
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import cm
            from io import BytesIO

            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4,
                                   rightMargin=72, leftMargin=72,
                                   topMargin=72, bottomMargin=72)
            styles = getSampleStyleSheet()
            elementos = []

            styles.add(ParagraphStyle(name='CenteredTitle',
                                     parent=styles['Title'],
                                     alignment=1,
                                     spaceAfter=30))
            titulo = Paragraph(f"RELATÓRIO CONSOLIDADO - {periodo_desc}", styles['CenteredTitle'])
            elementos.append(titulo)
            elementos.append(Spacer(1, 0.5*cm))

            elementos.append(Paragraph("<b>RESUMO GERAL</b>", styles['Heading2']))
            elementos.append(Spacer(1, 0.3*cm))
            resumo = [
                ["Período:", periodo_desc],
                ["Total de reuniões realizadas:", str(total_reunioes)],
                ["Total de obreiros:", str(len(obreiros))],
                ["Total de presenças:", str(total_presencas)],
                ["Total de ausências:", str(total_ausencias)],
                ["Média de presença:", f"{media_presenca:.1f}%"]
            ]
            resumo_table = Table(resumo, colWidths=[5*cm, 10*cm])
            resumo_table.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#ecf0f1')),
            ]))
            elementos.append(resumo_table)
            elementos.append(Spacer(1, 0.5*cm))

            elementos.append(Paragraph("<b>ESTATÍSTICAS INDIVIDUAIS</b>", styles['Heading2']))
            elementos.append(Spacer(1, 0.3*cm))

            dados = [["Obreiro", "Grau", "Total", "Presentes", "Ausentes", "% Presença"]]
            for s in stats:
                grau_str = "Mestre" if s["grau"] == 3 else ("Companheiro" if s["grau"] == 2 else "Aprendiz")
                dados.append([
                    s["nome"],
                    grau_str,
                    str(s["total"]),
                    str(s["presentes"]),
                    str(s["ausentes"]),
                    f"{s['percentual']:.1f}%"
                ])

            col_widths = [5*cm, 2.5*cm, 2*cm, 2*cm, 2*cm, 2.5*cm]
            tabela = Table(dados, colWidths=col_widths)
            tabela.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c3e50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                ('GRID', (0, 0), (-1, -1), 1, colors.grey),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            elementos.append(tabela)
            elementos.append(Spacer(1, 1*cm))

            if total_reunioes > 0:
                elementos.append(Paragraph("<b>REUNIÕES REALIZADAS NO PERÍODO</b>", styles['Heading2']))
                elementos.append(Spacer(1, 0.3*cm))
                reunioes_dados = [["Data", "Título", "Presenças", "Total Obreiros"]]
                for r in reunioes:
                    cursor2, conn2 = get_db()
                    cursor2.execute("""
                        SELECT COUNT(*) as total, SUM(CASE WHEN presente = 1 THEN 1 ELSE 0 END) as presentes
                        FROM presenca WHERE reuniao_id = %s
                    """, (r["id"],))
                    res = cursor2.fetchone()
                    return_connection(conn2)
                    reunioes_dados.append([
                        r["data"].strftime("%d/%m/%Y"),
                        r["titulo"],
                        str(res["presentes"] or 0),
                        str(res["total"] or 0)
                    ])
                reunioes_table = Table(reunioes_dados, colWidths=[3*cm, 6*cm, 3*cm, 3*cm])
                reunioes_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c3e50')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ]))
                elementos.append(reunioes_table)

            elementos.append(Spacer(1, 1*cm))
            data_emissao = datetime.now().strftime("%d/%m/%Y %H:%M")
            rodape = Paragraph(f"<i>Relatório gerado em {data_emissao} - Sistema Maçônico</i>", styles['Italic'])
            elementos.append(rodape)

            doc.build(elementos)
            buffer.seek(0)

            nome_arquivo = f"relatorio_consolidado_{periodo_desc}.pdf"
            nome_arquivo = nome_arquivo.replace(" ", "_").replace("/", "-")
            registrar_log("exportar_relatorio", "relatorios", None, dados_novos={"periodo": periodo_desc})
            return send_file(buffer, as_attachment=True, download_name=nome_arquivo, mimetype='application/pdf')
        except ImportError:
            flash("Biblioteca reportlab não instalada. Execute: pip install reportlab", "warning")
            return redirect("/relatorios/consolidados")
        except Exception as e:
            flash(f"Erro ao gerar relatório: {str(e)}", "danger")
            return redirect("/relatorios/consolidados")

    anos = range(2020, datetime.now().year + 1)
    return render_template("relatorios/consolidados.html", anos=anos)

@app.route("/exportar/presenca", methods=["GET", "POST"])
@admin_required
def exportar_presenca():
    if request.method == "POST":
        ano = request.form.get("ano")
        mes = request.form.get("mes")
        tipo = request.form.get("tipo", "ano")
        data_inicio = request.form.get("data_inicio")
        data_fim = request.form.get("data_fim")

        cursor, conn = get_db()

        if tipo == "ano":
            cursor.execute("""
                SELECT r.*, 
                       (SELECT COUNT(*) FROM presenca WHERE reuniao_id = r.id) as total,
                       (SELECT SUM(presente) FROM presenca WHERE reuniao_id = r.id) as presentes
                FROM reunioes r
                WHERE EXTRACT(YEAR FROM r.data) = %s AND r.status = 'realizada'
                ORDER BY r.data
            """, (int(ano),))
            reunioes = cursor.fetchall()
            filtro_desc = f"Ano {ano}"
        elif tipo == "mes":
            cursor.execute("""
                SELECT r.*, 
                       (SELECT COUNT(*) FROM presenca WHERE reuniao_id = r.id) as total,
                       (SELECT SUM(presente) FROM presenca WHERE reuniao_id = r.id) as presentes
                FROM reunioes r
                WHERE EXTRACT(YEAR FROM r.data) = %s AND EXTRACT(MONTH FROM r.data) = %s AND r.status = 'realizada'
                ORDER BY r.data
            """, (int(ano), int(mes)))
            reunioes = cursor.fetchall()
            filtro_desc = f"{int(mes):02d}/{ano}"
        elif tipo == "periodo":
            cursor.execute("""
                SELECT r.*, 
                       (SELECT COUNT(*) FROM presenca WHERE reuniao_id = r.id) as total,
                       (SELECT SUM(presente) FROM presenca WHERE reuniao_id = r.id) as presentes
                FROM reunioes r
                WHERE r.data BETWEEN %s AND %s AND r.status = 'realizada'
                ORDER BY r.data
            """, (data_inicio, data_fim))
            reunioes = cursor.fetchall()
            filtro_desc = f"{data_inicio} a {data_fim}"
        else:
            flash("Período inválido", "danger")
            return_connection(conn)
            return redirect("/exportar/presenca")

        if not reunioes:
            flash("Nenhuma reunião encontrada no período selecionado", "warning")
            return_connection(conn)
            return redirect("/exportar/presenca")

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Presença"

        headers = ["Reunião", "Data", "Obreiro", "Grau", "Presença", "Tipo Ausência", "Justificativa", "Validado Por"]
        ws.append(headers)

        for col in range(1, len(headers)+1):
            cell = ws.cell(row=1, column=col)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="2C3E50", end_color="2C3E50", fill_type="solid")
            cell.alignment = Alignment(horizontal="center", vertical="center")

        for reuniao in reunioes:
            cursor.execute("""
                SELECT u.nome_completo, u.grau_atual, p.presente, p.tipo_ausencia, p.justificativa, 
                       u2.nome_completo as validado_por
                FROM presenca p
                JOIN usuarios u ON p.obreiro_id = u.id
                LEFT JOIN usuarios u2 ON p.validado_por = u2.id
                WHERE p.reuniao_id = %s
                ORDER BY u.grau_atual DESC, u.nome_completo
            """, (reuniao["id"],))
            presencas = cursor.fetchall()

            for p in presencas:
                grau_texto = "Mestre" if p["grau_atual"] == 3 else ("Companheiro" if p["grau_atual"] == 2 else "Aprendiz")
                presente_texto = "Presente" if p["presente"] == 1 else "Ausente"
                tipo_ausencia = p["tipo_ausencia"] if p["tipo_ausencia"] else ""
                justificativa = p["justificativa"] if p["justificativa"] else ""
                validado = p["validado_por"] if p["validado_por"] else ""

                ws.append([
                    reuniao["titulo"],
                    reuniao["data"].strftime("%d/%m/%Y"),
                    p["nome_completo"],
                    grau_texto,
                    presente_texto,
                    tipo_ausencia,
                    justificativa,
                    validado
                ])

        return_connection(conn)

        for col in ws.columns:
            max_length = 0
            col_letter = get_column_letter(col[0].column)
            for cell in col:
                try:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                except:
                    pass
            adjusted_width = min(max_length + 2, 30)
            ws.column_dimensions[col_letter].width = adjusted_width

        from io import BytesIO
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        nome_arquivo = f"presenca_{filtro_desc}.xlsx".replace("/", "-").replace(" ", "_")
        registrar_log("exportar_presenca", "presenca", None, dados_novos={"periodo": filtro_desc})
        return send_file(
            output,
            as_attachment=True,
            download_name=nome_arquivo,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    anos = range(2020, datetime.now().year + 1)
    return render_template("exportar/presenca.html", anos=anos)

# =============================
# ROTA DE RELATÓRIO PDF (SINDICÂNCIA)
# =============================
@app.route("/relatorio/<int:id>")
@admin_required
def gerar_relatorio(id):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from io import BytesIO

        cursor, conn = get_db()
        cursor.execute("SELECT * FROM candidatos WHERE id = %s", (id,))
        candidato = cursor.fetchone()
        if not candidato:
            flash("Candidato não encontrado", "danger")
            return_connection(conn)
            return redirect("/candidatos")

        cursor.execute("""
            SELECT s.*, u.usuario, u.nome_completo, u.cim_numero, u.loja_nome, u.loja_numero, u.loja_orient
            FROM sindicancias s
            JOIN usuarios u ON s.sindicante = u.usuario
            WHERE s.candidato_id = %s
            ORDER BY s.data_envio DESC
        """, (id,))
        pareceres = cursor.fetchall()

        cursor.execute("""
            SELECT * FROM pareceres_conclusivos 
            WHERE candidato_id = %s
        """, (id,))
        pareceres_conclusivos = cursor.fetchall()
        return_connection(conn)

        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4,
                               rightMargin=72, leftMargin=72,
                               topMargin=72, bottomMargin=18)
        styles = getSampleStyleSheet()
        elementos = []

        styles.add(ParagraphStyle(name='CenteredTitle', parent=styles['Title'], alignment=1, spaceAfter=30))
        styles.add(ParagraphStyle(name='SectionHeader', parent=styles['Heading2'],
                                 textColor=colors.HexColor('#2c3e50'), spaceBefore=15, spaceAfter=10,
                                 borderWidth=1, borderColor=colors.HexColor('#2c3e50'), borderRadius=5,
                                 backColor=colors.HexColor('#ecf0f1'), padding=8))

        titulo = Paragraph("RELATÓRIO DE SINDICÂNCIA", styles['CenteredTitle'])
        elementos.append(titulo)
        elementos.append(Spacer(1, 0.5*cm))

        elementos.append(Paragraph("DADOS DO CANDIDATO", styles['SectionHeader']))
        data_abertura = candidato["data_criacao"].strftime("%d/%m/%Y %H:%M") if candidato["data_criacao"] else "N/A"
        data_fechamento = candidato["data_fechamento"].strftime("%d/%m/%Y %H:%M") if candidato["data_fechamento"] else "Em andamento"
        info_data = [
            ["Nome:", candidato["nome"]],
            ["Status:", candidato["status"]],
            ["Data de Abertura:", data_abertura],
            ["Data de Fechamento:", data_fechamento],
        ]
        if candidato["resultado_final"]:
            info_data.append(["Resultado:", candidato["resultado_final"]])
        info_table = Table(info_data, colWidths=[4*cm, 10*cm])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#ecf0f1')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#2c3e50')),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ]))
        elementos.append(info_table)
        elementos.append(Spacer(1, 0.5*cm))

        if pareceres_conclusivos:
            elementos.append(Paragraph("PARECERES CONCLUSIVOS", styles['SectionHeader']))
            for pc in pareceres_conclusivos:
                elementos.append(Paragraph(f"<b>Sindicante:</b> {pc['sindicante']}", styles['Normal']))
                elementos.append(Paragraph(f"<b>Data:</b> {pc['data_parecer'].strftime('%d/%m/%Y') if pc['data_parecer'] else 'N/A'}", styles['Normal']))
                try:
                    fontes = json.loads(pc['fontes'])
                    if fontes:
                        elementos.append(Paragraph("<b>Fontes consultadas:</b>", styles['Normal']))
                        for i, fonte in enumerate(fontes, 1):
                            elementos.append(Paragraph(f"<b>Fonte {i}:</b> {fonte.get('nome', '')}<br/><i>Informação:</i> {fonte.get('informacao', '')}", styles['Normal']))
                except:
                    pass
                elementos.append(Paragraph("<b>Parecer:</b>", styles['Normal']))
                elementos.append(Paragraph(pc['parecer_texto'], styles['Normal']))
                if pc['conclusao'] == "APROVADO":
                    elementos.append(Paragraph("<b>Conclusão:</b> <font color='green'>DEVERÁ INGRESSAR</font>", styles['Normal']))
                else:
                    elementos.append(Paragraph("<b>Conclusão:</b> <font color='red'>NÃO DEVERÁ INGRESSAR</font>", styles['Normal']))
                elementos.append(Spacer(1, 0.3*cm))

        if pareceres:
            elementos.append(Paragraph("PARECERES SIMPLES", styles['SectionHeader']))
            dados = [["Sindicante", "Loja", "Parecer", "Data"]]
            for p in pareceres:
                loja = f"{p['loja_nome'] or ''} {p['loja_numero'] or ''}".strip()
                dados.append([
                    p['nome_completo'] or p['usuario'],
                    loja or "-",
                    "✅ Positivo" if p['parecer'] == "positivo" else "❌ Negativo",
                    p['data_envio'].strftime("%d/%m/%Y %H:%M") if p['data_envio'] else "N/A"
                ])
            tabela = Table(dados, colWidths=[5*cm, 4*cm, 3*cm, 4*cm])
            tabela.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c3e50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                ('GRID', (0, 0), (-1, -1), 1, colors.grey),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
            ]))
            elementos.append(tabela)
        else:
            elementos.append(Paragraph("Nenhum parecer simples emitido.", styles['Normal']))

        elementos.append(Spacer(1, 1*cm))
        data_emissao = datetime.now().strftime("%d/%m/%Y %H:%M")
        rodape = Paragraph(f"<i>Relatório gerado em {data_emissao} pelo sistema de sindicâncias.</i>", styles['Italic'])
        elementos.append(rodape)

        doc.build(elementos)
        buffer.seek(0)
        nome_arquivo = f"relatorio_sindicancia_{candidato['nome']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        nome_arquivo = nome_arquivo.replace(" ", "_").replace("/", "_")
        registrar_log("gerar_relatorio", "relatorio", id, dados_novos={"candidato": candidato["nome"]})
        return send_file(buffer, as_attachment=True, download_name=nome_arquivo, mimetype='application/pdf')
    except ImportError:
        flash("Biblioteca reportlab não instalada. Execute: pip install reportlab", "warning")
        return redirect("/candidatos")
    except Exception as e:
        flash(f"Erro ao gerar relatório: {str(e)}", "danger")
        return redirect("/candidatos")


# =============================
# ROTAS DE DOCUMENTOS DOS OBREIROS
# =============================

@app.route("/obreiros/<int:id>/documentos")
@login_required
def listar_documentos(id):
    # Verificar permissão
    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para acessar esta página", "danger")
        return redirect("/obreiros")
    
    cursor, conn = get_db()
    
    # Buscar obreiro
    cursor.execute("SELECT nome_completo FROM usuarios WHERE id = %s", (id,))
    obreiro = cursor.fetchone()
    if not obreiro:
        flash("Obreiro não encontrado", "danger")
        return_connection(conn)
        return redirect("/obreiros")
    
    # Buscar documentos do obreiro
    cursor.execute("""
        SELECT d.*, c.nome as categoria_nome, c.icone
        FROM documentos_obreiro d
        LEFT JOIN categorias_documentos c ON d.categoria = c.nome
        WHERE d.obreiro_id = %s
        ORDER BY d.data_upload DESC
    """, (id,))
    documentos = cursor.fetchall()
    
    # Buscar categorias para o filtro
    cursor.execute("SELECT * FROM categorias_documentos WHERE ativo = 1 ORDER BY nome")
    categorias = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("obreiros/documentos.html", 
                          obreiro_id=id, 
                          obreiro_nome=obreiro["nome_completo"],
                          documentos=documentos,
                          categorias=categorias)

@app.route("/obreiros/<int:id>/documentos/upload", methods=["POST"])
@login_required
def upload_documento(id):
    # Verificar permissão
    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para esta ação", "danger")
        return redirect(f"/obreiros/{id}/documentos")
    
    if 'arquivo' not in request.files:
        flash("Nenhum arquivo selecionado", "danger")
        return redirect(f"/obreiros/{id}/documentos")
    
    arquivo = request.files['arquivo']
    if arquivo.filename == '':
        flash("Nenhum arquivo selecionado", "danger")
        return redirect(f"/obreiros/{id}/documentos")
    
    if not allowed_file(arquivo.filename):
        flash("Tipo de arquivo não permitido. Use: PDF, imagens, Word, Excel, TXT, ZIP", "danger")
        return redirect(f"/obreiros/{id}/documentos")
    
    try:
        titulo = request.form.get('titulo', '')
        descricao = request.form.get('descricao', '')
        categoria = request.form.get('categoria', 'outros')
        
        if not titulo:
            titulo = arquivo.filename
        
        # Salvar arquivo
        filename = secure_filename(arquivo.filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"{id}_{timestamp}_{filename}"
        caminho = os.path.join(UPLOAD_FOLDER, nome_arquivo)
        
        arquivo.save(caminho)
        tamanho = os.path.getsize(caminho)
        
        cursor, conn = get_db()
        
        cursor.execute("""
            INSERT INTO documentos_obreiro 
            (obreiro_id, titulo, descricao, categoria, tipo_arquivo, nome_arquivo, caminho_arquivo, tamanho, uploaded_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, titulo, descricao, categoria, filename.split('.')[-1], nome_arquivo, caminho, tamanho, session["user_id"]))
        
        conn.commit()
        doc_id = cursor.lastrowid
        return_connection(conn)
        
        registrar_log("upload_documento", "documento", doc_id, dados_novos={"titulo": titulo, "categoria": categoria})
        flash(f"Documento '{titulo}' enviado com sucesso!", "success")
        
    except Exception as e:
        flash(f"Erro ao enviar arquivo: {str(e)}", "danger")
    
    return redirect(f"/obreiros/{id}/documentos")

@app.route("/documentos/<int:id>/baixar")
@login_required
def baixar_documento(id):
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT d.*, u.id as obreiro_id
        FROM documentos_obreiro d
        JOIN usuarios u ON d.obreiro_id = u.id
        WHERE d.id = %s
    """, (id,))
    doc = cursor.fetchone()
    
    if not doc:
        flash("Documento não encontrado", "danger")
        return_connection(conn)
        return redirect("/obreiros")
    
    # Verificar permissão
    if session["tipo"] != "admin" and session["user_id"] != doc["obreiro_id"]:
        flash("Você não tem permissão para baixar este documento", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{doc['obreiro_id']}/documentos")
    
    if not os.path.exists(doc["caminho_arquivo"]):
        flash("Arquivo não encontrado no servidor", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{doc['obreiro_id']}/documentos")
    
    return_connection(conn)
    
    registrar_log("baixar_documento", "documento", id, dados_novos={"titulo": doc["titulo"]})
    
    return send_file(
        doc["caminho_arquivo"],
        as_attachment=True,
        download_name=doc["nome_arquivo"],
        mimetype="application/octet-stream"
    )

@app.route("/documentos/<int:id>/excluir")
@login_required
def excluir_documento(id):
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT d.*, u.id as obreiro_id
        FROM documentos_obreiro d
        JOIN usuarios u ON d.obreiro_id = u.id
        WHERE d.id = %s
    """, (id,))
    doc = cursor.fetchone()
    
    if not doc:
        flash("Documento não encontrado", "danger")
        return_connection(conn)
        return redirect("/obreiros")
    
    # Verificar permissão (apenas admin ou dono do documento)
    if session["tipo"] != "admin" and session["user_id"] != doc["obreiro_id"]:
        flash("Você não tem permissão para excluir este documento", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{doc['obreiro_id']}/documentos")
    
    try:
        # Remover arquivo físico
        if os.path.exists(doc["caminho_arquivo"]):
            os.remove(doc["caminho_arquivo"])
        
        # Remover registro do banco
        cursor.execute("DELETE FROM documentos_obreiro WHERE id = %s", (id,))
        conn.commit()
        
        registrar_log("excluir_documento", "documento", id, dados_anteriores={"titulo": doc["titulo"]})
        flash("Documento excluído com sucesso!", "success")
        
    except Exception as e:
        flash(f"Erro ao excluir documento: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/obreiros/{doc['obreiro_id']}/documentos")

# =============================
# ROTAS DE SUGESTÕES E MELHORIAS
# =============================

@app.route("/sugestoes")
@login_required
def listar_sugestoes():
    cursor, conn = get_db()
    
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
        query += " AND s.categoria = %s"
        params.append(categoria)
    if status:
        query += " AND s.status = %s"
        params.append(status)
    if prioridade:
        query += " AND s.prioridade = %s"
        params.append(prioridade)
    
    query += " ORDER BY s.prioridade = 'alta' DESC, s.votos DESC, s.data_criacao DESC"
    
    cursor.execute(query, params)
    sugestoes = cursor.fetchall()
    
    cursor.execute("SELECT * FROM categorias_sugestoes WHERE ativo = 1 ORDER BY nome")
    categorias = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("sugestoes/lista.html", 
                          sugestoes=sugestoes, 
                          categorias=categorias,
                          filtros={'categoria': categoria, 'status': status, 'prioridade': prioridade})

@app.route("/sugestoes/nova", methods=["GET", "POST"])
@admin_required
def nova_sugestao():
    cursor, conn = get_db()
    
    if request.method == "POST":
        titulo = request.form.get("titulo")
        descricao = request.form.get("descricao")
        categoria = request.form.get("categoria")
        prioridade = request.form.get("prioridade", "media")
        
        if not titulo or not descricao or not categoria:
            flash("Preencha todos os campos obrigatórios", "danger")
        else:
            try:
                cursor.execute("""
                    INSERT INTO sugestoes (titulo, descricao, categoria, prioridade, autor_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (titulo, descricao, categoria, prioridade, session["user_id"]))
                conn.commit()
                sugestao_id = cursor.lastrowid
                
                registrar_log("criar_sugestao", "sugestao", sugestao_id, 
                             dados_novos={"titulo": titulo, "categoria": categoria})
                flash("Sugestão enviada com sucesso!", "success")
                return_connection(conn)
                return redirect("/sugestoes")
            except Exception as e:
                flash(f"Erro ao salvar sugestão: {str(e)}", "danger")
                conn.rollback()
    
    cursor.execute("SELECT * FROM categorias_sugestoes WHERE ativo = 1 ORDER BY nome")
    categorias = cursor.fetchall()
    return_connection(conn)
    
    return render_template("sugestoes/nova.html", categorias=categorias)

@app.route("/sugestoes/<int:id>")
@login_required
def visualizar_sugestao(id):
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT s.*, u.nome_completo as autor_nome, u.id as autor_id
        FROM sugestoes s
        JOIN usuarios u ON s.autor_id = u.id
        WHERE s.id = %s
    """, (id,))
    sugestao = cursor.fetchone()
    
    if not sugestao:
        flash("Sugestão não encontrada", "danger")
        return_connection(conn)
        return redirect("/sugestoes")
    
    # Buscar comentários
    cursor.execute("""
        SELECT c.*, u.nome_completo as autor_nome
        FROM comentarios_sugestao c
        JOIN usuarios u ON c.autor_id = u.id
        WHERE c.sugestao_id = %s
        ORDER BY c.data_comentario DESC
    """, (id,))
    comentarios = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("sugestoes/visualizar.html", 
                          sugestao=sugestao, 
                          comentarios=comentarios)

@app.route("/sugestoes/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_sugestao(id):
    """Edita uma sugestão existente"""
    cursor, conn = get_db()
    
    if request.method == "POST":
        titulo = request.form.get("titulo")
        descricao = request.form.get("descricao")
        categoria = request.form.get("categoria")
        prioridade = request.form.get("prioridade")
        
        if not titulo or not descricao or not categoria:
            flash("Preencha todos os campos obrigatórios", "danger")
            return_connection(conn)
            return redirect(f"/sugestoes/{id}/editar")
        
        try:
            # Buscar dados antigos para log
            cursor.execute("SELECT * FROM sugestoes WHERE id = %s", (id,))
            dados_antigos = dict(cursor.fetchone())
            
            cursor.execute("""
                UPDATE sugestoes 
                SET titulo = %s, descricao = %s, categoria = %s, prioridade = %s,
                    data_atualizacao = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (titulo, descricao, categoria, prioridade, id))
            conn.commit()
            
            registrar_log("editar_sugestao", "sugestao", id, 
                         dados_anteriores=dados_antigos,
                         dados_novos={"titulo": titulo, "categoria": categoria})
            flash("Sugestão atualizada com sucesso!", "success")
            return_connection(conn)
            return redirect(f"/sugestoes/{id}")
            
        except Exception as e:
            flash(f"Erro ao atualizar sugestão: {str(e)}", "danger")
            conn.rollback()
            return_connection(conn)
            return redirect(f"/sugestoes/{id}/editar")
    
    # GET - Carregar dados da sugestão
    cursor.execute("SELECT * FROM sugestoes WHERE id = %s", (id,))
    sugestao = cursor.fetchone()
    
    if not sugestao:
        flash("Sugestão não encontrada", "danger")
        return_connection(conn)
        return redirect("/sugestoes")
    
    cursor.execute("SELECT * FROM categorias_sugestoes WHERE ativo = 1 ORDER BY nome")
    categorias = cursor.fetchall()
    return_connection(conn)
    
    return render_template("sugestoes/editar.html", sugestao=sugestao, categorias=categorias)

@app.route("/sugestoes/<int:id>/comentar", methods=["POST"])
@login_required
def comentar_sugestao(id):
    comentario = request.form.get("comentario")
    
    if not comentario:
        flash("Digite um comentário", "danger")
        return redirect(f"/sugestoes/{id}")
    
    cursor, conn = get_db()
    
    try:
        cursor.execute("""
            INSERT INTO comentarios_sugestao (sugestao_id, autor_id, comentario)
            VALUES (%s, %s, %s)
        """, (id, session["user_id"], comentario))
        conn.commit()
        
        registrar_log("comentar_sugestao", "sugestao", id, dados_novos={"comentario": comentario[:50]})
        flash("Comentário adicionado!", "success")
        
    except Exception as e:
        flash(f"Erro ao adicionar comentário: {str(e)}", "danger")
        conn.rollback()
    
    return_connection(conn)
    return redirect(f"/sugestoes/{id}")

@app.route("/sugestoes/<int:id>/votar")
@login_required
def votar_sugestao(id):
    cursor, conn = get_db()
    
    try:
        # Verificar se o usuário já votou nesta sugestão
        cursor.execute("""
            SELECT COUNT(*) as total FROM votos_sugestao 
            WHERE sugestao_id = %s AND usuario_id = %s
        """, (id, session["user_id"]))
        resultado = cursor.fetchone()
        
        if resultado and resultado["total"] > 0:
            flash("Você já votou nesta sugestão!", "warning")
        else:
            # Registrar voto
            cursor.execute("""
                INSERT INTO votos_sugestao (sugestao_id, usuario_id)
                VALUES (%s, %s)
            """, (id, session["user_id"]))
            
            # Atualizar contador de votos
            cursor.execute("UPDATE sugestoes SET votos = votos + 1 WHERE id = %s", (id,))
            conn.commit()
            
            registrar_log("votar_sugestao", "sugestao", id)
            flash("Voto computado com sucesso!", "success")
            
    except Exception as e:
        flash(f"Erro ao votar: {str(e)}", "danger")
        conn.rollback()
    
    return_connection(conn)
    return redirect(f"/sugestoes/{id}")

@app.route("/sugestoes/<int:id>/atualizar_status", methods=["POST"])
@admin_required
def atualizar_status_sugestao(id):
    status = request.form.get("status")
    observacao = request.form.get("observacao", "")
    
    cursor, conn = get_db()
    
    try:
        # Buscar dados antigos
        cursor.execute("SELECT status FROM sugestoes WHERE id = %s", (id,))
        status_antigo = cursor.fetchone()
        
        cursor.execute("""
            UPDATE sugestoes 
            SET status = %s, data_atualizacao = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (status, id))
        conn.commit()
        
        # Adicionar comentário automático sobre a mudança de status
        cursor.execute("""
            INSERT INTO comentarios_sugestao (sugestao_id, autor_id, comentario)
            VALUES (%s, %s, %s)
        """, (id, session["user_id"], f"Status alterado de '{status_antigo['status']}' para '{status}'. {observacao}"))
        conn.commit()
        
        registrar_log("atualizar_status_sugestao", "sugestao", id, 
                     dados_anteriores={"status": status_antigo['status']},
                     dados_novos={"status": status})
        flash(f"Status atualizado para: {status}", "success")
        
    except Exception as e:
        flash(f"Erro ao atualizar status: {str(e)}", "danger")
        conn.rollback()
    
    return_connection(conn)
    return redirect(f"/sugestoes/{id}")

@app.route("/sugestoes/<int:id>/implementar", methods=["POST"])
@admin_required
def implementar_sugestao(id):
    cursor, conn = get_db()
    
    try:
        cursor.execute("""
            UPDATE sugestoes 
            SET implementada = 1, 
                data_implementacao = CURRENT_TIMESTAMP,
                implementado_por = %s,
                status = 'implementada'
            WHERE id = %s
        """, (session["user_id"], id))
        conn.commit()
        
        registrar_log("implementar_sugestao", "sugestao", id)
        flash("Sugestão marcada como implementada!", "success")
        
    except Exception as e:
        flash(f"Erro ao implementar sugestão: {str(e)}", "danger")
        conn.rollback()
    
    return_connection(conn)
    return redirect(f"/sugestoes/{id}")

@app.route("/sugestoes/estatisticas")
@admin_required
def estatisticas_sugestoes():
    cursor, conn = get_db()
    
    # Estatísticas gerais
    cursor.execute("SELECT COUNT(*) as total FROM sugestoes")
    total = cursor.fetchone()["total"]
    
    cursor.execute("SELECT COUNT(*) as total FROM sugestoes WHERE status = 'pendente'")
    pendentes = cursor.fetchone()["total"]
    
    cursor.execute("SELECT COUNT(*) as total FROM sugestoes WHERE status = 'em_andamento'")
    em_andamento = cursor.fetchone()["total"]
    
    cursor.execute("SELECT COUNT(*) as total FROM sugestoes WHERE status = 'implementada'")
    implementadas = cursor.fetchone()["total"]
    
    cursor.execute("SELECT COUNT(*) as total FROM sugestoes WHERE status = 'rejeitada'")
    rejeitadas = cursor.fetchone()["total"]
    
    # Por categoria
    cursor.execute("""
        SELECT c.nome, COUNT(s.id) as total
        FROM categorias_sugestoes c
        LEFT JOIN sugestoes s ON c.nome = s.categoria
        GROUP BY c.nome
        ORDER BY total DESC
    """)
    por_categoria = cursor.fetchall()
    
    # Por prioridade
    cursor.execute("""
        SELECT prioridade, COUNT(*) as total
        FROM sugestoes
        GROUP BY prioridade
    """)
    por_prioridade = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("sugestoes/estatisticas.html",
                          total=total,
                          pendentes=pendentes,
                          em_andamento=em_andamento,
                          implementadas=implementadas,
                          rejeitadas=rejeitadas,
                          por_categoria=por_categoria,
                          por_prioridade=por_prioridade)
# =============================
# ROTAS DE FAMILIARES DOS OBREIROS
# =============================

@app.route("/obreiros/<int:obreiro_id>/familiares")
@login_required
def listar_familiares(obreiro_id):
    """Lista os familiares de um obreiro"""
    print(f"\n{'='*50}")
    print(f"DEBUG: listar_familiares chamado com obreiro_id={obreiro_id}")
    
    # Verificar permissão
    if session["tipo"] != "admin" and session["user_id"] != obreiro_id:
        flash("Você não tem permissão para acessar esta página", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    
    cursor, conn = get_db()
    
    try:
        # Buscar obreiro
        cursor.execute("SELECT id, nome_completo FROM usuarios WHERE id = %s", (obreiro_id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("Obreiro não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        print(f"DEBUG: Obreiro encontrado: {obreiro['nome_completo']}")
        
        # Buscar familiares - VERSÃO SIMPLES PARA TESTE
        cursor.execute("SELECT * FROM familiares WHERE obreiro_id = %s", (obreiro_id,))
        familiares = cursor.fetchall()
        
        print(f"DEBUG: Familiares encontrados na consulta simples: {len(familiares)}")
        
        # Converter para lista de dicionários e garantir que os dados estão corretos
        familiares_list = []
        for f in familiares:
            familiar_dict = dict(f)
            print(f"  - Familiar: {familiar_dict.get('nome')} ({familiar_dict.get('parentesco')})")
            familiares_list.append(familiar_dict)
        
        print(f"DEBUG: Total de familiares para template: {len(familiares_list)}")
        
    except Exception as e:
        print(f"ERRO: {e}")
        familiares_list = []
        flash(f"Erro ao carregar familiares: {str(e)}", "danger")
    
    return_connection(conn)
    
    return render_template("obreiros/familiares.html", 
                          obreiro=obreiro, 
                          familiares=familiares_list,
                          obreiro_id=obreiro_id)

                          # =============================
# ROTAS DE CONDECORAÇÕES
# =============================

@app.route("/obreiros/<int:obreiro_id>/condecoracoes")
@login_required
def listar_condecoracoes(obreiro_id):
    """Lista as condecorações de um obreiro"""
    if session["tipo"] != "admin" and session["user_id"] != obreiro_id:
        flash("Você não tem permissão para acessar esta página", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    
    cursor, conn = get_db()
    
    # Buscar obreiro
    cursor.execute("SELECT id, nome_completo FROM usuarios WHERE id = %s", (obreiro_id,))
    obreiro = cursor.fetchone()
    
    if not obreiro:
        flash("Obreiro não encontrado", "danger")
        return_connection(conn)
        return redirect("/obreiros")
    
    # Buscar condecorações do obreiro
    cursor.execute("""
        SELECT c.*, t.nome as tipo_nome, t.descricao as tipo_descricao, 
               t.cor, t.icone, t.nivel,
               u.nome_completo as concedido_por_nome
        FROM condecoracoes_obreiro c
        JOIN tipos_condecoracoes t ON c.tipo_id = t.id
        LEFT JOIN usuarios u ON c.concedido_por = u.id
        WHERE c.obreiro_id = %s
        ORDER BY t.nivel DESC, c.data_concessao DESC
    """, (obreiro_id,))
    condecoracoes = cursor.fetchall()
    
    # Buscar tipos disponíveis para nova condecoração
    cursor.execute("""
        SELECT * FROM tipos_condecoracoes 
        WHERE ativo = 1 
        ORDER BY nivel DESC, ordem
    """)
    tipos_condecoracoes = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("obreiros/condecoracoes.html", 
                          obreiro=obreiro, 
                          condecoracoes=condecoracoes,
                          tipos_condecoracoes=tipos_condecoracoes,
                          obreiro_id=obreiro_id)

@app.route("/obreiros/<int:obreiro_id>/condecoracoes/nova", methods=["POST"])
@admin_required
def nova_condecoracao(obreiro_id):
    """Concede uma nova condecoração a um obreiro"""
    cursor, conn = get_db()
    
    tipo_id = request.form.get("tipo_id")
    data_concessao = request.form.get("data_concessao")
    data_validade = request.form.get("data_validade")
    motivo = request.form.get("motivo")
    numero_registro = request.form.get("numero_registro")
    observacoes = request.form.get("observacoes")
    
    if not tipo_id or not data_concessao:
        flash("Tipo de condecoração e data são obrigatórios", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{obreiro_id}/condecoracoes")
    
    try:
        # Tratar data de validade vazia
        data_validade = data_validade if data_validade and data_validade.strip() else None
        
        cursor.execute("""
            INSERT INTO condecoracoes_obreiro 
            (obreiro_id, tipo_id, data_concessao, data_validade, concedido_por, 
             motivo, numero_registro, observacoes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (obreiro_id, tipo_id, data_concessao, data_validade, session["user_id"],
              motivo, numero_registro, observacoes))
        conn.commit()
        
        registrar_log("conceder_condecoracao", "condecoracao", cursor.lastrowid, 
                     dados_novos={"obreiro_id": obreiro_id, "tipo_id": tipo_id})
        flash("Condecoração concedida com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao conceder condecoração: {e}")
        conn.rollback()
        flash(f"Erro ao conceder condecoração: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/obreiros/{obreiro_id}/condecoracoes")

@app.route("/obreiros/condecoracoes/excluir/<int:id>")
@admin_required
def excluir_condecoracao(id):
    """Exclui uma condecoração"""
    cursor, conn = get_db()
    
    try:
        cursor.execute("SELECT obreiro_id FROM condecoracoes_obreiro WHERE id = %s", (id,))
        condecoracao = cursor.fetchone()
        
        if condecoracao:
            obreiro_id = condecoracao["obreiro_id"]
            cursor.execute("DELETE FROM condecoracoes_obreiro WHERE id = %s", (id,))
            conn.commit()
            registrar_log("excluir_condecoracao", "condecoracao", id)
            flash("Condecoração excluída com sucesso!", "success")
        else:
            flash("Condecoração não encontrada", "danger")
        
    except Exception as e:
        print(f"Erro ao excluir condecoração: {e}")
        conn.rollback()
        flash(f"Erro ao excluir condecoração: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/obreiros/{condecoracao['obreiro_id']}/condecoracoes" if condecoracao else "/obreiros")

@app.route("/tipos_condecoracoes")
@admin_required
def listar_tipos_condecoracoes():
    """Lista os tipos de condecorações"""
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT * FROM tipos_condecoracoes 
        ORDER BY nivel DESC, ordem
    """)
    tipos = cursor.fetchall()
    
    return_connection(conn)
    return render_template("admin/tipos_condecoracoes.html", tipos=tipos)

@app.route("/tipos_condecoracoes/novo", methods=["GET", "POST"])
@admin_required
def novo_tipo_condecoracao():
    """Cadastra um novo tipo de condecoração"""
    if request.method == "POST":
        nome = request.form.get("nome")
        descricao = request.form.get("descricao")
        nivel = request.form.get("nivel", 1)
        cor = request.form.get("cor", "#ffc107")
        icone = request.form.get("icone", "bi-award")
        ordem = request.form.get("ordem", 0)
        
        if not nome:
            flash("Nome da condecoração é obrigatório", "danger")
        else:
            cursor, conn = get_db()
            try:
                cursor.execute("""
                    INSERT INTO tipos_condecoracoes (nome, descricao, nivel, cor, icone, ordem, ativo)
                    VALUES (%s, %s, %s, %s, %s, %s, 1)
                """, (nome, descricao, nivel, cor, icone, ordem))
                conn.commit()
                flash(f"Tipo de condecoração '{nome}' adicionado com sucesso!", "success")
                return_connection(conn)
                return redirect("/tipos_condecoracoes")
            except Exception as e:
                flash(f"Erro ao adicionar: {str(e)}", "danger")
                conn.rollback()
            return_connection(conn)
    
    return render_template("admin/tipo_condecoracao_form.html")

@app.route("/obreiros/<int:obreiro_id>/familiares/novo", methods=["GET", "POST"])
@login_required
def novo_familiar(obreiro_id):
    """Cadastra um novo familiar"""
    if session["tipo"] != "admin" and session["user_id"] != obreiro_id:
        flash("Você não tem permissão para acessar esta página", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    
    cursor, conn = get_db()
    
    # Buscar obreiro
    cursor.execute("SELECT nome_completo FROM usuarios WHERE id = %s", (obreiro_id,))
    obreiro = cursor.fetchone()
    
    if not obreiro:
        flash("Obreiro não encontrado", "danger")
        return_connection(conn)
        return redirect("/obreiros")
    
    if request.method == "POST":
        nome = request.form.get("nome")
        parentesco = request.form.get("parentesco")
        data_nascimento = request.form.get("data_nascimento")
        telefone = request.form.get("telefone")
        email = request.form.get("email")
        observacoes = request.form.get("observacoes")
        receber_notificacoes = 1 if request.form.get("receber_notificacoes") else 0
        
        if not nome or not parentesco:
            flash("Nome e parentesco são obrigatórios", "danger")
        else:
            try:
                # Tratar data vazia
                data_nascimento = data_nascimento if data_nascimento and data_nascimento.strip() else None
                
                cursor.execute("""
                    INSERT INTO familiares 
                    (obreiro_id, nome, parentesco, data_nascimento, telefone, email, 
                     observacoes, receber_notificacoes, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (obreiro_id, nome, parentesco, data_nascimento, telefone, 
                      email, observacoes, receber_notificacoes, session["user_id"]))
                conn.commit()
                
                registrar_log("criar_familiar", "familiar", cursor.lastrowid, 
                             dados_novos={"nome": nome, "parentesco": parentesco})
                flash(f"Familiar '{nome}' adicionado com sucesso!", "success")
                return_connection(conn)
                return redirect(f"/obreiros/{obreiro_id}/familiares")
                
            except Exception as e:
                flash(f"Erro ao adicionar familiar: {str(e)}", "danger")
                conn.rollback()
    
    return_connection(conn)
    return render_template("obreiros/familiar_form.html", 
                          obreiro=obreiro, 
                          obreiro_id=obreiro_id,
                          familiar=None)

@app.route("/obreiros/familiares/editar/<int:id>", methods=["GET", "POST"])
@login_required
def editar_familiar(id):
    """Edita um familiar existente"""
    cursor, conn = get_db()
    
    # Buscar familiar
    cursor.execute("""
        SELECT f.*, u.id as obreiro_id, u.nome_completo as obreiro_nome
        FROM familiares f
        JOIN usuarios u ON f.obreiro_id = u.id
        WHERE f.id = %s
    """, (id,))
    familiar = cursor.fetchone()
    
    if not familiar:
        flash("Familiar não encontrado", "danger")
        return_connection(conn)
        return redirect("/obreiros")
    
    # Verificar permissão
    if session["tipo"] != "admin" and session["user_id"] != familiar["obreiro_id"]:
        flash("Você não tem permissão para editar este familiar", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{familiar['obreiro_id']}/familiares")
    
    if request.method == "POST":
        nome = request.form.get("nome")
        parentesco = request.form.get("parentesco")
        data_nascimento = request.form.get("data_nascimento")
        telefone = request.form.get("telefone")
        email = request.form.get("email")
        observacoes = request.form.get("observacoes")
        receber_notificacoes = 1 if request.form.get("receber_notificacoes") else 0
        
        if not nome or not parentesco:
            flash("Nome e parentesco são obrigatórios", "danger")
        else:
            try:
                # Tratar data vazia
                data_nascimento = data_nascimento if data_nascimento and data_nascimento.strip() else None
                
                # Buscar dados antigos para log
                dados_antigos = dict(familiar)
                
                cursor.execute("""
                    UPDATE familiares 
                    SET nome = %s, parentesco = %s, data_nascimento = %s,
                        telefone = %s, email = %s, observacoes = %s,
                        receber_notificacoes = %s
                    WHERE id = %s
                """, (nome, parentesco, data_nascimento, telefone, email, 
                      observacoes, receber_notificacoes, id))
                conn.commit()
                
                registrar_log("editar_familiar", "familiar", id, 
                             dados_anteriores=dados_antigos,
                             dados_novos={"nome": nome, "parentesco": parentesco})
                flash("Familiar atualizado com sucesso!", "success")
                return_connection(conn)
                return redirect(f"/obreiros/{familiar['obreiro_id']}/familiares")
                
            except Exception as e:
                flash(f"Erro ao atualizar familiar: {str(e)}", "danger")
                conn.rollback()
    
    return_connection(conn)
    return render_template("obreiros/familiar_form.html", 
                          obreiro={"nome_completo": familiar["obreiro_nome"]},
                          obreiro_id=familiar["obreiro_id"],
                          familiar=familiar)

@app.route("/obreiros/familiares/excluir/<int:id>")
@login_required
def excluir_familiar(id):
    """Exclui um familiar"""
    cursor, conn = get_db()
    
    # Buscar familiar
    cursor.execute("""
        SELECT f.*, u.id as obreiro_id
        FROM familiares f
        JOIN usuarios u ON f.obreiro_id = u.id
        WHERE f.id = %s
    """, (id,))
    familiar = cursor.fetchone()
    
    if not familiar:
        flash("Familiar não encontrado", "danger")
        return_connection(conn)
        return redirect("/obreiros")
    
    # Verificar permissão
    if session["tipo"] != "admin" and session["user_id"] != familiar["obreiro_id"]:
        flash("Você não tem permissão para excluir este familiar", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{familiar['obreiro_id']}/familiares")
    
    try:
        dados = dict(familiar)
        cursor.execute("DELETE FROM familiares WHERE id = %s", (id,))
        conn.commit()
        
        registrar_log("excluir_familiar", "familiar", id, dados_anteriores=dados)
        flash(f"Familiar '{familiar['nome']}' excluído com sucesso!", "success")
        
    except Exception as e:
        flash(f"Erro ao excluir familiar: {str(e)}", "danger")
        conn.rollback()
    
    return_connection(conn)
    return redirect(f"/obreiros/{familiar['obreiro_id']}/familiares")

@app.route("/api/aniversariantes")
@login_required
def api_aniversariantes():
    """Retorna os aniversariantes do mês"""
    cursor, conn = get_db()
    
    mes_atual = datetime.now().month
    dia_atual = datetime.now().day
    
    cursor.execute("""
        SELECT f.*, u.nome_completo as obreiro_nome, u.id as obreiro_id
        FROM familiares f
        JOIN usuarios u ON f.obreiro_id = u.id
        WHERE f.receber_notificacoes = 1
          AND EXTRACT(MONTH FROM f.data_nascimento) = %s
          AND u.ativo = 1
        ORDER BY EXTRACT(DAY FROM f.data_nascimento)
    """, (mes_atual,))
    
    aniversariantes = cursor.fetchall()
    
    # Separar aniversariantes do dia e do mês
    aniversariantes_hoje = []
    aniversariantes_mes = []
    
    for a in aniversariantes:
        if a["data_nascimento"] and a["data_nascimento"].day == dia_atual:
            aniversariantes_hoje.append(a)
        else:
            aniversariantes_mes.append(a)
    
    return_connection(conn)
    
    return jsonify({
        "hoje": aniversariantes_hoje,
        "mes": aniversariantes_mes
    })
                          
# =============================
# FUNÇÃO DE ENVIO DE WHATSAPP
# =============================

def enviar_whatsapp(numero, mensagem):
    """
    Envia mensagem via WhatsApp abrindo o WhatsApp Web
    Retorna True se o link foi aberto com sucesso
    """
    try:
        # Remove caracteres não numéricos
        numero_limpo = ''.join(filter(str.isdigit, numero))
        
        # Formata o número para o padrão internacional
        if len(numero_limpo) == 11:
            numero_limpo = '55' + numero_limpo
        elif len(numero_limpo) == 10:
            numero_limpo = '55' + numero_limpo
        
        # Codifica a mensagem para URL
        mensagem_codificada = quote(mensagem)
        
        # Cria o link do WhatsApp
        url = f"https://web.whatsapp.com/send?phone={numero_limpo}&text={mensagem_codificada}"
        
        # Abre no navegador
        webbrowser.open(url)
        
        print(f"✅ Link do WhatsApp aberto para {numero_limpo}")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao abrir WhatsApp: {e}")
        return False

# =============================
# FUNÇÕES DE NOTIFICAÇÃO WHATSAPP
# =============================
def notificar_nova_reuniao_whatsapp(reuniao_id, titulo, data, hora):
    """Notifica todos os obreiros sobre nova reunião via WhatsApp"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("SELECT lembrete_reuniao FROM whatsapp_config WHERE id = 1")
        config = cursor.fetchone()
        
        if config and config["lembrete_reuniao"] == 1:
            cursor.execute("SELECT id, telefone, nome_completo FROM usuarios WHERE ativo = 1 AND telefone IS NOT NULL AND telefone != ''")
            obreiros = cursor.fetchall()
            
            for obreiro in obreiros:
                if obreiro["telefone"]:
                    numero = obreiro["telefone"]
                    primeiro_nome = obreiro["nome_completo"].split()[0] if obreiro["nome_completo"] else "Irmão"
                    
                    mensagem = f"""📅 *NOVA REUNIÃO AGENDADA*

Olá {primeiro_nome},

Uma nova reunião foi agendada:
📝 {titulo}
📅 Data: {data}
⏰ Horário: {hora}

Acesse o sistema para mais detalhes:
🔗 http://localhost:5000/reunioes/{reuniao_id}

Atenciosamente,
Sistema Maçônico"""
                    
                    threading.Thread(target=enviar_whatsapp, args=(numero, mensagem)).start()
                    time.sleep(0.5)
        return_connection(conn)
    except Exception as e:
        print(f"Erro ao notificar nova reunião: {e}")

def notificar_comunicado_whatsapp(comunicado_id, titulo, conteudo):
    """Notifica todos os obreiros sobre novo comunicado via WhatsApp"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("SELECT notificar_comunicado FROM whatsapp_config WHERE id = 1")
        config = cursor.fetchone()
        
        if config and config["notificar_comunicado"] == 1:
            cursor.execute("SELECT id, telefone, nome_completo FROM usuarios WHERE ativo = 1 AND telefone IS NOT NULL AND telefone != ''")
            obreiros = cursor.fetchall()
            
            for obreiro in obreiros:
                if obreiro["telefone"]:
                    numero = obreiro["telefone"]
                    primeiro_nome = obreiro["nome_completo"].split()[0] if obreiro["nome_completo"] else "Irmão"
                    
                    conteudo_resumido = conteudo[:200] + "..." if len(conteudo) > 200 else conteudo
                    
                    mensagem = f"""📢 *NOVO COMUNICADO*

Olá {primeiro_nome},

{titulo}

{conteudo_resumido}

Acesse para visualizar completo:
🔗 http://localhost:5000/comunicados/{comunicado_id}

Atenciosamente,
Sistema Maçônico"""
                    
                    threading.Thread(target=enviar_whatsapp, args=(numero, mensagem)).start()
                    time.sleep(0.5)
        return_connection(conn)
    except Exception as e:
        print(f"Erro ao notificar comunicado: {e}")

# =============================
# ROTAS DE CONFIGURAÇÃO DE E-MAIL
# =============================

@app.route("/config/email", methods=["GET", "POST"])
@admin_required
def config_email():
    """Configuração de e-mail"""
    cursor, conn = get_db()
    
    if request.method == "POST":
        server = request.form.get("server")
        port = request.form.get("port")
        use_tls = 1 if request.form.get("use_tls") else 0
        username = request.form.get("username")
        password = request.form.get("password")
        sender = request.form.get("sender")
        sender_name = request.form.get("sender_name")
        active = 1 if request.form.get("active") else 0
        
        if not server or not port or not username or not password or not sender:
            flash("Preencha todos os campos obrigatórios", "danger")
        else:
            try:
                # Desativar configurações anteriores
                if active:
                    cursor.execute("UPDATE email_settings SET active = 0")
                
                # Inserir nova configuração
                cursor.execute("""
                    INSERT INTO email_settings 
                    (server, port, use_tls, username, password, sender, sender_name, active)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (server, port, use_tls, username, password, sender, sender_name, active))
                conn.commit()
                
                flash("Configuração de e-mail salva com sucesso!", "success")
                
            except Exception as e:
                flash(f"Erro ao salvar configuração: {str(e)}", "danger")
                conn.rollback()
    
    # Buscar configuração atual
    cursor.execute("""
        SELECT * FROM email_settings 
        WHERE active = 1 
        ORDER BY id DESC LIMIT 1
    """)
    config = cursor.fetchone()
    
    return_connection(conn)
    return render_template("admin/config_email.html", config=config)

@app.route("/config/email/testar", methods=["POST"])
@admin_required
def testar_email():
    """Testa o envio de e-mail"""
    cursor, conn = get_db()
    
    email_teste = request.form.get("email_teste")
    if not email_teste:
        flash("Informe um e-mail para teste", "danger")
        return redirect("/config/email")
    
    # Buscar configuração
    cursor.execute("""
        SELECT * FROM email_settings 
        WHERE active = 1 
        ORDER BY id DESC LIMIT 1
    """)
    config = cursor.fetchone()
    return_connection(conn)
    
    if not config:
        flash("Nenhuma configuração de e-mail ativa", "danger")
        return redirect("/config/email")
    
    from email_service import EmailService
    email_service = EmailService()
    
    assunto = "Teste de Configuração - Sistema Maçônico"
    corpo_html = """
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body>
        <h2>✅ Teste de E-mail</h2>
        <p>Esta é uma mensagem de teste do Sistema Maçônico.</p>
        <p>Se você está recebendo este e-mail, a configuração está funcionando corretamente!</p>
        <p>Data e hora do teste: {}</p>
    </body>
    </html>
    """.format(datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
    
    if email_service.enviar_email(email_teste, assunto, corpo_html):
        flash(f"E-mail de teste enviado com sucesso para {email_teste}!", "success")
    else:
        flash("Falha ao enviar e-mail de teste. Verifique as configurações.", "danger")
    
    return redirect("/config/email")        

# =============================
# ROTAS WHATSAPP
# =============================
@app.route("/config/whatsapp", methods=["GET", "POST"])
@admin_required
def config_whatsapp():
    cursor, conn = get_db()
    
    # Cria tabela se não existir (já deve existir pelo create_tables)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_config (
            id SERIAL PRIMARY KEY,
            notificar_ausencia INTEGER DEFAULT 1,
            notificar_nova_reuniao INTEGER DEFAULT 1,
            notificar_comunicado INTEGER DEFAULT 1,
            lembrete_reuniao INTEGER DEFAULT 1,
            grupo_id TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    
    # Insere configuração padrão se não existir
    cursor.execute("SELECT COUNT(*) as total FROM whatsapp_config")
    if cursor.fetchone()["total"] == 0:
        cursor.execute("""
            INSERT INTO whatsapp_config (notificar_ausencia, notificar_nova_reuniao, notificar_comunicado, lembrete_reuniao)
            VALUES (1, 1, 1, 1)
        """)
        conn.commit()
    
    if request.method == "POST":
        notificar_ausencia = 1 if request.form.get("notificar_ausencia") else 0
        notificar_nova_reuniao = 1 if request.form.get("notificar_nova_reuniao") else 0
        notificar_comunicado = 1 if request.form.get("notificar_comunicado") else 0
        lembrete_reuniao = 1 if request.form.get("lembrete_reuniao") else 0
        grupo_id = request.form.get("grupo_id", "")
        
        cursor.execute("""
            UPDATE whatsapp_config 
            SET notificar_ausencia = %s, 
                notificar_nova_reuniao = %s, 
                notificar_comunicado = %s, 
                lembrete_reuniao = %s, 
                grupo_id = %s, 
                updated_at = CURRENT_TIMESTAMP 
            WHERE id = 1
        """, (notificar_ausencia, notificar_nova_reuniao, notificar_comunicado, lembrete_reuniao, grupo_id))
        conn.commit()
        
        registrar_log("configurar_whatsapp", "config", 1, dados_novos={"notificacoes": "atualizadas", "grupo": grupo_id})
        flash("Configurações do WhatsApp salvas com sucesso!", "success")
        return_connection(conn)
        return redirect("/config/whatsapp")
    
    cursor.execute("SELECT * FROM whatsapp_config WHERE id = 1")
    config = cursor.fetchone()
    return_connection(conn)
    
    return render_template("config/whatsapp.html", config=config)

@app.route("/testar_whatsapp", methods=["POST"])
@admin_required
def testar_whatsapp():
    numero = request.form.get("numero")
    mensagem = request.form.get("mensagem")
    
    if not numero or not mensagem:
        flash("Número e mensagem são obrigatórios", "danger")
        return redirect("/config/whatsapp")
    
    if enviar_whatsapp(numero, mensagem):
        flash("Mensagem de teste aberta no navegador! Verifique se o WhatsApp Web está aberto e clique em enviar.", "success")
        registrar_log("testar_whatsapp", "whatsapp", None, dados_novos={"numero": numero})
    else:
        flash("Erro ao abrir WhatsApp. Verifique o número digitado.", "danger")
    
    return redirect("/config/whatsapp")


# =============================
# INICIALIZAÇÃO
# =============================
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)