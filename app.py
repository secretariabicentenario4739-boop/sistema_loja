from flask import Flask, render_template, request, redirect, session, flash, jsonify, send_file, after_this_request
import sqlite3
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
import os

# Configuração de uploads
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads', 'documentos')
ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'png', 'gif', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'zip', 'rar'}

# Criar pasta se não existir
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Chave segura aleatória

# =============================
# CONTEXTO GLOBAL PARA TEMPLATES
# =============================
@app.context_processor
def inject_global():
    return {'datetime': datetime, 'now': datetime.now()}

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
# CONEXÃO COM BANCO DE DADOS
# =============================
def get_db():
    conn = sqlite3.connect("banco.db")
    conn.row_factory = sqlite3.Row
    return conn

# =============================
# FUNÇÃO DE AUDITORIA
# =============================
def registrar_log(acao, entidade=None, entidade_id=None, dados_anteriores=None, dados_novos=None):
    """Registra uma ação no log de auditoria"""
    if "user_id" not in session:
        return
    
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        if dados_anteriores and isinstance(dados_anteriores, dict):
            dados_anteriores = json.dumps(dados_anteriores, ensure_ascii=False, default=str)
        if dados_novos and isinstance(dados_novos, dict):
            dados_novos = json.dumps(dados_novos, ensure_ascii=False, default=str)
        
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
# INICIALIZAÇÃO DO BANCO
# =============================
def init_db():
    """Inicializa o banco de dados com todas as tabelas"""
    conn = get_db()
    cursor = conn.cursor()

    # Candidatos
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS candidatos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        status TEXT DEFAULT 'Em análise',
        data_criacao TEXT NOT NULL,
        data_fechamento TEXT,
        fechado INTEGER DEFAULT 0,
        resultado_final TEXT
    )
    """)

    # Usuários/obreiros
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

    # Sindicâncias
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sindicancias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidato_id INTEGER NOT NULL,
        sindicante TEXT NOT NULL,
        parecer TEXT NOT NULL,
        data_envio TEXT NOT NULL,
        FOREIGN KEY (candidato_id) REFERENCES candidatos (id) ON DELETE CASCADE,
        UNIQUE(candidato_id, sindicante)
    )
    """)

    # Pareceres conclusivos
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pareceres_conclusivos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidato_id INTEGER NOT NULL,
        sindicante TEXT NOT NULL,
        parecer_texto TEXT NOT NULL,
        conclusao TEXT NOT NULL,
        observacoes TEXT,
        cim_numero TEXT,
        data_parecer TEXT NOT NULL,
        data_envio TEXT NOT NULL,
        fontes TEXT NOT NULL,
        loja_nome TEXT,
        loja_numero TEXT,
        loja_orient TEXT,
        FOREIGN KEY (candidato_id) REFERENCES candidatos (id) ON DELETE CASCADE,
        UNIQUE(candidato_id, sindicante)
    )
    """)

    # Lojas
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS lojas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        numero TEXT NOT NULL,
        oriente TEXT,
        cidade TEXT,
        estado TEXT,
        data_fundacao TEXT
    )
    """)

    # Cargos
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cargos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        sigla TEXT,
        grau_minimo INTEGER DEFAULT 1,
        ordem INTEGER,
        descricao TEXT,
        ativo INTEGER DEFAULT 1
    )
    """)

    # Ocupação de cargos
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ocupacao_cargos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        obreiro_id INTEGER NOT NULL,
        cargo_id INTEGER NOT NULL,
        loja_id INTEGER,
        data_inicio DATE NOT NULL,
        data_fim DATE,
        gestao TEXT,
        ativo INTEGER DEFAULT 1,
        FOREIGN KEY (obreiro_id) REFERENCES usuarios (id) ON DELETE CASCADE,
        FOREIGN KEY (cargo_id) REFERENCES cargos (id) ON DELETE CASCADE,
        FOREIGN KEY (loja_id) REFERENCES lojas (id) ON DELETE SET NULL
    )
    """)

    # Histórico de graus
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS historico_graus (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        obreiro_id INTEGER NOT NULL,
        grau INTEGER NOT NULL,
        data DATE NOT NULL,
        observacao TEXT,
        FOREIGN KEY (obreiro_id) REFERENCES usuarios (id) ON DELETE CASCADE
    )
    """)

    # Reuniões
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reunioes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        titulo TEXT NOT NULL,
        tipo TEXT DEFAULT 'Ordinária',
        grau INTEGER,
        data DATE NOT NULL,
        hora_inicio TIME,
        hora_termino TIME,
        local TEXT,
        loja_id INTEGER,
        pauta TEXT,
        observacoes TEXT,
        status TEXT DEFAULT 'agendada',
        data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        criado_por INTEGER,
        FOREIGN KEY (loja_id) REFERENCES lojas (id),
        FOREIGN KEY (criado_por) REFERENCES usuarios (id)
    )
    """)

    # Presença
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS presenca (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reuniao_id INTEGER NOT NULL,
        obreiro_id INTEGER NOT NULL,
        presente INTEGER DEFAULT 0,
        tipo_ausencia TEXT,
        justificativa TEXT,
        comprovante TEXT,
        validado_por INTEGER,
        data_validacao TIMESTAMP,
        observacao_validacao TEXT,
        data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        registrado_por INTEGER,
        FOREIGN KEY (reuniao_id) REFERENCES reunioes (id) ON DELETE CASCADE,
        FOREIGN KEY (obreiro_id) REFERENCES usuarios (id) ON DELETE CASCADE,
        FOREIGN KEY (registrado_por) REFERENCES usuarios (id),
        UNIQUE(reuniao_id, obreiro_id)
    )
    """)

    # Atas
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS atas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reuniao_id INTEGER UNIQUE NOT NULL,
        conteudo TEXT NOT NULL,
        redator_id INTEGER,
        aprovada INTEGER DEFAULT 0,
        data_aprovacao DATE,
        versao INTEGER DEFAULT 1,
        arquivo_pdf TEXT,
        data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        numero_ata INTEGER,
        ano_ata INTEGER,
        tipo_ata TEXT DEFAULT 'Ordinária',
        redator_nome TEXT,
        secretario_id INTEGER,
        aprovada_em DATE,
        aprovada_por INTEGER,
        observacoes_aprovacao TEXT,
        modelo_ata TEXT,
        assinaturas TEXT,
        hash_documento TEXT,
        data_impressao TIMESTAMP,
        impresso_por INTEGER,
        FOREIGN KEY (reuniao_id) REFERENCES reunioes (id) ON DELETE CASCADE,
        FOREIGN KEY (redator_id) REFERENCES usuarios (id),
        FOREIGN KEY (aprovada_por) REFERENCES usuarios (id),
        FOREIGN KEY (impresso_por) REFERENCES usuarios (id)
    )
    """)

    # Modelos de ata
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS modelos_ata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        descricao TEXT,
        tipo TEXT DEFAULT 'Ordinária',
        estrutura TEXT NOT NULL,
        campos_personalizados TEXT,
        ativo INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by INTEGER,
        FOREIGN KEY (created_by) REFERENCES usuarios (id)
    )
    """)

    # Assinaturas de atas
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS assinaturas_ata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ata_id INTEGER NOT NULL,
        obreiro_id INTEGER NOT NULL,
        cargo_id INTEGER,
        assinatura TEXT,
        data_assinatura TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ip_assinatura TEXT,
        hash_assinatura TEXT,
        validada INTEGER DEFAULT 0,
        FOREIGN KEY (ata_id) REFERENCES atas (id) ON DELETE CASCADE,
        FOREIGN KEY (obreiro_id) REFERENCES usuarios (id),
        FOREIGN KEY (cargo_id) REFERENCES cargos (id),
        UNIQUE(ata_id, obreiro_id)
    )
    """)

    # Anexos de atas
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS anexos_ata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ata_id INTEGER NOT NULL,
        nome_arquivo TEXT NOT NULL,
        caminho_arquivo TEXT NOT NULL,
        tipo_arquivo TEXT,
        tamanho INTEGER,
        data_upload TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        uploaded_by INTEGER,
        FOREIGN KEY (ata_id) REFERENCES atas (id) ON DELETE CASCADE,
        FOREIGN KEY (uploaded_by) REFERENCES usuarios (id)
    )
    """)

    # Tipos de ausência
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tipos_ausencia (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        descricao TEXT,
        requer_comprovante INTEGER DEFAULT 0,
        cor TEXT DEFAULT '#6c757d',
        ativo INTEGER DEFAULT 1
    )
    """)

    # Estatísticas de presença
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS estatisticas_presenca (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        obreiro_id INTEGER NOT NULL,
        ano INTEGER NOT NULL,
        mes INTEGER NOT NULL,
        total_reunioes INTEGER DEFAULT 0,
        presencas INTEGER DEFAULT 0,
        ausencias_justificadas INTEGER DEFAULT 0,
        ausencias_injustificadas INTEGER DEFAULT 0,
        percentual REAL DEFAULT 0,
        FOREIGN KEY (obreiro_id) REFERENCES usuarios (id) ON DELETE CASCADE,
        UNIQUE(obreiro_id, ano, mes)
    )
    """)

    # Alertas de presença
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS alertas_presenca (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        obreiro_id INTEGER NOT NULL,
        tipo TEXT NOT NULL,
        mensagem TEXT NOT NULL,
        data_gerado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        resolvido INTEGER DEFAULT 0,
        data_resolucao TIMESTAMP,
        resolvido_por INTEGER,
        FOREIGN KEY (obreiro_id) REFERENCES usuarios (id) ON DELETE CASCADE,
        FOREIGN KEY (resolvido_por) REFERENCES usuarios (id)
    )
    """)

    # Comunicados
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comunicados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        titulo TEXT NOT NULL,
        conteudo TEXT NOT NULL,
        tipo TEXT DEFAULT 'informativo',
        prioridade TEXT DEFAULT 'normal',
        data_inicio DATE NOT NULL,
        data_fim DATE,
        ativo INTEGER DEFAULT 1,
        criado_por INTEGER NOT NULL,
        data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (criado_por) REFERENCES usuarios (id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS visualizacoes_comunicado (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        comunicado_id INTEGER NOT NULL,
        obreiro_id INTEGER NOT NULL,
        data_visualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (comunicado_id) REFERENCES comunicados (id) ON DELETE CASCADE,
        FOREIGN KEY (obreiro_id) REFERENCES usuarios (id) ON DELETE CASCADE,
        UNIQUE(comunicado_id, obreiro_id)
    )
    """)

    # Email settings
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS email_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        server TEXT NOT NULL,
        port INTEGER NOT NULL,
        use_tls INTEGER DEFAULT 1,
        username TEXT NOT NULL,
        password TEXT NOT NULL,
        sender TEXT NOT NULL,
        sender_name TEXT,
        active INTEGER DEFAULT 1
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS notificacoes_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        destinatario TEXT NOT NULL,
        assunto TEXT NOT NULL,
        corpo TEXT NOT NULL,
        tipo TEXT,
        status TEXT,
        data_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        erro TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS preferencias_notificacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        obreiro_id INTEGER NOT NULL,
        lembrete_reuniao INTEGER DEFAULT 1,
        alerta_ausencia INTEGER DEFAULT 1,
        email TEXT,
        FOREIGN KEY (obreiro_id) REFERENCES usuarios (id)
    )
    """)

    # LOGS DE AUDITORIA
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS logs_auditoria (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id INTEGER NOT NULL,
        usuario_nome TEXT NOT NULL,
        acao TEXT NOT NULL,
        entidade TEXT,
        entidade_id INTEGER,
        dados_anteriores TEXT,
        dados_novos TEXT,
        ip TEXT,
        user_agent TEXT,
        data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (usuario_id) REFERENCES usuarios (id)
    )
    """)

    # ==================== DADOS PADRÃO ====================
    # Loja padrão
    cursor.execute("SELECT COUNT(*) as count FROM lojas")
    if cursor.fetchone()["count"] == 0:
        cursor.execute("""
            INSERT INTO lojas (nome, numero, oriente, cidade, estado)
            VALUES (?, ?, ?, ?, ?)
        """, ("ARLS Estrela do Oriente", "123", "Oriente de São Paulo", "São Paulo", "SP"))

    # Tipos de reunião padrão
    cursor.execute("SELECT COUNT(*) as total FROM tipos_reuniao")
    if cursor.fetchone()["total"] == 0:
        tipos = [
            ("Ordinária", "Reunião ordinária semanal/mensal", "#28a745"),
            ("Magna", "Reunião magna solene", "#dc3545"),
            ("Extraordinária", "Reunião extraordinária convocada", "#ffc107"),
            ("Administrativa", "Reunião da administração", "#17a2b8"),
            ("Iniciação", "Sessão de iniciação de novos obreiros", "#6610f2"),
            ("Elevação", "Sessão de elevação ao grau de Companheiro", "#fd7e14"),
            ("Exaltação", "Sessão de exaltação ao grau de Mestre", "#20c997")
        ]
        for tipo in tipos:
            cursor.execute("INSERT INTO tipos_reuniao (nome, descricao, cor) VALUES (?, ?, ?)", tipo)

    # Cargos padrão
    cursor.execute("SELECT COUNT(*) as total FROM cargos")
    if cursor.fetchone()["total"] == 0:
        cargos_padrao = [
            ("Venerável Mestre", "VM", 3, 1, "Presidente da loja"),
            ("1º Vigilante", "1V", 3, 2, "Primeiro vigilante"),
            ("2º Vigilante", "2V", 3, 3, "Segundo vigilante"),
            ("Orador", "OR", 3, 4, "Orador da loja"),
            ("Secretário", "SEC", 2, 5, "Secretário"),
            ("Tesoureiro", "TES", 2, 6, "Tesoureiro"),
            ("Chanceler", "CHAN", 2, 7, "Guardião do templo"),
            ("Mestre de Cerimônias", "MC", 2, 8, "Cerimonialista"),
            ("1º Diácono", "1D", 2, 9, "Primeiro diácono"),
            ("2º Diácono", "2D", 2, 10, "Segundo diácono"),
            ("Cobridor", "COB", 2, 11, "Cobridor interno"),
            ("Porta-Estandarte", "PE", 1, 12, "Porta estandarte"),
            ("Porta-Espada", "PESP", 1, 13, "Porta espada"),
            ("Hospitaleiro", "HOSP", 1, 14, "Responsável pelo ágape"),
        ]
        for cargo in cargos_padrao:
            cursor.execute("""
                INSERT INTO cargos (nome, sigla, grau_minimo, ordem, descricao, ativo)
                VALUES (?, ?, ?, ?, ?, 1)
            """, cargo)

    # Tipos de ausência padrão
    cursor.execute("SELECT COUNT(*) as total FROM tipos_ausencia")
    if cursor.fetchone()["total"] == 0:
        tipos_ausencia = [
            ("Justificada", "Ausência justificada por motivo pessoal", 0, "#28a745"),
            ("Injustificada", "Ausência sem justificativa", 0, "#dc3545"),
            ("Licença Saúde", "Licença médica ou problema de saúde", 1, "#ffc107"),
            ("Licença Profissional", "Compromisso profissional inadiável", 1, "#17a2b8"),
            ("Licença Particular", "Assuntos particulares", 1, "#6c757d"),
            ("Viagem", "Viagem a trabalho ou pessoal", 1, "#6610f2"),
            ("Luto", "Falecimento de familiar", 1, "#6c757d"),
            ("Doença na Família", "Acompanhamento familiar", 1, "#fd7e14")
        ]
        for tipo in tipos_ausencia:
            cursor.execute("""
                INSERT INTO tipos_ausencia (nome, descricao, requer_comprovante, cor)
                VALUES (?, ?, ?, ?)
            """, tipo)

    # Modelos de ata padrão
    cursor.execute("SELECT COUNT(*) as total FROM modelos_ata")
    if cursor.fetchone()["total"] == 0:
        modelos = [
            ("Ata Ordinária", "Modelo padrão para reuniões ordinárias", "Ordinária", 
             json.dumps({
                 "cabecalho": ["Abertura", "Verificacao de quorum", "Leitura da ata anterior"],
                 "expediente": ["Comunicacoes", "Propostas", "Informacoes"],
                 "ordem_do_dia": ["Assuntos em pauta", "Votacoes", "Deliberacoes"],
                 "encerramento": ["Palavra final", "Marcacao proxima reuniao", "Encerramento"]
             })),
            ("Ata Magna", "Modelo para reunioes magnas", "Magna",
             json.dumps({
                 "cabecalho": ["Abertura solene", "Composicao da mesa", "Hino"],
                 "expediente": ["Comunicacoes oficiais", "Propostas especiais"],
                 "ordem_do_dia": ["Rituais", "Iniciacoes", "Elevacoes", "Exaltacoes"],
                 "encerramento": ["Palavra do Veneravel", "Encerramento solene"]
             })),
            ("Ata Administrativa", "Modelo para reunioes administrativas", "Administrativa",
             json.dumps({
                 "cabecalho": ["Abertura", "Presentes", "Pauta"],
                 "expediente": ["Prestacao de contas", "Assuntos financeiros"],
                 "ordem_do_dia": ["Deliberacoes", "Votacoes", "Planejamento"],
                 "encerramento": ["Encaminhamentos", "Proxima reuniao", "Encerramento"]
             }))
        ]
        for modelo in modelos:
            cursor.execute("""
                INSERT INTO modelos_ata (nome, descricao, tipo, estrutura)
                VALUES (?, ?, ?, ?)
            """, modelo)

    # Admin padrão
    cursor.execute("SELECT * FROM usuarios WHERE usuario = 'admin'")
    if not cursor.fetchone():
        senha_hash = generate_password_hash("admin123")
        hoje = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO usuarios 
            (usuario, senha_hash, tipo, data_cadastro, ativo, nome_completo) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("admin", senha_hash, "admin", hoje, 1, "Administrador"))

    conn.commit()
    conn.close()
    print("✅ Banco de dados inicializado com sucesso!")

# Inicializar banco
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
        cursor.execute(
            "SELECT * FROM usuarios WHERE usuario = ? AND ativo = 1",
            (usuario,)
        )
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
    conn = get_db()
    cursor = conn.cursor()

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
                cursor.execute("SELECT sindicante FROM sindicancias WHERE candidato_id = ?", (c["id"],))
                enviados = [r["sindicante"] for r in cursor.fetchall()]
                faltam = [s["usuario"] for s in sindicantes if s["usuario"] not in enviados]
                if faltam:
                    pendentes.append({"candidato": dict(c), "faltam": faltam})

        # Prazo vencido
        prazo_vencido = []
        for c in candidatos:
            if not c["fechado"] and c["status"] == "Em análise" and c["data_criacao"]:
                try:
                    data_criacao = datetime.strptime(c["data_criacao"], "%Y-%m-%d %H:%M:%S")
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
            WHERE status = 'agendada' AND data >= date('now')
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
                "SELECT parecer FROM sindicancias WHERE candidato_id = ? AND sindicante = ?",
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

    conn.close()
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

# =============================
# ROTAS DE PERFIL
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

        # Buscar dados antigos
        cursor.execute("SELECT * FROM usuarios WHERE id = ?", (session["user_id"],))
        dados_antigos = dict(cursor.fetchone())

        cursor.execute("""
            UPDATE usuarios 
            SET nome_completo = ?, cim_numero = ?, loja_nome = ?, loja_numero = ?, loja_orient = ?
            WHERE id = ?
        """, (nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, session["user_id"]))

        conn.commit()

        session["nome_completo"] = nome_completo
        session["cim_numero"] = cim_numero
        session["loja_nome"] = loja_nome
        session["loja_numero"] = loja_numero
        session["loja_orient"] = loja_orient

        registrar_log("editar", "perfil", session["user_id"], 
                     dados_anteriores=dados_antigos,
                     dados_novos={"nome_completo": nome_completo, "loja_nome": loja_nome})

        flash("Perfil atualizado com sucesso!", "success")

    cursor.execute("SELECT * FROM usuarios WHERE id = ?", (session["user_id"],))
    usuario = cursor.fetchone()
    cursor.execute("SELECT * FROM lojas")
    lojas = cursor.fetchall()
    conn.close()

    return render_template("perfil.html", usuario=usuario, lojas=lojas)

# =============================
# ROTAS DE OBREIROS
# =============================
@app.route("/obreiros")
@login_required
def listar_obreiros():
    conn = get_db()
    cursor = conn.cursor()
    
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
        query += " AND (u.nome_completo LIKE ? OR u.usuario LIKE ?)"
        params.extend([f"%{nome}%", f"%{nome}%"])
    if grau:
        query += " AND u.grau_atual = ?"
        params.append(grau)
    if cargo:
        query += " AND EXISTS (SELECT 1 FROM ocupacao_cargos oc WHERE oc.obreiro_id = u.id AND oc.cargo_id = ? AND oc.ativo = 1)"
        params.append(cargo)
    if loja:
        query += " AND u.loja_nome = ?"
        params.append(loja)
    if status:
        query += " AND u.ativo = ?"
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

    conn.close()
    return render_template("obreiros/lista.html", 
                          obreiros=obreiros,
                          graus=graus,
                          cargos=cargos_list,
                          lojas=lojas,
                          filtros={'nome': nome, 'grau': grau, 'cargo': cargo, 'loja': loja, 'status': status})

@app.route("/obreiros/novo", methods=["GET", "POST"])
@admin_required
def novo_obreiro():
    conn = get_db()
    cursor = conn.cursor()

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

        if not usuario or not senha or not nome_completo:
            flash("Preencha os campos obrigatórios", "danger")
        else:
            try:
                senha_hash = generate_password_hash(senha)
                agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                cursor.execute("""
                    INSERT INTO usuarios 
                    (usuario, senha_hash, tipo, data_cadastro, ativo, 
                     nome_completo, nome_maconico, cim_numero, grau_atual,
                     data_iniciacao, data_elevacao, data_exaltacao,
                     telefone, email, endereco,
                     loja_nome, loja_numero, loja_orient) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (usuario, senha_hash, tipo, agora, 1,
                      nome_completo, nome_maconico, cim_numero, grau_atual,
                      data_iniciacao, data_elevacao, data_exaltacao,
                      telefone, email, endereco,
                      loja_nome, loja_numero, loja_orient))

                conn.commit()
                obreiro_id = cursor.lastrowid

                if data_iniciacao:
                    cursor.execute("""
                        INSERT INTO historico_graus (obreiro_id, grau, data, observacao)
                        VALUES (?, ?, ?, ?)
                    """, (obreiro_id, 1, data_iniciacao, "Iniciação"))

                conn.commit()
                
                registrar_log("criar", "obreiro", obreiro_id, 
                             dados_novos={"nome": nome_completo, "usuario": usuario})
                flash(f"Obreiro '{nome_completo}' adicionado com sucesso!", "success")
                return redirect("/obreiros")

            except sqlite3.IntegrityError:
                flash("Erro: Usuário ou CIM já existe", "danger")

    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    conn.close()
    return render_template("obreiros/novo.html", lojas=lojas)

@app.route("/obreiros/<int:id>")
@login_required
def visualizar_obreiro(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT u.*, l.nome as loja_nome_completo
        FROM usuarios u
        LEFT JOIN lojas l ON u.loja_nome = l.nome
        WHERE u.id = ?
    """, (id,))
    obreiro = cursor.fetchone()

    if not obreiro:
        flash("Obreiro não encontrado", "danger")
        return redirect("/obreiros")

    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para visualizar este obreiro", "danger")
        return redirect("/obreiros")

    cursor.execute("""
        SELECT oc.*, c.nome as cargo_nome, c.sigla
        FROM ocupacao_cargos oc
        JOIN cargos c ON oc.cargo_id = c.id
        WHERE oc.obreiro_id = ? AND oc.ativo = 1
        ORDER BY oc.data_inicio DESC
    """, (id,))
    cargos = cursor.fetchall()

    cursor.execute("""
        SELECT * FROM historico_graus
        WHERE obreiro_id = ?
        ORDER BY data DESC
    """, (id,))
    historico_graus = cursor.fetchall()

    cargos_disponiveis = []
    if session["tipo"] == "admin":
        cursor.execute("SELECT * FROM cargos WHERE ativo = 1 ORDER BY ordem")
        cargos_disponiveis = cursor.fetchall()

    conn.close()
    return render_template("obreiros/visualizar.html",
                          obreiro=obreiro,
                          cargos=cargos,
                          historico_graus=historico_graus,
                          cargos_disponiveis=cargos_disponiveis,
                          pode_editar=(session["tipo"] == "admin" or session["user_id"] == id))

@app.route("/obreiros/<int:id>/editar", methods=["GET", "POST"])
@login_required
def editar_obreiro(id):
    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para editar este obreiro", "danger")
        return redirect("/obreiros")

    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":
        nome_completo = request.form.get("nome_completo")
        nome_maconico = request.form.get("nome_maconico")
        cim_numero = request.form.get("cim_numero")
        telefone = request.form.get("telefone")
        email = request.form.get("email")
        endereco = request.form.get("endereco")
        loja_nome = request.form.get("loja_nome")
        loja_numero = request.form.get("loja_numero")
        loja_orient = request.form.get("loja_orient")

        # Buscar dados antigos
        cursor.execute("SELECT * FROM usuarios WHERE id = ?", (id,))
        dados_antigos = dict(cursor.fetchone())

        if session["tipo"] == "admin":
            tipo = request.form.get("tipo")
            grau_atual = request.form.get("grau_atual")
            data_iniciacao = request.form.get("data_iniciacao")
            data_elevacao = request.form.get("data_elevacao")
            data_exaltacao = request.form.get("data_exaltacao")
            ativo = request.form.get("ativo", 1)

            cursor.execute("""
                UPDATE usuarios 
                SET nome_completo = ?, nome_maconico = ?, cim_numero = ?, tipo = ?,
                    grau_atual = ?, data_iniciacao = ?, data_elevacao = ?, data_exaltacao = ?,
                    telefone = ?, email = ?, endereco = ?,
                    loja_nome = ?, loja_numero = ?, loja_orient = ?, ativo = ?
                WHERE id = ?
            """, (nome_completo, nome_maconico, cim_numero, tipo,
                  grau_atual, data_iniciacao, data_elevacao, data_exaltacao,
                  telefone, email, endereco,
                  loja_nome, loja_numero, loja_orient, ativo, id))
        else:
            cursor.execute("""
                UPDATE usuarios 
                SET nome_completo = ?, nome_maconico = ?, cim_numero = ?,
                    telefone = ?, email = ?, endereco = ?,
                    loja_nome = ?, loja_numero = ?, loja_orient = ?
                WHERE id = ?
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
        return redirect(f"/obreiros/{id}")

    cursor.execute("SELECT * FROM usuarios WHERE id = ?", (id,))
    obreiro = cursor.fetchone()
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    conn.close()

    return render_template("obreiros/editar.html",
                          obreiro=obreiro,
                          lojas=lojas,
                          is_admin=(session["tipo"] == "admin"),
                          is_own_profile=(session["user_id"] == id))

@app.route("/obreiros/<int:id>/cargo", methods=["POST"])
@admin_required
def atribuir_cargo(id):
    conn = get_db()
    cursor = conn.cursor()
    cargo_id = request.form.get("cargo_id")
    data_inicio = request.form.get("data_inicio")
    gestao = request.form.get("gestao")

    cursor.execute("""
        INSERT INTO ocupacao_cargos (obreiro_id, cargo_id, data_inicio, gestao, ativo)
        VALUES (?, ?, ?, ?, 1)
    """, (id, cargo_id, data_inicio, gestao))
    conn.commit()
    
    registrar_log("atribuir_cargo", "cargo", cargo_id, dados_novos={"obreiro_id": id, "cargo_id": cargo_id})
    conn.close()
    flash("Cargo atribuído com sucesso!", "success")
    return redirect(f"/obreiros/{id}")

@app.route("/obreiros/cargo/<int:id>/remover")
@admin_required
def remover_cargo(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT obreiro_id, cargo_id FROM ocupacao_cargos WHERE id = ?", (id,))
    cargo = cursor.fetchone()
    obreiro_id = cargo["obreiro_id"] if cargo else None
    cargo_id = cargo["cargo_id"] if cargo else None
    
    cursor.execute("UPDATE ocupacao_cargos SET ativo = 0 WHERE id = ?", (id,))
    conn.commit()
    
    registrar_log("remover_cargo", "cargo", cargo_id, dados_anteriores={"obreiro_id": obreiro_id})
    conn.close()
    flash("Cargo removido com sucesso!", "success")
    return redirect(f"/obreiros/{obreiro_id}")

@app.route("/obreiros/<int:id>/grau", methods=["POST"])
@admin_required
def registrar_grau(id):
    conn = get_db()
    cursor = conn.cursor()
    grau = request.form.get("grau")
    data = request.form.get("data")
    observacao = request.form.get("observacao")

    cursor.execute("""
        INSERT INTO historico_graus (obreiro_id, grau, data, observacao)
        VALUES (?, ?, ?, ?)
    """, (id, grau, data, observacao))
    cursor.execute("UPDATE usuarios SET grau_atual = ? WHERE id = ?", (grau, id))
    conn.commit()
    
    registrar_log("registrar_grau", "obreiro", id, dados_novos={"grau": grau, "data": data})
    conn.close()
    flash("Grau registrado com sucesso!", "success")
    return redirect(f"/obreiros/{id}")

# =============================
# ROTAS DE CARGOS
# =============================
@app.route("/cargos")
@admin_required
def listar_cargos():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cargos ORDER BY ordem")
    cargos = cursor.fetchall()
    conn.close()
    return render_template("cargos/lista.html", cargos=cargos)

@app.route("/cargos/novo", methods=["GET", "POST"])
@admin_required
def novo_cargo():
    if request.method == "POST":
        nome = request.form.get("nome")
        sigla = request.form.get("sigla")
        ordem = request.form.get("ordem")
        grau_minimo = request.form.get("grau_minimo")
        descricao = request.form.get("descricao")

        if not nome or not sigla or not ordem:
            flash("Preencha todos os campos obrigatórios", "danger")
        else:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO cargos (nome, sigla, ordem, grau_minimo, descricao, ativo)
                VALUES (?, ?, ?, ?, ?, 1)
            """, (nome, sigla, ordem, grau_minimo, descricao))
            conn.commit()
            cargo_id = cursor.lastrowid
            registrar_log("criar", "cargo", cargo_id, dados_novos={"nome": nome, "sigla": sigla})
            conn.close()
            flash(f"Cargo '{nome}' adicionado com sucesso!", "success")
            return redirect("/cargos")
    return render_template("cargos/novo.html")

@app.route("/cargos/editar/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_cargo(id):
    conn = get_db()
    cursor = conn.cursor()
    
    if request.method == "POST":
        nome = request.form.get("nome")
        sigla = request.form.get("sigla")
        ordem = request.form.get("ordem")
        grau_minimo = request.form.get("grau_minimo")
        descricao = request.form.get("descricao")
        ativo = request.form.get("ativo", 1)

        cursor.execute("SELECT * FROM cargos WHERE id = ?", (id,))
        dados_antigos = dict(cursor.fetchone())
        
        cursor.execute("""
            UPDATE cargos 
            SET nome = ?, sigla = ?, ordem = ?, grau_minimo = ?, descricao = ?, ativo = ?
            WHERE id = ?
        """, (nome, sigla, ordem, grau_minimo, descricao, ativo, id))
        conn.commit()
        
        registrar_log("editar", "cargo", id, dados_anteriores=dados_antigos,
                     dados_novos={"nome": nome, "sigla": sigla})
        flash("Cargo atualizado com sucesso!", "success")
        return redirect("/cargos")

    cursor.execute("SELECT * FROM cargos WHERE id = ?", (id,))
    cargo = cursor.fetchone()
    conn.close()
    if not cargo:
        flash("Cargo não encontrado", "danger")
        return redirect("/cargos")
    return render_template("cargos/editar.html", cargo=cargo)

@app.route("/cargos/excluir/<int:id>")
@admin_required
def excluir_cargo(id):
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM cargos WHERE id = ?", (id,))
    dados = dict(cursor.fetchone()) if cursor.fetchone() else None
    
    cursor.execute("SELECT COUNT(*) as total FROM ocupacao_cargos WHERE cargo_id = ?", (id,))
    resultado = cursor.fetchone()
    if resultado and resultado["total"] > 0:
        flash("Não é possível excluir este cargo pois existem obreiros ocupando-o.", "danger")
    else:
        cursor.execute("DELETE FROM cargos WHERE id = ?", (id,))
        conn.commit()
        registrar_log("excluir", "cargo", id, dados_anteriores=dados)
        flash("Cargo excluído com sucesso!", "success")
    
    conn.close()
    return redirect("/cargos")

# =============================
# ROTAS DE REUNIÕES
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
        query += " AND r.data >= ?"
        params.append(data_ini)
    if data_fim:
        query += " AND r.data <= ?"
        params.append(data_fim)
    if tipo:
        query += " AND r.tipo = ?"
        params.append(tipo)
    if status:
        query += " AND r.status = ?"
        params.append(status)
    if grau:
        query += " AND r.grau = ?"
        params.append(grau)
    if local:
        query += " AND r.local LIKE ?"
        params.append(f"%{local}%")

    query += " GROUP BY r.id ORDER BY r.data DESC, r.hora_inicio DESC"

    cursor.execute(query, params)
    reunioes = cursor.fetchall()

    cursor.execute("SELECT DISTINCT tipo FROM reunioes ORDER BY tipo")
    tipos = cursor.fetchall()
    cursor.execute("SELECT DISTINCT status FROM reunioes ORDER BY status")
    status_list = cursor.fetchall()
    cursor.execute("SELECT DISTINCT grau FROM reunioes WHERE grau IS NOT NULL ORDER BY grau")
    graus = cursor.fetchall()

    conn.close()
    return render_template("reunioes/lista.html", 
                          reunioes=reunioes,
                          tipos=tipos,
                          status_list=status_list,
                          graus=graus,
                          filtros={'data_ini': data_ini, 'data_fim': data_fim, 'tipo': tipo, 'status': status, 'grau': grau, 'local': local})

@app.route("/reunioes/calendario")
@login_required
def calendario_reunioes():
    return render_template("reunioes/calendario.html")

@app.route("/api/reunioes")
@login_required
def api_reunioes():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT r.*, t.cor, t.nome as tipo_nome,
               COUNT(p.id) as total_obreiros,
               SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presentes
        FROM reunioes r
        LEFT JOIN tipos_reuniao t ON r.tipo = t.nome
        LEFT JOIN presenca p ON r.id = p.reuniao_id
        GROUP BY r.id
        ORDER BY r.data
    """)
    rows = cursor.fetchall()
    conn.close()

    eventos = []
    for row in rows:
        r = dict(row)
        start = f"{r['data']}T{r['hora_inicio']}" if r.get('hora_inicio') else r['data']
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
    conn = get_db()
    cursor = conn.cursor()
    
    if request.method == "POST":
        titulo = request.form.get("titulo")
        tipo = request.form.get("tipo")
        grau = request.form.get("grau")
        data = request.form.get("data")
        hora_inicio = request.form.get("hora_inicio")
        hora_termino = request.form.get("hora_termino")
        local = request.form.get("local")
        loja_id = request.form.get("loja_id")
        pauta = request.form.get("pauta")
        observacoes = request.form.get("observacoes")

        cursor.execute("""
            INSERT INTO reunioes 
            (titulo, tipo, grau, data, hora_inicio, hora_termino, local, loja_id, pauta, observacoes, criado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (titulo, tipo, grau, data, hora_inicio, hora_termino, local, loja_id, pauta, observacoes, session["user_id"]))
        conn.commit()
        reuniao_id = cursor.lastrowid
        
        registrar_log("criar", "reuniao", reuniao_id, dados_novos={"titulo": titulo, "data": data, "tipo": tipo})
        flash("Reunião agendada com sucesso!", "success")
        return redirect(f"/reunioes/{reuniao_id}")

    cursor.execute("SELECT * FROM tipos_reuniao ORDER BY nome")
    tipos = cursor.fetchall()
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    conn.close()
    hoje = datetime.now().strftime("%Y-%m-%d")
    return render_template("reunioes/nova.html", tipos=tipos, lojas=lojas, hoje=hoje)

@app.route("/reunioes/<int:id>")
@login_required
def detalhes_reuniao(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT r.*, l.nome as loja_nome, t.cor as tipo_cor,
               u.nome_completo as criado_por_nome
        FROM reunioes r
        LEFT JOIN lojas l ON r.loja_id = l.id
        LEFT JOIN tipos_reuniao t ON r.tipo = t.nome
        LEFT JOIN usuarios u ON r.criado_por = u.id
        WHERE r.id = ?
    """, (id,))
    reuniao = cursor.fetchone()
    if not reuniao:
        flash("Reunião não encontrada", "danger")
        return redirect("/reunioes")

    cursor.execute("SELECT id, aprovada, numero_ata, ano_ata FROM atas WHERE reuniao_id = ?", (id,))
    ata_row = cursor.fetchone()
    ata_id = ata_row["id"] if ata_row else None
    ata_aprovada = ata_row["aprovada"] if ata_row else None
    ata_numero = ata_row["numero_ata"] if ata_row else None
    ata_ano = ata_row["ano_ata"] if ata_row else None

    cursor.execute("""
        SELECT u.id, u.nome_completo, u.grau_atual, 
               p.id as presenca_id, p.presente, p.tipo_ausencia, 
               p.justificativa, p.validado_por, p.data_registro,
               p.comprovante
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
    return render_template("reunioes/detalhes.html",
                          reuniao=reuniao,
                          presenca=presenca,
                          total_obreiros=total_obreiros,
                          presentes=presentes,
                          ausentes=ausentes,
                          ata_id=ata_id,
                          ata_aprovada=ata_aprovada,
                          ata_numero=ata_numero,
                          ata_ano=ata_ano)

@app.route("/reunioes/<int:id>/presenca", methods=["POST"])
@admin_required
def registrar_presenca(id):
    conn = get_db()
    cursor = conn.cursor()
    obreiro_id = request.form.get("obreiro_id")
    presente = request.form.get("presente", 0)
    justificativa = request.form.get("justificativa", "")
    tipo_ausencia = request.form.get("tipo_ausencia", None)

    cursor.execute("""
        INSERT INTO presenca (reuniao_id, obreiro_id, presente, justificativa, registrado_por, tipo_ausencia)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(reuniao_id, obreiro_id) 
        DO UPDATE SET presente = ?, justificativa = ?, registrado_por = ?, tipo_ausencia = ?, data_registro = CURRENT_TIMESTAMP
    """, (id, obreiro_id, presente, justificativa, session["user_id"], tipo_ausencia,
          presente, justificativa, session["user_id"], tipo_ausencia))
    conn.commit()
    
    registrar_log("registrar_presenca", "presenca", id, 
                 dados_novos={"obreiro_id": obreiro_id, "presente": presente})
    conn.close()
    flash("Presença registrada com sucesso!", "success")
    return redirect(f"/reunioes/{id}")

@app.route("/reunioes/<int:id>/ata", methods=["GET", "POST"])
@admin_required
def redigir_ata(id):
    conn = get_db()
    cursor = conn.cursor()
    
    if request.method == "POST":
        conteudo = request.form.get("conteudo")
        aprovada = request.form.get("aprovada", 0)
        cursor.execute("""
            INSERT INTO atas (reuniao_id, conteudo, redator_id, aprovada, data_aprovacao)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(reuniao_id) 
            DO UPDATE SET conteudo = ?, redator_id = ?, aprovada = ?, 
                          data_aprovacao = ?, versao = versao + 1
        """, (id, conteudo, session["user_id"], aprovada,
              datetime.now().date() if aprovada else None,
              conteudo, session["user_id"], aprovada,
              datetime.now().date() if aprovada else None))
        conn.commit()
        ata_id = cursor.lastrowid if cursor.lastrowid else id
        registrar_log("criar" if not cursor.lastrowid else "editar", "ata", ata_id)
        flash("Ata salva com sucesso!", "success")
        return redirect(f"/reunioes/{id}")

    cursor.execute("SELECT * FROM reunioes WHERE id = ?", (id,))
    reuniao = cursor.fetchone()
    cursor.execute("SELECT * FROM atas WHERE reuniao_id = ?", (id,))
    ata = cursor.fetchone()
    conn.close()
    return render_template("reunioes/ata.html", reuniao=reuniao, ata=ata)

@app.route("/reunioes/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_reuniao(id):
    conn = get_db()
    cursor = conn.cursor()
    
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

        cursor.execute("SELECT * FROM reunioes WHERE id = ?", (id,))
        dados_antigos = dict(cursor.fetchone())
        
        cursor.execute("""
            UPDATE reunioes 
            SET titulo = ?, tipo = ?, grau = ?, data = ?, hora_inicio = ?, 
                hora_termino = ?, local = ?, pauta = ?, observacoes = ?, status = ?
            WHERE id = ?
        """, (titulo, tipo, grau, data, hora_inicio, hora_termino, local, pauta, observacoes, status, id))
        conn.commit()
        
        registrar_log("editar", "reuniao", id, dados_anteriores=dados_antigos,
                     dados_novos={"titulo": titulo, "data": data, "status": status})
        flash("Reunião atualizada com sucesso!", "success")
        return redirect(f"/reunioes/{id}")

    cursor.execute("SELECT * FROM reunioes WHERE id = ?", (id,))
    reuniao = cursor.fetchone()
    cursor.execute("SELECT * FROM tipos_reuniao ORDER BY nome")
    tipos = cursor.fetchall()
    conn.close()
    return render_template("reunioes/editar.html", reuniao=reuniao, tipos=tipos)

@app.route("/reunioes/<int:id>/excluir")
@admin_required
def excluir_reuniao(id):
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM reunioes WHERE id = ?", (id,))
    dados = dict(cursor.fetchone()) if cursor.fetchone() else None
    
    cursor.execute("SELECT id FROM atas WHERE reuniao_id = ?", (id,))
    if cursor.fetchone():
        flash("Não é possível excluir uma reunião que já possui ata.", "danger")
    else:
        cursor.execute("DELETE FROM reunioes WHERE id = ?", (id,))
        conn.commit()
        registrar_log("excluir", "reuniao", id, dados_anteriores=dados)
        flash("Reunião excluída com sucesso!", "success")
    
    conn.close()
    return redirect("/reunioes")

# =============================
# ROTAS DE PRESENÇA E ESTATÍSTICAS
# =============================
@app.route("/presenca/estatisticas")
@login_required
def estatisticas_presenca():
    conn = get_db()
    cursor = conn.cursor()
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
        LEFT JOIN reunioes r ON p.reuniao_id = r.id AND strftime('%Y', r.data) = ?
        WHERE u.ativo = 1
        GROUP BY u.id
        ORDER BY u.grau_atual DESC, u.nome_completo
    """, (str(ano),))
    rows = cursor.fetchall()
    estatisticas = [dict(row) for row in rows]

    cursor.execute("""
        SELECT 
            strftime('%m', r.data) as mes,
            COUNT(*) as total_reunioes,
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

        cursor.execute("""
            UPDATE presenca 
            SET tipo_ausencia = ?, justificativa = ?, 
                data_registro = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (tipo_ausencia, justificativa, id))
        conn.commit()

        cursor.execute("SELECT reuniao_id FROM presenca WHERE id = ?", (id,))
        presenca = cursor.fetchone()
        reuniao_id = presenca["reuniao_id"] if presenca else None
        
        registrar_log("justificar_ausencia", "presenca", id, 
                     dados_novos={"tipo_ausencia": tipo_ausencia})
        conn.close()
        flash("Ausência justificada com sucesso!", "success")
        return redirect(f"/reunioes/{reuniao_id}")

    cursor.execute("""
        SELECT p.*, r.titulo, r.data as reuniao_data, r.hora_inicio,
               u.nome_completo, u.id as obreiro_id
        FROM presenca p
        JOIN reunioes r ON p.reuniao_id = r.id
        JOIN usuarios u ON p.obreiro_id = u.id
        WHERE p.id = ?
    """, (id,))
    presenca = cursor.fetchone()
    if not presenca:
        flash("Registro de presença não encontrado", "danger")
        return redirect("/reunioes")

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
        cursor.execute("""
            UPDATE presenca 
            SET validado_por = ?, data_validacao = CURRENT_TIMESTAMP,
                observacao_validacao = ?
            WHERE id = ?
        """, (session["user_id"], observacao, id))
        registrar_log("validar_ausencia", "presenca", id, dados_novos={"validado": True})
        flash("Ausência validada com sucesso!", "success")
    else:
        cursor.execute("""
            UPDATE presenca 
            SET tipo_ausencia = NULL, justificativa = NULL,
                validado_por = NULL, data_validacao = NULL,
                observacao_validacao = ?
            WHERE id = ?
        """, (observacao, id))
        registrar_log("rejeitar_ausencia", "presenca", id)
        flash("Validação removida!", "success")

    conn.commit()
    conn.close()
    return redirect(request.referrer or "/reunioes")

@app.route("/presenca/alertas")
@admin_required
def listar_alertas():
    conn = get_db()
    cursor = conn.cursor()
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
    conn.close()
    return render_template("presenca/alertas.html", alertas=alertas)

@app.route("/presenca/alerta/<int:id>/resolver", methods=["POST"])
@admin_required
def resolver_alerta(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE alertas_presenca 
        SET resolvido = 1, data_resolucao = CURRENT_TIMESTAMP,
            resolvido_por = ?
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

    cursor.execute("""
        SELECT 
            p.obreiro_id,
            u.nome_completo,
            COUNT(*) as ausencias,
            ? as mes
        FROM presenca p
        JOIN usuarios u ON p.obreiro_id = u.id
        JOIN reunioes r ON p.reuniao_id = r.id
        WHERE p.presente = 0 
          AND p.tipo_ausencia IS NULL
          AND r.status = 'realizada'
          AND strftime('%Y-%m', r.data) = ?
        GROUP BY p.obreiro_id
        HAVING COUNT(*) >= 3
    """, (mes_atual, mes_atual))
    alertas_ausencias = cursor.fetchall()

    for a in alertas_ausencias:
        cursor.execute("""
            INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
            VALUES (?, ?, ?)
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
        LEFT JOIN reunioes r ON p.reuniao_id = r.id AND strftime('%Y', r.data) = ?
        WHERE u.ativo = 1
        GROUP BY u.id
        HAVING total_reunioes > 0
    """, (str(ano_atual),))
    estatisticas = cursor.fetchall()

    for e in estatisticas:
        total = e["total_reunioes"]
        presencas = e["presencas"] or 0
        percentual = (presencas / total) * 100

        if percentual < 50:
            cursor.execute("""
                INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                VALUES (?, ?, ?)
            """, (e["id"], "presenca_critica",
                   f"{e['nome_completo']} tem apenas {percentual:.1f}% de presença no ano {ano_atual} (CRÍTICO)"))
        elif percentual < 75:
            cursor.execute("""
                INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                VALUES (?, ?, ?)
            """, (e["id"], "presenca_atencao",
                   f"{e['nome_completo']} tem {percentual:.1f}% de presença no ano {ano_atual} (ATENÇÃO)"))

    conn.commit()
    registrar_log("gerar_alertas", "alertas", None, dados_novos={"quantidade": len(alertas_ausencias)})
    conn.close()

    flash(f"Alertas gerados! ({len(alertas_ausencias)} por ausências + alertas de presença)", "success")
    return redirect("/presenca/alertas")

# =============================
# ROTAS DE ATAS
# =============================
@app.route("/atas")
@login_required
def listar_atas():
    conn = get_db()
    cursor = conn.cursor()
    
    data_ini = request.args.get('data_ini', '')
    data_fim = request.args.get('data_fim', '')
    aprovada = request.args.get('aprovada', '')
    reuniao_titulo = request.args.get('reuniao_titulo', '')

    query = """
        SELECT a.*, 
               r.titulo as reuniao_titulo,
               r.data as reuniao_data,
               u.nome_completo as redator_nome,
               (SELECT COUNT(*) FROM assinaturas_ata WHERE ata_id = a.id) as total_assinaturas
        FROM atas a
        JOIN reunioes r ON a.reuniao_id = r.id
        LEFT JOIN usuarios u ON a.redator_id = u.id
        WHERE 1=1
    """
    params = []

    if data_ini:
        query += " AND r.data >= ?"
        params.append(data_ini)
    if data_fim:
        query += " AND r.data <= ?"
        params.append(data_fim)
    if aprovada != '':
        query += " AND a.aprovada = ?"
        params.append(aprovada)
    if reuniao_titulo:
        query += " AND r.titulo LIKE ?"
        params.append(f"%{reuniao_titulo}%")

    query += " ORDER BY a.data_criacao DESC"

    cursor.execute(query, params)
    atas = cursor.fetchall()
    conn.close()
    return render_template("atas/lista.html", atas=atas, filtros={'data_ini': data_ini, 'data_fim': data_fim, 'aprovada': aprovada, 'reuniao_titulo': reuniao_titulo})

@app.route("/atas/nova/<int:reuniao_id>", methods=["GET", "POST"])
@admin_required
def nova_ata(reuniao_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT r.*, 
               (SELECT COUNT(*) FROM presenca WHERE reuniao_id = r.id AND presente = 1) as presentes
        FROM reunioes r
        WHERE r.id = ?
    """, (reuniao_id,))
    reuniao = cursor.fetchone()
    if not reuniao:
        flash("Reunião não encontrada", "danger")
        return redirect("/reunioes")

    if request.method == "POST":
        conteudo = request.form.get("conteudo")
        modelo_id = request.form.get("modelo_id")
        ano = datetime.now().year
        cursor.execute("SELECT COUNT(*) as total FROM atas WHERE ano_ata = ?", (ano,))
        total = cursor.fetchone()["total"]
        numero_ata = total + 1
        cursor.execute("""
            INSERT INTO atas 
            (reuniao_id, conteudo, redator_id, modelo_id, numero_ata, ano_ata, tipo_ata, data_criacao)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (reuniao_id, conteudo, session["user_id"], modelo_id, numero_ata, ano, reuniao["tipo"]))
        ata_id = cursor.lastrowid
        conn.commit()
        
        registrar_log("criar", "ata", ata_id, dados_novos={"reuniao_id": reuniao_id, "numero": numero_ata, "ano": ano})
        flash(f"Ata nº {numero_ata}/{ano} criada com sucesso!", "success")
        return redirect(f"/atas/{ata_id}")

    cursor.execute("SELECT * FROM modelos_ata WHERE ativo = 1")
    modelos = cursor.fetchall()
    conn.close()
    return render_template("atas/nova.html", reuniao=reuniao, modelos=modelos)

@app.route("/atas/<int:id>")
@login_required
def visualizar_ata(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT a.*, 
               r.titulo as reuniao_titulo,
               r.data as reuniao_data,
               r.hora_inicio,
               r.hora_termino,
               r.local,
               r.pauta,
               u.nome_completo as redator_nome,
               u2.nome_completo as aprovado_por_nome
        FROM atas a
        JOIN reunioes r ON a.reuniao_id = r.id
        LEFT JOIN usuarios u ON a.redator_id = u.id
        LEFT JOIN usuarios u2 ON a.aprovada_por = u2.id
        WHERE a.id = ?
    """, (id,))
    ata = cursor.fetchone()
    if not ata:
        flash("Ata não encontrada", "danger")
        return redirect("/atas")

    cursor.execute("""
        SELECT u.nome_completo, u.grau_atual,
               p.presente, p.tipo_ausencia,
               c.nome as cargo_nome
        FROM presenca p
        JOIN usuarios u ON p.obreiro_id = u.id
        LEFT JOIN ocupacao_cargos oc ON u.id = oc.obreiro_id AND oc.ativo = 1
        LEFT JOIN cargos c ON oc.cargo_id = c.id
        WHERE p.reuniao_id = ?
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
        WHERE ass.ata_id = ?
        ORDER BY ass.data_assinatura
    """, (id,))
    assinaturas = cursor.fetchall()
    conn.close()
    return render_template("atas/visualizar.html",
                          ata=ata,
                          presenca=presenca,
                          assinaturas=assinaturas)

@app.route("/atas/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_ata(id):
    conn = get_db()
    cursor = conn.cursor()
    
    if request.method == "POST":
        conteudo = request.form.get("conteudo")
        cursor.execute("""
            UPDATE atas 
            SET conteudo = ?, versao = versao + 1
            WHERE id = ?
        """, (conteudo, id))
        conn.commit()
        registrar_log("editar", "ata", id)
        flash("Ata atualizada com sucesso!", "success")
        return redirect(f"/atas/{id}")

    cursor.execute("SELECT * FROM atas WHERE id = ?", (id,))
    ata = cursor.fetchone()
    cursor.execute("SELECT * FROM modelos_ata WHERE ativo = 1")
    modelos = cursor.fetchall()
    conn.close()
    return render_template("atas/editar.html", ata=ata, modelos=modelos)

@app.route("/atas/<int:id>/aprovar", methods=["POST"])
@admin_required
def aprovar_ata(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE atas 
        SET aprovada = 1, 
            aprovada_em = CURRENT_DATE,
            aprovada_por = ?
        WHERE id = ?
    """, (session["user_id"], id))
    conn.commit()
    registrar_log("aprovar", "ata", id, dados_novos={"aprovada": 1})
    flash("Ata aprovada com sucesso!", "success")
    return redirect(f"/atas/{id}")

@app.route("/atas/<int:id>/assinar", methods=["POST"])
@login_required
def assinar_ata(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM assinaturas_ata WHERE ata_id = ? AND obreiro_id = ?", (id, session["user_id"]))
    if cursor.fetchone():
        flash("Você já assinou esta ata", "warning")
    else:
        cursor.execute("""
            SELECT cargo_id FROM ocupacao_cargos 
            WHERE obreiro_id = ? AND ativo = 1
            ORDER BY data_inicio DESC LIMIT 1
        """, (session["user_id"],))
        cargo = cursor.fetchone()
        cursor.execute("""
            INSERT INTO assinaturas_ata (ata_id, obreiro_id, cargo_id, ip_assinatura)
            VALUES (?, ?, ?, ?)
        """, (id, session["user_id"], cargo["cargo_id"] if cargo else None, request.remote_addr))
        conn.commit()
        registrar_log("assinar", "ata", id)
        flash("Ata assinada com sucesso!", "success")
    conn.close()
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
        from flask import send_file

        conn = get_db()
        cursor = conn.cursor()
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
            WHERE a.id = ?
        """, (id,))
        ata = cursor.fetchone()
        if not ata:
            flash("Ata não encontrada", "danger")
            return redirect("/atas")

        cursor.execute("""
            SELECT u.nome_completo, 
                   CASE WHEN p.presente = 1 THEN 'Presente' ELSE 'Ausente' END as status
            FROM presenca p
            JOIN usuarios u ON p.obreiro_id = u.id
            WHERE p.reuniao_id = ?
            ORDER BY u.nome_completo
        """, (ata["reuniao_id"],))
        presenca = cursor.fetchall()

        cursor.execute("""
            SELECT u.nome_completo, c.nome as cargo
            FROM assinaturas_ata ass
            JOIN usuarios u ON ass.obreiro_id = u.id
            LEFT JOIN cargos c ON ass.cargo_id = c.id
            WHERE ass.ata_id = ?
        """, (id,))
        assinaturas = cursor.fetchall()
        conn.close()

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
            ["Data de Criação:", ata["data_criacao"][:16]]
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
    conn = get_db()
    cursor = conn.cursor()
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
    conn.close()
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
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO modelos_ata (nome, descricao, tipo, estrutura, created_by)
                VALUES (?, ?, ?, ?, ?)
            """, (nome, descricao, tipo, estrutura, session["user_id"]))
            conn.commit()
            modelo_id = cursor.lastrowid
            registrar_log("criar", "modelo_ata", modelo_id, dados_novos={"nome": nome})
            flash("Modelo criado com sucesso!", "success")
            return redirect("/atas/modelos")
    return render_template("atas/modelo_form.html")

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
        query += " AND c.tipo = ?"
        params.append(tipo)
    if prioridade:
        query += " AND c.prioridade = ?"
        params.append(prioridade)
    if data_ini:
        query += " AND c.data_inicio >= ?"
        params.append(data_ini)
    if data_fim:
        query += " AND c.data_fim <= ?"
        params.append(data_fim)
    if ativo != '':
        query += " AND c.ativo = ?"
        params.append(ativo)
    else:
        query += " AND c.ativo = 1 AND c.data_inicio <= ? AND (c.data_fim IS NULL OR c.data_fim >= ?)"
        params.extend([hoje, hoje])

    query += " ORDER BY c.prioridade = 'urgente' DESC, c.data_criacao DESC"

    cursor.execute(query, params)
    comunicados = cursor.fetchall()

    cursor.execute("SELECT DISTINCT tipo FROM comunicados ORDER BY tipo")
    tipos = cursor.fetchall()
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
        cursor.execute("""
            INSERT INTO comunicados (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, criado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, session["user_id"]))
        conn.commit()
        comunicado_id = cursor.lastrowid
        
        registrar_log("criar", "comunicado", comunicado_id, dados_novos={"titulo": titulo, "prioridade": prioridade})
        flash("Comunicado publicado com sucesso!", "success")
        return redirect("/comunicados")
    hoje = datetime.now().strftime("%Y-%m-%d")
    return render_template("comunicados/novo.html", hoje=hoje)

@app.route("/comunicados/<int:id>/visualizar")
@login_required
def visualizar_comunicado(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO visualizacoes_comunicado (comunicado_id, obreiro_id)
        VALUES (?, ?)
    """, (id, session["user_id"]))
    conn.commit()
    cursor.execute("""
        SELECT c.*, u.nome_completo as autor_nome
        FROM comunicados c
        JOIN usuarios u ON c.criado_por = u.id
        WHERE c.id = ?
    """, (id,))
    comunicado = cursor.fetchone()
    conn.close()
    if not comunicado:
        flash("Comunicado não encontrado", "danger")
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
        
        cursor.execute("SELECT * FROM comunicados WHERE id = ?", (id,))
        dados_antigos = dict(cursor.fetchone())
        
        cursor.execute("""
            UPDATE comunicados
            SET titulo=?, conteudo=?, tipo=?, prioridade=?, data_inicio=?, data_fim=?, ativo=?
            WHERE id=?
        """, (titulo, conteudo, tipo, prioridade, data_inicio, data_fim, ativo, id))
        conn.commit()
        
        registrar_log("editar", "comunicado", id, dados_anteriores=dados_antigos,
                     dados_novos={"titulo": titulo, "prioridade": prioridade})
        flash("Comunicado atualizado com sucesso!", "success")
        return redirect("/comunicados")
    
    cursor.execute("SELECT * FROM comunicados WHERE id = ?", (id,))
    comunicado = cursor.fetchone()
    conn.close()
    return render_template("comunicados/editar.html", comunicado=comunicado)

@app.route("/comunicados/<int:id>/excluir")
@admin_required
def excluir_comunicado(id):
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM comunicados WHERE id = ?", (id,))
    dados = dict(cursor.fetchone()) if cursor.fetchone() else None
    
    cursor.execute("DELETE FROM comunicados WHERE id = ?", (id,))
    conn.commit()
    
    registrar_log("excluir", "comunicado", id, dados_anteriores=dados)
    flash("Comunicado excluído com sucesso!", "success")
    conn.close()
    return redirect("/comunicados")

# =============================
# ROTAS DE CANDIDATOS E SINDICÂNCIA
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
            cursor.execute("INSERT INTO candidatos (nome, data_criacao) VALUES (?, ?)", (nome, agora))
            conn.commit()
            candidato_id = cursor.lastrowid
            registrar_log("criar", "candidato", candidato_id, dados_novos={"nome": nome})
            flash(f"Candidato '{nome}' adicionado com sucesso!", "success")
        else:
            flash("Nome do candidato não pode estar vazio", "danger")
    cursor.execute("SELECT * FROM candidatos ORDER BY data_criacao DESC")
    candidatos = cursor.fetchall()
    conn.close()
    return render_template("candidatos.html", candidatos=candidatos, tipo=session["tipo"])

@app.route("/excluir_candidato/<int:id>")
@admin_required
def excluir_candidato(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM candidatos WHERE id = ?", (id,))
    dados = dict(cursor.fetchone()) if cursor.fetchone() else None
    cursor.execute("DELETE FROM candidatos WHERE id = ?", (id,))
    conn.commit()
    registrar_log("excluir", "candidato", id, dados_anteriores=dados)
    conn.close()
    flash("Candidato excluído com sucesso!", "success")
    return redirect("/candidatos")

@app.route("/sindicancia/<int:id>", methods=["GET", "POST"])
@login_required
def visualizar_sindicancia(id):
    if session["tipo"] == "admin":
        flash("Administradores não podem emitir pareceres", "warning")
        return redirect("/candidatos")

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM candidatos WHERE id = ?", (id,))
    candidato = cursor.fetchone()
    if not candidato:
        flash("Candidato não encontrado", "danger")
        return redirect("/minhas_sindicancias")

    bloqueado = candidato["fechado"] == 1
    usuario = session["usuario"]

    if request.method == "POST" and not bloqueado:
        parecer = request.form["parecer"]
        agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO sindicancias (candidato_id, sindicante, parecer, data_envio)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(candidato_id, sindicante) 
            DO UPDATE SET parecer = ?, data_envio = ?
        """, (id, usuario, parecer, agora, parecer, agora))
        conn.commit()
        registrar_log("emitir_parecer", "sindicancia", id, dados_novos={"parecer": parecer})
        flash("Parecer enviado com sucesso!", "success")
        
        cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo = 'sindicante' AND ativo = 1")
        total_sindicantes = cursor.fetchone()["total"]
        cursor.execute("SELECT COUNT(*) as votos FROM sindicancias WHERE candidato_id = ?", (id,))
        votos = cursor.fetchone()["votos"]
        if votos >= total_sindicantes and total_sindicantes > 0:
            cursor.execute("""
                SELECT 
                    SUM(CASE WHEN parecer = 'positivo' THEN 1 ELSE 0 END) as positivos,
                    SUM(CASE WHEN parecer = 'negativo' THEN 1 ELSE 0 END) as negativos
                FROM sindicancias WHERE candidato_id = ?
            """, (id,))
            res = cursor.fetchone()
            status = "Aprovado" if res["positivos"] > res["negativos"] else "Reprovado"
            agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("""
                UPDATE candidatos 
                SET status = ?, fechado = 1, data_fechamento = ?, resultado_final = ?
                WHERE id = ?
            """, (status, agora, f"{res['positivos']} votos positivos, {res['negativos']} negativos", id))
            conn.commit()
            registrar_log("fechar_sindicancia", "sindicancia", id, dados_novos={"status": status})

    cursor.execute("""
        SELECT s.*, u.usuario, u.nome_completo
        FROM sindicancias s
        JOIN usuarios u ON s.sindicante = u.usuario
        WHERE s.candidato_id = ?
        ORDER BY s.data_envio DESC
    """, (id,))
    registros = cursor.fetchall()

    cursor.execute("""
        SELECT * FROM sindicancias 
        WHERE candidato_id = ? AND sindicante = ?
    """, (id, usuario))
    meu_parecer = cursor.fetchone()

    cursor.execute("""
        SELECT id FROM pareceres_conclusivos 
        WHERE candidato_id = ? AND sindicante = ?
    """, (id, usuario))
    parecer_conclusivo_existente = cursor.fetchone()

    conn.close()

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
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT fechado FROM candidatos WHERE id = ?", (candidato_id,))
    candidato = cursor.fetchone()
    if candidato and candidato["fechado"] == 0:
        cursor.execute("DELETE FROM sindicancias WHERE candidato_id = ? AND sindicante = ?", (candidato_id, session["usuario"]))
        conn.commit()
        registrar_log("excluir_parecer", "sindicancia", candidato_id)
        flash("Parecer excluído com sucesso!", "success")
    else:
        flash("Não é possível excluir parecer de uma sindicância fechada", "danger")
    conn.close()
    return redirect(f"/sindicancia/{candidato_id}")

@app.route("/fechar_sindicancia/<int:id>")
@admin_required
def fechar_sindicancia_manual(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT parecer FROM sindicancias WHERE candidato_id = ?", (id,))
    pareceres = cursor.fetchall()
    if not pareceres:
        flash("Não é possível fechar: nenhum parecer enviado", "warning")
        return redirect("/candidatos")
    
    cursor.execute("SELECT * FROM candidatos WHERE id = ?", (id,))
    dados_antigos = dict(cursor.fetchone())
    
    positivos = sum(1 for p in pareceres if p["parecer"] == "positivo")
    negativos = len(pareceres) - positivos
    status = "Aprovado" if positivos > negativos else "Reprovado"
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    cursor.execute("""
        UPDATE candidatos 
        SET status = ?, fechado = 1, data_fechamento = ?, resultado_final = ?
        WHERE id = ?
    """, (status, agora, f"{positivos} votos positivos, {negativos} negativos", id))
    conn.commit()
    
    registrar_log("fechar_sindicancia_manual", "sindicancia", id, 
                 dados_anteriores=dados_antigos,
                 dados_novos={"status": status})
    conn.close()
    flash(f"Sindicância fechada! Resultado: {status}", "success")
    return redirect("/candidatos")

@app.route("/minhas_sindicancias")
@sindicante_required
def minhas_sindicancias():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.*, 
               CASE WHEN s.parecer IS NOT NULL THEN 1 ELSE 0 END as parecer_enviado,
               s.parecer,
               s.data_envio
        FROM candidatos c
        LEFT JOIN sindicancias s ON c.id = s.candidato_id AND s.sindicante = ?
        ORDER BY c.fechado ASC, c.data_criacao DESC
    """, (session["usuario"],))
    candidatos = cursor.fetchall()
    conn.close()
    return render_template("minhas_sindicancias.html", candidatos=candidatos)

@app.route("/parecer_conclusivo/<int:id>", methods=["GET"])
@login_required
def parecer_conclusivo(id):
    if session["tipo"] != "sindicante":
        flash("Acesso restrito a sindicantes", "danger")
        return redirect("/dashboard")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM candidatos WHERE id = ?", (id,))
    candidato = cursor.fetchone()
    if not candidato:
        flash("Candidato não encontrado", "danger")
        return redirect("/minhas_sindicancias")
    cursor.execute("""
        SELECT nome_completo, cim_numero, loja_nome, loja_numero, loja_orient 
        FROM usuarios WHERE id = ?
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
        WHERE candidato_id = ? AND sindicante = ?
    """, (id, session["usuario"]))
    parecer_existente = cursor.fetchone()
    fontes_existentes = []
    if parecer_existente and parecer_existente["fontes"]:
        try:
            fontes_existentes = json.loads(parecer_existente["fontes"])
        except:
            fontes_existentes = []
    conn.close()
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
    conn = get_db()
    cursor = conn.cursor()
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
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        cursor.execute("""
            INSERT INTO pareceres_conclusivos 
            (candidato_id, sindicante, parecer_texto, conclusao, observacoes, 
             cim_numero, data_parecer, data_envio, fontes, loja_nome, loja_numero, loja_orient)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(candidato_id, sindicante) 
            DO UPDATE SET 
                parecer_texto = excluded.parecer_texto,
                conclusao = excluded.conclusao,
                observacoes = excluded.observacoes,
                cim_numero = excluded.cim_numero,
                data_parecer = excluded.data_parecer,
                data_envio = excluded.data_envio,
                fontes = excluded.fontes,
                loja_nome = excluded.loja_nome,
                loja_numero = excluded.loja_numero,
                loja_orient = excluded.loja_orient
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
            VALUES (?, ?, ?, ?)
            ON CONFLICT(candidato_id, sindicante) 
            DO UPDATE SET parecer = ?, data_envio = ?
        """, (id, session["usuario"], parecer_simples, agora, parecer_simples, agora))
        conn.commit()
    except Exception as e:
        flash(f"Erro ao salvar parecer: {str(e)}", "danger")
    conn.close()
    return redirect(f"/sindicancia/{id}")

@app.route("/visualizar_parecer_conclusivo/<int:id>")
@login_required
def visualizar_parecer_conclusivo(id):
    sindicante = request.args.get("sindicante", session["usuario"])
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pc.*, c.nome as candidato_nome, u.nome_completo as sindicante_nome
        FROM pareceres_conclusivos pc
        JOIN candidatos c ON pc.candidato_id = c.id
        JOIN usuarios u ON pc.sindicante = u.usuario
        WHERE pc.candidato_id = ? AND pc.sindicante = ?
    """, (id, sindicante))
    parecer = cursor.fetchone()
    conn.close()
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
        from flask import send_file

        sindicante = request.args.get("sindicante")
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT pc.*, c.nome as candidato_nome, u.nome_completo as sindicante_nome
            FROM pareceres_conclusivos pc
            JOIN candidatos c ON pc.candidato_id = c.id
            JOIN usuarios u ON pc.sindicante = u.usuario
            WHERE pc.candidato_id = ? AND pc.sindicante = ?
        """, (id, sindicante))
        parecer = cursor.fetchone()
        if not parecer:
            flash("Parecer conclusivo não encontrado", "danger")
            return redirect("/dashboard")
        fontes = json.loads(parecer["fontes"]) if parecer["fontes"] else []
        conn.close()

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
            ["Data do Parecer:", parecer["data_parecer"]],
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
    conn = get_db()
    cursor = conn.cursor()
    if request.method == "POST":
        usuario = request.form["usuario"].strip()
        senha = request.form["senha"]
        nome_completo = request.form.get("nome_completo", "")
        cim_numero = request.form.get("cim_numero", "")
        loja_nome = request.form.get("loja_nome", "")
        loja_numero = request.form.get("loja_numero", "")
        loja_orient = request.form.get("loja_orient", "")
        if usuario and senha:
            try:
                senha_hash = generate_password_hash(senha)
                agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute("""
                    INSERT INTO usuarios 
                    (usuario, senha_hash, tipo, data_cadastro, ativo, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (usuario, senha_hash, "sindicante", agora, 1, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient))
                conn.commit()
                sindicante_id = cursor.lastrowid
                registrar_log("criar", "sindicante", sindicante_id, dados_novos={"usuario": usuario})
                flash(f"Sindicante '{usuario}' adicionado!", "success")
            except sqlite3.IntegrityError:
                flash("Usuário já existe", "danger")
    cursor.execute("""
        SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, ativo 
        FROM usuarios WHERE tipo = 'sindicante' AND ativo = 1
        ORDER BY nome_completo
    """)
    sindicantes = cursor.fetchall()
    cursor.execute("SELECT * FROM lojas")
    lojas = cursor.fetchall()
    conn.close()
    return render_template("sindicantes.html", sindicantes=sindicantes, lojas=lojas)

@app.route("/excluir_sindicante/<int:id>")
@admin_required
def excluir_sindicante(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM usuarios WHERE id = ?", (id,))
    dados = dict(cursor.fetchone()) if cursor.fetchone() else None
    cursor.execute("SELECT tipo FROM usuarios WHERE id = ?", (id,))
    usuario = cursor.fetchone()
    if usuario and usuario["tipo"] == "sindicante":
        cursor.execute("UPDATE usuarios SET ativo = 0 WHERE id = ?", (id,))
        conn.commit()
        registrar_log("desativar", "sindicante", id, dados_anteriores=dados)
        flash("Sindicante removido com sucesso!", "success")
    else:
        flash("Usuário não encontrado ou não é sindicante", "danger")
    conn.close()
    return redirect("/sindicantes")

@app.route("/reativar_sindicante/<int:id>")
@admin_required
def reativar_sindicante(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT tipo FROM usuarios WHERE id = ?", (id,))
    usuario = cursor.fetchone()
    if usuario and usuario["tipo"] == "sindicante":
        cursor.execute("UPDATE usuarios SET ativo = 1 WHERE id = ?", (id,))
        conn.commit()
        registrar_log("reativar", "sindicante", id)
        flash("Sindicante reativado com sucesso!", "success")
    else:
        flash("Usuário não encontrado ou não é sindicante", "danger")
    conn.close()
    return redirect("/sindicantes")

@app.route("/editar_sindicante/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_sindicante(id):
    conn = get_db()
    cursor = conn.cursor()
    if request.method == "POST":
        nome_completo = request.form.get("nome_completo", "")
        cim_numero = request.form.get("cim_numero", "")
        loja_nome = request.form.get("loja_nome", "")
        loja_numero = request.form.get("loja_numero", "")
        loja_orient = request.form.get("loja_orient", "")
        
        cursor.execute("SELECT * FROM usuarios WHERE id = ?", (id,))
        dados_antigos = dict(cursor.fetchone())
        
        cursor.execute("""
            UPDATE usuarios 
            SET nome_completo = ?, cim_numero = ?, loja_nome = ?, loja_numero = ?, loja_orient = ?
            WHERE id = ?
        """, (nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, id))
        conn.commit()
        
        registrar_log("editar", "sindicante", id, dados_anteriores=dados_antigos,
                     dados_novos={"nome_completo": nome_completo})
        flash("Sindicante atualizado!", "success")
        return redirect("/sindicantes")
    
    cursor.execute("SELECT * FROM usuarios WHERE id = ?", (id,))
    sindicante = cursor.fetchone()
    cursor.execute("SELECT * FROM lojas")
    lojas = cursor.fetchall()
    conn.close()
    return render_template("editar_sindicante.html", sindicante=sindicante, lojas=lojas)

# =============================
# ROTAS DE LOJAS
# =============================
@app.route("/lojas", methods=["GET", "POST"])
@admin_required
def gerenciar_lojas():
    conn = get_db()
    cursor = conn.cursor()
    if request.method == "POST":
        nome = request.form.get("nome", "")
        numero = request.form.get("numero", "")
        oriente = request.form.get("oriente", "")
        cidade = request.form.get("cidade", "")
        estado = request.form.get("estado", "")
        data_fundacao = request.form.get("data_fundacao", "")
        if nome and numero:
            cursor.execute("""
                INSERT INTO lojas (nome, numero, oriente, cidade, estado, data_fundacao)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (nome, numero, oriente, cidade, estado, data_fundacao))
            conn.commit()
            loja_id = cursor.lastrowid
            registrar_log("criar", "loja", loja_id, dados_novos={"nome": nome, "numero": numero})
            flash("Loja adicionada!", "success")
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    conn.close()
    return render_template("lojas.html", lojas=lojas)

@app.route("/excluir_loja/<int:id>")
@admin_required
def excluir_loja(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM lojas WHERE id = ?", (id,))
    dados = dict(cursor.fetchone()) if cursor.fetchone() else None
    cursor.execute("DELETE FROM lojas WHERE id = ?", (id,))
    conn.commit()
    registrar_log("excluir", "loja", id, dados_anteriores=dados)
    conn.close()
    flash("Loja excluída!", "success")
    return redirect("/lojas")

# =============================
# ROTAS DE BACKUP E RESTAURAÇÃO
# =============================
@app.route("/backup")
@admin_required
def backup_banco():
    db_path = "banco.db"
    if not os.path.exists(db_path):
        flash("Arquivo do banco de dados não encontrado.", "danger")
        return redirect("/dashboard")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"backup_banco_{timestamp}.db"
    
    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, backup_filename)
    shutil.copy2(db_path, temp_path)
    
    registrar_log("backup", "banco", None, dados_novos={"arquivo": backup_filename})
    
    response = send_file(
        temp_path,
        as_attachment=True,
        download_name=backup_filename,
        mimetype="application/x-sqlite3"
    )
    
    def remove_file():
        try:
            os.remove(temp_path)
        except:
            pass
    
    response.call_on_close(remove_file)
    return response

ALLOWED_EXTENSIONS = {'db', 'sqlite', 'sqlite3'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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
    
    if not allowed_file(file.filename):
        flash("Formato de arquivo não permitido. Use .db, .sqlite ou .sqlite3", "danger")
        return redirect("/restaurar")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_atual = f"backup_antes_restauracao_{timestamp}.db"
        backup_path = os.path.join(tempfile.gettempdir(), backup_atual)
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
            flash("Arquivo inválido: não é um banco de dados SQLite válido", "danger")
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
# ROTAS DE AUDITORIA
# =============================
@app.route("/auditoria")
@admin_required
def listar_logs():
    conn = get_db()
    cursor = conn.cursor()
    
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
        query += " AND date(l.data_hora) >= ?"
        params.append(data_ini)
    if data_fim:
        query += " AND date(l.data_hora) <= ?"
        params.append(data_fim)
    if acao:
        query += " AND l.acao = ?"
        params.append(acao)
    if entidade:
        query += " AND l.entidade = ?"
        params.append(entidade)
    if usuario:
        query += " AND l.usuario_nome LIKE ?"
        params.append(f"%{usuario}%")
    
    query += " ORDER BY l.data_hora DESC LIMIT 1000"
    
    cursor.execute(query, params)
    logs = cursor.fetchall()
    
    cursor.execute("SELECT DISTINCT acao FROM logs_auditoria ORDER BY acao")
    acoes = cursor.fetchall()
    cursor.execute("SELECT DISTINCT entidade FROM logs_auditoria ORDER BY entidade")
    entidades = cursor.fetchall()
    
    conn.close()
    
    return render_template("auditoria/logs.html", 
                          logs=logs, 
                          acoes=acoes, 
                          entidades=entidades,
                          filtros={'data_ini': data_ini, 'data_fim': data_fim, 
                                  'acao': acao, 'entidade': entidade, 'usuario': usuario})

@app.route("/auditoria/<int:id>")
@admin_required
def detalhes_log(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT l.*, u.usuario, u.nome_completo
        FROM logs_auditoria l
        LEFT JOIN usuarios u ON l.usuario_id = u.id
        WHERE l.id = ?
    """, (id,))
    log = cursor.fetchone()
    conn.close()
    
    if not log:
        flash("Registro não encontrado", "danger")
        return redirect("/auditoria")
    
    return render_template("auditoria/detalhes.html", log=log)

@app.route("/auditoria/exportar")
@admin_required
def exportar_logs():
    conn = get_db()
    cursor = conn.cursor()
    
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
        query += " AND date(l.data_hora) >= ?"
        params.append(data_ini)
    if data_fim:
        query += " AND date(l.data_hora) <= ?"
        params.append(data_fim)
    
    query += " ORDER BY l.data_hora DESC"
    
    cursor.execute(query, params)
    logs = cursor.fetchall()
    conn.close()
    
    import csv
    from io import StringIO
    
    output = StringIO()
    writer = csv.writer(output, delimiter=';')
    
    # Cabeçalho
    writer.writerow(['ID', 'Data/Hora', 'Usuário', 'Ação', 'Entidade', 'ID Entidade', 'IP', 'Dados Anteriores', 'Dados Novos'])
    
    for log in logs:
        # Tratamento de valores None
        dados_anteriores = log['dados_anteriores'] if log['dados_anteriores'] is not None else ''
        dados_novos = log['dados_novos'] if log['dados_novos'] is not None else ''
        
        writer.writerow([
            log['id'],
            log['data_hora'],
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
    
    # Usando Response diretamente (não precisa de make_response)
    from flask import Response
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

        conn = get_db()
        cursor = conn.cursor()

        if tipo == "ano":
            reunioes = cursor.execute("""
                SELECT * FROM reunioes 
                WHERE strftime('%Y', data) = ? AND status = 'realizada'
                ORDER BY data
            """, (str(ano),)).fetchall()
            periodo_desc = f"Ano {ano}"
        elif tipo == "mes":
            reunioes = cursor.execute("""
                SELECT * FROM reunioes 
                WHERE strftime('%Y-%m', data) = ? AND status = 'realizada'
                ORDER BY data
            """, (f"{ano}-{mes:02d}",)).fetchall()
            periodo_desc = f"{mes:02d}/{ano}"
        elif tipo == "periodo":
            reunioes = cursor.execute("""
                SELECT * FROM reunioes 
                WHERE data BETWEEN ? AND ? AND status = 'realizada'
                ORDER BY data
            """, (data_inicio, data_fim)).fetchall()
            periodo_desc = f"{data_inicio} a {data_fim}"
        else:
            flash("Período inválido", "danger")
            return redirect("/relatorios/consolidados")

        obreiros = cursor.execute("""
            SELECT id, nome_completo, grau_atual 
            FROM usuarios 
            WHERE ativo = 1 
            ORDER BY grau_atual DESC, nome_completo
        """).fetchall()

        stats = []
        for o in obreiros:
            total_reunioes = len(reunioes)
            placeholders = ','.join('?' * len(reunioes))
            presentes = cursor.execute(f"""
                SELECT COUNT(*) as count
                FROM presenca p
                JOIN reunioes r ON p.reuniao_id = r.id
                WHERE p.obreiro_id = ? 
                  AND p.presente = 1
                  AND r.id IN ({placeholders})
            """, [o["id"]] + [r["id"] for r in reunioes]).fetchone()["count"]
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

        conn.close()

        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import A4
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import cm
            from io import BytesIO
            from flask import send_file

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
                    conn2 = get_db()
                    cur2 = conn2.cursor()
                    cur2.execute("""
                        SELECT COUNT(*) as total, SUM(CASE WHEN presente = 1 THEN 1 ELSE 0 END) as presentes
                        FROM presenca WHERE reuniao_id = ?
                    """, (r["id"],))
                    res = cur2.fetchone()
                    conn2.close()
                    reunioes_dados.append([
                        r["data"][:10],
                        r["titulo"],
                        str(res["presentes"]),
                        str(res["total"])
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

        conn = get_db()
        cursor = conn.cursor()

        if tipo == "ano":
            reunioes = cursor.execute("""
                SELECT r.*, 
                       (SELECT COUNT(*) FROM presenca WHERE reuniao_id = r.id) as total,
                       (SELECT SUM(presente) FROM presenca WHERE reuniao_id = r.id) as presentes
                FROM reunioes r
                WHERE strftime('%Y', r.data) = ? AND r.status = 'realizada'
                ORDER BY r.data
            """, (str(ano),)).fetchall()
            filtro_desc = f"Ano {ano}"
        elif tipo == "mes":
            reunioes = cursor.execute("""
                SELECT r.*, 
                       (SELECT COUNT(*) FROM presenca WHERE reuniao_id = r.id) as total,
                       (SELECT SUM(presente) FROM presenca WHERE reuniao_id = r.id) as presentes
                FROM reunioes r
                WHERE strftime('%Y-%m', r.data) = ? AND r.status = 'realizada'
                ORDER BY r.data
            """, (f"{ano}-{int(mes):02d}",)).fetchall()
            filtro_desc = f"{int(mes):02d}/{ano}"
        elif tipo == "periodo":
            reunioes = cursor.execute("""
                SELECT r.*, 
                       (SELECT COUNT(*) FROM presenca WHERE reuniao_id = r.id) as total,
                       (SELECT SUM(presente) FROM presenca WHERE reuniao_id = r.id) as presentes
                FROM reunioes r
                WHERE r.data BETWEEN ? AND ? AND r.status = 'realizada'
                ORDER BY r.data
            """, (data_inicio, data_fim)).fetchall()
            filtro_desc = f"{data_inicio} a {data_fim}"
        else:
            flash("Período inválido", "danger")
            return redirect("/exportar/presenca")

        if not reunioes:
            flash("Nenhuma reunião encontrada no período selecionado", "warning")
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

        row = 2
        for reuniao in reunioes:
            cursor.execute("""
                SELECT u.nome_completo, u.grau_atual, p.presente, p.tipo_ausencia, p.justificativa, 
                       u2.nome_completo as validado_por
                FROM presenca p
                JOIN usuarios u ON p.obreiro_id = u.id
                LEFT JOIN usuarios u2 ON p.validado_por = u2.id
                WHERE p.reuniao_id = ?
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
                    reuniao["data"][:10],
                    p["nome_completo"],
                    grau_texto,
                    presente_texto,
                    tipo_ausencia,
                    justificativa,
                    validado
                ])
                row += 1

        conn.close()

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
        from flask import send_file

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM candidatos WHERE id = ?", (id,))
        candidato = cursor.fetchone()
        if not candidato:
            flash("Candidato não encontrado", "danger")
            return redirect("/candidatos")

        cursor.execute("""
            SELECT s.*, u.usuario, u.nome_completo, u.cim_numero, u.loja_nome, u.loja_numero, u.loja_orient
            FROM sindicancias s
            JOIN usuarios u ON s.sindicante = u.usuario
            WHERE s.candidato_id = ?
            ORDER BY s.data_envio DESC
        """, (id,))
        pareceres = cursor.fetchall()

        cursor.execute("""
            SELECT * FROM pareceres_conclusivos 
            WHERE candidato_id = ?
        """, (id,))
        pareceres_conclusivos = cursor.fetchall()
        conn.close()

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
        data_abertura = candidato["data_criacao"][:16] if candidato["data_criacao"] else "N/A"
        data_fechamento = candidato["data_fechamento"][:16] if candidato["data_fechamento"] else "Em andamento"
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
                elementos.append(Paragraph(f"<b>Data:</b> {pc['data_parecer']}", styles['Normal']))
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
                    p['data_envio'][:16]
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
from werkzeug.utils import secure_filename
import os

@app.route("/obreiros/<int:id>/documentos")
@login_required
def listar_documentos(id):
    # Verificar permissão
    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para acessar esta página", "danger")
        return redirect("/obreiros")
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Buscar obreiro
    cursor.execute("SELECT nome_completo FROM usuarios WHERE id = ?", (id,))
    obreiro = cursor.fetchone()
    if not obreiro:
        flash("Obreiro não encontrado", "danger")
        return redirect("/obreiros")
    
    # Buscar documentos do obreiro
    cursor.execute("""
        SELECT d.*, c.nome as categoria_nome, c.icone
        FROM documentos_obreiro d
        LEFT JOIN categorias_documentos c ON d.categoria = c.nome
        WHERE d.obreiro_id = ?
        ORDER BY d.data_upload DESC
    """, (id,))
    documentos = cursor.fetchall()
    
    # Buscar categorias para o filtro
    cursor.execute("SELECT * FROM categorias_documentos WHERE ativo = 1 ORDER BY nome")
    categorias = cursor.fetchall()
    
    conn.close()
    
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
        # Adicionar timestamp para evitar conflitos
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"{id}_{timestamp}_{filename}"
        caminho = os.path.join(UPLOAD_FOLDER, nome_arquivo)
        
        arquivo.save(caminho)
        
        # Obter tamanho do arquivo
        tamanho = os.path.getsize(caminho)
        
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO documentos_obreiro 
            (obreiro_id, titulo, descricao, categoria, tipo_arquivo, nome_arquivo, caminho_arquivo, tamanho, uploaded_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (id, titulo, descricao, categoria, filename.split('.')[-1], nome_arquivo, caminho, tamanho, session["user_id"]))
        
        conn.commit()
        doc_id = cursor.lastrowid
        conn.close()
        
        registrar_log("upload_documento", "documento", doc_id, dados_novos={"titulo": titulo, "categoria": categoria})
        flash(f"Documento '{titulo}' enviado com sucesso!", "success")
        
    except Exception as e:
        flash(f"Erro ao enviar arquivo: {str(e)}", "danger")
    
    return redirect(f"/obreiros/{id}/documentos")

@app.route("/documentos/<int:id>/baixar")
@login_required
def baixar_documento(id):
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT d.*, u.id as obreiro_id
        FROM documentos_obreiro d
        JOIN usuarios u ON d.obreiro_id = u.id
        WHERE d.id = ?
    """, (id,))
    doc = cursor.fetchone()
    
    if not doc:
        flash("Documento não encontrado", "danger")
        return redirect("/obreiros")
    
    # Verificar permissão
    if session["tipo"] != "admin" and session["user_id"] != doc["obreiro_id"]:
        flash("Você não tem permissão para baixar este documento", "danger")
        return redirect(f"/obreiros/{doc['obreiro_id']}/documentos")
    
    if not os.path.exists(doc["caminho_arquivo"]):
        flash("Arquivo não encontrado no servidor", "danger")
        return redirect(f"/obreiros/{doc['obreiro_id']}/documentos")
    
    conn.close()
    
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
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT d.*, u.id as obreiro_id
        FROM documentos_obreiro d
        JOIN usuarios u ON d.obreiro_id = u.id
        WHERE d.id = ?
    """, (id,))
    doc = cursor.fetchone()
    
    if not doc:
        flash("Documento não encontrado", "danger")
        return redirect("/obreiros")
    
    # Verificar permissão (apenas admin ou dono do documento)
    if session["tipo"] != "admin" and session["user_id"] != doc["obreiro_id"]:
        flash("Você não tem permissão para excluir este documento", "danger")
        return redirect(f"/obreiros/{doc['obreiro_id']}/documentos")
    
    try:
        # Remover arquivo físico
        if os.path.exists(doc["caminho_arquivo"]):
            os.remove(doc["caminho_arquivo"])
        
        # Remover registro do banco
        cursor.execute("DELETE FROM documentos_obreiro WHERE id = ?", (id,))
        conn.commit()
        
        registrar_log("excluir_documento", "documento", id, dados_anteriores={"titulo": doc["titulo"]})
        flash("Documento excluído com sucesso!", "success")
        
    except Exception as e:
        flash(f"Erro ao excluir documento: {str(e)}", "danger")
    
    conn.close()
    return redirect(f"/obreiros/{doc['obreiro_id']}/documentos")

# =============================
# INICIALIZAÇÃO
# =============================
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)