# app.py - Sistema Maçônico com PostgreSQL (VERSÃO COMPLETA COM BIBLIOTECA)
# -*- coding: utf-8 -*-
import requests
import os
import cloudinary
import cloudinary.uploader
import json
import zipfile
import shutil
import time
import threading
import traceback
import markdown
import csv
import subprocess
import tempfile
import webbrowser
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from functools import wraps
from urllib.parse import quote
from reportlab.lib.units import cm

from flask import (
    Blueprint, Flask, render_template, request, redirect, url_for, session, flash,
    jsonify, send_file, Response, after_this_request
)
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import openpyxl
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter
from database import get_db, return_connection, get_db_connection

# =============================
# BLUEPRINT DA BIBLIOTECA
# =============================
biblioteca_bp = Blueprint('biblioteca', __name__, url_prefix='/biblioteca')


# =============================
# FUNÇÕES DA BIBLIOTECA
# =============================

def tem_permissao_biblioteca():
    """Verifica se o usuário tem permissão para acessar a biblioteca"""
    if 'usuario_id' not in session:
        print(f"DEBUG: Usuário não está logado na sessão")
        print(f"DEBUG: Sessão keys: {list(session.keys())}")
        return False
    
    if session.get('tipo') == 'admin':
        print(f"DEBUG: Usuário é admin, acesso liberado")
        return True
    
    grau = session.get('grau_atual', 0)
    print(f"DEBUG: Grau do usuário: {grau}")
    
    if grau in [1, 2, 3]:
        print(f"DEBUG: Usuário tem grau {grau}, acesso liberado")
        return True
    
    print(f"DEBUG: Usuário sem permissão, grau {grau}")
    return False

def get_grau_usuario():
    """Retorna o grau do usuário logado"""
    return session.get('grau_atual', 1)

# =============================
# DECORATORS OTIMIZADOS
# =============================

# Cache para evitar consultas repetidas
_cache_grau = {}
_cache_timeout = 60  # 60 segundos

def _get_grau_usuario(usuario_id):
    """Busca o grau do usuário com cache"""
    # Verificar cache
    cache_key = f"grau_{usuario_id}"
    cache_entry = _cache_grau.get(cache_key)
    
    if cache_entry and (time.time() - cache_entry['timestamp']) < _cache_timeout:
        return cache_entry['grau']
    
    # Buscar no banco
    cursor, conn = None, None
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT grau_atual FROM usuarios WHERE id = %s", (usuario_id,))
        usuario = cursor.fetchone()
        grau = usuario['grau_atual'] if usuario else 1
        
        # Salvar no cache
        _cache_grau[cache_key] = {
            'grau': grau,
            'timestamp': time.time()
        }
        return grau
    except Exception as e:
        print(f"Erro ao buscar grau: {e}")
        return 1
    finally:
        if conn:
            return_connection(conn)

def _get_ata_grau(ata_id):
    """Busca o grau da ata com cache"""
    cache_key = f"ata_grau_{ata_id}"
    cache_entry = _cache_grau.get(cache_key)
    
    if cache_entry and (time.time() - cache_entry['timestamp']) < _cache_timeout:
        return cache_entry.get('grau', 1)
    
    cursor, conn = None, None
    try:
        cursor, conn = get_db()
        cursor.execute("""
            SELECT r.grau as reuniao_grau
            FROM atas a
            JOIN reunioes r ON a.reuniao_id = r.id
            WHERE a.id = %s
        """, (ata_id,))
        ata = cursor.fetchone()
        grau = ata['reuniao_grau'] if ata else 1
        
        _cache_grau[cache_key] = {
            'grau': grau,
            'timestamp': time.time()
        }
        return grau
    except Exception as e:
        print(f"Erro ao buscar grau da ata: {e}")
        return 1
    finally:
        if conn:
            return_connection(conn)

def require_grau(min_grau):
    """Decorator para verificar permissão por grau - OTIMIZADO"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'usuario_id' not in session:
                flash('Faça login para acessar esta página', 'warning')
                return redirect(url_for('login'))
            
            # Verificar se é admin (admin tem acesso a tudo)
            if session.get('tipo') == 'admin':
                return f(*args, **kwargs)
            
            # Usar cache para buscar grau
            grau_usuario = _get_grau_usuario(session['usuario_id'])
            
            if grau_usuario < min_grau:
                flash('Você não tem permissão para acessar este conteúdo', 'danger')
                return redirect(url_for('biblioteca.listar_materiais'))
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

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

def nivel_required(nivel_minimo):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if "usuario" not in session:
                flash("Faça login para acessar esta página", "warning")
                return redirect("/")
            
            # Admin tem acesso a tudo
            if session.get('tipo') == 'admin':
                return f(*args, **kwargs)
            
            nivel_usuario = session.get("nivel_acesso", 1)
            if nivel_usuario >= nivel_minimo:
                return f(*args, **kwargs)
            else:
                flash("Você não tem permissão para acessar esta página", "danger")
                return redirect("/dashboard")
        return decorated_function
    return decorator

def nivel_ata_required():
    """Decorator para verificar permissão de ata por grau - OTIMIZADO"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if "usuario" not in session:
                flash("Faça login para acessar esta página", "warning")
                return redirect("/")
            
            # Admin tem acesso a tudo
            if session.get('tipo') == 'admin':
                return f(*args, **kwargs)
            
            ata_id = kwargs.get('id')
            if ata_id:
                # Usar cache para buscar grau da ata
                reuniao_grau = _get_ata_grau(ata_id)
                nivel_usuario = session.get("nivel_acesso", 1)
                
                if nivel_usuario == 1 and reuniao_grau == 1:
                    return f(*args, **kwargs)
                elif nivel_usuario == 2 and reuniao_grau <= 2:
                    return f(*args, **kwargs)
                elif nivel_usuario >= 3:
                    return f(*args, **kwargs)
                else:
                    flash("Você não tem permissão para visualizar esta ata", "danger")
                    return redirect("/dashboard")
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator
    
    
def permissao_required(permissao_codigo):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not tem_permissao(permissao_codigo):
                flash("Você não tem permissão para acessar esta página", "danger")
                return redirect("/dashboard")
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def tem_permissao(permissao_codigo):
    """Verifica se o usuário logado tem determinada permissão"""
    if 'user_id' not in session:
        return False
    
    # Admin tem todas as permissões
    if session.get('tipo') == 'admin':
        return True
    
    # Mestres (grau >= 3) têm permissão para visualizar obreiros
    if permissao_codigo == 'obreiro.view' and session.get('grau_atual', 0) >= 3:
        return True
    
    return _verificar_permissao_db(permissao_codigo)

def _verificar_permissao_db(codigo):
    try:
        cursor, conn = get_db()
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

from functools import wraps

def permissao_required(permissao_chave):
    """Decorador para verificar permissão"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                flash("Você precisa estar logado para acessar esta página.", "danger")
                return redirect(url_for('login'))
            
            if verificar_permissao(session['user_id'], permissao_chave):
                return f(*args, **kwargs)
            else:
                flash("Você não tem permissão para acessar esta página.", "danger")
                return redirect(url_for('dashboard'))
        return decorated_function
    return decorator


# =============================
# RELATÓRIOS DA BIBLIOTECA
# =============================

@biblioteca_bp.route('/relatorios')
@admin_required
def relatorios_biblioteca():
    """Página de relatórios da biblioteca"""
    
    cursor, conn = get_db()
    
    # Estatísticas gerais
    cursor.execute("""
        SELECT 
            COUNT(*) as total_materiais,
            COUNT(CASE WHEN publicado = true THEN 1 END) as publicados,
            COUNT(CASE WHEN publicado = false THEN 1 END) as nao_publicados,
            SUM(downloads_count) as total_downloads,
            SUM(visualizacoes_count) as total_visualizacoes,
            COUNT(DISTINCT created_by) as total_contribuidores
        FROM materiais
    """)
    estatisticas_gerais = cursor.fetchone()
    
    # Materiais por grau
    cursor.execute("""
        SELECT 
            grau_acesso,
            COUNT(*) as quantidade,
            SUM(downloads_count) as downloads,
            SUM(visualizacoes_count) as visualizacoes
        FROM materiais
        GROUP BY grau_acesso
        ORDER BY grau_acesso
    """)
    materiais_por_grau = cursor.fetchall()
    
    # Materiais por tipo
    cursor.execute("""
        SELECT 
            tipo,
            COUNT(*) as quantidade,
            SUM(downloads_count) as downloads,
            SUM(visualizacoes_count) as visualizacoes
        FROM materiais
        GROUP BY tipo
        ORDER BY quantidade DESC
    """)
    materiais_por_tipo = cursor.fetchall()
    
    # Materiais por categoria
    cursor.execute("""
        SELECT 
            c.nome as categoria,
            COUNT(m.id) as quantidade,
            SUM(m.downloads_count) as downloads,
            SUM(m.visualizacoes_count) as visualizacoes
        FROM categorias_material c
        LEFT JOIN materiais m ON c.id = m.categoria_id
        GROUP BY c.id, c.nome
        ORDER BY quantidade DESC
    """)
    materiais_por_categoria = cursor.fetchall()
    
    # Materiais mais acessados (top 10)
    cursor.execute("""
        SELECT 
            m.id,
            m.titulo,
            m.tipo,
            m.visualizacoes_count as visualizacoes,
            m.downloads_count as downloads,
            c.nome as categoria
        FROM materiais m
        LEFT JOIN categorias_material c ON m.categoria_id = c.id
        WHERE m.publicado = true
        ORDER BY (m.visualizacoes_count + m.downloads_count) DESC
        LIMIT 10
    """)
    materiais_top = cursor.fetchall()
    
    # Materiais recentes
    cursor.execute("""
        SELECT 
            m.id,
            m.titulo,
            m.tipo,
            m.created_at,
            u.nome_completo as autor_nome,
            c.nome as categoria
        FROM materiais m
        LEFT JOIN categorias_material c ON m.categoria_id = c.id
        LEFT JOIN usuarios u ON m.created_by = u.id
        WHERE m.publicado = true
        ORDER BY m.created_at DESC
        LIMIT 10
    """)
    materiais_recentes = cursor.fetchall()
    
    # Downloads por período (últimos 30 dias)
    cursor.execute("""
        SELECT 
            DATE(data_download) as data,
            COUNT(*) as total_downloads
        FROM downloads_material
        WHERE data_download >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY DATE(data_download)
        ORDER BY data DESC
    """)
    downloads_periodo = cursor.fetchall()
    
    # Atividade por usuário
    cursor.execute("""
        SELECT 
            u.nome_completo as usuario,
            COUNT(DISTINCT d.material_id) as materiais_baixados,
            COUNT(DISTINCT f.material_id) as favoritos,
            COUNT(DISTINCT a.material_id) as avaliacoes
        FROM usuarios u
        LEFT JOIN downloads_material d ON u.id = d.usuario_id
        LEFT JOIN favoritos_material f ON u.id = f.usuario_id
        LEFT JOIN avaliacoes_material a ON u.id = a.usuario_id
        WHERE u.ativo = 1
        GROUP BY u.id, u.nome_completo
        ORDER BY materiais_baixados DESC
        LIMIT 10
    """)
    atividade_usuarios = cursor.fetchall()
    
    return_connection(conn)
    
    # Preparar dados para gráficos
    labels_grau = []
    dados_grau = []
    for item in materiais_por_grau:
        grau_nome = {1: 'Aprendiz', 2: 'Companheiro', 3: 'Mestre'}.get(item['grau_acesso'], 'Desconhecido')
        labels_grau.append(grau_nome)
        dados_grau.append(item['quantidade'])
    
    labels_tipo = [item['tipo'].capitalize() for item in materiais_por_tipo]
    dados_tipo = [item['quantidade'] for item in materiais_por_tipo]
    
    labels_categoria = [item['categoria'] for item in materiais_por_categoria if item['categoria']]
    dados_categoria = [item['quantidade'] for item in materiais_por_categoria if item['categoria']]
    
    return render_template('biblioteca/relatorios.html',
                         estatisticas_gerais=estatisticas_gerais,
                         materiais_por_grau=materiais_por_grau,
                         materiais_por_tipo=materiais_por_tipo,
                         materiais_por_categoria=materiais_por_categoria,
                         materiais_top=materiais_top,
                         materiais_recentes=materiais_recentes,
                         downloads_periodo=downloads_periodo,
                         atividade_usuarios=atividade_usuarios,
                         labels_grau=labels_grau,
                         dados_grau=dados_grau,
                         labels_tipo=labels_tipo,
                         dados_tipo=dados_tipo,
                         labels_categoria=labels_categoria,
                         dados_categoria=dados_categoria)


@biblioteca_bp.route('/relatorios/exportar/<formato>')
@admin_required
def exportar_relatorio(formato):
    """Exportar relatórios em CSV ou PDF"""
    
    cursor, conn = get_db()
    
    # Buscar dados para exportar
    cursor.execute("""
        SELECT 
            m.id,
            m.titulo,
            m.tipo,
            c.nome as categoria,
            CASE 
                WHEN m.grau_acesso = 1 THEN 'Aprendiz'
                WHEN m.grau_acesso = 2 THEN 'Companheiro'
                ELSE 'Mestre'
            END as grau,
            m.autor,
            m.editora,
            m.ano_publicacao,
            m.visualizacoes_count as visualizacoes,
            m.downloads_count as downloads,
            m.created_at as data_criacao,
            u.nome_completo as criado_por
        FROM materiais m
        LEFT JOIN categorias_material c ON m.categoria_id = c.id
        LEFT JOIN usuarios u ON m.created_by = u.id
        WHERE m.publicado = true
        ORDER BY m.created_at DESC
    """)
    materiais = cursor.fetchall()
    return_connection(conn)
    
    if formato == 'csv':
        # Exportar como CSV
        output = StringIO()
        writer = csv.writer(output, delimiter=';')
        
        # Cabeçalho
        writer.writerow(['ID', 'Título', 'Tipo', 'Categoria', 'Grau', 'Autor', 'Editora', 
                        'Ano', 'Visualizações', 'Downloads', 'Data Criação', 'Criado Por'])
        
        # Dados
        for m in materiais:
            writer.writerow([
                m['id'], m['titulo'], m['tipo'], m['categoria'] or '', 
                m['grau'], m['autor'] or '', m['editora'] or '', 
                m['ano_publicacao'] or '', m['visualizacoes'], m['downloads'],
                m['data_criacao'].strftime('%d/%m/%Y') if m['data_criacao'] else '',
                m['criado_por'] or ''
            ])
        
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=relatorio_materiais_{datetime.now().strftime("%Y%m%d")}.csv'}
        )
    
    elif formato == 'pdf':
        # Exportar como PDF (usando ReportLab)
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import cm
            
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), rightMargin=2*cm, leftMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm)
            elements = []
            styles = getSampleStyleSheet()
            
            # Título
            titulo_style = ParagraphStyle('CustomTitle', parent=styles['Title'], fontSize=16, alignment=1)
            elements.append(Paragraph("Relatório de Materiais - Biblioteca Maçônica", titulo_style))
            elements.append(Spacer(1, 0.5*cm))
            elements.append(Paragraph(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", styles['Normal']))
            elements.append(Spacer(1, 1*cm))
            
            # Tabela
            data = [['ID', 'Título', 'Tipo', 'Categoria', 'Grau', 'Visualizações', 'Downloads']]
            for m in materiais[:50]:  # Limitar a 50 itens no PDF
                data.append([
                    str(m['id']), m['titulo'][:40], m['tipo'], 
                    m['categoria'] or '', m['grau'], str(m['visualizacoes']), str(m['downloads'])
                ])
            
            table = Table(data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
            ]))
            elements.append(table)
            
            doc.build(elements)
            buffer.seek(0)
            
            return Response(
                buffer.getvalue(),
                mimetype='application/pdf',
                headers={'Content-Disposition': f'attachment; filename=relatorio_materiais_{datetime.now().strftime("%Y%m%d")}.pdf'}
            )
        except ImportError:
            flash('Módulo ReportLab não instalado. Use CSV para exportar.', 'warning')
            return redirect(url_for('biblioteca.relatorios_biblioteca'))


# =============================
# COMPARTILHAMENTO
# =============================

@biblioteca_bp.route('/material/<int:material_id>/compartilhar')
def compartilhar_material(material_id):
    """Gerar link de compartilhamento para o material"""
    
    cursor, conn = get_db()
    cursor.execute("""
        SELECT titulo, descricao, capa_url, tipo
        FROM materiais 
        WHERE id = %s AND publicado = true
    """, (material_id,))
    material = cursor.fetchone()
    return_connection(conn)
    
    if not material:
        return jsonify({'error': 'Material não encontrado'}), 404
    
    # Gerar URL completa
    url_completa = request.url_root.rstrip('/') + url_for('biblioteca.visualizar_material', material_id=material_id)
    
    # Preparar dados para compartilhamento
    dados_compartilhamento = {
        'url': url_completa,
        'titulo': material['titulo'],
        'descricao': material['descricao'][:150] if material['descricao'] else '',
        'imagem': material['capa_url'] or request.url_root + 'static/img/logo.png',
        'tipo': material['tipo']
    }
    
    return render_template('biblioteca/compartilhar.html', dados=dados_compartilhamento)


@biblioteca_bp.route('/api/material/<int:material_id>/compartilhar', methods=['POST'])
def api_compartilhar_material(material_id):
    """API para registrar compartilhamento"""
    
    plataforma = request.json.get('plataforma', 'link')
    
    cursor, conn = get_db()
    cursor.execute("""
        UPDATE materiais 
        SET compartilhamentos_count = compartilhamentos_count + 1 
        WHERE id = %s
    """, (material_id,))
    conn.commit()
    return_connection(conn)
    
    return jsonify({'success': True})        


# =============================
# CONFIGURAÇÃO DO CLOUDINARY
# =============================

cloudinary.config(
    cloud_name="da57u8plb",
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET")
)

# =============================
# CARREGAR VARIÁVEIS DE AMBIENTE
# =============================
load_dotenv()

# =============================
# CONFIGURAÇÕES GERAIS
# =============================
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads', 'documentos')
UPLOAD_FOLDER_FOTOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads', 'fotos')
ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'png', 'gif', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'zip', 'rar'}
ALLOWED_EXTENSIONS_FOTOS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(UPLOAD_FOLDER_FOTOS, exist_ok=True)

# =============================
# CRIAÇÃO DA APLICAÇÃO FLASK
# =============================
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

# =============================
# REGISTRAR BLUEPRINT DA BIBLIOTECA
# =============================
app.register_blueprint(biblioteca_bp)

# =============================
# CONEXÃO COM BANCO DE DADOS
# =============================
import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

# Configurar DATABASE_URL
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    # Fallback para desenvolvimento local
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'sistema_maconico')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print(f"⚠️ Usando conexão LOCAL: {DB_HOST}:{DB_PORT}/{DB_NAME}")
else:
    # Corrigir URL para PostgreSQL (Render usa postgres://)
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    print(f"✅ Usando conexão RENDER (PostgreSQL)")

# =============================
# FUNÇÕES DE BANCO DE DADOS
# =============================

import os
import psycopg2
from psycopg2.extras import RealDictCursor

def return_connection(conn):
    """Retorna a conexão - não faz nada pois o teardown cuida disso"""
    pass


import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
import os

# ============================================
# POOL DE CONEXÕES - Singleton
# ============================================
_db_pool = None
_pool_initialized = False

def init_db_pool():
    """Inicializa o pool de conexões (chamado apenas uma vez)"""
    global _db_pool, _pool_initialized
    
    if _pool_initialized:
        return _db_pool
    
    print("🚀 Inicializando pool de conexões...")
    
    try:
        DATABASE_URL = os.getenv('DATABASE_URL')
        
        if DATABASE_URL:
            print(f"🔧 Configurando pool para o banco do Render")
            
            # Converter postgres:// para postgresql:// se necessário
            if DATABASE_URL.startswith('postgres://'):
                DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
            
            # Adicionar sslmode e timeout se não tiver
            if 'sslmode' not in DATABASE_URL:
                separator = '&' if '?' in DATABASE_URL else '?'
                DATABASE_URL += f"{separator}sslmode=require&connect_timeout=30"
            
            # Usar ThreadedConnectionPool em vez de SimpleConnectionPool
            _db_pool = psycopg2.pool.ThreadedConnectionPool(
                1, 10,  # mínimo 1, máximo 10 (reduzido)
                DATABASE_URL,
                keepalives=1,
                keepalives_idle=5,
                keepalives_interval=2,
                keepalives_count=2
            )
        else:
            print(f"🔧 Configurando pool para banco local")
            _db_pool = psycopg2.pool.ThreadedConnectionPool(
                1, 10,
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                dbname=os.getenv('DB_NAME', 'sistema_maconico'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', 'postgres'),
                keepalives=1,
                keepalives_idle=5,
                keepalives_interval=2,
                keepalives_count=2
            )
        
        # Testar conexão (apenas uma)
        test_conn = _db_pool.getconn()
        cursor = test_conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        _db_pool.putconn(test_conn)
        
        _pool_initialized = True
        print(f"✅ Pool de conexões inicializado com sucesso!")
        return _db_pool
        
    except Exception as e:
        print(f"❌ Erro ao inicializar pool: {e}")
        import traceback
        traceback.print_exc()
        # Não levantar exceção - permitir que a aplicação continue
        _pool_initialized = True  # Marcar como tentado
        return None

# Variável global para reutilizar a mesma conexão na mesma requisição
_conexao_atual = None
_cursor_atual = None

def get_db():
    """Reutiliza a mesma conexão na mesma requisição"""
    global _conexao_atual, _cursor_atual
    
    # Se já existe uma conexão na mesma requisição, reutiliza
    if hasattr(request, 'db_connection') and request.db_connection:
        # Rollback para limpar transações pendentes
        try:
            request.db_connection.rollback()
        except:
            pass
        return request.db_cursor, request.db_connection
    
    pool = init_db_pool()
    
    # Se o pool falhou, criar conexão direta
    if pool is None:
        print("⚠️ Pool não disponível, criando conexão direta")
        return get_db_direct()
    
    try:
        conn = pool.getconn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Rollback inicial
        conn.rollback()
        
        # Armazenar na requisição
        request.db_connection = conn
        request.db_cursor = cursor
        
        print(f"🔌 Conexão obtida do pool")
        return cursor, conn
        
    except Exception as e:
        print(f"❌ Erro ao obter conexão do pool: {e}")
        # Fallback: conexão direta
        return get_db_direct()

def get_db_direct():
    """Cria conexão direta (fallback)"""
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    if DATABASE_URL:
        if DATABASE_URL.startswith('postgres://'):
            DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
        
        conn = psycopg2.connect(
            DATABASE_URL,
            cursor_factory=RealDictCursor,
            connect_timeout=30,
            keepalives=1,
            keepalives_idle=5,
            keepalives_interval=2,
            keepalives_count=2
        )
    else:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            dbname=os.getenv('DB_NAME', 'sistema_maconico'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'postgres'),
            cursor_factory=RealDictCursor,
            connect_timeout=30
        )
    
    cursor = conn.cursor()
    print(f"🔌 Conexão direta criada")
    return cursor, conn

def return_connection(conn):
    """Retorna a conexão - mas não fecha imediatamente"""
    pass

@app.teardown_request
def teardown_request(exception=None):
    """Fecha a conexão no final da requisição"""
    if hasattr(request, 'db_connection') and request.db_connection:
        try:
            # Sempre fazer rollback no final
            request.db_connection.rollback()
        except:
            pass
        try:
            pool = init_db_pool()
            if pool:
                pool.putconn(request.db_connection)
                print(f"🔌 Conexão retornada ao pool")
            else:
                request.db_connection.close()
                print(f"🔌 Conexão direta fechada")
        except Exception as e:
            print(f"Erro ao fechar conexão: {e}")
        finally:
            delattr(request, 'db_connection')
            delattr(request, 'db_cursor')
# ============================================
# CONTEXT MANAGER (recomendado)
# ============================================
from contextlib import contextmanager

@contextmanager
def get_db_connection():
    """Context manager para usar com 'with' - garante uma única conexão"""
    cursor, conn = get_db()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_connection(conn)

# =============================
# IMPORTANTE: Chamar test_connection() DEPOIS de definir as funções
# =============================
def test_connection():
    """Testa a conexão com o banco de dados"""
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        return_connection(conn)
        print("✅ Conexão com o banco de dados estabelecida com sucesso!")
        return True
    except Exception as e:
        print(f"❌ Erro na conexão com o banco: {e}")
        return False
        
# Verificar conexão (opcional)
#if __name__ != '__main__':
 #   print(f"🔧 Banco configurado: LOCAL (localhost:5432/sistema_maconico)")
  #  test_connection()

# ============================================
# CONFIGURAÇÕES INICIAIS DO WHATSAPP
# ============================================

def init_whatsapp_tables():
    """Inicializa as tabelas do WhatsApp (sem dependência do request)"""
    try:
        # Usar conexão direta sem passar pelo get_db()
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS grupos_whatsapp (
                id SERIAL PRIMARY KEY,
                grupo_id VARCHAR(255) UNIQUE NOT NULL,
                nome_grupo VARCHAR(255),
                ultimo_envio TIMESTAMP,
                criado_por INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mensagens_agendadas (
                id SERIAL PRIMARY KEY,
                grupo_id VARCHAR(255) NOT NULL,
                mensagem TEXT NOT NULL,
                nome_grupo VARCHAR(255),
                data_envio TIMESTAMP NOT NULL,
                recorrencia VARCHAR(50),
                criado_por INTEGER,
                status VARCHAR(50) DEFAULT 'agendado',
                enviado_em TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_grupos_grupo_id ON grupos_whatsapp(grupo_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mensagens_data_envio ON mensagens_agendadas(data_envio)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mensagens_status ON mensagens_agendadas(status)")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Tabelas do WhatsApp inicializadas com sucesso!")
        return True
    except Exception as e:
        print(f"❌ Erro ao inicializar tabelas do WhatsApp: {e}")
        return False

# Agora pode chamar no nível global
init_whatsapp_tables()

# =============================
# FUNÇÕES AUXILIARES
# =============================
import unicodedata

def formatar_data_pt(data):
    meses = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }
    return f"{data.day} de {meses[data.month]} de {data.year}"

def remover_acentos(texto):
    """Remove acentos de uma string"""
    if not texto:
        return texto
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.lower()

def tratar_valor_nulo(valor, tipo='string'):
    if valor is None:
        return None
    if isinstance(valor, str) and valor.strip() == '':
        return None
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

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def allowed_foto(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS_FOTOS

from datetime import datetime

# ============================================
# FUNÇÃO AUXILIAR - NOME DOS GRAUS
# ============================================

def get_nome_grau(grau):
    """Retorna o nome do grau pelo número"""
    graus_map = {
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
        13: "Grande Escocês da Abóbada Sagrada",
        14: "Grande Escocês da Perfeição",
        15: "Cavaleiro do Oriente",
        16: "Príncipe de Jerusalém",
        17: "Cavaleiro do Oriente e Ocidente",
        18: "Cavaleiro Rosa-Cruz",
        19: "Grande Pontífice",
        20: "Venerável Grande Mestre",
        21: "Cavaleiro do Sol",
        22: "Cavaleiro da Cruz Vermelha",
        23: "Cavaleiro do Santo Sepulcro",
        24: "Cavaleiro da Águia Branca",
        25: "Cavaleiro da Serpente",
        26: "Príncipe da Mercê",
        27: "Comendador do Templo",
        28: "Cavaleiro do Sol",
        29: "Cavaleiro de São Jorge",
        30: "Cavaleiro Kadosch",
        31: "Grande Escocês",
        32: "Príncipe do Real Segredo",
        33: "Soberano Grande Inspetor Geral"
    }
    return graus_map.get(grau, f"Grau {grau}")

# =============================
# FUNÇÃO AUX. ENVIO DE EMAIL INICIAÇÃO
# =============================

def enviar_email_iniciacao(email, nome, numero_placet, cim_numero):
    """Envia e-mail de confirmação de iniciação"""
    assunto = "🎉 Bem-vindo à ARLS Bicentenário - Sua Iniciação foi Registrada!"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Iniciação Registrada</title>
    </head>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <h2 style="color: #4f46e5;">🎉 Parabéns, {nome}!</h2>
            <p>Sua iniciação foi registrada com sucesso na ARLS Bicentenário.</p>
            <div style="background: #f3f4f6; padding: 15px; border-radius: 10px; margin: 20px 0;">
                <p><strong>📄 Placet de Iniciação:</strong> {numero_placet}</p>
                <p><strong>🆔 CIM (Carteira de Identidade Maçônica):</strong> {cim_numero}</p>
            </div>
            <p>Agora você é oficialmente um Obreiro da nossa Loja, no Grau de <strong>Aprendiz</strong>.</p>
            <p>Em breve você receberá mais informações sobre as próximas sessões.</p>
            <hr>
            <small>ARLS Bicentenário - Loja Maçônica</small>
        </div>
    </body>
    </html>
    """
    
    return enviar_email_resend(email, assunto, html_content)

# =============================
# FUNÇÃO DE LOG DE AUDITORIA
# =============================
import json

def registrar_log(acao, entidade=None, entidade_id=None, dados_anteriores=None, dados_novos=None):
    """Registra log de auditoria com detalhes completos"""
    if "user_id" not in session:
        return
    try:
        cursor, conn = get_db()
        
        # Converter dicionários para JSON string
        if dados_anteriores and isinstance(dados_anteriores, dict):
            dados_anteriores = json.dumps(dados_anteriores, ensure_ascii=False, default=str)
        if dados_novos and isinstance(dados_novos, dict):
            dados_novos = json.dumps(dados_novos, ensure_ascii=False, default=str)
        
        # Buscar nome completo do usuário
        cursor.execute("SELECT nome_completo FROM usuarios WHERE id = %s", (session["user_id"],))
        usuario = cursor.fetchone()
        nome_completo = usuario['nome_completo'] if usuario else session.get("usuario", "Desconhecido")
        
        cursor.execute("""
            INSERT INTO logs_auditoria 
            (usuario_id, usuario_nome, acao, entidade, entidade_id, dados_anteriores, dados_novos, ip, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            session["user_id"],
            nome_completo,
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

def redimensionar_foto(caminho_origem, tamanho=(300, 300)):
    try:
        from PIL import Image
        img = Image.open(caminho_origem)
        img.thumbnail(tamanho, Image.Resampling.LANCZOS)
        if img.mode in ('RGBA', 'LA'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
            img = background
        img.save(caminho_origem, 'JPEG', quality=85, optimize=True)
        return True
    except Exception as e:
        print(f"Erro ao redimensionar foto: {e}")
        return False

def enviar_whatsapp(numero, mensagem):
    try:
        numero_limpo = ''.join(filter(str.isdigit, numero))
        if len(numero_limpo) == 11:
            numero_limpo = '55' + numero_limpo
        elif len(numero_limpo) == 10:
            numero_limpo = '55' + numero_limpo
        mensagem_codificada = quote(mensagem)
        url = f"https://web.whatsapp.com/send?phone={numero_limpo}&text={mensagem_codificada}"
        webbrowser.open(url)
        return True
    except Exception as e:
        print(f"Erro ao abrir WhatsApp: {e}")
        return False
def verificar_permissao(usuario_id, permissao_chave):
    """Verifica se um usuário tem determinada permissão"""
    cursor, conn = get_db()
    
    # Buscar grau e tipo do usuário
    cursor.execute("SELECT grau_atual, tipo FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cursor.fetchone()
    
    if not usuario:
        return_connection(conn)
        return False
    
    # Admin tem todas as permissões
    if usuario['tipo'] == 'admin':
        return_connection(conn)
        return True
    
    # ✅ CORREÇÃO: Para graus >= 3, considerar como Mestre (grau 3)
    grau_original = usuario['grau_atual']
    if grau_original >= 3:
        grau_efetivo = 3  # Mestres (grau 3, 4, 5, 6...) têm as mesmas permissões
    else:
        grau_efetivo = grau_original
    
    # Buscar permissão pelo chave
    cursor.execute("SELECT id FROM permissoes WHERE chave = %s", (permissao_chave,))
    permissao = cursor.fetchone()
    
    if not permissao:
        return_connection(conn)
        return False
    
    permissao_id = permissao['id']
    
    # Verificar bloqueio específico do usuário
    try:
        cursor.execute("""
            SELECT permitido FROM permissoes_usuario 
            WHERE usuario_id = %s AND permissao_id = %s
        """, (usuario_id, permissao_id))
        bloqueio = cursor.fetchone()
        
        if bloqueio and bloqueio['permitido'] == 0:
            return_connection(conn)
            return False
    except:
        # Se a coluna não existir, tentar com 'tipo'
        try:
            cursor.execute("""
                SELECT tipo FROM permissoes_usuario 
                WHERE usuario_id = %s AND permissao_id = %s
            """, (usuario_id, permissao_id))
            bloqueio = cursor.fetchone()
            
            if bloqueio and bloqueio['tipo'] == 0:
                return_connection(conn)
                return False
        except:
            pass
    
    # Verificar permissão por grau (usando grau efetivo)
    cursor.execute("""
        SELECT 1 FROM permissoes_grau 
        WHERE grau_id = %s AND permissao_id = %s
    """, (grau_efetivo, permissao_id))
    tem_permissao = cursor.fetchone() is not None
    
    # Verificar permissão extra do usuário
    if not tem_permissao:
        try:
            cursor.execute("""
                SELECT 1 FROM permissoes_usuario 
                WHERE usuario_id = %s AND permissao_id = %s AND permitido = 1
            """, (usuario_id, permissao_id))
            tem_permissao = cursor.fetchone() is not None
        except:
            try:
                cursor.execute("""
                    SELECT 1 FROM permissoes_usuario 
                    WHERE usuario_id = %s AND permissao_id = %s AND tipo = 1
                """, (usuario_id, permissao_id))
                tem_permissao = cursor.fetchone() is not None
            except:
                pass
    
    return_connection(conn)
    return tem_permissao
    
def enviar_email_iniciacao_com_senha(email, nome, numero_placet, cim_numero, usuario, senha):
    """Envia e-mail de confirmação de iniciação com dados de acesso"""
    assunto = "🎉 Bem-vindo à ARLS Bicentenário - Sua Iniciação foi Registrada!"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Iniciação Registrada</title>
    </head>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <h2 style="color: #4f46e5;">🎉 Parabéns, {nome}!</h2>
            <p>Sua iniciação foi registrada com sucesso na ARLS Bicentenário.</p>
            <div style="background: #f3f4f6; padding: 15px; border-radius: 10px; margin: 20px 0;">
                <p><strong>📄 Placet de Iniciação:</strong> {numero_placet}</p>
                <p><strong>🆔 CIM (Carteira de Identidade Maçônica):</strong> {cim_numero}</p>
                <p><strong>👤 Usuário:</strong> {usuario}</p>
                <p><strong>🔑 Senha temporária:</strong> {senha}</p>
            </div>
            <p>Agora você é oficialmente um Obreiro da nossa Loja, no Grau de <strong>Aprendiz</strong>.</p>
            <p>Recomendamos que você acesse o sistema e altere sua senha no primeiro acesso.</p>
            <p>Em breve você receberá mais informações sobre as próximas sessões.</p>
            <hr>
            <small>ARLS Bicentenário - Loja Maçônica</small>
        </div>
    </body>
    </html>
    """
    
    return enviar_email_resend(email, assunto, html_content)    

def get_grau_efetivo(grau_atual):
    """Retorna o grau efetivo para permissões (1, 2 ou 3)"""
    if grau_atual == 1:
        return 1
    elif grau_atual == 2:
        return 2
    elif grau_atual >= 3:
        return 3  # Todos os graus >= 3 são tratados como Mestre
    return 3
    
def get_grau_nome(grau):
    """Retorna o nome do grau"""
    graus = {
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
    return graus.get(int(grau) if grau else 3, f"Grau Superior {grau}")

import secrets
from datetime import datetime, timedelta

def gerar_token_recuperacao(usuario_id):
    """Gera token único para recuperação de senha"""
    token = secrets.token_urlsafe(32)
    
    cursor, conn = get_db()
    expira_em = datetime.now() + timedelta(hours=24)
    
    cursor.execute("""
        INSERT INTO password_reset_tokens (usuario_id, token, expira_em)
        VALUES (%s, %s, %s)
    """, (usuario_id, token, expira_em))
    conn.commit()
    return_connection(conn)
    
    return token

def verificar_token_recuperacao(token):
    """Verifica se o token de recuperação é válido"""
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT usuario_id FROM password_reset_tokens 
        WHERE token = %s 
        AND expira_em > NOW()
        AND usado = FALSE
    """, (token,))
    
    result = cursor.fetchone()
    return_connection(conn)
    
    return result['usuario_id'] if result else None

def usar_token_recuperacao(token):
    """Marca token como usado"""
    cursor, conn = get_db()
    cursor.execute("UPDATE password_reset_tokens SET usado = TRUE WHERE token = %s", (token,))
    conn.commit()
    return_connection(conn)

# =============================
# FUNÇÕES DE NOTIFICAÇÕES
# =============================

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from io import BytesIO
import unicodedata

def remover_acentos(texto):
    """Remove acentos de um texto"""
    if not texto:
        return ""
    # Substituir caracteres acentuados manualmente
    mapa = {
        'á': 'a', 'à': 'a', 'ã': 'a', 'â': 'a', 'ä': 'a',
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
        'ó': 'o', 'ò': 'o', 'õ': 'o', 'ô': 'o', 'ö': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
        'ç': 'c', 'ñ': 'n',
        'Á': 'A', 'À': 'A', 'Ã': 'A', 'Â': 'A', 'Ä': 'A',
        'É': 'E', 'È': 'E', 'Ê': 'E', 'Ë': 'E',
        'Í': 'I', 'Ì': 'I', 'Î': 'I', 'Ï': 'I',
        'Ó': 'O', 'Ò': 'O', 'Õ': 'O', 'Ô': 'O', 'Ö': 'O',
        'Ú': 'U', 'Ù': 'U', 'Û': 'U', 'Ü': 'U',
        'Ç': 'C', 'Ñ': 'N'
    }
    for acentuado, sem_acento in mapa.items():
        texto = texto.replace(acentuado, sem_acento)
    return texto

def gerar_pdf_certificado(visitante):
    """Gera PDF do certificado de visita - Versão estável"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                           topMargin=2*cm, bottomMargin=2*cm,
                           leftMargin=2*cm, rightMargin=2*cm)
    
    styles = getSampleStyleSheet()
    story = []
    
    # Remover acentos
    nome = remover_acentos(visitante['nome'])
    data = visitante['reuniao_data'].strftime('%d/%m/%Y')
    hora = visitante['hora_inicio'].strftime('%H:%M')
    codigo = visitante['codigo_verificacao']
    loja_origem = remover_acentos(visitante.get('loja_origem', ''))
    loja_nome = "ARLS Bicentenário"
    cidade = "Brasilia - DF"
    
    # Estilos
    titulo_style = ParagraphStyle(
        'TituloStyle',
        parent=styles['Normal'],
        fontSize=22,
        alignment=1,
        spaceAfter=30,
        fontName='Helvetica-Bold'
    )
    
    texto_style = ParagraphStyle(
        'TextoStyle',
        parent=styles['Normal'],
        fontSize=14,
        alignment=1,
        spaceAfter=12,
        fontName='Helvetica'
    )
    
    nome_style = ParagraphStyle(
        'NomeStyle',
        parent=styles['Normal'],
        fontSize=18,
        alignment=1,
        spaceAfter=15,
        fontName='Helvetica-Bold'
    )
    
    small_style = ParagraphStyle(
        'SmallStyle',
        parent=styles['Normal'],
        fontSize=9,
        alignment=1,
        fontName='Helvetica'
    )
    
    # Conteúdo
    story.append(Paragraph("CERTIFICADO DE VISITA", titulo_style))
    story.append(Spacer(1, 0.5*cm))
    
    story.append(Paragraph("Certificamos que", texto_style))
    story.append(Paragraph(nome, nome_style))
    story.append(Spacer(1, 0.3*cm))
    
    story.append(Paragraph("visitou a Loja Maconica", texto_style))
    story.append(Paragraph(loja_nome, texto_style))
    story.append(Spacer(1, 0.3*cm))
    
    story.append(Paragraph(f"na reuniao realizada em {data}", texto_style))
    story.append(Paragraph(f"as {hora}", texto_style))
    story.append(Paragraph(f"na cidade de {cidade}", texto_style))
    story.append(Spacer(1, 0.3*cm))
    
    if loja_origem:
        story.append(Paragraph(f"Sendo integrante da Loja {loja_origem}", texto_style))
        story.append(Spacer(1, 0.5*cm))
    
    story.append(Spacer(1, 0.8*cm))
    story.append(Paragraph("_________________________________", texto_style))
    story.append(Paragraph("Veneravel Mestre", texto_style))
    story.append(Paragraph("ARLS Bicentenário", texto_style))
    story.append(Spacer(1, 0.5*cm))
    
    story.append(Paragraph(f"Codigo de verificacao: {codigo}", small_style))
    
    # Gerar PDF
    doc.build(story)
    buffer.seek(0)
    return buffer
    
def enviar_certificado_email(visitante):
    """Envia o certificado de visita por e-mail"""
    if not visitante.get('email'):
        return {'success': False, 'message': 'Visitante não tem e-mail cadastrado'}
    
    # Gerar PDF
    pdf_buffer = gerar_pdf_certificado(visitante)
    
    assunto = f"Certificado de Visita - ARLS Bicentenário"
    
    conteudo_html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; padding: 30px; text-align: center; border-radius: 10px;">
                <h1>📜 Certificado de Visita</h1>
            </div>
            <div style="background: white; padding: 30px; border-radius: 10px; margin-top: 20px;">
                <h2>Olá {visitante['nome']},</h2>
                <p>É com grande satisfação que lhe enviamos o certificado de sua visita à <strong>ARLS Bicentenário</strong>.</p>
                
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <p><strong>📅 Reunião:</strong> {visitante['reuniao_titulo']}</p>
                    <p><strong>📆 Data:</strong> {visitante['reuniao_data'].strftime('%d/%m/%Y')}</p>
                    <p><strong>⏰ Horário:</strong> {visitante['hora_inicio'].strftime('%H:%M')}</p>
                    <p><strong>📍 Local:</strong> {visitante.get('local', 'Brasília - DF')}</p>
                    <p><strong>🔑 Código de verificação:</strong> {visitante['codigo_verificacao']}</p>
                </div>
                
                <p>Em anexo, segue seu certificado de visita.</p>
                <p>Este certificado pode ser validado a qualquer momento através do nosso site.</p>
                
                <hr>
                <p style="font-size: 12px; color: #666;">
                    Certificado emitido pelo Sistema Maçônico da ARLS Bicentenário
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    try:
        if RESEND_API_KEY:
            import resend
            params = {
                "from": f"Sistema Maçônico <{EMAIL_FROM_DEFAULT}>",
                "to": [visitante['email']],
                "subject": assunto,
                "html": conteudo_html,
                "attachments": [
                    {
                        "filename": f"certificado_{visitante['codigo_verificacao']}.pdf",
                        "content": pdf_buffer.getvalue(),
                        "content_type": "application/pdf"
                    }
                ]
            }
            email = resend.Emails.send(params)
            return {'success': True, 'message': 'E-mail enviado com certificado!'}
        else:
            # Fallback: salvar PDF em arquivo
            filename = f"certificado_{visitante['codigo_verificacao']}.pdf"
            with open(filename, "wb") as f:
                f.write(pdf_buffer.getvalue())
            print(f"📧 Certificado salvo: {filename}")
            print(f"📧 E-mail seria enviado para: {visitante['email']}")
            return {'success': True, 'message': 'Certificado gerado (modo debug)'}
            
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")
        return {'success': False, 'message': str(e)}

def registrar_notificacao_sistema(usuario_id, titulo, mensagem, tipo='sistema', link=None):
    """Registra notificação no sistema"""
    try:
        cursor, conn = get_db()
        cursor.execute("""
            INSERT INTO notificacoes (usuario_id, titulo, mensagem, tipo, link, data_criacao, lida)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 0)
        """, (usuario_id, titulo, mensagem, tipo, link))
        conn.commit()
        return_connection(conn)
        return True
    except Exception as e:
        print(f"Erro ao registrar notificação: {e}")
        return False

def enviar_notificacao_reuniao_lembrete(participante, reuniao):
    """Envia lembrete de reunião para o dia anterior"""
    assunto = f"🔔 Lembrete: Reunião {reuniao['titulo']} - Amanhã"
    
    data_formatada = reuniao['data'].strftime('%d/%m/%Y')
    hora_formatada = reuniao['hora_inicio'].strftime('%H:%M') if reuniao['hora_inicio'] else 'Horário a confirmar'
    
    conteudo_html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; padding: 30px; text-align: center; border-radius: 10px;">
                <h1>🔔 Lembrete de Reunião</h1>
                <p>Não esqueça! Amanhã tem reunião</p>
            </div>
            <div style="background: white; padding: 30px; border-radius: 10px; margin-top: 20px;">
                <h2>Olá {participante['nome_completo']},</h2>
                <p>Você tem uma reunião marcada para <strong>AMANHÃ</strong>:</p>
                
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <p><strong>📌 Título:</strong> {reuniao['titulo']}</p>
                    <p><strong>📅 Data:</strong> {data_formatada}</p>
                    <p><strong>⏰ Horário:</strong> {hora_formatada}</p>
                    <p><strong>📍 Local:</strong> {reuniao['local'] or reuniao['loja_nome'] or 'Templo Maçônico'}</p>
                </div>
                
                <p>Por favor, confirme sua presença através do sistema.</p>
                <p>Atenciosamente,<br><strong>Sistema Maçônico</strong></p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Registrar notificação no sistema
    registrar_notificacao_sistema(
        participante['id'],
        assunto,
        f"Lembrete: Reunião {reuniao['titulo']} amanhã às {hora_formatada}",
        'reuniao_lembrete',
        f"/reunioes/{reuniao['id']}"
    )
    
    # Enviar e-mail
    if 'enviar_email_resend' in globals():
        enviar_email_resend(participante['email'], assunto, conteudo_html)

def verificar_reunioes_e_enviar_notificacoes():
    """Verifica reuniões do dia seguinte e envia notificações"""
    cursor, conn = get_db()
    
    try:
        hoje = datetime.now().date()
        amanha = hoje + timedelta(days=1)
        
        # Buscar reuniões agendadas para AMANHÃ
        cursor.execute("""
            SELECT r.*, l.nome as loja_nome
            FROM reunioes r
            LEFT JOIN lojas l ON r.loja_id = l.id
            WHERE r.status = 'agendada'
            AND r.data = %s
            ORDER BY r.hora_inicio
        """, (amanha,))
        
        reunioes_amanha = cursor.fetchall()
        
        total_notificados = 0
        
        for reuniao in reunioes_amanha:
            # Buscar participantes que receberão o lembrete
            cursor.execute("""
                SELECT u.id, u.nome_completo, u.email, 
                       COALESCE(nc.dias_antecedencia_reuniao, 1) as dias_antecedencia
                FROM usuarios u
                LEFT JOIN notificacoes_config nc ON u.id = nc.usuario_id
                WHERE u.ativo = 1 
                AND u.email IS NOT NULL 
                AND u.email != ''
            """)
            participantes = cursor.fetchall()
            
            for participante in participantes:
                # Verificar se o participante quer receber notificações com a antecedência configurada
                dias_antecedencia = participante.get('dias_antecedencia', 1)
                if dias_antecedencia >= 1:
                    enviar_notificacao_reuniao_lembrete(participante, reuniao)
                    total_notificados += 1
        
        return_connection(conn)
        return {
            'success': True,
            'reunioes_amanha': len(reunioes_amanha),
            'participantes_notificados': total_notificados
        }
        
    except Exception as e:
        print(f"Erro ao verificar reuniões: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            return_connection(conn)
        return {'success': False, 'error': str(e)}

def verificar_aniversarios_e_enviar_notificacoes():
    """Verifica aniversários do dia e envia notificações"""
    cursor, conn = get_db()
    
    try:
        hoje = datetime.now().date()
        
        # Buscar obreiros que fazem aniversário hoje
        cursor.execute("""
            SELECT u.id, u.nome_completo, u.email, u.telefone,
                   nc.notificar_aniversario_obreiro
            FROM usuarios u
            LEFT JOIN notificacoes_config nc ON u.id = nc.usuario_id
            WHERE EXTRACT(MONTH FROM u.data_nascimento) = %s
              AND EXTRACT(DAY FROM u.data_nascimento) = %s
              AND u.ativo = 1
              AND u.data_nascimento IS NOT NULL
        """, (hoje.month, hoje.day))
        
        obreiros_aniversariantes = cursor.fetchall()
        
        # Buscar familiares que fazem aniversário hoje
        cursor.execute("""
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
        """, (hoje.month, hoje.day))
        
        familiares_aniversariantes = cursor.fetchall()
        
        # Enviar notificações para obreiros aniversariantes
        for obreiro in obreiros_aniversariantes:
            if obreiro.get('notificar_aniversario_obreiro', 1):
                titulo = f"🎂 Feliz Aniversário, {obreiro['nome_completo']}!"
                mensagem = f"Neste dia especial, toda a Loja Maçônica celebra sua vida. Que a Sabedoria, Força e Beleza continuem guiando seus passos."
                
                registrar_notificacao_sistema(
                    obreiro['id'], 
                    titulo, 
                    mensagem, 
                    'aniversario_obreiro',
                    '/obreiros/perfil'
                )
                
                if obreiro.get('email'):
                    enviar_email_aniversario_obreiro(obreiro['email'], obreiro['nome_completo'])
        
        # Enviar notificações para obreiros sobre aniversário de familiares
        for familiar in familiares_aniversariantes:
            if familiar.get('notificar_aniversario_familiar', 1):
                titulo = f"🎂 Aniversário do Familiar: {familiar['nome']}"
                mensagem = f"Hoje é aniversário de {familiar['nome']} ({familiar['grau_parentesco']}). Que este dia seja repleto de alegria para sua família."
                
                registrar_notificacao_sistema(
                    familiar['obreiro_id'], 
                    titulo, 
                    mensagem, 
                    'aniversario_familiar',
                    '/familiares'
                )
                
                if familiar.get('obreiro_email'):
                    enviar_email_aniversario_familiar(
                        familiar['obreiro_email'], 
                        familiar['obreiro_nome'], 
                        familiar['nome'], 
                        familiar['grau_parentesco']
                    )
        
        return_connection(conn)
        return {
            'success': True,
            'obreiro_aniversariantes': len(obreiros_aniversariantes),
            'familiar_aniversariantes': len(familiares_aniversariantes)
        }
        
    except Exception as e:
        print(f"Erro ao verificar aniversários: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
        return_connection(conn)
        return {'success': False, 'error': str(e)}

def enviar_email_aniversario_obreiro(email, nome):
    """Envia e-mail de aniversário para obreiro"""
    assunto = f"🎂 Feliz Aniversário, {nome}!"
    
    conteudo_html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; padding: 30px; text-align: center; border-radius: 10px;">
                <h1>🎂 Feliz Aniversário!</h1>
            </div>
            <div style="background: white; padding: 30px; border-radius: 10px; margin-top: 20px;">
                <h2>Querido {nome},</h2>
                <p>Neste dia especial, toda a Loja Maçônica se une para celebrar sua vida e sua jornada.</p>
                <p>Que a Sabedoria, Força e Beleza continuem guiando seus passos.</p>
                <p style="margin-top: 30px;">Com fraternal abraço,<br><strong>Sistema Maçônico</strong></p>
            </div>
        </div>
    </body>
    </html>
    """
    
    if 'enviar_email_resend' in globals():
        enviar_email_resend(email, assunto, conteudo_html)

def enviar_email_aniversario_familiar(email, obreiro_nome, familiar_nome, parentesco):
    """Envia e-mail de aniversário de familiar para obreiro"""
    assunto = f"🎂 Aniversário do Familiar: {familiar_nome}"
    
    conteudo_html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; padding: 30px; text-align: center; border-radius: 10px;">
                <h1>🎂 Aniversário de Familiar</h1>
            </div>
            <div style="background: white; padding: 30px; border-radius: 10px; margin-top: 20px;">
                <h2>Querido {obreiro_nome},</h2>
                <p>Hoje é aniversário de <strong>{familiar_nome}</strong> ({parentesco}).</p>
                <p>Que este dia seja repleto de alegria e realizações para sua família.</p>
                <p style="margin-top: 30px;">Com fraternal abraço,<br><strong>Sistema Maçônico</strong></p>
            </div>
        </div>
    </body>
    </html>
    """
    
    if 'enviar_email_resend' in globals():
        enviar_email_resend(email, assunto, conteudo_html)
        
def get_email_config():
    """Busca a configuração de e-mail ativa do banco"""
    try:
        cursor, conn = get_db()
        cursor.execute("""
            SELECT sender, sender_name, active 
            FROM email_config 
            WHERE active = 1 
            LIMIT 1
        """)
        config = cursor.fetchone()
        return_connection(conn)
        
        if config:
            return {
                'sender': config['sender'],
                'sender_name': config['sender_name'] or 'Sistema Maçônico'
            }
        else:
            # Configuração padrão
            return {
                'sender': 'contato@juramelo.com.br',
                'sender_name': 'Sistema Maçônico'
            }
    except Exception as e:
        print(f"Erro ao buscar config de e-mail: {e}")
        return {
            'sender': 'contato@juramelo.com.br',
            'sender_name': 'Sistema Maçônico'
        }      

@app.route("/diagnostico-email")
@login_required
def diagnostico_email():
    """Rota para diagnosticar problemas de e-mail"""
    if session.get('tipo') != 'admin':
        return "Acesso negado", 403
    
    resultados = {}
    
    # 1. Verificar configuração do Resend
    import os
    import resend
    
    resultados['resend_api_key'] = '✅ Configurada' if os.environ.get("RESEND_API_KEY") else '❌ NÃO CONFIGURADA'
    resultados['resend_key_length'] = len(os.environ.get("RESEND_API_KEY", ''))
    
    # 2. Verificar configuração no banco
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT * FROM email_config WHERE active = 1")
        config = cursor.fetchone()
        if config:
            resultados['email_config'] = {
                'sender': config.get('sender'),
                'sender_name': config.get('sender_name'),
                'active': config.get('active')
            }
        else:
            resultados['email_config'] = '❌ Nenhuma configuração ativa no banco'
        return_connection(conn)
    except Exception as e:
        resultados['email_config'] = f'Erro: {str(e)}'
    
    # 3. Verificar tabelas necessárias
    try:
        cursor, conn = get_db()
        cursor.execute("SHOW TABLES LIKE 'password_reset_tokens'")
        tabela_tokens = cursor.fetchone()
        resultados['tabela_password_reset_tokens'] = '✅ Existe' if tabela_tokens else '❌ NÃO EXISTE'
        
        cursor.execute("SHOW TABLES LIKE 'email_logs'")
        tabela_logs = cursor.fetchone()
        resultados['tabela_email_logs'] = '✅ Existe' if tabela_logs else '⚠️ Não existe (opcional)'
        return_connection(conn)
    except Exception as e:
        resultados['tabelas'] = f'Erro: {str(e)}'
    
    # 4. Testar envio de e-mail simples
    try:
        resend.api_key = os.environ.get("RESEND_API_KEY")
        test_params = {
            "from": "onboarding@resend.dev",  # Domínio de teste do Resend
            "to": ["seu-email@teste.com"],  # Substitua por um e-mail real para teste
            "subject": "Teste Diagnóstico",
            "html": "<p>Teste</p>"
        }
        # Não enviar realmente, apenas verificar se a configuração está OK
        resultados['resend_config'] = '✅ Configuração OK'
    except Exception as e:
        resultados['resend_config'] = f'❌ Erro: {str(e)}'
    
    return jsonify(resultados)        

def executar_rotinas_diarias():
    """Executa todas as rotinas diárias (aniversários e lembretes)"""
    print(f"\n{'='*50}")
    print(f"🔄 Executando rotinas diárias em {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*50}")
    
    # Verificar aniversários (mesmo dia)
    print("📅 Verificando aniversários...")
    resultado_aniversarios = verificar_aniversarios_e_enviar_notificacoes()
    
    # Verificar reuniões (dia anterior)
    print("📆 Verificando reuniões de amanhã...")
    resultado_reunioes = verificar_reunioes_e_enviar_notificacoes()
    
    print(f"\n✅ Rotinas concluídas!")
    print(f"   - Aniversários: {resultado_aniversarios.get('obreiro_aniversariantes', 0)} obreiros, {resultado_aniversarios.get('familiar_aniversariantes', 0)} familiares")
    print(f"   - Reuniões amanhã: {resultado_reunioes.get('reunioes_amanha', 0)} reuniões, {resultado_reunioes.get('participantes_notificados', 0)} notificações")
    print(f"{'='*50}\n")
    
    return {
        'aniversarios': resultado_aniversarios,
        'reunioes': resultado_reunioes
    }

# =====================
# FUNÇÕES DE CARGOS
# =====================
def pode_ocupar_cargo(obreiro_id, cargo_id):
    """Verifica se um obreiro pode ocupar um determinado cargo baseado no grau"""
    cursor, conn = get_db()
    
    try:
        # Buscar grau do obreiro
        cursor.execute("SELECT grau_atual FROM usuarios WHERE id = %s", (obreiro_id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            return False, "Obreiro não encontrado"
        
        # Buscar grau mínimo do cargo
        cursor.execute("SELECT nome, grau_minimo FROM cargos WHERE id = %s", (cargo_id,))
        cargo = cursor.fetchone()
        
        if not cargo:
            return False, "Cargo não encontrado"
        
        grau_minimo = cargo["grau_minimo"] if cargo["grau_minimo"] else 3
        
        if obreiro["grau_atual"] < grau_minimo:
            return False, f"Grau mínimo exigido: {grau_minimo}º. Obreiro tem grau {obreiro['grau_atual']}º"
        
        return True, "OK"
        
    except Exception as e:
        return False, str(e)
    finally:
        return_connection(conn)


# =====================
# FUNÇÕES DE GRAU
# =====================

def get_grau_principal(grau_nivel):
    """Retorna a classificação principal do grau (para exibição em listas)"""
    if grau_nivel == 1:
        return "Aprendiz"
    elif grau_nivel == 2:
        return "Companheiro"
    elif grau_nivel >= 3:
        return "Mestre"
    else:
        return "Mestre"

def get_grau_detalhado(grau_nivel):
    """Retorna o nome detalhado do grau (para tooltips)"""
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
    """Retorna a descrição do grau (mantido para compatibilidade)"""
    if grau == 1:
        return "Aprendiz"
    elif grau == 2:
        return "Companheiro"
    elif grau == 3:
        return "Mestre"
    elif grau == 4:
        return "Mestre Instalado"
    elif grau == 5:
        return "Mestre Inst. (5°)"
    elif grau == 6:
        return "Grau 6 - Superior"
    elif grau >= 7:
        return f"Grau Superior {grau}"
    else:
        return "Mestre"

def get_grau_badge_class(grau_nivel):
    """Retorna a classe CSS para o badge do grau"""
    if grau_nivel == 1:
        return 'bg-secondary'
    elif grau_nivel == 2:
        return 'bg-primary'
    elif grau_nivel == 3:
        return 'bg-warning text-dark'
    elif grau_nivel >= 4:
        return 'bg-info'
    else:
        return 'bg-secondary'

def get_grau_icon(grau_nivel):
    """Retorna o ícone para o grau"""
    if grau_nivel == 1 or grau_nivel == 2:
        return 'bi bi-star'
    elif grau_nivel >= 3:
        return 'bi bi-star-fill'
    else:
        return 'bi bi-star'
# =============================
# FUNÇÕES DE PERMISSÃO
# =============================

def verificar_permissao(usuario_id, permissao_chave):
    """Verifica se um usuário tem determinada permissão"""
    cursor, conn = get_db()
    
    try:
        # Buscar grau e tipo do usuário
        cursor.execute("SELECT grau_atual, tipo FROM usuarios WHERE id = %s", (usuario_id,))
        usuario = cursor.fetchone()
        
        if not usuario:
            return_connection(conn)
            return False
        
        # Admin tem todas as permissões
        if usuario['tipo'] == 'admin':
            return_connection(conn)
            return True
        
        # Para graus >= 3, considerar como Mestre (grau 3)
        grau_original = usuario['grau_atual']
        if grau_original >= 3:
            grau_efetivo = 3
        else:
            grau_efetivo = grau_original
        
        # Buscar permissão pelo chave
        cursor.execute("SELECT id FROM permissoes WHERE chave = %s", (permissao_chave,))
        permissao = cursor.fetchone()
        
        if not permissao:
            return_connection(conn)
            return False
        
        permissao_id = permissao['id']
        
        # Verificar permissão por grau
        cursor.execute("""
            SELECT 1 FROM permissoes_grau 
            WHERE grau_id = %s AND permissao_id = %s
        """, (grau_efetivo, permissao_id))
        tem_permissao = cursor.fetchone() is not None
        
        return_connection(conn)
        return tem_permissao
        
    except Exception as e:
        print(f"Erro ao verificar permissão: {e}")
        if conn:
            return_connection(conn)
        return False


def tem_permissao(permissao_chave):
    """Verifica se o usuário logado tem determinada permissão"""
    if 'user_id' not in session:
        return False
    return verificar_permissao(session['user_id'], permissao_chave)


def permissao_required(permissao_chave):
    """Decorador para verificar permissão"""
    from functools import wraps
    
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                flash("Você precisa estar logado para acessar esta página.", "danger")
                return redirect(url_for('login'))
            
            if tem_permissao(permissao_chave):
                return f(*args, **kwargs)
            else:
                flash("Você não tem permissão para acessar esta página.", "danger")
                return redirect(url_for('dashboard'))
        return decorated_function
    return decorator

# =============================
# CONTEXTO GLOBAL
# =============================
@app.context_processor
def inject_global():
    return {
        'datetime': datetime, 
        'now': datetime.now(), 
        'tem_permissao': tem_permissao,
        'verificar_permissao': verificar_permissao,
        'get_grau_principal': get_grau_principal,
        'get_grau_detalhado': get_grau_detalhado,
        'get_grau_nome': get_grau_nome,
        'get_grau_badge_class': get_grau_badge_class,
        'get_grau_icon': get_grau_icon
    }

@app.context_processor
def inject_permissions():
    def tem_permissao(codigo):
        if 'user_id' not in session:
            return False
        if session.get('tipo') == 'admin':
            return True
        return _verificar_permissao_db(codigo)
    return {'tem_permissao': tem_permissao}

@app.template_filter('markdown')

def render_markdown(text):
    if not text:
        return ''
    try:
        html = markdown.markdown(text, extensions=['extra', 'codehilite', 'tables', 'fenced_code', 'nl2br'])
        return html
    except Exception as e:
        print(f"Erro ao converter markdown: {e}")
        return text.replace('\n', '<br>')

# =============================
# ROTAS PÚBLICAS
# =============================
@app.route("/")
def home():
    return render_template("public/index.html")

@app.route("/sobre")
def sobre():
    return render_template("public/sobre.html")

@app.route("/calendario")
def calendario_publico():
    cursor, conn = get_db()
    cursor.execute("""
        SELECT titulo, data, hora_inicio, local 
        FROM reunioes 
        WHERE status = 'agendada' AND data >= CURRENT_DATE
        ORDER BY data ASC
        LIMIT 10
    """)
    eventos = cursor.fetchall()
    return_connection(conn)
    return render_template("public/calendario.html", eventos=eventos)

@app.route("/contato")
def contato():
    return render_template("public/contato.html")

@app.route("/noticias")
def noticias():
    cursor, conn = get_db()
    cursor.execute("""
        SELECT titulo, conteudo, data_criacao 
        FROM comunicados 
        WHERE ativo = 1 AND (tipo = 'publico' OR tipo = 'informativo')
        ORDER BY data_criacao DESC
        LIMIT 10
    """)
    noticias = cursor.fetchall()
    return_connection(conn)
    return render_template("public/noticias.html", noticias=noticias)

@app.route("/galeria")
def galeria():
    return render_template("public/galeria.html")



# =============================
# ROTAS DE AUTENTICAÇÃO
# =============================
@app.route("/login", methods=["GET", "POST"])
def login():
    session.pop('_flashes', None)
    if request.method == "POST":
        usuario = request.form.get("usuario")
        senha = request.form.get("senha")
        
        # Validar campos vazios
        if not usuario or not senha:
            flash('❌ Preencha usuário e senha!', 'danger')
            return render_template("login.html")
        
        cursor, conn = get_db()
        cursor.execute("""
            SELECT id, usuario, senha_hash, tipo, grau_atual, nome_completo
            FROM usuarios
            WHERE usuario = %s AND ativo = 1
        """, (usuario,))
        user = cursor.fetchone()
        return_connection(conn)
        
        # Verificar senha
        if user and check_password_hash(user['senha_hash'], senha):
            session['usuario_id'] = user['id']
            session['usuario'] = user['usuario']
            session['tipo'] = user['tipo']
            session['grau_atual'] = user['grau_atual']
            session['nome_completo'] = user['nome_completo']
            session['user_id'] = user['id']
            session['nivel_acesso'] = user.get('nivel_acesso', 1)
            
            flash(f'✅ Bem-vindo, {user["nome_completo"]}!', 'success')
            return redirect(url_for('dashboard'))
        else:
            # Mensagem de erro clara
            flash('❌ Usuário ou senha incorretos! Tente novamente.', 'danger')
            return render_template("login.html")
    
    return render_template("login.html")
    
@app.route("/logout")
@login_required
def logout():
    session.pop('_flashes', None)
    """Faz logout do usuário"""
    session.clear()
    flash("Você saiu do sistema com sucesso!", "success")
    return redirect("/")

# =============================
# ROTAS DE CRIAR TABELAS DO WHATSAPP
# =============================

@app.route("/admin/criar-tabelas-whatsapp")
@admin_required
def admin_criar_tabelas_whatsapp():
    if init_whatsapp_tables():
        flash("✅ Tabelas do WhatsApp criadas com sucesso!", "success")
    else:
        flash("❌ Erro ao criar tabelas do WhatsApp", "danger")
    return redirect("/dashboard")
    

# ============================================
# ROTA PARA ACESSO DO CANDIDATO
# ============================================
@app.route("/candidatos/<int:candidato_id>/gerar-link")
@admin_required
def gerar_link_candidato(candidato_id):
    """Gera link de acesso para o candidato"""
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT id, nome, token_acesso
        FROM candidatos 
        WHERE id = %s
    """, (candidato_id,))
    candidato = cursor.fetchone()
    
    return_connection(conn)
    
    if not candidato or not candidato['token_acesso']:
        flash("Candidato ou token não encontrado!", "danger")
        return redirect(f"/candidatos/{candidato_id}/documentos")
    
    link = f"https://www.juramelo.com.br/candidato/acesso/{candidato['token_acesso']}"
    
    return jsonify({
        'link': link,
        'token': candidato['token_acesso'],
        'candidato': candidato['nome']
    })

@app.route("/candidato/acesso/<token>")
def candidato_acesso(token):
    """Página de acesso do candidato via token"""
    print(f"🔍 ROTA ACIONADA - Token: {token}")
    
    cursor, conn = get_db()
    
    try:
        # Buscar candidato pelo token
        cursor.execute("""
            SELECT id, nome, status, token_acesso, email
            FROM candidatos 
            WHERE token_acesso = %s
        """, (token,))
        candidato = cursor.fetchone()
        
        print(f"🔍 Candidato encontrado: {candidato is not None}")
        
        if not candidato:
            flash("Link inválido ou candidato não encontrado!", "danger")
            return redirect("/")
        
        print(f"✅ Candidato: {candidato['nome']} - ID: {candidato['id']}")
        
        return_connection(conn)
        
        return render_template("candidatos/acesso.html", candidato=candidato)
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            return_connection(conn)
        flash("Erro ao acessar o link. Tente novamente.", "danger")
        return redirect("/")

@app.route("/candidato/<int:candidato_id>/documentos")
def candidato_documentos_externo(candidato_id):
    """Página de documentos para o candidato (acesso externo)"""
    token = request.args.get('token')
    
    if not token:
        flash("Token de acesso não informado!", "danger")
        return redirect("/")
    
    cursor, conn = get_db()
    
    try:
        cursor.execute("""
            SELECT id, nome, status, token_acesso, email
            FROM candidatos 
            WHERE id = %s AND token_acesso = %s
        """, (candidato_id, token))
        candidato = cursor.fetchone()
        
        if not candidato:
            flash("Acesso não autorizado!", "danger")
            return redirect("/")
        
        # Buscar tipos de documentos
        cursor.execute("""
            SELECT id, nome, descricao, obrigatorio, ordem
            FROM tipos_documentos_candidato
            WHERE ativo = 1
            ORDER BY ordem
        """)
        tipos_documentos = cursor.fetchall()
        
        # Buscar documentos já enviados
        cursor.execute("""
            SELECT * FROM documentos_candidato
            WHERE candidato_id = %s
        """, (candidato_id,))
        documentos = cursor.fetchall()
        
        documentos_map = {doc['tipo_documento_id']: doc for doc in documentos}
        
        total_obrigatorios = sum(1 for t in tipos_documentos if t['obrigatorio'] == 1)
        total_enviados = sum(1 for t in tipos_documentos 
                            if t['id'] in documentos_map and documentos_map[t['id']]['status'] == 'aprovado')
        
        percentual = int((total_enviados / total_obrigatorios * 100)) if total_obrigatorios > 0 else 0
        
        return_connection(conn)
        
        return render_template("candidatos/documentos_externo.html",
                              candidato=candidato,
                              tipos_documentos=tipos_documentos,
                              documentos_map=documentos_map,
                              total_obrigatorios=total_obrigatorios,
                              total_enviados=total_enviados,
                              percentual=percentual)
        
    except Exception as e:
        print(f"Erro: {e}")
        if conn:
            return_connection(conn)
        flash("Erro ao carregar página.", "danger")
        return redirect("/")


@app.route("/candidato/documentos/upload/<int:candidato_id>/<int:tipo_id>", methods=["POST"])
def candidato_upload_documento_externo(candidato_id, tipo_id):
    """Upload de documento pelo candidato"""
    token = request.form.get('token')
    
    if not token:
        flash("Token não informado!", "danger")
        return redirect("/")
    
    if 'arquivo' not in request.files:
        flash("Nenhum arquivo selecionado!", "danger")
        return redirect(f"/candidato/{candidato_id}/documentos?token={token}")
    
    arquivo = request.files['arquivo']
    
    if arquivo.filename == '':
        flash("Nenhum arquivo selecionado!", "danger")
        return redirect(f"/candidato/{candidato_id}/documentos?token={token}")
    
    extensao = arquivo.filename.rsplit('.', 1)[1].lower() if '.' in arquivo.filename else ''
    allowed_extensions = ['pdf', 'jpg', 'jpeg', 'png']
    
    if extensao not in allowed_extensions:
        flash(f"Tipo de arquivo não permitido. Use: {', '.join(allowed_extensions)}", "danger")
        return redirect(f"/candidato/{candidato_id}/documentos?token={token}")
    
    cursor, conn = get_db()
    
    try:
        cursor.execute("""
            SELECT id, nome, token_acesso
            FROM candidatos 
            WHERE id = %s AND token_acesso = %s
        """, (candidato_id, token))
        candidato = cursor.fetchone()
        
        if not candidato:
            flash("Acesso não autorizado!", "danger")
            return redirect("/")
        
        cursor.execute("SELECT nome FROM tipos_documentos_candidato WHERE id = %s", (tipo_id,))
        tipo_doc = cursor.fetchone()
        
        if not tipo_doc:
            flash("Tipo de documento inválido!", "danger")
            return redirect(f"/candidato/{candidato_id}/documentos?token={token}")
        
        import cloudinary.uploader
        from werkzeug.utils import secure_filename
        
        resource_type = "raw" if extensao == 'pdf' else "image"
        
        upload_result = cloudinary.uploader.upload(
            arquivo,
            folder=f"candidatos/{candidato_id}/documentos",
            resource_type=resource_type,
            type="upload",
            access_mode="public"
        )
        
        url_arquivo = upload_result.get('secure_url')
        public_id = upload_result.get('public_id')
        tamanho = upload_result.get('bytes', 0)
        
        if extensao == 'pdf' and '/image/' in url_arquivo:
            url_arquivo = url_arquivo.replace('/image/', '/raw/')
        
        cursor.execute("""
            SELECT id FROM documentos_candidato 
            WHERE candidato_id = %s AND tipo_documento_id = %s
        """, (candidato_id, tipo_id))
        
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute("""
                UPDATE documentos_candidato SET
                    nome_arquivo = %s,
                    caminho_arquivo = %s,
                    tipo_arquivo = %s,
                    tamanho = %s,
                    status = 'pendente',
                    data_envio = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (public_id, url_arquivo, extensao, tamanho, existing['id']))
        else:
            cursor.execute("""
                INSERT INTO documentos_candidato 
                (candidato_id, tipo_documento_id, nome_arquivo, caminho_arquivo, 
                 tipo_arquivo, tamanho, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'pendente')
            """, (candidato_id, tipo_id, public_id, url_arquivo, extensao, tamanho))
        
        conn.commit()
        flash(f"Documento '{tipo_doc['nome']}' enviado com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro: {e}")
        flash(f"Erro ao enviar: {str(e)}", "danger")
        if conn:
            conn.rollback()
    
    return_connection(conn)
    return redirect(f"/candidato/{candidato_id}/documentos?token={token}")
    
    
# =============================
# ROTAS DA BIBLIOTECA
# =============================

@app.route("/biblioteca/admin/upload", methods=['GET', 'POST'])
@login_required
@permissao_required('material.create')
def upload_material():
    """Upload de novos materiais para o Cloudinary"""
    
    if request.method == 'POST':
        # Coletar dados do formulário
        titulo = request.form.get('titulo')
        subtitulo = request.form.get('subtitulo')
        descricao = request.form.get('descricao')
        tipo = request.form.get('tipo')
        categoria_id = request.form.get('categoria_id')
        grau_acesso = request.form.get('grau_acesso')
        autor = request.form.get('autor')
        editora = request.form.get('editora')
        ano_publicacao = request.form.get('ano_publicacao')
        num_paginas = request.form.get('num_paginas')
        isbn = request.form.get('isbn')
        tags = request.form.get('tags')
        destaque = request.form.get('destaque') == 'on'
        
        # Validar campos obrigatórios
        if not titulo:
            flash('O título é obrigatório', 'danger')
            return redirect(url_for('upload_material'))
        
        if not tipo:
            flash('O tipo de material é obrigatório', 'danger')
            return redirect(url_for('upload_material'))
        
        if not categoria_id:
            flash('A categoria é obrigatória', 'danger')
            return redirect(url_for('upload_material'))
        
        if not grau_acesso:
            flash('O grau de acesso é obrigatório', 'danger')
            return redirect(url_for('upload_material'))
        
        # Processar arquivos
        arquivo = request.files.get('arquivo')
        capa = request.files.get('capa')
        
        if not arquivo or arquivo.filename == '':
            flash('Selecione um arquivo para upload', 'danger')
            return redirect(url_for('upload_material'))
        
        # Validar tamanho do arquivo (50MB)
        arquivo.seek(0, 2)
        tamanho_arquivo = arquivo.tell()
        arquivo.seek(0)
        
        if tamanho_arquivo > 50 * 1024 * 1024:
            flash('Arquivo muito grande! O tamanho máximo é 50MB.', 'danger')
            return redirect(url_for('upload_material'))
        
        try:
            # Upload do arquivo principal
            import uuid
            from werkzeug.utils import secure_filename
            
            nome_arquivo = f"{uuid.uuid4().hex}_{secure_filename(arquivo.filename)}"
            
            upload_result = cloudinary.uploader.upload(
                arquivo,
                folder="biblioteca_maconica/materiais",
                public_id=nome_arquivo,
                resource_type="auto",
                use_filename=True,
                unique_filename=False
            )
            
            arquivo_url = upload_result.get('secure_url')
            arquivo_tamanho = upload_result.get('bytes')
            formato = upload_result.get('format')
            arquivo_nome_original = arquivo.filename
            
            # Upload da capa (se houver)
            capa_url = None
            if capa and capa.filename:
                capa_result = cloudinary.uploader.upload(
                    capa,
                    folder="biblioteca_maconica/capas",
                    resource_type="image",
                    transformation=[
                        {'width': 300, 'height': 400, 'crop': 'fill'},
                        {'quality': 'auto'}
                    ]
                )
                capa_url = capa_result.get('secure_url')
            
            # Inserir no banco
            cursor, conn = get_db()
            
            ano_publicacao_int = None
            if ano_publicacao and ano_publicacao.strip():
                try:
                    ano_publicacao_int = int(ano_publicacao)
                except:
                    pass
            
            num_paginas_int = None
            if num_paginas and num_paginas.strip():
                try:
                    num_paginas_int = int(num_paginas)
                except:
                    pass
            
            cursor.execute("""
                INSERT INTO materiais (
                    titulo, subtitulo, descricao, tipo, categoria_id, grau_acesso,
                    arquivo_url, arquivo_nome, arquivo_tamanho, formato, capa_url,
                    autor, editora, ano_publicacao, num_paginas, isbn, tags, 
                    destaque, publicado, created_by, created_at, data_publicacao
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, true, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING id
            """, (
                titulo, subtitulo, descricao, tipo, categoria_id, grau_acesso,
                arquivo_url, arquivo_nome_original, arquivo_tamanho, formato, capa_url,
                autor, editora, ano_publicacao_int, num_paginas_int, isbn, tags,
                destaque, session['user_id']
            ))
            
            material_id = cursor.fetchone()['id']
            conn.commit()
            return_connection(conn)
            
            registrar_log("criar", "material", material_id, dados_novos={"titulo": titulo})
            
            flash(f'✅ Material "{titulo}" enviado com sucesso!', 'success')
            return redirect(url_for('visualizar_material', material_id=material_id))
            
        except cloudinary.exceptions.Error as e:
            print(f"❌ Erro no Cloudinary: {e}")
            traceback.print_exc()
            flash(f'Erro no envio para a nuvem: {str(e)}', 'danger')
            return redirect(url_for('upload_material'))
        
        except Exception as e:
            print(f"❌ Erro no upload: {e}")
            traceback.print_exc()
            flash(f'Erro ao enviar arquivo: {str(e)}', 'danger')
            return redirect(url_for('upload_material'))
    
    # GET - mostrar formulário
    cursor, conn = get_db()
    cursor.execute("SELECT * FROM categorias_material ORDER BY ordem, nome")
    categorias = cursor.fetchall()
    return_connection(conn)
    
    return render_template('biblioteca/upload.html', categorias=categorias)

@app.route("/biblioteca/admin/editar/<int:material_id>", methods=['GET', 'POST'])
@login_required
@permissao_required('material.edit')
def editar_material(material_id):
    """Editar um material existente"""
    cursor, conn = get_db()
    
    if request.method == 'POST':
        # Coletar dados do formulário
        titulo = request.form.get('titulo')
        subtitulo = request.form.get('subtitulo')
        descricao = request.form.get('descricao')
        tipo = request.form.get('tipo')
        categoria_id = request.form.get('categoria_id')
        grau_acesso = request.form.get('grau_acesso')
        autor = request.form.get('autor')
        editora = request.form.get('editora')
        
        # Tratar campos numéricos
        ano_publicacao = request.form.get('ano_publicacao')
        if ano_publicacao and ano_publicacao.strip():
            try:
                ano_publicacao = int(ano_publicacao)
            except ValueError:
                ano_publicacao = None
        else:
            ano_publicacao = None
        
        num_paginas = request.form.get('num_paginas')
        if num_paginas and num_paginas.strip():
            try:
                num_paginas = int(num_paginas)
            except ValueError:
                num_paginas = None
        else:
            num_paginas = None
        
        isbn = request.form.get('isbn')
        tags = request.form.get('tags')
        
        # ✅ CORREÇÃO: Usar BOOLEAN (True/False) em vez de INTEGER (1/0)
        destaque = True if request.form.get('destaque') else False
        publicado = True if request.form.get('publicado') else False
        
        try:
            cursor.execute("""
                UPDATE materiais SET
                    titulo = %s,
                    subtitulo = %s,
                    descricao = %s,
                    tipo = %s,
                    categoria_id = %s,
                    grau_acesso = %s,
                    autor = %s,
                    editora = %s,
                    ano_publicacao = %s,
                    num_paginas = %s,
                    isbn = %s,
                    tags = %s,
                    destaque = %s,
                    publicado = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (titulo, subtitulo, descricao, tipo, categoria_id, grau_acesso,
                  autor, editora, ano_publicacao, num_paginas, isbn, tags,
                  destaque, publicado, material_id))
            
            conn.commit()
            flash('Material atualizado com sucesso!', 'success')
            return redirect(url_for('visualizar_material', material_id=material_id))
            
        except Exception as e:
            print(f"Erro ao editar: {e}")
            conn.rollback()
            flash(f'Erro ao editar: {str(e)}', 'danger')
            return redirect(url_for('editar_material', material_id=material_id))
    
    # GET - carregar dados do material
    cursor.execute("""
        SELECT m.*, c.nome as categoria_nome
        FROM materiais m
        LEFT JOIN categorias_material c ON m.categoria_id = c.id
        WHERE m.id = %s
    """, (material_id,))
    material = cursor.fetchone()
    
    cursor.execute("SELECT * FROM categorias_material ORDER BY nome")
    categorias = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template('biblioteca/editar.html', material=material, categorias=categorias)
    
@app.route("/biblioteca/admin/excluir/<int:material_id>", methods=['POST'])
@login_required
@permissao_required('material.delete')
def excluir_material(material_id):
    """Excluir um material"""
    cursor, conn = get_db()
    
    try:
        # Buscar material para log
        cursor.execute("SELECT titulo FROM materiais WHERE id = %s", (material_id,))
        material = cursor.fetchone()
        
        if not material:
            flash('Material não encontrado', 'danger')
            return redirect(url_for('listar_materiais'))
        
        # Excluir registros relacionados
        cursor.execute("DELETE FROM downloads_material WHERE material_id = %s", (material_id,))
        cursor.execute("DELETE FROM favoritos_material WHERE material_id = %s", (material_id,))
        cursor.execute("DELETE FROM avaliacoes_material WHERE material_id = %s", (material_id,))
        
        # Excluir material
        cursor.execute("DELETE FROM materiais WHERE id = %s", (material_id,))
        conn.commit()
        
        registrar_log("excluir", "material", material_id, dados_antigos={"titulo": material['titulo']})
        flash(f'Material "{material["titulo"]}" excluído com sucesso!', 'success')
        
    except Exception as e:
        print(f"Erro ao excluir: {e}")
        conn.rollback()
        flash(f'Erro ao excluir: {str(e)}', 'danger')
    
    return_connection(conn)
    return redirect(url_for('listar_materiais'))
    
@app.route("/biblioteca/categoria/<int:categoria_id>")
@login_required
@permissao_required('material.view')
def materiais_por_categoria(categoria_id):
    """Materiais de uma categoria específica"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT m.*, c.nome as categoria_nome, c.cor as categoria_cor
            FROM materiais m
            LEFT JOIN categorias_material c ON m.categoria_id = c.id
            WHERE m.categoria_id = %s AND m.publicado = true
            ORDER BY m.data_publicacao DESC
        """, (categoria_id,))
        materiais = cursor.fetchall()
        
        cursor.execute("SELECT * FROM categorias_material WHERE id = %s", (categoria_id,))
        categoria = cursor.fetchone()
        
        return_connection(conn)
        
        if not categoria:
            flash('Categoria não encontrada', 'danger')
            return redirect(url_for('listar_materiais'))
        
        return render_template('biblioteca/categoria.html',
                             materiais=materiais,
                             categoria=categoria)
    
    except Exception as e:
        print(f"❌ Erro ao carregar categoria: {e}")
        if 'conn' in locals():
            return_connection(conn)
        flash('Erro ao carregar a categoria', 'danger')
        return redirect(url_for('listar_materiais'))
    
    
@app.route("/biblioteca")
@app.route("/biblioteca/")
@login_required
@permissao_required('material.view')
def listar_materiais():
    """Página principal da biblioteca"""
    try:
        cursor, conn = get_db()
        
        # Buscar materiais publicados
        cursor.execute("""
            SELECT m.*, 
                   c.nome as categoria_nome,
                   c.cor as categoria_cor,
                   (SELECT COUNT(*) FROM favoritos_material WHERE material_id = m.id) as total_favoritos
            FROM materiais m
            LEFT JOIN categorias_material c ON m.categoria_id = c.id
            WHERE m.publicado = true
            ORDER BY m.destaque DESC, m.data_publicacao DESC
            LIMIT 20
        """)
        materiais = cursor.fetchall()
        
        # Buscar categorias
        cursor.execute("SELECT * FROM categorias_material ORDER BY ordem, nome")
        categorias = cursor.fetchall()
        
        # Buscar materiais em destaque
        cursor.execute("""
            SELECT m.*, c.nome as categoria_nome, c.cor as categoria_cor
            FROM materiais m
            LEFT JOIN categorias_material c ON m.categoria_id = c.id
            WHERE m.destaque = true AND m.publicado = true
            LIMIT 5
        """)
        destaques = cursor.fetchall()
        
        return_connection(conn)
        
        return render_template('biblioteca/index.html',
                             materiais=materiais,
                             categorias=categorias,
                             destaques=destaques)
    
    except Exception as e:
        print(f"❌ Erro ao carregar biblioteca: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            return_connection(conn)
        flash('Erro ao carregar a biblioteca. Tente novamente mais tarde.', 'danger')
        return redirect(url_for('dashboard'))


@app.route("/biblioteca/material/<int:material_id>")
@login_required
@permissao_required('material.view_one')
def visualizar_material(material_id):
    """Visualizar um material específico"""
    try:
        cursor, conn = get_db()
        
        # Buscar material com verificação de permissão de grau
        cursor.execute("""
            SELECT m.*, 
                   c.nome as categoria_nome, 
                   c.cor as categoria_cor,
                   (SELECT COUNT(*) FROM favoritos_material WHERE material_id = m.id) as total_favoritos
            FROM materiais m
            LEFT JOIN categorias_material c ON m.categoria_id = c.id
            WHERE m.id = %s AND m.publicado = true
        """, (material_id,))
        material = cursor.fetchone()
        
        if not material:
            flash('Material não encontrado', 'danger')
            return_connection(conn)
            return redirect(url_for('listar_materiais'))
        
        # Verificar permissão por grau
        usuario_grau = session.get('grau_atual', 0)
        grau_acesso = material.get('grau_acesso', 1)
        
        if usuario_grau < grau_acesso and session.get('tipo') != 'admin':
            flash(f'Você não tem permissão para acessar este material (Grau necessário: {grau_acesso})', 'danger')
            return_connection(conn)
            return redirect(url_for('listar_materiais'))
        
        # Incrementar visualizações
        cursor.execute("""
            UPDATE materiais SET visualizacoes_count = COALESCE(visualizacoes_count, 0) + 1 
            WHERE id = %s
        """, (material_id,))
        conn.commit()
        
        # Buscar avaliações
        cursor.execute("""
            SELECT a.*, u.nome_completo as usuario_nome
            FROM avaliacoes_material a
            LEFT JOIN usuarios u ON a.usuario_id = u.id
            WHERE a.material_id = %s
            ORDER BY a.data_avaliacao DESC
            LIMIT 10
        """, (material_id,))
        avaliacoes = cursor.fetchall()
        
        # Verificar se usuário já favoritou
        cursor.execute("""
            SELECT id FROM favoritos_material 
            WHERE material_id = %s AND usuario_id = %s
        """, (material_id, session['user_id']))
        favoritado = cursor.fetchone() is not None
        
        # Verificar se pode baixar
        pode_baixar = verificar_permissao(session['user_id'], 'material.download')
        
        # Verificar se pode avaliar
        pode_avaliar = verificar_permissao(session['user_id'], 'material.rate')
        
        # Buscar avaliação do usuário atual
        if pode_avaliar:
            cursor.execute("""
                SELECT * FROM avaliacoes_material 
                WHERE material_id = %s AND usuario_id = %s
            """, (material_id, session['user_id']))
            avaliacao_usuario = cursor.fetchone()
        else:
            avaliacao_usuario = None
        
        # Determinar o formato do arquivo baseado na URL
        arquivo_url = material.get('arquivo_url', '')
        formato = material.get('formato', '')
        
        # Se não tiver formato definido, tentar extrair da URL
        if not formato and arquivo_url:
            if '.pdf' in arquivo_url.lower():
                formato = 'pdf'
            elif any(ext in arquivo_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                formato = 'imagem'
            else:
                formato = 'outro'
        
        return_connection(conn)
        
        return render_template('biblioteca/visualizar.html',
                             material=material,
                             avaliacoes=avaliacoes,
                             favoritado=favoritado,
                             pode_baixar=pode_baixar,
                             pode_avaliar=pode_avaliar,
                             avaliacao_usuario=avaliacao_usuario,
                             formato=formato)  # Passar formato para o template
    
    except Exception as e:
        print(f"❌ Erro ao visualizar material: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            return_connection(conn)
        flash('Erro ao carregar o material', 'danger')
        return redirect(url_for('listar_materiais'))


@app.route("/biblioteca/material/<int:material_id>/download")
@login_required
@permissao_required('material.download')
def download_material(material_id):
    """Download do material"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT m.*, c.nome as categoria_nome
            FROM materiais m
            LEFT JOIN categorias_material c ON m.categoria_id = c.id
            WHERE m.id = %s AND m.publicado = true
        """, (material_id,))
        material = cursor.fetchone()
        
        if not material or not material.get('arquivo_url'):
            flash('Arquivo não encontrado', 'danger')
            return_connection(conn)
            return redirect(url_for('listar_materiais'))
        
        # Verificar permissão por grau
        usuario_grau = session.get('grau_atual', 0)
        grau_acesso = material.get('grau_acesso', 1)
        
        if usuario_grau < grau_acesso and session.get('tipo') != 'admin':
            flash('Você não tem permissão para baixar este material', 'danger')
            return_connection(conn)
            return redirect(url_for('listar_materiais'))
        
        arquivo_url = material.get('arquivo_url')
        titulo = material.get('titulo', 'documento')
        
        # Registrar download
        cursor.execute("""
            INSERT INTO downloads_material (material_id, usuario_id, data_download, ip)
            VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
        """, (material_id, session['user_id'], request.remote_addr))
        
        # Incrementar contador de downloads
        cursor.execute("""
            UPDATE materiais SET downloads_count = COALESCE(downloads_count, 0) + 1 
            WHERE id = %s
        """, (material_id,))
        conn.commit()
        
        return_connection(conn)
        
        # IMPORTANTE: Redirecionar diretamente para a URL do Cloudinary
        # Isso funciona se os arquivos forem públicos
        return redirect(arquivo_url)
    
    except Exception as e:
        print(f"Erro no download: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            return_connection(conn)
        flash('Erro ao baixar o arquivo', 'danger')
        return redirect(url_for('visualizar_material', material_id=material_id))
        
@app.route("/biblioteca/material/<int:material_id>/registrar-download", methods=['POST'])
@login_required
def registrar_download_material(material_id):
    """Registra download sem redirecionar (AJAX)"""
    try:
        cursor, conn = get_db()
        
        # Registrar download
        cursor.execute("""
            INSERT INTO downloads_material (material_id, usuario_id, data_download, ip)
            VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
        """, (material_id, session['user_id'], request.remote_addr))
        
        # Incrementar contador
        cursor.execute("""
            UPDATE materiais SET downloads_count = COALESCE(downloads_count, 0) + 1 
            WHERE id = %s
        """, (material_id,))
        conn.commit()
        
        return_connection(conn)
        return jsonify({'success': True})
        
    except Exception as e:
        print(f"Erro ao registrar download: {e}")
        if 'conn' in locals():
            return_connection(conn)
        return jsonify({'success': False, 'error': str(e)}), 500
        

@app.route("/admin/corrigir-urls-materiais")
@login_required
def corrigir_urls_materiais():
    """Corrige URLs duplicadas .pdf.pdf no banco de dados"""
    if session.get('tipo') != 'admin':
        return "Acesso negado", 403
    
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT id, arquivo_url
            FROM materiais 
            WHERE arquivo_url LIKE '%.pdf.pdf%'
        """)
        materiais = cursor.fetchall()
        
        corrigidos = []
        
        for material in materiais:
            url_original = material['arquivo_url']
            url_corrigida = url_original.replace('.pdf.pdf', '.pdf')
            
            cursor.execute("""
                UPDATE materiais 
                SET arquivo_url = %s 
                WHERE id = %s
            """, (url_corrigida, material['id']))
            conn.commit()
            
            corrigidos.append({
                'id': material['id'],
                'antes': url_original,
                'depois': url_corrigida
            })
        
        return_connection(conn)
        
        return jsonify({
            'total': len(materiais),
            'corrigidos': corrigidos
        })
        
    except Exception as e:
        if 'conn' in locals():
            return_connection(conn)
        return jsonify({'error': str(e)}), 500        
        
# =============================
# ROTAS DO DASHBOARD OTIMIZADO
# =============================
@app.route("/dashboard")
@login_required
def dashboard():
    try:
        # UMA única conexão para todo o dashboard usando context manager
        with get_db_connection() as cursor:
            
            # ========== MEU CARGO ATUAL ==========
            usuario_id = session['user_id']
            cursor.execute("""
                SELECT c.nome as cargo_nome, oc.data_inicio
                FROM ocupacao_cargos oc
                JOIN cargos c ON oc.cargo_id = c.id
                WHERE oc.obreiro_id = %s AND oc.ativo = 1
                ORDER BY oc.data_inicio DESC
                LIMIT 1
            """, (usuario_id,))
            meu_cargo_row = cursor.fetchone()
            meu_cargo = meu_cargo_row['cargo_nome'] if meu_cargo_row else None
            meu_cargo_data_inicio = meu_cargo_row['data_inicio'] if meu_cargo_row else None
            
            # ========== CARGOS OCUPADOS ==========
            cursor.execute("""
                SELECT oc.*, c.nome as cargo_nome, u.nome_completo as obreiro_nome, u.id as obreiro_id
                FROM ocupacao_cargos oc
                JOIN cargos c ON oc.cargo_id = c.id
                JOIN usuarios u ON oc.obreiro_id = u.id
                WHERE oc.ativo = 1
                ORDER BY oc.data_inicio DESC
                LIMIT 10
            """)
            cargos_ocupados = cursor.fetchall()
            
            # ========== ESTATÍSTICAS CORRIGIDAS (COM TIPO E GRAU >= 3) ==========
            cursor.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM familiares) as total_familiares,
                    (SELECT COUNT(*) FROM condecoracoes_obreiro) as total_condecoracoes,
                    (SELECT COUNT(*) FROM usuarios WHERE tipo IN ('admin', 'obreiro', 'sindicante') AND ativo = 1) as total_obreiros,
                    (SELECT COUNT(*) FROM usuarios WHERE grau_atual >= 3 AND ativo = 1 AND tipo IN ('admin', 'obreiro', 'sindicante')) as mestres,
                    (SELECT COUNT(*) FROM usuarios WHERE grau_atual = 2 AND ativo = 1 AND tipo IN ('admin', 'obreiro', 'sindicante')) as companheiros,
                    (SELECT COUNT(*) FROM usuarios WHERE grau_atual = 1 AND ativo = 1 AND tipo IN ('admin', 'obreiro', 'sindicante')) as aprendizes,
                    (SELECT COUNT(*) FROM reunioes) as total_reunioes,
                    (SELECT COUNT(*) FROM reunioes WHERE status = 'realizada') as reunioes_realizadas,
                    (SELECT COUNT(*) FROM reunioes WHERE status = 'agendada') as reunioes_agendadas
            """)
            stats = cursor.fetchone()
            
            total_familiares = stats['total_familiares']
            total_condecoracoes = stats['total_condecoracoes']
            total_obreiros = stats['total_obreiros']
            mestres = stats['mestres']
            companheiros = stats['companheiros']
            aprendizes = stats['aprendizes']
            total_reunioes = stats['total_reunioes']
            reunioes_realizadas = stats['reunioes_realizadas']
            reunioes_agendadas = stats['reunioes_agendadas']
            
            # ========== DOCUMENTOS RECENTES ==========
            cursor.execute("""
                SELECT d.*, u.nome_completo as obreiro_nome, u.usuario as obreiro_usuario
                FROM documentos_obreiro d
                JOIN usuarios u ON d.obreiro_id = u.id
                ORDER BY d.data_upload DESC
                LIMIT 10
            """)
            documentos_recentes = cursor.fetchall()
            
            # ========== CANDIDATOS COM SINDICANTES ==========
            cursor.execute("""
                SELECT c.*, 
                       COALESCE(
                           (SELECT string_agg(s.sindicante, ',') 
                            FROM sindicancias s 
                            WHERE s.candidato_id = c.id), 
                           ''
                       ) as sindicantes_enviados
                FROM candidatos c
                ORDER BY c.data_criacao DESC
            """)
            candidatos = cursor.fetchall()
            
            # ========== DOCUMENTOS STATUS PARA CADA CANDIDATO ==========
            documentos_status = {}
            for candidato in candidatos:
                try:
                    cursor.execute("""
                        SELECT COUNT(*) as total
                        FROM documentos_candidato 
                        WHERE candidato_id = %s
                    """, (candidato['id'],))
                    result = cursor.fetchone()
                    total = result['total'] if result else 0
                    
                    documentos_status[candidato['id']] = {
                        'total': total,
                        'enviados': total
                    }
                except:
                    documentos_status[candidato['id']] = {
                        'total': 0,
                        'enviados': 0
                    }
            
            # ========== SINDICANTES ==========
            cursor.execute("""
                SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, ativo 
                FROM usuarios 
                WHERE tipo = 'sindicante' AND ativo = 1
                ORDER BY nome_completo
            """)
            sindicantes = cursor.fetchall()
            
            total_sindicantes_ativos = len(sindicantes)
            total_candidatos = len(candidatos)
            
            # ========== PARECERES CONCLUSIVOS ==========
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
            
            # ========== AVISOS ==========
            usuario_grau = session.get('grau_atual', 1)
            cursor.execute("""
                SELECT a.*, u.nome_completo as autor_nome,
                       CASE WHEN av.id IS NOT NULL THEN 1 ELSE 0 END as ja_visto
                FROM avisos a
                LEFT JOIN usuarios u ON a.created_by = u.id
                LEFT JOIN avisos_visualizacoes av ON a.id = av.aviso_id AND av.usuario_id = %s
                WHERE a.ativo = 1 
                AND a.data_inicio <= CURRENT_TIMESTAMP
                AND (a.data_fim IS NULL OR a.data_fim >= CURRENT_TIMESTAMP)
                AND a.grau_destino <= %s
                ORDER BY a.created_at DESC
                LIMIT 5
            """, (usuario_id, usuario_grau))
            ultimos_avisos = cursor.fetchall()
            
            # ============================================
            # ADMIN VS NÃO-ADMIN
            # ============================================
            if session["tipo"] == "admin":
                em_analise = 0
                aprovados = 0
                reprovados = 0
                pendentes = []
                prazo_vencido = []
                
                sindicantes_set = {s["usuario"] for s in sindicantes}
                
                for c in candidatos:
                    if c["status"] == "Em análise" and not c["fechado"]:
                        em_analise += 1
                        enviados = c["sindicantes_enviados"].split(',') if c["sindicantes_enviados"] else []
                        faltam = [s for s in sindicantes_set if s not in enviados]
                        if faltam:
                            pendentes.append({"candidato": dict(c), "faltam": faltam})
                        if c["data_criacao"]:
                            dias = (datetime.now() - c["data_criacao"]).days
                            if dias > 7:
                                prazo_vencido.append(dict(c))
                    elif c["status"] == "Aprovado":
                        aprovados += 1
                    elif c["status"] == "Reprovado":
                        reprovados += 1
                
                cursor.execute("""
                    SELECT id, titulo, data, hora_inicio 
                    FROM reunioes 
                    WHERE status = 'agendada' AND data >= CURRENT_DATE
                    ORDER BY data ASC, hora_inicio ASC
                    LIMIT 5
                """)
                proximas_reunioes = cursor.fetchall()
                
            else:
                em_analise = aprovados = reprovados = 0
                pendentes = []
                prazo_vencido = []
                
                usuario_grau = session.get('grau_atual', 1)
                
                if usuario_grau == 1:
                    grau_filter = "(grau = 1 OR grau IS NULL)"
                elif usuario_grau == 2:
                    grau_filter = "(grau IN (1, 2) OR grau IS NULL)"
                else:
                    grau_filter = "(grau <= 3 OR grau IS NULL OR grau > 3)"
                
                cursor.execute(f"""
                    SELECT id, titulo, data, hora_inicio, grau, tipo, local, status
                    FROM reunioes 
                    WHERE status = 'agendada' 
                    AND data >= CURRENT_DATE
                    AND {grau_filter}
                    ORDER BY data ASC, hora_inicio ASC
                    LIMIT 5
                """)
                proximas_reunioes = cursor.fetchall()
                
                cursor.execute("""
                    SELECT candidato_id, parecer 
                    FROM sindicancias 
                    WHERE sindicante = %s
                """, (session["usuario"],))
                pareceres_dict = {p["candidato_id"]: p["parecer"] for p in cursor.fetchall()}
                
                for c in candidatos:
                    if c["id"] in pareceres_dict:
                        if pareceres_dict[c["id"]] == "positivo":
                            aprovados += 1
                        else:
                            reprovados += 1
                    elif not c["fechado"]:
                        em_analise += 1
        
        # Conexão é fechada automaticamente ao sair do with
        
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
            em_analise=em_analise,
            aprovados=aprovados,
            reprovados=reprovados,
            pendentes=pendentes,
            prazo_vencido=prazo_vencido,
            sindicantes=sindicantes,
            candidatos=candidatos,  # <-- ADICIONADO
            documentos_status=documentos_status,  # <-- ADICIONADO
            pareceres_conclusivos=pareceres_conclusivos,
            ultimos_avisos=ultimos_avisos,
            meu_cargo=meu_cargo,
            meu_cargo_data_inicio=meu_cargo_data_inicio,
            cargos_ocupados=cargos_ocupados,
            total_familiares=total_familiares,
            total_condecoracoes=total_condecoracoes,
            documentos_recentes=documentos_recentes,
            now=datetime.now()
        )
    except Exception as e:
        print(f"Erro no dashboard: {e}")
        traceback.print_exc()
        flash(f"Erro ao carregar dashboard: {e}", "danger")
        return redirect("/")

# =============================
# ROTAS DE PERFIL
# =============================
@app.route("/perfil", methods=["GET", "POST"])
@login_required
def perfil():
    cursor, conn = get_db()
    if request.method == "POST":
        if request.form.get("acao") == "alterar_senha":
            senha_atual = request.form.get("senha_atual")
            nova_senha = request.form.get("nova_senha")
            confirmar_senha = request.form.get("confirmar_senha")
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
            nova_senha_hash = generate_password_hash(nova_senha)
            cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (nova_senha_hash, session["user_id"]))
            conn.commit()
            registrar_log("alterar_senha", "perfil", session["user_id"], dados_novos={"usuario": session["usuario"]})
            flash("Senha alterada com sucesso! Faça login novamente.", "success")
            return redirect("/logout")
        else:
            nome_completo = request.form.get("nome_completo", "")
            cim_numero = request.form.get("cim_numero", "")
            loja_nome = request.form.get("loja_nome", "")
            loja_numero = request.form.get("loja_numero", "")
            loja_orient = request.form.get("loja_orient", "")
            telefone = request.form.get("telefone", "")
            email = request.form.get("email", "")
            endereco = request.form.get("endereco", "")
            cursor.execute("SELECT * FROM usuarios WHERE id = %s", (session["user_id"],))
            dados_antigos = dict(cursor.fetchone())
            cursor.execute("""
                UPDATE usuarios 
                SET nome_completo = %s, cim_numero = %s, loja_nome = %s, 
                    loja_numero = %s, loja_orient = %s, telefone = %s, 
                    email = %s, endereco = %s
                WHERE id = %s
            """, (nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, telefone, email, endereco, session["user_id"]))
            conn.commit()
            session["nome_completo"] = nome_completo
            session["cim_numero"] = cim_numero
            session["loja_nome"] = loja_nome
            session["loja_numero"] = loja_numero
            session["loja_orient"] = loja_orient
            registrar_log("editar", "perfil", session["user_id"], dados_anteriores=dados_antigos, dados_novos={"nome_completo": nome_completo, "email": email})
            flash("Perfil atualizado com sucesso!", "success")
            return redirect("/perfil")
    cursor.execute("SELECT * FROM usuarios WHERE id = %s", (session["user_id"],))
    usuario = cursor.fetchone()
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    return_connection(conn)
    return render_template("perfil.html", usuario=usuario, lojas=lojas)
# =============================
# ROTAS DE DOCUMENTOS DO OBREIRO
# =============================

@app.route("/api/obreiros/<int:obreiro_id>/documentos")
@login_required
def api_listar_documentos_obreiro(obreiro_id):
    """Lista documentos do obreiro via API"""
    cursor, conn = get_db()
    
    # Verificar permissão
    if session.get('tipo') != 'admin' and session['user_id'] != obreiro_id:
        return jsonify({'success': False, 'error': 'Permissão negada'}), 403
    
    # Buscar documentos
    cursor.execute("""
        SELECT 
            id, 
            titulo, 
            descricao, 
            categoria, 
            tipo_arquivo, 
            caminho_arquivo as url, 
            data_upload
        FROM documentos_obreiro
        WHERE obreiro_id = %s
        ORDER BY data_upload DESC
    """, (obreiro_id,))
    
    rows = cursor.fetchall()
    return_connection(conn)
    
    documentos = []
    for row in rows:
        documentos.append({
            'id': row['id'],
            'titulo': row['titulo'],
            'descricao': row['descricao'],
            'categoria': row['categoria'],
            'tipo_arquivo': row['tipo_arquivo'],
            'url': row['url'],
            'data_upload': row['data_upload'].isoformat() if row['data_upload'] else None
        })
    
    return jsonify({
        'success': True,
        'documentos': documentos
    })


@app.route("/obreiros/<int:obreiro_id>/documentos/upload", methods=["POST"])
@login_required
def upload_documento_obreiro(obreiro_id):
    """Upload de documento para o Cloudinary"""
    cursor, conn = get_db()
    
    # Verificar permissão
    if session.get('tipo') != 'admin' and session['user_id'] != obreiro_id:
        flash("Permissão negada!", "danger")
        return redirect(f"/obreiros/{obreiro_id}/editar")
    
    if 'arquivo' not in request.files:
        flash("Nenhum arquivo selecionado!", "danger")
        return redirect(f"/obreiros/{obreiro_id}/editar")
    
    arquivo = request.files['arquivo']
    titulo = request.form.get('titulo')
    descricao = request.form.get('descricao')
    categoria = request.form.get('categoria', 'outros')
    
    if not titulo:
        titulo = arquivo.filename
    
    if arquivo.filename == '':
        flash("Nenhum arquivo selecionado!", "danger")
        return redirect(f"/obreiros/{obreiro_id}/editar")
    
    try:
        # Upload para Cloudinary
        import cloudinary.uploader
        from werkzeug.utils import secure_filename
        
        nome_arquivo = secure_filename(arquivo.filename)
        extensao = nome_arquivo.split('.')[-1].lower()
        
        # Validar extensão
        allowed_extensions = ['pdf', 'jpg', 'jpeg', 'png', 'doc', 'docx']
        if extensao not in allowed_extensions:
            flash(f"Tipo de arquivo não permitido. Use: {', '.join(allowed_extensions)}", "danger")
            return redirect(f"/obreiros/{obreiro_id}/editar")
        
        # Upload para Cloudinary
        upload_result = cloudinary.uploader.upload(
            arquivo,
            folder=f"obreiros/{obreiro_id}/documentos",
            resource_type="auto",
            use_filename=True,
            unique_filename=True
        )
        
        url_arquivo = upload_result.get('secure_url')
        public_id = upload_result.get('public_id')
        tamanho = upload_result.get('bytes', 0)
        
        # Salvar no banco
        cursor.execute("""
            INSERT INTO documentos_obreiro 
            (obreiro_id, titulo, descricao, categoria, tipo_arquivo, 
             nome_arquivo, caminho_arquivo, tamanho, uploaded_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (obreiro_id, titulo, descricao, categoria, extensao,
              public_id, url_arquivo, tamanho, session['user_id']))
        
        doc_id = cursor.fetchone()['id']
        conn.commit()
        
        registrar_log("upload_documento", "documento_obreiro", doc_id, 
                     dados_novos={"obreiro_id": obreiro_id, "titulo": titulo})
        
        flash(f"Documento '{titulo}' enviado com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro no upload: {e}")
        flash(f"Erro ao enviar documento: {str(e)}", "danger")
        if conn:
            conn.rollback()
    
    return_connection(conn)
    return redirect(f"/obreiros/{obreiro_id}/editar")


@app.route("/api/documentos/<int:doc_id>/excluir", methods=["DELETE"])
@login_required
def api_excluir_documento_obreiro(doc_id):
    """Excluir documento do Cloudinary e do banco"""
    cursor, conn = get_db()
    
    try:
        # Buscar documento
        cursor.execute("""
            SELECT d.*, u.id as obreiro_id
            FROM documentos_obreiro d
            JOIN usuarios u ON d.obreiro_id = u.id
            WHERE d.id = %s
        """, (doc_id,))
        
        doc = cursor.fetchone()
        
        if not doc:
            return jsonify({'success': False, 'error': 'Documento não encontrado'}), 404
        
        # Verificar permissão
        if session.get('tipo') != 'admin' and session['user_id'] != doc['obreiro_id']:
            return jsonify({'success': False, 'error': 'Permissão negada'}), 403
        
        # Excluir do Cloudinary
        import cloudinary.uploader
        if doc.get('nome_arquivo'):
            cloudinary.uploader.destroy(doc['nome_arquivo'])
        
        # Excluir do banco
        cursor.execute("DELETE FROM documentos_obreiro WHERE id = %s", (doc_id,))
        conn.commit()
        
        registrar_log("excluir_documento", "documento_obreiro", doc_id,
                     dados_anteriores={"titulo": doc['titulo']})
        
        return jsonify({'success': True, 'message': 'Documento excluído com sucesso!'})
        
    except Exception as e:
        print(f"Erro ao excluir documento: {e}")
        if conn:
            conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        return_connection(conn)


# =============================
# ROTAS DE VISITANTES
# =============================

@app.route("/visitante")
def pagina_visitante():
    """Página inicial do visitante"""
    return render_template("visitante/index.html")

@app.route("/visitante/cadastro", methods=["GET", "POST"])
def cadastro_visitante():
    """Cadastro de visitante para reunião específica"""
    cursor, conn = get_db()
    
    if request.method == "POST":
        nome = request.form.get("nome")
        email = request.form.get("email")
        telefone = request.form.get("telefone")
        loja_origem = request.form.get("loja_origem")
        grau = request.form.get("grau")
        reuniao_id = request.form.get("reuniao_id")
        enviar_email = request.form.get("enviar_email") == 'on'
        
        if not nome or not reuniao_id:
            flash("Nome e Reunião são obrigatórios!", "danger")
            return_connection(conn)
            return redirect("/visitante/cadastro")
        
        # Verificar se a reunião existe
        cursor.execute("""
            SELECT id, titulo, data, hora_inicio, local 
            FROM reunioes 
            WHERE id = %s AND status = 'agendada' AND data >= CURRENT_DATE
        """, (reuniao_id,))
        reuniao = cursor.fetchone()
        
        if not reuniao:
            flash("Reunião não encontrada ou já realizada!", "danger")
            return_connection(conn)
            return redirect("/visitante/cadastro")
        
        import uuid
        codigo_verificacao = str(uuid.uuid4())[:8].upper()
        
        try:
            cursor.execute("""
                INSERT INTO visitantes 
                (nome, email, telefone, loja_origem, grau, reuniao_id, data_visita, codigo_verificacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (nome, email, telefone, loja_origem, grau, reuniao_id, reuniao['data'], codigo_verificacao))
            
            visitante_id = cursor.fetchone()['id']
            conn.commit()
            
            # Buscar dados completos do visitante
            cursor.execute("""
                SELECT v.*, r.titulo as reuniao_titulo, r.data as reuniao_data, r.hora_inicio, r.local
                FROM visitantes v
                JOIN reunioes r ON v.reuniao_id = r.id
                WHERE v.id = %s
            """, (visitante_id,))
            visitante_completo = cursor.fetchone()
            
            session['visitante_id'] = visitante_id
            session['visitante_nome'] = nome
            
            # Enviar e-mail com certificado
            email_enviado = False
            if enviar_email and email:
                resultado = enviar_certificado_email(visitante_completo)
                if resultado['success']:
                    email_enviado = True
                    flash(f"✅ Cadastro realizado! Certificado enviado para {email}.", "success")
                else:
                    flash(f"✅ Cadastro realizado! Mas não foi possível enviar o e-mail: {resultado['message']}", "warning")
            else:
                flash(f"✅ Cadastro realizado com sucesso, {nome}!", "success")
            
            return_connection(conn)
            return redirect(f"/visitante/certificado/{visitante_id}")
            
        except Exception as e:
            print(f"Erro ao cadastrar visitante: {e}")
            conn.rollback()
            flash(f"Erro ao cadastrar: {str(e)}", "danger")
            return_connection(conn)
            return redirect("/visitante/cadastro")
    
    # GET - Buscar próximas reuniões
    try:
        cursor.execute("""
            SELECT id, titulo, data, hora_inicio, local 
            FROM reunioes 
            WHERE status = 'agendada' AND data >= CURRENT_DATE
            ORDER BY data ASC
            LIMIT 10
        """)
        reunioes = cursor.fetchall()
        
        return_connection(conn)
        return render_template("visitante/cadastro.html", reunioes=reunioes)
        
    except Exception as e:
        print(f"Erro ao carregar reuniões: {e}")
        return_connection(conn)
        flash(f"Erro ao carregar reuniões: {str(e)}", "danger")
        return redirect("/visitante")

@app.route("/visitante/reenviar/<int:visitante_id>")
def reenviar_certificado(visitante_id):
    """Reenviar certificado por e-mail"""
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT v.*, r.titulo as reuniao_titulo, r.data as reuniao_data, r.hora_inicio, r.local
        FROM visitantes v
        JOIN reunioes r ON v.reuniao_id = r.id
        WHERE v.id = %s
    """, (visitante_id,))
    visitante = cursor.fetchone()
    
    if not visitante:
        flash("Visitante não encontrado!", "danger")
        return redirect("/visitante")
    
    if not visitante.get('email'):
        flash("Visitante não possui e-mail cadastrado!", "warning")
        return redirect(f"/visitante/certificado/{visitante_id}")
    
    resultado = enviar_certificado_email(visitante)
    
    if resultado['success']:
        flash(f"✅ Certificado reenviado para {visitante['email']}!", "success")
    else:
        flash(f"❌ Erro ao reenviar: {resultado['message']}", "danger")
    
    return redirect(f"/visitante/certificado/{visitante_id}")        

@app.route("/admin/visitantes")
@login_required
@admin_required
def admin_visitantes():
    """Lista todos os visitantes"""
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT v.*, r.titulo as reuniao_titulo, r.data as reuniao_data
        FROM visitantes v
        JOIN reunioes r ON v.reuniao_id = r.id
        ORDER BY v.created_at DESC
    """)
    visitantes = cursor.fetchall()
    
    return_connection(conn)
    return render_template("admin/visitantes.html", visitantes=visitantes)
    
    # GET - Buscar próximas reuniões
    cursor.execute("""
        SELECT id, titulo, data, hora_inicio, local 
        FROM reunioes 
        WHERE status = 'agendada' AND data >= CURRENT_DATE
        ORDER BY data ASC
        LIMIT 10
    """)
    reunioes = cursor.fetchall()
    
    return_connection(conn)
    return render_template("visitante/cadastro.html", reunioes=reunioes)
    
# =============================
# ROTAS DE VISITANTES
# =============================    

@app.route("/visitante/certificado/<int:visitante_id>")
def certificado_visitante(visitante_id):
    """Gerar certificado de visita"""
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT v.*, r.titulo as reuniao_titulo, r.data as reuniao_data, 
               r.hora_inicio, r.local, l.nome as loja_nome
        FROM visitantes v
        JOIN reunioes r ON v.reuniao_id = r.id
        LEFT JOIN lojas l ON r.loja_id = l.id
        WHERE v.id = %s
    """, (visitante_id,))
    
    visitante = cursor.fetchone()
    
    if not visitante:
        flash("Visitante não encontrado!", "danger")
        return redirect("/visitante")
    
    # Marcar certificado como gerado
    if not visitante['certificado_gerado']:
        cursor.execute("UPDATE visitantes SET certificado_gerado = TRUE WHERE id = %s", (visitante_id,))
        conn.commit()
    
    return_connection(conn)
    
    return render_template("visitante/certificado.html", visitante=visitante)

@app.route("/visitante/presenca/<int:reuniao_id>")
def marcar_presenca(reuniao_id):
    """Página para marcar presença na reunião"""
    cursor, conn = get_db()
    
    cursor.execute("SELECT * FROM reunioes WHERE id = %s AND status = 'agendada'", (reuniao_id,))
    reuniao = cursor.fetchone()
    
    if not reuniao:
        flash("Reunião não encontrada ou já realizada!", "danger")
        return redirect("/visitante")
    
    return_connection(conn)
    return render_template("visitante/presenca.html", reuniao=reuniao)

@app.route("/api/visitante/verificar/<codigo>")
def verificar_visitante(codigo):
    """API para verificar código de visitante (para validar certificado)"""
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT v.*, r.titulo as reuniao_titulo, r.data as reuniao_data
        FROM visitantes v
        JOIN reunioes r ON v.reuniao_id = r.id
        WHERE v.codigo_verificacao = %s
    """, (codigo,))
    
    visitante = cursor.fetchone()
    return_connection(conn)
    
    if visitante:
        return jsonify({
            'success': True,
            'nome': visitante['nome'],
            'reuniao': visitante['reuniao_titulo'],
            'data': visitante['reuniao_data'].strftime('%d/%m/%Y'),
            'certificado': visitante['certificado_gerado']
        })
    else:
        return jsonify({'success': False, 'message': 'Código inválido'})

# =============================
# ROTAS DE OBREIROS
# =============================
@app.route("/obreiros/<int:id>/reativar")
@login_required
@permissao_required('obreiro.edit')
def reativar_obreiro(id):
    """Reativa um obreiro que estava inativo"""
    cursor, conn = get_db()
    
    try:
        # Buscar dados do obreiro
        cursor.execute("SELECT id, nome_completo, usuario, ativo FROM usuarios WHERE id = %s", (id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("Obreiro não encontrado!", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        # Verificar se já está ativo
        if obreiro['ativo'] == 1:
            flash(f"Obreiro {obreiro['nome_completo']} já está ativo!", "warning")
            return_connection(conn)
            return redirect(f"/obreiros/{id}")
        
        # Reativar obreiro
        cursor.execute("UPDATE usuarios SET ativo = 1 WHERE id = %s", (id,))
        conn.commit()
        
        registrar_log("reativar", "obreiro", id, dados_novos={"nome": obreiro['nome_completo'], "status": "ativo"})
        
        flash(f"✅ Obreiro {obreiro['nome_completo']} reativado com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao reativar obreiro: {e}")
        conn.rollback()
        flash(f"Erro ao reativar obreiro: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect("/obreiros")
    
@app.route("/obreiros")
@login_required
def listar_obreiros():
    """Lista obreiros com filtros por nome, grau, cargo, loja e status"""
    cursor, conn = get_db()
    
    # ============================================
    # PERMISSÃO: Todos os obreiros podem visualizar a lista
    # ============================================
    
    # Obter parâmetros de filtro
    nome = request.args.get('nome', '').strip()
    grau = request.args.get('grau', '')
    cargo = request.args.get('cargo', '')
    loja = request.args.get('loja', '')
    status = request.args.get('status', 'ativos')
    
    # Construir query base
    query = """
        SELECT DISTINCT u.*, 
               l.nome as loja_nome_completo,
               l.cidade as loja_cidade,
               l.uf as loja_uf,
               u.grau_atual as grau_nivel,
               (SELECT COUNT(*) FROM ocupacao_cargos oc 
                WHERE oc.obreiro_id = u.id AND oc.ativo = 1) as total_cargos_ativos,
               (SELECT string_agg(c.nome, ', ') 
                FROM ocupacao_cargos oc 
                JOIN cargos c ON oc.cargo_id = c.id 
                WHERE oc.obreiro_id = u.id AND oc.ativo = 1 
                LIMIT 3) as cargos_atuais,
               (SELECT data_inicio FROM ocupacao_cargos 
                WHERE obreiro_id = u.id AND ativo = 1 
                ORDER BY data_inicio DESC LIMIT 1) as data_ultimo_cargo,
               (SELECT COUNT(*) FROM presenca p 
                JOIN reunioes r ON p.reuniao_id = r.id 
                WHERE p.obreiro_id = u.id 
                AND r.status = 'realizada' 
                AND EXTRACT(YEAR FROM r.data) = EXTRACT(YEAR FROM CURRENT_DATE)) as presencas_ano,
               (SELECT COUNT(*) FROM presenca p 
                JOIN reunioes r ON p.reuniao_id = r.id 
                WHERE p.obreiro_id = u.id 
                AND r.status = 'realizada' 
                AND EXTRACT(YEAR FROM r.data) = EXTRACT(YEAR FROM CURRENT_DATE)
                AND p.presente = 1) as presencas_confirmadas_ano
        FROM usuarios u
        LEFT JOIN lojas l ON u.loja_nome = l.nome
        WHERE u.tipo IN ('obreiro', 'admin', 'sindicante')
    """
    params = []
    
    # Filtro por nome (nome completo ou usuário)
    if nome:
        query += " AND (u.nome_completo ILIKE %s OR u.usuario ILIKE %s)"
        params.extend([f"%{nome}%", f"%{nome}%"])
    
    # Filtro por grau
    if grau:
        try:
            grau_int = int(grau)
            if grau_int == 3:
                query += " AND u.grau_atual >= 3"
            else:
                query += " AND u.grau_atual = %s"
                params.append(grau_int)
        except ValueError:
            pass
    
    # Filtro por cargo
    if cargo:
        query += """ AND EXISTS (
            SELECT 1 FROM ocupacao_cargos oc 
            WHERE oc.obreiro_id = u.id 
            AND oc.cargo_id = %s 
            AND oc.ativo = 1
        )"""
        params.append(cargo)
    
    # Filtro por loja
    if loja:
        query += " AND u.loja_nome = %s"
        params.append(loja)
    
    # Filtro por status
    if status == 'inativos':
        query += " AND u.ativo = 0"
    elif status == 'todos':
        pass
    else:
        query += " AND u.ativo = 1"
    
    # Ordenação (alfabética por nome completo)
    query += """
        ORDER BY 
            u.nome_completo ASC
    """
    
    # Executar query
    cursor.execute(query, params)
    obreiros = cursor.fetchall()
    
    # Converter para lista de dicionários
    obreiros_list = []
    for row in obreiros:
        obreiro = dict(row)
        grau_nivel = obreiro.get('grau_atual', 0)
        
        obreiro['grau_principal'] = get_grau_principal(grau_nivel)
        obreiro['grau_detalhado'] = get_grau_detalhado(grau_nivel)
        obreiro['grau_badge_class'] = get_grau_badge_class(grau_nivel)
        obreiro['grau_icon'] = get_grau_icon(grau_nivel)
        
        if obreiro.get('presencas_ano', 0) > 0:
            percentual = (obreiro.get('presencas_confirmadas_ano', 0) / obreiro.get('presencas_ano', 1)) * 100
            obreiro['percentual_presenca'] = round(percentual, 1)
        else:
            obreiro['percentual_presenca'] = 0
        
        obreiro['status_class'] = 'table-success' if obreiro['ativo'] == 1 else 'table-secondary'
        obreiro['status_badge'] = 'success' if obreiro['ativo'] == 1 else 'secondary'
        obreiro['status_text'] = 'Ativo' if obreiro['ativo'] == 1 else 'Inativo'
        
        obreiros_list.append(obreiro)
    
    # ============================================
    # ESTATÍSTICAS GERAIS
    # ============================================
    
    # Total de obreiros (considerando todos os tipos)
    cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo IN ('obreiro', 'admin', 'sindicante')")
    total_obreiros = cursor.fetchone()['total']
    
    # Total de ativos
    cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo IN ('obreiro', 'admin', 'sindicante') AND ativo = 1")
    total_ativos = cursor.fetchone()['total']
    
    # Total de inativos
    total_inativos = total_obreiros - total_ativos
    
    # Mestres (grau >= 3)
    cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo IN ('obreiro', 'admin', 'sindicante') AND ativo = 1 AND grau_atual >= 3")
    mestres = cursor.fetchone()['total']
    
    # Companheiros (grau = 2)
    cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo IN ('obreiro', 'admin', 'sindicante') AND ativo = 1 AND grau_atual = 2")
    companheiros = cursor.fetchone()['total']
    
    # Aprendizes (grau = 1)
    cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo IN ('obreiro', 'admin', 'sindicante') AND ativo = 1 AND grau_atual = 1")
    aprendizes = cursor.fetchone()['total']
    
    # Taxa de ativos
    taxa_ativos = (total_ativos / total_obreiros * 100) if total_obreiros > 0 else 0
    
    # Buscar dados para os filtros (dropdowns)
    cursor.execute("SELECT DISTINCT grau_atual as grau FROM usuarios WHERE grau_atual IS NOT NULL ORDER BY grau_atual")
    graus_raw = cursor.fetchall()
    
    graus_disponiveis = []
    for g in graus_raw:
        grau_nivel = g['grau']
        graus_disponiveis.append({
            'grau': grau_nivel,
            'nome_grau': get_grau_principal(grau_nivel),
            'nome_detalhado': get_grau_detalhado(grau_nivel)
        })
    
    cursor.execute("SELECT id, nome, sigla FROM cargos WHERE ativo = 1 ORDER BY ordem, nome")
    cargos_disponiveis = cursor.fetchall()
    
    cursor.execute("SELECT DISTINCT loja_nome as nome FROM usuarios WHERE loja_nome IS NOT NULL AND loja_nome != '' ORDER BY loja_nome")
    lojas_disponiveis = cursor.fetchall()
    
    return_connection(conn)
    
    filtros = {'nome': nome, 'grau': grau, 'cargo': cargo, 'loja': loja, 'status': status}
    
    # Estatísticas completas para o template
    estatisticas = {
        'total_obreiros': total_obreiros,
        'total_ativos': total_ativos,
        'total_inativos': total_inativos,
        'exibidos': len(obreiros_list),
        'mestres': mestres,
        'companheiros': companheiros,
        'aprendizes': aprendizes,
        'taxa_ativos': taxa_ativos,
        'media_presenca': 0  # Pode ser calculado se necessário
    }
    
    return render_template(
        "obreiros/lista.html",
        obreiros=obreiros_list,
        graus=graus_disponiveis,
        cargos=cargos_disponiveis,
        lojas=lojas_disponiveis,
        filtros=filtros,
        estatisticas=estatisticas,
        now=datetime.now()
    )
   
# ============================================
    # ROTAS DE OBREIROS
# ============================================   

@app.route("/obreiros/<int:id>/excluir")
@login_required
@permissao_required('obreiro.delete')
def excluir_obreiro(id):
    """Desativa ou exclui um obreiro dependendo dos vínculos existentes"""
    cursor, conn = get_db()
    
    try:
        # Buscar dados do obreiro
        cursor.execute("""
            SELECT id, nome_completo, usuario, tipo, ativo, grau_atual
            FROM usuarios 
            WHERE id = %s
        """, (id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("Obreiro não encontrado!", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        # Impedir que o usuário exclua a si mesmo
        if session.get("user_id") == id:
            flash("Você não pode excluir seu próprio usuário!", "danger")
            return_connection(conn)
            return redirect(f"/obreiros/{id}")
        
        # Impedir excluir admin (opcional)
        if obreiro['tipo'] == 'admin' and session.get('tipo') != 'admin':
            flash("Você não tem permissão para excluir administradores!", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        # Verificar vínculos em outras tabelas
        cursor.execute("SELECT COUNT(*) as total FROM presenca WHERE obreiro_id = %s", (id,))
        presencas = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM ocupacao_cargos WHERE obreiro_id = %s", (id,))
        cargos = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM familiares WHERE obreiro_id = %s", (id,))
        familiares = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM condecoracoes_obreiro WHERE obreiro_id = %s", (id,))
        condecoracoes = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM documentos_obreiro WHERE obreiro_id = %s", (id,))
        documentos = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM logs_auditoria WHERE usuario_id = %s", (id,))
        logs = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM notificacoes WHERE usuario_id = %s", (id,))
        notificacoes = cursor.fetchone()['total']
        
        # ✅ CORRIGIDO: sindicancias usa a coluna 'sindicante'
        cursor.execute("SELECT COUNT(*) as total FROM sindicancias WHERE sindicante = %s", (str(id),))
        sindicancias = cursor.fetchone()['total']
        
        # Total de vínculos
        total_vinculos = presencas + cargos + familiares + condecoracoes + documentos + logs + notificacoes + sindicancias
        
        if total_vinculos > 0:
            # Se há vínculos, apenas desativar
            cursor.execute("UPDATE usuarios SET ativo = 0 WHERE id = %s", (id,))
            conn.commit()
            
            registrar_log("desativar", "obreiro", id, 
                         dados_anteriores={"nome": obreiro['nome_completo'], "ativo": 1},
                         dados_novos={"status": "inativo", "motivo": f"possui {total_vinculos} vínculos no sistema"})
            
            flash(f"⚠️ Obreiro '{obreiro['nome_completo']}' foi DESATIVADO (possui {total_vinculos} vínculos no sistema).", "warning")
        else:
            # Sem vínculos, pode excluir permanentemente
            # Remover foto se existir
            cursor.execute("SELECT foto FROM usuarios WHERE id = %s", (id,))
            foto = cursor.fetchone()
            if foto and foto['foto']:
                try:
                    import os
                    if os.path.exists(os.path.join(UPLOAD_FOLDER_FOTOS, foto['foto'])):
                        os.remove(os.path.join(UPLOAD_FOLDER_FOTOS, foto['foto']))
                except:
                    pass  # Se for Cloudinary, ignora
            
            # Excluir registros relacionados
            cursor.execute("DELETE FROM presenca WHERE obreiro_id = %s", (id,))
            cursor.execute("DELETE FROM ocupacao_cargos WHERE obreiro_id = %s", (id,))
            cursor.execute("DELETE FROM familiares WHERE obreiro_id = %s", (id,))
            cursor.execute("DELETE FROM condecoracoes_obreiro WHERE obreiro_id = %s", (id,))
            cursor.execute("DELETE FROM documentos_obreiro WHERE obreiro_id = %s", (id,))
            cursor.execute("DELETE FROM notificacoes WHERE usuario_id = %s", (id,))
            cursor.execute("DELETE FROM logs_auditoria WHERE usuario_id = %s", (id,))
            cursor.execute("DELETE FROM sindicancias WHERE sindicante = %s", (str(id),))
            
            # Excluir o obreiro
            cursor.execute("DELETE FROM usuarios WHERE id = %s", (id,))
            conn.commit()
            
            registrar_log("excluir", "obreiro", id, dados_anteriores={"nome": obreiro['nome_completo']})
            flash(f"✅ Obreiro '{obreiro['nome_completo']}' excluído permanentemente!", "success")
        
        return_connection(conn)
        return redirect("/obreiros")
        
    except Exception as e:
        print(f"Erro ao excluir/desativar obreiro: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
        flash(f"Erro ao processar solicitação: {str(e)}", "danger")
        return_connection(conn)
        return redirect("/obreiros")   
        
@app.route("/obreiros/<int:id>/excluir_definitivo", methods=["POST"])
@login_required
@permissao_required('obreiro.delete')
def excluir_obreiro_definitivo(id):
    """Exclui um obreiro permanentemente (apenas se estiver inativo)"""
    try:
        cursor, conn = get_db()
        
        # Verificar se o obreiro existe
        cursor.execute("SELECT id, nome_completo, ativo FROM usuarios WHERE id = %s", (id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            return jsonify({'success': False, 'error': 'Obreiro não encontrado!'}), 404
        
        # Verificar se está inativo
        if obreiro['ativo'] == 1:
            return jsonify({'success': False, 'error': f'Obreiro {obreiro["nome_completo"]} está ATIVO. Desative primeiro para excluir definitivamente.'}), 400
        
        # Registrar log antes de excluir
        registrar_log("excluir_definitivo", "usuario", id, dados_anteriores={
            "nome": obreiro['nome_completo'],
            "id": id,
            "ativo": obreiro['ativo']
        })
        
        # Excluir registros relacionados
        cursor.execute("DELETE FROM presenca WHERE obreiro_id = %s", (id,))
        cursor.execute("DELETE FROM ocupacao_cargos WHERE obreiro_id = %s", (id,))
        cursor.execute("DELETE FROM familiares WHERE obreiro_id = %s", (id,))
        cursor.execute("DELETE FROM condecoracoes_obreiro WHERE obreiro_id = %s", (id,))
        cursor.execute("DELETE FROM documentos_obreiro WHERE obreiro_id = %s", (id,))
        cursor.execute("DELETE FROM notificacoes WHERE usuario_id = %s", (id,))
        cursor.execute("DELETE FROM logs_auditoria WHERE usuario_id = %s", (id,))
        cursor.execute("DELETE FROM sindicancias WHERE sindicante = %s", (str(id),))
        cursor.execute("DELETE FROM historico_graus WHERE obreiro_id = %s", (id,))
        
        # Excluir o obreiro
        cursor.execute("DELETE FROM usuarios WHERE id = %s", (id,))
        conn.commit()
        
        return jsonify({'success': True, 'message': f'Obreiro {obreiro["nome_completo"]} excluído permanentemente!'})
        
    except Exception as e:
        print(f"❌ Erro ao excluir obreiro: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if conn:
            return_connection(conn)        
    

    
@app.route("/obreiros/novo", methods=["GET", "POST"])
@login_required
@permissao_required('obreiro.create')
def novo_obreiro():
    """Cria um novo obreiro"""
    cursor, conn = get_db()
    
    if request.method == "POST":
        try:
            # ========== CAMPOS OBRIGATÓRIOS ==========
            usuario = request.form.get("usuario")
            senha = request.form.get("senha")
            nome_completo = request.form.get("nome_completo")
            tipo = request.form.get("tipo", "obreiro")
            ativo = 1 if request.form.get("ativo") == '1' else 1
            grau_principal = request.form.get("grau_principal", 1)
            
            # Calcular grau atual
            grau_superior = request.form.get("grau_superior", "")
            if int(grau_principal) == 3 and grau_superior and grau_superior != '':
                grau_atual = int(grau_superior)
            else:
                grau_atual = int(grau_principal)
            
            # Validações
            if not usuario or not senha or not nome_completo:
                flash("Preencha os campos obrigatórios", "danger")
                return redirect("/obreiros/novo")
            
            if len(senha) < 6:
                flash("A senha deve ter no mínimo 6 caracteres", "danger")
                return redirect("/obreiros/novo")
            
            # Verificar permissões
            if tipo == 'sindicante' and grau_atual < 3:
                flash("Apenas Mestres (grau 3) e superiores podem ser Sindicantes!", "danger")
                return redirect("/obreiros/novo")
            
            from werkzeug.security import generate_password_hash
            senha_hash = generate_password_hash(senha)
            
            # ========== INSERT com campos obrigatórios APENAS ==========
            cursor.execute("""
                INSERT INTO usuarios 
                (usuario, senha_hash, tipo, data_cadastro, ativo, nome_completo, grau_atual)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (usuario, senha_hash, tipo, datetime.now(), ativo, nome_completo, grau_atual))
            
            obreiro_id = cursor.fetchone()['id']
            conn.commit()
            
            # ========== CAMPOS OPCIONAIS (UPDATE gradual) ==========
            
            # Dados pessoais
            nome_maconico = request.form.get("nome_maconico")
            if nome_maconico:
                cursor.execute("UPDATE usuarios SET nome_maconico = %s WHERE id = %s", (nome_maconico, obreiro_id))
            
            data_nascimento = request.form.get("data_nascimento")
            if data_nascimento:
                cursor.execute("UPDATE usuarios SET data_nascimento = %s WHERE id = %s", (data_nascimento, obreiro_id))
            
            cpf = request.form.get("cpf")
            if cpf:
                cursor.execute("UPDATE usuarios SET cpf = %s WHERE id = %s", (cpf, obreiro_id))
            
            tipo_sanguineo = request.form.get("tipo_sanguineo")
            if tipo_sanguineo:
                cursor.execute("UPDATE usuarios SET tipo_sanguineo = %s WHERE id = %s", (tipo_sanguineo, obreiro_id))
            
            rg = request.form.get("rg")
            if rg:
                cursor.execute("UPDATE usuarios SET rg = %s WHERE id = %s", (rg, obreiro_id))
            
            orgao_emissor = request.form.get("orgao_emissor")
            if orgao_emissor:
                cursor.execute("UPDATE usuarios SET orgao_emissor = %s WHERE id = %s", (orgao_emissor, obreiro_id))
            
            grau_instrucao = request.form.get("grau_instrucao")
            if grau_instrucao:
                cursor.execute("UPDATE usuarios SET grau_instrucao = %s WHERE id = %s", (grau_instrucao, obreiro_id))
            
            titulo_eleitor = request.form.get("titulo_eleitor")
            if titulo_eleitor:
                cursor.execute("UPDATE usuarios SET titulo_eleitor = %s WHERE id = %s", (titulo_eleitor, obreiro_id))
            
            naturalidade = request.form.get("naturalidade")
            if naturalidade:
                cursor.execute("UPDATE usuarios SET naturalidade = %s WHERE id = %s", (naturalidade, obreiro_id))
            
            estado_civil = request.form.get("estado_civil")
            if estado_civil and estado_civil != "Solteiro":
                cursor.execute("UPDATE usuarios SET estado_civil = %s WHERE id = %s", (estado_civil, obreiro_id))
            
            # Contato
            telefone = request.form.get("telefone")
            if telefone:
                cursor.execute("UPDATE usuarios SET telefone = %s WHERE id = %s", (telefone, obreiro_id))
            
            email = request.form.get("email")
            if email:
                cursor.execute("UPDATE usuarios SET email = %s WHERE id = %s", (email, obreiro_id))
            
            # Endereço
            endereco = request.form.get("endereco")
            if endereco:
                cursor.execute("UPDATE usuarios SET endereco = %s WHERE id = %s", (endereco, obreiro_id))
            
            cep = request.form.get("cep")
            if cep:
                cursor.execute("UPDATE usuarios SET cep = %s WHERE id = %s", (cep, obreiro_id))
            
            cidade = request.form.get("cidade")
            if cidade:
                cursor.execute("UPDATE usuarios SET cidade = %s WHERE id = %s", (cidade, obreiro_id))
            
            uf = request.form.get("uf")
            if uf:
                cursor.execute("UPDATE usuarios SET uf = %s WHERE id = %s", (uf, obreiro_id))
            
            bairro = request.form.get("bairro")
            if bairro:
                cursor.execute("UPDATE usuarios SET bairro = %s WHERE id = %s", (bairro, obreiro_id))
            
            numero = request.form.get("numero")
            if numero:
                cursor.execute("UPDATE usuarios SET numero = %s WHERE id = %s", (numero, obreiro_id))
            
            complemento = request.form.get("complemento")
            if complemento:
                cursor.execute("UPDATE usuarios SET complemento = %s WHERE id = %s", (complemento, obreiro_id))
            
            # Dados maçônicos
            cim_numero = request.form.get("cim_numero")
            if cim_numero:
                cursor.execute("UPDATE usuarios SET cim_numero = %s WHERE id = %s", (cim_numero, obreiro_id))
            
            data_iniciacao = request.form.get("data_iniciacao")
            if data_iniciacao:
                cursor.execute("UPDATE usuarios SET data_iniciacao = %s WHERE id = %s", (data_iniciacao, obreiro_id))
            
            data_elevacao = request.form.get("data_elevacao")
            if data_elevacao:
                cursor.execute("UPDATE usuarios SET data_elevacao = %s WHERE id = %s", (data_elevacao, obreiro_id))
            
            data_exaltacao = request.form.get("data_exaltacao")
            if data_exaltacao:
                cursor.execute("UPDATE usuarios SET data_exaltacao = %s WHERE id = %s", (data_exaltacao, obreiro_id))
            
            data_instalacao = request.form.get("data_instalacao")
            if data_instalacao:
                cursor.execute("UPDATE usuarios SET data_instalacao = %s WHERE id = %s", (data_instalacao, obreiro_id))
            
            status_maconico = request.form.get("status_maconico")
            if status_maconico and status_maconico != "Regular":
                cursor.execute("UPDATE usuarios SET status_maconico = %s WHERE id = %s", (status_maconico, obreiro_id))
            
            distincao_maconica = request.form.get("distincao_maconica")
            if distincao_maconica:
                cursor.execute("UPDATE usuarios SET distincao_maconica = %s WHERE id = %s", (distincao_maconica, obreiro_id))
            
            isento = request.form.get("isento")
            if isento and isento != "NÃO":
                cursor.execute("UPDATE usuarios SET isento = %s WHERE id = %s", (isento, obreiro_id))
            
            artigo_27 = request.form.get("artigo_27")
            if artigo_27 and artigo_27 != "NÃO":
                cursor.execute("UPDATE usuarios SET artigo_27 = %s WHERE id = %s", (artigo_27, obreiro_id))
            
            recolhe = request.form.get("recolhe")
            if recolhe and recolhe != "Sim":
                cursor.execute("UPDATE usuarios SET recolhe = %s WHERE id = %s", (recolhe, obreiro_id))
            
            loja_iniciacao = request.form.get("loja_iniciacao")
            if loja_iniciacao:
                cursor.execute("UPDATE usuarios SET loja_iniciacao = %s WHERE id = %s", (loja_iniciacao, obreiro_id))
            
            # Loja atual
            loja_nome = request.form.get("loja_nome")
            if loja_nome:
                cursor.execute("UPDATE usuarios SET loja_nome = %s WHERE id = %s", (loja_nome, obreiro_id))
            
            loja_numero = request.form.get("loja_numero")
            if loja_numero:
                cursor.execute("UPDATE usuarios SET loja_numero = %s WHERE id = %s", (loja_numero, obreiro_id))
            
            loja_orient = request.form.get("loja_orient")
            if loja_orient:
                cursor.execute("UPDATE usuarios SET loja_orient = %s WHERE id = %s", (loja_orient, obreiro_id))
            
            loja_cidade = request.form.get("loja_cidade")
            if loja_cidade:
                cursor.execute("UPDATE usuarios SET loja_cidade = %s WHERE id = %s", (loja_cidade, obreiro_id))
            
            loja_uf = request.form.get("loja_uf")
            if loja_uf:
                cursor.execute("UPDATE usuarios SET loja_uf = %s WHERE id = %s", (loja_uf, obreiro_id))
            
            # Filiação
            nome_pai = request.form.get("nome_pai")
            if nome_pai:
                cursor.execute("UPDATE usuarios SET nome_pai = %s WHERE id = %s", (nome_pai, obreiro_id))
            
            nome_mae = request.form.get("nome_mae")
            if nome_mae:
                cursor.execute("UPDATE usuarios SET nome_mae = %s WHERE id = %s", (nome_mae, obreiro_id))
            
            # Dados profissionais
            profissao = request.form.get("profissao")
            if profissao:
                cursor.execute("UPDATE usuarios SET profissao = %s WHERE id = %s", (profissao, obreiro_id))
            
            empresa = request.form.get("empresa")
            if empresa:
                cursor.execute("UPDATE usuarios SET empresa = %s WHERE id = %s", (empresa, obreiro_id))
            
            email_profissional = request.form.get("email_profissional")
            if email_profissional:
                cursor.execute("UPDATE usuarios SET email_profissional = %s WHERE id = %s", (email_profissional, obreiro_id))
            
            telefone_profissional = request.form.get("telefone_profissional")
            if telefone_profissional:
                cursor.execute("UPDATE usuarios SET telefone_profissional = %s WHERE id = %s", (telefone_profissional, obreiro_id))
            
            endereco_profissional = request.form.get("endereco_profissional")
            if endereco_profissional:
                cursor.execute("UPDATE usuarios SET endereco_profissional = %s WHERE id = %s", (endereco_profissional, obreiro_id))
            
            # Grau superior
            if grau_superior:
                cursor.execute("UPDATE usuarios SET grau_superior = %s WHERE id = %s", (grau_superior, obreiro_id))
            
            conn.commit()
            
            # Registrar histórico de grau inicial
            if data_iniciacao:
                nome_grau = get_nome_grau(grau_atual)
                try:
                    cursor.execute("""
                        INSERT INTO historico_graus (obreiro_id, grau, data, observacao)
                        VALUES (%s, %s, %s, %s)
                    """, (obreiro_id, grau_atual, data_iniciacao, f"{nome_grau} - Iniciação"))
                    conn.commit()
                except Exception as e:
                    print(f"⚠️ Erro ao registrar histórico: {e}")
            
            # Inserir dependentes se houver
            dependentes_nomes = request.form.getlist('dependente_nome[]')
            dependentes_parentescos = request.form.getlist('dependente_parentesco[]')
            dependentes_nascimentos = request.form.getlist('dependente_nascimento[]')
            
            for i in range(len(dependentes_nomes)):
                if dependentes_nomes[i] and dependentes_parentescos[i]:
                    data_nasc_dep = dependentes_nascimentos[i] if i < len(dependentes_nascimentos) else None
                    try:
                        cursor.execute("""
                            INSERT INTO dependentes (obreiro_id, nome, parentesco, data_nascimento)
                            VALUES (%s, %s, %s, %s)
                        """, (obreiro_id, dependentes_nomes[i], dependentes_parentescos[i], data_nasc_dep))
                    except Exception as e:
                        print(f"⚠️ Erro ao inserir dependente: {e}")
            
            conn.commit()
            
            registrar_log("criar", "obreiro", obreiro_id, dados_novos={"nome": nome_completo, "usuario": usuario, "grau": grau_atual})
            flash(f"Obreiro '{nome_completo}' adicionado com sucesso!", "success")
            return_connection(conn)
            return redirect("/obreiros")
            
        except psycopg2.IntegrityError as e:
            conn.rollback()
            if "usuarios_usuario_key" in str(e):
                flash("Erro: Usuário já existe! Escolha outro nome de usuário.", "danger")
            elif "usuarios_cim_numero_key" in str(e):
                flash("Erro: CIM já cadastrado para outro obreiro!", "danger")
            elif "usuarios_cpf_key" in str(e):
                flash("Erro: CPF já cadastrado para outro obreiro!", "danger")
            else:
                flash(f"Erro ao criar obreiro: {str(e)}", "danger")
        except Exception as e:
            conn.rollback()
            flash(f"Erro ao criar obreiro: {str(e)}", "danger")
        
        return_connection(conn)
        return redirect("/obreiros/novo")
    
    # GET - Carregar dados para o formulário
    cursor.execute("SELECT id, nome, numero, oriente, cidade, uf FROM lojas WHERE ativo = 1 ORDER BY nome")
    lojas = cursor.fetchall()
    
    cursor.execute("SELECT nivel, nome FROM graus WHERE nivel IN (1, 2, 3) AND ativo = 1 ORDER BY nivel")
    graus = cursor.fetchall()
    
    cursor.execute("SELECT nivel, nome FROM graus WHERE nivel >= 4 AND ativo = 1 ORDER BY nivel")
    graus_superiores = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("obreiros/novo.html", 
                          lojas=lojas, 
                          graus=graus,
                          graus_superiores=graus_superiores)
    
@app.route("/obreiros/<int:id>/editar", methods=["GET", "POST"])
@login_required
def editar_obreiro(id):
    """Edita um obreiro existente"""
    cursor, conn = get_db()

    try:
        # Buscar dados atuais
        cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
        obreiro = cursor.fetchone()

        if not obreiro:
            flash("Obreiro não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")

        is_admin = session.get("tipo") == "admin"
        is_own_profile = session.get("user_id") == id

        # Se não for admin e não for o próprio perfil, não pode editar
        if not is_admin and not is_own_profile:
            flash("Você não tem permissão para editar este obreiro", "danger")
            return_connection(conn)
            return redirect("/obreiros")

        # =============================
        # 📥 POST (SALVAR ALTERAÇÕES)
        # =============================
        if request.method == "POST":
            
            # ========== ALTERAR USUÁRIO (apenas admin) ==========
            if is_admin:
                novo_usuario = request.form.get("usuario")
                usuario_atual = obreiro["usuario"]
                
                if novo_usuario and novo_usuario != usuario_atual:
                    # Verificar se o novo usuário já existe
                    cursor.execute("SELECT id FROM usuarios WHERE usuario = %s AND id != %s", (novo_usuario, id))
                    if cursor.fetchone():
                        flash("Nome de usuário já existe! Escolha outro.", "danger")
                        return redirect(f"/obreiros/{id}/editar")
                    
                    cursor.execute("UPDATE usuarios SET usuario = %s WHERE id = %s", (novo_usuario, id))
                    registrar_log("alterar_usuario", "obreiro", id, 
                                 dados_anteriores={"usuario": usuario_atual},
                                 dados_novos={"usuario": novo_usuario})
                    flash("Nome de usuário alterado com sucesso!", "success")
                    
                    # Atualizar sessão se for o próprio perfil
                    if is_own_profile:
                        session['usuario'] = novo_usuario
            
            # Dados básicos
            nome_completo = request.form.get("nome_completo")
            nome_maconico = request.form.get("nome_maconico")
            cim_numero = request.form.get("cim_numero")
            
            # Contato
            telefone = request.form.get("telefone")
            email = request.form.get("email")
            
            # Endereço
            endereco = request.form.get("endereco")
            cep = request.form.get("cep")
            cidade = request.form.get("cidade")
            uf = request.form.get("uf")
            bairro = request.form.get("bairro")
            numero = request.form.get("numero")
            complemento = request.form.get("complemento")
            
            # Loja atual
            loja_nome = request.form.get("loja_nome")
            loja_numero = request.form.get("loja_numero")
            loja_orient = request.form.get("loja_orient")
            loja_cidade = request.form.get("loja_cidade")
            loja_uf = request.form.get("loja_uf")
            
            # Dados pessoais
            data_nascimento = request.form.get("data_nascimento") or None
            cpf = request.form.get("cpf") or None
            tipo_sanguineo = request.form.get("tipo_sanguineo") or None
            rg = request.form.get("rg") or None
            orgao_emissor = request.form.get("orgao_emissor") or None
            grau_instrucao = request.form.get("grau_instrucao") or None
            titulo_eleitor = request.form.get("titulo_eleitor") or None
            naturalidade = request.form.get("naturalidade") or None
            estado_civil = request.form.get("estado_civil") or "Solteiro"
            
            # Dados maçônicos
            data_iniciacao = request.form.get("data_iniciacao") or None
            data_elevacao = request.form.get("data_elevacao") or None
            data_exaltacao = request.form.get("data_exaltacao") or None
            data_instalacao = request.form.get("data_instalacao") or None
            status_maconico = request.form.get("status_maconico") or "Regular"
            distincao_maconica = request.form.get("distincao_maconica") or None
            isento = request.form.get("isento") or "NÃO"
            artigo_27 = request.form.get("artigo_27") or "NÃO"
            recolhe = request.form.get("recolhe") or "Sim"
            loja_iniciacao = request.form.get("loja_iniciacao") or None
            
            # Filiação
            nome_pai = request.form.get("nome_pai") or None
            nome_mae = request.form.get("nome_mae") or None
            
            # Dados profissionais
            profissao = request.form.get("profissao") or None
            empresa = request.form.get("empresa") or None
            email_profissional = request.form.get("email_profissional") or None
            telefone_profissional = request.form.get("telefone_profissional") or None
            endereco_profissional = request.form.get("endereco_profissional") or None
            
            # Senha
            senha = request.form.get("senha", "")
            senha_atual = request.form.get("senha_atual", "")
            
            # Campos de admin
            if is_admin:
                tipo = request.form.get("tipo", obreiro["tipo"])
                ativo = 1 if request.form.get("ativo") == '1' else 0
                grau_principal = request.form.get("grau_principal", 1)
                grau_superior = request.form.get("grau_superior", "")
                
                # Calcular grau atual
                if int(grau_principal) == 3 and grau_superior and grau_superior != '':
                    grau_atual = int(grau_superior)
                else:
                    grau_atual = int(grau_principal)
            else:
                tipo = obreiro["tipo"]
                ativo = obreiro["ativo"]
                grau_atual = obreiro["grau_atual"]
                grau_superior = obreiro.get("grau_superior")
            
            grau_antigo = obreiro["grau_atual"]
            
            # Validar senha
            if senha:
                if not senha_atual:
                    flash("Digite sua senha atual para alterar a senha!", "danger")
                    return redirect(f"/obreiros/{id}/editar")
                
                cursor.execute("SELECT senha_hash FROM usuarios WHERE id = %s", (id,))
                user_data = cursor.fetchone()
                
                from werkzeug.security import check_password_hash
                if not check_password_hash(user_data['senha_hash'], senha_atual):
                    flash("Senha atual incorreta!", "danger")
                    return redirect(f"/obreiros/{id}/editar")
                
                if len(senha) < 6:
                    flash("A nova senha deve ter no mínimo 6 caracteres!", "danger")
                    return redirect(f"/obreiros/{id}/editar")
                
                from werkzeug.security import generate_password_hash
                nova_senha_hash = generate_password_hash(senha)
                cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (nova_senha_hash, id))
                flash("Senha alterada com sucesso!", "success")
            
            # Validar sindicante
            if tipo == 'sindicante' and grau_atual < 3:
                flash("⚠️ Apenas obreiros com grau de Mestre (3) ou superior podem ser Sindicantes!", "danger")
                return redirect(f"/obreiros/{id}/editar")
            
            # UPDATE completo
            cursor.execute("""
                UPDATE usuarios SET
                    nome_completo = %s,
                    nome_maconico = %s,
                    cim_numero = %s,
                    telefone = %s,
                    email = %s,
                    endereco = %s,
                    cep = %s,
                    cidade = %s,
                    uf = %s,
                    bairro = %s,
                    numero = %s,
                    complemento = %s,
                    loja_nome = %s,
                    loja_numero = %s,
                    loja_orient = %s,
                    loja_cidade = %s,
                    loja_uf = %s,
                    tipo = %s,
                    ativo = %s,
                    grau_atual = %s,
                    data_nascimento = %s,
                    cpf = %s,
                    tipo_sanguineo = %s,
                    rg = %s,
                    orgao_emissor = %s,
                    grau_instrucao = %s,
                    titulo_eleitor = %s,
                    naturalidade = %s,
                    estado_civil = %s,
                    data_iniciacao = %s,
                    data_elevacao = %s,
                    data_exaltacao = %s,
                    data_instalacao = %s,
                    status_maconico = %s,
                    distincao_maconica = %s,
                    isento = %s,
                    artigo_27 = %s,
                    recolhe = %s,
                    loja_iniciacao = %s,
                    nome_pai = %s,
                    nome_mae = %s,
                    profissao = %s,
                    empresa = %s,
                    email_profissional = %s,
                    telefone_profissional = %s,
                    endereco_profissional = %s,
                    grau_superior = %s
                WHERE id = %s
            """, (
                nome_completo, nome_maconico, cim_numero, telefone, email,
                endereco, cep, cidade, uf, bairro, numero, complemento,
                loja_nome, loja_numero, loja_orient, loja_cidade, loja_uf,
                tipo, ativo, grau_atual,
                data_nascimento, cpf, tipo_sanguineo, rg, orgao_emissor,
                grau_instrucao, titulo_eleitor, naturalidade, estado_civil,
                data_iniciacao, data_elevacao, data_exaltacao, data_instalacao,
                status_maconico, distincao_maconica, isento, artigo_27, recolhe,
                loja_iniciacao, nome_pai, nome_mae, profissao, empresa,
                email_profissional, telefone_profissional, endereco_profissional,
                grau_superior if grau_superior else None,
                id
            ))
            
            conn.commit()
            
            # Registrar histórico de grau
            if is_admin and grau_atual != grau_antigo and grau_atual > 0:
                try:
                    nome_grau = get_nome_grau(grau_atual)
                    cursor.execute("""
                        INSERT INTO historico_graus (obreiro_id, grau, data, observacao)
                        VALUES (%s, %s, CURRENT_DATE, %s)
                    """, (id, grau_atual, f"Alteração de grau de {grau_antigo} para {grau_atual} - {nome_grau}"))
                    conn.commit()
                except Exception as e:
                    print(f"⚠️ Erro ao registrar histórico: {e}")
            
            # Atualizar sessão
            if is_own_profile:
                session['nome_completo'] = nome_completo
                session['grau_atual'] = grau_atual
                session['tipo'] = tipo
            
            registrar_log("editar", "obreiro", id, dados_novos={"nome": nome_completo})
            flash("Obreiro atualizado com sucesso!", "success")
            return_connection(conn)
            return redirect(f"/obreiros/{id}")

        # =============================
        # 📊 GET (CARREGAR TELA)
        # =============================
        cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
        obreiro = cursor.fetchone()

        cursor.execute("SELECT id, nome, numero, oriente, cidade, uf FROM lojas WHERE ativo = 1 ORDER BY nome")
        lojas = cursor.fetchall()

        cursor.execute("SELECT nivel, nome FROM graus WHERE nivel IN (1, 2, 3) AND ativo = 1 ORDER BY nivel")
        graus = cursor.fetchall()
        
        cursor.execute("SELECT nivel, nome FROM graus WHERE nivel >= 4 AND ativo = 1 ORDER BY nivel")
        graus_superiores = cursor.fetchall()

        return_connection(conn)

        return render_template(
            "obreiros/editar.html",
            obreiro=obreiro,
            lojas=lojas,
            graus=graus,
            graus_superiores=graus_superiores,
            is_admin=is_admin,
            is_own_profile=is_own_profile
        )
                              
    except Exception as e:
        print(f"❌ Erro ao editar obreiro: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
        flash(f"Erro ao atualizar: {str(e)}", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{id}")

@app.route("/obreiros/<int:id>")
@login_required
def visualizar_obreiro(id):
    cursor, conn = get_db()
    
    try:
        # Buscar dados do obreiro
        cursor.execute("""
            SELECT u.*, l.nome as loja_nome_completo, l.cidade as loja_cidade, l.uf as loja_uf
            FROM usuarios u
            LEFT JOIN lojas l ON u.loja_nome = l.nome
            WHERE u.id = %s
        """, (id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("Obreiro não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        # ============================================
        # BUSCAR HISTÓRICO DO CANDIDATO (ORIGEM)
        # ============================================
        cursor.execute("""
            SELECT 
                c.id as candidato_id,
                c.nome as candidato_nome,
                c.cpf,
                c.data_criacao,
                c.status,
                c.data_fechamento as data_aprovacao,
                c.numero_placet,
                c.data_transformacao,
                c.data_iniciacao,
                COALESCE(pc.total_votos, 0) as total_votos,
                COALESCE(pc.votos_positivos, 0) as votos_positivos,
                COALESCE(pc.votos_negativos, 0) as votos_negativos,
                (SELECT parecer_texto FROM pareceres_conclusivos 
                 WHERE candidato_id = c.id LIMIT 1) as parecer_final
            FROM candidatos c
            LEFT JOIN (
                SELECT 
                    candidato_id,
                    COUNT(*) as total_votos,
                    COUNT(CASE WHEN conclusao = 'APROVADO' THEN 1 END) as votos_positivos,
                    COUNT(CASE WHEN conclusao = 'REPROVADO' THEN 1 END) as votos_negativos
                FROM pareceres_conclusivos
                GROUP BY candidato_id
            ) pc ON c.id = pc.candidato_id
            WHERE c.obreiro_id = %s
        """, (id,))
        historico_candidato = cursor.fetchone()
        
        # ============================================
        # BUSCAR CARGO ATUAL DO OBREIRO (ativo)
        # ============================================
        cursor.execute("""
            SELECT c.nome as cargo_nome, c.sigla, c.grau_minimo, oc.data_inicio, oc.id as ocupacao_id
            FROM ocupacao_cargos oc
            JOIN cargos c ON oc.cargo_id = c.id
            WHERE oc.obreiro_id = %s AND oc.ativo = 1
            ORDER BY oc.data_inicio DESC
            LIMIT 1
        """, (id,))
        cargo_atual_row = cursor.fetchone()
        cargo_atual = cargo_atual_row['cargo_nome'] if cargo_atual_row else None
        
        # ============================================
        # BUSCAR TODOS OS CARGOS DO OBREIRO (apenas ativos)
        # ============================================
        cursor.execute("""
            SELECT oc.*, c.nome as cargo_nome, c.sigla, c.grau_minimo
            FROM ocupacao_cargos oc
            JOIN cargos c ON oc.cargo_id = c.id
            WHERE oc.obreiro_id = %s AND oc.ativo = 1
            ORDER BY oc.data_inicio DESC
        """, (id,))
        cargos = cursor.fetchall()
        
        # ============================================
        # BUSCAR HISTÓRICO DE GRAUS
        # ============================================
        cursor.execute("""
            SELECT h.*, 
                   CASE 
                       WHEN h.grau = 1 THEN 'Aprendiz'
                       WHEN h.grau = 2 THEN 'Companheiro'
                       WHEN h.grau = 3 THEN 'Mestre'
                       WHEN h.grau = 4 THEN 'Mestre Instalado'
                       WHEN h.grau = 5 THEN 'Arquiteto Real'
                       WHEN h.grau = 6 THEN 'Soberano Grande Inspetor Geral'
                       ELSE CONCAT('Grau ', h.grau)
                   END as nome_grau
            FROM historico_graus h
            WHERE h.obreiro_id = %s
            ORDER BY h.data DESC
        """, (id,))
        historico_graus = cursor.fetchall()
        
        # ============================================
        # CONTAR FAMILIARES
        # ============================================
        familiares_count = 0
        try:
            cursor.execute("SELECT COUNT(*) as total FROM familiares WHERE obreiro_id = %s", (id,))
            result = cursor.fetchone()
            if result:
                familiares_count = result["total"]
        except Exception as e:
            print(f"Erro ao contar familiares: {e}")
        
        # ============================================
        # CONTAR CONDECORAÇÕES
        # ============================================
        try:
            cursor.execute("SELECT COUNT(*) as total FROM condecoracoes_obreiro WHERE obreiro_id = %s", (id,))
            result = cursor.fetchone()
            condecoracoes_count = result["total"] if result else 0
        except:
            condecoracoes_count = 0
        
        # ============================================
        # CONTAR COMUNICADOS
        # ============================================
        try:
            cursor.execute("SELECT COUNT(*) as total FROM comunicados_obreiro WHERE obreiro_id = %s AND ativo = 1", (id,))
            result = cursor.fetchone()
            comunicados_count = result["total"] if result else 0
        except:
            comunicados_count = 0
        
        # ============================================
        # CARGOS DISPONÍVEIS PARA ADICIONAR (apenas admin)
        # ============================================
        cargos_disponiveis = []
        if session.get("tipo") == "admin":
            cursor.execute("""
                SELECT id, nome, sigla, grau_minimo, descricao 
                FROM cargos 
                WHERE ativo = 1 
                ORDER BY grau_minimo ASC, ordem ASC, nome
            """)
            cargos_disponiveis = cursor.fetchall()
        
        # ============================================
        # GRAUS DISPONÍVEIS (apenas admin)
        # ============================================
        graus_disponiveis = []
        if session.get("tipo") == "admin":
            cursor.execute("SELECT nivel, nome FROM graus WHERE ativo = 1 ORDER BY nivel")
            graus_disponiveis = cursor.fetchall()
        
        return_connection(conn)
        
        pode_editar = (session.get("tipo") == "admin" or session.get("user_id") == id)
        
        return render_template("obreiros/visualizar.html",
                              obreiro=obreiro,
                              cargo_atual=cargo_atual,
                              cargos=cargos,
                              historico_graus=historico_graus,
                              cargos_disponiveis=cargos_disponiveis,
                              graus_disponiveis=graus_disponiveis,
                              familiares_count=familiares_count,
                              condecoracoes_count=condecoracoes_count,
                              comunicados_count=comunicados_count,
                              pode_editar=pode_editar,
                              historico_candidato=historico_candidato)  # ← NOVO
        
    except Exception as e:
        print(f"❌ Erro ao visualizar obreiro {id}: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            return_connection(conn)
        flash(f"Erro ao carregar dados do obreiro: {str(e)}", "danger")
        return redirect("/obreiros")
    
@app.route("/obreiros/<int:id>/foto", methods=["POST"])
@login_required
def upload_foto(id):
    import cloudinary.uploader

    # 🔐 Validação de permissão
    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para esta ação", "danger")
        return redirect(f"/obreiros/{id}/editar")

    foto = request.files.get("foto")

    if not foto or foto.filename == "":
        flash("Nenhuma foto selecionada", "danger")
        return redirect(f"/obreiros/{id}/editar")

    try:
        # 🚀 Upload para Cloudinary com otimização
        resultado = cloudinary.uploader.upload(
            foto,
            folder="obreiros",
            transformation=[
                {"width": 300, "height": 300, "crop": "fill"}
            ]
        )

        url = resultado["secure_url"]

        # 💾 Salvar URL no banco
        cursor, conn = get_db()
        cursor.execute(
            "UPDATE usuarios SET foto=%s WHERE id=%s",
            (url, id)
        )
        conn.commit()
        return_connection(conn)

        flash("Foto atualizada com sucesso!", "success")

    except Exception as e:
        flash(f"Erro ao enviar foto: {str(e)}", "danger")

    return redirect(f"/obreiros/{id}/editar")

@app.route("/obreiros/<int:id>/foto/remover")
@login_required
def remover_foto_obreiro(id):
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
    from flask import send_from_directory
    return send_from_directory(UPLOAD_FOLDER_FOTOS, filename)

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
        # Buscar dados do obreiro e cargo
        cursor.execute("SELECT id, nome_completo FROM usuarios WHERE id = %s", (id,))
        obreiro = cursor.fetchone()
        
        cursor.execute("SELECT id, nome FROM cargos WHERE id = %s AND ativo = 1", (cargo_id,))
        cargo = cursor.fetchone()
        
        if not cargo:
            flash("Cargo não encontrado ou inativo!", "danger")
            return_connection(conn)
            return redirect(f"/obreiros/{id}")
        
        cursor.execute("""
            INSERT INTO ocupacao_cargos (obreiro_id, cargo_id, data_inicio, gestao, ativo)
            VALUES (%s, %s, %s, %s, 1)
            RETURNING id
        """, (id, cargo_id, data_inicio, gestao))
        
        novo_id = cursor.fetchone()['id']
        conn.commit()
        
        registrar_log("atribuir_cargo", "cargo", cargo_id, 
                     dados_novos={"ocupacao_id": novo_id, "obreiro_id": id, "obreiro_nome": obreiro['nome_completo'], 
                                  "cargo_id": cargo_id, "cargo_nome": cargo['nome'], "data_inicio": data_inicio, "gestao": gestao})
        
        flash(f"Cargo '{cargo['nome']}' atribuído com sucesso!", "success")
        
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
        # Buscar dados do cargo antes de remover
        cursor.execute("""
            SELECT oc.*, c.nome as cargo_nome, u.nome_completo as obreiro_nome
            FROM ocupacao_cargos oc
            JOIN cargos c ON oc.cargo_id = c.id
            JOIN usuarios u ON oc.obreiro_id = u.id
            WHERE oc.id = %s
        """, (id,))
        cargo = cursor.fetchone()
        
        if not cargo:
            flash("Cargo não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        obreiro_id = cargo["obreiro_id"]
        
        cursor.execute("UPDATE ocupacao_cargos SET ativo = 0 WHERE id = %s", (id,))
        conn.commit()
        
        registrar_log("remover_cargo", "cargo", cargo["cargo_id"], 
                     dados_anteriores={"obreiro_id": obreiro_id, "obreiro_nome": cargo['obreiro_nome'], 
                                      "cargo_nome": cargo['cargo_nome'], "data_inicio": str(cargo['data_inicio'])})
        
        flash("Cargo removido com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao remover cargo: {e}")
        conn.rollback()
        flash(f"Erro ao remover cargo: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/obreiros/{obreiro_id}")

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
        # Buscar dados antigos do obreiro
        cursor.execute("SELECT id, nome_completo, usuario, grau_atual FROM usuarios WHERE id = %s", (id,))
        obreiro_antigo = cursor.fetchone()
        
        cursor.execute("SELECT id, nome, nivel FROM graus WHERE id = %s", (grau_id,))
        grau = cursor.fetchone()
        
        if not grau:
            flash("Grau não encontrado", "danger")
            return_connection(conn)
            return redirect(f"/obreiros/{id}")
        
        cursor.execute("""
            INSERT INTO historico_graus (obreiro_id, grau, grau_id, data, observacao)
            VALUES (%s, %s, %s, %s, %s)
        """, (id, grau['nivel'], grau_id, data, observacao))
        
        cursor.execute("UPDATE usuarios SET grau_atual = %s WHERE id = %s", (grau['nivel'], id))
        conn.commit()
        
        # Registrar log com detalhes da alteração
        registrar_log("registrar_grau", "obreiro", id, 
                     dados_anteriores={"grau_anterior": obreiro_antigo['grau_atual'], "nome": obreiro_antigo['nome_completo']},
                     dados_novos={"grau_novo": grau['nivel'], "grau_nome": grau['nome'], "data": data, "observacao": observacao})
        
        flash(f"Grau '{grau['nome']}' registrado com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao registrar grau: {e}")
        conn.rollback()
        flash(f"Erro ao registrar grau: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/obreiros/{id}")

@app.route("/obreiros/<int:id>/documentos")
@login_required
def listar_documentos(id):
    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para acessar esta página", "danger")
        return redirect("/obreiros")
    cursor, conn = get_db()
    cursor.execute("SELECT nome_completo FROM usuarios WHERE id = %s", (id,))
    obreiro = cursor.fetchone()
    if not obreiro:
        flash("Obreiro não encontrado", "danger")
        return_connection(conn)
        return redirect("/obreiros")
    cursor.execute("""
        SELECT d.*, c.nome as categoria_nome, c.icone
        FROM documentos_obreiro d
        LEFT JOIN categorias_documentos c ON d.categoria = c.nome
        WHERE d.obreiro_id = %s
        ORDER BY d.data_upload DESC
    """, (id,))
    documentos = cursor.fetchall()
    cursor.execute("SELECT * FROM categorias_documentos WHERE ativo = 1 ORDER BY nome")
    categorias = cursor.fetchall()
    return_connection(conn)
    return render_template("obreiros/documentos.html", obreiro_id=id, obreiro_nome=obreiro["nome_completo"],
                          documentos=documentos, categorias=categorias)

@app.route("/obreiros/<int:id>/documentos/upload", methods=["POST"])
@login_required
def upload_documento(id):
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

        filename = secure_filename(arquivo.filename)
        extensao = filename.split('.')[-1]

        # 🔥 Upload para Cloudinary
        resultado = cloudinary.uploader.upload(
            arquivo,
            resource_type="auto",  # aceita pdf, zip, doc, etc
            folder="documentos_obreiro"
        )

        url_arquivo = resultado['secure_url']
        public_id = resultado['public_id']
        tamanho = resultado.get('bytes', 0)

        # Banco de dados
        cursor, conn = get_db()
        cursor.execute("""
            INSERT INTO documentos_obreiro 
            (obreiro_id, titulo, descricao, categoria, tipo_arquivo, nome_arquivo, caminho_arquivo, tamanho, uploaded_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            id,
            titulo,
            descricao,
            categoria,
            extensao,
            public_id,     # 👈 substitui nome_arquivo
            url_arquivo,   # 👈 agora é URL, não caminho local
            tamanho,
            session["user_id"]
        ))

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
    return send_file(doc["caminho_arquivo"], as_attachment=True, download_name=doc["nome_arquivo"], mimetype="application/octet-stream")

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
    if session["tipo"] != "admin" and session["user_id"] != doc["obreiro_id"]:
        flash("Você não tem permissão para excluir este documento", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{doc['obreiro_id']}/documentos")
    try:
        if os.path.exists(doc["caminho_arquivo"]):
            os.remove(doc["caminho_arquivo"])
        cursor.execute("DELETE FROM documentos_obreiro WHERE id = %s", (id,))
        conn.commit()
        registrar_log("excluir_documento", "documento", id, dados_anteriores={"titulo": doc["titulo"]})
        flash("Documento excluído com sucesso!", "success")
    except Exception as e:
        flash(f"Erro ao excluir documento: {str(e)}", "danger")
    return_connection(conn)
    return redirect(f"/obreiros/{doc['obreiro_id']}/documentos")
    

@app.route("/recuperar-senha", methods=["GET", "POST"])
def recuperar_senha():
    """Página para solicitar recuperação de senha"""
    
    if request.method == "POST":
        email = request.form.get("email")
        
        if not email:
            flash("Digite seu e-mail!", "danger")
            return redirect("/recuperar-senha")
        
        try:
            # get_db() retorna (cursor, conn)
            cursor, conn = get_db()
            
            cursor.execute("SELECT id, nome_completo, email FROM usuarios WHERE email = %s", (email,))
            usuario = cursor.fetchone()
            
            if usuario:
                import secrets
                token = secrets.token_urlsafe(32)
                expira_em = datetime.utcnow() + timedelta(hours=1)
                
                # Criar tabela se não existir
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS password_reset_tokens (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER NOT NULL,
                        token VARCHAR(255) NOT NULL UNIQUE,
                        expira_em TIMESTAMP NOT NULL,
                        usado BOOLEAN DEFAULT FALSE,
                        criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()
                
                # Salvar token no banco
                cursor.execute("""
                    INSERT INTO password_reset_tokens (usuario_id, token, expira_em, usado)
                    VALUES (%s, %s, %s, FALSE)
                """, (usuario['id'], token, expira_em))
                conn.commit()
                
                # Construir link de recuperação
                link_recuperacao = url_for('redefinir_senha', token=token, _external=True)
                
                               
                # Preparar e-mail
                assunto = "🔐 Recuperação de Senha - ARLS Bicentenário"
                
                html_content = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="UTF-8">
                    <title>Recuperação de Senha</title>
                </head>
                <body style="font-family: Arial, sans-serif;">
                    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                        <h2>🔐 Recuperação de Senha</h2>
                        <p>Olá, <strong>{usuario['nome_completo']}</strong>!</p>
                        <p>Clique no link abaixo para redefinir sua senha:</p>
                        <p><a href="{link_recuperacao}">{link_recuperacao}</a></p>
                        <p>Este link é válido por 1 hora.</p>
                        <hr>
                        <small>ARLS Bicentenário</small>
                    </div>
                </body>
                </html>
                """
                
                texto_alternativo = f"""
                Recuperação de Senha - ARLS Bicentenário
                
                Olá {usuario['nome_completo']}!
                
                Para redefinir sua senha, acesse:
                {link_recuperacao}
                
                Este link é válido por 1 hora.
                """
                
                # Enviar e-mail
                resultado = enviar_email_resend(
                    destinatario=email,
                    assunto=assunto,
                    conteudo_html=html_content,
                    conteudo_texto=texto_alternativo
                )
                
                if resultado['success']:
                    flash("✅ Link de recuperação enviado para seu e-mail! Verifique sua caixa de entrada.", "success")
                else:
                    flash(f"❌ Erro ao enviar e-mail: {resultado['message']}", "danger")
                
            else:
                flash("Se o e-mail estiver cadastrado, você receberá as instruções.", "info")
            
            return_connection(conn)
            
        except Exception as e:
            print(f"❌ Erro na recuperação: {e}")
            traceback.print_exc()
            if 'conn' in locals():
                return_connection(conn)
            flash("Erro ao processar solicitação. Tente novamente.", "danger")
        
        return redirect("/login")
    
    return render_template("recuperar_senha.html")
    
    
@app.route("/redefinir-senha", methods=["GET", "POST"])
def redefinir_senha():
    """Redefine a senha usando token de recuperação"""
    
    token = request.args.get("token") or request.form.get("token")
    
    if not token:
        flash("Token inválido!", "danger")
        return redirect("/login")
    
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT usuario_id FROM password_reset_tokens 
            WHERE token = %s 
            AND expira_em > NOW()
            AND usado = FALSE
        """, (token,))
        
        result = cursor.fetchone()
        
        if not result:
            return_connection(conn)
            flash("Link inválido ou expirado! Solicite uma nova recuperação.", "danger")
            return redirect("/recuperar-senha")
        
        usuario_id = result['usuario_id']
        
        if request.method == "POST":
            nova_senha = request.form.get("nova_senha")
            confirmar_senha = request.form.get("confirmar_senha")
            
            if not nova_senha or len(nova_senha) < 6:
                flash("A senha deve ter no mínimo 6 caracteres!", "danger")
            elif nova_senha != confirmar_senha:
                flash("As senhas não coincidem!", "danger")
            else:
                # Gerar novo hash
                senha_hash = generate_password_hash(nova_senha)
                
                # Atualizar senha
                cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (senha_hash, usuario_id))
                
                # Marcar token como usado
                cursor.execute("UPDATE password_reset_tokens SET usado = TRUE WHERE token = %s", (token,))
                conn.commit()
                
                return_connection(conn)
                
                flash("✅ Senha redefinida com sucesso! Faça login com sua nova senha.", "success")
                return redirect("/login")
        
        return_connection(conn)
        return render_template("redefinir_senha.html", token=token)
        
    except Exception as e:
        print(f"Erro ao redefinir senha: {e}")
        traceback.print_exc()
        if 'conn' in locals():
            return_connection(conn)
        flash("Erro ao processar solicitação. Tente novamente.", "danger")
        return redirect("/login")
  

@app.route("/admin/verificar-tabelas", methods=["GET"])
@admin_required
def verificar_tabelas():
    """Rota para verificar quais tabelas existem no banco de dados"""
    if session.get("tipo") != "admin":
        flash("Acesso restrito a administradores", "danger")
        return redirect("/dashboard")
    
    cursor, conn = get_db()
    
    # Lista de tabelas esperadas
    tabelas_esperadas = [
        'notificacoes', 'sugestoes', 'votos_sugestao', 
        'comentarios_sugestao', 'categorias_sugestoes'
    ]
    
    resultados = []
    
    for tabela in tabelas_esperadas:
        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (tabela,))
            existe = cursor.fetchone()[0]
            
            if existe:
                # Obter contagem de registros
                cursor.execute(f"SELECT COUNT(*) as total FROM {tabela}")
                total = cursor.fetchone()[0]
                resultados.append({
                    'nome': tabela,
                    'existe': True,
                    'total': total,
                    'status': 'success'
                })
            else:
                resultados.append({
                    'nome': tabela,
                    'existe': False,
                    'total': 0,
                    'status': 'error'
                })
        except Exception as e:
            resultados.append({
                'nome': tabela,
                'existe': False,
                'total': 0,
                'status': 'warning',
                'erro': str(e)
            })
    
    return_connection(conn)
    
    # Gerar HTML com resultados
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Verificação de Tabelas</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .container { max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; }
            th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
            th { background-color: #4CAF50; color: white; }
            .success { color: #155724; background-color: #d4edda; }
            .error { color: #721c24; background-color: #f8d7da; }
            .warning { color: #856404; background-color: #fff3cd; }
            .btn { display: inline-block; padding: 10px 15px; background: #4CAF50; color: white; text-decoration: none; border-radius: 4px; margin-top: 20px; margin-right: 10px; }
            .btn-blue { background: #2196F3; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔍 Verificação de Tabelas</h1>
            <p>Executado em: """ + datetime.now().strftime('%d/%m/%Y %H:%M:%S') + """</p>
            
            <table>
                <thead>
                    <tr>
                        <th>Tabela</th>
                        <th>Status</th>
                        <th>Registros</th>
                        <th>Detalhes</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for r in resultados:
        if r['status'] == 'success':
            status_html = '<span style="color: green;">✅ Existe</span>'
            detalhes = f"{r['total']} registro(s)"
        elif r['status'] == 'error':
            status_html = '<span style="color: red;">❌ Não existe</span>'
            detalhes = 'Tabela não encontrada - executar migração'
        else:
            status_html = '<span style="color: orange;">⚠️ Erro</span>'
            detalhes = r.get('erro', 'Erro desconhecido')[:100]
        
        html += f"""
            <tr class="{r['status']}">
                <td><strong>{r['nome']}</strong></td>
                <td>{status_html}</td>
                <td>{r['total']}</td>
                <td>{detalhes}</td>
            </tr>
        """
    
    html += """
                </tbody>
            </table>
            
            <div style="margin-top: 20px;">
                <a href="/admin/migrar-tabelas" class="btn">🔧 Executar Migração</a>
                <a href="/dashboard" class="btn btn-blue">← Voltar ao Dashboard</a>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html

@app.route("/api/backup/restaurar-externo", methods=["POST"])
@admin_required
def api_restaurar_backup_externo():
    """Restaura backup a partir de um arquivo enviado pelo usuário"""
    try:
        # Verificar se arquivo foi enviado
        if 'arquivo_backup' not in request.files:
            return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'}), 400
        
        arquivo = request.files['arquivo_backup']
        
        if arquivo.filename == '':
            return jsonify({'success': False, 'error': 'Nenhum arquivo selecionado'}), 400
        
        # Verificar extensão
        if not arquivo.filename.endswith('.zip'):
            return jsonify({'success': False, 'error': 'Formato inválido. Use arquivos .zip'}), 400
        
        # Criar backup de emergência antes da restauração
        print("Criando backup de emergência...")
        emergency_backup = criar_backup_sistema()
        
        if not emergency_backup['success']:
            return jsonify({
                'success': False,
                'error': 'Não foi possível criar backup de emergência. Operação cancelada.',
                'emergency_backup': None
            }), 500
        
        # Salvar arquivo temporariamente
        temp_dir = os.path.join(TEMP_RESTORE_DIR, f'upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Salvar arquivo enviado
        backup_path = os.path.join(temp_dir, arquivo.filename)
        arquivo.save(backup_path)
        
        # Verificar integridade do ZIP
        try:
            with zipfile.ZipFile(backup_path, 'r') as zf:
                # Testar se o zip está íntegro
                bad_file = zf.testzip()
                if bad_file:
                    raise Exception(f"Arquivo ZIP corrompido: {bad_file}")
        except Exception as e:
            shutil.rmtree(temp_dir)
            return jsonify({'success': False, 'error': f'Arquivo ZIP inválido: {str(e)}'}), 400
        
        # Restaurar usando a função existente
        result = restaurar_backup_sistema_arquivo(backup_path, emergency_backup)
        
        # Limpar diretório temporário
        shutil.rmtree(temp_dir)
        
        return jsonify(result)
        
    except Exception as e:
        print(f"Erro ao restaurar backup externo: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

def restaurar_backup_sistema_arquivo(backup_path, emergency_backup):
    """Restaura um backup a partir de um arquivo específico"""
    temp_dir = None
    try:
        # Validar arquivo
        if not os.path.exists(backup_path):
            return {'success': False, 'error': 'Arquivo de backup não encontrado'}
        
        # Criar diretório temporário
        temp_dir = os.path.join(TEMP_RESTORE_DIR, f'restore_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Extrair arquivo
        with zipfile.ZipFile(backup_path, 'r') as zf:
            zf.extractall(temp_dir)
            extracted_files = os.listdir(temp_dir)
            
            # Verificar tipo de backup
            dump_files = [f for f in extracted_files if f.endswith('.dump')]
            sql_files = [f for f in extracted_files if f.endswith('.sql')]
            
            if dump_files:
                # Restaurar com pg_restore
                dump_file = os.path.join(temp_dir, dump_files[0])
                
                import urllib.parse
                db_url = DATABASE_URL
                
                if db_url.startswith('postgresql://'):
                    parsed = urllib.parse.urlparse(db_url)
                    db_host = parsed.hostname
                    db_port = parsed.port or 5432
                    db_name_parsed = parsed.path[1:] if parsed.path else 'sistema_maconico'
                    db_user = parsed.username
                    db_password = parsed.password
                    
                    cmd = [
                        'pg_restore',
                        '-h', db_host,
                        '-p', str(db_port),
                        '-U', db_user,
                        '-d', db_name_parsed,
                        '--clean',
                        '--if-exists',
                        '--no-owner',
                        '--no-privileges',
                        dump_file
                    ]
                    
                    env = os.environ.copy()
                    env['PGPASSWORD'] = db_password
                    
                    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
                    
                    if result.returncode != 0:
                        raise Exception(f"pg_restore falhou: {result.stderr}")
                    
                    statements_executed = "pg_restore completed"
                    
            elif sql_files:
                # Restaurar com SQL
                sql_file_path = os.path.join(temp_dir, sql_files[0])
                
                # Conectar ao banco
                conn = psycopg2.connect(DATABASE_URL)
                conn.autocommit = False
                cursor = conn.cursor()
                
                # Ler e executar SQL
                with open(sql_file_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                # Separar comandos SQL
                statements = []
                current = []
                in_string = False
                string_char = None
                
                for line in sql_content.split('\n'):
                    stripped = line.strip()
                    
                    # Ignorar comentários de linha
                    if stripped.startswith('--') and not in_string:
                        continue
                    
                    current.append(line)
                    
                    # Verificar se estamos dentro de uma string
                    for char in line:
                        if char in ("'", '"') and not in_string:
                            in_string = True
                            string_char = char
                        elif char == string_char and in_string:
                            # Verificar se não é escape
                            idx = line.find(char)
                            if idx > 0 and line[idx-1] == '\\':
                                continue
                            in_string = False
                            string_char = None
                    
                    # Se não estamos dentro de string e linha termina com ;
                    if not in_string and stripped.endswith(';'):
                        statements.append('\n'.join(current))
                        current = []
                
                if current:
                    statements.append('\n'.join(current))
                
                # Executar statements
                executed = 0
                errors = []
                
                for i, stmt in enumerate(statements):
                    if stmt.strip() and not stmt.strip().startswith('--'):
                        try:
                            cursor.execute(stmt)
                            executed += 1
                        except Exception as e:
                            errors.append(f"Statement {i+1}: {str(e)[:200]}")
                            print(f"Erro na statement {i+1}: {e}")
                            print(f"SQL: {stmt[:300]}")
                            
                            # Se erro crítico, abortar
                            if 'syntax error' in str(e).lower():
                                raise Exception(f"Erro de sintaxe: {str(e)}")
                
                if errors and not executed:
                    conn.rollback()
                    raise Exception(f"{len(errors)} erros encontrados. Primeiro erro: {errors[0]}")
                
                conn.commit()
                cursor.close()
                conn.close()
                
                statements_executed = executed
            else:
                raise Exception("Formato de backup não reconhecido. Use arquivo .dump ou .sql dentro do ZIP")
        
        # Limpar diretório temporário
        shutil.rmtree(temp_dir)
        
        result = {
            'success': True,
            'message': f'Backup restaurado com sucesso!',
            'emergency_backup': emergency_backup.get('filename'),
            'emergency_backup_size': emergency_backup.get('size_mb'),
            'statements_executed': statements_executed,
            'restored_from': os.path.basename(backup_path)
        }
        
        log_backup_operation('restore_externo', os.path.basename(backup_path), True, result)
        return result
        
    except Exception as e:
        print(f"Erro ao restaurar backup: {e}")
        traceback.print_exc()
        
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
        
        error_msg = str(e)
        log_backup_operation('restore_externo', os.path.basename(backup_path) if backup_path else None, False, error=error_msg)
        
        return {
            'success': False,
            'error': error_msg,
            'emergency_backup': emergency_backup.get('filename') if 'emergency_backup' in locals() else None
        }    
    

# =============================
# ROTAS DE FAMILIARES
# =============================
@app.route("/obreiros/<int:obreiro_id>/familiares")
@login_required
def listar_familiares(obreiro_id):
    """Lista familiares do obreiro"""
    cursor, conn = get_db()
    
    # Verificar permissão: admin, mestre (grau >= 3) ou o próprio obreiro
    usuario_tipo = session.get('tipo', '')
    usuario_grau = session.get('grau_atual', 0)
    
    if usuario_tipo != 'admin' and usuario_grau < 3 and session["user_id"] != obreiro_id:
        flash("Você não tem permissão para acessar esta página", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    
    try:
        cursor.execute("SELECT id, nome_completo FROM usuarios WHERE id = %s", (obreiro_id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("Obreiro não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        cursor.execute("SELECT * FROM familiares WHERE obreiro_id = %s", (obreiro_id,))
        familiares = cursor.fetchall()
        familiares_list = [dict(f) for f in familiares]
        
    except Exception as e:
        print(f"ERRO: {e}")
        familiares_list = []
        flash(f"Erro ao carregar familiares: {str(e)}", "danger")
    
    return_connection(conn)
    
    # Verificar se pode editar (apenas admin ou o próprio obreiro)
    pode_editar = (session["tipo"] == "admin" or session["user_id"] == obreiro_id)
    
    return render_template("obreiros/familiares.html", 
                          obreiro=obreiro, 
                          familiares=familiares_list, 
                          obreiro_id=obreiro_id,
                          pode_editar=pode_editar)

@app.route("/obreiros/<int:obreiro_id>/familiares/novo", methods=["GET", "POST"])
@login_required
def novo_familiar(obreiro_id):
    """Cria um novo familiar (apenas admin ou o próprio obreiro)"""
    cursor, conn = get_db()
    
    # Verificar permissão: apenas admin ou o próprio obreiro
    if session["tipo"] != 'admin' and session["user_id"] != obreiro_id:
        flash("Você não tem permissão para adicionar familiares", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    
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
                data_nascimento = data_nascimento if data_nascimento and data_nascimento.strip() else None
                cursor.execute("""
                    INSERT INTO familiares 
                    (obreiro_id, nome, parentesco, data_nascimento, telefone, email, 
                     observacoes, receber_notificacoes, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (obreiro_id, nome, parentesco, data_nascimento, telefone, 
                      email, observacoes, receber_notificacoes, session["user_id"]))
                conn.commit()
                registrar_log("criar_familiar", "familiar", cursor.lastrowid, dados_novos={"nome": nome, "parentesco": parentesco})
                flash(f"Familiar '{nome}' adicionado com sucesso!", "success")
                return_connection(conn)
                return redirect(f"/obreiros/{obreiro_id}/familiares")
            except Exception as e:
                flash(f"Erro ao adicionar familiar: {str(e)}", "danger")
                conn.rollback()
    
    return_connection(conn)
    return render_template("obreiros/familiar_form.html", obreiro=obreiro, obreiro_id=obreiro_id, familiar=None)

@app.route("/obreiros/familiares/editar/<int:id>", methods=["GET", "POST"])
@login_required
def editar_familiar(id):
    """Edita um familiar (apenas admin)"""
    cursor, conn = get_db()
    
    # Apenas admin pode editar
    if session["tipo"] != 'admin':
        flash("Apenas administradores podem editar familiares", "danger")
        return redirect("/obreiros")
    
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
                data_nascimento = data_nascimento if data_nascimento and data_nascimento.strip() else None
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
                registrar_log("editar_familiar", "familiar", id, dados_anteriores=dados_antigos,
                             dados_novos={"nome": nome, "parentesco": parentesco})
                flash("Familiar atualizado com sucesso!", "success")
                return_connection(conn)
                return redirect(f"/obreiros/{familiar['obreiro_id']}/familiares")
            except Exception as e:
                flash(f"Erro ao atualizar familiar: {str(e)}", "danger")
                conn.rollback()
    
    return_connection(conn)
    return render_template("obreiros/familiar_form.html", obreiro={"nome_completo": familiar["obreiro_nome"]},
                          obreiro_id=familiar["obreiro_id"], familiar=familiar)

@app.route("/obreiros/familiares/excluir/<int:id>")
@login_required
def excluir_familiar(id):
    cursor, conn = get_db()
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

# =============================
# ROTAS DE REUNIÕES
# =============================
@app.route("/reunioes/<int:reuniao_id>/presenca/lote", methods=["POST"])
@login_required
@admin_required
def registrar_presenca_lote(reuniao_id):
    cursor, conn = get_db()
    
    try:
        # Buscar apenas obreiros ATIVOS (incluindo sindicantes ativos)
        cursor.execute("""
            SELECT id, nome_completo, tipo
            FROM usuarios 
            WHERE ativo = 1 
              AND tipo IN ('admin', 'obreiro', 'sindicante')
            ORDER BY nome_completo
        """)
        todos_obreiros = cursor.fetchall()
        
        print(f"Total de obreiros ativos: {len(todos_obreiros)}")
        
        # Coletar IDs dos presentes
        presentes_ids = set()
        for key, value in request.form.items():
            if key.startswith('presenca_') and not key.endswith('_hidden') and value == '1':
                try:
                    obreiro_id = int(key.replace('presenca_', ''))
                    presentes_ids.add(obreiro_id)
                except ValueError:
                    continue
        
        print(f"Presentes IDs: {presentes_ids}")
        
        # Salvar presenças
        for obreiro in todos_obreiros:
            obreiro_id = obreiro['id']
            presente = True if obreiro_id in presentes_ids else False
            
            tipo_ausencia = None
            justificativa = None
            
            if not presente:
                tipo_ausencia = request.form.get(f'tipo_ausencia_{obreiro_id}', '')
                justificativa = request.form.get(f'justificativa_{obreiro_id}', '')
                
                if not tipo_ausencia:
                    tipo_ausencia = None
                if not justificativa:
                    justificativa = None
            
            cursor.execute("""
                INSERT INTO presenca_reuniao (reuniao_id, obreiro_id, presente, tipo_ausencia, justificativa)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (reuniao_id, obreiro_id) 
                DO UPDATE SET 
                    presente = EXCLUDED.presente,
                    tipo_ausencia = EXCLUDED.tipo_ausencia,
                    justificativa = EXCLUDED.justificativa
            """, (reuniao_id, obreiro_id, presente, tipo_ausencia, justificativa))
        
        conn.commit()
        
        total_presentes = len(presentes_ids)
        flash(f"✅ Presenças registradas! Presentes: {total_presentes} | Ausentes: {len(todos_obreiros) - total_presentes}", "success")
        
    except Exception as e:
        conn.rollback()
        print(f"ERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f"❌ Erro: {str(e)}", "danger")
    
    finally:
        return_connection(conn)
    
    return redirect(f"/reunioes/{reuniao_id}")
    
@app.route("/reunioes/<int:reuniao_id>/inicializar_presenca", methods=["GET"])
@login_required
@admin_required
def inicializar_presenca_reuniao(reuniao_id):
    """Inicializa os registros de presença para todos os obreiros"""
    cursor, conn = get_db()
    
    try:
        conn.rollback()
        
        # Buscar todos os obreiros ativos
        cursor.execute("SELECT id FROM usuarios WHERE tipo = 'obreiro' AND ativo = 1")
        obreiros = cursor.fetchall()
        
        registros = 0
        for obreiro in obreiros:
            cursor.execute("""
                INSERT INTO presenca_reuniao (reuniao_id, obreiro_id, presente)
                VALUES (%s, %s, false)
                ON CONFLICT (reuniao_id, obreiro_id) DO NOTHING
            """, (reuniao_id, obreiro['id']))
            registros += 1
        
        conn.commit()
        
        flash(f"✅ {registros} registros de presença inicializados!", "success")
        
    except Exception as e:
        conn.rollback()
        print(f"Erro: {str(e)}")
        flash(f"❌ Erro ao inicializar: {str(e)}", "danger")
    
    finally:
        return_connection(conn)
    
    return redirect(f"/reunioes/{reuniao_id}")    
    

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
    
    # ============================================
    # PERMISSÃO POR GRAU
    # ============================================
    usuario_grau = session.get('grau_atual', 1)
    usuario_tipo = session.get('tipo', 'obreiro')
    
    # ============================================
    # CONSTRUIR FILTRO DE GRAU
    # ============================================
    if usuario_tipo != 'admin':
        if usuario_grau == 1:
            grau_filter = "(r.grau = 1 OR r.grau IS NULL)"
        elif usuario_grau == 2:
            grau_filter = "(r.grau IN (1, 2) OR r.grau IS NULL)"
        elif usuario_grau >= 3:
            grau_filter = "(r.grau <= 3 OR r.grau IS NULL OR r.grau > 3)"
    else:
        grau_filter = "1=1"
    
    # ============================================
    # QUERY PRINCIPAL COM FILTROS E PRESENÇA CORRETA
    # ============================================
    query = """
        WITH obreiros_ativos AS (
            SELECT id 
            FROM usuarios 
            WHERE ativo = 1 
              AND tipo IN ('admin', 'obreiro', 'sindicante')
        ),
        presenca_calculada AS (
            SELECT 
                pr.reuniao_id,
                COUNT(DISTINCT oa.id) as total_obreiros,
                COUNT(DISTINCT CASE WHEN pr.presente = TRUE THEN oa.id END) as presentes_confirmados
            FROM obreiros_ativos oa
            CROSS JOIN reunioes r
            LEFT JOIN presenca_reuniao pr ON r.id = pr.reuniao_id AND oa.id = pr.obreiro_id
            GROUP BY pr.reuniao_id
        ),
        ata_info AS (
            SELECT 
                reuniao_id,
                id as ata_id,
                TRUE as tem_ata
            FROM atas
        )
        SELECT 
            r.id, 
            r.titulo, 
            r.tipo, 
            r.grau, 
            r.data, 
            r.hora_inicio, 
            r.hora_termino, 
            r.local, 
            r.loja_id, 
            r.pauta, 
            r.observacoes, 
            r.status, 
            r.criado_por,
            l.nome as loja_nome, 
            t.cor,
            COALESCE(pc.total_obreiros, 0) as total_obreiros,
            COALESCE(pc.presentes_confirmados, 0) as presentes_confirmados,
            CASE 
                WHEN COALESCE(pc.total_obreiros, 0) > 0 
                THEN ROUND((COALESCE(pc.presentes_confirmados, 0)::decimal / COALESCE(pc.total_obreiros, 0) * 100), 1)
                ELSE 0 
            END as percentual_presenca,
            COALESCE(ai.tem_ata, FALSE) as tem_ata,
            ai.ata_id
        FROM reunioes r
        LEFT JOIN lojas l ON r.loja_id = l.id
        LEFT JOIN tipos_reuniao t ON r.tipo = t.nome
        LEFT JOIN presenca_calculada pc ON r.id = pc.reuniao_id
        LEFT JOIN ata_info ai ON r.id = ai.reuniao_id
        WHERE {grau_filter}
    """.format(grau_filter=grau_filter)
    
    params = []
    
    # Filtros adicionais
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
        query += " AND r.local ILIKE %s"
        params.append(f"%{local}%")
    
    query += """ 
        ORDER BY r.data ASC, r.hora_inicio ASC
    """
    
    cursor.execute(query, params)
    reunioes = cursor.fetchall()
    
    # ============================================
    # ESTATÍSTICAS PARA OS CARDS
    # ============================================
    stats_query = """
        WITH obreiros_ativos AS (
            SELECT id 
            FROM usuarios 
            WHERE ativo = 1 
              AND tipo IN ('admin', 'obreiro', 'sindicante')
        ),
        presenca_calculada AS (
            SELECT 
                pr.reuniao_id,
                COUNT(DISTINCT CASE WHEN pr.presente = TRUE THEN oa.id END) as presentes_confirmados
            FROM obreiros_ativos oa
            CROSS JOIN reunioes r
            LEFT JOIN presenca_reuniao pr ON r.id = pr.reuniao_id AND oa.id = pr.obreiro_id
            GROUP BY pr.reuniao_id
        )
        SELECT 
            COUNT(DISTINCT r.id) as total_reunioes,
            SUM(CASE WHEN r.status = 'realizada' THEN 1 ELSE 0 END) as realizadas,
            SUM(CASE WHEN r.status = 'agendada' THEN 1 ELSE 0 END) as agendadas,
            SUM(CASE WHEN r.status = 'cancelada' THEN 1 ELSE 0 END) as canceladas,
            SUM(CASE WHEN r.status = 'agendada' AND r.data >= CURRENT_DATE THEN 1 ELSE 0 END) as proximas,
            SUM(CASE WHEN r.status = 'realizada' AND EXTRACT(MONTH FROM r.data) = EXTRACT(MONTH FROM CURRENT_DATE)
                      AND EXTRACT(YEAR FROM r.data) = EXTRACT(YEAR FROM CURRENT_DATE) THEN 1 ELSE 0 END) as reunioes_mes,
            SUM(CASE WHEN r.data < CURRENT_DATE AND r.status = 'agendada' THEN 1 ELSE 0 END) as reunioes_atrasadas,
            COALESCE(SUM(pc.presentes_confirmados), 0) as total_presentes
        FROM reunioes r
        LEFT JOIN presenca_calculada pc ON r.id = pc.reuniao_id
        WHERE {grau_filter}
    """.format(grau_filter=grau_filter)
    
    # Aplicar os mesmos filtros de data às estatísticas
    stats_params = []
    stats_filter = ""
    
    if data_ini:
        stats_filter += " AND r.data >= %s"
        stats_params.append(data_ini)
    if data_fim:
        stats_filter += " AND r.data <= %s"
        stats_params.append(data_fim)
    if tipo:
        stats_filter += " AND r.tipo = %s"
        stats_params.append(tipo)
    if local:
        stats_filter += " AND r.local ILIKE %s"
        stats_params.append(f"%{local}%")
    
    stats_query += stats_filter
    
    cursor.execute(stats_query, stats_params)
    stats = cursor.fetchone()
    
    # ============================================
    # BUSCAR DADOS PARA FILTROS
    # ============================================
    
    # Buscar tipos únicos para filtro
    tipos_query = """
        SELECT DISTINCT tipo FROM reunioes 
        WHERE tipo IS NOT NULL 
        ORDER BY tipo
    """
    cursor.execute(tipos_query)
    tipos = cursor.fetchall()
    
    # Buscar status únicos para filtro
    status_query = """
        SELECT DISTINCT status FROM reunioes 
        ORDER BY status
    """
    cursor.execute(status_query)
    status_list = cursor.fetchall()
    
    # Buscar graus únicos para filtro
    graus_query = """
        SELECT DISTINCT grau FROM reunioes 
        WHERE grau IS NOT NULL 
        ORDER BY grau
    """
    cursor.execute(graus_query)
    graus = cursor.fetchall()
    
    # ============================================
    # MONTAR DICIONÁRIO DE ESTATÍSTICAS
    # ============================================
    estatisticas = {
        'total_reunioes': stats['total_reunioes'] or 0,
        'realizadas': stats['realizadas'] or 0,
        'agendadas': stats['agendadas'] or 0,
        'canceladas': stats['canceladas'] or 0,
        'proximas': stats['proximas'] or 0,
        'reunioes_mes': stats['reunioes_mes'] or 0,
        'reunioes_atrasadas': stats['reunioes_atrasadas'] or 0,
        'total_presentes': stats['total_presentes'] or 0,
        'exibidas': len(reunioes)
    }
    
    return_connection(conn)
    
    return render_template("reunioes/lista.html", 
                          reunioes=reunioes,
                          estatisticas=estatisticas,
                          tipos=tipos,
                          status_list=status_list,
                          graus=graus,
                          filtros={
                              'data_ini': data_ini,
                              'data_fim': data_fim,
                              'tipo': tipo,
                              'status': status,
                              'grau': grau,
                              'local': local
                          },
                          now=datetime.now())
                          
@app.route("/reunioes/<int:id>")
@login_required
def visualizar_reuniao(id):
    cursor, conn = get_db()
    
    try:
        conn.rollback()
        
        # Buscar dados da reunião
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
            flash("Reunião não encontrada!", "danger")
            return redirect("/reunioes")
        
        # ============================================
        # BUSCAR APENAS OBREIROS ATIVOS COM MESMO CRITÉRIO DA LISTA
        # ============================================
        cursor.execute("""
            SELECT 
                u.id, 
                u.nome_completo, 
                u.grau_atual,
                u.tipo,
                u.cim_numero,
                COALESCE(pr.presente, FALSE) as presente,
                pr.tipo_ausencia,
                pr.justificativa,
                pr.id as presenca_id
            FROM usuarios u
            LEFT JOIN presenca_reuniao pr ON u.id = pr.obreiro_id AND pr.reuniao_id = %s
            WHERE u.ativo = 1 
              AND u.tipo IN ('admin', 'obreiro', 'sindicante')
            ORDER BY 
                CASE u.tipo
                    WHEN 'admin' THEN 1
                    WHEN 'obreiro' THEN 2
                    WHEN 'sindicante' THEN 3
                    ELSE 4
                END,
                u.grau_atual DESC,
                u.nome_completo
        """, (id,))
        presenca = cursor.fetchall()
        
        # Calcular estatísticas (USAR MESMO CRITÉRIO)
        total_obreiros = len(presenca)
        presentes = sum(1 for p in presenca if p['presente'] == True)
        ausentes = total_obreiros - presentes
        
        # DEBUG - Verificar se os números estão corretos
        print(f"=== DEBUG detalhes_reuniao ===")
        print(f"Total de obreiros ativos: {total_obreiros}")
        print(f"Presentes: {presentes}")
        print(f"Ausentes: {ausentes}")
        for p in presenca:
            print(f"  - {p['nome_completo']} (tipo: {p['tipo']}): presente={p['presente']}")
        print(f"==============================")
        
        # Buscar tipos de ausência
        try:
            cursor.execute("SELECT id, nome FROM tipos_ausencia WHERE ativo = 1 ORDER BY nome")
            tipos_ausencia = cursor.fetchall()
        except:
            tipos_ausencia = []
        
        # Buscar ata da reunião
        cursor.execute("""
            SELECT a.* FROM atas a
            WHERE a.reuniao_id = %s 
            ORDER BY a.id DESC 
            LIMIT 1
        """, (id,))
        ata = cursor.fetchone()
        
        ata_id = ata['id'] if ata else None
        if ata:
            ata_numero = ata.get('numero') or ata.get('numero_ata') or ata.get('num_ata') or str(ata.get('id', '?'))
            ata_ano = ata.get('ano') or ata.get('ano_ata') or str(datetime.now().year)
            ata_aprovada = ata.get('status') == 'aprovada' if ata else False
        else:
            ata_numero = None
            ata_ano = None
            ata_aprovada = False
        
        return_connection(conn)
        
        return render_template("reunioes/detalhes.html",
                              reuniao=reuniao,
                              presenca=presenca,
                              total_obreiros=total_obreiros,
                              presentes=presentes,
                              ausentes=ausentes,
                              tipos_ausencia=tipos_ausencia,
                              ata_id=ata_id,
                              ata_numero=ata_numero,
                              ata_ano=ata_ano,
                              ata_aprovada=ata_aprovada,
                              now=datetime.now())
        
    except Exception as e:
        print(f"Erro ao visualizar reunião: {str(e)}")
        import traceback
        traceback.print_exc()
        if conn:
            return_connection(conn)
        flash(f"Erro ao carregar reunião: {str(e)}", "danger")
        return redirect("/reunioes")
        
@app.route("/reunioes/calendario")
@login_required
def calendario_reunioes():
    return render_template("reunioes/calendario.html")

@app.route("/api/reunioes")
@login_required
def api_reunioes():
    cursor, conn = get_db()
@app.route("/api/lojas/<int:loja_id>/horarios")
@login_required
def api_loja_horarios(loja_id):
    """Retorna os horários configurados da loja"""
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT horario_inicio, horario_termino, dias_sessao, frequencia_sessao, observacoes_horario
        FROM lojas WHERE id = %s
    """, (loja_id,))
    
    loja = cursor.fetchone()
    return_connection(conn)
    
    if loja:
        return jsonify({
            'success': True,
            'horario_inicio': loja['horario_inicio'].strftime('%H:%M') if loja['horario_inicio'] else None,
            'horario_termino': loja['horario_termino'].strftime('%H:%M') if loja['horario_termino'] else None,
            'dias_sessao': loja['dias_sessao'],
            'frequencia_sessao': loja['frequencia_sessao'],
            'observacoes': loja['observacoes_horario']
        })
    else:
        return jsonify({'success': False, 'error': 'Loja não encontrada'}), 404
    
    # ============================================
    # PERMISSÃO POR GRAU
    # ============================================
    usuario_grau = session.get('grau_atual', 1)
    usuario_tipo = session.get('tipo', 'obreiro')
    
    query = """
        SELECT r.id, r.titulo, r.data, r.hora_inicio, r.hora_termino, 
               r.tipo, r.local, r.status,
               t.cor, t.nome as tipo_nome,
               COUNT(p.id) as total_obreiros,
               SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presentes
        FROM reunioes r
        LEFT JOIN tipos_reuniao t ON r.tipo = t.nome
        LEFT JOIN presenca p ON r.id = p.reuniao_id
        WHERE 1=1
    """
    params = []
    
    # Filtrar por grau do usuário
    if usuario_tipo != 'admin':
        if usuario_grau == 1:
            query += " AND (r.grau = 1 OR r.grau IS NULL)"
        elif usuario_grau == 2:
            query += " AND (r.grau IN (1, 2) OR r.grau IS NULL)"
        elif usuario_grau >= 3:
            query += " AND (r.grau <= 3 OR r.grau IS NULL OR r.grau > 3)"
    
    query += """
        GROUP BY r.id, r.titulo, r.data, r.hora_inicio, r.hora_termino, 
                 r.tipo, r.local, r.status, t.cor, t.nome
        ORDER BY r.data
    """
    
    cursor.execute(query, params)
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
    
@app.route("/reunioes/<int:id>/cancelar", methods=["POST"])
@login_required
@permissao_required('reuniao.edit')
def cancelar_reuniao(id):
    """Cancela uma reunião (muda status para cancelada)"""
    cursor, conn = get_db()
    
    try:
        # Buscar dados da reunião
        cursor.execute("SELECT id, titulo, status FROM reunioes WHERE id = %s", (id,))
        reuniao = cursor.fetchone()
        
        if not reuniao:
            flash("Reunião não encontrada!", "danger")
            return_connection(conn)
            return redirect("/reunioes")
        
        # Verificar se já está cancelada
        if reuniao['status'] == 'cancelada':
            flash(f"A reunião '{reuniao['titulo']}' já está cancelada!", "warning")
            return_connection(conn)
            return redirect(f"/reunioes/{id}")
        
        # Cancelar a reunião
        cursor.execute("UPDATE reunioes SET status = 'cancelada' WHERE id = %s", (id,))
        conn.commit()
        
        registrar_log("cancelar", "reuniao", id, dados_novos={"status": "cancelada"})
        
        flash(f"✅ Reunião '{reuniao['titulo']}' cancelada com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao cancelar reunião: {e}")
        conn.rollback()
        flash(f"Erro ao cancelar reunião: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect("/reunioes")

@app.route("/reunioes/<int:id>/reativar", methods=["POST"])
@login_required
@permissao_required('reuniao.edit')
def reativar_reuniao(id):
    """Reativa uma reunião cancelada (volta para agendada)"""
    cursor, conn = get_db()
    
    try:
        cursor.execute("UPDATE reunioes SET status = 'agendada' WHERE id = %s", (id,))
        conn.commit()
        
        registrar_log("reativar", "reuniao", id, dados_novos={"status": "agendada"})
        flash("Reunião reativada com sucesso!", "success")
        
    except Exception as e:
        flash(f"Erro ao reativar reunião: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect("/reunioes")    

@app.route("/reunioes/nova", methods=["GET", "POST"])
@login_required
@permissao_required('reuniao.create')
def nova_reuniao():
    """Cria uma nova reunião"""
    cursor, conn = get_db()
    
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
        
        # Validações básicas
        if not titulo or not tipo or not data or not hora_inicio:
            flash("Preencha todos os campos obrigatórios (Título, Tipo, Data e Horário)", "danger")
            return_connection(conn)
            return redirect("/reunioes/nova")
        
        # Verificar se o usuário tem permissão para criar reunião com grau específico
        if grau and grau.strip():
            try:
                grau_int = int(grau)
                # Se o grau da reunião for maior que o grau do usuário, verificar permissão
                usuario_grau = session.get('grau_atual', 0)
                if grau_int > usuario_grau and session.get('tipo') != 'admin':
                    if not verificar_permissao(session['user_id'], 'reuniao.create_superior'):
                        flash("Você não tem permissão para criar reuniões para graus superiores ao seu.", "danger")
                        return_connection(conn)
                        return redirect("/reunioes/nova")
            except ValueError:
                pass
        
        # Tratamento dos campos
        grau = grau if grau and grau.strip() else None
        if grau:
            try:
                grau = int(grau)
            except ValueError:
                grau = None
                
        hora_termino = hora_termino if hora_termino and hora_termino.strip() else None
        local = local if local and local.strip() else None
        loja_id = loja_id if loja_id and loja_id.strip() else None
        if loja_id:
            try:
                loja_id = int(loja_id)
            except ValueError:
                loja_id = None
                
        pauta = pauta if pauta and pauta.strip() else None
        observacoes = observacoes if observacoes and observacoes.strip() else None
        
        # Validar datas
        try:
            data_obj = datetime.strptime(data, '%Y-%m-%d').date() if data else None
            hora_inicio_obj = datetime.strptime(hora_inicio, '%H:%M').time() if hora_inicio else None
            hora_termino_obj = datetime.strptime(hora_termino, '%H:%M').time() if hora_termino else None
        except ValueError as e:
            flash(f"Erro no formato da data/hora: {str(e)}", "danger")
            return_connection(conn)
            return redirect("/reunioes/nova")
        
        try:
            # Inserir reunião com status padrão 'agendada'
            cursor.execute("""
                INSERT INTO reunioes 
                (titulo, tipo, grau, data, hora_inicio, hora_termino, local, loja_id, pauta, observacoes, criado_por, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'agendada')
                RETURNING id
            """, (titulo, tipo, grau, data_obj, hora_inicio_obj, hora_termino_obj, 
                  local, loja_id, pauta, observacoes, session["user_id"]))
            
            reuniao_id = cursor.fetchone()['id']
            conn.commit()
            
            print(f"✅ Reunião criada com ID: {reuniao_id}")
            
            registrar_log("criar", "reuniao", reuniao_id, dados_novos={"titulo": titulo, "data": data, "tipo": tipo})
            
            # ============================================
            # ENVIO DE E-MAILS VIA RESEND
            # ============================================
            emails_enviados = 0
            
            try:
                # Buscar participantes com permissão para ver a reunião
                # Apenas usuários com grau >= grau da reunião podem ser notificados
                if grau:
                    cursor.execute("""
                        SELECT id, nome_completo, email 
                        FROM usuarios 
                        WHERE ativo = 1 
                        AND email IS NOT NULL 
                        AND email != ''
                        AND grau_atual >= %s
                        ORDER BY nome_completo
                    """, (grau,))
                else:
                    cursor.execute("""
                        SELECT id, nome_completo, email 
                        FROM usuarios 
                        WHERE ativo = 1 
                        AND email IS NOT NULL 
                        AND email != ''
                        ORDER BY nome_completo
                    """)
                participantes = cursor.fetchall()
                
                if participantes:
                    print(f"📧 Enviando e-mails para {len(participantes)} participantes...")
                    
                    data_formatada = data_obj.strftime('%d/%m/%Y') if data_obj else data
                    hora_formatada = hora_inicio_obj.strftime('%H:%M') if hora_inicio_obj else hora_inicio
                    
                    loja_nome = None
                    if loja_id:
                        try:
                            cursor.execute("SELECT nome FROM lojas WHERE id = %s", (loja_id,))
                            loja_result = cursor.fetchone()
                            loja_nome = loja_result['nome'] if loja_result else None
                        except Exception as e:
                            print(f"Erro ao buscar loja: {e}")
                    
                    dados_reuniao = {
                        'id': reuniao_id,
                        'titulo': titulo,
                        'tipo': tipo,
                        'grau': grau,
                        'data': data_formatada,
                        'hora_inicio': hora_formatada,
                        'hora_termino': hora_termino_obj.strftime('%H:%M') if hora_termino_obj else None,
                        'local': local or (loja_nome if loja_nome else 'Templo Maçônico'),
                        'pauta': pauta,
                        'observacoes': observacoes
                    }
                    
                    for participante in participantes:
                        try:
                            if 'enviar_email_reuniao' not in globals():
                                print("❌ Função enviar_email_reuniao não encontrada!")
                                continue
                                
                            resultado = enviar_email_reuniao(
                                destinatario=participante['email'],
                                nome_destinatario=participante['nome_completo'],
                                dados_reuniao=dados_reuniao
                            )
                            
                            if resultado.get('success'):
                                emails_enviados += 1
                                print(f"✅ E-mail enviado para {participante['email']}")
                            else:
                                print(f"❌ Falha ao enviar para {participante['email']}: {resultado.get('message')}")
                                
                        except Exception as e:
                            print(f"❌ Erro ao enviar para {participante['email']}: {e}")
                    
                    if emails_enviados > 0:
                        flash(f"✅ Reunião agendada com sucesso! {emails_enviados} e-mail(s) enviado(s).", "success")
                    else:
                        flash("✅ Reunião agendada com sucesso! Nenhum e-mail enviado (verifique e-mails dos participantes).", "success")
                else:
                    flash("✅ Reunião agendada com sucesso! Nenhum participante com e-mail cadastrado.", "success")
                    
            except Exception as e:
                print(f"❌ Erro no envio de e-mails: {e}")
                import traceback
                traceback.print_exc()
                flash(f"✅ Reunião agendada com sucesso! Mas houve erro no envio de e-mails: {str(e)}", "warning")
            
            return_connection(conn)
            return redirect(f"/reunioes/{reuniao_id}")
            
        except Exception as e:
            print(f"ERRO: {e}")
            conn.rollback()
            flash(f"Erro ao salvar reunião: {str(e)}", "danger")
            return_connection(conn)
            return redirect("/reunioes/nova")
    
    # GET - Carregar formulário
    cursor.execute("SELECT * FROM tipos_reuniao ORDER BY nome")
    tipos = cursor.fetchall()
    
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    
    return_connection(conn)
    
    hoje = datetime.now().strftime("%Y-%m-%d")
    
    return render_template("reunioes/nova.html", tipos=tipos, lojas=lojas, hoje=hoje)
    

@app.route("/reunioes/<int:id>/editar", methods=["GET", "POST"])
@login_required
@permissao_required('reuniao.edit')
def editar_reuniao(id):
    """Edita uma reunião existente"""
    cursor, conn = get_db()
    
    # Buscar dados atuais da reunião
    cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
    reuniao_atual = cursor.fetchone()
    
    if not reuniao_atual:
        flash("Reunião não encontrada", "danger")
        return_connection(conn)
        return redirect("/reunioes")
    
    # Verificar permissão para editar reunião
    is_admin = session.get("tipo") == "admin"
    
    # Se não for admin, verificar se pode editar reuniões do grau
    if not is_admin:
        usuario_grau = session.get('grau_atual', 0)
        reuniao_grau = reuniao_atual.get('grau', 0)
        
        # Se a reunião é de grau superior, verificar permissão especial
        if reuniao_grau and reuniao_grau > usuario_grau:
            if not verificar_permissao(session['user_id'], 'reuniao.edit_superior'):
                flash("Você não tem permissão para editar reuniões de grau superior ao seu.", "danger")
                return_connection(conn)
                return redirect("/reunioes")
        
        # Verificar se a reunião já foi realizada (não pode editar)
        if reuniao_atual['status'] == 'realizada':
            flash("Reuniões realizadas não podem ser editadas!", "warning")
            return_connection(conn)
            return redirect(f"/reunioes/{id}")
    
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
        
        # Validações básicas
        if not titulo or not tipo or not data or not hora_inicio:
            flash("Preencha todos os campos obrigatórios", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{id}/editar")
        
        # Tratamento dos campos opcionais
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
        
        # Salvar dados antigos para log
        dados_antigos = dict(reuniao_atual)
        
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
    
    # GET - Carregar dados para o formulário
    cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
    reuniao = cursor.fetchone()
    
    cursor.execute("SELECT * FROM tipos_reuniao ORDER BY nome")
    tipos = cursor.fetchall()
    
    # Buscar lojas para o select (opcional)
    cursor.execute("SELECT id, nome FROM lojas WHERE ativo = 1 ORDER BY nome")
    lojas = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("reunioes/editar.html", 
                          reuniao=reuniao, 
                          tipos=tipos,
                          lojas=lojas,
                          is_admin=is_admin)

@app.route("/reunioes/<int:id>/status", methods=["POST"])
@admin_required
def alterar_status_reuniao(id):
    try:
        cursor, conn = get_db()
        novo_status = request.form.get("status")
        
        if not novo_status:
            flash("Status não informado", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{id}")
        
        status_validos = ['agendada', 'realizada', 'cancelada']
        if novo_status not in status_validos:
            flash("Status inválido", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{id}")
        
        # Buscar dados antigos
        cursor.execute("SELECT id, titulo, status FROM reunioes WHERE id = %s", (id,))
        reuniao_antiga = cursor.fetchone()
        
        if not reuniao_antiga:
            flash("Reunião não encontrada", "danger")
            return_connection(conn)
            return redirect("/reunioes")
        
        cursor.execute("UPDATE reunioes SET status = %s WHERE id = %s", (novo_status, id))
        conn.commit()
        
        registrar_log("alterar_status", "reuniao", id, 
                     dados_anteriores={"status": reuniao_antiga["status"], "titulo": reuniao_antiga["titulo"]},
                     dados_novos={"status": novo_status, "titulo": reuniao_antiga["titulo"]})
        
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
@login_required
@permissao_required('reuniao.delete')
def excluir_reuniao(id):
    """Exclui uma reunião (somente se não tiver ata vinculada)"""
    cursor, conn = get_db()
    
    try:
        # Buscar dados da reunião
        cursor.execute("SELECT id, titulo, data, status FROM reunioes WHERE id = %s", (id,))
        reuniao = cursor.fetchone()
        
        if not reuniao:
            flash("Reunião não encontrada!", "danger")
            return_connection(conn)
            return redirect("/reunioes")
        
        # Verificar se tem ata vinculada
        cursor.execute("SELECT id FROM atas WHERE reuniao_id = %s", (id,))
        ata = cursor.fetchone()
        
        if ata:
            flash(f"Não é possível excluir a reunião '{reuniao['titulo']}' pois ela possui uma ata vinculada!", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{id}")
        
        # Registrar log antes de excluir
        registrar_log("excluir", "reuniao", id, dados_anteriores={"titulo": reuniao['titulo'], "data": reuniao['data']})
        
        # Excluir presenças primeiro
        cursor.execute("DELETE FROM presenca WHERE reuniao_id = %s", (id,))
        
        # Excluir a reunião
        cursor.execute("DELETE FROM reunioes WHERE id = %s", (id,))
        conn.commit()
        
        flash(f"Reunião '{reuniao['titulo']}' excluída com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao excluir reunião: {e}")
        conn.rollback()
        flash(f"Erro ao excluir reunião: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect("/reunioes")

@app.route("/reunioes/<int:id>/presenca", methods=["POST"])
@admin_required
def registrar_presenca(id):
    cursor, conn = get_db()
    obreiro_id = request.form.get("obreiro_id")
    presente = request.form.get("presente", 0)
    justificativa = request.form.get("justificativa", "")
    tipo_ausencia = request.form.get("tipo_ausencia", None)
    
    try:
        # Converter valores
        presente_val = 1 if str(presente) == '1' else 0
        justificativa_val = justificativa if justificativa and justificativa.strip() else None
        tipo_ausencia_val = tipo_ausencia if tipo_ausencia and tipo_ausencia.strip() else None
        
        # Primeiro, verificar se já existe um registro
        cursor.execute("""
            SELECT id FROM presenca 
            WHERE reuniao_id = %s AND obreiro_id = %s
        """, (id, obreiro_id))
        
        existing = cursor.fetchone()
        
        if existing:
            # Atualizar registro existente
            cursor.execute("""
                UPDATE presenca 
                SET presente = %s, 
                    justificativa = %s, 
                    registrado_por = %s, 
                    tipo_ausencia = %s, 
                    data_registro = CURRENT_TIMESTAMP
                WHERE reuniao_id = %s AND obreiro_id = %s
            """, (presente_val, justificativa_val, session["user_id"], tipo_ausencia_val, id, obreiro_id))
            flash("Presença atualizada com sucesso!", "success")
        else:
            # Obter próximo ID
            cursor.execute("SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM presenca")
            next_id = cursor.fetchone()['next_id']
            
            # Inserir novo registro
            cursor.execute("""
                INSERT INTO presenca (id, reuniao_id, obreiro_id, presente, justificativa, registrado_por, tipo_ausencia)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (next_id, id, obreiro_id, presente_val, justificativa_val, session["user_id"], tipo_ausencia_val))
            flash("Presença registrada com sucesso!", "success")
        
        conn.commit()
        registrar_log("registrar_presenca", "presenca", id, 
                     dados_novos={"obreiro_id": obreiro_id, "presente": presente_val})
        
    except Exception as e:
        print(f"Erro ao registrar presença: {e}")
        conn.rollback()
        flash(f"Erro ao registrar presença: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/reunioes/{id}")
@app.route("/reunioes/<int:id>/ata", methods=["GET", "POST"])
@admin_required
def redigir_ata(id):
    cursor, conn = get_db()
    
    if request.method == "POST":
        conteudo = request.form.get("conteudo")
        aprovada = request.form.get("aprovada", 0)
        
        try:
            # Converter valores
            conteudo_val = conteudo if conteudo and conteudo.strip() else ""
            aprovada_val = 1 if str(aprovada) == '1' else 0
            data_aprovacao_val = datetime.now().date() if aprovada_val == 1 else None
            
            # Verificar se já existe uma ata para esta reunião
            cursor.execute("""
                SELECT id, versao FROM atas WHERE reuniao_id = %s
            """, (id,))
            
            existing = cursor.fetchone()
            
            if existing:
                # Obter versão atual, tratando None
                versao_atual = existing['versao'] or 0
                nova_versao = versao_atual + 1
                
                # Atualizar ata existente
                cursor.execute("""
                    UPDATE atas 
                    SET conteudo = %s, 
                        redator_id = %s, 
                        aprovada = %s, 
                        data_aprovacao = %s, 
                        versao = %s
                    WHERE reuniao_id = %s
                """, (conteudo_val, session["user_id"], aprovada_val, data_aprovacao_val, nova_versao, id))
                
                ata_id = existing['id']
                registrar_log("editar", "ata", ata_id, dados_novos={"versao": nova_versao})
                flash("Ata atualizada com sucesso!", "success")
            else:
                # Obter próximo ID de forma segura
                cursor.execute("SELECT MAX(id) FROM atas")
                max_id = cursor.fetchone()['max']
                next_id = (max_id or 0) + 1
                
                # Obter número da ata para o ano atual
                ano_atual = datetime.now().year
                cursor.execute("SELECT COUNT(*) as total FROM atas WHERE ano_ata = %s", (ano_atual,))
                total = cursor.fetchone()["total"]
                numero_ata = total + 1
                
                # Buscar tipo da reunião
                cursor.execute("SELECT tipo FROM reunioes WHERE id = %s", (id,))
                reuniao_info = cursor.fetchone()
                tipo_ata = reuniao_info['tipo'] if reuniao_info else None
                
                # Inserir nova ata com ID explícito e versão inicial 1
                cursor.execute("""
                    INSERT INTO atas (
                        id, reuniao_id, conteudo, redator_id, aprovada, 
                        data_aprovacao, numero_ata, ano_ata, tipo_ata, versao, data_criacao
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (next_id, id, conteudo_val, session["user_id"], aprovada_val, 
                      data_aprovacao_val, numero_ata, ano_atual, tipo_ata, 1))
                
                ata_id = next_id
                registrar_log("criar", "ata", ata_id, dados_novos={"reuniao_id": id, "numero": numero_ata, "ano": ano_atual})
                flash(f"Ata nº {numero_ata}/{ano_atual} criada com sucesso!", "success")
            
            conn.commit()
            
        except Exception as e:
            print(f"ERRO ao salvar ata: {e}")
            conn.rollback()
            flash(f"Erro ao salvar ata: {str(e)}", "danger")
        
        return_connection(conn)
        return redirect(f"/reunioes/{id}")
    
    # GET - Carregar formulário
    cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
    reuniao = cursor.fetchone()
    
    if not reuniao:
        flash("Reunião não encontrada", "danger")
        return_connection(conn)
        return redirect("/reunioes")
    
    cursor.execute("SELECT * FROM atas WHERE reuniao_id = %s", (id,))
    ata = cursor.fetchone()
    
    return_connection(conn)
    return render_template("reunioes/ata.html", reuniao=reuniao, ata=ata)

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
    return render_template("presenca/estatisticas.html", estatisticas=estatisticas, mensal=mensal, ano=ano, anos=anos)

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
        registrar_log("justificar_ausencia", "presenca", id, dados_novos={"tipo_ausencia": tipo_ausencia})
        return_connection(conn)
        flash("Ausencia justificada com sucesso!", "success")
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
        flash("Registro de presenca nao encontrado", "danger")
        return_connection(conn)
        return redirect("/reunioes")
    cursor.execute("SELECT * FROM tipos_ausencia WHERE ativo = 1")
    tipos_ausencia = cursor.fetchall()
    return_connection(conn)
    return render_template("presenca/justificar.html", presenca=presenca, tipos_ausencia=tipos_ausencia)

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
        flash("Ausencia validada com sucesso!", "success")
    else:
        cursor.execute("""
            UPDATE presenca 
            SET tipo_ausencia = NULL, justificativa = NULL,
                validado_por = NULL, data_validacao = NULL,
                observacao_validacao = %s
            WHERE id = %s
        """, (observacao, id))
        registrar_log("rejeitar_ausencia", "presenca", id)
        flash("Validacao removida!", "success")
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
               f"{a['nome_completo']} possui {a['ausencias']} ausencias injustificadas em {a['mes']}"))
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
                   f"{e['nome_completo']} tem apenas {percentual:.1f}% de presenca no ano {ano_atual} (CRITICO)"))
        elif percentual < 75:
            cursor.execute("""
                INSERT INTO alertas_presenca (obreiro_id, tipo, mensagem)
                VALUES (%s, %s, %s)
            """, (e["id"], "presenca_atencao",
                   f"{e['nome_completo']} tem {percentual:.1f}% de presenca no ano {ano_atual} (ATENCAO)"))
    conn.commit()
    registrar_log("gerar_alertas", "alertas", None, dados_novos={"quantidade": len(alertas_ausencias)})
    return_connection(conn)
    flash(f"Alertas gerados! ({len(alertas_ausencias)} por ausencias + alertas de presenca)", "success")
    return redirect("/presenca/alertas")
    
# =============================
# ROTAS DE ATAS 
# =============================

@app.route("/atas/<int:id>/pdf-oficial")
@login_required
def gerar_pdf_ata_oficial(id):
    """Gera PDF oficial da ata com cabeçalho da loja e assinaturas"""
    from flask import render_template, url_for, Response
    import pdfkit
    import os
    import base64
    
    cursor, conn = get_db()
    
    # Buscar dados da ata, reunião e assinaturas
    cursor.execute("""
        SELECT a.*, 
               a.id as ata_id, 
               a.numero_ata, 
               a.ano_ata, 
               a.conteudo as ata_conteudo,
               a.assinatura_veneravel,
               a.assinatura_orador,
               a.assinatura_secretario,
               a.data_assinatura_veneravel,
               a.data_assinatura_orador,
               a.data_assinatura_secretario,
               a.veneravel_mestre_nome,
               a.orador_nome,
               a.secretario_nome,
               r.id as reuniao_id, 
               r.titulo as reuniao_titulo, 
               r.tipo as reuniao_tipo,
               r.grau as reuniao_grau, 
               r.data as reuniao_data,
               r.hora_inicio as reuniao_hora_inicio, 
               r.hora_termino as reuniao_hora_termino,
               r.local as reuniao_local, 
               r.pauta as reuniao_pauta, 
               r.observacoes as reuniao_observacoes,
               l.nome as loja_nombre, 
               l.numero as loja_numero, 
               l.oriente as loja_oriente,
               l.veneravel_mestre as loja_veneravel_mestre,
               l.secretario as loja_secretario,
               l.tesoureiro as loja_tesoureiro,
               l.orador as loja_orador
        FROM atas a
        JOIN reunioes r ON a.reuniao_id = r.id
        LEFT JOIN lojas l ON r.loja_id = l.id
        WHERE a.id = %s
    """, (id,))
    
    ata = cursor.fetchone()
    
    if not ata:
        flash("Ata não encontrada!", "danger")
        return_connection(conn)
        return redirect("/atas")
    
    # Buscar presentes na reunião
    cursor.execute("""
        SELECT u.id, u.nome_completo, u.grau_atual, c.nome as cargo
        FROM presenca p
        JOIN usuarios u ON p.obreiro_id = u.id
        LEFT JOIN ocupacao_cargos oc ON u.id = oc.obreiro_id AND oc.ativo = 1
        LEFT JOIN cargos c ON oc.cargo_id = c.id
        WHERE p.reuniao_id = %s AND p.presente = 1
        ORDER BY u.grau_atual DESC, u.nome_completo
    """, (ata['reuniao_id'],))
    presentes = cursor.fetchall()
    
    # Buscar ausentes justificados
    cursor.execute("""
        SELECT u.id, u.nome_completo, u.grau_atual, p.justificativa
        FROM presenca p
        JOIN usuarios u ON p.obreiro_id = u.id
        WHERE p.reuniao_id = %s AND p.presente = 0 AND p.justificativa IS NOT NULL
    """, (ata['reuniao_id'],))
    ausentes = cursor.fetchall()
    
    return_connection(conn)
    
    # Preparar dados para o template
    reuniao = {
        'id': ata['reuniao_id'],
        'titulo': ata['reuniao_titulo'],
        'tipo': ata['reuniao_tipo'],
        'grau': ata['reuniao_grau'],
        'data': ata['reuniao_data'],
        'hora_inicio': ata['reuniao_hora_inicio'],
        'hora_termino': ata['reuniao_hora_termino'],
        'local': ata['reuniao_local'] or 'Templo Maçônico'
    }
    
    # ============================================
    # CONVERTER LOGO PARA BASE64
    # ============================================
    logo_base64 = None
    logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'images', 'Logo.png')
    
    if os.path.exists(logo_path):
        try:
            with open(logo_path, 'rb') as image_file:
                image_data = image_file.read()
                logo_base64 = base64.b64encode(image_data).decode('utf-8')
                print(f"✅ Logo convertida para base64! Tamanho: {len(logo_base64)} caracteres")
        except Exception as e:
            print(f"❌ Erro ao converter logo: {e}")
    else:
        print(f"❌ Logo não encontrada em: {logo_path}")
    
    # Renderizar HTML para PDF
    html = render_template("atas/pdf_ata.html",
                          ata=ata,
                          reuniao=reuniao,
                          presentes=presentes,
                          ausentes=ausentes,
                          numero_ata=ata['numero_ata'],
                          ano_ata=ata['ano_ata'],
                          conteudo=ata['conteudo'],
                          assinatura_veneravel=ata['assinatura_veneravel'],
                          assinatura_orador=ata['assinatura_orador'],
                          assinatura_secretario=ata['assinatura_secretario'],
                          data_assinatura_veneravel=ata['data_assinatura_veneravel'],
                          data_assinatura_orador=ata['data_assinatura_orador'],
                          data_assinatura_secretario=ata['data_assinatura_secretario'],
                          veneravel_mestre_nome=ata['veneravel_mestre_nome'],
                          orador_nome=ata['orador_nome'],
                          secretario_nome=ata['secretario_nome'],
                          loja_nome=ata.get('loja_nombre', 'ARLS Bicentenário'),
                          loja_numero=ata.get('loja_numero', '4739'),
                          loja_oriente=ata.get('loja_oriente', 'Ceilândia - DF'),
                          logo_base64=logo_base64,
                          now=datetime.now())
    
    # Configurar opções do PDF
    options = {
        'page-size': 'A4',
        'margin-top': '20mm',
        'margin-bottom': '20mm',
        'margin-left': '15mm',
        'margin-right': '15mm',
        'encoding': 'UTF-8',
        'no-outline': None,
        'enable-local-file-access': None
    }
    
    try:
        config = None
        if os.name == 'nt':  # Windows
            wkhtmltopdf_path = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
            if os.path.exists(wkhtmltopdf_path):
                config = pdfkit.configuration(wkhtmltopdf=wkhtmltopdf_path)
        
        if config:
            pdf = pdfkit.from_string(html, False, options=options, configuration=config)
        else:
            pdf = pdfkit.from_string(html, False, options=options)
        
        response = Response(pdf, content_type='application/pdf')
        response.headers['Content-Disposition'] = f'inline; filename=ata_{ata["numero_ata"]}_{ata["ano_ata"]}.pdf'
        return response
        
    except Exception as e:
        print(f"Erro ao gerar PDF: {e}")
        return html


@app.route("/atas/<int:id>")
@login_required
@permissao_required('ata.view_one')
def ver_ata_por_id(id):
    """Visualizar ata pelo ID com sistema de assinaturas por cargo"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT 
                a.*,
                a.numero_ata as numero,
                a.data_criacao as data,
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
        
        # Verificar permissão de acesso por grau da reunião
        is_admin = session.get("tipo") == "admin"
        
        if not is_admin:
            usuario_grau = session.get('grau_atual', 0)
            reuniao_grau = ata.get('reuniao_grau', 0)
            
            if reuniao_grau and reuniao_grau > usuario_grau:
                if not verificar_permissao(session['user_id'], 'ata.view_superior'):
                    flash("Você não tem permissão para visualizar esta ata.", "danger")
                    return_connection(conn)
                    return redirect("/atas")
        
        # Buscar lista de presença da reunião
        presenca = []
        if verificar_permissao(session['user_id'], 'reuniao.view_one') or is_admin:
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
        
        # ============================================
        # SISTEMA DE ASSINATURAS POR CARGO
        # ============================================
        
        ata_aprovada = ata.get('aprovada', 0) == 1
        
        # Buscar cargo atual do usuário (TAMBÉM PARA ADMIN)
        usuario_id = session.get('user_id')
        cargo_usuario = None
        
        # REMOVEMOS a condição "if not is_admin" para que admin também possa ter cargo
        cursor.execute("""
            SELECT c.nome as cargo_nome, c.id as cargo_id
            FROM ocupacao_cargos oc
            JOIN cargos c ON oc.cargo_id = c.id
            WHERE oc.obreiro_id = %s AND oc.ativo = 1
            ORDER BY oc.data_inicio DESC
            LIMIT 1
        """, (usuario_id,))
        cargo = cursor.fetchone()
        
        if cargo:
            cargo_nome_lower = cargo['cargo_nome'].lower()
            print(f"🔍 Cargo do usuário {usuario_id}: '{cargo['cargo_nome']}' -> lower: '{cargo_nome_lower}'")
            
            if 'venerável' in cargo_nome_lower or 'veneravel' in cargo_nome_lower:
                cargo_usuario = 'veneravel'
            elif 'orador' in cargo_nome_lower:
                cargo_usuario = 'orador'
            elif 'secretário' in cargo_nome_lower or 'secretario' in cargo_nome_lower:
                cargo_usuario = 'secretario'
            
            print(f"🔍 cargo_usuario definido como: '{cargo_usuario}'")
        else:
            print(f"⚠️ Nenhum cargo ativo encontrado para o usuário {usuario_id}")
        
        # Verificar status das assinaturas na ata
        assinatura_veneravel = ata.get('assinatura_veneravel', False)
        assinatura_orador = ata.get('assinatura_orador', False)
        assinatura_secretario = ata.get('assinatura_secretario', False)
        
        # Calcular progresso das assinaturas
        total_assinaturas = 3
        assinadas = sum([1 for x in [assinatura_veneravel, assinatura_orador, assinatura_secretario] if x])
        percentual_assinaturas = int((assinadas / total_assinaturas * 100)) if total_assinaturas > 0 else 0
        
        # Verificar se o usuário já assinou
        ja_assinou = False
        if cargo_usuario == 'veneravel' and assinatura_veneravel:
            ja_assinou = True
        elif cargo_usuario == 'orador' and assinatura_orador:
            ja_assinou = True
        elif cargo_usuario == 'secretario' and assinatura_secretario:
            ja_assinou = True
        
        # Verificar se pode assinar (ata aprovada, tem cargo correto, não assinou ainda)
        # Admin também pode assinar se tiver o cargo correto
        pode_assinar = ata_aprovada and cargo_usuario is not None and not ja_assinou
        
        print(f"🔍 DEBUG FINAL:")
        print(f"   - ata_aprovada: {ata_aprovada}")
        print(f"   - cargo_usuario: {cargo_usuario}")
        print(f"   - ja_assinou: {ja_assinou}")
        print(f"   - pode_assinar: {pode_assinar}")
        
        # Buscar assinaturas existentes (para admin visualizar)
        assinaturas_lista = []
        if assinatura_veneravel:
            assinaturas_lista.append({
                'cargo': 'Venerável Mestre',
                'assinado': True,
                'data_assinatura': ata.get('data_assinatura_veneravel'),
                'nome': ata.get('veneravel_mestre_nome', 'Venerável Mestre')
            })
        if assinatura_orador:
            assinaturas_lista.append({
                'cargo': 'Orador',
                'assinado': True,
                'data_assinatura': ata.get('data_assinatura_orador'),
                'nome': ata.get('orador_nome', 'Orador')
            })
        if assinatura_secretario:
            assinaturas_lista.append({
                'cargo': 'Secretário',
                'assinado': True,
                'data_assinatura': ata.get('data_assinatura_secretario'),
                'nome': ata.get('secretario_nome', 'Secretário')
            })
        
        return_connection(conn)
        
        return render_template("atas/visualizar.html", 
                              ata=ata, 
                              presenca=presenca, 
                              assinaturas=assinaturas_lista,
                              pode_assinar=pode_assinar,
                              ata_aprovada=ata_aprovada,
                              cargo_usuario=cargo_usuario,
                              ja_assinou=ja_assinou,
                              assinatura_veneravel=assinatura_veneravel,
                              assinatura_orador=assinatura_orador,
                              assinatura_secretario=assinatura_secretario,
                              percentual_assinaturas=percentual_assinaturas,
                              assinadas=assinadas,
                              total_assinaturas=total_assinaturas,
                              is_admin=is_admin)
        
    except Exception as e:
        print(f"❌ Erro ao ver ata {id}: {e}")
        import traceback
        traceback.print_exc()
        
        if 'conn' in locals():
            return_connection(conn)
        
        flash(f"Erro ao carregar ata: {str(e)}", "danger")
        return redirect("/atas")


# ============================================
# ROTAS DE ASSINATURA POR CARGO
# ============================================

@app.route("/atas/<int:id>/assinar/veneravel", methods=["POST"])
@login_required
def assinar_ata_veneravel(id):
    """Assina a ata como Venerável Mestre"""
    return assinar_ata_por_cargo(id, 'veneravel', 'assinatura_veneravel', 'data_assinatura_veneravel', 'Venerável Mestre')


@app.route("/atas/<int:id>/assinar/orador", methods=["POST"])
@login_required
def assinar_ata_orador(id):
    """Assina a ata como Orador"""
    return assinar_ata_por_cargo(id, 'orador', 'assinatura_orador', 'data_assinatura_orador', 'Orador')


@app.route("/atas/<int:id>/assinar/secretario", methods=["POST"])
@login_required
def assinar_ata_secretario(id):
    """Assina a ata como Secretário"""
    return assinar_ata_por_cargo(id, 'secretario', 'assinatura_secretario', 'data_assinatura_secretario', 'Secretário')


def assinar_ata_por_cargo(id, cargo, coluna_assinatura, coluna_data, nome_cargo):
    """Função genérica para assinar ata por cargo"""
    cursor, conn = get_db()
    
    try:
        print(f"🚀 Assinando ata {id} como {nome_cargo}")
        
        # 1. Verificar se a ata existe e está aprovada
        cursor.execute(f"""
            SELECT aprovada, {coluna_assinatura} 
            FROM atas 
            WHERE id = %s
        """, (id,))
        resultado = cursor.fetchone()
        
        if not resultado:
            flash("Ata não encontrada", "danger")
            return_connection(conn)
            return redirect("/atas")
        
        if resultado['aprovada'] != 1:
            flash("A ata precisa ser aprovada antes de ser assinada", "warning")
            return_connection(conn)
            return redirect(f"/atas/{id}")
        
        if resultado[coluna_assinatura]:
            flash(f"A ata já foi assinada pelo {nome_cargo}", "warning")
            return_connection(conn)
            return redirect(f"/atas/{id}")
        
        # 2. Verificar cargo do usuário
        usuario_id = session.get('user_id')
        cursor.execute("""
            SELECT c.nome as cargo_nome
            FROM ocupacao_cargos oc
            JOIN cargos c ON oc.cargo_id = c.id
            WHERE oc.obreiro_id = %s AND oc.ativo = 1
            ORDER BY oc.data_inicio DESC
            LIMIT 1
        """, (usuario_id,))
        cargo_usuario = cursor.fetchone()
        
        if not cargo_usuario:
            flash("Você não possui cargo para assinar esta ata", "danger")
            return_connection(conn)
            return redirect(f"/atas/{id}")
        
        # 3. Comparação SIMPLIFICADA - aceita qualquer cargo que contenha a palavra-chave
        cargo_do_usuario = cargo_usuario['cargo_nome'].lower()
        
        print(f"🔍 Cargo do usuário: '{cargo_do_usuario}'")
        print(f"🔍 Cargo esperado: '{cargo}'")
        
        cargo_valido = False
        
        # Verificação por palavras-chave (mais flexível)
        if cargo == 'veneravel':
            # Aceita: Venerável, Venerável Mestre, Veneravel, etc.
            if 'vener' in cargo_do_usuario:
                cargo_valido = True
                cursor.execute("UPDATE atas SET veneravel_mestre_nome = %s WHERE id = %s", 
                             (cargo_usuario['cargo_nome'], id))
        elif cargo == 'orador':
            if 'orador' in cargo_do_usuario:
                cargo_valido = True
                cursor.execute("UPDATE atas SET orador_nome = %s WHERE id = %s", 
                             (cargo_usuario['cargo_nome'], id))
        elif cargo == 'secretario':
            if 'secret' in cargo_do_usuario:
                cargo_valido = True
                cursor.execute("UPDATE atas SET secretario_nome = %s WHERE id = %s", 
                             (cargo_usuario['cargo_nome'], id))
        
        print(f"✅ Cargo válido: {cargo_valido}")
        
        if not cargo_valido:
            flash(f"Você não é {nome_cargo} para assinar esta ata. Seu cargo: {cargo_usuario['cargo_nome']}", "danger")
            return_connection(conn)
            return redirect(f"/atas/{id}")
        
        # 4. Registrar assinatura
        cursor.execute(f"""
            UPDATE atas 
            SET {coluna_assinatura} = TRUE, 
                {coluna_data} = NOW()
            WHERE id = %s
        """, (id,))
        
        conn.commit()
        
        registrar_log("assinar", "ata", id, dados_novos={"cargo": nome_cargo, "data": datetime.now()})
        flash(f"✅ Ata assinada com sucesso como {nome_cargo}!", "success")
        
    except Exception as e:
        print(f"❌ Erro ao assinar ata: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        flash(f"Erro ao assinar ata: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/atas/{id}")

@app.route("/atas")
@login_required
def listar_atas():
    cursor, conn = get_db()
    
    # Pegar filtros da request
    data_ini = request.args.get('data_ini', '')
    data_fim = request.args.get('data_fim', '')
    aprovada = request.args.get('aprovada', '')
    reuniao_titulo = request.args.get('reuniao_titulo', '')
    
    # ============================================
    # PERMISSÃO POR GRAU
    # ============================================
    usuario_grau = session.get('grau_atual', 1)
    usuario_tipo = session.get('tipo', 'obreiro')
    
    # ============================================
    # QUERY PRINCIPAL - ADICIONAR total_assinaturas
    # ============================================
    query = """
        SELECT 
            a.id,
            a.numero_ata,
            a.ano_ata,
            a.data_criacao as data,
            a.aprovada,
            a.aprovada_em,
            a.tipo_ata,
            a.conteudo,
            a.redator_id,
            a.redator_nome,
            a.secretario_id,
            a.titulo as reuniao_titulo,
            r.id as reuniao_id,
            r.titulo as reuniao_titulo_original,
            r.data as reuniao_data,
            r.local as reuniao_local,
            r.tipo as reuniao_tipo,
            -- Status das assinaturas
            a.assinatura_veneravel,
            a.assinatura_orador,
            a.assinatura_secretario,
            -- CALCULAR total_assinaturas
            (CASE WHEN a.assinatura_veneravel = true THEN 1 ELSE 0 END +
             CASE WHEN a.assinatura_orador = true THEN 1 ELSE 0 END +
             CASE WHEN a.assinatura_secretario = true THEN 1 ELSE 0 END) as total_assinaturas,
            -- Nomes dos assinantes
            a.veneravel_mestre_nome,
            a.orador_nome,
            a.secretario_nome,
            a.versao,
            a.arquivo_pdf,
            a.data_impressao,
            a.hash_documento,
            COALESCE(
                a.redator_nome, 
                redator_user.nome_completo,
                'Redator não informado'
            ) as redator_nome_completo,
            secretario_user.nome_completo as secretario_nome
        FROM atas a
        LEFT JOIN reunioes r ON a.reuniao_id = r.id
        LEFT JOIN usuarios redator_user ON a.redator_id = redator_user.id
        LEFT JOIN usuarios secretario_user ON a.secretario_id = secretario_user.id
        WHERE 1=1
    """
    
    params = []
    
    # Filtro por grau do usuário (se não for admin)
    if usuario_tipo != 'admin':
        if usuario_grau == 1:
            query += " AND (r.grau = 1 OR r.grau IS NULL)"
        elif usuario_grau == 2:
            query += " AND (r.grau IN (1, 2) OR r.grau IS NULL)"
        elif usuario_grau >= 3:
            query += " AND (r.grau <= 3 OR r.grau IS NULL OR r.grau > 3)"
    
    if data_ini:
        query += " AND a.data_criacao >= %s"
        params.append(data_ini)
    if data_fim:
        query += " AND a.data_criacao <= %s"
        params.append(data_fim)
    if aprovada != '':
        query += " AND a.aprovada = %s"
        params.append(1 if aprovada == '1' else 0)
    if reuniao_titulo:
        query += " AND (r.titulo ILIKE %s OR a.titulo ILIKE %s)"
        params.append(f"%{reuniao_titulo}%")
        params.append(f"%{reuniao_titulo}%")
    
    query += """
        ORDER BY a.data_criacao DESC, a.numero_ata DESC
    """
    
    cursor.execute(query, params)
    atas = cursor.fetchall()
    
    # ============================================
    # ESTATÍSTICAS PARA OS CARDS
    # ============================================
    
    stats_filter = ""
    stats_params = []
    
    if usuario_tipo != 'admin':
        if usuario_grau == 1:
            stats_filter += " AND (r.grau = 1 OR r.grau IS NULL)"
        elif usuario_grau == 2:
            stats_filter += " AND (r.grau IN (1, 2) OR r.grau IS NULL)"
        elif usuario_grau >= 3:
            stats_filter += " AND (r.grau <= 3 OR r.grau IS NULL OR r.grau > 3)"
    
    if data_ini:
        stats_filter += " AND a.data_criacao >= %s"
        stats_params.append(data_ini)
    if data_fim:
        stats_filter += " AND a.data_criacao <= %s"
        stats_params.append(data_fim)
    if reuniao_titulo:
        stats_filter += " AND (r.titulo ILIKE %s OR a.titulo ILIKE %s)"
        stats_params.append(f"%{reuniao_titulo}%")
        stats_params.append(f"%{reuniao_titulo}%")
    
    stats_query = f"""
        SELECT 
            COUNT(DISTINCT a.id) as total_atas,
            SUM(CASE WHEN a.aprovada = 1 THEN 1 ELSE 0 END) as aprovadas,
            SUM(CASE WHEN a.aprovada = 0 THEN 1 ELSE 0 END) as pendentes,
            SUM(CASE WHEN EXTRACT(MONTH FROM a.data_criacao) = EXTRACT(MONTH FROM CURRENT_DATE)
                      AND EXTRACT(YEAR FROM a.data_criacao) = EXTRACT(YEAR FROM CURRENT_DATE) THEN 1 ELSE 0 END) as atas_mes,
            SUM(CASE WHEN a.assinatura_veneravel = true THEN 1 ELSE 0 END) as assinaturas_veneravel,
            SUM(CASE WHEN a.assinatura_orador = true THEN 1 ELSE 0 END) as assinaturas_orador,
            SUM(CASE WHEN a.assinatura_secretario = true THEN 1 ELSE 0 END) as assinaturas_secretario
        FROM atas a
        LEFT JOIN reunioes r ON a.reuniao_id = r.id
        WHERE 1=1 {stats_filter}
    """
    
    cursor.execute(stats_query, stats_params)
    stats = cursor.fetchone()
    
    total_atas = stats['total_atas'] or 0
    aprovadas = stats['aprovadas'] or 0
    taxa_aprovacao = (aprovadas / total_atas * 100) if total_atas > 0 else 0
    
    estatisticas = {
        'total_atas': total_atas,
        'aprovadas': aprovadas,
        'pendentes': stats['pendentes'] or 0,
        'atas_mes': stats['atas_mes'] or 0,
        'total_assinaturas': (stats['assinaturas_veneravel'] or 0) + (stats['assinaturas_orador'] or 0) + (stats['assinaturas_secretario'] or 0),
        'media_assinaturas': ((stats['assinaturas_veneravel'] or 0) + (stats['assinaturas_orador'] or 0) + (stats['assinaturas_secretario'] or 0)) / total_atas if total_atas > 0 else 0,
        'taxa_aprovacao': taxa_aprovacao,
        'exibidas': len(atas)
    }
    
    return_connection(conn)
    
    return render_template("atas/lista.html", 
                          atas=atas,
                          estatisticas=estatisticas,
                          filtros={
                              'data_ini': data_ini,
                              'data_fim': data_fim,
                              'aprovada': aprovada,
                              'reuniao_titulo': reuniao_titulo
                          })


@app.route("/atas/<int:id>/visualizar_simples")
@login_required
def visualizar_ata_simples(id):
    """Visualizar ata de forma simples"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT 
                a.*,
                a.numero_ata as numero,
                a.data_criacao as data,
                r.titulo as reuniao_titulo,
                r.data as reuniao_data,
                r.hora_inicio,
                r.hora_termino,
                r.local,
                r.pauta,
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
        
        return_connection(conn)
        
        return render_template("atas/visualizar_simples.html", ata=ata)
        
    except Exception as e:
        print(f"❌ Erro ao visualizar ata simples {id}: {e}")
        import traceback
        traceback.print_exc()
        
        if 'conn' in locals():
            return_connection(conn)
        
        flash(f"Erro ao carregar ata: {str(e)}", "danger")
        return redirect("/atas")


@app.route("/atas/<int:id>/visualizar")
@login_required
def visualizar_ata_completa(id):
    """Visualizar ata completa com presença e assinaturas"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT 
                a.*,
                a.numero_ata as numero,
                a.data_criacao as data,
                r.titulo as reuniao_titulo,
                r.data as reuniao_data,
                r.hora_inicio,
                r.hora_termino,
                r.local,
                r.pauta,
                r.grau as reuniao_grau,
                u.nome_completo as redator_nome,
                u2.nome_completo as aprovado_por_nome,
                a.assinatura_veneravel,
                a.assinatura_orador,
                a.assinatura_secretario,
                a.data_assinatura_veneravel,
                a.data_assinatura_orador,
                a.data_assinatura_secretario,
                a.veneravel_mestre_nome,
                a.orador_nome,
                a.secretario_nome
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
        
        # Buscar lista de presença da reunião
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
        
        return_connection(conn)
        
        return render_template("atas/visualizar.html", ata=ata, presenca=presenca)
        
    except Exception as e:
        print(f"❌ Erro ao visualizar ata completa {id}: {e}")
        import traceback
        traceback.print_exc()
        
        if 'conn' in locals():
            return_connection(conn)
        
        flash(f"Erro ao carregar ata: {str(e)}", "danger")
        return redirect("/atas")


@app.route("/atas/nova/<int:reuniao_id>", methods=["GET", "POST"])
@login_required
@permissao_required('ata.create')
def nova_ata(reuniao_id):
    """Cria uma nova ata para uma reunião"""
    cursor, conn = get_db()
    
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
    
    # Verificar permissão específica para criar ata nesta reunião
    if session.get('tipo') != 'admin':
        usuario_grau = session.get('grau_atual', 0)
        reuniao_grau = reuniao.get('grau', 0)
        if reuniao_grau and reuniao_grau > usuario_grau:
            if not verificar_permissao(session['user_id'], 'ata.create_superior'):
                flash("Você não tem permissão para criar atas para reuniões de grau superior ao seu.", "danger")
                return_connection(conn)
                return redirect(f"/reunioes/{reuniao_id}")
    
    if request.method == "POST":
        conteudo = request.form.get("conteudo")
        modelo_id = request.form.get("modelo_id")
        
        if not conteudo or not conteudo.strip():
            flash("O conteúdo da ata é obrigatório!", "danger")
            return_connection(conn)
            return redirect(f"/atas/nova/{reuniao_id}")
        
        try:
            ano_atual = datetime.now().year
            cursor.execute("SELECT COUNT(*) as total FROM atas WHERE ano_ata = %s", (ano_atual,))
            total = cursor.fetchone()["total"]
            numero_ata = total + 1
            
            cursor.execute("""
                INSERT INTO atas (
                    reuniao_id, conteudo, redator_id, numero_ata, 
                    ano_ata, tipo_ata, data_criacao
                ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING id
            """, (reuniao_id, conteudo, session["user_id"], numero_ata, 
                  ano_atual, reuniao["tipo"]))
            
            ata_id = cursor.fetchone()['id']
            conn.commit()
            
            registrar_log("criar", "ata", ata_id, dados_novos={"reuniao_id": reuniao_id, "numero": numero_ata, "ano": ano_atual})
            
            flash(f"Ata nº {numero_ata}/{ano_atual} criada com sucesso!", "success")
            
            return_connection(conn)
            return redirect(f"/atas/{ata_id}/visualizar")
            
        except Exception as e:
            print(f"ERRO ao criar ata: {e}")
            conn.rollback()
            flash(f"Erro ao criar ata: {str(e)}", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{reuniao_id}")
    
    # GET - Carregar modelos
    modelos = []
    if verificar_permissao(session['user_id'], 'ata.use_template'):
        cursor.execute("SELECT * FROM modelos_ata WHERE ativo = 1")
        modelos = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("atas/nova.html", reuniao=reuniao, modelos=modelos)


@app.route("/atas/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_ata(id):
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT a.*, r.titulo as reuniao_titulo, r.status as reuniao_status, r.data as reuniao_data
        FROM atas a
        JOIN reunioes r ON a.reuniao_id = r.id
        WHERE a.id = %s
    """, (id,))
    ata = cursor.fetchone()
    
    if not ata:
        flash("Ata não encontrada", "danger")
        return_connection(conn)
        return redirect("/atas")
    
    if ata["aprovada"] == 1:
        flash("Ata já aprovada, não pode ser editada!", "warning")
        return_connection(conn)
        return redirect(f"/atas/{id}")
    
    if request.method == "POST":
        titulo = request.form.get("titulo")
        conteudo = request.form.get("conteudo")
        tipo_ata = request.form.get("tipo_ata")
        numero_ata = request.form.get("numero_ata")
        ano_ata = request.form.get("ano_ata")
        
        if not conteudo:
            flash("Conteúdo da ata é obrigatório", "danger")
        else:
            try:
                versao_atual = ata.get('versao') or 0
                nova_versao = versao_atual + 1
                
                cursor.execute("""
                    UPDATE atas 
                    SET titulo = %s,
                        conteudo = %s,
                        tipo_ata = %s,
                        numero_ata = %s,
                        ano_ata = %s,
                        versao = %s
                    WHERE id = %s
                """, (titulo, conteudo, tipo_ata, numero_ata, ano_ata, nova_versao, id))
                
                conn.commit()
                registrar_log("editar", "ata", id, dados_novos={"versao": nova_versao, "titulo": titulo})
                flash("Ata atualizada com sucesso!", "success")
                return_connection(conn)
                return redirect(f"/atas/{id}")
                
            except Exception as e:
                print(f"Erro ao atualizar ata: {e}")
                conn.rollback()
                flash(f"Erro ao atualizar ata: {str(e)}", "danger")
    
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
    flash("Ata aprovada com sucesso! Agora os responsáveis podem assinar.", "success")
    return_connection(conn)
    return redirect(f"/atas/{id}")


@app.route("/atas/<int:id>/excluir", methods=["POST"])
@login_required
@permissao_required('ata.delete')
def excluir_ata(id):
    """Exclui uma ata (apenas se não estiver aprovada ou se for admin)"""
    cursor, conn = get_db()
    
    try:
        cursor.execute("""
            SELECT a.id, a.numero_ata, a.ano_ata, a.aprovada, r.titulo as reuniao_titulo
            FROM atas a
            JOIN reunioes r ON a.reuniao_id = r.id
            WHERE a.id = %s
        """, (id,))
        ata = cursor.fetchone()
        
        if not ata:
            flash("Ata não encontrada!", "danger")
            return_connection(conn)
            return redirect("/atas")
        
        if ata['aprovada'] == 1 and session.get('tipo') != 'admin':
            flash("Não é possível excluir uma ata já aprovada!", "danger")
            return_connection(conn)
            return redirect(f"/atas/{id}")
        
        registrar_log("excluir", "ata", id, dados_anteriores={
            "numero": ata['numero_ata'],
            "ano": ata['ano_ata'],
            "reuniao": ata['reuniao_titulo']
        })
        
        cursor.execute("DELETE FROM atas WHERE id = %s", (id,))
        conn.commit()
        
        flash(f"Ata nº {ata['numero_ata']}/{ata['ano_ata']} excluída com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao excluir ata: {e}")
        conn.rollback()
        flash(f"Erro ao excluir ata: {str(e)}", "danger")
    
    return_connection(conn)
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


@app.route("/atas/modelos/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_modelo(id):
    cursor, conn = get_db()
    
    if request.method == "POST":
        nome = request.form.get("nome")
        descricao = request.form.get("descricao")
        tipo = request.form.get("tipo")
        estrutura = request.form.get("estrutura")
        
        cursor.execute("""
            UPDATE modelos_ata 
            SET nome = %s, descricao = %s, tipo = %s, estrutura = %s
            WHERE id = %s
        """, (nome, descricao, tipo, estrutura, id))
        conn.commit()
        registrar_log("editar", "modelo_ata", id, dados_novos={"nome": nome})
        flash("Modelo atualizado com sucesso!", "success")
        return_connection(conn)
        return redirect("/atas/modelos")
    
    cursor.execute("SELECT * FROM modelos_ata WHERE id = %s", (id,))
    modelo = cursor.fetchone()
    return_connection(conn)
    return render_template("atas/modelo_editar.html", modelo=modelo)

# =============================
# ROTAS DE COMUNICADOS/OCORRÊNCIAS
# =============================

@app.route("/obreiros/<int:obreiro_id>/comunicados")
@login_required
def listar_comunicados_obreiro(obreiro_id):
    """Lista comunicados/ocorrências do obreiro"""
    cursor, conn = get_db()
    
    # Verificar permissão: admin, mestre (grau >= 3) ou o próprio obreiro
    usuario_tipo = session.get('tipo', '')
    usuario_grau = session.get('grau_atual', 0)
    
    if usuario_tipo != 'admin' and usuario_grau < 3 and session['user_id'] != obreiro_id:
        flash("Permissão negada!", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    
    # Buscar obreiro
    cursor.execute("SELECT id, nome_completo, usuario FROM usuarios WHERE id = %s", (obreiro_id,))
    obreiro = cursor.fetchone()
    
    if not obreiro:
        flash("Obreiro não encontrado!", "danger")
        return_connection(conn)
        return redirect("/obreiros")
    
    # Buscar comunicados
    cursor.execute("""
        SELECT c.*, t.nome as tipo_nome, t.cor as tipo_cor, t.icone as tipo_icone,
               u.nome_completo as registrado_por_nome,
               a.nome_completo as aprovado_por_nome
        FROM comunicados_obreiro c
        LEFT JOIN tipos_ocorrencia t ON c.tipo_ocorrencia_id = t.id
        LEFT JOIN usuarios u ON c.registrado_por = u.id
        LEFT JOIN usuarios a ON c.aprovado_por = a.id
        WHERE c.obreiro_id = %s AND c.ativo = 1
        ORDER BY c.data_ocorrencia DESC, c.data_registro DESC
    """, (obreiro_id,))
    
    comunicados = cursor.fetchall()
    
    # Buscar tipos de ocorrência (apenas admin pode criar)
    tipos_ocorrencia = []
    if session["tipo"] == "admin" or usuario_grau >= 3:
        cursor.execute("SELECT * FROM tipos_ocorrencia WHERE ativo = 1 ORDER BY ordem")
        tipos_ocorrencia = cursor.fetchall()
    
    return_connection(conn)
    
    # Verificar se pode criar/editar (apenas admin ou mestre)
    pode_editar = (session["tipo"] == "admin" or usuario_grau >= 3)
    
    return render_template("obreiros/comunicados.html", 
                          obreiro=obreiro, 
                          comunicados=comunicados,
                          tipos_ocorrencia=tipos_ocorrencia,
                          pode_editar=pode_editar)

@app.route("/obreiros/<int:obreiro_id>/comunicados/novo", methods=["GET", "POST"])
@login_required
def novo_comunicado_obreiro(obreiro_id):
    """Criar novo comunicado (apenas admin ou mestre)"""
    cursor, conn = get_db()
    
    usuario_tipo = session.get('tipo', '')
    usuario_grau = session.get('grau_atual', 0)
    
    # Verificar permissão: apenas admin ou mestre (grau >= 3)
    if usuario_tipo != 'admin' and usuario_grau < 3:
        flash("Apenas Mestres e Administradores podem criar comunicados!", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    
    # Buscar obreiro
    cursor.execute("SELECT id, nome_completo, usuario, ativo FROM usuarios WHERE id = %s", (obreiro_id,))
    obreiro = cursor.fetchone()
    
    if not obreiro:
        flash("Obreiro não encontrado!", "danger")
        return redirect("/obreiros")
    
    if request.method == "POST":
        tipo_ocorrencia_id = request.form.get("tipo_ocorrencia_id")
        titulo = request.form.get("titulo")
        descricao = request.form.get("descricao")
        data_ocorrencia = request.form.get("data_ocorrencia")
        motivo = request.form.get("motivo")
        detalhes = request.form.get("detalhes")
        data_inicio_licenca = request.form.get("data_inicio_licenca")
        data_fim_licenca = request.form.get("data_fim_licenca")
        data_desligamento = request.form.get("data_desligamento")
        motivo_desligamento = request.form.get("motivo_desligamento")
        
        # Validações
        if not tipo_ocorrencia_id or not titulo or not descricao or not data_ocorrencia:
            flash("Preencha todos os campos obrigatórios!", "danger")
            return redirect(f"/obreiros/{obreiro_id}/comunicados/novo")
        
        try:
            # Buscar tipo de ocorrência
            cursor.execute("SELECT * FROM tipos_ocorrencia WHERE id = %s", (tipo_ocorrencia_id,))
            tipo = cursor.fetchone()
            
            # Definir status inicial
            status = 'aprovado' if session.get('tipo') == 'admin' else 'pendente'
            
            cursor.execute("""
                INSERT INTO comunicados_obreiro 
                (obreiro_id, tipo_ocorrencia_id, titulo, descricao, data_ocorrencia,
                 status, motivo, detalhes, data_inicio_licenca, data_fim_licenca,
                 data_desligamento, motivo_desligamento, registrado_por,
                 aprovado_por, data_aprovacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (obreiro_id, tipo_ocorrencia_id, titulo, descricao, data_ocorrencia,
                  status, motivo, detalhes, data_inicio_licenca, data_fim_licenca,
                  data_desligamento, motivo_desligamento, session['user_id'],
                  session['user_id'] if status == 'aprovado' else None,
                  CURRENT_TIMESTAMP if status == 'aprovado' else None))
            
            comunicado_id = cursor.fetchone()['id']
            
            # Registrar histórico
            cursor.execute("""
                INSERT INTO historico_comunicados 
                (ocorrencia_id, acao, status_novo, realizado_por, observacao)
                VALUES (%s, %s, %s, %s, %s)
            """, (comunicado_id, 'criar', status, session['user_id'], 'Comunicado criado'))
            conn.commit()
            
            # Se for desligamento, atualizar status do obreiro
            if tipo['nome'] in ['Desligamento Voluntário', 'Desligamento por Frequência', 
                                'Desligamento por Inadimplência', 'Exclusão']:
                cursor.execute("UPDATE usuarios SET ativo = 0 WHERE id = %s", (obreiro_id,))
                conn.commit()
                flash(f"⚠️ Obreiro {obreiro['nome_completo']} foi desativado!", "warning")
            
            registrar_log("criar_comunicado", "comunicado_obreiro", comunicado_id,
                         dados_novos={"obreiro_id": obreiro_id, "tipo": tipo['nome'], "titulo": titulo})
            
            flash(f"Comunicado '{titulo}' criado com sucesso!", "success")
            
            # Se precisar de aprovação
            if status == 'pendente':
                flash("O comunicado foi enviado para aprovação da diretoria.", "info")
            
            return redirect(f"/obreiros/{obreiro_id}/comunicados")
            
        except Exception as e:
            print(f"Erro ao criar comunicado: {e}")
            conn.rollback()
            flash(f"Erro ao criar comunicado: {str(e)}", "danger")
    
    # GET - Carregar formulário
    cursor.execute("SELECT * FROM tipos_ocorrencia WHERE ativo = 1 ORDER BY ordem")
    tipos_ocorrencia = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("obreiros/comunicado_form.html", 
                          obreiro=obreiro, 
                          tipos_ocorrencia=tipos_ocorrencia,
                          hoje=datetime.now().strftime('%Y-%m-%d'))


@app.route("/comunicados/<int:id>/aprovar", methods=["POST"])
@login_required
def aprovar_comunicado(id):
    """Aprovar um comunicado pendente"""
    cursor, conn = get_db()
    
    # Verificar permissão (apenas admin ou mestres)
    if session.get('tipo') != 'admin' and session.get('grau_atual', 0) < 3:
        flash("Apenas Mestres e Administradores podem aprovar comunicados!", "danger")
        return redirect(request.referrer or "/dashboard")
    
    observacao = request.form.get("observacao", "")
    
    try:
        # Buscar comunicado
        cursor.execute("""
            SELECT c.*, t.nome as tipo_nome, u.nome_completo as obreiro_nome
            FROM comunicados_obreiro c
            LEFT JOIN tipos_ocorrencia t ON c.tipo_ocorrencia_id = t.id
            LEFT JOIN usuarios u ON c.obreiro_id = u.id
            WHERE c.id = %s
        """, (id,))
        
        comunicado = cursor.fetchone()
        
        if not comunicado:
            flash("Comunicado não encontrado!", "danger")
            return redirect("/dashboard")
        
        if comunicado['status'] != 'pendente':
            flash("Este comunicado já foi processado!", "warning")
            return redirect(request.referrer or "/dashboard")
        
        # Atualizar status
        cursor.execute("""
            UPDATE comunicados_obreiro 
            SET status = 'aprovado', 
                aprovado_por = %s, 
                data_aprovacao = CURRENT_TIMESTAMP,
                observacao_aprovacao = %s
            WHERE id = %s
        """, (session['user_id'], observacao, id))
        
        # Registrar histórico
        cursor.execute("""
            INSERT INTO historico_comunicados 
            (ocorrencia_id, acao, status_anterior, status_novo, realizado_por, observacao)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (id, 'aprovar', 'pendente', 'aprovado', session['user_id'], observacao))
        
        conn.commit()
        
        registrar_log("aprovar_comunicado", "comunicado_obreiro", id,
                     dados_novos={"status": "aprovado", "aprovado_por": session['user_id']})
        
        flash(f"Comunicado '{comunicado['titulo']}' aprovado com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao aprovar comunicado: {e}")
        conn.rollback()
        flash(f"Erro ao aprovar comunicado: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(request.referrer or "/dashboard")


@app.route("/comunicados/<int:id>/rejeitar", methods=["POST"])
@login_required
def rejeitar_comunicado(id):
    """Rejeitar um comunicado pendente"""
    cursor, conn = get_db()
    
    # Verificar permissão
    if session.get('tipo') != 'admin' and session.get('grau_atual', 0) < 3:
        flash("Apenas Mestres e Administradores podem rejeitar comunicados!", "danger")
        return redirect(request.referrer or "/dashboard")
    
    motivo = request.form.get("motivo", "")
    
    try:
        cursor.execute("""
            UPDATE comunicados_obreiro 
            SET status = 'rejeitado', 
                observacao_aprovacao = %s
            WHERE id = %s
        """, (motivo, id))
        
        # Registrar histórico
        cursor.execute("""
            INSERT INTO historico_comunicados 
            (ocorrencia_id, acao, status_anterior, status_novo, realizado_por, observacao)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (id, 'rejeitar', 'pendente', 'rejeitado', session['user_id'], motivo))
        
        conn.commit()
        
        flash(f"Comunicado rejeitado.", "warning")
        
    except Exception as e:
        print(f"Erro ao rejeitar comunicado: {e}")
        conn.rollback()
        flash(f"Erro ao rejeitar comunicado: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(request.referrer or "/dashboard")


@app.route("/comunicados/<int:id>/documentos/upload", methods=["POST"])
@login_required
def upload_documento_comunicado(id):
    """Upload de documento anexo ao comunicado"""
    cursor, conn = get_db()
    
    # Verificar permissão
    if session.get('tipo') != 'admin' and session.get('grau_atual', 0) < 3:
        flash("Permissão negada!", "danger")
        return redirect(request.referrer or "/dashboard")
    
    if 'arquivo' not in request.files:
        flash("Nenhum arquivo selecionado!", "danger")
        return redirect(request.referrer or "/dashboard")
    
    arquivo = request.files['arquivo']
    titulo = request.form.get('titulo')
    descricao = request.form.get('descricao')
    
    if not titulo:
        titulo = arquivo.filename
    
    if arquivo.filename == '':
        flash("Nenhum arquivo selecionado!", "danger")
        return redirect(request.referrer or "/dashboard")
    
    try:
        import cloudinary.uploader
        from werkzeug.utils import secure_filename
        
        nome_arquivo = secure_filename(arquivo.filename)
        extensao = nome_arquivo.split('.')[-1].lower()
        
        # Upload para Cloudinary
        upload_result = cloudinary.uploader.upload(
            arquivo,
            folder=f"comunicados/{id}",
            resource_type="auto",
            use_filename=True,
            unique_filename=True
        )
        
        url_arquivo = upload_result.get('secure_url')
        public_id = upload_result.get('public_id')
        tamanho = upload_result.get('bytes', 0)
        
        # Salvar no banco
        cursor.execute("""
            INSERT INTO documentos_ocorrencia 
            (ocorrencia_id, titulo, descricao, nome_arquivo, caminho_arquivo, tipo_arquivo, tamanho, uploaded_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (id, titulo, descricao, public_id, url_arquivo, extensao, tamanho, session['user_id']))
        
        conn.commit()
        
        flash(f"Documento '{titulo}' anexado com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro no upload: {e}")
        flash(f"Erro ao enviar documento: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(request.referrer or f"/comunicados/{id}/detalhes")
    
# =============================
# ROTAS DE AVISOS/COMUNICADOS
# =============================

@app.route("/avisos")
@login_required
def listar_avisos():
    """Lista avisos conforme grau do usuário"""
    cursor, conn = get_db()
    
    usuario_grau = session.get('grau_atual', 1)
    usuario_id = session['user_id']
    
    # Buscar avisos permitidos para o grau do usuário
    cursor.execute("""
        SELECT a.*, u.nome_completo as autor_nome,
               CASE WHEN av.id IS NOT NULL THEN 1 ELSE 0 END as ja_visto
        FROM avisos a
        LEFT JOIN usuarios u ON a.created_by = u.id
        LEFT JOIN avisos_visualizacoes av ON a.id = av.aviso_id AND av.usuario_id = %s
        WHERE a.ativo = 1 
        AND a.data_inicio <= CURRENT_TIMESTAMP
        AND (a.data_fim IS NULL OR a.data_fim >= CURRENT_TIMESTAMP)
        AND a.grau_destino <= %s
        ORDER BY 
            CASE a.prioridade 
                WHEN 'urgente' THEN 1 
                WHEN 'importante' THEN 2 
                ELSE 3 
            END,
            a.created_at DESC
    """, (usuario_id, usuario_grau))
    
    avisos = cursor.fetchall()
    
    # Estatísticas
    cursor.execute("""
        SELECT COUNT(*) as total_nao_lidos
        FROM avisos a
        LEFT JOIN avisos_visualizacoes av ON a.id = av.aviso_id AND av.usuario_id = %s
        WHERE a.ativo = 1 
        AND a.data_inicio <= CURRENT_TIMESTAMP
        AND (a.data_fim IS NULL OR a.data_fim >= CURRENT_TIMESTAMP)
        AND a.grau_destino <= %s
        AND av.id IS NULL
    """, (usuario_id, usuario_grau))
    
    nao_lidos = cursor.fetchone()['total_nao_lidos']
    
    return_connection(conn)
    
    return render_template("avisos/lista.html", 
                          avisos=avisos, 
                          nao_lidos=nao_lidos,
                          usuario_grau=usuario_grau)


@app.route("/avisos/novo", methods=["GET", "POST"])
@login_required
def novo_aviso():
    """Criar novo aviso (Mestres e Admin)"""
    cursor, conn = get_db()
    
    usuario_grau = session.get('grau_atual', 1)
    usuario_tipo = session.get('tipo', '')
    
    # Verificar permissão: apenas Mestres (grau >= 3) e Admin
    if usuario_grau < 3 and usuario_tipo != 'admin':
        flash("Apenas Mestres e Administradores podem criar avisos!", "danger")
        return redirect("/dashboard")
    
    if request.method == "POST":
        titulo = request.form.get("titulo")
        conteudo = request.form.get("conteudo")
        grau_destino = request.form.get("grau_destino", 1)
        prioridade = request.form.get("prioridade", "normal")
        data_fim = request.form.get("data_fim")
        
        if not titulo or not conteudo:
            flash("Título e conteúdo são obrigatórios!", "danger")
            return redirect("/avisos/novo")
        
        try:
            data_fim = data_fim if data_fim and data_fim.strip() else None
            
            cursor.execute("""
                INSERT INTO avisos (titulo, conteudo, grau_destino, prioridade, 
                                   data_fim, created_by, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING id
            """, (titulo, conteudo, grau_destino, prioridade, data_fim, session['user_id']))
            
            aviso_id = cursor.fetchone()['id']
            conn.commit()
            
            registrar_log("criar", "aviso", aviso_id, dados_novos={"titulo": titulo})
            flash("Aviso criado com sucesso!", "success")
            
            return redirect("/avisos")
            
        except Exception as e:
            print(f"Erro ao criar aviso: {e}")
            conn.rollback()
            flash(f"Erro ao criar aviso: {str(e)}", "danger")
            return redirect("/avisos/novo")
    
    return_connection(conn)
    return render_template("avisos/novo.html")


@app.route("/avisos/<int:id>/editar", methods=["GET", "POST"])
@login_required
def editar_aviso(id):
    """Editar aviso (apenas autor ou admin)"""
    cursor, conn = get_db()
    
    usuario_id = session['user_id']
    usuario_grau = session.get('grau_atual', 1)
    usuario_tipo = session.get('tipo', '')
    
    # Buscar aviso
    cursor.execute("""
        SELECT * FROM avisos WHERE id = %s
    """, (id,))
    aviso = cursor.fetchone()
    
    if not aviso:
        flash("Aviso não encontrado!", "danger")
        return redirect("/avisos")
    
    # Verificar permissão (autor ou admin)
    if aviso['created_by'] != usuario_id and usuario_tipo != 'admin':
        flash("Você não tem permissão para editar este aviso!", "danger")
        return redirect("/avisos")
    
    if request.method == "POST":
        titulo = request.form.get("titulo")
        conteudo = request.form.get("conteudo")
        grau_destino = request.form.get("grau_destino")
        prioridade = request.form.get("prioridade")
        data_fim = request.form.get("data_fim")
        ativo = 1 if request.form.get("ativo") else 0
        
        if not titulo or not conteudo:
            flash("Título e conteúdo são obrigatórios!", "danger")
            return redirect(f"/avisos/{id}/editar")
        
        try:
            data_fim = data_fim if data_fim and data_fim.strip() else None
            
            dados_antigos = dict(aviso)
            
            cursor.execute("""
                UPDATE avisos SET
                    titulo = %s,
                    conteudo = %s,
                    grau_destino = %s,
                    prioridade = %s,
                    data_fim = %s,
                    ativo = %s,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = %s
                WHERE id = %s
            """, (titulo, conteudo, grau_destino, prioridade, data_fim, ativo, usuario_id, id))
            
            conn.commit()
            
            registrar_log("editar", "aviso", id, 
                         dados_anteriores=dados_antigos,
                         dados_novos={"titulo": titulo})
            
            flash("Aviso atualizado com sucesso!", "success")
            return redirect("/avisos")
            
        except Exception as e:
            print(f"Erro ao editar aviso: {e}")
            conn.rollback()
            flash(f"Erro ao editar aviso: {str(e)}", "danger")
            return redirect(f"/avisos/{id}/editar")
    
    return_connection(conn)
    return render_template("avisos/editar.html", aviso=aviso)


@app.route("/avisos/<int:id>/excluir", methods=["POST"])
@login_required
def excluir_aviso(id):
    """Excluir aviso (apenas autor ou admin)"""
    cursor, conn = get_db()
    
    usuario_id = session['user_id']
    usuario_tipo = session.get('tipo', '')
    
    # Buscar aviso
    cursor.execute("SELECT * FROM avisos WHERE id = %s", (id,))
    aviso = cursor.fetchone()
    
    if not aviso:
        flash("Aviso não encontrado!", "danger")
        return redirect("/avisos")
    
    # Verificar permissão
    if aviso['created_by'] != usuario_id and usuario_tipo != 'admin':
        flash("Você não tem permissão para excluir este aviso!", "danger")
        return redirect("/avisos")
    
    try:
        # Excluir visualizações primeiro
        cursor.execute("DELETE FROM avisos_visualizacoes WHERE aviso_id = %s", (id,))
        # Excluir aviso
        cursor.execute("DELETE FROM avisos WHERE id = %s", (id,))
        conn.commit()
        
        registrar_log("excluir", "aviso", id, dados_anteriores={"titulo": aviso['titulo']})
        flash("Aviso excluído com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao excluir aviso: {e}")
        conn.rollback()
        flash(f"Erro ao excluir aviso: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect("/avisos")


@app.route("/avisos/<int:id>/marcar-visto", methods=["POST"])
@login_required
def marcar_aviso_visto(id):
    """Marcar aviso como visualizado"""
    cursor, conn = get_db()
    
    try:
        cursor.execute("""
            INSERT INTO avisos_visualizacoes (aviso_id, usuario_id)
            VALUES (%s, %s)
            ON CONFLICT (aviso_id, usuario_id) DO NOTHING
        """, (id, session['user_id']))
        
        cursor.execute("""
            UPDATE avisos SET visualizacoes = visualizacoes + 1
            WHERE id = %s
        """, (id,))
        
        conn.commit()
        return jsonify({'success': True})
        
    except Exception as e:
        print(f"Erro ao marcar visto: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        return_connection(conn)


@app.route("/api/avisos/nao-lidos")
@login_required
def api_avisos_nao_lidos():
    """API para buscar quantidade de avisos não lidos (para o badge)"""
    cursor, conn = get_db()
    
    usuario_grau = session.get('grau_atual', 1)
    usuario_id = session['user_id']
    
    cursor.execute("""
        SELECT COUNT(*) as total
        FROM avisos a
        LEFT JOIN avisos_visualizacoes av ON a.id = av.aviso_id AND av.usuario_id = %s
        WHERE a.ativo = 1 
        AND a.data_inicio <= CURRENT_TIMESTAMP
        AND (a.data_fim IS NULL OR a.data_fim >= CURRENT_TIMESTAMP)
        AND a.grau_destino <= %s
        AND av.id IS NULL
    """, (usuario_id, usuario_grau))
    
    total = cursor.fetchone()['total']
    return_connection(conn)
    
    return jsonify({'nao_lidos': total})
    
@app.route("/api/avisos/ultimos")
@login_required
def api_avisos_ultimos():
    """Retorna os últimos 5 avisos para o sininho"""
    cursor, conn = get_db()
    
    usuario_grau = session.get('grau_atual', 1)
    usuario_id = session['user_id']
    
    cursor.execute("""
        SELECT a.id, a.titulo, a.conteudo, a.prioridade, a.created_at,
               u.nome_completo as autor_nome,
               CASE WHEN av.id IS NOT NULL THEN 1 ELSE 0 END as ja_visto
        FROM avisos a
        LEFT JOIN usuarios u ON a.created_by = u.id
        LEFT JOIN avisos_visualizacoes av ON a.id = av.aviso_id AND av.usuario_id = %s
        WHERE a.ativo = 1 
        AND a.data_inicio <= CURRENT_TIMESTAMP
        AND (a.data_fim IS NULL OR a.data_fim >= CURRENT_TIMESTAMP)
        AND a.grau_destino <= %s
        ORDER BY a.created_at DESC
        LIMIT 5
    """, (usuario_id, usuario_grau))
    
    avisos = cursor.fetchall()
    
    # Contar não lidos
    cursor.execute("""
        SELECT COUNT(*) as total
        FROM avisos a
        LEFT JOIN avisos_visualizacoes av ON a.id = av.aviso_id AND av.usuario_id = %s
        WHERE a.ativo = 1 
        AND a.data_inicio <= CURRENT_TIMESTAMP
        AND (a.data_fim IS NULL OR a.data_fim >= CURRENT_TIMESTAMP)
        AND a.grau_destino <= %s
        AND av.id IS NULL
    """, (usuario_id, usuario_grau))
    
    nao_lidos = cursor.fetchone()['total']
    
    return_connection(conn)
    
    # Converter dados para JSON
    avisos_list = []
    for aviso in avisos:
        avisos_list.append({
            'id': aviso['id'],
            'titulo': aviso['titulo'],
            'conteudo': aviso['conteudo'],
            'prioridade': aviso['prioridade'],
            'created_at': aviso['created_at'].isoformat() if aviso['created_at'] else None,
            'autor_nome': aviso['autor_nome'],
            'ja_visto': aviso['ja_visto']
        })
    
    return jsonify({
        'success': True,
        'avisos': avisos_list,
        'nao_lidos': nao_lidos
    })


@app.route("/api/avisos/marcar-visto/<int:id>", methods=["POST"])
@login_required
def api_marcar_aviso_visto(id):
    """Marca aviso como visualizado via AJAX"""
    cursor, conn = get_db()
    
    try:
        cursor.execute("""
            INSERT INTO avisos_visualizacoes (aviso_id, usuario_id, visualizado_em)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (aviso_id, usuario_id) DO NOTHING
        """, (id, session['user_id']))
        
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        print(f"Erro ao marcar visto: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        return_connection(conn)    

# =============================
# ROTAS DE CANDIDATOS PLACET E OBREIRO
# =============================
@app.route("/candidatos", methods=["GET", "POST"])
@login_required
@permissao_required('candidato.view')
def gerenciar_candidatos():
    cursor, conn = get_db()
    
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        
        if nome:
            try:
                from datetime import datetime
                agora = datetime.now()
                cursor.execute("INSERT INTO candidatos (nome, data_criacao) VALUES (%s, %s)", (nome, agora))
                conn.commit()
                candidato_id = cursor.lastrowid
                
                try:
                    registrar_log("criar", "candidato", candidato_id, dados_novos={"nome": nome})
                except Exception as log_err:
                    print(f"Erro no log: {log_err}")
                
                flash(f"Candidato '{nome}' adicionado com sucesso!", "success")
                
            except Exception as e:
                conn.rollback()
                print(f"Erro ao inserir: {str(e)}")
                flash(f"Erro ao adicionar candidato: {str(e)}", "danger")
        else:
            flash("Nome do candidato não pode estar vazio", "danger")
        
        return_connection(conn)
        return redirect("/candidatos")
    
    # ============================================
    # GET - Buscar candidatos (APENAS NÃO OBREIROS)
    # ============================================
    
    try:
        conn.rollback()
    except:
        pass
    
    try:
        cursor.execute("""
            SELECT 
                c.*,
                COALESCE(pc.total_votos, 0) as total_votos,
                COALESCE(pc.votos_positivos, 0) as votos_positivos,
                COALESCE(pc.votos_negativos, 0) as votos_negativos,
                (SELECT COUNT(*) FROM usuarios WHERE tipo = 'sindicante' AND ativo = 1) as total_sindicantes
            FROM candidatos c
            LEFT JOIN (
                SELECT 
                    candidato_id,
                    COUNT(*) as total_votos,
                    COUNT(CASE WHEN conclusao = 'APROVADO' THEN 1 END) as votos_positivos,
                    COUNT(CASE WHEN conclusao = 'REPROVADO' THEN 1 END) as votos_negativos
                FROM pareceres_conclusivos
                GROUP BY candidato_id
            ) pc ON c.id = pc.candidato_id
            WHERE c.obreiro_id IS NULL
            ORDER BY c.data_criacao DESC
        """)
        candidatos = cursor.fetchall()
        
    except Exception as e:
        print(f"Erro ao buscar candidatos: {str(e)}")
        conn.rollback()
        candidatos = []
    
    # ============================================
    # BUSCAR STATUS DOS DOCUMENTOS POR CANDIDATO
    # ============================================
    
    documentos_status = {}
    
    try:
        cursor.execute("SELECT COUNT(*) as total FROM tipos_documentos_candidato WHERE obrigatorio = 1")
        result = cursor.fetchone()
        total_tipos_obrigatorios = result['total'] if result else 0
        
        cursor.execute("""
            SELECT 
                d.candidato_id,
                COUNT(d.id) as enviados,
                SUM(CASE WHEN d.status = 'aprovado' THEN 1 ELSE 0 END) as aprovados,
                SUM(CASE WHEN d.status = 'pendente' THEN 1 ELSE 0 END) as pendentes,
                SUM(CASE WHEN d.status = 'rejeitado' THEN 1 ELSE 0 END) as rejeitados
            FROM documentos_candidato d
            GROUP BY d.candidato_id
        """)
        docs_enviados = cursor.fetchall()
        
        for doc in docs_enviados:
            documentos_status[doc['candidato_id']] = {
                'total': total_tipos_obrigatorios,
                'enviados': doc['enviados'],
                'faltantes': max(0, total_tipos_obrigatorios - doc['enviados']),
                'aprovados': doc['aprovados'] or 0,
                'pendentes': doc['pendentes'] or 0,
                'rejeitados': doc['rejeitados'] or 0
            }
        
        for candidato in candidatos:
            if candidato['id'] not in documentos_status:
                documentos_status[candidato['id']] = {
                    'total': total_tipos_obrigatorios,
                    'enviados': 0,
                    'faltantes': total_tipos_obrigatorios,
                    'aprovados': 0,
                    'pendentes': 0,
                    'rejeitados': 0
                }
    except Exception as e:
        print(f"Erro ao buscar status dos documentos: {str(e)}")
        conn.rollback()
        for candidato in candidatos:
            documentos_status[candidato['id']] = {
                'total': 0,
                'enviados': 0,
                'faltantes': 0,
                'aprovados': 0,
                'pendentes': 0,
                'rejeitados': 0
            }
    
    # Buscar sindicantes
    try:
        cursor.execute("""
            SELECT id, usuario, nome_completo, cim_numero, 
                   loja_nome, loja_numero, loja_orient, ativo,
                   telefone
            FROM usuarios 
            WHERE tipo = 'sindicante' AND ativo = 1 AND grau_atual >= 3
            ORDER BY nome_completo
        """)
        sindicantes = cursor.fetchall()
    except Exception as e:
        print(f"Erro ao buscar sindicantes: {str(e)}")
        conn.rollback()
        sindicantes = []
    
    # Buscar lojas para o modal do Placet
    try:
        cursor.execute("SELECT id, nome, numero FROM lojas WHERE ativo = 1 ORDER BY nome")
        lojas = cursor.fetchall()
    except Exception as e:
        print(f"Erro ao buscar lojas: {str(e)}")
        lojas = []
    
    # Buscar sindicantes disponíveis (para o modal de designação)
    try:
        cursor.execute("""
            SELECT id, usuario, nome_completo, cim_numero 
            FROM usuarios 
            WHERE tipo = 'sindicante' AND ativo = 1
            ORDER BY nome_completo
        """)
        sindicantes_disponiveis = cursor.fetchall()
    except Exception as e:
        print(f"Erro ao buscar sindicantes disponíveis: {str(e)}")
        sindicantes_disponiveis = []
    
    # Buscar designações existentes por candidato
    try:
        cursor.execute("""
            SELECT candidato_id, sindicante_id 
            FROM sindicantes_candidato
        """)
        designacoes = cursor.fetchall()
        
        designados_por_candidato = {}
        for d in designacoes:
            if d['candidato_id'] not in designados_por_candidato:
                designados_por_candidato[d['candidato_id']] = []
            designados_por_candidato[d['candidato_id']].append(d['sindicante_id'])
    except Exception as e:
        print(f"Erro ao buscar designações: {str(e)}")
        designados_por_candidato = {}
    
    return_connection(conn)
    
    return render_template("candidatos.html", 
                          candidatos=candidatos, 
                          sindicantes=sindicantes, 
                          documentos_status=documentos_status,
                          lojas=lojas,
                          sindicantes_disponiveis=sindicantes_disponiveis,
                          designados_por_candidato=designados_por_candidato,
                          tipo=session.get("tipo", "admin"))

@app.route("/emitir-placet/<int:candidato_id>", methods=["POST"])
@login_required
def emitir_placet(candidato_id):
    """Emitir placet de iniciação e transformar candidato em obreiro"""
    if session.get('tipo') != 'admin':
        flash("Acesso negado! Apenas administradores podem emitir placet.", "danger")
        return redirect("/candidatos")
    
    try:
        cursor, conn = get_db()
        
        # 1. Buscar candidato aprovado
        cursor.execute("""
            SELECT c.*, u.id as usuario_id 
            FROM candidatos c
            LEFT JOIN usuarios u ON u.cpf = c.cpf
            WHERE c.id = %s AND c.fechado = 1 AND c.status = 'Aprovado'
        """, (candidato_id,))
        candidato = cursor.fetchone()
        
        if not candidato:
            flash("Candidato não encontrado ou não está aprovado!", "danger")
            return redirect("/candidatos")
        
        # 2. Coletar dados do formulário
        numero_placet = request.form.get("numero_placet")
        data_emissao = request.form.get("data_emissao")
        data_iniciacao = request.form.get("data_iniciacao")
        loja_id = request.form.get("loja_id") or None
        observacoes = request.form.get("observacoes")
        
        if not numero_placet or not data_emissao:
            flash("Número do Placet e Data de Emissão são obrigatórios!", "danger")
            return redirect("/candidatos")
        
        # 3. Verificar se o placet já existe
        cursor.execute("SELECT id FROM placet_iniciacao WHERE numero_placet = %s", (numero_placet,))
        if cursor.fetchone():
            flash(f"Placet número {numero_placet} já está cadastrado!", "danger")
            return redirect("/candidatos")
        
        # 4. Verificar se o candidato já virou obreiro
        if candidato.get('obreiro_id'):
            flash("Este candidato já foi transformado em obreiro!", "warning")
            return redirect("/candidatos")
        
        # 5. Buscar dados da loja
        loja_nome = None
        loja_numero = None
        loja_orient = None
        if loja_id:
            cursor.execute("SELECT nome, numero, oriente FROM lojas WHERE id = %s", (loja_id,))
            loja = cursor.fetchone()
            if loja:
                loja_nome = loja['nome']
                loja_numero = loja['numero']
                loja_orient = loja['oriente']
        
        # 6. Gerar CIM
        import hashlib
        import secrets
        import re
        from werkzeug.security import generate_password_hash
        from datetime import datetime
        
        cim_base = f"{candidato['cpf']}{candidato['id']}{secrets.token_hex(4)}"
        cim_numero = hashlib.md5(cim_base.encode()).hexdigest()[:12].upper()
        cim_numero = f"GOB-{cim_numero[:4]}-{cim_numero[4:]}"
        
        # 7. Gerar nome de usuário (login) - GARANTIR QUE NÃO SEJA NULL
        nome_original = candidato['nome']
        nome_usuario = nome_original.lower()
        nome_usuario = re.sub(r'[^a-z0-9]', '.', nome_usuario)
        nome_usuario = re.sub(r'\.+', '.', nome_usuario)
        nome_usuario = nome_usuario.strip('.')
        nome_usuario = nome_usuario[:30]
        
        # Se ficar vazio, usar um padrão
        if not nome_usuario or len(nome_usuario) < 3:
            nome_usuario = f"obreiro_{candidato['id']}"
        
        # Verificar se o nome de usuário já existe e adicionar sufixo se necessário
        cursor.execute("SELECT id FROM usuarios WHERE usuario = %s", (nome_usuario,))
        if cursor.fetchone():
            nome_usuario = f"{nome_usuario}_{secrets.token_hex(3)}"
        
        # 8. Gerar senha temporária
        senha_temporaria = secrets.token_urlsafe(8)
        senha_hash = generate_password_hash(senha_temporaria)
        
        # 9. Processar data de iniciação
        data_iniciacao_date = None
        if data_iniciacao:
            data_iniciacao_date = datetime.strptime(data_iniciacao, '%Y-%m-%d').date()
        else:
            data_iniciacao_date = datetime.now().date()
        
        # 10. Verificar se o candidato já tem usuário (pelo CPF)
        cursor.execute("SELECT id FROM usuarios WHERE cpf = %s", (candidato['cpf'],))
        usuario_existente = cursor.fetchone()
        
        if usuario_existente:
            # Usuário já existe, apenas atualizar
            obreiro_id = usuario_existente['id']
            cursor.execute("""
                UPDATE usuarios 
                SET tipo = 'obreiro',
                    cim_numero = %s,
                    data_iniciacao = %s,
                    grau_atual = 1,
                    status_membro = 'ativo',
                    ativo = 1,
                    loja_nome = %s,
                    loja_numero = %s,
                    loja_orient = %s,
                    nome_completo = %s
                WHERE id = %s
            """, (cim_numero, data_iniciacao_date, loja_nome, loja_numero, loja_orient, candidato['nome'], obreiro_id))
        else:
            # Criar novo usuário (obreiro) - COM TODOS OS CAMPOS OBRIGATÓRIOS
            cursor.execute("""
                INSERT INTO usuarios (
                    usuario, senha_hash, tipo, data_cadastro,
                    nome_completo, cim_numero, grau_atual, data_iniciacao,
                    telefone, email, loja_nome, loja_numero, loja_orient,
                    cpf, status_membro, ativo, nome_maconico, endereco
                )
                VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s)
                RETURNING id
            """, (
                nome_usuario,
                senha_hash,
                'obreiro',
                candidato['nome'],
                cim_numero,
                1,  # Aprendiz
                data_iniciacao_date,
                candidato.get('telefone') or candidato.get('celular'),
                candidato.get('email'),
                loja_nome,
                loja_numero,
                loja_orient,
                candidato['cpf'],
                'ativo',
                candidato['nome'],  # nome_maconico (pode ser igual ao nome)
                candidato.get('endereco_residencial') or candidato.get('endereco')
            ))
            obreiro_id = cursor.fetchone()['id']
        
        # 11. Registrar Placet
        cursor.execute("""
            INSERT INTO placet_iniciacao (
                candidato_id, numero_placet, data_emissao, 
                data_iniciacao, loja_id, status, observacoes, emitido_por
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            candidato_id, numero_placet, data_emissao,
            data_iniciacao if data_iniciacao else None,
            loja_id, 'emitido', observacoes, session['user_id']
        ))
        placet_id = cursor.fetchone()['id']
        
        # 12. Registrar fluxo de iniciação
        cursor.execute("""
            INSERT INTO fluxo_iniciacao (
                candidato_id, etapa, status, data_entrada, usuario_id
            )
            VALUES (%s, %s, %s, %s, %s)
        """, (candidato_id, 'iniciado', 'concluido', datetime.now(), session['user_id']))
        
        # 13. Atualizar candidato (SEM data_iniciacao e status_processo - apenas colunas que existem)
        cursor.execute("""
            UPDATE candidatos 
            SET obreiro_id = %s, 
                data_transformacao = NOW(),
                numero_placet = %s,
                placet_emitido = TRUE
            WHERE id = %s
        """, (obreiro_id, numero_placet, candidato_id))
        
        conn.commit()
        
        # 14. Registrar log
        registrar_log(
            acao=f"emitir_placet_{placet_id}",
            entidade="placet_iniciacao",
            entidade_id=placet_id,
            dados_anteriores=None,
            dados_novos={'candidato': candidato['nome'], 'placet': numero_placet, 'obreiro_id': obreiro_id}
        )
        
        flash(f"✅ Placet {numero_placet} emitido com sucesso!", "success")
        flash(f"📝 Usuário criado: {nome_usuario} | Senha temporária: {senha_temporaria}", "info")
        
        # 15. Enviar e-mail de confirmação com senha
        if candidato.get('email'):
            try:
                enviar_email_iniciacao_com_senha(
                    candidato['email'], 
                    candidato['nome'], 
                    numero_placet, 
                    cim_numero,
                    nome_usuario,
                    senha_temporaria
                )
            except Exception as e:
                print(f"Erro ao enviar e-mail: {e}")
        
        return_connection(conn)
        return redirect("/candidatos")
        
    except Exception as e:
        print(f"Erro ao emitir placet: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            return_connection(conn)
        flash(f"Erro ao emitir placet: {str(e)}", "danger")
        return redirect("/candidatos")

@app.route("/candidato/<int:candidato_id>/processo")
@login_required
@permissao_required('candidato.view')
def visualizar_processo_candidato(candidato_id):
    """Visualizar o processo completo de um candidato"""
    try:
        cursor, conn = get_db()
        
        # 1. Buscar candidato
        cursor.execute("""
            SELECT c.*, u.id as obreiro_id, u.nome_completo as obreiro_nome
            FROM candidatos c
            LEFT JOIN usuarios u ON c.obreiro_id = u.id
            WHERE c.id = %s
        """, (candidato_id,))
        candidato = cursor.fetchone()
        
        if not candidato:
            flash("Candidato não encontrado!", "danger")
            return redirect("/candidatos")
        
        # 2. Buscar pareceres (tratar erro)
        pareceres = []
        try:
            cursor.execute("""
                SELECT pc.*, u.nome_completo as sindicante_nome
                FROM pareceres_conclusivos pc
                JOIN usuarios u ON pc.sindicante = u.usuario
                WHERE pc.candidato_id = %s
                ORDER BY pc.data_envio DESC
            """, (candidato_id,))
            pareceres = cursor.fetchall()
            print(f"✅ Encontrados {len(pareceres)} pareceres")
        except Exception as e:
            print(f"⚠️ Erro ao buscar pareceres: {e}")
        
        # 3. Buscar sindicantes designados
        sindicantes_designados = []
        try:
            cursor.execute("""
                SELECT sc.*, u.nome_completo as sindicante_nome
                FROM sindicantes_candidato sc
                JOIN usuarios u ON sc.sindicante_id = u.id
                WHERE sc.candidato_id = %s
            """, (candidato_id,))
            sindicantes_designados = cursor.fetchall()
            print(f"✅ Encontrados {len(sindicantes_designados)} sindicantes designados")
        except Exception as e:
            print(f"⚠️ Erro ao buscar sindicantes: {e}")
        
        # 4. Buscar votação
        votacao = None
        try:
            cursor.execute("""
                SELECT * FROM votacao_candidato 
                WHERE candidato_id = %s
                ORDER BY data_votacao DESC
                LIMIT 1
            """, (candidato_id,))
            votacao = cursor.fetchone()
            print(f"✅ Votação encontrada: {votacao is not None}")
        except Exception as e:
            print(f"⚠️ Erro ao buscar votação: {e}")
        
        # 5. Buscar leituras
        leituras = []
        try:
            cursor.execute("""
                SELECT * FROM leituras_loja 
                WHERE candidato_id = %s
                ORDER BY data_leitura ASC
            """, (candidato_id,))
            leituras = cursor.fetchall()
            print(f"✅ Encontradas {len(leituras)} leituras")
        except Exception as e:
            print(f"⚠️ Erro ao buscar leituras: {e}")
        
        # 6. Buscar fluxo
        fluxo = []
        try:
            cursor.execute("""
                SELECT * FROM fluxo_iniciacao 
                WHERE candidato_id = %s
                ORDER BY data_entrada ASC
            """, (candidato_id,))
            fluxo = cursor.fetchall()
            print(f"✅ Encontradas {len(fluxo)} etapas no fluxo")
        except Exception as e:
            print(f"⚠️ Erro ao buscar fluxo: {e}")
        
        # 7. Buscar documentos
        documentos = []
        try:
            cursor.execute("""
                SELECT d.*, t.nome as tipo_nome
                FROM documentos_candidato d
                LEFT JOIN tipos_documentos_candidato t ON d.tipo_documento_id = t.id
                WHERE d.candidato_id = %s
                ORDER BY d.data_upload DESC
            """, (candidato_id,))
            documentos = cursor.fetchall()
            print(f"✅ Encontrados {len(documentos)} documentos")
        except Exception as e:
            print(f"⚠️ Erro ao buscar documentos: {e}")
        
        return_connection(conn)
        
        return render_template("candidato_processo.html", 
                              candidato=candidato,
                              pareceres=pareceres,
                              sindicantes_designados=sindicantes_designados,
                              votacao=votacao,
                              leituras=leituras,
                              fluxo=fluxo,
                              documentos=documentos)
        
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            return_connection(conn)
        flash(f"Erro ao carregar processo: {str(e)}", "danger")
        return redirect("/candidatos")

#atualizar o banco

@app.route("/admin/verificar-tabelas-sistema")
@login_required
def verificar_tabelas_sistema():
    """Verifica apenas as tabelas do sistema de candidatos/obreiros"""
    if session.get('tipo') != 'admin':
        return "Acesso negado", 403
    
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Verificação de Tabelas do Sistema</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .existe { color: green; font-weight: bold; }
            .nao-existe { color: red; font-weight: bold; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
        </style>
    </head>
    <body>
        <h1>Verificação de Tabelas do Sistema</h1>
    """
    
    try:
        cursor, conn = get_db()
        
        # Tabelas que realmente precisamos
        tabelas = [
            'candidatos',
            'usuarios', 
            'lojas',
            'pareceres_conclusivos',
            'sindicantes_candidato',
            'placet_iniciacao',
            'fluxo_iniciacao',
            'votacao_candidato',
            'leituras_loja',
            'historico_graus',
            'password_reset_tokens',
            'email_logs'
        ]
        
        html += '<table>'
        html += '<tr><th>Tabela</th><th>Status</th><th>Registros</th></tr>'
        
        for tabela in tabelas:
            try:
                # Verificar se a tabela existe
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (tabela,))
                existe = cursor.fetchone()[0]
                
                # Contar registros se existir
                registros = 0
                if existe:
                    try:
                        cursor.execute(f"SELECT COUNT(*) as total FROM {tabela}")
                        result = cursor.fetchone()
                        registros = result[0] if result else 0
                    except Exception as e:
                        registros = f"Erro: {str(e)[:30]}"
                
                status = "✅ Existe" if existe else "❌ NÃO EXISTE"
                status_class = "existe" if existe else "nao-existe"
                
                html += f"""
                    <tr>
                        <td><strong>{tabela}</strong></td>
                        <td class="{status_class}">{status}</td>
                        <td>{registros}</td>
                    </tr>
                """
            except Exception as e:
                html += f"""
                    <tr>
                        <td><strong>{tabela}</strong></td>
                        <td class="nao-existe">❌ Erro ao verificar</td>
                        <td>{str(e)[:50]}</td>
                    </tr>
                """
        
        html += '</table>'
        
        # Verificar tabelas faltantes
        html += """
        <div style="margin-top: 30px;">
            <h3>Ações:</h3>
            <ul>
                <li><a href="/admin/criar-tabelas-sistema">Criar tabelas faltantes</a></li>
                <li><a href="/dashboard">Voltar ao Dashboard</a></li>
            </ul>
        </div>
        """
        
        return_connection(conn)
        
    except Exception as e:
        html += f'<p style="color:red">❌ Erro na conexão: {str(e)}</p>'
    
    html += '</body></html>'
    return html
        
@app.route("/admin/criar-tabelas-sistema")
@login_required
def criar_tabelas_sistema():
    """Cria apenas as tabelas necessárias para o sistema"""
    if session.get('tipo') != 'admin':
        return "Acesso negado", 403
    
    try:
        cursor, conn = get_db()
        
        comandos = []
        
        # 1. Verificar se a tabela sindicantes_candidato existe
        comandos.append("""
            CREATE TABLE IF NOT EXISTS sindicantes_candidato (
                id SERIAL PRIMARY KEY,
                candidato_id INTEGER NOT NULL REFERENCES candidatos(id) ON DELETE CASCADE,
                sindicante_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
                data_designacao DATE NOT NULL,
                data_conclusao DATE,
                recomendacao VARCHAR(20),
                status VARCHAR(20) DEFAULT 'designado',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 2. Verificar se a tabela placet_iniciacao existe
        comandos.append("""
            CREATE TABLE IF NOT EXISTS placet_iniciacao (
                id SERIAL PRIMARY KEY,
                candidato_id INTEGER NOT NULL REFERENCES candidatos(id) ON DELETE CASCADE,
                numero_placet VARCHAR(50) UNIQUE NOT NULL,
                data_emissao DATE NOT NULL,
                data_iniciacao DATE,
                loja_id INTEGER REFERENCES lojas(id),
                status VARCHAR(20) DEFAULT 'emitido',
                observacoes TEXT,
                emitido_por INTEGER REFERENCES usuarios(id),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 3. Verificar se a tabela fluxo_iniciacao existe
        comandos.append("""
            CREATE TABLE IF NOT EXISTS fluxo_iniciacao (
                id SERIAL PRIMARY KEY,
                candidato_id INTEGER NOT NULL REFERENCES candidatos(id) ON DELETE CASCADE,
                etapa VARCHAR(50) NOT NULL,
                status VARCHAR(20) DEFAULT 'pendente',
                data_entrada TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_saida TIMESTAMP,
                observacoes TEXT,
                usuario_id INTEGER REFERENCES usuarios(id),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 4. Verificar se a tabela votacao_candidato existe
        comandos.append("""
            CREATE TABLE IF NOT EXISTS votacao_candidato (
                id SERIAL PRIMARY KEY,
                candidato_id INTEGER NOT NULL REFERENCES candidatos(id) ON DELETE CASCADE,
                data_votacao DATE NOT NULL,
                votos_favoraveis INTEGER DEFAULT 0,
                votos_contrarios INTEGER DEFAULT 0,
                votos_brancos INTEGER DEFAULT 0,
                resultado VARCHAR(20),
                observacoes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 5. Verificar se a tabela leituras_loja existe
        comandos.append("""
            CREATE TABLE IF NOT EXISTS leituras_loja (
                id SERIAL PRIMARY KEY,
                candidato_id INTEGER NOT NULL REFERENCES candidatos(id) ON DELETE CASCADE,
                tipo_documento VARCHAR(20) NOT NULL,
                numero_leitura INTEGER NOT NULL,
                data_leitura DATE NOT NULL,
                observacoes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 6. Verificar se a tabela historico_graus existe
        comandos.append("""
            CREATE TABLE IF NOT EXISTS historico_graus (
                id SERIAL PRIMARY KEY,
                obreiro_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
                grau INTEGER NOT NULL,
                data DATE NOT NULL,
                motivo TEXT,
                autorizado_por INTEGER REFERENCES usuarios(id),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 7. Verificar se a tabela password_reset_tokens existe
        comandos.append("""
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
                token VARCHAR(255) NOT NULL UNIQUE,
                expira_em TIMESTAMP NOT NULL,
                usado BOOLEAN DEFAULT FALSE,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 8. Verificar se a tabela email_logs existe
        comandos.append("""
            CREATE TABLE IF NOT EXISTS email_logs (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER REFERENCES usuarios(id) ON DELETE SET NULL,
                tipo VARCHAR(50),
                destinatario VARCHAR(255),
                status VARCHAR(50),
                mensagem_id VARCHAR(255),
                erro TEXT,
                data_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Executar comandos
        for cmd in comandos:
            cursor.execute(cmd)
            conn.commit()
        
        # Criar índices
        indices = [
            "CREATE INDEX IF NOT EXISTS idx_placet_candidato ON placet_iniciacao(candidato_id)",
            "CREATE INDEX IF NOT EXISTS idx_sindicantes_candidato ON sindicantes_candidato(candidato_id)",
            "CREATE INDEX IF NOT EXISTS idx_fluxo_candidato ON fluxo_iniciacao(candidato_id)",
            "CREATE INDEX IF NOT EXISTS idx_votacao_candidato ON votacao_candidato(candidato_id)"
        ]
        
        for idx in indices:
            try:
                cursor.execute(idx)
                conn.commit()
            except:
                pass
        
        return_connection(conn)
        
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Tabelas Criadas</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .success { color: green; }
            </style>
        </head>
        <body>
            <h1>✅ Tabelas do sistema criadas com sucesso!</h1>
            <ul>
                <li class="success">✅ sindicantes_candidato</li>
                <li class="success">✅ placet_iniciacao</li>
                <li class="success">✅ fluxo_iniciacao</li>
                <li class="success">✅ votacao_candidato</li>
                <li class="success">✅ leituras_loja</li>
                <li class="success">✅ historico_graus</li>
                <li class="success">✅ password_reset_tokens</li>
                <li class="success">✅ email_logs</li>
            </ul>
            <p><a href="/admin/verificar-tabelas-sistema">Verificar novamente</a></p>
            <p><a href="/dashboard">Voltar ao Dashboard</a></p>
        </body>
        </html>
        """
        
    except Exception as e:
        return f"❌ Erro ao criar tabelas: {str(e)}"        

# =============================
# ROTAS DE CANDIDATOS E SINDICÂNCIA
# =============================

@app.route("/candidatos/historico")
@login_required
@permissao_required('candidato.view')
def historico_candidatos():
    """Lista de candidatos que já foram transformados em obreiros"""
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT 
                c.id,
                c.nome as candidato_nome,
                c.cpf,
                c.data_criacao,
                c.status,
                c.data_fechamento,
                c.numero_placet,
                c.data_transformacao,
                c.data_iniciacao as candidato_data_iniciacao,
                u.id as obreiro_id,
                u.nome_completo as obreiro_nome,
                u.usuario as obreiro_usuario,
                u.cim_numero as obreiro_cim,
                u.grau_atual as obreiro_grau,
                u.data_iniciacao as obreiro_data_iniciacao,
                u.loja_nome,
                u.loja_numero
            FROM candidatos c
            INNER JOIN usuarios u ON c.obreiro_id = u.id
            WHERE c.obreiro_id IS NOT NULL
            ORDER BY c.data_transformacao DESC
        """)
        historico = cursor.fetchall()
        
        return_connection(conn)
        
        return render_template("candidatos_historico.html", historico=historico)
        
    except Exception as e:
        print(f"Erro ao carregar histórico: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            return_connection(conn)
        flash("Erro ao carregar histórico", "danger")
        return redirect("/candidatos")
                          
@app.route("/designar-sindicantes/<int:candidato_id>", methods=["POST"])
@login_required
def designar_sindicantes_candidato(candidato_id):
    """Designar sindicantes para um candidato específico"""
    if session.get('tipo') != 'admin':
        flash("Acesso negado!", "danger")
        return redirect("/candidatos")
    
    try:
        cursor, conn = get_db()
        
        # Remover designações anteriores
        cursor.execute("DELETE FROM sindicantes_candidato WHERE candidato_id = %s", (candidato_id,))
        
        # Adicionar novas designações
        sindicantes_ids = request.form.getlist("sindicantes_ids")
        
        for sindicante_id in sindicantes_ids:
            cursor.execute("""
                INSERT INTO sindicantes_candidato (candidato_id, sindicante_id, data_designacao, status)
                VALUES (%s, %s, NOW(), 'designado')
            """, (candidato_id, sindicante_id))
        
        conn.commit()
        
        flash(f"Designações salvas com sucesso! {len(sindicantes_ids)} sindicante(s) designado(s).", "success")
        
    except Exception as e:
        print(f"Erro: {e}")
        if 'conn' in locals():
            return_connection(conn)
        flash(f"Erro ao designar sindicantes: {str(e)}", "danger")
    
    return redirect("/candidatos")                          

@app.route("/admin/designar-sindicantes", methods=["GET", "POST"])
@login_required
def admin_designar_sindicantes():
    """Página para designar sindicantes aos candidatos"""
    if session.get('tipo') != 'admin':
        flash("Acesso negado!", "danger")
        return redirect("/dashboard")
    
    try:
        cursor, conn = get_db()
        
        if request.method == "POST":
            # Buscar todos os candidatos
            cursor.execute("SELECT id, nome FROM candidatos WHERE fechado = 0")
            candidatos = cursor.fetchall()
            
            # Limpar designações existentes
            cursor.execute("DELETE FROM sindicantes_candidato")
            
            # Para cada candidato, verificar quais sindicantes foram marcados
            for candidato in candidatos:
                sindicantes_ids = request.form.getlist(f'sindicantes_{candidato["id"]}')
                
                for sindicante_id in sindicantes_ids:
                    cursor.execute("""
                        INSERT INTO sindicantes_candidato (candidato_id, sindicante_id, data_designacao, status)
                        VALUES (%s, %s, NOW(), 'designado')
                    """, (candidato['id'], sindicante_id))
            
            conn.commit()
            flash("Designações salvas com sucesso!", "success")
            return redirect("/candidatos")
        
        # GET - Buscar dados para o formulário
        cursor.execute("SELECT id, nome FROM candidatos WHERE fechado = 0 ORDER BY data_criacao DESC")
        candidatos = cursor.fetchall()
        
        cursor.execute("SELECT id, usuario, nome_completo FROM usuarios WHERE tipo = 'sindicante' AND ativo = 1 ORDER BY nome_completo")
        sindicantes = cursor.fetchall()
        
        # Buscar designações existentes
        cursor.execute("SELECT candidato_id, sindicante_id FROM sindicantes_candidato")
        designacoes = cursor.fetchall()
        
        # Criar dicionário para fácil consulta
        designados = {}
        for d in designacoes:
            if d['candidato_id'] not in designados:
                designados[d['candidato_id']] = []
            designados[d['candidato_id']].append(d['sindicante_id'])
        
        return_connection(conn)
        
        return render_template("admin_designar_sindicantes.html", 
                               candidatos=candidatos,
                               sindicantes=sindicantes,
                               designados=designados)
        
    except Exception as e:
        print(f"Erro: {e}")
        if 'conn' in locals():
            return_connection(conn)
        flash(f"Erro: {str(e)}", "danger")
        return redirect("/candidatos")        
    
    
    
    # ============================================
    # BUSCAR STATUS DOS DOCUMENTOS POR CANDIDATO
    # ============================================
    
    documentos_status = {}
    
    try:
        cursor.execute("SELECT COUNT(*) as total FROM tipos_documentos_candidato WHERE obrigatorio = 1")
        result = cursor.fetchone()
        total_tipos_obrigatorios = result['total'] if result else 0
        
        cursor.execute("""
            SELECT 
                d.candidato_id,
                COUNT(d.id) as enviados,
                SUM(CASE WHEN d.status = 'aprovado' THEN 1 ELSE 0 END) as aprovados,
                SUM(CASE WHEN d.status = 'pendente' THEN 1 ELSE 0 END) as pendentes,
                SUM(CASE WHEN d.status = 'rejeitado' THEN 1 ELSE 0 END) as rejeitados
            FROM documentos_candidato d
            GROUP BY d.candidato_id
        """)
        docs_enviados = cursor.fetchall()
        
        for doc in docs_enviados:
            documentos_status[doc['candidato_id']] = {
                'total': total_tipos_obrigatorios,
                'enviados': doc['enviados'],
                'faltantes': max(0, total_tipos_obrigatorios - doc['enviados']),
                'aprovados': doc['aprovados'] or 0,
                'pendentes': doc['pendentes'] or 0,
                'rejeitados': doc['rejeitados'] or 0
            }
        
        for candidato in candidatos:
            if candidato['id'] not in documentos_status:
                documentos_status[candidato['id']] = {
                    'total': total_tipos_obrigatorios,
                    'enviados': 0,
                    'faltantes': total_tipos_obrigatorios,
                    'aprovados': 0,
                    'pendentes': 0,
                    'rejeitados': 0
                }
    except Exception as e:
        print(f"Erro ao buscar status dos documentos: {str(e)}")
        conn.rollback()
        for candidato in candidatos:
            documentos_status[candidato['id']] = {
                'total': 0,
                'enviados': 0,
                'faltantes': 0,
                'aprovados': 0,
                'pendentes': 0,
                'rejeitados': 0
            }
    
    # Buscar sindicantes
    try:
        cursor.execute("""
            SELECT id, usuario, nome_completo, cim_numero, 
                   loja_nome, loja_numero, loja_orient, ativo,
                   telefone
            FROM usuarios 
            WHERE tipo = 'sindicante' AND ativo = 1 AND grau_atual >= 3
            ORDER BY nome_completo
        """)
        sindicantes = cursor.fetchall()
    except Exception as e:
        print(f"Erro ao buscar sindicantes: {str(e)}")
        conn.rollback()
        sindicantes = []
    
    # Buscar lojas para o modal do Placet
    try:
        cursor.execute("SELECT id, nome, numero FROM lojas WHERE ativo = 1 ORDER BY nome")
        lojas = cursor.fetchall()
    except Exception as e:
        print(f"Erro ao buscar lojas: {str(e)}")
        lojas = []
    
    return_connection(conn)
    
    return render_template("candidatos.html", 
                          candidatos=candidatos, 
                          sindicantes=sindicantes, 
                          documentos_status=documentos_status,
                          lojas=lojas,
                          tipo=session.get("tipo", "admin"))

@app.route("/candidatos/excluir/<int:id>", methods=["POST"])
def excluir_candidato(id):
    cursor, conn = get_db()

    cursor.execute("DELETE FROM candidatos WHERE id=%s", (id,))
    conn.commit()

    return_connection(conn)

    return redirect("/candidatos")

@app.route("/candidato/formulario/<int:candidato_id>", methods=["GET", "POST"])
@login_required
def formulario_candidato(candidato_id):
    cursor, conn = get_db()
    
    # Buscar dados do candidato
    cursor.execute("SELECT * FROM candidatos WHERE id = %s", (candidato_id,))
    candidato = cursor.fetchone()
    
    if not candidato:
        flash("Candidato não encontrado", "danger")
        return_connection(conn)
        return redirect("/candidatos")
    
    # ✅ Buscar dados do usuário logado (loja do obreiro que está preenchendo)
    cursor.execute("""
        SELECT u.id, u.nome_completo, u.loja_nome, u.loja_numero, u.loja_orient,
               l.numero as loja_numero_completo, l.oriente as loja_oriente
        FROM usuarios u
        LEFT JOIN lojas l ON u.loja_nome = l.nome
        WHERE u.id = %s
    """, (session['user_id'],))
    usuario = cursor.fetchone()
    
    # Buscar filhos do candidato
    cursor.execute("SELECT * FROM filhos_candidato WHERE candidato_id = %s ORDER BY data_nascimento", (candidato_id,))
    filhos = cursor.fetchall()
    
    if request.method == "POST":
        dados = {
            'loja_nome': request.form.get('loja_nome') or (usuario['loja_nome'] if usuario else None),
            'loja_numero': request.form.get('loja_numero') or (usuario['loja_numero'] or usuario['loja_numero_completo'] if usuario else None),
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
        
        # Atualizar filhos
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
    return render_template("candidatos/formulario.html", 
                          candidato=candidato, 
                          filhos=filhos,
                          usuario=usuario)
                          
@app.route("/sindicancia/<int:candidato_id>")
@login_required
def visualizar_sindicancia(candidato_id):
    """Visualizar sindicância do candidato"""
    try:
        cursor, conn = get_db()
        
        # Buscar candidato
        cursor.execute("SELECT * FROM candidatos WHERE id = %s", (candidato_id,))
        candidato = cursor.fetchone()
        
        if not candidato:
            flash("Candidato não encontrado", "danger")
            return redirect("/candidatos")
        
        # Verificar se usuário é sindicante designado para este candidato
        user_is_sindicante = False
        if session.get('tipo') == 'sindicante':
            cursor.execute("""
                SELECT id FROM sindicantes_candidato 
                WHERE candidato_id = %s AND sindicante_id = %s
            """, (candidato_id, session.get('user_id')))
            user_is_sindicante = cursor.fetchone() is not None
        
        # Buscar todos os pareceres conclusivos (apenas para admin)
        pareceres = []
        if session.get('tipo') == 'admin':
            cursor.execute("""
                SELECT * FROM pareceres_conclusivos 
                WHERE candidato_id = %s 
                ORDER BY data_envio DESC
            """, (candidato_id,))
            pareceres = cursor.fetchall()
        
        # Calcular votos
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN conclusao = 'APROVADO' THEN 1 END) as positivos,
                COUNT(CASE WHEN conclusao = 'REPROVADO' THEN 1 END) as negativos
            FROM pareceres_conclusivos 
            WHERE candidato_id = %s
        """, (candidato_id,))
        votos = cursor.fetchone()
        
        # Total de sindicantes ativos
        cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo = 'sindicante' AND ativo = 1")
        total_sindicantes = cursor.fetchone()['total']
        
        votos_recebidos = votos['total'] if votos else 0
        votos_positivos = votos['positivos'] if votos else 0
        votos_negativos = votos['negativos'] if votos else 0
        
        percentual_votos = (votos_recebidos / total_sindicantes * 100) if total_sindicantes > 0 else 0
        
        return_connection(conn)
        
        return render_template("sindicancia.html", 
                               candidato=candidato,
                               pareceres=pareceres,
                               user_is_sindicante=user_is_sindicante,
                               total_sindicantes=total_sindicantes,
                               votos_recebidos=votos_recebidos,
                               votos_positivos=votos_positivos,
                               votos_negativos=votos_negativos,
                               percentual_votos=percentual_votos)
        
    except Exception as e:
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            return_connection(conn)
        flash("Erro ao carregar sindicância", "danger")
        return redirect("/candidatos")
        
@app.route("/api/sindicantes-disponiveis/<int:candidato_id>")
@login_required
def api_sindicantes_disponiveis(candidato_id):
    """API para buscar sindicantes disponíveis e designações existentes"""
    if session.get('tipo') != 'admin':
        return jsonify({'error': 'Acesso negado'}), 403
    
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT id, usuario, nome_completo, cim_numero 
            FROM usuarios 
            WHERE tipo = 'sindicante' AND ativo = 1
            ORDER BY nome_completo
        """)
        sindicantes = cursor.fetchall()
        
        cursor.execute("""
            SELECT sindicante_id 
            FROM sindicantes_candidato 
            WHERE candidato_id = %s
        """, (candidato_id,))
        designados = [row['sindicante_id'] for row in cursor.fetchall()]
        
        return_connection(conn)
        
        return jsonify({
            'sindicantes': [dict(s) for s in sindicantes],
            'designados': designados
        })
        
    except Exception as e:
        print(f"Erro: {e}")
        if 'conn' in locals():
            return_connection(conn)
        return jsonify({'error': str(e)}), 500        

        

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
    registrar_log("fechar_sindicancia_manual", "sindicancia", id, dados_anteriores=dados_antigos, dados_novos={"status": status})
    return_connection(conn)
    flash(f"Sindicância fechada! Resultado: {status}", "success")
    return redirect("/candidatos")

@app.route("/minhas_sindicancias")
@sindicante_required
def minhas_sindicancias():
    cursor, conn = get_db()
    
    # Rollback para limpar transações pendentes
    try:
        conn.rollback()
    except:
        pass
    
    usuario_nome = session.get("usuario")  # Nome do usuário (texto)
    
    cursor.execute("""
        SELECT c.*, 
               CASE WHEN s.parecer IS NOT NULL THEN 1 ELSE 0 END as parecer_enviado,
               s.parecer,
               s.data_envio
        FROM candidatos c
        LEFT JOIN sindicancias s ON c.id = s.candidato_id AND s.sindicante = %s
        ORDER BY c.fechado ASC, c.data_criacao DESC
    """, (usuario_nome,))
    candidatos = cursor.fetchall()
    
    return_connection(conn)
    return render_template("minhas_sindicancias.html", candidatos=candidatos)
    
@app.route("/parecer_conclusivo/<int:candidato_id>/excluir", methods=["POST"])
@login_required
def excluir_parecer_conclusivo(candidato_id):
    """Sindicante exclui seu parecer conclusivo"""
    if session.get("tipo") != 'sindicante':
        flash("❌ Apenas sindicantes podem excluir seus próprios pareceres!", "danger")
        return redirect("/dashboard")
    
    # Limpar flashes anteriores
    session.pop('_flashes', None)
    
    cursor, conn = get_db()
    
    try:
        conn.rollback()
        
        usuario_nome = session.get("usuario")
        
        # Verificar se o parecer existe
        cursor.execute("""
            SELECT id FROM pareceres_conclusivos 
            WHERE candidato_id = %s AND sindicante = %s
        """, (candidato_id, usuario_nome))
        
        resultado = cursor.fetchone()
        
        if not resultado:
            flash("❌ Você não tem um parecer conclusivo para este candidato!", "warning")
            return redirect(f"/sindicancia/{candidato_id}")
        
        # Excluir o parecer conclusivo
        cursor.execute("""
            DELETE FROM pareceres_conclusivos 
            WHERE candidato_id = %s AND sindicante = %s
        """, (candidato_id, usuario_nome))
        
        conn.commit()
        
        # Reabrir a sindicância se estava fechada
        cursor.execute("""
            UPDATE candidatos 
            SET fechado = 0, data_fechamento = NULL
            WHERE id = %s
        """, (candidato_id,))
        conn.commit()
        
        flash("🗑️ Seu parecer conclusivo foi excluído com sucesso!", "success")
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao excluir parecer conclusivo: {str(e)}")
        flash(f"❌ Erro ao excluir parecer conclusivo: {str(e)}", "danger")
    
    finally:
        return_connection(conn)
    
    return redirect(f"/sindicancia/{candidato_id}")

@app.route("/sindicancia/<int:candidato_id>/excluir_parecer", methods=["POST"])
@login_required
def excluir_parecer_simples(candidato_id):
    """Sindicante exclui seu voto simples (positivo/negativo)"""
    if session.get("tipo") != 'sindicante':
        flash("❌ Apenas sindicantes podem excluir seus próprios votos!", "danger")
        return redirect("/dashboard")
    
    cursor, conn = get_db()
    
    try:
        conn.rollback()
        
        usuario_nome = session.get("usuario")
        
        # Verificar se o voto existe
        cursor.execute("""
            SELECT id FROM sindicancias 
            WHERE candidato_id = %s AND sindicante = %s
        """, (candidato_id, usuario_nome))
        
        resultado = cursor.fetchone()
        
        if not resultado:
            flash("❌ Você não tem um voto registrado para este candidato!", "warning")
            return redirect(f"/sindicancia/{candidato_id}")
        
        # Excluir o voto
        cursor.execute("""
            DELETE FROM sindicancias 
            WHERE candidato_id = %s AND sindicante = %s
        """, (candidato_id, usuario_nome))
        
        conn.commit()
        
        flash("🗑️ Seu voto foi excluído com sucesso! Você pode votar novamente.", "success")
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao excluir voto: {str(e)}")
        flash(f"❌ Erro ao excluir voto: {str(e)}", "danger")
    
    finally:
        return_connection(conn)
    
    return redirect(f"/sindicancia/{candidato_id}")    

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
    return render_template("parecer_conclusivo.html", candidato=candidato, parecer_existente=parecer_existente,
                          fontes_existentes=fontes_existentes, hoje=hoje, loja_nome=session.get("loja_nome", ""),
                          loja_numero=session.get("loja_numero", ""), loja_orient=session.get("loja_orient", ""),
                          nome_completo=session.get("nome_completo", ""), cim_numero=session.get("cim_numero", ""))

@app.route("/salvar_parecer_conclusivo/<int:id>", methods=["POST"])
@login_required
def salvar_parecer_conclusivo(id):
    if session["tipo"] != "sindicante":
        flash("Acesso restrito a sindicantes", "danger")
        return redirect("/dashboard")
    
    # Limpar flashes anteriores
    session.pop('_flashes', None)
    
    cursor, conn = get_db()
    
    try:
        conn.rollback()
        
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
        
        usuario_nome = session.get("usuario")
        
        # Verificar se já existe parecer conclusivo
        cursor.execute("""
            SELECT id FROM pareceres_conclusivos 
            WHERE candidato_id = %s AND sindicante = %s
        """, (id, usuario_nome))
        
        existe = cursor.fetchone()
        
        if existe:
            # UPDATE
            cursor.execute("""
                UPDATE pareceres_conclusivos 
                SET parecer_texto = %s, conclusao = %s, observacoes = %s,
                    cim_numero = %s, data_parecer = %s, data_envio = %s,
                    fontes = %s, loja_nome = %s, loja_numero = %s, loja_orient = %s
                WHERE candidato_id = %s AND sindicante = %s
            """, (parecer_texto, conclusao, observacoes, cim_numero, data_parecer, agora,
                  fontes_json, loja_nome, loja_numero, loja_orient, id, usuario_nome))
        else:
            # INSERT
            cursor.execute("""
                INSERT INTO pareceres_conclusivos 
                (candidato_id, sindicante, parecer_texto, conclusao, observacoes, 
                 cim_numero, data_parecer, data_envio, fontes, loja_nome, loja_numero, loja_orient)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (id, usuario_nome, parecer_texto, conclusao, observacoes,
                  cim_numero, data_parecer, agora, fontes_json,
                  loja_nome, loja_numero, loja_orient))
        
        conn.commit()
        
        registrar_log("salvar_parecer_conclusivo", "parecer_conclusivo", id, dados_novos={"conclusao": conclusao})
        
        # ============================================
        # VERIFICAR SE TODOS OS SINDICANTES JÁ VOTARAM
        # ============================================
        
        # Total de sindicantes ativos
        cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo = 'sindicante' AND ativo = 1")
        total_sindicantes = cursor.fetchone()["total"]
        
        # Total de pareceres conclusivos já emitidos
        cursor.execute("SELECT COUNT(*) as votos FROM pareceres_conclusivos WHERE candidato_id = %s", (id,))
        votos = cursor.fetchone()["votos"]
        
        # Se todos já votaram, encerrar a sindicância
        if votos >= total_sindicantes and total_sindicantes > 0:
            # Calcular resultado
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN conclusao = 'APROVADO' THEN 1 END) as positivos,
                    COUNT(CASE WHEN conclusao = 'REPROVADO' THEN 1 END) as negativos
                FROM pareceres_conclusivos 
                WHERE candidato_id = %s
            """, (id,))
            res = cursor.fetchone()
            
            positivos = res["positivos"] if res else 0
            negativos = res["negativos"] if res else 0
            
            status = "Aprovado" if positivos > negativos else "Reprovado"
            agora_fechamento = datetime.now()
            
            cursor.execute("""
                UPDATE candidatos 
                SET status = %s, fechado = 1, data_fechamento = %s, resultado_final = %s
                WHERE id = %s
            """, (status, agora_fechamento, f"{positivos} votos positivos, {negativos} negativos", id))
            
            conn.commit()
            registrar_log("fechar_sindicancia", "sindicancia", id, dados_novos={"status": status})
            flash(f"🎉 Sindicância encerrada! Resultado: {status}", "success")
        else:
            flash("Parecer conclusivo salvo com sucesso!", "success")
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao salvar parecer: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f"Erro ao salvar parecer: {str(e)}", "danger")
    
    finally:
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
        doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=72)
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

@app.route("/sindicancia/<int:candidato_id>/parecer", methods=["POST"])
@login_required
def enviar_parecer(candidato_id):
    cursor, conn = get_db()
    
    try:
        conn.rollback()
        
        usuario_nome = session.get("usuario")
        parecer = request.form.get('parecer')
        
        if not parecer or parecer not in ['positivo', 'negativo']:
            flash("❌ Parecer inválido!", "danger")
            return redirect(f"/sindicancia/{candidato_id}")
        
        # Verificar se já votou
        cursor.execute("""
            SELECT id FROM sindicancias 
            WHERE candidato_id = %s AND sindicante = %s
        """, (candidato_id, usuario_nome))
        
        if cursor.fetchone():
            flash("⚠️ Você já votou neste candidato!", "warning")
            return redirect(f"/sindicancia/{candidato_id}")
        
        # Inserir o voto
        from datetime import datetime
        agora = datetime.now()
        
        cursor.execute("""
            INSERT INTO sindicancias (candidato_id, sindicante, parecer, data_envio)
            VALUES (%s, %s, %s, %s)
        """, (candidato_id, usuario_nome, parecer, agora))
        
        conn.commit()
        
        flash(f"✅ Parecer {parecer} registrado com sucesso!", "success")
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao enviar parecer: {str(e)}")
        flash(f"❌ Erro ao enviar parecer: {str(e)}", "danger")
    
    return redirect(f"/candidatos")  # ← MUDE PARA REDIRECIONAR PARA CANDIDATOS, NÃO PARA SINDICANCIA
        
# =============================
# ROTAS DE DOCUMENTOS DO CANDIDATO
# =============================
@app.route("/candidatos/<int:candidato_id>/documentos")
@login_required
def listar_documentos_candidato(candidato_id):
    """Lista documentos do candidato e tipos obrigatórios"""
    cursor, conn = get_db()
    
    # Verificar permissão (admin ou sindicante)
    if session.get('tipo') not in ['admin', 'sindicante']:
        flash("Permissão negada!", "danger")
        return redirect("/candidatos")
    
    # Buscar candidato
    cursor.execute("""
        SELECT id, nome, status, token_acesso, email 
        FROM candidatos 
        WHERE id = %s
    """, (candidato_id,))
    candidato = cursor.fetchone()
    
    if not candidato:
        flash("Candidato não encontrado!", "danger")
        return_connection(conn)
        return redirect("/candidatos")
    
    # Gerar token se não existir
    if not candidato['token_acesso']:
        import secrets
        token = secrets.token_hex(32)
        cursor.execute("UPDATE candidatos SET token_acesso = %s WHERE id = %s", (token, candidato_id))
        conn.commit()
        candidato['token_acesso'] = token
    
    # ============================================
    # CORREÇÃO: Garantir que TODOS os documentos obrigatórios existem
    # ============================================
    
    # Lista obrigatória completa
    documentos_obrigatorios_padrao = [
        ('Certidão Negativa Eleitoral', 'Certidão Negativa da Justiça Eleitoral', 1, 1),
        ('Certidão Negativa Militar', 'Certidão Negativa do Serviço Militar', 1, 2),
        ('Tribunal Regional Federal da 1ª Região', 'Certidão Negativa do TRF1', 1, 3),
        ('Certidões Negativas do TJDFT', 'Certidão Negativa do Tribunal de Justiça do DF', 1, 4),
        ('Certidão de Antecedentes Criminais Polícia Federal', 'Certidão de Antecedentes Criminais da PF', 1, 5),
        ('Certidão Negativa de Débitos - GDF', 'Certidão Negativa de Débitos do Governo do DF', 1, 6),
        ('RG', 'Registro Geral - Documento de Identidade', 1, 7),
        ('CPF', 'Cadastro de Pessoa Física', 1, 8),
        ('Foto 3x4', 'Foto 3x4 recente e fundo branco', 1, 9)
    ]
    
    # Verificar se os tipos obrigatórios existem, se não, criar
    for nome, descricao, obrigatorio, ordem in documentos_obrigatorios_padrao:
        cursor.execute("SELECT id FROM tipos_documentos_candidato WHERE nome = %s", (nome,))
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO tipos_documentos_candidato (nome, descricao, obrigatorio, ordem, ativo)
                VALUES (%s, %s, %s, %s, 1)
            """, (nome, descricao, obrigatorio, ordem))
            conn.commit()
    
    # Buscar TODOS os tipos de documentos ativos
    cursor.execute("""
        SELECT * FROM tipos_documentos_candidato 
        WHERE ativo = 1 
        ORDER BY obrigatorio DESC, ordem, nome
    """)
    tipos_documentos = cursor.fetchall()
    
    # Buscar documentos já enviados
    cursor.execute("""
        SELECT d.*, t.nome as tipo_nome, u.nome_completo as enviado_por_nome,
               a.nome_completo as aprovado_por_nome
        FROM documentos_candidato d
        JOIN tipos_documentos_candidato t ON d.tipo_documento_id = t.id
        LEFT JOIN usuarios u ON d.enviado_por = u.id
        LEFT JOIN usuarios a ON d.aprovado_por = a.id
        WHERE d.candidato_id = %s
        ORDER BY d.data_envio DESC
    """, (candidato_id,))
    documentos = cursor.fetchall()
    
    # Mapear documentos enviados por tipo
    documentos_map = {d['tipo_documento_id']: d for d in documentos}
    
    # Calcular progresso
    total_obrigatorios = sum(1 for t in tipos_documentos if t['obrigatorio'] == 1)
    total_enviados = 0
    for t in tipos_documentos:
        if t['obrigatorio'] == 1 and t['id'] in documentos_map:
            total_enviados += 1
    
    percentual = int((total_enviados / total_obrigatorios * 100)) if total_obrigatorios > 0 else 0
    
    # Documentos pendentes de aprovação
    documentos_pendentes = 0
    for t in tipos_documentos:
        if t['obrigatorio'] == 1 and t['id'] in documentos_map:
            doc = documentos_map[t['id']]
            if doc['status'] == 'pendente':
                documentos_pendentes += 1
    
    # Documentos opcionais
    documentos_opcionais_count = sum(1 for t in tipos_documentos if t['obrigatorio'] == 0)
    
    print(f"📊 Tipos encontrados: {len(tipos_documentos)}")
    print(f"📊 Obrigatórios: {total_obrigatorios}")
    print(f"📊 Opcionais: {documentos_opcionais_count}")
    
    return_connection(conn)
    
    return render_template("candidatos/documentos.html",
                          candidato=candidato,
                          tipos_documentos=tipos_documentos,
                          documentos_map=documentos_map,
                          total_obrigatorios=total_obrigatorios,
                          total_enviados=total_enviados,
                          percentual=percentual,
                          documentos_pendentes=documentos_pendentes,
                          documentos_opcionais_count=documentos_opcionais_count)
                          
@app.route("/candidatos/<int:candidato_id>/documentos/upload/<int:tipo_id>", methods=["POST"])
@login_required
def upload_documento_candidato(candidato_id, tipo_id):
    """Upload de documento do candidato"""
    cursor, conn = get_db()
    
    # Verificar permissão
    if session.get('tipo') not in ['admin', 'sindicante']:
        return jsonify({'success': False, 'error': 'Permissão negada!'}), 403
    
    if 'arquivo' not in request.files:
        return jsonify({'success': False, 'error': 'Nenhum arquivo selecionado!'}), 400
    
    arquivo = request.files['arquivo']
    
    if arquivo.filename == '':
        return jsonify({'success': False, 'error': 'Nenhum arquivo selecionado!'}), 400
    
    # Validar tipo de arquivo
    extensao = arquivo.filename.rsplit('.', 1)[1].lower() if '.' in arquivo.filename else ''
    allowed_extensions = ['pdf', 'jpg', 'jpeg', 'png']
    
    if extensao not in allowed_extensions:
        return jsonify({'success': False, 'error': f'Tipo de arquivo não permitido. Use: {", ".join(allowed_extensions)}'}), 400
    
    try:
        import cloudinary.uploader
        from werkzeug.utils import secure_filename
        
        # Buscar tipo do documento
        cursor.execute("SELECT id, nome FROM tipos_documentos_candidato WHERE id = %s", (tipo_id,))
        tipo_doc = cursor.fetchone()
        
        if not tipo_doc:
            return jsonify({'success': False, 'error': 'Tipo de documento inválido!'}), 400
        
        # Definir resource_type baseado na extensão
        if extensao == 'pdf':
            resource_type = "raw"
        else:
            resource_type = "image"
        
        # Upload para Cloudinary
        nome_arquivo = secure_filename(arquivo.filename)
        upload_result = cloudinary.uploader.upload(
            arquivo,
            folder=f"candidatos/{candidato_id}/documentos",
            resource_type=resource_type,
            type="upload",
            access_mode="public",
            use_filename=True,
            unique_filename=True
        )
        
        url_arquivo = upload_result.get('secure_url')
        public_id = upload_result.get('public_id')
        tamanho = upload_result.get('bytes', 0)
        
        # Corrigir URL se for PDF
        if extensao == 'pdf' and '/image/' in url_arquivo:
            url_arquivo = url_arquivo.replace('/image/', '/raw/')
        
        # Verificar se já existe documento deste tipo
        cursor.execute("""
            SELECT id FROM documentos_candidato 
            WHERE candidato_id = %s AND tipo_documento_id = %s
        """, (candidato_id, tipo_id))
        
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute("""
                UPDATE documentos_candidato SET
                    nome_arquivo = %s,
                    caminho_arquivo = %s,
                    tipo_arquivo = %s,
                    tamanho = %s,
                    status = 'pendente',
                    data_envio = CURRENT_TIMESTAMP,
                    enviado_por = %s,
                    observacao = NULL,
                    data_aprovacao = NULL,
                    aprovado_por = NULL
                WHERE id = %s
            """, (public_id, url_arquivo, extensao, tamanho, session['user_id'], existing['id']))
        else:
            cursor.execute("""
                INSERT INTO documentos_candidato 
                (candidato_id, tipo_documento_id, nome_arquivo, caminho_arquivo, 
                 tipo_arquivo, tamanho, enviado_por)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (candidato_id, tipo_id, public_id, url_arquivo, extensao, tamanho, session['user_id']))
        
        conn.commit()
        
        # Registrar log
        registrar_log("upload_documento", "documentos_candidato", candidato_id,
                     dados_novos={"tipo_documento": tipo_doc['nome'], "candidato_id": candidato_id})
        
        return jsonify({'success': True, 'message': f'Documento {tipo_doc["nome"]} enviado com sucesso!'})
        
    except Exception as e:
        print(f"Erro no upload: {e}")
        import traceback
        traceback.print_exc()
        
        if conn:
            conn.rollback()
        
        return jsonify({'success': False, 'error': str(e)}), 500
    
    finally:
        return_connection(conn)


@app.route("/api/documentos-candidato/<int:doc_id>/aprovar", methods=["POST"])
@login_required
def aprovar_documento_candidato(doc_id):
    """Aprovar ou rejeitar documento do candidato"""
    cursor, conn = get_db()
    
    if session.get('tipo') not in ['admin', 'sindicante']:
        return jsonify({'success': False, 'error': 'Permissão negada'}), 403
    
    data = request.get_json()
    status = data.get('status')  # 'aprovado' ou 'rejeitado'
    observacao = data.get('observacao', '')
    
    try:
        cursor.execute("""
            UPDATE documentos_candidato SET
                status = %s,
                observacao = %s,
                data_aprovacao = CURRENT_TIMESTAMP,
                aprovado_por = %s
            WHERE id = %s
        """, (status, observacao, session['user_id'], doc_id))
        
        conn.commit()
        
        return jsonify({'success': True, 'message': f'Documento {status} com sucesso!'})
        
    except Exception as e:
        print(f"Erro ao aprovar documento: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        return_connection(conn)
        
@app.route("/candidatos/<int:candidato_id>/documentos/ver/<int:doc_id>")
@login_required
def ver_documento_candidato(candidato_id, doc_id):
    """Visualizar documento do candidato"""
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT d.*, t.nome as tipo_nome
        FROM documentos_candidato d
        JOIN tipos_documentos_candidato t ON d.tipo_documento_id = t.id
        WHERE d.id = %s AND d.candidato_id = %s
    """, (doc_id, candidato_id))
    documento = cursor.fetchone()
    
    return_connection(conn)
    
    if not documento:
        flash("Documento não encontrado!", "danger")
        return redirect(f"/candidatos/{candidato_id}/documentos")
    
    # Redirecionar para a URL do arquivo
    return redirect(documento['caminho_arquivo'])        


@app.route("/api/documentos-candidato/<int:doc_id>/excluir", methods=["DELETE"])
@login_required
def excluir_documento_candidato(doc_id):
    """Excluir documento do candidato"""
    cursor, conn = get_db()
    
    if session.get('tipo') not in ['admin', 'sindicante']:
        return jsonify({'success': False, 'error': 'Permissão negada'}), 403
    
    try:
        # Buscar documento
        cursor.execute("""
            SELECT caminho_arquivo, nome_arquivo FROM documentos_candidato WHERE id = %s
        """, (doc_id,))
        doc = cursor.fetchone()
        
        if doc:
            # Excluir do Cloudinary
            import cloudinary.uploader
            cloudinary.uploader.destroy(doc['nome_arquivo'])
        
        cursor.execute("DELETE FROM documentos_candidato WHERE id = %s", (doc_id,))
        conn.commit()
        
        return jsonify({'success': True, 'message': 'Documento excluído com sucesso!'})
        
    except Exception as e:
        print(f"Erro ao excluir documento: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        return_connection(conn)
        
        
@app.route('/api/documentos-candidato/<int:doc_id>/rejeitar', methods=['POST'])
def rejeitar_documento_candidato(doc_id):
    """Rejeita um documento do candidato"""
    try:
        from flask import request, jsonify
        from datetime import datetime
        
        data = request.get_json()
        
        # Busca o documento
        cursor, conn = get_db()
        cursor.execute("SELECT * FROM documentos_candidato WHERE id = %s", (doc_id,))
        documento = cursor.fetchone()
        
        if not documento:
            return jsonify({'success': False, 'error': 'Documento não encontrado'}), 404
        
        # Atualiza o status
        cursor.execute("""
            UPDATE documentos_candidato 
            SET status = 'rejeitado', 
                observacao = %s,
                data_reprovacao = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (data.get('observacao', ''), doc_id))
        
        conn.commit()
        return_connection(conn)
        
        return jsonify({'success': True, 'message': 'Documento rejeitado'})
        
    except Exception as e:
        print(f"Erro ao rejeitar documento: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================
# ROTAS PARA GERENCIAR TIPOS DE DOCUMENTO
# =============================

@app.route("/admin/tipos-documentos")
@login_required
def admin_tipos_documentos():
    """Gerenciar tipos de documentos"""
    if session.get('tipo') != 'admin':
        flash("Acesso restrito a administradores!", "danger")
        return redirect("/dashboard")
    
    cursor, conn = get_db()
    
    cursor.execute("""
        SELECT * FROM tipos_documentos_candidato 
        ORDER BY obrigatorio DESC, ordem, nome
    """)
    tipos = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template("admin/tipos_documentos.html", tipos=tipos)


@app.route("/admin/tipos-documentos/editar/<int:tipo_id>", methods=["POST"])
@login_required
def editar_tipo_documento(tipo_id):
    """Editar tipo de documento (alterar obrigatoriedade)"""
    if session.get('tipo') != 'admin':
        return jsonify({'success': False, 'error': 'Permissão negada'}), 403
    
    data = request.get_json()
    obrigatorio = data.get('obrigatorio', 0)
    
    cursor, conn = get_db()
    
    try:
        cursor.execute("""
            UPDATE tipos_documentos_candidato 
            SET obrigatorio = %s 
            WHERE id = %s
        """, (obrigatorio, tipo_id))
        conn.commit()
        
        return jsonify({'success': True, 'message': 'Tipo de documento atualizado!'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        return_connection(conn)


@app.route("/admin/tipos-documentos/resetar", methods=["POST"])
@login_required
def resetar_tipos_documentos():
    """Resetar para a configuração padrão"""
    if session.get('tipo') != 'admin':
        return jsonify({'success': False, 'error': 'Permissão negada'}), 403
    
    cursor, conn = get_db()
    
    try:
        # Desativar todos
        cursor.execute("UPDATE tipos_documentos_candidato SET ativo = 0")
        
        # Recriar padrão
        tipos_padrao = [
            ('Certidão Negativa Eleitoral', 'Certidão Negativa da Justiça Eleitoral', 1, 1),
            ('Certidão Negativa Militar', 'Certidão Negativa do Serviço Militar', 1, 2),
            ('Tribunal Regional Federal da 1ª Região', 'Certidão Negativa do TRF1', 1, 3),
            ('Certidões Negativas do TJDFT', 'Certidão Negativa do Tribunal de Justiça do DF', 1, 4),
            ('Certidão de Antecedentes Criminais Polícia Federal', 'Certidão de Antecedentes Criminais da PF', 1, 5),
            ('Certidão Negativa de Débitos - GDF', 'Certidão Negativa de Débitos do Governo do DF', 1, 6),
            ('RG', 'Registro Geral - Documento de Identidade', 1, 7),
            ('CPF', 'Cadastro de Pessoa Física', 1, 8),
            ('Foto 3x4', 'Foto 3x4 recente e fundo branco', 1, 9),
            ('Certidão de Nascimento', 'Certidão de Nascimento (opcional)', 0, 10),
            ('Comprovante de Residência', 'Comprovante de endereço recente (opcional)', 0, 11),
            ('Currículo', 'Currículo profissional (opcional)', 0, 12),
            ('Título de Eleitor', 'Título de Eleitor (opcional)', 0, 13),
            ('Carteira de Trabalho', 'Carteira de Trabalho (opcional)', 0, 14),
            ('Diploma', 'Diploma de formação (opcional)', 0, 15)
        ]
        
        for nome, descricao, obrigatorio, ordem in tipos_padrao:
            cursor.execute("""
                INSERT INTO tipos_documentos_candidato (nome, descricao, obrigatorio, ordem, ativo)
                VALUES (%s, %s, %s, %s, 1)
                ON DUPLICATE KEY UPDATE
                descricao = VALUES(descricao),
                obrigatorio = VALUES(obrigatorio),
                ordem = VALUES(ordem),
                ativo = 1
            """, (nome, descricao, obrigatorio, ordem))
        
        conn.commit()
        
        return jsonify({'success': True, 'message': 'Tipos de documento resetados com sucesso!'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        return_connection(conn)

@app.route("/admin/inicializar-tipos-documentos")
@login_required
def inicializar_tipos_documentos():
    """Rota única para inicializar os tipos de documento (apenas admin)"""
    if session.get('tipo') != 'admin':
        flash("Acesso restrito!", "danger")
        return redirect("/dashboard")
    
    cursor, conn = get_db()
    
    try:
        # Verificar se já existem tipos
        cursor.execute("SELECT COUNT(*) as total FROM tipos_documentos_candidato")
        total = cursor.fetchone()['total']
        
        if total > 0:
            # Método seguro: apenas atualizar, não excluir
            flash("⚠️ Tipos de documento já existem. Atualizando obrigatoriedades...", "warning")
            
            # Definir todos como não obrigatórios primeiro
            cursor.execute("UPDATE tipos_documentos_candidato SET obrigatorio = 0, ativo = 1")
            
            # Lista de documentos que devem ser obrigatórios
            obrigatorios = [
                'Certidão Negativa Eleitoral',
                'Certidão Negativa Militar',
                'Tribunal Regional Federal da 1ª Região',
                'Certidões Negativas do TJDFT',
                'Certidão de Antecedentes Criminais Polícia Federal',
                'Certidão Negativa de Débitos - GDF',
                'RG',
                'CPF',
                'Foto 3x4'
            ]
            
            # Marcar os obrigatórios
            for nome in obrigatorios:
                cursor.execute("""
                    UPDATE tipos_documentos_candidato 
                    SET obrigatorio = 1 
                    WHERE nome = %s
                """, (nome,))
            
            conn.commit()
            flash(f"✅ {len(obrigatorios)} documentos marcados como obrigatórios!", "success")
        else:
            # Inserir todos
            tipos = [
                ('Certidão Negativa Eleitoral', 'Certidão Negativa da Justiça Eleitoral', 1, 1),
                ('Certidão Negativa Militar', 'Certidão Negativa do Serviço Militar', 1, 2),
                ('Tribunal Regional Federal da 1ª Região', 'Certidão Negativa do TRF1', 1, 3),
                ('Certidões Negativas do TJDFT', 'Certidão Negativa do Tribunal de Justiça do DF', 1, 4),
                ('Certidão de Antecedentes Criminais Polícia Federal', 'Certidão de Antecedentes Criminais da PF', 1, 5),
                ('Certidão Negativa de Débitos - GDF', 'Certidão Negativa de Débitos do Governo do DF', 1, 6),
                ('RG', 'Registro Geral - Documento de Identidade', 1, 7),
                ('CPF', 'Cadastro de Pessoa Física', 1, 8),
                ('Foto 3x4', 'Foto 3x4 recente e fundo branco', 1, 9),
                ('Certidão de Nascimento', 'Certidão de Nascimento (opcional)', 0, 10),
                ('Comprovante de Residência', 'Comprovante de endereço recente (opcional)', 0, 11),
                ('Currículo', 'Currículo profissional (opcional)', 0, 12),
                ('Título de Eleitor', 'Título de Eleitor (opcional)', 0, 13),
                ('Carteira de Trabalho', 'Carteira de Trabalho (opcional)', 0, 14),
                ('Diploma', 'Diploma de formação (opcional)', 0, 15)
            ]
            
            for nome, descricao, obrigatorio, ordem in tipos:
                cursor.execute("""
                    INSERT INTO tipos_documentos_candidato (nome, descricao, obrigatorio, ordem, ativo)
                    VALUES (%s, %s, %s, %s, 1)
                """, (nome, descricao, obrigatorio, ordem))
            
            conn.commit()
            flash(f"✅ {len(tipos)} tipos de documento inicializados com sucesso!", "success")
        
    except Exception as e:
        flash(f"❌ Erro ao inicializar: {str(e)}", "danger")
        print(f"Erro detalhado: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        return_connection(conn)
    
    return redirect("/admin/tipos-documentos")   

@app.route("/admin/sincronizar-tipos-documentos")
@login_required
def sincronizar_tipos_documentos():
    """Sincroniza os tipos de documento com a lista obrigatória"""
    if session.get('tipo') != 'admin':
        flash("Acesso restrito!", "danger")
        return redirect("/dashboard")
    
    cursor, conn = get_db()
    
    # Lista completa de documentos obrigatórios
    documentos_obrigatorios = [
        ('Certidão Negativa Eleitoral', 'Certidão Negativa da Justiça Eleitoral', 1, 1),
        ('Certidão Negativa Militar', 'Certidão Negativa do Serviço Militar', 1, 2),
        ('Tribunal Regional Federal da 1ª Região', 'Certidão Negativa do TRF1', 1, 3),
        ('Certidões Negativas do TJDFT', 'Certidão Negativa do Tribunal de Justiça do DF', 1, 4),
        ('Certidão de Antecedentes Criminais Polícia Federal', 'Certidão de Antecedentes Criminais da PF', 1, 5),
        ('Certidão Negativa de Débitos - GDF', 'Certidão Negativa de Débitos do Governo do DF', 1, 6),
        ('RG', 'Registro Geral - Documento de Identidade', 1, 7),
        ('CPF', 'Cadastro de Pessoa Física', 1, 8),
        ('Foto 3x4', 'Foto 3x4 recente e fundo branco', 1, 9)
    ]
    
    # Lista de documentos opcionais
    documentos_opcionais = [
        ('Certidão de Nascimento', 'Certidão de Nascimento (opcional)', 0, 10),
        ('Comprovante de Residência', 'Comprovante de endereço recente (opcional)', 0, 11),
        ('Currículo', 'Currículo profissional (opcional)', 0, 12),
        ('Título de Eleitor', 'Título de Eleitor (opcional)', 0, 13),
        ('Carteira de Trabalho', 'Carteira de Trabalho (opcional)', 0, 14),
        ('Diploma', 'Diploma de formação (opcional)', 0, 15)
    ]
    
    try:
        # Inserir ou atualizar documentos obrigatórios
        for nome, descricao, obrigatorio, ordem in documentos_obrigatorios:
            cursor.execute("""
                INSERT INTO tipos_documentos_candidato (nome, descricao, obrigatorio, ordem, ativo)
                VALUES (%s, %s, %s, %s, 1)
                ON CONFLICT (nome) DO UPDATE SET
                    descricao = EXCLUDED.descricao,
                    obrigatorio = EXCLUDED.obrigatorio,
                    ordem = EXCLUDED.ordem,
                    ativo = 1
            """, (nome, descricao, obrigatorio, ordem))
        
        # Inserir ou atualizar documentos opcionais
        for nome, descricao, obrigatorio, ordem in documentos_opcionais:
            cursor.execute("""
                INSERT INTO tipos_documentos_candidato (nome, descricao, obrigatorio, ordem, ativo)
                VALUES (%s, %s, %s, %s, 1)
                ON CONFLICT (nome) DO UPDATE SET
                    descricao = EXCLUDED.descricao,
                    obrigatorio = EXCLUDED.obrigatorio,
                    ordem = EXCLUDED.ordem,
                    ativo = 1
            """, (nome, descricao, obrigatorio, ordem))
        
        conn.commit()
        
        # Contar quantos foram inseridos/atualizados
        cursor.execute("SELECT COUNT(*) as total FROM tipos_documentos_candidato WHERE ativo = 1")
        total = cursor.fetchone()['total']
        
        flash(f"✅ {total} tipos de documento sincronizados com sucesso! (9 obrigatórios, {total-9} opcionais)", "success")
        
    except Exception as e:
        flash(f"❌ Erro ao sincronizar: {str(e)}", "danger")
        print(f"Erro detalhado: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        return_connection(conn)
    
    return redirect("/admin/tipos-documentos")    

# =============================
# ROTAS DE CHECKLIST DIGITAL
# =============================

@app.route("/candidatos/<int:candidato_id>/checklist")
@login_required
def checklist_candidato(candidato_id):
    """Visualiza o checklist do candidato"""
    cursor, conn = get_db()
    
    # Verificar permissão (admin, sindicante ou mestre)
    usuario_tipo = session.get('tipo', '')
    usuario_grau = session.get('grau_atual', 0)
    
    if usuario_tipo not in ['admin', 'sindicante'] and usuario_grau < 3:
        flash("Permissão negada!", "danger")
        return redirect("/candidatos")
    
    # Buscar candidato
    cursor.execute("SELECT id, nome, status FROM candidatos WHERE id = %s", (candidato_id,))
    candidato = cursor.fetchone()
    
    if not candidato:
        flash("Candidato não encontrado!", "danger")
        return_connection(conn)
        return redirect("/candidatos")
    
    # Buscar categorias do checklist
    cursor.execute("""
        SELECT * FROM categorias_checklist 
        WHERE ativo = 1 
        ORDER BY ordem
    """)
    categorias = cursor.fetchall()
    
    # Buscar itens do checklist por categoria
    checklist = {}
    for cat in categorias:
        cursor.execute("""
            SELECT i.*, 
                   COALESCE(p.status, 'pendente') as status,
                   p.observacao as progresso_obs,
                   p.concluido_por,
                   p.data_conclusao,
                   u.nome_completo as concluido_por_nome,
                   p.data_limite
            FROM itens_checklist i
            LEFT JOIN progresso_checklist p ON i.id = p.item_id AND p.candidato_id = %s
            LEFT JOIN usuarios u ON p.concluido_por = u.id
            WHERE i.categoria_id = %s AND i.ativo = 1
            ORDER BY i.ordem
        """, (candidato_id, cat['id']))
        itens = cursor.fetchall()
        checklist[cat['id']] = {
            'categoria': cat,
            'itens': itens
        }
    
    # Calcular progresso total (considerando TODOS os itens obrigatórios)
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN COALESCE(p.status, 'pendente') = 'concluido' THEN 1 ELSE 0 END) as concluidos
        FROM itens_checklist i
        LEFT JOIN progresso_checklist p ON i.id = p.item_id AND p.candidato_id = %s
        WHERE i.obrigatorio = 1 AND i.ativo = 1
    """, (candidato_id,))
    
    progresso = cursor.fetchone()
    total_obrigatorios = progresso['total'] if progresso['total'] else 0
    concluidos = progresso['concluidos'] if progresso['concluidos'] else 0
    percentual = int((concluidos / total_obrigatorios * 100)) if total_obrigatorios > 0 else 0
    
    return_connection(conn)
    
    return render_template("candidatos/checklist.html",
                          candidato=candidato,
                          checklist=checklist,
                          total_obrigatorios=total_obrigatorios,
                          concluidos=concluidos,
                          percentual=percentual)


@app.route("/api/checklist/<int:candidato_id>/item/<int:item_id>/atualizar", methods=["POST"])
@login_required
def atualizar_item_checklist(candidato_id, item_id):
    """Atualiza o status de um item do checklist"""
    cursor, conn = get_db()
    
    # Verificar permissão
    usuario_tipo = session.get('tipo', '')
    usuario_grau = session.get('grau_atual', 0)
    
    if usuario_tipo not in ['admin', 'sindicante'] and usuario_grau < 3:
        return jsonify({'success': False, 'error': 'Permissão negada'}), 403
    
    data = request.get_json()
    status = data.get('status')
    observacao = data.get('observacao', '')
    
    try:
        # Verificar se já existe registro
        cursor.execute("""
            SELECT id FROM progresso_checklist 
            WHERE candidato_id = %s AND item_id = %s
        """, (candidato_id, item_id))
        
        existing = cursor.fetchone()
        
        if existing:
            if status:  # Se veio status, atualiza
                cursor.execute("""
                    UPDATE progresso_checklist SET
                        status = %s,
                        observacao = COALESCE(NULLIF(%s, ''), observacao),
                        concluido_por = CASE WHEN %s = 'concluido' THEN %s ELSE NULL END,
                        data_conclusao = CASE WHEN %s = 'concluido' THEN CURRENT_TIMESTAMP ELSE NULL END
                    WHERE candidato_id = %s AND item_id = %s
                """, (status, observacao, status, session['user_id'], status, candidato_id, item_id))
            else:
                # Apenas atualizar observação
                cursor.execute("""
                    UPDATE progresso_checklist SET
                        observacao = %s
                    WHERE candidato_id = %s AND item_id = %s
                """, (observacao, candidato_id, item_id))
        else:
            cursor.execute("""
                INSERT INTO progresso_checklist (candidato_id, item_id, status, observacao, concluido_por)
                VALUES (%s, %s, %s, %s, %s)
            """, (candidato_id, item_id, status or 'pendente', observacao, 
                  session['user_id'] if status == 'concluido' else None))
        
        conn.commit()
        
        # Calcular novo progresso
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN COALESCE(p.status, 'pendente') = 'concluido' THEN 1 ELSE 0 END) as concluidos
            FROM itens_checklist i
            LEFT JOIN progresso_checklist p ON i.id = p.item_id AND p.candidato_id = %s
            WHERE i.obrigatorio = 1 AND i.ativo = 1
        """, (candidato_id,))
        
        progresso = cursor.fetchone()
        total = progresso['total'] if progresso['total'] else 0
        concluidos = progresso['concluidos'] if progresso['concluidos'] else 0
        percentual = int((concluidos / total * 100)) if total > 0 else 0
        
        registrar_log("atualizar_checklist", "checklist", candidato_id,
                     dados_novos={"item_id": item_id, "status": status})
        
        return jsonify({
            'success': True, 
            'message': 'Checklist atualizado!',
            'progresso': {
                'total': total,
                'concluidos': concluidos,
                'percentual': percentual
            }
        })
        
    except Exception as e:
        print(f"Erro ao atualizar checklist: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        return_connection(conn)


@app.route("/api/checklist/<int:candidato_id>/resetar", methods=["POST"])
@login_required
def resetar_checklist(candidato_id):
    """Reseta todo o checklist do candidato (apenas admin)"""
    cursor, conn = get_db()
    
    if session.get('tipo') != 'admin':
        return jsonify({'success': False, 'error': 'Apenas administradores podem resetar o checklist'}), 403
    
    try:
        cursor.execute("""
            DELETE FROM progresso_checklist WHERE candidato_id = %s
        """, (candidato_id,))
        conn.commit()
        
        return jsonify({'success': True, 'message': 'Checklist resetado com sucesso!'})
        
    except Exception as e:
        print(f"Erro ao resetar checklist: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        return_connection(conn)        

@app.route("/sindicantes", methods=["GET", "POST"])
@login_required
@permissao_required('sindicante.view')
def gerenciar_sindicantes():
    cursor, conn = get_db()
    
    if request.method == "POST":
        # Verificar se veio obreiro_id (promoção) ou dados de novo sindicante
        obreiro_id = request.form.get("obreiro_id")
        
        if obreiro_id:
            # PROMOVER OBREIRO EXISTENTE A SINDICANTE
            cursor.execute("""
                SELECT id, nome_completo, grau_atual FROM usuarios 
                WHERE id = %s AND tipo = 'obreiro' AND ativo = 1 AND grau_atual >= 3
            """, (obreiro_id,))
            obreiro = cursor.fetchone()
            
            if obreiro:
                # Verificar se tem permissão para promover
                if tem_permissao('obreiro.promote'):
                    cursor.execute("UPDATE usuarios SET tipo = 'sindicante' WHERE id = %s", (obreiro_id,))
                    conn.commit()
                    registrar_log("promover", "sindicante", obreiro_id, 
                                dados_novos={"nome": obreiro['nome_completo'], "grau": obreiro['grau_atual']})
                    flash(f"✅ {obreiro['nome_completo']} foi promovido a Sindicante com sucesso!", "success")
                else:
                    flash("Você não tem permissão para promover obreiros a sindicantes.", "danger")
            else:
                flash("Obreiro não encontrado ou não atende aos requisitos (precisa ser Mestre ou superior e estar ativo)", "danger")
            
            return_connection(conn)
            return redirect("/sindicantes")
        
        else:
            # CADASTRAR NOVO SINDICANTE (apenas admin)
            if not tem_permissao('sindicante.create'):
                flash("Você não tem permissão para cadastrar novos sindicantes.", "danger")
                return_connection(conn)
                return redirect("/sindicantes")
            
            # Pegar dados do formulário
            usuario = request.form.get('usuario', '').strip()
            senha = request.form.get('senha', '')
            nome_completo = request.form.get('nome_completo', '').strip()
            cim_numero = request.form.get('cim_numero', '').strip()
            grau_atual = request.form.get('grau_atual', 3)
            loja_nome = request.form.get('loja_nome', '').strip()
            loja_numero = request.form.get('loja_numero', '').strip()
            loja_orient = request.form.get('loja_orient', '').strip()
            telefone = request.form.get('telefone', '').strip()
            email = request.form.get('email', '').strip()
            
            # Validações
            if not usuario:
                flash("❌ O campo Usuário é obrigatório!", "danger")
                return_connection(conn)
                return redirect("/sindicantes")
            
            if not senha:
                flash("❌ O campo Senha é obrigatório!", "danger")
                return_connection(conn)
                return redirect("/sindicantes")
            
            if len(senha) < 6:
                flash("❌ A senha deve ter no mínimo 6 caracteres!", "danger")
                return_connection(conn)
                return redirect("/sindicantes")
            
            # Verificar se usuário já existe
            cursor.execute("SELECT id FROM usuarios WHERE usuario = %s", (usuario,))
            if cursor.fetchone():
                flash(f"❌ O usuário '{usuario}' já existe!", "danger")
                return_connection(conn)
                return redirect("/sindicantes")
            
            # Verificar se CIM já existe (se informado)
            if cim_numero:
                cursor.execute("SELECT id FROM usuarios WHERE cim_numero = %s", (cim_numero,))
                if cursor.fetchone():
                    flash(f"❌ O CIM número '{cim_numero}' já está cadastrado!", "danger")
                    return_connection(conn)
                    return redirect("/sindicantes")
            
            # Hash da senha
            from werkzeug.security import generate_password_hash
            from datetime import datetime
            senha_hash = generate_password_hash(senha)
            data_cadastro = datetime.now()
            
            try:
                # Inserir novo sindicante
                cursor.execute("""
                    INSERT INTO usuarios 
                    (usuario, senha_hash, tipo, data_cadastro, nome_completo, cim_numero, grau_atual, 
                     loja_nome, loja_numero, loja_orient, telefone, email, ativo,
                     status_maconico, isento, artigo_27, recolhe)
                    VALUES (%s, %s, 'sindicante', %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, 1,
                            'Regular', 'NÃO', 'NÃO', 'Sim')
                """, (usuario, senha_hash, data_cadastro, nome_completo, cim_numero, grau_atual, 
                      loja_nome, loja_numero, loja_orient, telefone, email))
                
                conn.commit()
                novo_id = cursor.lastrowid
                
                registrar_log("criar", "sindicante", novo_id, 
                            dados_novos={"usuario": usuario, "nome": nome_completo})
                
                flash(f"✅ Sindicante '{usuario}' cadastrado com sucesso!", "success")
                
            except Exception as e:
                conn.rollback()
                print(f"Erro ao cadastrar sindicante: {str(e)}")
                flash(f"❌ Erro ao cadastrar sindicante: {str(e)}", "danger")
            
            return_connection(conn)
            return redirect("/sindicantes")
    
    # GET - Listar sindicantes (fora do POST)
    cursor.execute("""
        SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, 
               ativo, telefone, email, grau_atual
        FROM usuarios 
        WHERE tipo = 'sindicante' AND ativo = 1
        ORDER BY nome_completo
    """)
    sindicantes = cursor.fetchall()
    
    # Buscar obreiros que podem ser promovidos (apenas para quem tem permissão)
    if tem_permissao('obreiro.promote'):
        cursor.execute("""
            SELECT id, usuario, nome_completo, cim_numero, loja_nome, grau_atual
            FROM usuarios 
            WHERE tipo = 'obreiro' 
            AND ativo = 1 
            AND grau_atual >= 3
            ORDER BY grau_atual DESC, nome_completo
        """)
        obreiros_mestres = cursor.fetchall()
    else:
        obreiros_mestres = []
    
    # Buscar lojas para o cadastro
    cursor.execute("SELECT id, nome, numero, oriente FROM lojas WHERE ativo = 1 ORDER BY nome")
    lojas = cursor.fetchall()
    
    return_connection(conn)
    
    return render_template(
        "sindicantes.html",
        sindicantes=sindicantes,
        obreiros_mestres=obreiros_mestres,
        lojas=lojas
    )
    
@app.route("/sindicantes/<int:id>/rebaixar")
@admin_required
def rebaixar_sindicante(id):
    """Rebaixa um sindicante para obreiro (mantém o grau de Mestre)"""
    cursor, conn = get_db()
    
    try:
        # Buscar dados do sindicante
        cursor.execute("""
            SELECT id, nome_completo, usuario, grau_atual, tipo 
            FROM usuarios 
            WHERE id = %s AND tipo = 'sindicante'
        """, (id,))
        sindicante = cursor.fetchone()
        
        if not sindicante:
            flash("Sindicante não encontrado!", "danger")
            return_connection(conn)
            return redirect("/sindicantes")
        
        # Verificar se o sindicante tem grau suficiente para ser obreiro
        if sindicante['grau_atual'] < 1:
            flash("Grau inválido para rebaixamento!", "danger")
            return_connection(conn)
            return redirect("/sindicantes")
        
        # Rebaixar para obreiro
        cursor.execute("""
            UPDATE usuarios 
            SET tipo = 'obreiro' 
            WHERE id = %s
        """, (id,))
        conn.commit()
        
        # Registrar log
        registrar_log("rebaixar", "sindicante", id, 
                     dados_anteriores={"tipo": "sindicante", "nome": sindicante['nome_completo']},
                     dados_novos={"tipo": "obreiro", "nome": sindicante['nome_completo']})
        
        flash(f"✅ {sindicante['nome_completo']} foi rebaixado para Obreiro com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao rebaixar sindicante: {e}")
        conn.rollback()
        flash(f"Erro ao rebaixar sindicante: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect("/sindicantes")

    
        
@app.route("/reverter_sindicante/<int:id>")
@admin_required
def reverter_sindicante(id):
    cursor, conn = get_db()

    try:
        # buscar usuário independente do tipo
        cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
        sindicante = cursor.fetchone()

        if not sindicante:
            flash("Usuário não encontrado", "danger")
            return redirect("/sindicantes")

        if sindicante["tipo"] != "sindicante":
            flash("Este usuário não é sindicante", "warning")
            return redirect("/sindicantes")

        # update seguro
        cursor.execute("""
            UPDATE usuarios
            SET tipo = 'obreiro'
            WHERE id = %s
        """, (id,))

        conn.commit()

        registrar_log(
            "reverter_sindicante",
            "usuarios",
            id,
            dados_anteriores={"tipo": "sindicante"},
            dados_novos={"tipo": "obreiro"}
        )

        flash(f"Sindicante {sindicante['usuario']} revertido para obreiro", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao reverter: {str(e)}", "danger")

    finally:
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
        registrar_log("editar", "sindicante", id, dados_anteriores=dados_antigos, dados_novos={"nome_completo": nome_completo})
        flash("Sindicante atualizado!", "success")
        return_connection(conn)
        return redirect("/sindicantes")
    cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
    sindicante = cursor.fetchone()
    cursor.execute("SELECT * FROM lojas")
    lojas = cursor.fetchall()
    return_connection(conn)
    return render_template("editar_sindicante.html", sindicante=sindicante, lojas=lojas)
    

@app.route("/sindicantes/<int:id>/excluir", methods=["GET", "POST"])
@admin_required
def excluir_sindicante(id):
    """Exclui um sindicante permanentemente"""
    cursor, conn = get_db()
    
    try:
        # Buscar dados do sindicante
        cursor.execute("""
            SELECT id, nome_completo, usuario, grau_atual, tipo, ativo, foto
            FROM usuarios 
            WHERE id = %s AND tipo = 'sindicante'
        """, (id,))
        sindicante = cursor.fetchone()
        
        if not sindicante:
            flash("Sindicante não encontrado!", "danger")
            return_connection(conn)
            return redirect("/sindicantes")
        
        # Verificar se o sindicante tem vínculos em outras tabelas
        # CORRIGIDO: Converter id para texto porque a coluna sindicante é text
        cursor.execute("SELECT COUNT(*) as total FROM sindicancias WHERE sindicante = %s", (str(id),))
        sindicancias_votos = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM presenca WHERE obreiro_id = %s", (id,))
        presencas = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM ocupacao_cargos WHERE obreiro_id = %s", (id,))
        cargos = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM atas WHERE redator_id = %s", (id,))
        atas = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM comunicados WHERE criado_por = %s", (id,))
        comunicados = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM logs_auditoria WHERE usuario_id = %s", (id,))
        logs = cursor.fetchone()['total']
        
        # Total de vínculos
        total_vinculos = sindicancias_votos + presencas + cargos + atas + comunicados + logs
        
        if total_vinculos > 0:
            # Se tiver vínculos, apenas desativar
            cursor.execute("UPDATE usuarios SET ativo = 0 WHERE id = %s", (id,))
            conn.commit()
            
            registrar_log("desativar", "sindicante", id, 
                         dados_anteriores={"nome": sindicante['nome_completo'], "ativo": 1},
                         dados_novos={"status": "inativo", "motivo": f"possui {total_vinculos} vínculos no sistema"})
            
            flash(f"⚠️ Sindicante '{sindicante['nome_completo']}' foi DESATIVADO (possui {total_vinculos} vínculos no sistema).", "warning")
        else:
            # Sem vínculos, pode excluir permanentemente
            # Remover foto se existir
            if sindicante.get('foto'):
                try:
                    import os
                    if os.path.exists(os.path.join(UPLOAD_FOLDER_FOTOS, sindicante['foto'])):
                        os.remove(os.path.join(UPLOAD_FOLDER_FOTOS, sindicante['foto']))
                except:
                    pass  # Se for Cloudinary, ignora
            
            # Excluir o sindicante
            cursor.execute("DELETE FROM usuarios WHERE id = %s", (id,))
            conn.commit()
            
            registrar_log("excluir", "sindicante", id, dados_anteriores={"nome": sindicante['nome_completo']})
            flash(f"✅ Sindicante '{sindicante['nome_completo']}' excluído permanentemente!", "success")
        
        return_connection(conn)
        return redirect("/sindicantes")
        
    except Exception as e:
        print(f"Erro ao excluir sindicante: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
        flash(f"Erro ao excluir sindicante: {str(e)}", "danger")
        return_connection(conn)
        return redirect("/sindicantes")    

# =============================
# ROTAS DE LOJAS - COMPLETAS
# =============================
@app.route("/lojas/editar/<int:id>", methods=["GET", "POST"])
@login_required
@permissao_required('loja.edit')
def editar_loja(id):
    cursor, conn = get_db()
    
    # Buscar dados da loja
    cursor.execute("SELECT * FROM lojas WHERE id = %s", (id,))
    loja = cursor.fetchone()
    
    if not loja:
        flash("Loja não encontrada!", "danger")
        return_connection(conn)
        return redirect("/lojas")
    
    if request.method == "POST":
        def safe(v):
            return v if v and v.strip() != "" else None

        nome = safe(request.form.get("nome"))
        numero = safe(request.form.get("numero"))
        oriente = safe(request.form.get("oriente"))
        cidade = safe(request.form.get("cidade"))
        uf = safe(request.form.get("uf"))
        endereco = safe(request.form.get("endereco"))
        bairro = safe(request.form.get("bairro"))
        cep = safe(request.form.get("cep"))
        telefone = safe(request.form.get("telefone"))
        email = safe(request.form.get("email"))
        site = safe(request.form.get("site"))
        data_fundacao = safe(request.form.get("data_fundacao"))
        data_instalacao = safe(request.form.get("data_instalacao"))
        data_autorizacao = safe(request.form.get("data_autorizacao"))
        veneravel_mestre = safe(request.form.get("veneravel_mestre"))
        secretario = safe(request.form.get("secretario"))
        tesoureiro = safe(request.form.get("tesoureiro"))
        orador = safe(request.form.get("orador"))
        
        # Campos de horário
        horario_inicio = request.form.get("horario_inicio")
        horario_termino = request.form.get("horario_termino")
        dias_sessao = request.form.get("dias_sessao")
        frequencia_sessao = request.form.get("frequencia_sessao")
        observacoes_horario = request.form.get("observacoes_horario")
        
        rito = safe(request.form.get("rito"))
        observacoes = safe(request.form.get("observacoes"))
        ativo = 1 if request.form.get("ativo") else 0

        if not nome:
            flash("O nome da loja é obrigatório!", "danger")
            return_connection(conn)
            return redirect(f"/lojas/editar/{id}")

        try:
            dados_anteriores = dict(loja)
            
            cursor.execute("""
                UPDATE lojas SET
                    nome = %s,
                    numero = %s,
                    oriente = %s,
                    cidade = %s,
                    uf = %s,
                    endereco = %s,
                    bairro = %s,
                    cep = %s,
                    telefone = %s,
                    email = %s,
                    site = %s,
                    data_fundacao = %s,
                    data_instalacao = %s,
                    data_autorizacao = %s,
                    veneravel_mestre = %s,
                    secretario = %s,
                    tesoureiro = %s,
                    orador = %s,
                    horario_inicio = %s,
                    horario_termino = %s,
                    dias_sessao = %s,
                    frequencia_sessao = %s,
                    observacoes_horario = %s,
                    rito = %s,
                    observacoes = %s,
                    ativo = %s
                WHERE id = %s
            """, (
                nome, numero, oriente, cidade, uf,
                endereco, bairro, cep,
                telefone, email, site,
                data_fundacao, data_instalacao, data_autorizacao,
                veneravel_mestre, secretario, tesoureiro, orador,
                horario_inicio, horario_termino, dias_sessao, frequencia_sessao, observacoes_horario,
                rito, observacoes, ativo,
                id
            ))
            
            conn.commit()
            
            registrar_log("editar", "loja", id, dados_anteriores=dados_anteriores, dados_novos={"nome": nome})
            flash(f"Loja '{nome}' atualizada com sucesso!", "success")
            return_connection(conn)
            return redirect("/lojas")
            
        except Exception as e:
            print(f"Erro ao editar loja: {e}")
            conn.rollback()
            flash(f"Erro ao editar loja: {str(e)}", "danger")
            return_connection(conn)
            return redirect(f"/lojas/editar/{id}")
    
    return_connection(conn)
    return render_template("lojas_editar.html", loja=loja)
    
    
@app.route("/lojas")
@login_required
@permissao_required('loja.view')
def listar_lojas():
    cursor, conn = get_db()

    cursor.execute("""
        SELECT id, nome, numero, oriente, cidade, uf, ativo
        FROM lojas
        ORDER BY nome ASC
    """)

    lojas = cursor.fetchall()
    return_connection(conn)

    return render_template("lojas_listar.html", lojas=lojas)


@app.route("/lojas/nova", methods=["GET", "POST"])
@login_required
@permissao_required('loja.create')
def nova_loja():
    if request.method == "POST":
        def safe(v):
            return v if v and v.strip() != "" else None

        nome = safe(request.form.get("nome"))
        numero = safe(request.form.get("numero"))
        oriente = safe(request.form.get("oriente"))
        cidade = safe(request.form.get("cidade"))
        uf = safe(request.form.get("uf"))
        endereco = safe(request.form.get("endereco"))
        bairro = safe(request.form.get("bairro"))
        cep = safe(request.form.get("cep"))
        telefone = safe(request.form.get("telefone"))
        email = safe(request.form.get("email"))
        site = safe(request.form.get("site"))
        data_fundacao = safe(request.form.get("data_fundacao"))
        data_instalacao = safe(request.form.get("data_instalacao"))
        data_autorizacao = safe(request.form.get("data_autorizacao"))
        veneravel_mestre = safe(request.form.get("veneravel_mestre"))
        secretario = safe(request.form.get("secretario"))
        tesoureiro = safe(request.form.get("tesoureiro"))
        orador = safe(request.form.get("orador"))
        horario_inicio = request.form.get("horario_inicio")
        horario_termino = request.form.get("horario_termino")
        dias_sessao = request.form.get("dias_sessao")
        frequencia_sessao = request.form.get("frequencia_sessao")
        observacoes_horario = request.form.get("observacoes_horario")
        rito = safe(request.form.get("rito"))
        observacoes = safe(request.form.get("observacoes"))
        ativo = 1 if request.form.get("ativo") else 0

        if not nome:
            flash("O nome da loja é obrigatório!", "danger")
            return redirect("/lojas/nova")

        try:
            cursor, conn = get_db()
            
            # Verificar duplicidade
            cursor.execute("SELECT id FROM lojas WHERE nome = %s", (nome,))
            if cursor.fetchone():
                flash(f"Já existe uma loja com o nome '{nome}'!", "danger")
                return_connection(conn)
                return redirect("/lojas/nova")

            # Inserir nova loja (deixar o id ser gerado automaticamente)
            cursor.execute("""
                INSERT INTO lojas (
                    nome, numero, oriente, cidade, uf,
                    endereco, bairro, cep,
                    telefone, email, site,
                    data_fundacao, data_instalacao, data_autorizacao,
                    veneravel_mestre, secretario, tesoureiro, orador,
                    horario_inicio, horario_termino, dias_sessao, frequencia_sessao, observacoes_horario,
                    rito, observacoes, ativo
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s)
                RETURNING id
            """, (
                nome, numero, oriente, cidade, uf,
                endereco, bairro, cep,
                telefone, email, site,
                data_fundacao, data_instalacao, data_autorizacao,
                veneravel_mestre, secretario, tesoureiro, orador,
                horario_inicio, horario_termino, dias_sessao, frequencia_sessao, observacoes_horario,
                rito, observacoes, ativo
            ))
            
            loja_id = cursor.fetchone()['id']
            conn.commit()
            
            registrar_log("criar", "loja", loja_id, dados_novos={"nome": nome})
            flash(f"Loja '{nome}' criada com sucesso!", "success")
            return_connection(conn)
            return redirect("/lojas")
            
        except Exception as e:
            print(f"Erro ao criar loja: {e}")
            conn.rollback()
            flash(f"Erro ao criar loja: {str(e)}", "danger")
            return_connection(conn)
            return redirect("/lojas/nova")
    
    return render_template("lojas_nova.html")


@app.route("/lojas/excluir/<int:id>", methods=["POST"])
@login_required
@permissao_required('loja.delete')
def excluir_loja(id):
    try:
        cursor, conn = get_db()
        
        # Buscar dados da loja
        cursor.execute("SELECT id, nome FROM lojas WHERE id = %s", (id,))
        loja = cursor.fetchone()
        
        if not loja:
            flash("Loja não encontrada!", "danger")
            return redirect("/lojas")
        
        # Verificar obreiros vinculados
        cursor.execute("""
            SELECT id, usuario, nome_completo, cim_numero 
            FROM usuarios 
            WHERE loja_nome = %s
            ORDER BY nome_completo
        """, (loja['nome'],))
        obreiros = cursor.fetchall()
        total_obreiros = len(obreiros)
        
        if total_obreiros > 0:
            # Redirecionar para página de erro com detalhes
            return render_template("erro_exclusao_loja.html", 
                                 loja_id=loja['id'],
                                 loja_nome=loja['nome'],
                                 total_obreiros=total_obreiros,
                                 obreiros=obreiros)
        
        # Verificar reuniões vinculadas
        cursor.execute("SELECT COUNT(*) as total FROM reunioes WHERE loja_id = %s", (id,))
        reunioes = cursor.fetchone()['total']
        
        if reunioes > 0:
            flash(f"""
                <div class="d-flex align-items-center">
                    <i class="bi bi-calendar-x-fill fs-1 me-3 text-danger"></i>
                    <div>
                        <strong class="fs-5">Não é possível excluir a loja "{loja['nome']}"</strong><br>
                        <span>{reunioes} reuniões estão vinculadas a esta loja.</span>
                    </div>
                </div>
            """, "danger")
            return redirect("/lojas")
        
        # Excluir loja (se não houver vínculos)
        cursor.execute("DELETE FROM lojas WHERE id = %s", (id,))
        conn.commit()
        
        registrar_log("excluir", "loja", id, dados_anteriores={"nome": loja['nome']})
        
        flash(f"""
            <div class="d-flex align-items-center">
                <i class="bi bi-check-circle-fill fs-1 me-3 text-success"></i>
                <div>
                    <strong class="fs-5">Loja excluída com sucesso!</strong><br>
                    A loja "{loja['nome']}" foi removida permanentemente.
                </div>
            </div>
        """, "success")
        
        return redirect("/lojas")
        
    except Exception as e:
        print(f"Erro ao excluir loja: {e}")
        if conn:
            conn.rollback()
        flash(f"""
            <div class="d-flex align-items-center">
                <i class="bi bi-bug-fill fs-1 me-3 text-danger"></i>
                <div>
                    <strong class="fs-5">Erro ao excluir loja</strong><br>
                    {str(e)}
                </div>
            </div>
        """, "danger")
        return redirect("/lojas")
    finally:
        if conn:
            return_connection(conn)


# =============================
# ROTAS DE CARGOS
# =============================
@app.route("/cargos")
@admin_required
def listar_cargos():
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT * FROM cargos ORDER BY ordem NULLS LAST, nome")
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
            try:
                ordem = int(ordem)
            except ValueError:
                ordem = 999
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
    return render_template("cargos/novo.html")

@app.route("/cargos/editar/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_cargo(id):
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
            cursor.execute("SELECT * FROM cargos WHERE id = %s", (id,))
            dados_antigos = dict(cursor.fetchone())
            try:
                ordem = int(ordem)
            except ValueError:
                ordem = 999
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
            registrar_log("editar", "cargo", id, dados_anteriores=dados_antigos, dados_novos={"nome": nome, "sigla": sigla})
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
    cursor, conn = get_db()
    try:
        cursor.execute("SELECT * FROM cargos WHERE id = %s", (id,))
        cargo = cursor.fetchone()
        if not cargo:
            flash("Cargo não encontrado", "danger")
            return_connection(conn)
            return redirect("/cargos")
        dados = dict(cargo)
        cursor.execute("SELECT COUNT(*) as total FROM ocupacao_cargos WHERE cargo_id = %s", (id,))
        resultado = cursor.fetchone()
        if resultado and resultado["total"] > 0:
            cursor.execute("UPDATE cargos SET ativo = 0 WHERE id = %s", (id,))
            conn.commit()
            registrar_log("desativar", "cargo", id, dados_anteriores=dados)
            flash(f"Cargo '{cargo['nome']}' desativado pois está em uso.", "warning")
        else:
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
        
        
@app.route("/obreiros/<int:obreiro_id>/atribuir_cargo", methods=["POST"])
@login_required
@admin_required
def atribuir_cargo_obreiro(obreiro_id):  # Nome alterado
    """Atribui um cargo a um obreiro verificando o grau mínimo"""
    cursor, conn = get_db()
    
    try:
        conn.rollback()
        
        cargo_id = request.form.get("cargo_id")
        data_inicio = request.form.get("data_inicio", datetime.now().strftime("%Y-%m-%d"))
        observacao = request.form.get("observacao", "")
        
        if not cargo_id:
            flash("❌ Selecione um cargo!", "danger")
            return redirect(f"/obreiros/{obreiro_id}")
        
        # Buscar grau do obreiro
        cursor.execute("SELECT grau_atual, nome_completo FROM usuarios WHERE id = %s", (obreiro_id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("❌ Obreiro não encontrado!", "danger")
            return redirect("/obreiros")
        
        grau_obreiro = obreiro["grau_atual"]
        
        # Buscar grau mínimo do cargo
        cursor.execute("SELECT id, nome, sigla, grau_minimo FROM cargos WHERE id = %s", (cargo_id,))
        cargo = cursor.fetchone()
        
        if not cargo:
            flash("❌ Cargo não encontrado!", "danger")
            return redirect(f"/obreiros/{obreiro_id}")
        
        grau_minimo = cargo["grau_minimo"] if cargo["grau_minimo"] else 3
        
        # Verificar se o obreiro tem grau suficiente
        if grau_obreiro < grau_minimo:
            flash(f"❌ O obreiro tem grau {grau_obreiro}º, mas o cargo '{cargo['nome']}' exige no mínimo {grau_minimo}º Grau!", "danger")
            return redirect(f"/obreiros/{obreiro_id}")
        
        # Verificar se já está ocupando algum cargo ativo
        cursor.execute("""
            SELECT oc.id, c.nome as cargo_nome 
            FROM ocupacao_cargos oc
            JOIN cargos c ON oc.cargo_id = c.id
            WHERE oc.obreiro_id = %s AND oc.ativo = 1
        """, (obreiro_id,))
        cargo_atual = cursor.fetchone()
        
        if cargo_atual:
            # Desativar cargo atual
            cursor.execute("""
                UPDATE ocupacao_cargos 
                SET ativo = 0, data_fim = CURRENT_DATE
                WHERE obreiro_id = %s AND ativo = 1
            """, (obreiro_id,))
            flash(f"ℹ️ Cargo anterior '{cargo_atual['cargo_nome']}' foi desativado.", "info")
        
        # Atribuir novo cargo
        cursor.execute("""
            INSERT INTO ocupacao_cargos (cargo_id, obreiro_id, data_inicio, observacao, ativo)
            VALUES (%s, %s, %s, %s, 1)
        """, (cargo_id, obreiro_id, data_inicio, observacao))
        
        conn.commit()
        
        registrar_log("atribuir_cargo", "ocupacao_cargos", cursor.lastrowid, 
                     dados_novos={"obreiro_id": obreiro_id, "cargo_id": cargo_id, "cargo_nome": cargo['nome']})
        
        flash(f"✅ Cargo '{cargo['nome']}' atribuído com sucesso a {obreiro['nome_completo']}!", "success")
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao atribuir cargo: {str(e)}")
        flash(f"❌ Erro ao atribuir cargo: {str(e)}", "danger")
    finally:
        return_connection(conn)
    
    return redirect(f"/obreiros/{obreiro_id}")
    
@app.route("/obreiros/<int:obreiro_id>/vincular_cargo", methods=["POST"])
@login_required
@admin_required
def vincular_cargo_obreiro(obreiro_id):
    """Atribui um cargo a um obreiro verificando o grau mínimo"""
    cursor, conn = get_db()
    
    try:
        conn.rollback()
        
        cargo_id = request.form.get("cargo_id")
        data_inicio = request.form.get("data_inicio")
        gestao = request.form.get("gestao", "")
        
        if not cargo_id:
            flash("❌ Selecione um cargo!", "danger")
            return redirect(f"/obreiros/{obreiro_id}")
        
        if not data_inicio:
            from datetime import datetime
            data_inicio = datetime.now().strftime("%Y-%m-%d")
        
        # Buscar grau do obreiro
        cursor.execute("SELECT grau_atual, nome_completo FROM usuarios WHERE id = %s", (obreiro_id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("❌ Obreiro não encontrado!", "danger")
            return redirect("/obreiros")
        
        grau_obreiro = obreiro["grau_atual"] if obreiro["grau_atual"] else 0
        
        # Buscar grau mínimo do cargo
        cursor.execute("SELECT id, nome, sigla, grau_minimo FROM cargos WHERE id = %s", (cargo_id,))
        cargo = cursor.fetchone()
        
        if not cargo:
            flash("❌ Cargo não encontrado!", "danger")
            return redirect(f"/obreiros/{obreiro_id}")
        
        grau_minimo = cargo["grau_minimo"] if cargo["grau_minimo"] else 3
        
        # Verificar se o obreiro tem grau suficiente
        if grau_obreiro < grau_minimo:
            flash(f"❌ O obreiro tem grau {grau_obreiro}º, mas o cargo '{cargo['nome']}' exige no mínimo {grau_minimo}º Grau!", "danger")
            return redirect(f"/obreiros/{obreiro_id}")
        
        # Verificar se já está ocupando algum cargo ativo (opcional)
        cursor.execute("""
            SELECT oc.id, c.nome as cargo_nome 
            FROM ocupacao_cargos oc
            JOIN cargos c ON oc.cargo_id = c.id
            WHERE oc.obreiro_id = %s AND oc.ativo = 1
        """, (obreiro_id,))
        cargo_atual = cursor.fetchone()
        
        if cargo_atual:
            # Desativar cargo atual
            cursor.execute("""
                UPDATE ocupacao_cargos 
                SET ativo = 0, data_fim = CURRENT_DATE
                WHERE obreiro_id = %s AND ativo = 1
            """, (obreiro_id,))
            flash(f"ℹ️ Cargo anterior '{cargo_atual['cargo_nome']}' foi desativado.", "info")
        
        # Atribuir novo cargo
        cursor.execute("""
            INSERT INTO ocupacao_cargos (cargo_id, obreiro_id, data_inicio, gestao, ativo)
            VALUES (%s, %s, %s, %s, 1)
        """, (cargo_id, obreiro_id, data_inicio, gestao))
        
        conn.commit()
        
        registrar_log("atribuir_cargo", "ocupacao_cargos", cursor.lastrowid, 
                     dados_novos={"obreiro_id": obreiro_id, "cargo_id": cargo_id, "cargo_nome": cargo['nome']})
        
        flash(f"✅ Cargo '{cargo['nome']}' atribuído com sucesso a {obreiro['nome_completo']}!", "success")
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao atribuir cargo: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f"❌ Erro ao atribuir cargo: {str(e)}", "danger")
    finally:
        return_connection(conn)
    
    return redirect(f"/obreiros/{obreiro_id}")    

# =============================
# ROTAS DE GRAUS
# =============================
@app.route("/graus")
@admin_required
def listar_graus():
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
            cursor.execute("SELECT * FROM graus WHERE id = %s", (id,))
            dados_antigos = dict(cursor.fetchone())
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
            registrar_log("editar", "grau", id, dados_anteriores=dados_antigos, dados_novos={"nome": nome, "nivel": nivel})
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
    cursor, conn = get_db()
    try:
        cursor.execute("SELECT * FROM graus WHERE id = %s", (id,))
        grau = cursor.fetchone()
        if not grau:
            flash("Grau não encontrado", "danger")
            return_connection(conn)
            return redirect("/graus")
        dados = dict(grau)
        cursor.execute("SELECT COUNT(*) as total FROM historico_graus WHERE grau_id = %s", (id,))
        resultado = cursor.fetchone()
        if resultado and resultado["total"] > 0:
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
        registrar_log("editar", "tipo_ausencia", id, dados_anteriores=dados_antigos, dados_novos={"nome": nome})
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
    cursor.execute("SELECT COUNT(*) as total FROM presenca WHERE tipo_ausencia = %s", (tipo["nome"],))
    resultado = cursor.fetchone()
    if resultado and resultado["total"] > 0:
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

# ============================================
# ROTAS PARA CONDECORAÇÕES
# ============================================

@app.route("/obreiros/<int:obreiro_id>/condecoracoes")
@login_required
def listar_condecoracoes(obreiro_id):
    """Lista todas as condecorações de um obreiro"""
    cursor, conn = get_db()
    
    try:
        # Buscar dados do obreiro
        cursor.execute("""
            SELECT id, nome_completo, foto, usuario, cim_numero 
            FROM usuarios WHERE id = %s
        """, (obreiro_id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("Obreiro não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        # Buscar todas as condecorações do obreiro
        cursor.execute("""
            SELECT c.*, 
                   t.nome as tipo_nome, 
                   t.nivel, 
                   t.cor, 
                   t.icone,
                   t.descricao as tipo_descricao
            FROM condecoracoes_obreiro c
            JOIN tipos_condecoracoes t ON c.tipo_id = t.id
            WHERE c.obreiro_id = %s
            ORDER BY t.nivel DESC, c.data_concessao DESC
        """, (obreiro_id,))
        
        condecoracoes = cursor.fetchall()
        
        # Para cada condecoração, formatar os dados
        for c in condecoracoes:
            if 'nome' not in c:
                c['nome'] = c.get('tipo_nome', 'Condecoração')
            if 'nivel' not in c:
                c['nivel'] = 1
            if 'cor' not in c:
                c['cor'] = '#ffc107'
            if 'icone' not in c:
                c['icone'] = 'bi-award'
        
        return_connection(conn)
        
        return render_template("obreiros/condecoracoes/lista.html", 
                              obreiro=obreiro,
                              condecoracoes=condecoracoes,
                              now=datetime.now())
                              
    except Exception as e:
        print(f"❌ Erro ao listar condecorações: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            return_connection(conn)
        flash(f"Erro ao carregar condecorações: {str(e)}", "danger")
        return redirect(f"/obreiros/{obreiro_id}")


@app.route("/obreiros/<int:obreiro_id>/condecoracoes/<int:condecoracao_id>")
@login_required
def detalhes_condecoracao(obreiro_id, condecoracao_id):
    """Visualiza os detalhes de uma condecoração específica"""
    cursor, conn = get_db()
    
    try:
        # Buscar dados do obreiro
        cursor.execute("""
            SELECT id, nome_completo, foto, usuario, cim_numero 
            FROM usuarios WHERE id = %s
        """, (obreiro_id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("Obreiro não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        # Buscar a condecoração específica
        cursor.execute("""
            SELECT c.*, 
                   t.nome as tipo_nome, 
                   t.nivel, 
                   t.cor, 
                   t.icone,
                   t.descricao as tipo_descricao
            FROM condecoracoes_obreiro c
            JOIN tipos_condecoracoes t ON c.tipo_id = t.id
            WHERE c.id = %s AND c.obreiro_id = %s
        """, (condecoracao_id, obreiro_id))
        
        condecoracao = cursor.fetchone()
        
        if not condecoracao:
            flash("Condecoração não encontrada", "danger")
            return_connection(conn)
            return redirect(f"/obreiros/{obreiro_id}/condecoracoes")
        
        # Buscar quem concedeu
        concedente = None
        if condecoracao.get('concedido_por'):
            cursor.execute("""
                SELECT id, nome_completo, usuario, foto 
                FROM usuarios WHERE id = %s
            """, (condecoracao['concedido_por'],))
            concedente = cursor.fetchone()
        
        return_connection(conn)
        
        return render_template("obreiros/condecoracoes/detalhes.html", 
                              obreiro=obreiro,
                              condecoracao=condecoracao,
                              concedente=concedente,
                              now=datetime.now())
                              
    except Exception as e:
        print(f"❌ Erro ao visualizar condecoração: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            return_connection(conn)
        flash(f"Erro ao carregar detalhes: {str(e)}", "danger")
        return redirect(f"/obreiros/{obreiro_id}/condecoracoes")


@app.route("/obreiros/<int:obreiro_id>/condecoracoes/nova", methods=["GET", "POST"])
@login_required
def nova_condecoracao(obreiro_id):
    """Adiciona uma nova condecoração para um obreiro"""
    if session.get('tipo') != 'admin':
        flash("Apenas administradores podem adicionar condecorações", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    
    cursor, conn = get_db()
    
    try:
        # Buscar dados do obreiro
        cursor.execute("SELECT id, nome_completo FROM usuarios WHERE id = %s", (obreiro_id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("Obreiro não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        if request.method == "POST":
            tipo_id = request.form.get("tipo_id")
            data_concessao = request.form.get("data_concessao")
            data_validade = request.form.get("data_validade")
            motivo = request.form.get("motivo")
            numero_registro = request.form.get("numero_registro")
            observacoes = request.form.get("observacoes")
            
            if not tipo_id or not data_concessao:
                flash("Tipo de condecoração e data são obrigatórios", "danger")
                return redirect(f"/obreiros/{obreiro_id}/condecoracoes/nova")
            
            data_validade = data_validade if data_validade and data_validade.strip() else None
            
            # CORREÇÃO: Removemos o campo 'id' da inserção e usamos RETURNING
            cursor.execute("""
                INSERT INTO condecoracoes_obreiro 
                (obreiro_id, tipo_id, data_concessao, data_validade, concedido_por, 
                 motivo, numero_registro, observacoes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (obreiro_id, tipo_id, data_concessao, data_validade, session.get('user_id'),
                  motivo, numero_registro, observacoes))
            
            resultado = cursor.fetchone()
            if resultado:
                condecoracao_id = resultado['id']
            else:
                flash("Erro ao obter ID da condecoração", "danger")
                return redirect(f"/obreiros/{obreiro_id}/condecoracoes/nova")
            
            conn.commit()
            
            registrar_log(
                acao="criar",
                entidade="condecoracao",
                entidade_id=condecoracao_id,
                dados_anteriores=None,
                dados_novos={"obreiro_id": obreiro_id, "tipo_id": tipo_id}
            )
            
            flash(f"Condecoração concedida com sucesso para {obreiro['nome_completo']}!", "success")
            return_connection(conn)
            return redirect(f"/obreiros/{obreiro_id}/condecoracoes")
        
        # GET - Buscar tipos de condecoração disponíveis
        # CORREÇÃO: Usar TRUE em vez de 1
        cursor.execute("""
            SELECT id, nome, nivel, cor, icone, descricao 
            FROM tipos_condecoracoes 
            WHERE ativo = true
            ORDER BY nivel, nome
        """)
        tipos = cursor.fetchall()
        
        return_connection(conn)
        
        return render_template("obreiros/condecoracoes/nova.html", 
                              obreiro=obreiro,
                              tipos=tipos)
                              
    except Exception as e:
        print(f"❌ Erro ao criar condecoração: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            return_connection(conn)
        flash(f"Erro ao criar condecoração: {str(e)}", "danger")
        return redirect(f"/obreiros/{obreiro_id}/condecoracoes")


@app.route("/obreiros/<int:obreiro_id>/condecoracoes/<int:condecoracao_id>/editar", methods=["GET", "POST"])
@login_required
def editar_condecoracao(obreiro_id, condecoracao_id):
    """Edita uma condecoração existente"""
    if session.get('tipo') != 'admin':
        flash("Apenas administradores podem editar condecorações", "danger")
        return redirect(f"/obreiros/{obreiro_id}/condecoracoes")
    
    cursor, conn = get_db()
    
    try:
        # Buscar dados do obreiro
        cursor.execute("SELECT id, nome_completo FROM usuarios WHERE id = %s", (obreiro_id,))
        obreiro = cursor.fetchone()
        
        if not obreiro:
            flash("Obreiro não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        
        # Buscar a condecoração
        cursor.execute("""
            SELECT c.*, t.nome as tipo_nome, t.nivel, t.cor, t.icone
            FROM condecoracoes_obreiro c
            JOIN tipos_condecoracoes t ON c.tipo_id = t.id
            WHERE c.id = %s AND c.obreiro_id = %s
        """, (condecoracao_id, obreiro_id))
        
        condecoracao = cursor.fetchone()
        
        if not condecoracao:
            flash("Condecoração não encontrada", "danger")
            return_connection(conn)
            return redirect(f"/obreiros/{obreiro_id}/condecoracoes")
        
        if request.method == "POST":
            tipo_id = request.form.get("tipo_id")
            data_concessao = request.form.get("data_concessao")
            data_validade = request.form.get("data_validade")
            motivo = request.form.get("motivo")
            numero_registro = request.form.get("numero_registro")
            observacoes = request.form.get("observacoes")
            
            if not tipo_id or not data_concessao:
                flash("Tipo de condecoração e data são obrigatórios", "danger")
                return redirect(f"/obreiros/{obreiro_id}/condecoracoes/{condecoracao_id}/editar")
            
            data_validade = data_validade if data_validade and data_validade.strip() else None
            
            # Salvar dados antigos para o log
            dados_antigos = dict(condecoracao)
            
            cursor.execute("""
                UPDATE condecoracoes_obreiro 
                SET tipo_id = %s,
                    data_concessao = %s,
                    data_validade = %s,
                    motivo = %s,
                    numero_registro = %s,
                    observacoes = %s
                WHERE id = %s
            """, (tipo_id, data_concessao, data_validade, motivo, numero_registro, observacoes, condecoracao_id))
            
            conn.commit()
            
            registrar_log(
                acao="editar",
                entidade="condecoracao",
                entidade_id=condecoracao_id,
                dados_anteriores=dados_antigos,
                dados_novos={"tipo_id": tipo_id, "data_concessao": data_concessao}
            )
            
            flash("Condecoração atualizada com sucesso!", "success")
            return_connection(conn)
            return redirect(f"/obreiros/{obreiro_id}/condecoracoes")
        
        # GET - Buscar tipos de condecoração disponíveis
        # CORREÇÃO: Usar TRUE em vez de 1
        cursor.execute("""
            SELECT id, nome, nivel, cor, icone, descricao 
            FROM tipos_condecoracoes 
            WHERE ativo = true
            ORDER BY nivel, nome
        """)
        tipos = cursor.fetchall()
        
        # Buscar quem concedeu
        concedente = None
        if condecoracao.get('concedido_por'):
            cursor.execute("""
                SELECT id, nome_completo, usuario 
                FROM usuarios WHERE id = %s
            """, (condecoracao['concedido_por'],))
            concedente = cursor.fetchone()
        
        return_connection(conn)
        
        return render_template("obreiros/condecoracoes/editar.html", 
                              obreiro=obreiro,
                              condecoracao=condecoracao,
                              tipos=tipos,
                              concedente=concedente)
                              
    except Exception as e:
        print(f"❌ Erro ao editar condecoração: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            return_connection(conn)
        flash(f"Erro ao editar condecoração: {str(e)}", "danger")
        return redirect(f"/obreiros/{obreiro_id}/condecoracoes")


@app.route("/api/condecoracoes/<int:condecoracao_id>/excluir", methods=["DELETE"])
@login_required
def excluir_condecoracao(condecoracao_id):
    """Exclui uma condecoração (apenas admin)"""
    if session.get('tipo') != 'admin':
        return jsonify({"success": False, "error": "Acesso negado"}), 403
    
    cursor, conn = get_db()
    
    try:
        # Buscar dados para o log
        cursor.execute("""
            SELECT c.*, t.nome as tipo_nome 
            FROM condecoracoes_obreiro c
            JOIN tipos_condecoracoes t ON c.tipo_id = t.id
            WHERE c.id = %s
        """, (condecoracao_id,))
        condecoracao = cursor.fetchone()
        
        if not condecoracao:
            return jsonify({"success": False, "error": "Condecoração não encontrada"}), 404
        
        # Excluir
        cursor.execute("DELETE FROM condecoracoes_obreiro WHERE id = %s", (condecoracao_id,))
        conn.commit()
        
        # Registrar log
        registrar_log(
            acao="excluir",
            entidade="condecoracao",
            entidade_id=condecoracao_id,
            dados_anteriores={"tipo": condecoracao['tipo_nome'], "data": condecoracao['data_concessao']},
            dados_novos=None
        )
        
        return_connection(conn)
        return jsonify({"success": True, "message": "Condecoração excluída com sucesso"})
        
    except Exception as e:
        conn.rollback()
        return_connection(conn)
        return jsonify({"success": False, "error": str(e)}), 500

        

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
    return render_template("auditoria/logs.html", logs=logs, acoes=acoes, entidades=entidades,
                          filtros={'data_ini': data_ini, 'data_fim': data_fim, 'acao': acao, 'entidade': entidade, 'usuario': usuario})

@app.route("/auditoria/<int:id>")
@login_required
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
    
    # Corrigir o caminho do template
    return render_template("auditoria/detalhes_log.html", log=log)

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
    output = StringIO()
    writer = csv.writer(output, delimiter=';')
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
    return Response(output.getvalue(), mimetype="text/csv; charset=utf-8",
                   headers={"Content-Disposition": f"attachment;filename=logs_auditoria_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"})

# =============================
# ROTAS DE CONFIGURAÇÃO DE E-MAIL
# =============================
import json
import os
import requests  # <-- IMPORT ADICIONADO!
import resend
from datetime import datetime
from flask import render_template, request, flash, redirect, jsonify

# Configurar Resend globalmente
RESEND_API_KEY = os.getenv('RESEND_API_KEY')
EMAIL_FROM_DEFAULT = os.getenv('EMAIL_FROM', 'contato@juramelo.com.br')

# DEBUG - Verificar se a chave foi carregada
print("=" * 50)
print("🔧 CONFIGURAÇÃO DE E-MAIL")
print(f"RESEND_API_KEY presente: {RESEND_API_KEY is not None}")
if RESEND_API_KEY:
    print(f"RESEND_API_KEY (primeiros 10 caracteres): {RESEND_API_KEY[:10]}...")
    print(f"Tamanho da chave: {len(RESEND_API_KEY)}")
else:
    print("⚠️ RESEND_API_KEY NÃO ENCONTRADA no ambiente!")
print("=" * 50)

if RESEND_API_KEY:
    try:
        resend.api_key = RESEND_API_KEY
        print("✅ Resend configurado com sucesso")
    except Exception as e:
        print(f"❌ Erro ao configurar Resend: {e}")
else:
    print("⚠️ RESEND_API_KEY não configurada - e-mails não serão enviados")


# =============================
# FUNÇÃO PRINCIPAL DE ENVIO
# =============================
def enviar_email_resend(destinatario, assunto, conteudo_html, conteudo_texto=None):
    """Envia e-mail usando a API do Resend"""
    api_key = os.environ.get('RESEND_API_KEY', '')
    
    if not api_key:
        print("❌ RESEND_API_KEY não configurada")
        return {'success': False, 'message': 'API key não configurada'}
    
    # Verificar se o conteúdo HTML não está vazio
    if not conteudo_html:
        conteudo_html = "<p>Conteúdo do e-mail não disponível.</p>"
    
    url = "https://api.resend.com/emails"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # IMPORTANTE: Usar domínio verificado (juramelo.com.br)
    # NÃO usar gmail.com, hotmail.com, etc.
    from_email = "ARLS Bicentenário <contato@juramelo.com.br>"
    
    data = {
        "from": from_email,
        "to": [destinatario],
        "subject": assunto,
        "html": conteudo_html
    }
    
    if conteudo_texto:
        data["text"] = conteudo_texto
    
    print(f"📧 Enviando e-mail para: {destinatario}")
    print(f"📧 Assunto: {assunto}")
    print(f"📧 From: {from_email}")
    
    try:
        response = requests.post(url, headers=headers, json=data)
        print(f"📧 Resposta Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            return {'success': True, 'message': 'E-mail enviado com sucesso', 'id': result.get('id')}
        else:
            return {'success': False, 'message': f'Erro {response.status_code}: {response.text}'}
    except Exception as e:
        print(f"❌ Exceção: {e}")
        return {'success': False, 'message': str(e)}


# =============================
# FUNÇÃO DE ENVIO PARA REUNIÕES (ÚNICA)
# =============================
def enviar_email_reuniao(destinatario, nome_destinatario, dados_reuniao):
    """Envia e-mail de convocação para reunião via Resend usando templates"""
    
    reuniao_id = dados_reuniao.get('id', '')
    assunto = f"📅 Convite: {dados_reuniao.get('titulo', 'Nova Reunião')} - ARLS Bicentenário"
    
    # Formatar horário
    hora_termino = dados_reuniao.get('hora_termino')
    horario = dados_reuniao.get('hora_inicio')
    if hora_termino:
        horario = f"{dados_reuniao.get('hora_inicio')} às {hora_termino}"
    
    dados_reuniao['horario_formatado'] = horario
    link_reuniao = f"https://www.juramelo.com.br/reunioes/{reuniao_id}" if reuniao_id else "#"
    dados_reuniao['link_reuniao'] = link_reuniao
    
    # Carrega o template HTML
    try:
        html_content = render_template('email/reuniao_agendada.html', 
                                       nome=nome_destinatario, 
                                       reuniao=dados_reuniao)
    except Exception as e:
        print(f"Erro ao carregar template HTML: {e}")
        html_content = gerar_html_fallback(nome_destinatario, dados_reuniao)
    
    # Envia o e-mail via Resend
    return enviar_email_resend(destinatario, assunto, html_content)


def gerar_html_fallback(nome_destinatario, dados_reuniao):
    """Fallback em caso de erro no template"""
    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body>
        <h2>Olá {nome_destinatario},</h2>
        <p>Você foi convidado para uma reunião:</p>
        <p><strong>{dados_reuniao.get('titulo')}</strong></p>
        <p>📅 Data: {dados_reuniao.get('data')}</p>
        <p>⏰ Horário: {dados_reuniao.get('horario_formatado')}</p>
        <p>📍 Local: {dados_reuniao.get('local')}</p>
        <p>🔗 Link: <a href="{dados_reuniao.get('link_reuniao')}">Ver detalhes</a></p>
        <p>Atenciosamente,<br>Secretaria do Sistema Maçônico</p>
    </body>
    </html>
    """


# =============================
# ROTA: CONFIGURAÇÃO DE E-MAIL
# =============================
@app.route("/config/email", methods=["GET", "POST"])
@admin_required
def config_email():
    cursor, conn = get_db()
    
    if request.method == "POST":
        server = request.form.get("server", "")
        port = request.form.get("port", "")
        use_tls = 1 if request.form.get("use_tls") else 0
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        sender = request.form.get("sender", EMAIL_FROM_DEFAULT)
        sender_name = request.form.get("sender_name", "Sistema Maçônico")
        active = 1 if request.form.get("active") else 0
        
        if not sender:
            flash("Preencha o e-mail remetente", "danger")
        else:
            try:
                if active:
                    cursor.execute("UPDATE email_settings SET active = 0")
                
                cursor.execute("""
                    INSERT INTO email_settings 
                    (server, port, use_tls, username, password, sender, sender_name, active)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (server, port, use_tls, username, password, sender, sender_name, active))
                
                conn.commit()
                flash("Configuração de e-mail salva com sucesso! (Usando Resend)", "success")
            except Exception as e:
                flash(f"Erro ao salvar configuração: {str(e)}", "danger")
                conn.rollback()
    
    cursor.execute("SELECT * FROM email_settings WHERE active = 1 ORDER BY id DESC LIMIT 1")
    config = cursor.fetchone()
    return_connection(conn)
    
    return render_template("admin/config_email.html", config=config)


# =============================
# ROTA: TESTAR E-MAIL (VIA RESEND)
# =============================
@app.route("/config/email/testar", methods=["POST"])
@admin_required
def testar_email():
    email_teste = request.form.get("email_teste")
    
    if not email_teste:
        flash("Informe um e-mail para teste", "danger")
        return redirect("/config/email")
    
    if not RESEND_API_KEY:
        flash("Resend não configurado. Adicione RESEND_API_KEY nas variáveis de ambiente.", "danger")
        return redirect("/config/email")
    
    assunto = "✅ Teste de Configuração - ARLS Bicentenário"
    
    dados_template = {
        'nome': 'Irmão',
        'remetente': 'contato@juramelo.com.br',
        'nome_remetente': 'ARLS Bicentenário',
        'data_hora': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        'ano': datetime.now().year
    }
    
    # DEBUG: Verificar se o arquivo do template existe
    import os
    template_path = os.path.join('templates', 'email', 'teste.html')
    print(f"🔍 Verificando template em: {template_path}")
    print(f"📁 Arquivo existe? {os.path.exists(template_path)}")
    
    try:
        conteudo_html = render_template('email/teste.html', **dados_template)
        print(f"✅ Template carregado com sucesso! Tamanho: {len(conteudo_html)} caracteres")
        print(f"📧 Primeiros 200 caracteres do HTML: {conteudo_html[:200]}...")
    except Exception as e:
        print(f"❌ Erro ao carregar template: {e}")
        import traceback
        traceback.print_exc()
        # Fallback
        conteudo_html = f"""
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"></head>
        <body>
            <h2>✅ Teste de E-mail</h2>
            <p>Olá, esta é uma mensagem de teste do Sistema Maçônico.</p>
            <p>Data: {dados_template['data_hora']}</p>
        </body>
        </html>
        """
    
    resultado = enviar_email_resend(
        destinatario=email_teste,
        assunto=assunto,
        conteudo_html=conteudo_html
    )
    
    if resultado['success']:
        flash(f"✅ E-mail de teste enviado com sucesso para {email_teste}!", "success")
    else:
        flash(f"❌ Falha ao enviar e-mail: {resultado['message']}", "danger")
    
    return redirect("/config/email")

# =============================
# ROTA: STATUS DO RESEND (DIAGNÓSTICO)
# =============================
@app.route("/config/email/status")
@admin_required
def email_status():
    status = {
        "resend_configurado": bool(RESEND_API_KEY),
        "email_from": EMAIL_FROM_DEFAULT,
        "dominio_verificado": "juramelo.com.br",
        "metodo_envio": "Resend API"
    }
    
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT * FROM email_settings WHERE active = 1 ORDER BY id DESC LIMIT 1")
        config = cursor.fetchone()
        return_connection(conn)
        
        if config:
            status["configuracao_ativa"] = {
                "remetente": config['sender'],
                "nome_remetente": config['sender_name']
            }
        else:
            status["configuracao_ativa"] = "Nenhuma configuração ativa no banco"
    except Exception as e:
        status["erro_busca_config"] = str(e)
    
    return jsonify(status)
    
# =============================
# ROTAS DE WHATSAPP
# =============================

@app.route("/enviar_para_grupo", methods=["POST"])
@login_required
def enviar_para_grupo():
    """Envia mensagem para um grupo do WhatsApp"""
    try:
        grupo_id = request.form.get('grupo_id')
        mensagem = request.form.get('mensagem')
        nome_grupo = request.form.get('nome_grupo', '')
        data_agendamento = request.form.get('data_agendamento')
        hora_agendamento = request.form.get('hora_agendamento')
        recorrencia = request.form.get('recorrencia', '')
        agendar = request.form.get('agendar')
        
        if not grupo_id or not mensagem:
            flash('Preencha o ID do grupo e a mensagem', 'danger')
            return redirect('/whatsapp_config')
        
        # Se for agendamento
        if agendar and data_agendamento and hora_agendamento:
            data_hora = f"{data_agendamento} {hora_agendamento}"
            
            cursor, conn = get_db()
            cursor.execute("""
                INSERT INTO mensagens_agendadas 
                (grupo_id, mensagem, nome_grupo, data_envio, recorrencia, criado_por, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'agendado')
            """, (grupo_id, mensagem, nome_grupo, data_hora, recorrencia, session['user_id']))
            return_connection(conn)
            
            flash(f'Mensagem agendada para {data_agendamento} às {hora_agendamento}', 'success')
            return redirect('/whatsapp_config')
        
        # Envio imediato
        # Usar a biblioteca whatsapp-web.js ou selenium para enviar
        resultado = enviar_mensagem_grupo(grupo_id, mensagem)
        
        if resultado['success']:
            # Salvar grupo na lista de grupos salvos
            cursor, conn = get_db()
            cursor.execute("""
                INSERT INTO grupos_whatsapp (grupo_id, nome_grupo, ultimo_envio, criado_por)
                VALUES (%s, %s, NOW(), %s)
                ON CONFLICT (grupo_id) DO UPDATE SET 
                    nome_grupo = EXCLUDED.nome_grupo,
                    ultimo_envio = NOW()
            """, (grupo_id, nome_grupo, session['user_id']))
            return_connection(conn)
            
            flash('✅ Mensagem enviada para o grupo com sucesso!', 'success')
        else:
            flash(f'❌ Erro ao enviar: {resultado["error"]}', 'danger')
            
    except Exception as e:
        flash(f'Erro ao enviar mensagem: {str(e)}', 'danger')
    
    return redirect('/whatsapp_config')

def enviar_mensagem_grupo(grupo_id, mensagem):
    """Função para enviar mensagem para grupo via WhatsApp Web"""
    # Aqui você implementa a lógica com selenium ou whatsapp-web.js
    # Exemplo com selenium:
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        import time
        
        # Configurar driver (use o caminho correto do seu chromedriver)
        driver = webdriver.Chrome()
        driver.get('https://web.whatsapp.com')
        
        # Aguardar QR Code escanear
        time.sleep(10)
        
        # Buscar grupo pelo ID
        search_box = driver.find_element(By.XPATH, '//div[@contenteditable="true"][@data-tab="3"]')
        search_box.send_keys(grupo_id)
        time.sleep(2)
        
        # Clicar no grupo
        grupo = driver.find_element(By.XPATH, f'//span[@title="{grupo_id}"]')
        grupo.click()
        time.sleep(2)
        
        # Digitar mensagem
        message_box = driver.find_element(By.XPATH, '//div[@contenteditable="true"][@data-tab="10"]')
        message_box.send_keys(mensagem)
        message_box.send_keys(Keys.ENTER)
        
        time.sleep(2)
        driver.quit()
        
        return {'success': True}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.route("/config/whatsapp", methods=["GET", "POST"])
@admin_required
def config_whatsapp():
    cursor, conn = get_db()
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
            SET notificar_ausencia = %s, notificar_nova_reuniao = %s, notificar_comunicado = %s, 
                lembrete_reuniao = %s, grupo_id = %s, updated_at = CURRENT_TIMESTAMP 
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
# ROTAS DE PERMISSÕES
# =============================
@app.route("/admin/permissoes")
@admin_required
def gerenciar_permissoes():
    cursor, conn = get_db()
    
    # Buscar graus
    cursor.execute("SELECT * FROM graus WHERE nivel IN (1, 2, 3) AND ativo = 1 ORDER BY nivel")
    graus = cursor.fetchall()
    
    # Buscar usuários
    cursor.execute("SELECT id, usuario, nome_completo, grau_atual, tipo FROM usuarios WHERE ativo = 1 ORDER BY nome_completo")
    usuarios = cursor.fetchall()
    
    # Buscar permissões com módulos (usando o módulo correto)
    cursor.execute("""
        SELECT 
            m.id as modulo_id, 
            m.nome as modulo_nome, 
            m.icone as modulo_icone,
            m.ordem as modulo_ordem,
            p.id as permissao_id, 
            p.nome as permissao_nome, 
            p.codigo, 
            p.descricao,
            p.ordem as permissao_ordem
        FROM modulos m
        JOIN permissoes p ON m.id = p.modulo_id
        WHERE m.ativo = 1
        ORDER BY m.ordem, p.ordem
    """)
    permissoes = cursor.fetchall()
    
    # Organizar permissões por módulo
    permissoes_por_modulo = {}
    for p in permissoes:
        modulo_nome = p['modulo_nome']
        
        if modulo_nome not in permissoes_por_modulo:
            permissoes_por_modulo[modulo_nome] = {
                'icone': p['modulo_icone'],
                'permissoes': []
            }
        
        permissoes_por_modulo[modulo_nome]['permissoes'].append({
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
    
    # Buscar permissões por usuário
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
    cursor, conn = get_db()
    
    try:
        # Remover permissões existentes
        cursor.execute("DELETE FROM permissoes_grau WHERE grau_id = %s", (grau_id,))
        
        # Adicionar novas permissões
        permissoes = request.form.getlist("permissoes")
        
        for permissao_id in permissoes:
            # Obter próximo ID para permissoes_grau
            cursor.execute("SELECT MAX(id) FROM permissoes_grau")
            max_id = cursor.fetchone()['max']
            next_id = (max_id or 0) + 1
            
            cursor.execute("""
                INSERT INTO permissoes_grau (id, grau_id, permissao_id)
                VALUES (%s, %s, %s)
            """, (next_id, grau_id, permissao_id))
        
        conn.commit()
        flash("Permissões do grau atualizadas com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao salvar permissões do grau: {e}")
        conn.rollback()
        flash(f"Erro ao salvar permissões: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect("/admin/permissoes")

@app.route("/admin/permissoes/usuario/<int:usuario_id>", methods=["POST"])
@admin_required
def salvar_permissoes_usuario(usuario_id):
    cursor, conn = get_db()
    
    try:
        # Remover permissões existentes
        cursor.execute("DELETE FROM permissoes_usuario WHERE usuario_id = %s", (usuario_id,))
        
        # Adicionar permissões extras
        permissoes_extra = request.form.getlist("permissoes_extra")
        for permissao_id in permissoes_extra:
            # Obter próximo ID para permissoes_usuario
            cursor.execute("SELECT MAX(id) FROM permissoes_usuario")
            max_id = cursor.fetchone()['max']
            next_id = (max_id or 0) + 1
            
            cursor.execute("""
                INSERT INTO permissoes_usuario (id, usuario_id, permissao_id, permitido)
                VALUES (%s, %s, %s, %s)
            """, (next_id, usuario_id, permissao_id, 1))
        
        # Adicionar permissões bloqueadas
        permissoes_bloqueadas = request.form.getlist("permissoes_bloqueadas")
        for permissao_id in permissoes_bloqueadas:
            # Obter próximo ID para permissoes_usuario
            cursor.execute("SELECT MAX(id) FROM permissoes_usuario")
            max_id = cursor.fetchone()['max']
            next_id = (max_id or 0) + 1
            
            cursor.execute("""
                INSERT INTO permissoes_usuario (id, usuario_id, permissao_id, permitido)
                VALUES (%s, %s, %s, %s)
            """, (next_id, usuario_id, permissao_id, 0))
        
        conn.commit()
        flash("Permissões do usuário atualizadas com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao salvar permissões do usuário: {e}")
        conn.rollback()
        flash(f"Erro ao salvar permissões: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect("/admin/permissoes")

@app.route("/lojas/<int:id>")
@login_required
def detalhes_loja(id):
    cursor, conn = get_db()
    
    try:
        # Buscar dados da loja
        cursor.execute("SELECT * FROM lojas WHERE id = %s", (id,))
        loja = cursor.fetchone()
        
        if not loja:
            flash("Loja não encontrada", "danger")
            return_connection(conn)
            return redirect("/lojas")
        
        # Buscar reuniões diretamente da tabela reunioes (NÃO da coluna JSON da loja)
        cursor.execute("""
            SELECT id, titulo, tipo, grau, data, hora_inicio, hora_termino, 
                   local, pauta, observacoes, status
            FROM reunioes 
            WHERE loja_id = %s 
            ORDER BY data DESC, hora_inicio DESC
            LIMIT 10
        """, (id,))
        
        reunioes = []
        for row in cursor.fetchall():
            reunioes.append(dict(row))
        
        # Buscar obreiros desta loja
        cursor.execute("""
            SELECT u.id, u.usuario, u.nome_completo, u.grau_atual,
                   (SELECT c.nome FROM ocupacao_cargos oc 
                    LEFT JOIN cargos c ON oc.cargo_id = c.id 
                    WHERE oc.obreiro_id = u.id AND oc.ativo = 1 
                    LIMIT 1) as cargo
            FROM usuarios u
            WHERE u.loja_nome = %s AND u.ativo = 1
            ORDER BY u.grau_atual DESC, u.nome_completo
        """, (loja['nome'],))
        
        obreiros = []
        for row in cursor.fetchall():
            obreiros.append(dict(row))
        
        return_connection(conn)
        
        return render_template("lojas/detalhes.html", 
                              loja=loja, 
                              reunioes=reunioes, 
                              obreiros=obreiros)
                              
    except Exception as e:
        print(f"Erro ao carregar detalhes da loja: {e}")
        if conn:
            return_connection(conn)
        flash(f"Erro ao carregar detalhes: {str(e)}", "danger")
        return redirect("/lojas")
                          
# =============================
# ROTAS DE SUGESTÕES
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
    return render_template("sugestoes/lista.html", sugestoes=sugestoes, categorias=categorias,
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
                registrar_log("criar_sugestao", "sugestao", sugestao_id, dados_novos={"titulo": titulo, "categoria": categoria})
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
    cursor.execute("""
        SELECT c.*, u.nome_completo as autor_nome
        FROM comentarios_sugestao c
        JOIN usuarios u ON c.autor_id = u.id
        WHERE c.sugestao_id = %s
        ORDER BY c.data_comentario DESC
    """, (id,))
    comentarios = cursor.fetchall()
    return_connection(conn)
    return render_template("sugestoes/visualizar.html", sugestao=sugestao, comentarios=comentarios)

@app.route("/sugestoes/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_sugestao(id):
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
            cursor.execute("SELECT * FROM sugestoes WHERE id = %s", (id,))
            dados_antigos = dict(cursor.fetchone())
            cursor.execute("""
                UPDATE sugestoes 
                SET titulo = %s, descricao = %s, categoria = %s, prioridade = %s,
                    data_atualizacao = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (titulo, descricao, categoria, prioridade, id))
            conn.commit()
            registrar_log("editar_sugestao", "sugestao", id, dados_anteriores=dados_antigos, dados_novos={"titulo": titulo, "categoria": categoria})
            flash("Sugestão atualizada com sucesso!", "success")
            return_connection(conn)
            return redirect(f"/sugestoes/{id}")
        except Exception as e:
            flash(f"Erro ao atualizar sugestão: {str(e)}", "danger")
            conn.rollback()
            return_connection(conn)
            return redirect(f"/sugestoes/{id}/editar")
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
        cursor.execute("SELECT COUNT(*) as total FROM votos_sugestao WHERE sugestao_id = %s AND usuario_id = %s", (id, session["user_id"]))
        resultado = cursor.fetchone()
        if resultado and resultado["total"] > 0:
            flash("Você já votou nesta sugestão!", "warning")
        else:
            cursor.execute("INSERT INTO votos_sugestao (sugestao_id, usuario_id) VALUES (%s, %s)", (id, session["user_id"]))
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
        cursor.execute("SELECT status FROM sugestoes WHERE id = %s", (id,))
        status_antigo = cursor.fetchone()
        cursor.execute("UPDATE sugestoes SET status = %s, data_atualizacao = CURRENT_TIMESTAMP WHERE id = %s", (status, id))
        conn.commit()
        cursor.execute("""
            INSERT INTO comentarios_sugestao (sugestao_id, autor_id, comentario)
            VALUES (%s, %s, %s)
        """, (id, session["user_id"], f"Status alterado de '{status_antigo['status']}' para '{status}'. {observacao}"))
        conn.commit()
        registrar_log("atualizar_status_sugestao", "sugestao", id, dados_anteriores={"status": status_antigo['status']}, dados_novos={"status": status})
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
    cursor.execute("""
        SELECT c.nome, COUNT(s.id) as total
        FROM categorias_sugestoes c
        LEFT JOIN sugestoes s ON c.nome = s.categoria
        GROUP BY c.nome
        ORDER BY total DESC
    """)
    por_categoria = cursor.fetchall()
    cursor.execute("SELECT prioridade, COUNT(*) as total FROM sugestoes GROUP BY prioridade")
    por_prioridade = cursor.fetchall()
    return_connection(conn)
    return render_template("sugestoes/estatisticas.html", total=total, pendentes=pendentes,
                          em_andamento=em_andamento, implementadas=implementadas, rejeitadas=rejeitadas,
                          por_categoria=por_categoria, por_prioridade=por_prioridade)

# =============================
# ROTAS DE NOTIFICAÇÕES (API)
# =============================

@app.route('/api/notificacoes')
def api_notificacoes():
    """Retorna as notificações do usuário logado"""
    try:
        if 'user_id' not in session:
            return jsonify({'success': True, 'notificacoes': [], 'nao_lidas': 0})
        
        cursor, conn = get_db()
        
        # Verificar se a tabela notificacoes existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'notificacoes'
            )
        """)
        tabela_existe = cursor.fetchone()['exists']
        
        if not tabela_existe:
            # Criar tabela se não existir
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS notificacoes (
                    id SERIAL PRIMARY KEY,
                    usuario_id INTEGER NOT NULL,
                    titulo VARCHAR(200) NOT NULL,
                    mensagem TEXT NOT NULL,
                    tipo VARCHAR(50) DEFAULT 'sistema',
                    link VARCHAR(500),
                    lida INTEGER DEFAULT 0,
                    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_leitura TIMESTAMP
                )
            """)
            conn.commit()
            notificacoes = []
            nao_lidas = 0
        else:
            try:
                cursor.execute("""
                    SELECT id, titulo, mensagem, tipo, link, lida, data_criacao as data
                    FROM notificacoes
                    WHERE usuario_id = %s
                    ORDER BY data_criacao DESC
                    LIMIT 50
                """, (session['user_id'],))
                notificacoes = cursor.fetchall()
                
                cursor.execute("""
                    SELECT COUNT(*) as total
                    FROM notificacoes
                    WHERE usuario_id = %s AND lida = 0
                """, (session['user_id'],))
                nao_lidas = cursor.fetchone()['total'] if cursor.rowcount > 0 else 0
                
            except Exception as e:
                print(f"Erro ao buscar notificações: {e}")
                notificacoes = []
                nao_lidas = 0
        
        return_connection(conn)
        
        notificacoes_list = []
        for n in notificacoes:
            notificacoes_list.append({
                'id': n['id'], 
                'titulo': n['titulo'], 
                'mensagem': n['mensagem'],
                'tipo': n['tipo'], 
                'link': n['link'], 
                'lida': n['lida'],
                'data': n['data'].isoformat() if n['data'] else datetime.now().isoformat()
            })
        
        return jsonify({'success': True, 'notificacoes': notificacoes_list, 'nao_lidas': nao_lidas})
        
    except Exception as e:
        print(f"Erro ao buscar notificações: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': True, 'notificacoes': [], 'nao_lidas': 0})


@app.route('/api/notificacoes/marcar-lida/<int:id>', methods=['POST'])
def api_marcar_notificacao_lida(id):
    """Marca uma notificação como lida"""
    try:
        if 'user_id' not in session:
            return jsonify({'success': False, 'error': 'Não autenticado'}), 401
        
        cursor, conn = get_db()
        cursor.execute("""
            UPDATE notificacoes SET lida = 1, data_leitura = CURRENT_TIMESTAMP
            WHERE id = %s AND usuario_id = %s
        """, (id, session['user_id']))
        conn.commit()
        return_connection(conn)
        return jsonify({'success': True})
        
    except Exception as e:
        print(f"Erro ao marcar notificação: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/notificacoes/marcar-todas-lidas', methods=['POST'])
def api_marcar_todas_notificacoes_lidas():
    """Marca todas as notificações como lidas"""
    try:
        if 'user_id' not in session:
            return jsonify({'success': False, 'error': 'Não autenticado'}), 401
        
        cursor, conn = get_db()
        cursor.execute("""
            UPDATE notificacoes SET lida = 1, data_leitura = CURRENT_TIMESTAMP
            WHERE usuario_id = %s AND lida = 0
        """, (session['user_id'],))
        conn.commit()
        return_connection(conn)
        return jsonify({'success': True})
        
    except Exception as e:
        print(f"Erro ao marcar todas: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/notificacoes/stream')
def notificacoes_stream():
    """Stream de notificações em tempo real (Server-Sent Events)"""
    def generate():
        try:
            yield f"data: {json.dumps({'type': 'connected', 'message': 'Conectado'})}\n\n"
            while True:
                yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now().isoformat()})}\n\n"
                time.sleep(30)
        except GeneratorExit:
            pass
    return Response(generate(), mimetype="text/event-stream")

@app.route("/configuracoes/notificacoes", methods=["GET", "POST"])
@login_required
@permissao_required('configuracoes.view')
def configuracoes_notificacoes():
    """Página de configurações de notificações do usuário"""
    cursor, conn = get_db()
    user_id = session["user_id"]
    
    # Verificar e criar tabela se necessário
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'notificacoes_config'
        )
    """)
    tabela_existe = cursor.fetchone()['exists']
    
    if not tabela_existe:
        cursor.execute("""
            CREATE TABLE notificacoes_config (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL UNIQUE,
                notificar_aniversario_obreiro INTEGER DEFAULT 1,
                notificar_aniversario_familiar INTEGER DEFAULT 1,
                notificar_reuniao INTEGER DEFAULT 1,
                notificar_ata_publicada INTEGER DEFAULT 1,
                notificar_sindicancia INTEGER DEFAULT 1,
                dias_antecedencia INTEGER DEFAULT 3,
                horario_envio TIME DEFAULT '08:00:00',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    
    # Verificar se as colunas existem (se não, adicionar)
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'notificacoes_config'
    """)
    colunas = [c['column_name'] for c in cursor.fetchall()]
    
    if 'dias_antecedencia' not in colunas:
        cursor.execute("ALTER TABLE notificacoes_config ADD COLUMN dias_antecedencia INTEGER DEFAULT 3")
        conn.commit()
    
    if 'horario_envio' not in colunas:
        cursor.execute("ALTER TABLE notificacoes_config ADD COLUMN horario_envio TIME DEFAULT '08:00:00'")
        conn.commit()
    
    if 'notificar_reuniao' not in colunas:
        cursor.execute("ALTER TABLE notificacoes_config ADD COLUMN notificar_reuniao INTEGER DEFAULT 1")
        conn.commit()
    
    if 'notificar_ata_publicada' not in colunas:
        cursor.execute("ALTER TABLE notificacoes_config ADD COLUMN notificar_ata_publicada INTEGER DEFAULT 1")
        conn.commit()
    
    if 'notificar_sindicancia' not in colunas:
        cursor.execute("ALTER TABLE notificacoes_config ADD COLUMN notificar_sindicancia INTEGER DEFAULT 1")
        conn.commit()
    
    if request.method == "POST":
        try:
            # Coletar dados do formulário
            notificar_aniversario_obreiro = 1 if request.form.get("notificar_aniversario_obreiro") else 0
            notificar_aniversario_familiar = 1 if request.form.get("notificar_aniversario_familiar") else 0
            notificar_reuniao = 1 if request.form.get("notificar_reuniao") else 0
            notificar_ata_publicada = 1 if request.form.get("notificar_ata_publicada") else 0
            notificar_sindicancia = 1 if request.form.get("notificar_sindicancia") else 0
            dias_antecedencia = request.form.get("dias_antecedencia", 3)
            horario_envio = request.form.get("horario_envio", "08:00")
            
            # Verificar se já existe configuração
            cursor.execute("SELECT id FROM notificacoes_config WHERE usuario_id = %s", (user_id,))
            existing = cursor.fetchone()
            
            if existing:
                cursor.execute("""
                    UPDATE notificacoes_config SET
                        notificar_aniversario_obreiro = %s,
                        notificar_aniversario_familiar = %s,
                        notificar_reuniao = %s,
                        notificar_ata_publicada = %s,
                        notificar_sindicancia = %s,
                        dias_antecedencia = %s,
                        horario_envio = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE usuario_id = %s
                """, (notificar_aniversario_obreiro, notificar_aniversario_familiar,
                      notificar_reuniao, notificar_ata_publicada, notificar_sindicancia,
                      dias_antecedencia, horario_envio, user_id))
            else:
                cursor.execute("""
                    INSERT INTO notificacoes_config 
                    (usuario_id, notificar_aniversario_obreiro, notificar_aniversario_familiar,
                     notificar_reuniao, notificar_ata_publicada, notificar_sindicancia,
                     dias_antecedencia, horario_envio)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (user_id, notificar_aniversario_obreiro, notificar_aniversario_familiar,
                      notificar_reuniao, notificar_ata_publicada, notificar_sindicancia,
                      dias_antecedencia, horario_envio))
            
            conn.commit()
            flash("✅ Configurações de notificações salvas com sucesso!", "success")
            
        except Exception as e:
            print(f"Erro ao salvar configurações: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()
            flash(f"Erro ao salvar configurações: {str(e)}", "danger")
        
        return_connection(conn)
        return redirect("/configuracoes/notificacoes")
    
    # GET - Carregar configurações existentes
    try:
        cursor.execute("SELECT * FROM notificacoes_config WHERE usuario_id = %s", (user_id,))
        config = cursor.fetchone()
    except Exception as e:
        print(f"Erro ao buscar configurações: {e}")
        config = None
    
    # Se não tiver configuração, criar valores padrão
    if not config:
        config = {
            'notificar_aniversario_obreiro': 1,
            'notificar_aniversario_familiar': 1,
            'notificar_reuniao': 1,
            'notificar_ata_publicada': 1,
            'notificar_sindicancia': 1,
            'dias_antecedencia': 3,
            'horario_envio': '08:00:00'
        }
    
    return_connection(conn)
    
    return render_template("configuracoes/notificacoes.html", config=config)



# ============================================
# GERENCIAMENTO DE BACKUPS COM CLOUDINARY
# ============================================

import cloudinary.uploader
import cloudinary.api
import cloudinary
import subprocess
import tempfile
import os
from datetime import datetime

# Configuração do Cloudinary (já deve estar no config.py)
# cloudinary.config(
#     cloud_name=os.getenv('CLOUDINARY_CLOUD_NAME'),
#     api_key=os.getenv('CLOUDINARY_API_KEY'),
#     api_secret=os.getenv('CLOUDINARY_API_SECRET')
# )

CLOUDINARY_BACKUP_FOLDER = 'backups/sistema_maconico'

def get_backup_list():
    """Retorna lista de backups do Cloudinary"""
    try:
        result = cloudinary.api.resources(
            type="upload",
            prefix=CLOUDINARY_BACKUP_FOLDER,
            resource_type="raw",
            max_results=500
        )
        
        backups = []
        for resource in result.get('resources', []):
            public_id = resource.get('public_id')
            created_at = resource.get('created_at')
            try:
                date_obj = datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S%z')
                date_str = date_obj.strftime('%d/%m/%Y %H:%M:%S')
            except:
                date_str = created_at
            
            backups.append({
                'public_id': public_id,
                'name': public_id.split('/')[-1] + '.sql',
                'size_bytes': resource.get('bytes', 0),
                'size_mb': round(resource.get('bytes', 0) / (1024 * 1024), 2),
                'created_at': created_at,
                'date_str': date_str,
                'date': date_obj if 'date_obj' in locals() else None,
                'url': resource.get('secure_url')
            })
        
        # Ordenar por data (mais recentes primeiro)
        backups.sort(key=lambda x: x.get('date') or datetime.min, reverse=True)
        return backups
    except Exception as e:
        print(f"Erro ao listar backups: {e}")
        return []


@app.route("/backups")
@login_required
@admin_required
def gerenciar_backups():
    """Página de gerenciamento de backups"""
    backups = get_backup_list()
    
    # Estatísticas
    total_backups = len(backups)
    total_size_bytes = sum(b['size_bytes'] for b in backups)
    total_size_mb = round(total_size_bytes / (1024 * 1024), 2)
    total_size_gb = round(total_size_bytes / (1024 * 1024 * 1024), 2)
    
    stats = {
        'total': total_backups,
        'total_size_mb': total_size_mb,
        'total_size_gb': total_size_gb,
        'newest': backups[0]['date_str'] if backups else 'Nenhum'
    }
    
    return render_template("backups.html", backups=backups, stats=stats)


@app.route("/api/backup/criar", methods=["POST"])
@login_required
@admin_required
def criar_backup():
    """Cria backup e envia para Cloudinary"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"backup_{timestamp}.sql"
        
        # Criar backup temporário
        with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as tmp_file:
            temp_path = tmp_file.name
        
        try:
            # Executar pg_dump
            db_url = os.getenv('DATABASE_URL')
            cmd = f"pg_dump -Fc --no-owner --no-privileges {db_url} > {temp_path}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"Erro no pg_dump: {result.stderr}")
            
            # Upload para Cloudinary
            upload_result = cloudinary.uploader.upload(
                temp_path,
                folder=CLOUDINARY_BACKUP_FOLDER,
                resource_type="raw",
                public_id=f"backup_{timestamp}",
                use_filename=True,
                unique_filename=False
            )
            
            # Registrar log
            registrar_log("criar_backup", "backup", None, 
                         dados_novos={"filename": filename, "size": upload_result.get('bytes')})
            
            return jsonify({
                'success': True,
                'filename': filename,
                'public_id': upload_result.get('public_id'),
                'size_mb': round(upload_result.get('bytes') / (1024 * 1024), 2),
                'url': upload_result.get('secure_url')
            })
            
        finally:
            # Remover arquivo temporário
            if os.path.exists(temp_path):
                os.unlink(temp_path)
                
    except Exception as e:
        print(f"Erro ao criar backup: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route("/api/backup/baixar/<path:public_id>")
@login_required
@admin_required
def baixar_backup(public_id):
    """Redireciona para download do backup no Cloudinary"""
    try:
        resource = cloudinary.api.resource(public_id, resource_type="raw")
        url = resource.get('secure_url')
        
        registrar_log("baixar_backup", "backup", None, dados_novos={"backup": public_id})
        
        return redirect(url)
    except Exception as e:
        flash(f"Erro ao baixar backup: {str(e)}", "danger")
        return redirect("/backups")


@app.route("/api/backup/restaurar/<path:public_id>", methods=["POST"])
@login_required
@admin_required
def restaurar_backup(public_id):
    """Restaura backup do Cloudinary"""
    try:
        # Criar backup de emergência antes de restaurar
        backup_emergencia = criar_backup_emergencia()
        
        # Baixar backup do Cloudinary
        resource = cloudinary.api.resource(public_id, resource_type="raw")
        url = resource.get('secure_url')
        
        import requests
        response = requests.get(url)
        
        if response.status_code != 200:
            raise Exception("Erro ao baixar backup do Cloudinary")
        
        # Salvar em arquivo temporário
        with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as tmp_file:
            tmp_file.write(response.content)
            temp_path = tmp_file.name
        
        try:
            # Restaurar banco
            db_url = os.getenv('DATABASE_URL')
            cmd = f"pg_restore -c --no-owner --no-privileges -d {db_url} {temp_path}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"Erro na restauração: {result.stderr}")
            
            registrar_log("restaurar_backup", "backup", None, 
                         dados_novos={"backup": public_id, "emergencia": backup_emergencia})
            
            return jsonify({
                'success': True,
                'message': 'Backup restaurado com sucesso',
                'emergency_backup': backup_emergencia
            })
            
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
                
    except Exception as e:
        print(f"Erro ao restaurar backup: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route("/api/backup/excluir/<path:public_id>", methods=["DELETE"])
@login_required
@admin_required
def excluir_backup(public_id):
    """Exclui backup do Cloudinary"""
    try:
        cloudinary.uploader.destroy(public_id, resource_type="raw")
        
        registrar_log("excluir_backup", "backup", None, dados_novos={"backup": public_id})
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route("/api/backup/info/<path:public_id>")
@login_required
@admin_required
def info_backup(public_id):
    """Obtém informações do backup"""
    try:
        resource = cloudinary.api.resource(public_id, resource_type="raw")
        
        info = {
            'filename': public_id.split('/')[-1] + '.sql',
            'size_bytes': resource.get('bytes', 0),
            'size_mb': round(resource.get('bytes', 0) / (1024 * 1024), 2),
            'created': resource.get('created_at'),
            'modified': resource.get('updated_at'),
            'url': resource.get('secure_url'),
            'public_id': public_id
        }
        
        return jsonify({'success': True, 'info': info})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route("/api/backup/limpar", methods=["POST"])
@login_required
@admin_required
def limpar_backups():
    """Limpa backups antigos (mantém últimos 20)"""
    try:
        backups = get_backup_list()
        
        if len(backups) <= 20:
            return jsonify({
                'success': True,
                'deleted': 0,
                'remaining': len(backups),
                'message': 'Nenhum backup antigo para remover'
            })
        
        # Manter os 20 mais recentes
        to_delete = backups[20:]
        deleted = 0
        
        for backup in to_delete:
            try:
                cloudinary.uploader.destroy(backup['public_id'], resource_type="raw")
                deleted += 1
            except Exception as e:
                print(f"Erro ao excluir {backup['public_id']}: {e}")
        
        registrar_log("limpar_backups", "backup", None, 
                     dados_novos={"deleted": deleted, "remaining": len(backups) - deleted})
        
        return jsonify({
            'success': True,
            'deleted': deleted,
            'remaining': len(backups) - deleted
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def criar_backup_emergencia():
    """Cria backup de emergência antes de restaurar"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as tmp_file:
            temp_path = tmp_file.name
        
        db_url = os.getenv('DATABASE_URL')
        cmd = f"pg_dump -Fc --no-owner --no-privileges {db_url} > {temp_path}"
        subprocess.run(cmd, shell=True, check=True)
        
        upload_result = cloudinary.uploader.upload(
            temp_path,
            folder=CLOUDINARY_BACKUP_FOLDER,
            resource_type="raw",
            public_id=f"emergencia_antes_restore_{timestamp}"
        )
        
        os.unlink(temp_path)
        
        return upload_result.get('public_id')
    except Exception as e:
        print(f"Erro ao criar backup de emergência: {e}")
        return None


# =============================
# INICIALIZAÇÃO DA APLICAÇÃO
# =============================
if __name__ == "__main__":
    debug_mode = os.getenv('FLASK_ENV', 'production') == 'development'
    port = int(os.environ.get('PORT', 5000))
    
    # Testar conexão antes de iniciar
    test_connection()
    
    app.run(debug=debug_mode, host='0.0.0.0', port=port)