# app.py - Sistema Maçônico com PostgreSQL (VERSÃO COMPLETA COM BIBLIOTECA)
# -*- coding: utf-8 -*-

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
# DECORATORS
# =============================

# Decorator para verificar permissão por grau
def require_grau(min_grau):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'usuario_id' not in session:
                flash('Faça login para acessar esta página', 'warning')
                return redirect(url_for('login'))
            
            # Buscar grau do usuário
            cursor, conn = get_db()
            cursor.execute("SELECT grau_atual FROM usuarios WHERE id = %s", (session['usuario_id'],))
            usuario = cursor.fetchone()
            return_connection(conn)
            
            if not usuario or usuario['grau_atual'] < min_grau:
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
            nivel_usuario = session.get("nivel_acesso", 1)
            tipo_usuario = session.get("tipo", "obreiro")
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
                    if tipo_usuario == "admin":
                        return f(*args, **kwargs)
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
    if 'user_id' not in session:
        return False
    if session.get('tipo') == 'admin':
        return True
    try:
        cursor, conn = get_db()
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM permissoes_grau pg
            JOIN permissoes p ON pg.permissao_id = p.id
            WHERE pg.grau_id = %s AND p.codigo = %s
        """, (session.get('grau_atual', 1), permissao_codigo))
        result = cursor.fetchone()
        return_connection(conn)
        return result and result['total'] > 0
    except Exception as e:
        print(f"Erro ao verificar permissão: {e}")
        return False

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

# =============================
# ROTAS DA BIBLIOTECA
# =============================
@biblioteca_bp.route('/admin/upload', methods=['GET', 'POST'])
@admin_required
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
            return redirect(url_for('biblioteca.upload_material'))
        
        if not tipo:
            flash('O tipo de material é obrigatório', 'danger')
            return redirect(url_for('biblioteca.upload_material'))
        
        if not categoria_id:
            flash('A categoria é obrigatória', 'danger')
            return redirect(url_for('biblioteca.upload_material'))
        
        if not grau_acesso:
            flash('O grau de acesso é obrigatório', 'danger')
            return redirect(url_for('biblioteca.upload_material'))
        
        # Processar arquivos
        arquivo = request.files.get('arquivo')
        capa = request.files.get('capa')
        
        if not arquivo or arquivo.filename == '':
            flash('Selecione um arquivo para upload', 'danger')
            return redirect(url_for('biblioteca.upload_material'))
        
        # Validar tamanho do arquivo (50MB)
        arquivo.seek(0, 2)  # Vai para o final do arquivo
        tamanho_arquivo = arquivo.tell()
        arquivo.seek(0)  # Volta para o início
        
        if tamanho_arquivo > 50 * 1024 * 1024:
            flash('Arquivo muito grande! O tamanho máximo é 50MB.', 'danger')
            return redirect(url_for('biblioteca.upload_material'))
        
        try:
            # Upload do arquivo principal para o Cloudinary
            print(f"📤 Enviando arquivo: {arquivo.filename}")
            
            # Gerar nome único para o arquivo
            import uuid
            nome_arquivo = f"{uuid.uuid4().hex}_{secure_filename(arquivo.filename)}"
            
            # Fazer upload para o Cloudinary
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
            
            print(f"✅ Arquivo enviado: {arquivo_url}")
            
            # Upload da capa (se houver)
            capa_url = None
            if capa and capa.filename:
                print(f"📸 Enviando capa: {capa.filename}")
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
                print(f"✅ Capa enviada: {capa_url}")
            
            # Inserir no banco de dados
            cursor, conn = get_db()
            
            # Converter ano_publicacao para int se existir
            ano_publicacao_int = None
            if ano_publicacao and ano_publicacao.strip():
                try:
                    ano_publicacao_int = int(ano_publicacao)
                except:
                    pass
            
            # Converter num_paginas para int se existir
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
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, true, %s, NOW(), NOW())
                RETURNING id
            """, (
                titulo, subtitulo, descricao, tipo, categoria_id, grau_acesso,
                arquivo_url, arquivo_nome_original, arquivo_tamanho, formato, capa_url,
                autor, editora, ano_publicacao_int, num_paginas_int, isbn, tags,
                destaque, session['usuario_id']
            ))
            
            material_id = cursor.fetchone()['id']
            conn.commit()
            return_connection(conn)
            
            flash(f'✅ Material "{titulo}" enviado com sucesso para a nuvem!', 'success')
            return redirect(url_for('biblioteca.visualizar_material', material_id=material_id))
            
        except cloudinary.exceptions.Error as e:
            print(f"❌ Erro no Cloudinary: {e}")
            traceback.print_exc()
            flash(f'Erro no envio para a nuvem: {str(e)}', 'danger')
            return redirect(url_for('biblioteca.upload_material'))
        
        except Exception as e:
            print(f"❌ Erro no upload: {e}")
            traceback.print_exc()
            flash(f'Erro ao enviar arquivo: {str(e)}', 'danger')
            return redirect(url_for('biblioteca.upload_material'))
    
    # GET - mostrar formulário
    cursor, conn = get_db()
    cursor.execute("SELECT * FROM categorias_material ORDER BY ordem")
    categorias = cursor.fetchall()
    return_connection(conn)
    
    return render_template('biblioteca/upload.html', categorias=categorias)

@biblioteca_bp.route('/admin/editar/<int:material_id>', methods=['GET', 'POST'])
@admin_required
def editar_material(material_id):
    """Editar um material existente"""
    
    # Buscar o material
    cursor, conn = get_db()
    cursor.execute("""
        SELECT m.*, c.nome as categoria_nome
        FROM materiais m
        LEFT JOIN categorias_material c ON m.categoria_id = c.id
        WHERE m.id = %s
    """, (material_id,))
    material = cursor.fetchone()
    
    if not material:
        flash('Material não encontrado', 'danger')
        return redirect(url_for('biblioteca.listar_materiais'))
    
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
        publicado = request.form.get('publicado') == 'on'
        
        # Validar campos obrigatórios
        if not titulo:
            flash('O título é obrigatório', 'danger')
            return redirect(url_for('biblioteca.editar_material', material_id=material_id))
        
        # Processar novo arquivo (se houver)
        arquivo = request.files.get('arquivo')
        arquivo_url = material['arquivo_url']
        arquivo_nome = material['arquivo_nome']
        arquivo_tamanho = material['arquivo_tamanho']
        formato = material['formato']
        
        if arquivo and arquivo.filename:
            # Upload do novo arquivo
            import uuid
            nome_arquivo = f"{uuid.uuid4().hex}_{secure_filename(arquivo.filename)}"
            
            upload_result = cloudinary.uploader.upload(
                arquivo,
                folder="biblioteca_maconica/materiais",
                public_id=nome_arquivo,
                resource_type="auto"
            )
            
            arquivo_url = upload_result.get('secure_url')
            arquivo_tamanho = upload_result.get('bytes')
            formato = upload_result.get('format')
            arquivo_nome = arquivo.filename
            
            # Se tinha arquivo antigo, deletar do Cloudinary
            if material['arquivo_url']:
                try:
                    public_id = material['arquivo_url'].split('/')[-1].split('.')[0]
                    cloudinary.uploader.destroy(f"biblioteca_maconica/materiais/{public_id}")
                except:
                    pass
        
        # Processar nova capa (se houver)
        capa = request.files.get('capa')
        capa_url = material['capa_url']
        
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
            
            # Se tinha capa antiga, deletar
            if material['capa_url']:
                try:
                    public_id = material['capa_url'].split('/')[-1].split('.')[0]
                    cloudinary.uploader.destroy(f"biblioteca_maconica/capas/{public_id}")
                except:
                    pass
        
        # Converter valores
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
        
        # Atualizar no banco
        cursor.execute("""
            UPDATE materiais SET
                titulo = %s,
                subtitulo = %s,
                descricao = %s,
                tipo = %s,
                categoria_id = %s,
                grau_acesso = %s,
                arquivo_url = %s,
                arquivo_nome = %s,
                arquivo_tamanho = %s,
                formato = %s,
                capa_url = %s,
                autor = %s,
                editora = %s,
                ano_publicacao = %s,
                num_paginas = %s,
                isbn = %s,
                tags = %s,
                destaque = %s,
                publicado = %s,
                updated_at = NOW()
            WHERE id = %s
        """, (
            titulo, subtitulo, descricao, tipo, categoria_id, grau_acesso,
            arquivo_url, arquivo_nome, arquivo_tamanho, formato, capa_url,
            autor, editora, ano_publicacao_int, num_paginas_int, isbn, tags,
            destaque, publicado, material_id
        ))
        
        conn.commit()
        return_connection(conn)
        
        flash('Material atualizado com sucesso!', 'success')
        return redirect(url_for('biblioteca.visualizar_material', material_id=material_id))
    
    # GET - mostrar formulário
    cursor.execute("SELECT * FROM categorias_material ORDER BY ordem")
    categorias = cursor.fetchall()
    return_connection(conn)
    
    return render_template('biblioteca/editar.html', 
                         material=material, 
                         categorias=categorias)

@biblioteca_bp.route('/admin/excluir/<int:material_id>', methods=['POST'])
@admin_required
def excluir_material(material_id):
    """Excluir um material"""
    
    cursor, conn = get_db()
    
    # Buscar o material para pegar as URLs
    cursor.execute("SELECT * FROM materiais WHERE id = %s", (material_id,))
    material = cursor.fetchone()
    
    if not material:
        flash('Material não encontrado', 'danger')
        return redirect(url_for('biblioteca.listar_materiais'))
    
    try:
        # Deletar arquivo do Cloudinary
        if material['arquivo_url']:
            try:
                public_id = material['arquivo_url'].split('/')[-1].split('.')[0]
                cloudinary.uploader.destroy(f"biblioteca_maconica/materiais/{public_id}")
            except Exception as e:
                print(f"Erro ao deletar arquivo: {e}")
        
        # Deletar capa do Cloudinary
        if material['capa_url']:
            try:
                public_id = material['capa_url'].split('/')[-1].split('.')[0]
                cloudinary.uploader.destroy(f"biblioteca_maconica/capas/{public_id}")
            except Exception as e:
                print(f"Erro ao deletar capa: {e}")
        
        # Deletar registros relacionados
        cursor.execute("DELETE FROM downloads_material WHERE material_id = %s", (material_id,))
        cursor.execute("DELETE FROM favoritos_material WHERE material_id = %s", (material_id,))
        cursor.execute("DELETE FROM avaliacoes_material WHERE material_id = %s", (material_id,))
        
        # Deletar o material
        cursor.execute("DELETE FROM materiais WHERE id = %s", (material_id,))
        conn.commit()
        
        flash(f'Material "{material["titulo"]}" excluído com sucesso!', 'success')
        
    except Exception as e:
        print(f"Erro ao excluir: {e}")
        flash('Erro ao excluir o material', 'danger')
    
    return_connection(conn)
    return redirect(url_for('biblioteca.listar_materiais'))                         
    
@biblioteca_bp.route('/')
def listar_materiais():
    """Página principal da biblioteca"""
    if 'usuario_id' not in session:
        flash('Faça login para acessar a biblioteca', 'warning')
        return redirect(url_for('login'))
    
    if not tem_permissao_biblioteca():
        flash('Você não tem permissão para acessar a biblioteca', 'danger')
        return redirect(url_for('dashboard'))
    
    grau_usuario = get_grau_usuario()
    
    try:
        cursor, conn = get_db()
        
        # Buscar materiais liberados para o grau do usuário (com DISTINCT para evitar duplicatas)
        cursor.execute("""
            SELECT DISTINCT m.*, c.nome as categoria_nome, c.cor as categoria_cor
            FROM materiais m
            LEFT JOIN categorias_material c ON m.categoria_id = c.id
            WHERE m.publicado = true AND m.grau_acesso <= %s
            ORDER BY m.destaque DESC, m.data_publicacao DESC
            LIMIT 20
        """, (grau_usuario,))
        materiais = cursor.fetchall()
        
        # Buscar categorias (sem duplicatas)
        cursor.execute("""
            SELECT DISTINCT c.*
            FROM categorias_material c
            ORDER BY c.ordem
        """)
        categorias = cursor.fetchall()
        
        # Buscar materiais em destaque (com DISTINCT)
        cursor.execute("""
            SELECT DISTINCT m.*, c.nome as categoria_nome
            FROM materiais m
            LEFT JOIN categorias_material c ON m.categoria_id = c.id
            WHERE m.destaque = true AND m.publicado = true AND m.grau_acesso <= %s
            LIMIT 5
        """, (grau_usuario,))
        destaques = cursor.fetchall()
        
        return_connection(conn)
        
        return render_template('biblioteca/index.html',
                             materiais=materiais,
                             categorias=categorias,
                             destaques=destaques,
                             grau_usuario=grau_usuario)
    
    except Exception as e:
        print(f"Erro ao carregar biblioteca: {e}")
        traceback.print_exc()
        flash('Erro ao carregar a biblioteca. Tente novamente mais tarde.', 'danger')
        return redirect(url_for('dashboard'))
        
@biblioteca_bp.route('/material/<int:material_id>')
def visualizar_material(material_id):
    """Visualizar um material específico"""
    if 'usuario_id' not in session:
        flash('Faça login para acessar a biblioteca', 'warning')
        return redirect(url_for('login'))
    
    if not tem_permissao_biblioteca():
        flash('Você não tem permissão para acessar este conteúdo', 'danger')
        return redirect(url_for('biblioteca.listar_materiais'))
    
    grau_usuario = get_grau_usuario()
    
    try:
        cursor, conn = get_db()
        
        # Buscar material com verificação de permissão
        cursor.execute("""
            SELECT m.*, c.nome as categoria_nome, c.cor as categoria_cor
            FROM materiais m
            LEFT JOIN categorias_material c ON m.categoria_id = c.id
            WHERE m.id = %s AND m.publicado = true AND m.grau_acesso <= %s
        """, (material_id, grau_usuario))
        material = cursor.fetchone()
        
        if not material:
            flash('Material não encontrado ou sem permissão de acesso', 'danger')
            return redirect(url_for('biblioteca.listar_materiais'))
        
        # Incrementar visualizações
        cursor.execute("""
            UPDATE materiais SET visualizacoes_count = visualizacoes_count + 1 
            WHERE id = %s
        """, (material_id,))
        conn.commit()
        
        # Buscar avaliações
        cursor.execute("""
            SELECT a.*, u.nome_completo as usuario_nome
            FROM avaliacoes_material a
            LEFT JOIN usuarios u ON a.usuario_id = u.id
            WHERE a.material_id = %s AND a.moderado = true
            ORDER BY a.data_avaliacao DESC
            LIMIT 10
        """, (material_id,))
        avaliacoes = cursor.fetchall()
        
        # Verificar se usuário já favoritou
        cursor.execute("""
            SELECT id FROM favoritos_material 
            WHERE material_id = %s AND usuario_id = %s
        """, (material_id, session['usuario_id']))
        favoritado = cursor.fetchone() is not None
        
        return_connection(conn)
        
        return render_template('biblioteca/visualizar.html',
                             material=material,
                             avaliacoes=avaliacoes,
                             favoritado=favoritado)
    
    except Exception as e:
        print(f"Erro ao visualizar material: {e}")
        traceback.print_exc()
        flash('Erro ao carregar o material', 'danger')
        return redirect(url_for('biblioteca.listar_materiais'))

@biblioteca_bp.route('/material/<int:material_id>/download')
def download_material(material_id):
    """Download do material"""
    if 'usuario_id' not in session:
        flash('Faça login para acessar a biblioteca', 'warning')
        return redirect(url_for('login'))
    
    if not tem_permissao_biblioteca():
        flash('Você não tem permissão para baixar este conteúdo', 'danger')
        return redirect(url_for('biblioteca.listar_materiais'))
    
    grau_usuario = get_grau_usuario()
    
    try:
        cursor, conn = get_db()
        
        cursor.execute("""
            SELECT * FROM materiais 
            WHERE id = %s AND publicado = true AND grau_acesso <= %s
        """, (material_id, grau_usuario))
        material = cursor.fetchone()
        
        if not material or not material.get('arquivo_url'):
            flash('Arquivo não encontrado', 'danger')
            return redirect(url_for('biblioteca.listar_materiais'))
        
        # Registrar download
        cursor.execute("""
            INSERT INTO downloads_material (material_id, usuario_id, ip_address)
            VALUES (%s, %s, %s)
        """, (material_id, session['usuario_id'], request.remote_addr))
        
        # Incrementar contador de downloads
        cursor.execute("""
            UPDATE materiais SET downloads_count = downloads_count + 1 
            WHERE id = %s
        """, (material_id,))
        conn.commit()
        
        return_connection(conn)
        
        # Se for URL externa, redirecionar
        if material['arquivo_url'].startswith('http'):
            return redirect(material['arquivo_url'])
        
        flash('Download iniciado!', 'success')
        return redirect(url_for('biblioteca.visualizar_material', material_id=material_id))
    
    except Exception as e:
        print(f"Erro no download: {e}")
        traceback.print_exc()
        flash('Erro ao baixar o arquivo', 'danger')
        return redirect(url_for('biblioteca.visualizar_material', material_id=material_id))

@biblioteca_bp.route('/buscar')
def buscar_materiais():
    """Buscar materiais"""
    if 'usuario_id' not in session:
        flash('Faça login para acessar a biblioteca', 'warning')
        return redirect(url_for('login'))
    
    if not tem_permissao_biblioteca():
        flash('Você não tem permissão para acessar a biblioteca', 'danger')
        return redirect(url_for('dashboard'))
    
    termo = request.args.get('q', '')
    tipo = request.args.get('tipo', '')
    categoria = request.args.get('categoria', '')
    grau_usuario = get_grau_usuario()
    
    try:
        cursor, conn = get_db()
        
        query = """
            SELECT DISTINCT m.*, c.nome as categoria_nome
            FROM materiais m
            LEFT JOIN categorias_material c ON m.categoria_id = c.id
            WHERE m.publicado = true AND m.grau_acesso <= %s
        """
        params = [grau_usuario]
        
        if termo:
            query += " AND (m.titulo ILIKE %s OR m.descricao ILIKE %s OR m.tags ILIKE %s OR m.autor ILIKE %s)"
            termo_like = f"%{termo}%"
            params.extend([termo_like, termo_like, termo_like, termo_like])
        
        if tipo:
            query += " AND m.tipo = %s"
            params.append(tipo)
        
        if categoria:
            query += " AND m.categoria_id = %s"
            params.append(categoria)
        
        query += " ORDER BY m.data_publicacao DESC"
        
        cursor.execute(query, params)
        materiais = cursor.fetchall()
        
        return_connection(conn)
        
        return render_template('biblioteca/buscar.html',
                             materiais=materiais,
                             termo=termo,
                             tipo=tipo,
                             categoria=categoria)
    
    except Exception as e:
        print(f"Erro na busca: {e}")
        traceback.print_exc()
        flash('Erro ao realizar a busca', 'danger')
        return redirect(url_for('biblioteca.listar_materiais'))

@biblioteca_bp.route('/categoria/<int:categoria_id>')
def materiais_por_categoria(categoria_id):
    """Materiais de uma categoria específica"""
    if 'usuario_id' not in session:
        flash('Faça login para acessar a biblioteca', 'warning')
        return redirect(url_for('login'))
    
    if not tem_permissao_biblioteca():
        flash('Você não tem permissão para acessar a biblioteca', 'danger')
        return redirect(url_for('dashboard'))
    
    grau_usuario = get_grau_usuario()
    
    try:
        cursor, conn = get_db()
        
        # Buscar materiais da categoria (com DISTINCT)
        cursor.execute("""
            SELECT DISTINCT m.*, c.nome as categoria_nome, c.cor as categoria_cor
            FROM materiais m
            LEFT JOIN categorias_material c ON m.categoria_id = c.id
            WHERE m.categoria_id = %s AND m.publicado = true AND m.grau_acesso <= %s
            ORDER BY m.data_publicacao DESC
        """, (categoria_id, grau_usuario))
        materiais = cursor.fetchall()
        
        # Buscar a categoria (apenas uma)
        cursor.execute("SELECT * FROM categorias_material WHERE id = %s", (categoria_id,))
        categoria = cursor.fetchone()
        
        return_connection(conn)
        
        if not categoria:
            flash('Categoria não encontrada', 'danger')
            return redirect(url_for('biblioteca.listar_materiais'))
        
        return render_template('biblioteca/categoria.html',
                             materiais=materiais,
                             categoria=categoria)
    
    except Exception as e:
        print(f"Erro ao carregar categoria: {e}")
        traceback.print_exc()
        flash('Erro ao carregar a categoria', 'danger')
        return redirect(url_for('biblioteca.listar_materiais'))

@biblioteca_bp.route('/material/<int:material_id>/favoritar', methods=['POST'])
def favoritar_material(material_id):
    """Adicionar ou remover dos favoritos"""
    if 'usuario_id' not in session:
        return jsonify({'success': False, 'mensagem': 'Usuário não autenticado'}), 401
    
    try:
        cursor, conn = get_db()
        
        # Verificar se já está nos favoritos
        cursor.execute("""
            SELECT id FROM favoritos_material 
            WHERE material_id = %s AND usuario_id = %s
        """, (material_id, session['usuario_id']))
        favorito = cursor.fetchone()
        
        if favorito:
            # Remover dos favoritos
            cursor.execute("""
                DELETE FROM favoritos_material 
                WHERE material_id = %s AND usuario_id = %s
            """, (material_id, session['usuario_id']))
            conn.commit()
            mensagem = 'Removido dos favoritos'
            favoritado = False
        else:
            # Adicionar aos favoritos
            cursor.execute("""
                INSERT INTO favoritos_material (material_id, usuario_id)
                VALUES (%s, %s)
            """, (material_id, session['usuario_id']))
            conn.commit()
            mensagem = 'Adicionado aos favoritos'
            favoritado = True
        
        return_connection(conn)
        
        return jsonify({
            'success': True,
            'favoritado': favoritado,
            'mensagem': mensagem
        })
    
    except Exception as e:
        print(f"Erro ao favoritar: {e}")
        return jsonify({'success': False, 'mensagem': str(e)}), 500

@biblioteca_bp.route('/material/<int:material_id>/avaliar', methods=['POST'])
def avaliar_material(material_id):
    """Avaliar um material"""
    if 'usuario_id' not in session:
        return jsonify({'success': False, 'mensagem': 'Usuário não autenticado'}), 401
    
    data = request.get_json()
    nota = data.get('nota')
    comentario = data.get('comentario', '')
    
    if not nota or nota < 1 or nota > 5:
        return jsonify({'success': False, 'mensagem': 'Nota inválida'}), 400
    
    try:
        cursor, conn = get_db()
        
        # Verificar se já avaliou
        cursor.execute("""
            SELECT id FROM avaliacoes_material 
            WHERE material_id = %s AND usuario_id = %s
        """, (material_id, session['usuario_id']))
        ja_avaliou = cursor.fetchone()
        
        if ja_avaliou:
            # Atualizar avaliação
            cursor.execute("""
                UPDATE avaliacoes_material 
                SET nota = %s, comentario = %s, data_avaliacao = NOW()
                WHERE id = %s
            """, (nota, comentario, ja_avaliou['id']))
        else:
            # Nova avaliação
            cursor.execute("""
                INSERT INTO avaliacoes_material (material_id, usuario_id, nota, comentario)
                VALUES (%s, %s, %s, %s)
            """, (material_id, session['usuario_id'], nota, comentario))
        
        conn.commit()
        return_connection(conn)
        
        return jsonify({'success': True, 'mensagem': 'Avaliação enviada com sucesso!'})
    
    except Exception as e:
        print(f"Erro ao avaliar: {e}")
        return jsonify({'success': False, 'mensagem': str(e)}), 500
        

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

def get_db():
    """Retorna cursor e conexão do banco"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        return cursor, conn
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        raise

def return_connection(conn):
    """Fecha conexão"""
    if conn:
        conn.close()

def test_connection():
    """Testa conexão com banco (apenas diagnóstico)"""
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        print(f"✅ PostgreSQL conectado: {version['version'][:50]}...")
        return_connection(conn)
        return True
    except Exception as e:
        print(f"❌ Falha na conexão: {e}")
        return False

# ✅ Testar conexão APENAS se não estiver em ambiente de teste
if __name__ != '__main__':
    # Em produção, apenas loga que configurou
    print(f"🔧 Banco configurado: {'RENDER' if os.getenv('DATABASE_URL') else 'LOCAL'}")
    
    # Opcional: testar conexão (mas não falhar se não conectar no primeiro momento)
    if os.getenv('DATABASE_URL'):
        test_connection()

# =============================
# FUNÇÕES AUXILIARES
# =============================
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

def registrar_log(acao, entidade=None, entidade_id=None, dados_anteriores=None, dados_novos=None):
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

# =====================
# FUNÇÕES DE GRAU (NOVAS)
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
    """Retorna o nome detalhado do grau (para tooltips e detalhes)"""
    grau_map = {
        1: "Aprendiz",
        2: "Companheiro",
        3: "Mestre",
        4: "Mestre Instalado",
        5: "Mestre Instalado - 5° Grau",
        6: "Grau Superior 6",
        7: "Grau Superior 7",
        8: "Grau Superior 8",
        9: "Grau Superior 9",
        10: "Grau Superior 10",
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

# =====================
# FUNÇÕES DE GRAU (NOVAS)
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
    """Retorna o nome detalhado do grau (para tooltips e detalhes)"""
    grau_map = {
        1: "Aprendiz",
        2: "Companheiro",
        3: "Mestre",
        4: "Mestre Instalado",
        5: "Mestre Instalado - 5° Grau",
        6: "Grau Superior 6",
        7: "Grau Superior 7",
        8: "Grau Superior 8",
        9: "Grau Superior 9",
        10: "Grau Superior 10",
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
# CONTEXTO GLOBAL
# =============================
@app.context_processor
def inject_global():
    return {'datetime': datetime, 'now': datetime.now(), 'tem_permissao': tem_permissao}

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
    if request.method == "POST":
        usuario = request.form.get("usuario")
        senha = request.form.get("senha")
        
        cursor, conn = get_db()
        cursor.execute("""
            SELECT id, usuario, senha_hash, tipo, grau_atual, nome_completo 
            FROM usuarios 
            WHERE usuario = %s AND ativo = 1
        """, (usuario,))
        user = cursor.fetchone()
        return_connection(conn)
        
        if user and check_password_hash(user['senha_hash'], senha):
            # DEFINIR TODAS AS VARIÁVEIS DE SESSÃO
            session['usuario_id'] = user['id']
            session['usuario'] = user['usuario']
            session['tipo'] = user['tipo']
            session['grau_atual'] = user['grau_atual']
            session['nome_completo'] = user['nome_completo']
            session['user_id'] = user['id']  # Alguns decorators usam user_id
            
            flash('Login realizado com sucesso!', 'success')
            
            # Redirecionar para a página que o usuário estava tentando acessar
            next_page = request.args.get('next')
            if next_page:
                return redirect(next_page)
            return redirect(url_for('dashboard'))
        else:
            flash('Usuário ou senha inválidos', 'danger')
    
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    """Faz logout do usuário"""
    session.clear()
    flash("Você saiu do sistema com sucesso!", "success")
    return redirect("/")

# =============================
# ROTAS DO DASHBOARD
# =============================
@app.route("/dashboard")
@login_required
def dashboard():
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT * FROM candidatos ORDER BY data_criacao DESC")
        candidatos = cursor.fetchall()
        cursor.execute("""
            SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, ativo 
            FROM usuarios 
            WHERE tipo = 'sindicante' AND ativo = 1
            ORDER BY nome_completo
        """)
        sindicantes = cursor.fetchall()
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
            pendentes = []
            for c in candidatos:
                if not c["fechado"]:
                    cursor.execute("SELECT sindicante FROM sindicancias WHERE candidato_id = %s", (c["id"],))
                    enviados = [r["sindicante"] for r in cursor.fetchall()]
                    faltam = [s["usuario"] for s in sindicantes if s["usuario"] not in enviados]
                    if faltam:
                        pendentes.append({"candidato": dict(c), "faltam": faltam})
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
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo IN ('admin', 'sindicante', 'obreiro') AND ativo = 1")
            total_obreiros = cursor.fetchone()["total"]
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 3 AND ativo = 1")
            mestres = cursor.fetchone()["total"]
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 2 AND ativo = 1")
            companheiros = cursor.fetchone()["total"]
            cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE grau_atual = 1 AND ativo = 1")
            aprendizes = cursor.fetchone()["total"]
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
            em_analise = aprovados = reprovados = total_obreiros = mestres = companheiros = aprendizes = 0
            total_reunioes = reunioes_realizadas = reunioes_agendadas = 0
            pendentes = []
            prazo_vencido = []
            proximas_reunioes = []
            proxima_reuniao = None
            for c in candidatos:
                cursor.execute("SELECT parecer FROM sindicancias WHERE candidato_id = %s AND sindicante = %s", (c["id"], session["usuario"]))
                parecer = cursor.fetchone()
                if parecer:
                    if parecer["parecer"] == "positivo":
                        aprovados += 1
                    else:
                        reprovados += 1
                elif not c["fechado"]:
                    em_analise += 1
        return_connection(conn)
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
# ROTAS DE OBREIROS
# =============================

@app.route("/obreiros")
@login_required
def listar_obreiros():
    """Lista obreiros com filtros por nome, grau, cargo, loja e status"""
    cursor, conn = get_db()
    
    # Obter parâmetros de filtro
    nome = request.args.get('nome', '').strip()
    grau = request.args.get('grau', '')
    cargo = request.args.get('cargo', '')
    loja = request.args.get('loja', '')
    status = request.args.get('status', 'ativos')  # ativos, inativos, todos
    
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
        WHERE 1=1
    """
    params = []
    
    # Filtro por nome (nome completo ou usuário)
    if nome:
        query += " AND (u.nome_completo ILIKE %s OR u.usuario ILIKE %s)"
        params.extend([f"%{nome}%", f"%{nome}%"])
    
    # Filtro por grau (considerando que grau >= 3 inclui todos os mestres)
    if grau:
        try:
            grau_int = int(grau)
            if grau_int == 3:
                # Mestre inclui todos os graus >= 3
                query += " AND u.grau_atual >= 3"
            else:
                query += " AND u.grau_atual = %s"
                params.append(grau_int)
        except ValueError:
            pass
    
    # Filtro por cargo (apenas obreiros que possuem um cargo específico)
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
    
    # Filtro por status (ativo/inativo)
    if status == 'inativos':
        query += " AND u.ativo = 0"
    elif status == 'todos':
        # Não filtrar por status
        pass
    else:  # ativos (padrão)
        query += " AND u.ativo = 1"
    
    # Ordenação
    query += """
        ORDER BY 
            u.ativo DESC,  -- ativos primeiro
            u.grau_atual DESC,  -- grau mais alto primeiro
            u.nome_completo ASC
    """
    
    # Executar query
    cursor.execute(query, params)
    obreiros = cursor.fetchall()
    
    # Converter para lista de dicionários
    obreiros_list = []
    for row in obreiros:
        obreiro = dict(row)
        
        # Obter nível do grau
        grau_nivel = obreiro.get('grau_atual', 0)
        
        # ✅ Adicionar campos de grau usando as funções auxiliares
        obreiro['grau_principal'] = get_grau_principal(grau_nivel)
        obreiro['grau_detalhado'] = get_grau_detalhado(grau_nivel)
        obreiro['grau_badge_class'] = get_grau_badge_class(grau_nivel)
        obreiro['grau_icon'] = get_grau_icon(grau_nivel)
        obreiro['grau_descricao'] = get_grau_descricao(grau_nivel)  # compatibilidade
        
        # Calcular percentual de presença
        if obreiro.get('presencas_ano', 0) > 0:
            percentual = (obreiro.get('presencas_confirmadas_ano', 0) / obreiro.get('presencas_ano', 1)) * 100
            obreiro['percentual_presenca'] = round(percentual, 1)
        else:
            obreiro['percentual_presenca'] = 0
        
        # Adicionar classe CSS para status
        obreiro['status_class'] = 'table-success' if obreiro['ativo'] == 1 else 'table-secondary'
        obreiro['status_badge'] = 'success' if obreiro['ativo'] == 1 else 'secondary'
        obreiro['status_text'] = 'Ativo' if obreiro['ativo'] == 1 else 'Inativo'
        
        obreiros_list.append(obreiro)
    
    # Buscar dados para os filtros (dropdowns)
    
    # ✅ Lista de graus disponíveis (usando as funções auxiliares)
    cursor.execute("""
        SELECT DISTINCT grau_atual as grau
        FROM usuarios 
        WHERE grau_atual IS NOT NULL
        ORDER BY grau_atual
    """)
    graus_raw = cursor.fetchall()
    
    graus_disponiveis = []
    for g in graus_raw:
        grau_nivel = g['grau']
        graus_disponiveis.append({
            'grau': grau_nivel,
            'nome_grau': get_grau_principal(grau_nivel),
            'nome_detalhado': get_grau_detalhado(grau_nivel)
        })
    
    # Lista de cargos ativos
    cursor.execute("""
        SELECT id, nome, sigla 
        FROM cargos 
        WHERE ativo = 1 
        ORDER BY ordem, nome
    """)
    cargos_disponiveis = cursor.fetchall()
    
    # Lista de lojas com obreiros
    cursor.execute("""
        SELECT DISTINCT loja_nome as nome 
        FROM usuarios 
        WHERE loja_nome IS NOT NULL AND loja_nome != ''
        ORDER BY loja_nome
    """)
    lojas_disponiveis = cursor.fetchall()
    
    # Estatísticas gerais
    cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE ativo = 1")
    total_ativos = cursor.fetchone()['total']
    
    cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE ativo = 0")
    total_inativos = cursor.fetchone()['total']
    
    cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo = 'admin'")
    total_admins = cursor.fetchone()['total']
    
    # ✅ Sindicantes: apenas mestres e superiores (grau >= 3)
    cursor.execute("SELECT COUNT(*) as total FROM usuarios WHERE tipo = 'sindicante' AND ativo = 1 AND grau_atual >= 3")
    total_sindicantes = cursor.fetchone()['total']
    
    # ✅ Estatísticas por grau (com nome principal)
    cursor.execute("""
        SELECT 
            grau_atual,
            COUNT(*) as total
        FROM usuarios 
        WHERE ativo = 1
        GROUP BY grau_atual
        ORDER BY grau_atual
    """)
    estatisticas_graus_raw = cursor.fetchall()
    
    estatisticas_graus = []
    for eg in estatisticas_graus_raw:
        estatisticas_graus.append({
            'grau_atual': eg['grau_atual'],
            'grau_nome': get_grau_principal(eg['grau_atual']),
            'grau_detalhado': get_grau_detalhado(eg['grau_atual']),
            'total': eg['total']
        })
    
    return_connection(conn)
    
    # Montar dicionário de filtros para o template
    filtros = {
        'nome': nome,
        'grau': grau,
        'cargo': cargo,
        'loja': loja,
        'status': status
    }
    
    # Estatísticas para o template
    estatisticas = {
        'total_ativos': total_ativos,
        'total_inativos': total_inativos,
        'total_obreiros': total_ativos + total_inativos,
        'total_admins': total_admins,
        'total_sindicantes': total_sindicantes,
        'por_grau': estatisticas_graus,
        'exibidos': len(obreiros_list)
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
        data_iniciacao = data_iniciacao if data_iniciacao and data_iniciacao.strip() else None
        data_elevacao = data_elevacao if data_elevacao and data_elevacao.strip() else None
        data_exaltacao = data_exaltacao if data_exaltacao and data_exaltacao.strip() else None
        if not usuario or not senha or not nome_completo:
            flash("Preencha os campos obrigatórios", "danger")
        else:
            try:
                senha_hash = generate_password_hash(senha)
                cursor.execute("""
                    INSERT INTO usuarios 
                    (usuario, senha_hash, tipo, data_cadastro, ativo, 
                     nome_completo, nome_maconico, cim_numero, grau_atual,
                     data_iniciacao, data_elevacao, data_exaltacao,
                     telefone, email, endereco,
                     loja_nome, loja_numero, loja_orient) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (usuario, senha_hash, tipo, datetime.now(), 1,
                      nome_completo, nome_maconico, cim_numero, grau_atual,
                      data_iniciacao, data_elevacao, data_exaltacao,
                      telefone, email, endereco,
                      loja_nome, loja_numero, loja_orient))
                conn.commit()
                obreiro_id = cursor.lastrowid
                if data_iniciacao:
                    cursor.execute("""
                        INSERT INTO historico_graus (obreiro_id, grau, data, observacao)
                        VALUES (%s, %s, %s, %s)
                    """, (obreiro_id, 1, data_iniciacao, "Iniciação"))
                    conn.commit()
                registrar_log("criar", "obreiro", obreiro_id, dados_novos={"nome": nome_completo, "usuario": usuario})
                flash(f"Obreiro '{nome_completo}' adicionado com sucesso!", "success")
                return_connection(conn)
                return redirect("/obreiros")
            except psycopg2.IntegrityError:
                flash("Erro: Usuário ou CIM já existe", "danger")
                conn.rollback()
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    cursor.execute("SELECT * FROM graus WHERE ativo = 1 ORDER BY nivel, ordem")
    graus = cursor.fetchall()
    return_connection(conn)
    return render_template("obreiros/novo.html", lojas=lojas, graus=graus)

@app.route("/obreiros/<int:id>")
@login_required
def visualizar_obreiro(id):
    cursor, conn = get_db()
    try:
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
        if session["tipo"] != "admin" and session["user_id"] != id:
            flash("Você não tem permissão para visualizar este obreiro", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        cursor.execute("""
            SELECT oc.*, c.nome as cargo_nome, c.sigla
            FROM ocupacao_cargos oc
            JOIN cargos c ON oc.cargo_id = c.id
            WHERE oc.obreiro_id = %s AND oc.ativo = 1
            ORDER BY oc.data_inicio DESC
        """, (id,))
        cargos = cursor.fetchall()
        cursor.execute("""
            SELECT h.*, g.nome as grau_nome
            FROM historico_graus h
            LEFT JOIN graus g ON h.grau_id = g.id
            WHERE h.obreiro_id = %s
            ORDER BY h.data DESC
        """, (id,))
        historico_graus = cursor.fetchall()
        cursor.execute("SELECT nome FROM graus WHERE nivel = %s", (obreiro['grau_atual'],))
        grau_atual_info = cursor.fetchone()
        nome_grau_atual = grau_atual_info['nome'] if grau_atual_info else None
        cursor.execute("SELECT COUNT(*) as total FROM familiares WHERE obreiro_id = %s", (id,))
        familiares_count = cursor.fetchone()["total"]
        cursor.execute("SELECT COUNT(*) as total FROM condecoracoes_obreiro WHERE obreiro_id = %s", (id,))
        condecoracoes_count = cursor.fetchone()["total"]
        cargos_disponiveis = []
        if session["tipo"] == "admin":
            cursor.execute("SELECT * FROM cargos WHERE ativo = 1 ORDER BY ordem")
            cargos_disponiveis = cursor.fetchall()
        graus_disponiveis = []
        if session["tipo"] == "admin":
            cursor.execute("SELECT * FROM graus WHERE ativo = 1 ORDER BY nivel, ordem")
            graus_disponiveis = cursor.fetchall()
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
                              obreiro=obreiro, cargos=cargos, historico_graus=historico_graus,
                              cargos_disponiveis=cargos_disponiveis, graus_disponiveis=graus_disponiveis,
                              familiares_count=familiares_count, condecoracoes_count=condecoracoes_count,
                              ultimas_condecoracoes=ultimas_condecoracoes, nome_grau_atual=nome_grau_atual,
                              pode_editar=(session["tipo"] == "admin" or session["user_id"] == id))
    except Exception as e:
        print(f"Erro ao visualizar obreiro: {e}")
        if conn:
            return_connection(conn)
        flash(f"Erro ao carregar dados do obreiro: {str(e)}", "danger")
        return redirect("/obreiros")
    
        # ============================================
        # PROCESSAR UPLOAD DA FOTO
        # ============================================
        foto_path = None
        
        if 'foto' in request.files:
            arquivo_foto = request.files['foto']
            if arquivo_foto and arquivo_foto.filename:
                # Salvar nova foto
                from werkzeug.utils import secure_filename
                from datetime import datetime
                import os
                
                # Criar diretório se não existir
                UPLOAD_FOLDER = 'uploads/obreiros'
                os.makedirs(UPLOAD_FOLDER, exist_ok=True)
                
                # Verificar extensão
                ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
                ext = arquivo_foto.filename.rsplit('.', 1)[1].lower() if '.' in arquivo_foto.filename else ''
                
                if ext in ALLOWED_EXTENSIONS:
                    # Gerar nome único
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    nome_seguro = secure_filename(arquivo_foto.filename)
                    nome_arquivo = f"{timestamp}_{nome_seguro}"
                    caminho_arquivo = os.path.join(UPLOAD_FOLDER, nome_arquivo)
                    
                    # Salvar arquivo
                    arquivo_foto.save(caminho_arquivo)
                    foto_path = f"uploads/obreiros/{nome_arquivo}"
                    print(f"✅ Foto salva: {foto_path}")
                else:
                    flash("Formato de imagem não permitido. Use: PNG, JPG, JPEG, GIF ou WEBP", "warning")
        
        if session["tipo"] == "admin":
            tipo = request.form.get("tipo", "obreiro")
            grau_atual = request.form.get("grau_atual", 1)
            ativo = 1 if request.form.get("ativo") else 0
            data_iniciacao = request.form.get("data_iniciacao", "")
            data_elevacao = request.form.get("data_elevacao", "")
            data_exaltacao = request.form.get("data_exaltacao", "")
            
            data_iniciacao = data_iniciacao if data_iniciacao and data_iniciacao.strip() else None
            data_elevacao = data_elevacao if data_elevacao and data_elevacao.strip() else None
            data_exaltacao = data_exaltacao if data_exaltacao and data_exaltacao.strip() else None
            
            try:
                grau_atual = int(grau_atual)
            except ValueError:
                grau_atual = 1
            
            # Construir query com ou sem foto
            if foto_path:
                cursor.execute("""
                    UPDATE usuarios 
                    SET nome_completo = %s, nome_maconico = %s, cim_numero = %s, tipo = %s,
                        grau_atual = %s, data_iniciacao = %s, data_elevacao = %s, data_exaltacao = %s,
                        telefone = %s, email = %s, endereco = %s,
                        loja_nome = %s, loja_numero = %s, loja_orient = %s, ativo = %s,
                        foto = %s
                    WHERE id = %s
                """, (nome_completo, nome_maconico, cim_numero, tipo,
                      grau_atual, data_iniciacao, data_elevacao, data_exaltacao,
                      telefone, email, endereco,
                      loja_nome, loja_numero, loja_orient, ativo, foto_path, id))
            else:
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
            
            if grau_atual != grau_antigo:
                cursor.execute("SELECT nome FROM graus WHERE nivel = %s", (grau_atual,))
                grau_info = cursor.fetchone()
                nome_grau = grau_info['nome'] if grau_info else f"Grau {grau_atual}"
                cursor.execute("""
                    INSERT INTO historico_graus (obreiro_id, grau, data, observacao)
                    VALUES (%s, %s, %s, %s)
                """, (id, grau_atual, datetime.now().date(), f"Atualização de grau para {nome_grau}"))
                flash(f"Grau alterado. Registro adicionado ao histórico!", "info")
        else:
            # Não admin - atualizar sem campos admin
            if foto_path:
                cursor.execute("""
                    UPDATE usuarios 
                    SET nome_completo = %s, nome_maconico = %s, cim_numero = %s,
                        telefone = %s, email = %s, endereco = %s,
                        loja_nome = %s, loja_numero = %s, loja_orient = %s,
                        foto = %s
                    WHERE id = %s
                """, (nome_completo, nome_maconico, cim_numero,
                      telefone, email, endereco,
                      loja_nome, loja_numero, loja_orient, foto_path, id))
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
        
        # Atualizar sessão se for o próprio perfil
        if session["user_id"] == id:
            session["nome_completo"] = nome_completo
            session["cim_numero"] = cim_numero
            session["loja_nome"] = loja_nome
            session["loja_numero"] = loja_numero
            session["loja_orient"] = loja_orient
        
        registrar_log("editar", "obreiro", id, dados_anteriores=dados_antigos, dados_novos={"nome": nome_completo})
        flash("Perfil atualizado com sucesso!", "success")
        return_connection(conn)
        return redirect(f"/obreiros/{id}")
    
    # GET - Carregar dados
    cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
    obreiro = cursor.fetchone()
    cursor.execute("SELECT * FROM lojas ORDER BY nome")
    lojas = cursor.fetchall()
    cursor.execute("SELECT * FROM graus WHERE ativo = 1 ORDER BY nivel, ordem")
    graus = cursor.fetchall()
    cursor.execute("SELECT nome FROM graus WHERE nivel = %s", (obreiro['grau_atual'],))
    grau_atual_info = cursor.fetchone()
    nome_grau_atual = grau_atual_info['nome'] if grau_atual_info else None
    
    return_connection(conn)
    
    return render_template("obreiros/editar.html", 
                          obreiro=obreiro, 
                          lojas=lojas, 
                          graus=graus,
                          nome_grau_atual=nome_grau_atual, 
                          is_admin=(session["tipo"] == "admin"),
                          is_own_profile=(session["user_id"] == id))
from datetime import datetime

@app.route("/obreiros/<int:id>/editar", methods=["GET", "POST"])
@login_required
def editar_obreiro(id):

    # 🔐 Controle de acesso
    if session["tipo"] != "admin" and session["user_id"] != id:
        flash("Você não tem permissão para editar este obreiro", "danger")
        return redirect("/obreiros")

    cursor, conn = get_db()

    try:
        # 🔎 Buscar dados atuais
        cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
        obreiro = cursor.fetchone()

        if not obreiro:
            flash("Obreiro não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")

        # =============================
        # 📥 POST (SALVAR ALTERAÇÕES)
        # =============================
        if request.method == "POST":

            # 🧾 Campos do formulário
            nome_completo = request.form.get("nome_completo")
            nome_maconico = request.form.get("nome_maconico")
            cim_numero = request.form.get("cim_numero")
            telefone = request.form.get("telefone")
            email = request.form.get("email")
            endereco = request.form.get("endereco")
            loja_nome = request.form.get("loja_nome")
            loja_numero = request.form.get("loja_numero")
            loja_orient = request.form.get("loja_orient")
            senha = request.form.get("senha", "")

            # 🔥 CAMPO TIPO
            tipo = request.form.get("tipo", obreiro["tipo"])

            # =============================
            # 🎯 GRAU (capturar do campo hidden)
            # =============================
            grau_antigo = obreiro["grau_atual"]
            
            # ✅ CAPTURAR O GRAU DO CAMPO HIDDEN
            grau_form = request.form.get("grau_atual")
            print(f"🔍 DEBUG - grau_form recebido: {grau_form}")
            print(f"🔍 DEBUG - tipo: {type(grau_form)}")
            
            if grau_form and str(grau_form).strip().isdigit():
                grau_atual = int(grau_form)
            else:
                grau_atual = grau_antigo
            
            print(f"🔍 DEBUG - grau_atual final: {grau_atual}")

            # =============================
            # 🔒 STATUS (somente admin)
            # =============================
            if session["tipo"] == "admin":
                ativo = request.form.get("ativo", obreiro["ativo"])
                if ativo:
                    ativo = int(ativo)
                else:
                    ativo = obreiro["ativo"]
            else:
                ativo = obreiro["ativo"]

            # =============================
            # ✅ VALIDAÇÃO DE SINDICANTE
            # =============================
            if tipo == 'sindicante' and grau_atual < 3:
                flash("⚠️ Apenas obreiros com grau de Mestre (3) ou superior podem ser Sindicantes!", "danger")
                return_connection(conn)
                return redirect(f"/obreiros/{id}/editar")

            # =============================
            # 💾 UPDATE COMPLETO
            # =============================
            cursor.execute("""
                UPDATE usuarios SET
                    nome_completo = %s,
                    nome_maconico = %s,
                    cim_numero = %s,
                    telefone = %s,
                    email = %s,
                    endereco = %s,
                    loja_nome = %s,
                    loja_numero = %s,
                    loja_orient = %s,
                    grau_atual = %s,
                    ativo = %s,
                    tipo = %s
                WHERE id = %s
            """, (
                nome_completo,
                nome_maconico,
                cim_numero,
                telefone,
                email,
                endereco,
                loja_nome,
                loja_numero,
                loja_orient,
                grau_atual,  # ✅ USANDO O GRAU CAPTURADO
                ativo,
                tipo,
                id
            ))

            # =============================
            # 🔐 ATUALIZAR SENHA SE FORNECIDA
            # =============================
            if senha and len(senha) >= 6:
                import hashlib
                senha_hash = hashlib.sha256(senha.encode()).hexdigest()
                cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (senha_hash, id))

            # =============================
            # 📜 HISTÓRICO DE GRAU
            # =============================
            if (
                session["tipo"] == "admin"
                and grau_atual != grau_antigo
                and grau_atual > 0
            ):
                try:
                    # Buscar nome do grau
                    cursor.execute("SELECT nome FROM graus WHERE nivel = %s", (grau_atual,))
                    grau_info = cursor.fetchone()
                    grau_nome = grau_info['nome'] if grau_info else f"Grau {grau_atual}"
                    
                    cursor.execute("""
                        INSERT INTO historico_graus 
                        (obreiro_id, grau, data, observacao)
                        VALUES (%s, %s, CURRENT_DATE, %s)
                    """, (
                        id,
                        grau_atual,
                        f"Alteração de grau de {grau_antigo} para {grau_atual} - {grau_nome}"
                    ))
                except Exception as e:
                    print(f"⚠️ Erro ao registrar histórico: {e}")

            conn.commit()
            
            # ✅ VERIFICAR SE O GRAU FOI SALVO
            cursor.execute("SELECT grau_atual FROM usuarios WHERE id = %s", (id,))
            verificar_grau = cursor.fetchone()
            print(f"🔍 DEBUG - Grau no banco após UPDATE: {verificar_grau['grau_atual']}")

            flash("Obreiro atualizado com sucesso!", "success")
            return_connection(conn)
            return redirect(f"/obreiros/{id}")

    except Exception as e:
        print(f"❌ Erro ao editar obreiro: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
        flash(f"Erro ao atualizar: {str(e)}", "danger")
        return_connection(conn)
        return redirect(f"/obreiros/{id}")

    # =============================
    # 📊 GET (CARREGAR TELA)
    # =============================
    cursor.execute("SELECT * FROM usuarios WHERE id = %s", (id,))
    obreiro = cursor.fetchone()

    cursor.execute("SELECT id, nome, numero, oriente FROM lojas WHERE ativo = 1 ORDER BY nome")
    lojas = cursor.fetchall()

    cursor.execute("SELECT nivel, nome FROM graus ORDER BY nivel")
    graus = cursor.fetchall()
    
    # Buscar graus superiores (nivel >= 4)
    cursor.execute("SELECT nivel, nome FROM graus WHERE nivel >= 4 ORDER BY nivel")
    graus_superiores = cursor.fetchall()

    return_connection(conn)

    return render_template(
        "obreiros/editar.html",
        obreiro=obreiro,
        lojas=lojas,
        graus=graus,
        graus_superiores=graus_superiores,
        is_admin=(session["tipo"] == "admin"),
        is_own_profile=(session["user_id"] == id)
    )


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
        cursor.execute("SELECT obreiro_id, cargo_id FROM ocupacao_cargos WHERE id = %s", (id,))
        cargo = cursor.fetchone()
        if not cargo:
            flash("Cargo não encontrado", "danger")
            return_connection(conn)
            return redirect("/obreiros")
        obreiro_id = cargo["obreiro_id"]
        cargo_id = cargo["cargo_id"]
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
        registrar_log("registrar_grau", "obreiro", id, dados_novos={"grau": grau['nome'], "data": data})
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
    if session["tipo"] != "admin" and session["user_id"] != obreiro_id:
        flash("Você não tem permissão para acessar esta página", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    cursor, conn = get_db()
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
    return render_template("obreiros/familiares.html", obreiro=obreiro, familiares=familiares_list, obreiro_id=obreiro_id)

@app.route("/obreiros/<int:obreiro_id>/familiares/novo", methods=["GET", "POST"])
@login_required
def novo_familiar(obreiro_id):
    if session["tipo"] != "admin" and session["user_id"] != obreiro_id:
        flash("Você não tem permissão para acessar esta página", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    cursor, conn = get_db()
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
    cursor, conn = get_db()
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
        SELECT r.id, r.titulo, r.tipo, r.grau, r.data, r.hora_inicio, 
               r.hora_termino, r.local, r.loja_id, r.pauta, r.observacoes, 
               r.status, r.criado_por,
               l.nome as loja_nome, t.cor,
               COUNT(p.id) as total_presentes,
               SUM(CASE WHEN p.presente = 1 THEN 1 ELSE 0 END) as presentes_confirmados
        FROM reunioes r
        LEFT JOIN lojas l ON r.loja_id = l.id
        LEFT JOIN tipos_reuniao t ON r.tipo = t.nome
        LEFT JOIN presenca p ON r.id = p.reuniao_id
        WHERE 1=1
    """
    params = []
    if tipo_usuario != "admin":
        if nivel_usuario == 1:
            query += " AND (r.grau = 1 OR r.grau IS NULL)"
        elif nivel_usuario == 2:
            query += " AND (r.grau IN (1, 2) OR r.grau IS NULL)"
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
    query += """ 
        GROUP BY r.id, r.titulo, r.tipo, r.grau, r.data, r.hora_inicio, 
                 r.hora_termino, r.local, r.loja_id, r.pauta, r.observacoes, 
                 r.status, r.criado_por, l.nome, t.cor
        ORDER BY r.data DESC, r.hora_inicio DESC
    """
    cursor.execute(query, params)
    reunioes = cursor.fetchall()
    cursor.execute("SELECT DISTINCT tipo FROM reunioes ORDER BY tipo")
    tipos = cursor.fetchall()
    cursor.execute("SELECT DISTINCT status FROM reunioes ORDER BY status")
    status_list = cursor.fetchall()
    cursor.execute("SELECT DISTINCT grau FROM reunioes WHERE grau IS NOT NULL ORDER BY grau")
    graus = cursor.fetchall()
    return_connection(conn)
    return render_template("reunioes/lista.html", reunioes=reunioes, tipos=tipos, status_list=status_list,
                          graus=graus, filtros={'data_ini': data_ini, 'data_fim': data_fim,
                                  'tipo': tipo, 'status': status, 'grau': grau, 'local': local})

@app.route("/reunioes/calendario")
@login_required
def calendario_reunioes():
    return render_template("reunioes/calendario.html")

@app.route("/api/reunioes")
@login_required
def api_reunioes():
    cursor, conn = get_db()
    nivel_usuario = session.get("nivel_acesso", 1)
    tipo_usuario = session.get("tipo", "obreiro")
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
    if tipo_usuario != "admin":
        if nivel_usuario == 1:
            query += " AND (r.grau = 1 OR r.grau IS NULL)"
        elif nivel_usuario == 2:
            query += " AND (r.grau IN (1, 2) OR r.grau IS NULL)"
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

@app.route("/reunioes/nova", methods=["GET", "POST"])
@admin_required
def nova_reuniao():
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
        
        if not titulo or not tipo or not data or not hora_inicio:
            flash("Preencha todos os campos obrigatórios (Título, Tipo, Data e Horário)", "danger")
            return_connection(conn)
            return redirect("/reunioes/nova")
        
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
        
        try:
            data_obj = datetime.strptime(data, '%Y-%m-%d').date() if data else None
            hora_inicio_obj = datetime.strptime(hora_inicio, '%H:%M').time() if hora_inicio else None
            hora_termino_obj = datetime.strptime(hora_termino, '%H:%M').time() if hora_termino else None
        except ValueError as e:
            flash(f"Erro no formato da data/hora: {str(e)}", "danger")
            return_connection(conn)
            return redirect("/reunioes/nova")
        
        try:
            # Inserir reunião
            cursor.execute("""
                INSERT INTO reunioes 
                (titulo, tipo, grau, data, hora_inicio, hora_termino, local, loja_id, pauta, observacoes, criado_por)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (titulo, tipo, grau, data_obj, hora_inicio_obj, hora_termino_obj, 
                  local, loja_id, pauta, observacoes, session["user_id"]))
            conn.commit()
            reuniao_id = cursor.lastrowid
            
            registrar_log("criar", "reuniao", reuniao_id, dados_novos={"titulo": titulo, "data": data, "tipo": tipo})
            
            # ============================================
            # ENVIO DE E-MAILS VIA RESEND (CORRIGIDO)
            # ============================================
            emails_enviados = 0
            
            try:
                # Buscar participantes que receberão o e-mail
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
                    
                    # Formatar data para exibição
                    data_formatada = data_obj.strftime('%d/%m/%Y') if data_obj else data
                    hora_formatada = hora_inicio_obj.strftime('%H:%M') if hora_inicio_obj else hora_inicio
                    
                    # Buscar nome do tipo de reunião - CORRIGIDO: tipo já é o nome, não o ID
                    # O campo 'tipo' na tabela reunioes armazena o nome, não o ID
                    tipo_nome = tipo  # tipo já é o nome da reunião (ex: "Ordinária")
                    
                    # Buscar nome da loja (se houver loja_id)
                    loja_nome = None
                    if loja_id:
                        try:
                            cursor.execute("SELECT nome FROM lojas WHERE id = %s", (loja_id,))
                            loja_result = cursor.fetchone()
                            loja_nome = loja_result['nome'] if loja_result else None
                        except Exception as e:
                            print(f"Erro ao buscar loja: {e}")
                            loja_nome = None
                    
                    # Dados da reunião
                    dados_reuniao = {
                        'titulo': titulo,
                        'tipo': tipo_nome,
                        'grau': grau,
                        'data': data_formatada,
                        'hora_inicio': hora_formatada,
                        'hora_termino': hora_termino_obj.strftime('%H:%M') if hora_termino_obj else None,
                        'local': local or (loja_nome if loja_nome else 'Templo Maçônico'),
                        'pauta': pauta,
                        'observacoes': observacoes
                    }
                    
                    # Enviar e-mail para cada participante
                    for participante in participantes:
                        try:
                            # Verificar se a função existe
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

    # Buscar ata
    cursor.execute("""
        SELECT id, aprovada, numero_ata, ano_ata 
        FROM atas 
        WHERE reuniao_id = %s
    """, (id,))
    ata_row = cursor.fetchone()
    ata_id = ata_row["id"] if ata_row else None
    ata_aprovada = ata_row["aprovada"] if ata_row else None
    ata_numero = ata_row["numero_ata"] if ata_row else None
    ata_ano = ata_row["ano_ata"] if ata_row else None

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
                          tipos_ausencia=tipos_ausencia)

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
        if not titulo or not tipo or not data or not hora_inicio:
            flash("Preencha todos os campos obrigatórios", "danger")
            return_connection(conn)
            return redirect(f"/reunioes/{id}/editar")
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
        cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
        dados_antigos = dict(cursor.fetchone())
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
        cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
        reuniao_antiga = cursor.fetchone()
        if not reuniao_antiga:
            flash("Reunião não encontrada", "danger")
            return_connection(conn)
            return redirect("/reunioes")
        cursor.execute("UPDATE reunioes SET status = %s WHERE id = %s", (novo_status, id))
        conn.commit()
        registrar_log("alterar_status", "reuniao", id, dados_anteriores={"status": reuniao_antiga["status"]},
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
    cursor.execute("SELECT * FROM reunioes WHERE id = %s", (id,))
    reuniao = cursor.fetchone()
    if not reuniao:
        flash("Reunião não encontrada", "danger")
        return_connection(conn)
        return redirect("/reunioes")
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
# ROTAS DE ATAS (CORRIGIDAS)
# =============================

@app.route("/atas/<int:id>")
@login_required
def ver_ata_por_id(id):
    """Visualizar ata pelo ID"""
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
        
        # Buscar assinaturas da ata
        cursor.execute("""
            SELECT 
                s.id,
                s.data_assinatura,
                s.ip_assinatura,
                s.validada,
                u.nome_completo,
                u.grau_atual,
                c.nome as cargo_nome
            FROM assinaturas_ata s
            LEFT JOIN usuarios u ON s.obreiro_id = u.id
            LEFT JOIN cargos c ON s.cargo_id = c.id
            WHERE s.ata_id = %s
            ORDER BY s.data_assinatura DESC
        """, (id,))
        
        assinaturas = cursor.fetchall()
        
        return_connection(conn)
        
        return render_template("atas/visualizar.html", ata=ata, presenca=presenca, assinaturas=assinaturas)
        
    except Exception as e:
        print(f"❌ Erro ao ver ata {id}: {e}")
        import traceback
        traceback.print_exc()
        
        if 'conn' in locals():
            return_connection(conn)
        
        flash(f"Erro ao carregar ata: {str(e)}", "danger")
        return redirect("/atas")

@app.route("/atas")
@login_required
def listar_atas():
    """Lista todas as atas"""
    try:
        cursor, conn = get_db()
        
        # Buscar atas com informações da reunião e total de assinaturas
        cursor.execute("""
            SELECT 
                a.id,
                a.reuniao_id,
                a.numero_ata as numero,
                a.data_criacao as data,
                a.conteudo,
                a.aprovada,
                a.aprovada_em,
                a.redator_id as criado_por,
                a.data_criacao as created_at,
                r.titulo as reuniao_titulo,
                r.data as reuniao_data,
                u.nome_completo as criado_por_nome,
                (SELECT COUNT(*) FROM assinaturas_ata WHERE ata_id = a.id) as total_assinaturas
            FROM atas a
            LEFT JOIN reunioes r ON a.reuniao_id = r.id
            LEFT JOIN usuarios u ON a.redator_id = u.id
            ORDER BY a.data_criacao DESC, a.numero_ata DESC
        """)
        
        atas = cursor.fetchall()
        return_connection(conn)
        
        filtros = {}
        
        return render_template("atas/lista.html", atas=atas, filtros=filtros)
        
    except Exception as e:
        print(f"❌ Erro ao listar atas: {e}")
        import traceback
        traceback.print_exc()
        
        if 'conn' in locals():
            return_connection(conn)
        
        flash(f"Erro ao carregar atas: {str(e)}", "danger")
        return redirect("/dashboard")


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
        
        # Buscar assinaturas da ata
        cursor.execute("""
            SELECT 
                s.id,
                s.ata_id,
                s.obreiro_id,
                s.cargo_id,
                s.data_assinatura,
                s.ip_assinatura,
                s.validada,
                u.nome_completo,
                u.grau_atual,
                c.nome as cargo_nome
            FROM assinaturas_ata s
            LEFT JOIN usuarios u ON s.obreiro_id = u.id
            LEFT JOIN cargos c ON s.cargo_id = c.id
            WHERE s.ata_id = %s
            ORDER BY s.data_assinatura DESC
        """, (id,))
        
        assinaturas = cursor.fetchall()
        
        return_connection(conn)
        
        return render_template("atas/visualizar.html", ata=ata, presenca=presenca, assinaturas=assinaturas)
        
    except Exception as e:
        print(f"❌ Erro ao visualizar ata completa {id}: {e}")
        import traceback
        traceback.print_exc()
        
        if 'conn' in locals():
            return_connection(conn)
        
        flash(f"Erro ao carregar ata: {str(e)}", "danger")
        return redirect("/atas")


@app.route("/atas/nova/<int:reuniao_id>", methods=["GET", "POST"])
@admin_required
def nova_ata(reuniao_id):
    cursor, conn = get_db()
    
    # Buscar reunião
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
        
        try:
            # Obter número da ata para o ano atual
            ano_atual = datetime.now().year
            cursor.execute("SELECT COUNT(*) as total FROM atas WHERE ano_ata = %s", (ano_atual,))
            total = cursor.fetchone()["total"]
            numero_ata = total + 1
            
            # Inserir ata
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
    cursor.execute("SELECT * FROM modelos_ata WHERE ativo = 1")
    modelos = cursor.fetchall()
    return_connection(conn)
    
    return render_template("atas/nova.html", reuniao=reuniao, modelos=modelos)


@app.route("/atas/<int:id>/editar", methods=["GET", "POST"])
@admin_required
def editar_ata(id):
    cursor, conn = get_db()
    
    # Buscar ata com informações da reunião
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
        return redirect(f"/atas/{id}/visualizar")
    
    if request.method == "POST":
        conteudo = request.form.get("conteudo")
        
        if not conteudo:
            flash("Conteúdo da ata é obrigatório", "danger")
        else:
            try:
                # Obter versão atual
                versao_atual = ata.get('versao') or 0
                nova_versao = versao_atual + 1
                
                cursor.execute("""
                    UPDATE atas 
                    SET conteudo = %s, versao = %s
                    WHERE id = %s
                """, (conteudo, nova_versao, id))
                
                conn.commit()
                registrar_log("editar", "ata", id, dados_novos={"versao": nova_versao})
                flash("Ata atualizada com sucesso!", "success")
                return_connection(conn)
                return redirect(f"/atas/{id}/visualizar")
                
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
    flash("Ata aprovada com sucesso!", "success")
    return_connection(conn)
    return redirect(f"/atas/{id}/visualizar")


@app.route("/atas/<int:id>/assinar", methods=["POST"])
@login_required
def assinar_ata(id):
    cursor, conn = get_db()
    
    try:
        # Verificar se já existe assinatura
        cursor.execute("""
            SELECT id FROM assinaturas_ata 
            WHERE ata_id = %s AND obreiro_id = %s
        """, (id, session["user_id"]))
        
        if cursor.fetchone():
            flash("Você já assinou esta ata", "warning")
        else:
            # Buscar cargo do obreiro
            cursor.execute("""
                SELECT cargo_id FROM ocupacao_cargos 
                WHERE obreiro_id = %s AND ativo = 1
                ORDER BY data_inicio DESC LIMIT 1
            """, (session["user_id"],))
            cargo = cursor.fetchone()
            cargo_id = cargo["cargo_id"] if cargo else None
            
            # Inserir assinatura
            cursor.execute("""
                INSERT INTO assinaturas_ata (ata_id, obreiro_id, cargo_id, ip_assinatura, data_assinatura, validada)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, 1)
            """, (id, session["user_id"], cargo_id, request.remote_addr))
            
            conn.commit()
            registrar_log("assinar", "ata", id)
            flash("Ata assinada com sucesso!", "success")
            
    except Exception as e:
        print(f"Erro ao assinar ata: {e}")
        conn.rollback()
        flash(f"Erro ao assinar ata: {str(e)}", "danger")
    
    return_connection(conn)
    return redirect(f"/atas/{id}/visualizar")


@app.route("/atas/<int:id>/excluir", methods=["POST"])
@admin_required
def excluir_ata(id):
    cursor, conn = get_db()
    
    try:
        # Buscar dados da ata antes de excluir
        cursor.execute("""
            SELECT a.id, a.numero_ata, a.ano_ata, a.aprovada, a.reuniao_id,
                   r.titulo as reuniao_titulo
            FROM atas a
            JOIN reunioes r ON a.reuniao_id = r.id
            WHERE a.id = %s
        """, (id,))
        ata = cursor.fetchone()
        
        if not ata:
            flash("Ata não encontrada", "danger")
            return_connection(conn)
            return redirect("/atas")
        
        # Verificar se é admin
        if session.get("tipo") != "admin":
            flash("Apenas administradores podem excluir atas", "danger")
            return_connection(conn)
            return redirect("/atas")
        
        # Registrar log antes de excluir
        registrar_log("excluir_ata", "ata", id, dados_anteriores={
            "numero": ata['numero_ata'],
            "ano": ata['ano_ata'],
            "reuniao": ata['reuniao_titulo'],
            "aprovada": "Sim" if ata['aprovada'] == 1 else "Não"
        })
        
        # Excluir assinaturas primeiro
        cursor.execute("DELETE FROM assinaturas_ata WHERE ata_id = %s", (id,))
        
        # Excluir a ata
        cursor.execute("DELETE FROM atas WHERE id = %s", (id,))
        
        conn.commit()
        
        flash(f"Ata {ata['numero_ata']}/{ata['ano_ata']} excluída com sucesso!", "success")
        
    except Exception as e:
        print(f"Erro ao excluir ata: {e}")
        traceback.print_exc()
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
    return render_template("comunicados/lista.html", comunicados=comunicados, tipos=tipos,
                          prioridades=prioridades, filtros={'tipo': tipo, 'prioridade': prioridade,
                                  'data_ini': data_ini, 'data_fim': data_fim, 'ativo': ativo})

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
        
        if not titulo or not conteudo or not tipo:
            flash("Preencha todos os campos obrigatórios (Título, Conteúdo e Tipo)", "danger")
            return redirect("/comunicados/novo")
        
        cursor, conn = get_db()
        
        try:
            # Obter próximo ID para comunicados
            cursor.execute("SELECT MAX(id) FROM comunicados")
            max_id = cursor.fetchone()['max']
            next_id = (max_id or 0) + 1
            
            # Inserir comunicado com ID explícito
            cursor.execute("""
                INSERT INTO comunicados (
                    id, titulo, conteudo, tipo, prioridade, 
                    data_inicio, data_fim, ativo, criado_por, data_criacao
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (next_id, titulo, conteudo, tipo, prioridade, data_inicio, data_fim, 1, session["user_id"]))
            
            conn.commit()
            comunicado_id = next_id
            
            registrar_log("criar", "comunicado", comunicado_id, dados_novos={"titulo": titulo, "prioridade": prioridade})
            flash("Comunicado publicado com sucesso!", "success")
            
            return_connection(conn)
            return redirect("/comunicados")
            
        except Exception as e:
            print(f"Erro ao criar comunicado: {e}")
            conn.rollback()
            flash(f"Erro ao criar comunicado: {str(e)}", "danger")
            return_connection(conn)
            return redirect("/comunicados/novo")
    
    hoje = datetime.now().strftime("%Y-%m-%d")
    return render_template("comunicados/novo.html", hoje=hoje)

@app.route("/comunicados/<int:id>/visualizar")
@login_required
def visualizar_comunicado(id):
    cursor, conn = get_db()
    
    try:
        # Registrar visualização
        cursor.execute("""
            SELECT id FROM visualizacoes_comunicado 
            WHERE comunicado_id = %s AND obreiro_id = %s
        """, (id, session["user_id"]))
        
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO visualizacoes_comunicado (comunicado_id, obreiro_id)
                VALUES (%s, %s)
            """, (id, session["user_id"]))
            conn.commit()
    except Exception as e:
        print(f"Erro ao registrar visualização: {e}")
        conn.rollback()
    
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
        flash("Comunicado nao encontrado", "danger")
    return_connection(conn)
    return redirect("/comunicados")

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
    
    cursor.execute("""
        SELECT c.*,
               (SELECT COUNT(*) FROM sindicancias WHERE candidato_id = c.id) as total_votos,
               (SELECT COUNT(*) FROM sindicancias WHERE candidato_id = c.id AND parecer = 'positivo') as votos_positivos,
               (SELECT COUNT(*) FROM sindicancias WHERE candidato_id = c.id AND parecer = 'negativo') as votos_negativos
        FROM candidatos c
        ORDER BY c.data_criacao DESC
    """)
    candidatos = cursor.fetchall()
    
    # ✅ CORRIGIDO: Adicionado campo telefone
    cursor.execute("""
        SELECT id, usuario, nome_completo, cim_numero, 
               loja_nome, loja_numero, loja_orient, ativo,
               telefone
        FROM usuarios 
        WHERE tipo = 'sindicante' AND ativo = 1
        ORDER BY nome_completo
    """)
    sindicantes = cursor.fetchall()
    
    return_connection(conn)
    return render_template("candidatos.html", candidatos=candidatos, sindicantes=sindicantes, tipo=session["tipo"])

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
    cursor.execute("SELECT * FROM candidatos WHERE id = %s", (candidato_id,))
    candidato = cursor.fetchone()
    if not candidato:
        flash("Candidato não encontrado", "danger")
        return_connection(conn)
        return redirect("/candidatos")
    cursor.execute("SELECT * FROM filhos_candidato WHERE candidato_id = %s ORDER BY data_nascimento", (candidato_id,))
    filhos = cursor.fetchall()
    if request.method == "POST":
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
    
    cursor.execute("SELECT * FROM filhos_candidato WHERE candidato_id = %s ORDER BY data_nascimento", (id,))
    filhos = cursor.fetchall()
    
    bloqueado = candidato["fechado"] == 1
    usuario = session["usuario"]
    
    if request.method == "POST" and not bloqueado:
        parecer = request.form["parecer"]
        agora = datetime.now()
        
        # Verificar se já existe
        cursor.execute("""
            SELECT id FROM sindicancias 
            WHERE candidato_id = %s AND sindicante = %s
        """, (id, usuario))
        
        if cursor.fetchone():
            cursor.execute("""
                UPDATE sindicancias 
                SET parecer = %s, data_envio = %s
                WHERE candidato_id = %s AND sindicante = %s
            """, (parecer, agora, id, usuario))
        else:
            cursor.execute("""
                INSERT INTO sindicancias (candidato_id, sindicante, parecer, data_envio)
                VALUES (%s, %s, %s, %s)
            """, (id, usuario, parecer, agora))
        
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
    
    cursor.execute("SELECT * FROM sindicancias WHERE candidato_id = %s AND sindicante = %s", (id, usuario))
    meu_parecer = cursor.fetchone()
    
    cursor.execute("SELECT id FROM pareceres_conclusivos WHERE candidato_id = %s AND sindicante = %s", (id, usuario))
    parecer_conclusivo_existente = cursor.fetchone()
    
    return_connection(conn)
    
    total_votos = len(registros)
    votos_positivos = sum(1 for r in registros if r["parecer"] == "positivo")
    votos_negativos = total_votos - votos_positivos
    
    return render_template("sindicancia.html", candidato=candidato, filhos=filhos, registros=registros,
                          meu_parecer=meu_parecer, parecer_conclusivo_existente=parecer_conclusivo_existente,
                          total_votos=total_votos, votos_positivos=votos_positivos, votos_negativos=votos_negativos,
                          bloqueado=bloqueado, tipo=session["tipo"], usuario_atual=usuario)

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
    registrar_log("fechar_sindicancia_manual", "sindicancia", id, dados_anteriores=dados_antigos, dados_novos={"status": status})
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
        # Verificar se já existe
        cursor.execute("""
            SELECT id FROM pareceres_conclusivos 
            WHERE candidato_id = %s AND sindicante = %s
        """, (id, session["usuario"]))
        
        if cursor.fetchone():
            cursor.execute("""
                UPDATE pareceres_conclusivos 
                SET parecer_texto = %s, conclusao = %s, observacoes = %s,
                    cim_numero = %s, data_parecer = %s, data_envio = %s,
                    fontes = %s, loja_nome = %s, loja_numero = %s, loja_orient = %s
                WHERE candidato_id = %s AND sindicante = %s
            """, (parecer_texto, conclusao, observacoes, cim_numero, data_parecer, agora,
                  fontes_json, loja_nome, loja_numero, loja_orient, id, session["usuario"]))
        else:
            cursor.execute("""
                INSERT INTO pareceres_conclusivos 
                (candidato_id, sindicante, parecer_texto, conclusao, observacoes, 
                 cim_numero, data_parecer, data_envio, fontes, loja_nome, loja_numero, loja_orient)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (id, session["usuario"], parecer_texto, conclusao, observacoes,
                  cim_numero, data_parecer, agora, fontes_json,
                  loja_nome, loja_numero, loja_orient))
        
        conn.commit()
        registrar_log("salvar_parecer_conclusivo", "parecer_conclusivo", id, dados_novos={"conclusao": conclusao})
        flash("Parecer conclusivo salvo com sucesso!", "success")
        
        parecer_simples = "positivo" if conclusao == "APROVADO" else "negativo"
        
        # Atualizar sindicância simples
        cursor.execute("""
            SELECT id FROM sindicancias 
            WHERE candidato_id = %s AND sindicante = %s
        """, (id, session["usuario"]))
        
        if cursor.fetchone():
            cursor.execute("""
                UPDATE sindicancias 
                SET parecer = %s, data_envio = %s
                WHERE candidato_id = %s AND sindicante = %s
            """, (parecer_simples, agora, id, session["usuario"]))
        else:
            cursor.execute("""
                INSERT INTO sindicancias (candidato_id, sindicante, parecer, data_envio)
                VALUES (%s, %s, %s, %s)
            """, (id, session["usuario"], parecer_simples, agora))
        
        conn.commit()
        
    except Exception as e:
        print(f"Erro ao salvar parecer: {e}")
        conn.rollback()
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

# =============================
# ROTAS DE SINDICANTES
# =============================
@app.route("/sindicantes", methods=["GET", "POST"])
@admin_required
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
                cursor.execute("UPDATE usuarios SET tipo = 'sindicante' WHERE id = %s", (obreiro_id,))
                conn.commit()
                registrar_log("promover", "sindicante", obreiro_id, 
                            dados_novos={"nome": obreiro['nome_completo'], "grau": obreiro['grau_atual']})
                flash(f"✅ {obreiro['nome_completo']} foi promovido a Sindicante com sucesso!", "success")
            else:
                flash("Obreiro não encontrado ou não atende aos requisitos (precisa ser Mestre ou superior e estar ativo)", "danger")
        else:
            # CADASTRAR NOVO SINDICANTE
            usuario = request.form.get("usuario")
            senha = request.form.get("senha")
            nome_completo = request.form.get("nome_completo")
            cim_numero = request.form.get("cim_numero")
            grau_atual = request.form.get("grau_atual", 3)  # Padrão Mestre
            loja_nome = request.form.get("loja_nome")
            loja_numero = request.form.get("loja_numero")
            loja_orient = request.form.get("loja_orient")
            
            # Validações
            if not usuario or not senha:
                flash("Usuário e senha são obrigatórios!", "danger")
                return_connection(conn)
                return redirect("/sindicantes")
            
            # Verificar se usuário já existe
            cursor.execute("SELECT id FROM usuarios WHERE usuario = %s", (usuario,))
            if cursor.fetchone():
                flash("Usuário já existe!", "danger")
                return_connection(conn)
                return redirect("/sindicantes")
            
            # Verificar senha mínima
            if len(senha) < 6:
                flash("A senha deve ter no mínimo 6 caracteres!", "danger")
                return_connection(conn)
                return redirect("/sindicantes")
            
            import hashlib
            senha_hash = hashlib.sha256(senha.encode()).hexdigest()
            
            cursor.execute("""
                INSERT INTO usuarios 
                (usuario, senha_hash, tipo, nome_completo, cim_numero, grau_atual, 
                 loja_nome, loja_numero, loja_orient, ativo, data_cadastro)
                VALUES (%s, %s, 'sindicante', %s, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP)
                RETURNING id
            """, (usuario, senha_hash, nome_completo, cim_numero, grau_atual, 
                  loja_nome, loja_numero, loja_orient))
            
            novo_id = cursor.fetchone()['id']
            conn.commit()
            registrar_log("criar", "sindicante", novo_id, dados_novos={"usuario": usuario, "nome": nome_completo})
            flash(f"✅ Sindicante {usuario} cadastrado com sucesso!", "success")
        
        return_connection(conn)
        return redirect("/sindicantes")
    
    # =============================
    # GET - Listar sindicantes e obreiros mestres
    # =============================
    
    # ✅ Buscar sindicantes ativos (INCLUINDO grau_atual)
    cursor.execute("""
        SELECT id, usuario, nome_completo, cim_numero, loja_nome, loja_numero, loja_orient, 
               ativo, telefone, email, grau_atual
        FROM usuarios 
        WHERE tipo = 'sindicante' AND ativo = 1
        ORDER BY nome_completo
    """)
    sindicantes = cursor.fetchall()
    
    # ✅ Buscar obreiros que podem ser promovidos a sindicante (Mestre ou superior, ativo, não sindicante)
    cursor.execute("""
        SELECT id, usuario, nome_completo, cim_numero, loja_nome, grau_atual
        FROM usuarios 
        WHERE tipo = 'obreiro' 
        AND ativo = 1 
        AND grau_atual >= 3
        ORDER BY grau_atual DESC, nome_completo
    """)
    obreiros_mestres = cursor.fetchall()
    
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
# ROTAS DE LOJAS
# =============================

@app.route("/lojas")
@admin_required
def listar_lojas():
    cursor, conn = get_db()

    cursor.execute("""
        SELECT id, nome, numero, oriente, cidade, uf, ativo
        FROM lojas
        ORDER BY id DESC
    """)

    lojas = cursor.fetchall()
    return_connection(conn)

    return render_template("lojas_listar.html", lojas=lojas)

@app.route("/lojas/nova", methods=["GET", "POST"])
@admin_required
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
        horario_reuniao = safe(request.form.get("horario_reuniao"))
        dia_reuniao = safe(request.form.get("dia_reuniao"))
        rito = safe(request.form.get("rito"))
        observacoes = safe(request.form.get("observacoes"))

        try:
            cursor, conn = get_db()

            cursor.execute("""
                INSERT INTO lojas (
                    nome, numero, oriente,
                    cidade, uf,
                    endereco, bairro, cep,
                    telefone, email, site,
                    data_fundacao,
                    data_instalacao,
                    data_autorizacao,
                    veneravel_mestre,
                    secretario,
                    tesoureiro,
                    orador,
                    horario_reuniao,
                    dia_reuniao,
                    rito,
                    observacoes,
                    ativo,
                    created_by
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    1,
                    %s
                )
            """, (
                nome, numero, oriente,
                cidade, uf,
                endereco, bairro, cep,
                telefone, email, site,
                data_fundacao,
                data_instalacao,
                data_autorizacao,
                veneravel_mestre,
                secretario,
                tesoureiro,
                orador,
                horario_reuniao,
                dia_reuniao,
                rito,
                observacoes,
                session.get("user_id")
            ))

            conn.commit()
            return_connection(conn)

            flash("Loja criada com sucesso!", "success")
            return redirect("/lojas")

        except Exception as e:
            print("Erro ao criar loja:", e)
            conn.rollback()
            return_connection(conn)

            flash(str(e), "danger")
            return redirect("/lojas/nova")

    return render_template("lojas_nova.html")

@app.route("/lojas/editar/<int:id>", methods=["GET", "POST"])
@admin_required
def editar_loja(id):
    cursor, conn = get_db()

    if request.method == "POST":

        nome = request.form.get("nome")
        numero = request.form.get("numero")
        oriente = request.form.get("oriente")
        cidade = request.form.get("cidade")
        uf = request.form.get("uf")
        cep = request.form.get("cep")
        telefone = request.form.get("telefone")
        email = request.form.get("email")
        site = request.form.get("site")
        endereco = request.form.get("endereco")
        bairro = request.form.get("bairro")
        observacoes = request.form.get("observacoes")

        cursor.execute("""
    UPDATE lojas
    SET nome=%s,
        numero=%s,
        oriente=%s,
        cidade=%s,
        uf=%s,
        cep=%s,
        telefone=%s,
        email=%s,
        site=%s,
        endereco=%s,
        bairro=%s,
        observacoes=%s
    WHERE id=%s
""", (
    nome,
    numero,
    oriente,
    cidade,
    uf,
    cep,
    telefone,
    email,
    site,
    endereco,
    bairro,
    observacoes,
    id
 
))

        conn.commit()
        return_connection(conn)

        flash("Loja atualizada!", "success")
        return redirect("/lojas")

    cursor.execute("SELECT * FROM lojas WHERE id=%s", (id,))
    loja = cursor.fetchone()
    return_connection(conn)

    return render_template("lojas_editar.html", loja=loja)    

from flask import jsonify

@app.route("/lojas/excluir/<int:id>", methods=["POST"])
@admin_required
def excluir_loja(id):
    try:
        cursor, conn = get_db()

        cursor.execute("DELETE FROM lojas WHERE id=%s", (id,))
        conn.commit()
        return_connection(conn)

        return jsonify({
            "ok": True,
            "msg": "Loja excluída com sucesso"
        })

    except Exception as e:
        print("ERRO EXCLUIR LOJA:", e)

        try:
            conn.rollback()
            return_connection(conn)
        except:
            pass

        return jsonify({
            "ok": False,
            "msg": str(e)
        }), 500

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

# =============================
# ROTAS DE CONDECORAÇÕES
# =============================
@app.route("/obreiros/<int:obreiro_id>/condecoracoes")
@login_required
def listar_condecoracoes(obreiro_id):
    if session["tipo"] != "admin" and session["user_id"] != obreiro_id:
        flash("Você não tem permissão para acessar esta página", "danger")
        return redirect(f"/obreiros/{obreiro_id}")
    cursor, conn = get_db()
    cursor.execute("SELECT id, nome_completo FROM usuarios WHERE id = %s", (obreiro_id,))
    obreiro = cursor.fetchone()
    if not obreiro:
        flash("Obreiro não encontrado", "danger")
        return_connection(conn)
        return redirect("/obreiros")
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
    cursor.execute("""
        SELECT * FROM tipos_condecoracoes 
        WHERE ativo = 1 
        ORDER BY nivel DESC, ordem
    """)
    tipos_condecoracoes = cursor.fetchall()
    return_connection(conn)
    return render_template("obreiros/condecoracoes.html", obreiro=obreiro, condecoracoes=condecoracoes,
                          tipos_condecoracoes=tipos_condecoracoes, obreiro_id=obreiro_id)

@app.route("/obreiros/<int:obreiro_id>/condecoracoes/nova", methods=["POST"])
@admin_required
def nova_condecoracao(obreiro_id):
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
        data_validade = data_validade if data_validade and data_validade.strip() else None
        cursor.execute("""
            INSERT INTO condecoracoes_obreiro 
            (obreiro_id, tipo_id, data_concessao, data_validade, concedido_por, 
             motivo, numero_registro, observacoes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (obreiro_id, tipo_id, data_concessao, data_validade, session["user_id"],
              motivo, numero_registro, observacoes))
        conn.commit()
        registrar_log("conceder_condecoracao", "condecoracao", cursor.lastrowid, dados_novos={"obreiro_id": obreiro_id, "tipo_id": tipo_id})
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
    cursor, conn = get_db()
    cursor.execute("SELECT * FROM tipos_condecoracoes ORDER BY nivel DESC, ordem")
    tipos = cursor.fetchall()
    return_connection(conn)
    return render_template("admin/tipos_condecoracoes.html", tipos=tipos)

@app.route("/tipos_condecoracoes/novo", methods=["GET", "POST"])
@admin_required
def novo_tipo_condecoracao():
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

import os
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
    print("Variáveis de ambiente disponíveis:")
    for key in os.environ.keys():
        if 'RESEND' in key or 'EMAIL' in key:
            print(f"  - {key}")
print("=" * 50)

if RESEND_API_KEY:
    try:
        resend.api_key = RESEND_API_KEY
        print("✅ Resend configurado com sucesso")
    except Exception as e:
        print(f"❌ Erro ao configurar Resend: {e}")
else:
    print("⚠️ RESEND_API_KEY não configurada - e-mails não serão enviados")

def enviar_email_resend(destinatario, assunto, conteudo_html, conteudo_texto=None):
    """
    Função auxiliar para enviar e-mails via Resend
    """
    try:
        if not RESEND_API_KEY:
            return {
                'success': False,
                'message': 'Resend não configurado. Adicione RESEND_API_KEY nas variáveis de ambiente do Render.'
            }
        
        # Garantir que destinatário é uma lista
        if isinstance(destinatario, str):
            destinatario_lista = [destinatario]
        else:
            destinatario_lista = destinatario
        
        params = {
            "from": f"Sistema Maçônico <{EMAIL_FROM_DEFAULT}>",
            "to": destinatario_lista,
            "subject": assunto,
            "html": conteudo_html,
        }
        
        if conteudo_texto:
            params["text"] = conteudo_texto
        
        print(f"📧 Enviando e-mail para: {destinatario_lista}")
        print(f"📧 Assunto: {assunto}")
        
        email = resend.Emails.send(params)
        print(f"✅ E-mail enviado para {destinatario} - ID: {email}")
        return {
            'success': True,
            'message': 'E-mail enviado com sucesso!',
            'id': email
        }
        
    except Exception as e:
        print(f"❌ Erro ao enviar e-mail: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'message': str(e)
        }

def enviar_email_reuniao(destinatario, nome_destinatario, dados_reuniao):
    """Envia e-mail de convocação para reunião via Resend"""
    reuniao_id = dados_reuniao.get('id', '')
    assunto = f"📅 Convite: {dados_reuniao.get('titulo', 'Nova Reunião')} - Sistema Maçônico"
    
    # Formatar horário
    hora_termino = dados_reuniao.get('hora_termino')
    horario = dados_reuniao.get('hora_inicio')
    if hora_termino:
        horario = f"{dados_reuniao.get('hora_inicio')} às {hora_termino}"
    
    # Formatar pauta
    pauta_html = ""
    if dados_reuniao.get('pauta'):
        pauta_html = f"""
        <div class="info-row">
            <div class="info-label">📋 Pauta:</div>
            <div class="info-value">{dados_reuniao.get('pauta')}</div>
        </div>
        """
    
    # Formatar observações
    observacoes_html = ""
    if dados_reuniao.get('observacoes'):
        observacoes_html = f"""
        <div class="info-row">
            <div class="info-label">📝 Observações:</div>
            <div class="info-value">{dados_reuniao.get('observacoes')}</div>
        </div>
        """
    
    # Link para confirmação (ajuste conforme sua rota)
    link_confirmacao = f"https://www.juramelo.com.br/reunioes/{reuniao_id}" if reuniao_id else "#"
    
    conteudo_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; padding: 30px 20px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ padding: 30px 20px; background: #fff; }}
            .info-card {{ background: #f8f9fa; border-left: 4px solid #1a472a; padding: 15px; margin: 20px 0; border-radius: 8px; }}
            .info-row {{ margin-bottom: 10px; }}
            .info-label {{ font-weight: bold; color: #1a472a; display: inline-block; width: 100px; }}
            .info-value {{ display: inline-block; }}
            .button {{ display: inline-block; padding: 12px 30px; background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; text-decoration: none; border-radius: 5px; margin: 20px 0; }}
            .footer {{ background: #f5f5f5; padding: 20px; text-align: center; font-size: 12px; color: #666; border-radius: 0 0 10px 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📅 Convite para Reunião</h1>
                <p>Sistema Maçônico</p>
            </div>
            <div class="content">
                <h2>Olá {nome_destinatario},</h2>
                <p>Você foi convidado para uma reunião:</p>
                
                <div class="info-card">
                    <h3 style="color: #1a472a; margin-bottom: 15px;">{dados_reuniao.get('titulo')}</h3>
                    
                    <div class="info-row">
                        <div class="info-label">📌 Tipo:</div>
                        <div class="info-value">{dados_reuniao.get('tipo', 'Não informado')}</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">📅 Data:</div>
                        <div class="info-value">{dados_reuniao.get('data')}</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">⏰ Horário:</div>
                        <div class="info-value">{horario}</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">📍 Local:</div>
                        <div class="info-value">{dados_reuniao.get('local')}</div>
                    </div>
                    {pauta_html}
                    {observacoes_html}
                </div>
                
                <p style="text-align: center;">
                    <a href="{link_confirmacao}" class="button">Ver Detalhes</a>
                </p>
                
                <p>Por favor, confirme sua presença através do sistema.</p>
                <p>Atenciosamente,<br><strong>Secretaria do Sistema Maçônico</strong></p>
            </div>
            <div class="footer">
                <p>Sistema Maçônico - www.juramelo.com.br</p>
                <p>Este é um e-mail automático, por favor não responda.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return enviar_email_resend(destinatario, assunto, conteudo_html)
# =============================
# ROTA: CONFIGURAÇÃO DE E-MAIL
# =============================
@app.route("/config/email", methods=["GET", "POST"])
@admin_required
def config_email():
    cursor, conn = get_db()
    
    if request.method == "POST":
        # Coletar dados do formulário
        server = request.form.get("server", "")  # Mantido para compatibilidade
        port = request.form.get("port", "")      # Mantido para compatibilidade
        use_tls = 1 if request.form.get("use_tls") else 0
        username = request.form.get("username", "")  # Mantido para compatibilidade
        password = request.form.get("password", "")  # Mantido para compatibilidade
        sender = request.form.get("sender", EMAIL_FROM_DEFAULT)
        sender_name = request.form.get("sender_name", "Sistema Maçônico")
        active = 1 if request.form.get("active") else 0
        
        # Validar campos obrigatórios (apenas sender é obrigatório agora)
        if not sender:
            flash("Preencha o e-mail remetente", "danger")
        else:
            try:
                # Desativar outras configurações se esta for ativa
                if active:
                    cursor.execute("UPDATE email_settings SET active = 0")
                
                # Inserir nova configuração (compatível com estrutura existente)
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
    
    # Buscar configuração ativa
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
    
    # Buscar configuração ativa
    cursor, conn = get_db()
    cursor.execute("SELECT * FROM email_settings WHERE active = 1 ORDER BY id DESC LIMIT 1")
    config = cursor.fetchone()
    return_connection(conn)
    
    # Usar configuração do banco ou valores padrão
    remetente = config['sender'] if config else EMAIL_FROM_DEFAULT
    nome_remetente = config['sender_name'] if config else "Sistema Maçônico"
    
    # Verificar se Resend está configurado
    if not RESEND_API_KEY:
        flash("Resend não configurado. Adicione RESEND_API_KEY nas variáveis de ambiente do Render.", "danger")
        return redirect("/config/email")
    
    # Preparar e-mail de teste
    assunto = "Teste de Configuração - Sistema Maçônico"
    
    conteudo_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; padding: 30px 20px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ padding: 30px 20px; background: #fff; }}
            .footer {{ background: #f5f5f5; padding: 20px; text-align: center; font-size: 12px; color: #666; border-radius: 0 0 10px 10px; }}
            .success {{ color: #4CAF50; font-size: 48px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>✅ Teste de E-mail</h1>
            </div>
            <div class="content">
                <p style="text-align: center; font-size: 48px;">📧</p>
                <p>Olá,</p>
                <p>Esta é uma mensagem de teste do <strong>Sistema Maçônico</strong>.</p>
                <p>Se você está recebendo este e-mail, a configuração está funcionando corretamente!</p>
                <p><strong>Remetente:</strong> {nome_remetente} &lt;{remetente}&gt;</p>
                <p><strong>Data e hora do teste:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
                <p><strong>Plataforma:</strong> Resend (servidor em São Paulo)</p>
            </div>
            <div class="footer">
                <p>Sistema Maçônico - www.juramelo.com.br</p>
                <p>Este é um e-mail automático de teste, por favor não responda.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Enviar via Resend
    resultado = enviar_email_resend(
        destinatario=email_teste,
        assunto=assunto,
        conteudo_html=conteudo_html
    )
    
    if resultado['success']:
        flash(f"✅ E-mail de teste enviado com sucesso para {email_teste}! ID: {resultado['id']}", "success")
    else:
        flash(f"❌ Falha ao enviar e-mail: {resultado['message']}", "danger")
    
    return redirect("/config/email")

# =============================
# ROTA: STATUS DO RESEND (DIAGNÓSTICO)
# =============================
@app.route("/config/email/status")
@admin_required
def email_status():
    """Endpoint para verificar status da configuração de e-mail"""
    status = {
        "resend_configurado": bool(RESEND_API_KEY),
        "email_from": EMAIL_FROM_DEFAULT,
        "dominio_verificado": "juramelo.com.br",
        "metodo_envio": "Resend API (SMTP alternativo)"
    }
    
    # Tentar obter configuração ativa do banco
    try:
        cursor, conn = get_db()
        cursor.execute("SELECT * FROM email_settings WHERE active = 1 ORDER BY id DESC LIMIT 1")
        config = cursor.fetchone()
        return_connection(conn)
        
        if config:
            status["configuracao_ativa"] = {
                "remetente": config['sender'],
                "nome_remetente": config['sender_name'],
                "server": config['server'] or "Resend API (não usado)",
                "port": config['port'] or "N/A"
            }
        else:
            status["configuracao_ativa"] = "Nenhuma configuração ativa no banco"
            
    except Exception as e:
        status["erro_busca_config"] = str(e)
    
    return jsonify(status)

# =============================
# FUNÇÕES DE ENVIO PARA O SISTEMA
# =============================

def enviar_email_bem_vindo(usuario_nome, usuario_email):
    """Envia e-mail de boas-vindas"""
    assunto = "Bem-vindo ao Sistema Maçônico"
    
    conteudo_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; padding: 30px 20px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ padding: 30px 20px; background: #fff; }}
            .button {{ display: inline-block; padding: 12px 30px; background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; text-decoration: none; border-radius: 5px; margin: 20px 0; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Sistema Maçônico</h1>
                <p>Bem-vindo à Loja</p>
            </div>
            <div class="content">
                <h2>Olá {usuario_nome},</h2>
                <p>É com grande satisfação que damos as boas-vindas ao <strong>Sistema Maçônico</strong>.</p>
                <p>Seu cadastro foi realizado com sucesso e agora você já pode acessar todas as funcionalidades do sistema.</p>
                <p style="text-align: center;">
                    <a href="https://www.juramelo.com.br" class="button">Acessar Sistema</a>
                </p>
                <p>Fraternalmente,<br><strong>Equipe do Sistema Maçônico</strong></p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return enviar_email_resend(usuario_email, assunto, conteudo_html)

def enviar_email_recuperacao_senha(usuario_nome, usuario_email, token_recuperacao):
    """Envia e-mail de recuperação de senha"""
    assunto = "Recuperação de Senha - Sistema Maçônico"
    
    link_recuperacao = f"https://www.juramelo.com.br/resetar-senha?token={token_recuperacao}"
    
    conteudo_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #ff9800, #e65100); color: white; padding: 30px 20px; text-align: center; border-radius: 10px 10px 0 0; }}
            .button {{ display: inline-block; padding: 12px 30px; background: #ff9800; color: white; text-decoration: none; border-radius: 5px; margin: 20px 0; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Recuperação de Senha</h1>
            </div>
            <div class="content">
                <p>Olá <strong>{usuario_nome}</strong>,</p>
                <p>Recebemos uma solicitação para redefinir sua senha no Sistema Maçônico.</p>
                <p style="text-align: center;">
                    <a href="{link_recuperacao}" class="button">Redefinir Senha</a>
                </p>
                <p>Se você não solicitou essa alteração, ignore este e-mail.</p>
                <p><strong>Este link expira em 24 horas.</strong></p>
                <p>Atenciosamente,<br><strong>Equipe do Sistema Maçônico</strong></p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return enviar_email_resend(usuario_email, assunto, conteudo_html)

# =============================
# ENVIO DE E-MAIL PARA REUNIÕES
# =============================

def enviar_email_reuniao(destinatario, nome_destinatario, dados_reuniao):
    """
    Envia e-mail de convocação para reunião
    
    Args:
        destinatario: E-mail do destinatário
        nome_destinatario: Nome do destinatário
        dados_reuniao: Dict com dados da reunião (titulo, data, hora, local, descricao, etc)
    """
    assunto = f"📅 Convite: {dados_reuniao.get('titulo', 'Nova Reunião')} - Sistema Maçônico"
    
    # Formatar data e hora
    data_reuniao = dados_reuniao.get('data', '')
    hora_reuniao = dados_reuniao.get('hora', '')
    local = dados_reuniao.get('local', 'A definir')
    descricao = dados_reuniao.get('descricao', '')
    
    conteudo_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; padding: 30px 20px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ padding: 30px 20px; background: #fff; }}
            .info-card {{ background: #f8f9fa; border-left: 4px solid #1a472a; padding: 15px; margin: 20px 0; border-radius: 8px; }}
            .info-row {{ display: flex; margin-bottom: 10px; }}
            .info-label {{ font-weight: bold; width: 80px; color: #1a472a; }}
            .info-value {{ flex: 1; }}
            .button {{ display: inline-block; padding: 12px 30px; background: linear-gradient(135deg, #1a472a, #0a2a1a); color: #ffd700; text-decoration: none; border-radius: 5px; margin: 20px 0; }}
            .footer {{ background: #f5f5f5; padding: 20px; text-align: center; font-size: 12px; color: #666; border-radius: 0 0 10px 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📅 Convite para Reunião</h1>
                <p>Sistema Maçônico</p>
            </div>
            <div class="content">
                <h2>Olá {nome_destinatario},</h2>
                <p>Você foi convidado para uma reunião:</p>
                
                <div class="info-card">
                    <h3 style="color: #1a472a; margin-bottom: 15px;">{dados_reuniao.get('titulo', 'Nova Reunião')}</h3>
                    
                    <div class="info-row">
                        <div class="info-label">📅 Data:</div>
                        <div class="info-value">{data_reuniao}</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">⏰ Hora:</div>
                        <div class="info-value">{hora_reuniao}</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">📍 Local:</div>
                        <div class="info-value">{local}</div>
                    </div>
                    {f'<div class="info-row"><div class="info-label">📝 Descrição:</div><div class="info-value">{descricao}</div></div>' if descricao else ''}
                </div>
                
                <p style="text-align: center;">
                    <a href="https://www.juramelo.com.br/reunioes" class="button">Ver Detalhes</a>
                </p>
                
                <p>Por favor, confirme sua presença através do sistema.</p>
                <p>Atenciosamente,<br><strong>Secretaria do Sistema Maçônico</strong></p>
            </div>
            <div class="footer">
                <p>Sistema Maçônico - www.juramelo.com.br</p>
                <p>Este é um e-mail automático, por favor não responda.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return enviar_email_resend(destinatario, assunto, conteudo_html)


def enviar_lembrete_reuniao(destinatario, nome_destinatario, dados_reuniao):
    """
    Envia e-mail de lembrete para reunião (24h antes)
    """
    assunto = f"⏰ Lembrete: {dados_reuniao.get('titulo', 'Reunião')} - Amanhã!"
    
    data_reuniao = dados_reuniao.get('data', '')
    hora_reuniao = dados_reuniao.get('hora', '')
    local = dados_reuniao.get('local', 'A definir')
    
    conteudo_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #ff9800, #e65100); color: white; padding: 30px 20px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ padding: 30px 20px; background: #fff; }}
            .info-card {{ background: #f8f9fa; border-left: 4px solid #ff9800; padding: 15px; margin: 20px 0; border-radius: 8px; }}
            .button {{ display: inline-block; padding: 12px 30px; background: #ff9800; color: white; text-decoration: none; border-radius: 5px; margin: 20px 0; }}
            .footer {{ background: #f5f5f5; padding: 20px; text-align: center; font-size: 12px; color: #666; border-radius: 0 0 10px 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>⏰ Lembrete de Reunião</h1>
                <p>Sistema Maçônico</p>
            </div>
            <div class="content">
                <h2>Olá {nome_destinatario},</h2>
                <p>Este é um lembrete da reunião que acontecerá amanhã:</p>
                
                <div class="info-card">
                    <h3 style="color: #ff9800;">{dados_reuniao.get('titulo', 'Reunião')}</h3>
                    <p><strong>📅 Data:</strong> {data_reuniao}</p>
                    <p><strong>⏰ Hora:</strong> {hora_reuniao}</p>
                    <p><strong>📍 Local:</strong> {local}</p>
                </div>
                
                <p style="text-align: center;">
                    <a href="https://www.juramelo.com.br/reunioes" class="button">Confirmar Presença</a>
                </p>
                
                <p>Confirme sua presença no sistema.</p>
                <p>Atenciosamente,<br><strong>Secretaria do Sistema Maçônico</strong></p>
            </div>
            <div class="footer">
                <p>Sistema Maçônico - www.juramelo.com.br</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return enviar_email_resend(destinatario, assunto, conteudo_html)

@app.route('/admin/testar-email-reuniao-completo')
@admin_required
def testar_email_reuniao_completo():
    """Teste completo do envio de e-mail de reunião"""
    try:
        email_teste = request.args.get('email')
        if not email_teste:
            return jsonify({"erro": "Informe um e-mail: ?email=seu@email.com"}), 400
        
        dados_teste = {
            'titulo': 'Reunião de Teste - Loja Maçônica',
            'data': '27/03/2026',
            'hora': '19:30',
            'local': 'Templo Maçônico - Rua Principal, 123',
            'descricao': 'Pauta: Assuntos administrativos e planejamento de atividades.'
        }
        
        resultado = enviar_email_reuniao(
            destinatario=email_teste,
            nome_destinatario="Irmão Teste",
            dados_reuniao=dados_teste
        )
        
        return jsonify(resultado)
        
    except Exception as e:
        return jsonify({"erro": str(e)}), 500    
    
# =============================
# ROTAS DE WHATSAPP
# =============================
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
    cursor.execute("SELECT * FROM graus WHERE nivel IN (1, 2, 3) AND ativo = 1 ORDER BY nivel")
    graus = cursor.fetchall()
    cursor.execute("SELECT id, usuario, nome_completo, grau_atual, tipo FROM usuarios WHERE ativo = 1 ORDER BY nome_completo")
    usuarios = cursor.fetchall()
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
    permissoes_por_modulo = {}
    for p in permissoes:
        if p['modulo_nome'] not in permissoes_por_modulo:
            permissoes_por_modulo[p['modulo_nome']] = {'icone': p['modulo_icone'], 'permissoes': []}
        permissoes_por_modulo[p['modulo_nome']]['permissoes'].append({
            'id': p['permissao_id'],
            'nome': p['permissao_nome'],
            'codigo': p['codigo'],
            'descricao': p['descricao']
        })
    cursor.execute("SELECT grau_id, permissao_id FROM permissoes_grau WHERE grau_id IN (1, 2, 3)")
    permissoes_grau_raw = cursor.fetchall()
    permissoes_grau = [(pg['grau_id'], pg['permissao_id']) for pg in permissoes_grau_raw]
    cursor.execute("SELECT usuario_id, permissao_id, permitido FROM permissoes_usuario")
    permissoes_usuario_raw = cursor.fetchall()
    permissoes_usuario = [(pu['usuario_id'], pu['permissao_id'], pu['permitido']) for pu in permissoes_usuario_raw]
    return_connection(conn)
    return render_template("admin/permissoes.html", usuarios=usuarios, graus=graus,
                          permissoes_por_modulo=permissoes_por_modulo, permissoes_grau=permissoes_grau,
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



# =============================
# ROTAS DE BACKUP E RESTAURAÇÃO (VERSÃO CORRIGIDA)
# =============================

import zipfile
import json
import shutil
import re
from datetime import datetime, timedelta
from flask import send_file, make_response
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import tempfile

BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
TEMP_RESTORE_DIR = os.path.join(BACKUP_DIR, 'temp_restore')
BACKUP_LOG_FILE = os.path.join(BACKUP_DIR, 'backup_log.json')

# Criar diretórios
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(TEMP_RESTORE_DIR, exist_ok=True)

def log_backup_operation(operation, filename, success, details=None, error=None):
    """Registra operações de backup/restauração em log"""
    try:
        logs = []
        if os.path.exists(BACKUP_LOG_FILE):
            with open(BACKUP_LOG_FILE, 'r', encoding='utf-8') as f:
                logs = json.load(f)
        
        logs.append({
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'filename': filename,
            'success': success,
            'details': details,
            'error': error,
            'user': session.get('usuario', 'unknown'),
            'user_id': session.get('user_id'),
            'ip': request.remote_addr
        })
        
        # Manter apenas últimos 100 registros
        logs = logs[-100:]
        
        with open(BACKUP_LOG_FILE, 'w', encoding='utf-8') as f:
            json.dump(logs, f, ensure_ascii=False, indent=2)
            
    except Exception as e:
        print(f"Erro ao registrar log: {e}")

def escape_sql_string(value):
    """Escapa strings para SQL de forma segura"""
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        # Escapar aspas simples e caracteres especiais
        escaped = value.replace("'", "''")
        # Escapar barras invertidas
        escaped = escaped.replace("\\", "\\\\")
        return f"'{escaped}'"
    elif isinstance(value, datetime):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

def listar_backups_sistema():
    """Lista todos os backups disponíveis com informações detalhadas"""
    backups = []
    if not os.path.exists(BACKUP_DIR):
        return backups
    
    for file in os.listdir(BACKUP_DIR):
        if file.endswith('.zip'):
            filepath = os.path.join(BACKUP_DIR, file)
            mtime = os.path.getmtime(filepath)
            ctime = os.path.getctime(filepath)
            size = os.path.getsize(filepath) / (1024 * 1024)
            
            backups.append({
                'name': file,
                'date_str': datetime.fromtimestamp(mtime).strftime('%d/%m/%Y %H:%M:%S'),
                'date': datetime.fromtimestamp(mtime),
                'size_mb': round(size, 2),
                'size_bytes': os.path.getsize(filepath),
                'path': filepath,
                'created': datetime.fromtimestamp(ctime)
            })
    
    backups.sort(key=lambda x: x['date'], reverse=True)
    return backups

def criar_backup_sistema():
    """Cria backup completo usando pg_dump (mais confiável)"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        db_name = os.getenv('DB_NAME', 'sistema_maconico')
        backup_name = f'backup_{db_name}_{timestamp}'
        
        # Usar pg_dump para criar backup (mais confiável que SQL manual)
        dump_file = os.path.join(BACKUP_DIR, f'{backup_name}.dump')
        
        # Extrair dados de conexão
        import urllib.parse
        db_url = DATABASE_URL
        
        # Se for URL do PostgreSQL, parsear
        if db_url.startswith('postgresql://'):
            parsed = urllib.parse.urlparse(db_url)
            db_host = parsed.hostname
            db_port = parsed.port or 5432
            db_name_parsed = parsed.path[1:] if parsed.path else db_name
            db_user = parsed.username
            db_password = parsed.password
            
            # Comando pg_dump
            cmd = [
                'pg_dump',
                '-h', db_host,
                '-p', str(db_port),
                '-U', db_user,
                '-d', db_name_parsed,
                '-F', 'c',  # Formato customizado
                '-f', dump_file,
                '-v'
            ]
            
            # Definir variável de ambiente para senha
            env = os.environ.copy()
            env['PGPASSWORD'] = db_password
            
            # Executar pg_dump
            result = subprocess.run(cmd, capture_output=True, text=True, env=env)
            
            if result.returncode != 0:
                raise Exception(f"pg_dump falhou: {result.stderr}")
        
        # Se pg_dump não estiver disponível, usar método SQL alternativo
        if not os.path.exists(dump_file):
            raise Exception("pg_dump não disponível, usando método SQL alternativo")
        
        # Compactar em ZIP
        zip_file = dump_file + '.zip'
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(dump_file, os.path.basename(dump_file))
        
        # Remover arquivo dump temporário
        os.remove(dump_file)
        
        tamanho_mb = os.path.getsize(zip_file) / (1024 * 1024)
        
        # Manter apenas últimos 20 backups
        backups = listar_backups_sistema()
        deleted = []
        for i, b in enumerate(backups):
            if i >= 20:
                try:
                    os.remove(b['path'])
                    deleted.append(b['name'])
                except:
                    pass
        
        result = {
            'success': True,
            'filename': os.path.basename(zip_file),
            'size_mb': round(tamanho_mb, 2),
            'size_bytes': os.path.getsize(zip_file),
            'deleted_old': len(deleted)
        }
        
        log_backup_operation('backup', result['filename'], True, result)
        return result
        
    except Exception as e:
        print(f"Erro ao criar backup com pg_dump: {e}")
        traceback.print_exc()
        
        # Fallback: método SQL manual melhorado
        return criar_backup_sql_fallback()

def criar_backup_sql_fallback():
    """Método alternativo de backup usando SQL (com escape melhorado)"""
    temp_sql_file = None
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        db_name = os.getenv('DB_NAME', 'sistema_maconico')
        backup_name = f'backup_{db_name}_{timestamp}'
        temp_sql_file = os.path.join(BACKUP_DIR, f'{backup_name}.sql')
        
        # Conectar ao banco
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Obter lista de tabelas
        cursor.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename NOT LIKE 'pg_%'
            ORDER BY tablename
        """)
        tables = [row['tablename'] for row in cursor.fetchall()]
        
        # Criar arquivo SQL
        with open(temp_sql_file, 'w', encoding='utf-8') as f:
            f.write(f"-- ============================================\n")
            f.write(f"-- BACKUP DO SISTEMA MAÇÔNICO\n")
            f.write(f"-- ============================================\n")
            f.write(f"-- Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write(f"-- Banco: {db_name}\n")
            f.write(f"-- Tabelas: {len(tables)}\n")
            f.write(f"-- ============================================\n\n")
            
            f.write("BEGIN;\n\n")
            f.write("SET client_encoding = 'UTF8';\n\n")
            
            # Backup de cada tabela
            tables_backuped = []
            for table in tables:
                try:
                    # Obter estrutura da tabela
                    cursor.execute(f"""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns 
                        WHERE table_name = '{table}'
                        ORDER BY ordinal_position
                    """)
                    columns = cursor.fetchall()
                    
                    if not columns:
                        continue
                    
                    f.write(f"-- ============================================\n")
                    f.write(f"-- Tabela: {table}\n")
                    f.write(f"-- ============================================\n\n")
                    
                    # DROP e CREATE TABLE
                    f.write(f"DROP TABLE IF EXISTS {table} CASCADE;\n")
                    f.write(f"CREATE TABLE {table} (\n")
                    
                    col_defs = []
                    for col in columns:
                        col_def = f"    {col['column_name']} {col['data_type']}"
                        if col['is_nullable'] == 'NO':
                            col_def += " NOT NULL"
                        col_defs.append(col_def)
                    
                    f.write(",\n".join(col_defs))
                    f.write("\n);\n\n")
                    
                    # Backup dos dados
                    cursor.execute(f"SELECT * FROM {table}")
                    rows = cursor.fetchall()
                    
                    if rows:
                        col_names = [col['column_name'] for col in columns]
                        col_names_str = ', '.join(col_names)
                        
                        batch_size = 100
                        for i in range(0, len(rows), batch_size):
                            batch = rows[i:i+batch_size]
                            f.write(f"INSERT INTO {table} ({col_names_str}) VALUES\n")
                            
                            values_list = []
                            for row in batch:
                                values = []
                                for col_name in col_names:
                                    val = row.get(col_name)
                                    values.append(escape_sql_string(val))
                                values_list.append(f"    ({', '.join(values)})")
                            
                            f.write(",\n".join(values_list))
                            f.write(";\n\n")
                    
                    tables_backuped.append(table)
                    
                except Exception as e:
                    print(f"Erro ao fazer backup da tabela {table}: {e}")
                    f.write(f"-- ERRO AO FAZER BACKUP DA TABELA {table}: {str(e)}\n\n")
            
            f.write("COMMIT;\n")
        
        cursor.close()
        conn.close()
        
        # Compactar em ZIP
        zip_file = temp_sql_file + '.zip'
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(temp_sql_file, os.path.basename(temp_sql_file))
        
        # Remover arquivo SQL temporário
        os.remove(temp_sql_file)
        
        tamanho_mb = os.path.getsize(zip_file) / (1024 * 1024)
        
        # Manter apenas últimos 20 backups
        backups = listar_backups_sistema()
        deleted = []
        for i, b in enumerate(backups):
            if i >= 20:
                try:
                    os.remove(b['path'])
                    deleted.append(b['name'])
                except:
                    pass
        
        result = {
            'success': True,
            'filename': os.path.basename(zip_file),
            'size_mb': round(tamanho_mb, 2),
            'size_bytes': os.path.getsize(zip_file),
            'tables': len(tables_backuped),
            'total_tables': len(tables),
            'deleted_old': len(deleted)
        }
        
        log_backup_operation('backup', result['filename'], True, result)
        return result
        
    except Exception as e:
        print(f"Erro ao criar backup SQL: {e}")
        traceback.print_exc()
        
        if temp_sql_file and os.path.exists(temp_sql_file):
            try:
                os.remove(temp_sql_file)
            except:
                pass
        
        error_msg = str(e)
        log_backup_operation('backup', None, False, error=error_msg)
        return {'success': False, 'error': error_msg}

def restaurar_backup_sistema(filename, confirm=False):
    """Restaura um backup do sistema usando pg_restore ou SQL"""
    if not confirm:
        return {'success': False, 'error': 'Confirmação necessária', 'requires_confirmation': True}
    
    temp_dir = None
    try:
        # Validar arquivo
        backup_path = os.path.join(BACKUP_DIR, filename)
        if not os.path.exists(backup_path):
            return {'success': False, 'error': 'Arquivo de backup não encontrado'}
        
        # Criar backup de emergência
        print("Criando backup de emergência...")
        emergency_backup = criar_backup_sistema()
        
        if not emergency_backup['success']:
            return {
                'success': False,
                'error': 'Não foi possível criar backup de emergência. Operação cancelada.',
                'emergency_backup': None
            }
        
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
                
                # Separar comandos SQL de forma mais inteligente
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
                raise Exception("Formato de backup não reconhecido")
        
        # Limpar diretório temporário
        shutil.rmtree(temp_dir)
        
        result = {
            'success': True,
            'message': f'Backup restaurado com sucesso!',
            'emergency_backup': emergency_backup.get('filename'),
            'emergency_backup_size': emergency_backup.get('size_mb'),
            'statements_executed': statements_executed,
            'restored_from': filename
        }
        
        log_backup_operation('restore', filename, True, result)
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
        log_backup_operation('restore', filename, False, error=error_msg)
        
        return {
            'success': False,
            'error': error_msg,
            'emergency_backup': emergency_backup.get('filename') if 'emergency_backup' in locals() else None
        }

# =============================
# ROTAS DE BACKUP (mantidas as mesmas)
# =============================

@app.route("/admin/backup")
@admin_required
def pagina_backup_sistema():
    """Página principal de gerenciamento de backups"""
    backups = listar_backups_sistema()
    
    total_size = sum(b['size_mb'] for b in backups)
    total_backups = len(backups)
    oldest = backups[-1]['date_str'] if backups else None
    newest = backups[0]['date_str'] if backups else None
    
    disk_usage = shutil.disk_usage(BACKUP_DIR)
    disk_free_gb = disk_usage.free / (1024**3)
    disk_total_gb = disk_usage.total / (1024**3)
    
    stats = {
        'total': total_backups,
        'total_size_mb': round(total_size, 2),
        'total_size_gb': round(total_size / 1024, 2),
        'newest': newest,
        'oldest': oldest,
        'disk_free_gb': round(disk_free_gb, 2),
        'disk_total_gb': round(disk_total_gb, 2),
        'disk_used_percent': round((1 - disk_free_gb / disk_total_gb) * 100, 1) if disk_total_gb > 0 else 0
    }
    
    logs = []
    if os.path.exists(BACKUP_LOG_FILE):
        try:
            with open(BACKUP_LOG_FILE, 'r', encoding='utf-8') as f:
                logs = json.load(f)
                logs = logs[-20:]
        except:
            pass
    
    return render_template("admin/backup.html", 
                          backups=backups, 
                          stats=stats,
                          logs=logs,
                          now=datetime.now())

@app.route("/api/backup/criar", methods=["POST"])
@admin_required
def api_criar_backup_sistema():
    """Cria um novo backup"""
    result = criar_backup_sistema()
    
    if result['success']:
        flash(f"Backup criado com sucesso! Arquivo: {result['filename']} ({result['size_mb']} MB)", "success")
    else:
        flash(f"Erro ao criar backup: {result['error']}", "danger")
    
    return jsonify(result)

@app.route("/api/backup/restaurar/<filename>", methods=["POST"])
@admin_required
def api_restaurar_backup(filename):
    """Restaura um backup existente"""
    data = request.get_json() or {}
    confirm = data.get('confirm', False)
    
    result = restaurar_backup_sistema(filename, confirm)
    
    if result.get('requires_confirmation'):
        return jsonify(result), 400
    elif result['success']:
        flash(f"Backup restaurado com sucesso! Backup de emergência: {result.get('emergency_backup', 'N/A')}", "success")
    else:
        flash(f"Erro ao restaurar backup: {result['error']}", "danger")
        if result.get('emergency_backup'):
            flash(f"Backup de emergência criado: {result['emergency_backup']}", "info")
    
    return jsonify(result)

@app.route("/api/backup/listar")
@admin_required
def api_listar_backups_sistema():
    """Lista todos os backups disponíveis"""
    backups = listar_backups_sistema()
    return jsonify({'success': True, 'backups': backups})

@app.route("/api/backup/baixar/<filename>")
@admin_required
def api_baixar_backup_sistema(filename):
    """Download de um backup"""
    backup_path = os.path.join(BACKUP_DIR, filename)
    
    if not os.path.exists(backup_path):
        flash("Arquivo não encontrado", "danger")
        return redirect("/admin/backup")
    
    log_backup_operation('download', filename, True)
    
    return send_file(backup_path, 
                    as_attachment=True, 
                    download_name=filename, 
                    mimetype='application/zip')

@app.route("/api/backup/excluir/<filename>", methods=["DELETE"])
@admin_required
def api_excluir_backup_sistema(filename):
    """Exclui um backup específico"""
    backup_path = os.path.join(BACKUP_DIR, filename)
    
    if not os.path.exists(backup_path):
        return jsonify({'success': False, 'error': 'Arquivo não encontrado'}), 404
    
    try:
        os.remove(backup_path)
        log_backup_operation('delete', filename, True)
        return jsonify({'success': True, 'message': 'Backup excluído com sucesso'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route("/api/backup/limpar", methods=["POST"])
@admin_required
def api_limpar_backups_sistema():
    """Remove backups antigos mantendo apenas os últimos 20"""
    try:
        backups = listar_backups_sistema()
        deleted = []
        
        for i, backup in enumerate(backups):
            if i >= 20:
                try:
                    os.remove(backup['path'])
                    deleted.append(backup['name'])
                except Exception as e:
                    print(f"Erro ao remover {backup['name']}: {e}")
        
        log_backup_operation('cleanup', None, True, {'deleted': len(deleted), 'files': deleted})
        
        return jsonify({
            'success': True, 
            'deleted': len(deleted),
            'deleted_files': deleted,
            'remaining': min(len(backups), 20)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route("/api/backup/info/<filename>")
@admin_required
def api_backup_info(filename):
    """Obtém informações detalhadas de um backup"""
    backup_path = os.path.join(BACKUP_DIR, filename)
    
    if not os.path.exists(backup_path):
        return jsonify({'success': False, 'error': 'Arquivo não encontrado'}), 404
    
    try:
        info = {
            'filename': filename,
            'size_bytes': os.path.getsize(backup_path),
            'size_mb': round(os.path.getsize(backup_path) / (1024 * 1024), 2),
            'modified': datetime.fromtimestamp(os.path.getmtime(backup_path)).isoformat(),
            'created': datetime.fromtimestamp(os.path.getctime(backup_path)).isoformat()
        }
        
        # Tentar ler cabeçalho
        with zipfile.ZipFile(backup_path, 'r') as zf:
            sql_files = [f for f in zf.namelist() if f.endswith(('.sql', '.dump'))]
            if sql_files:
                with zf.open(sql_files[0]) as f:
                    header = f.read(2000).decode('utf-8', errors='ignore')
                    for line in header.split('\n'):
                        if 'Data:' in line:
                            info['backup_date'] = line.strip()
                        elif 'Tabelas:' in line:
                            try:
                                info['tables'] = int(line.split(':')[1].strip())
                            except:
                                pass
        
        return jsonify({'success': True, 'info': info})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route("/api/backup/logs")
@admin_required
def api_backup_logs():
    """Retorna logs de operações de backup"""
    limit = request.args.get('limit', 50, type=int)
    
    logs = []
    if os.path.exists(BACKUP_LOG_FILE):
        try:
            with open(BACKUP_LOG_FILE, 'r', encoding='utf-8') as f:
                logs = json.load(f)
                logs = logs[-limit:]
        except:
            pass
    
    return jsonify({'success': True, 'logs': logs, 'total': len(logs)})


# =============================
# INICIALIZAÇÃO DA APLICAÇÃO
# =============================
if __name__ == "__main__":
    debug_mode = os.getenv('FLASK_ENV', 'production') == 'development'
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=debug_mode, host='0.0.0.0', port=port)