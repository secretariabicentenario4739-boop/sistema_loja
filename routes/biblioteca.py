from flask import Blueprint, render_template, session, flash, redirect, url_for, request
from models import get_db_connection

biblioteca_bp = Blueprint('biblioteca', __name__, url_prefix='/biblioteca')



def tem_permissao_biblioteca(grau_usuario):
    """Verifica se o usuário tem permissão para acessar a biblioteca"""
    # Admin tem acesso total
    if session.get('tipo') == 'admin':
        return True
    
    # Obreiros com grau 1, 2 ou 3 têm acesso
    if grau_usuario in [1, 2, 3]:
        return True
    
    return False

@biblioteca_bp.route('/')
def listar_materiais():
    """Página principal da biblioteca"""
    if 'usuario_id' not in session:
        flash('Faça login para acessar a biblioteca', 'warning')
        return redirect(url_for('login'))
    
    grau_usuario = session.get('grau_atual', 1)
    
    if not tem_permissao_biblioteca(grau_usuario):
        flash('Você não tem permissão para acessar a biblioteca', 'danger')
        return redirect(url_for('dashboard'))
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Buscar materiais liberados para o grau do usuário
    cursor.execute("""
        SELECT m.*, c.nome as categoria_nome, c.cor as categoria_cor
        FROM materiais m
        LEFT JOIN categorias_material c ON m.categoria_id = c.id
        WHERE m.publicado = 1 AND m.grau_acesso <= %s
        ORDER BY m.destaque DESC, m.data_publicacao DESC
        LIMIT 20
    """, (grau_usuario,))
    materiais = cursor.fetchall()
    
    # Buscar categorias
    cursor.execute("SELECT * FROM categorias_material ORDER BY ordem")
    categorias = cursor.fetchall()
    
    # Buscar materiais em destaque
    cursor.execute("""
        SELECT m.*, c.nome as categoria_nome
        FROM materiais m
        LEFT JOIN categorias_material c ON m.categoria_id = c.id
        WHERE m.destaque = 1 AND m.publicado = 1 AND m.grau_acesso <= %s
        LIMIT 5
    """, (grau_usuario,))
    destaques = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('biblioteca/index.html',
                         materiais=materiais,
                         categorias=categorias,
                         destaques=destaques,
                         grau_usuario=grau_usuario)

# Adicione outras rotas conforme necessário...