from flask import Blueprint, render_template, request, jsonify, current_app, redirect, url_for, flash
from flask_login import login_required, current_user
from datetime import datetime
import json
import requests

# Criar o blueprint (SOMENTE UMA VEZ)
notificacoes_bp = Blueprint('notificacoes', __name__, url_prefix='/notificacoes')

# Importar models DENTRO das funções para evitar circular import
# NÃO importar no topo do arquivo

@notificacoes_bp.route('/candidato/<int:candidato_id>/notificar-iniciacao', methods=['GET', 'POST'])
@login_required
def notificar_iniciacao(candidato_id):
    # Importar DENTRO da função
    from app.models import Candidato, db
    from app.models.notificacoes_gob import NotificacaoIniciacao
    
    candidato = Candidato.query.get_or_404(candidato_id)
    notificacao = NotificacaoIniciacao.query.filter_by(candidato_id=candidato_id).first()
    
    if request.method == 'POST':
        acao = request.form.get('acao')
        
        # Coletar dados do formulário
        dados = {
            'candidato_id': candidato_id,
            'numero_processo': request.form.get('numero_processo') or f"PROC-{candidato_id}-{datetime.now().year}",
            'loja_nome': request.form.get('loja_nome'),
            'loja_numero': request.form.get('loja_numero'),
            'loja_oriente': request.form.get('loja_oriente'),
            'data_sessao': datetime.strptime(request.form.get('data_sessao'), '%Y-%m-%d') if request.form.get('data_sessao') else None,
            'nome_candidato': request.form.get('nome_candidato') or candidato.nome,
            'data_iniciacao': datetime.strptime(request.form.get('data_iniciacao'), '%Y-%m-%d') if request.form.get('data_iniciacao') else None,
            'hora_iniciacao': request.form.get('hora_iniciacao'),
            'ritual_utilizado': request.form.get('ritual_utilizado'),
            'numero_obreiros_presentes': request.form.get('numero_obreiros_presentes'),
            'presidente_comissao': request.form.get('presidente_comissao'),
            'membros_comissao': request.form.get('membros_comissao'),
            'ata_numero': request.form.get('ata_numero'),
            'ata_data': datetime.strptime(request.form.get('ata_data'), '%Y-%m-%d') if request.form.get('ata_data') else None,
        }
        
        if not notificacao:
            notificacao = NotificacaoIniciacao(**dados)
            db.session.add(notificacao)
        else:
            for key, value in dados.items():
                setattr(notificacao, key, value)
        
        if acao == 'rascunho':
            notificacao.status_envio = 'rascunho'
            db.session.commit()
            flash('Rascunho salvo com sucesso!', 'success')
            return redirect(url_for('notificacoes.notificar_iniciacao', candidato_id=candidato_id))
        
        elif acao == 'preview':
            db.session.commit()
            # Gerar PDF preview
            from app.services.pdf_service import PDFService
            pdf_service = PDFService()
            url_pdf = pdf_service.gerar_pdf_iniciacao_gob(candidato, notificacao)
            return redirect(url_pdf)
        
        elif acao == 'enviar':
            # Simular envio (por enquanto)
            notificacao.status_envio = 'enviado'
            notificacao.data_envio = datetime.utcnow()
            notificacao.protocolo_gob = f"GOB-{datetime.now().year}-{candidato_id}-{int(datetime.now().timestamp())}"
            db.session.commit()
            
            flash(f'✅ Notificação enviada com sucesso! Protocolo: {notificacao.protocolo_gob}', 'success')
            return redirect(url_for('sindicancia', candidato_id=candidato_id))
    
    return render_template('notificacao_iniciacao.html', 
                         candidato=candidato, 
                         notificacao=notificacao,
                         now=datetime.now())


@notificacoes_bp.route('/candidato/<int:candidato_id>/notificar-elevacao', methods=['GET', 'POST'])
@login_required
def notificar_elevacao(candidato_id):
    from app.models import Candidato, db
    from app.models.notificacoes_gob import NotificacaoElevacao
    
    candidato = Candidato.query.get_or_404(candidato_id)
    notificacao = NotificacaoElevacao.query.filter_by(candidato_id=candidato_id).first()
    
    if request.method == 'POST':
        acao = request.form.get('acao')
        
        dados = {
            'candidato_id': candidato_id,
            'numero_processo': request.form.get('numero_processo') or f"ELEV-{candidato_id}-{datetime.now().year}",
            'loja_nome': request.form.get('loja_nome'),
            'loja_numero': request.form.get('loja_numero'),
            'loja_oriente': request.form.get('loja_oriente'),
            'data_sessao': datetime.strptime(request.form.get('data_sessao'), '%Y-%m-%d') if request.form.get('data_sessao') else None,
            'nome_aprendiz': candidato.nome,
            'data_iniciacao': datetime.strptime(request.form.get('data_iniciacao'), '%Y-%m-%d') if request.form.get('data_iniciacao') else None,
            'data_elevacao': datetime.strptime(request.form.get('data_elevacao'), '%Y-%m-%d') if request.form.get('data_elevacao') else None,
            'tempo_aprendiz': request.form.get('tempo_aprendiz'),
            'frequencia_sessoes': request.form.get('frequencia_sessoes'),
            'trabalhos_apresentados': request.form.get('trabalhos_apresentados'),
            'nota_exame': request.form.get('nota_exame'),
            'conceito_final': request.form.get('conceito_final'),
            'ata_numero': request.form.get('ata_numero'),
            'ata_data': datetime.strptime(request.form.get('ata_data'), '%Y-%m-%d') if request.form.get('ata_data') else None,
        }
        
        if not notificacao:
            notificacao = NotificacaoElevacao(**dados)
            db.session.add(notificacao)
        else:
            for key, value in dados.items():
                setattr(notificacao, key, value)
        
        if acao == 'enviar':
            notificacao.status_envio = 'enviado'
            notificacao.data_envio = datetime.utcnow()
            notificacao.protocolo_gob = f"GOB-ELEV-{datetime.now().year}-{candidato_id}-{int(datetime.now().timestamp())}"
            db.session.commit()
            flash('✅ Notificação de Elevação enviada com sucesso!', 'success')
            return redirect(url_for('sindicancia', candidato_id=candidato_id))
        else:
            db.session.commit()
            flash('Rascunho salvo!', 'success')
            return redirect(url_for('notificacoes.notificar_elevacao', candidato_id=candidato_id))
    
    return render_template('notificacao_elevacao.html', 
                         candidato=candidato, 
                         notificacao=notificacao,
                         now=datetime.now())


@notificacoes_bp.route('/candidato/<int:candidato_id>/notificar-exaltacao', methods=['GET', 'POST'])
@login_required
def notificar_exaltacao(candidato_id):
    from app.models import Candidato, db
    from app.models.notificacoes_gob import NotificacaoExaltacao
    
    candidato = Candidato.query.get_or_404(candidato_id)
    notificacao = NotificacaoExaltacao.query.filter_by(candidato_id=candidato_id).first()
    
    if request.method == 'POST':
        acao = request.form.get('acao')
        
        dados = {
            'candidato_id': candidato_id,
            'numero_processo': request.form.get('numero_processo') or f"EXALT-{candidato_id}-{datetime.now().year}",
            'loja_nome': request.form.get('loja_nome'),
            'loja_numero': request.form.get('loja_numero'),
            'loja_oriente': request.form.get('loja_oriente'),
            'data_sessao': datetime.strptime(request.form.get('data_sessao'), '%Y-%m-%d') if request.form.get('data_sessao') else None,
            'nome_companheiro': candidato.nome,
            'data_iniciacao': datetime.strptime(request.form.get('data_iniciacao'), '%Y-%m-%d') if request.form.get('data_iniciacao') else None,
            'data_elevacao': datetime.strptime(request.form.get('data_elevacao'), '%Y-%m-%d') if request.form.get('data_elevacao') else None,
            'data_exaltacao': datetime.strptime(request.form.get('data_exaltacao'), '%Y-%m-%d') if request.form.get('data_exaltacao') else None,
            'trabalhos_apresentados': request.form.get('trabalhos_apresentados'),
            'terca_camara': request.form.get('terca_camara'),
            'prova_camara_meio': request.form.get('prova_camara_meio'),
            'prova_camara_justica': request.form.get('prova_camara_justica'),
            'ata_numero': request.form.get('ata_numero'),
            'ata_data': datetime.strptime(request.form.get('ata_data'), '%Y-%m-%d') if request.form.get('ata_data') else None,
        }
        
        if not notificacao:
            notificacao = NotificacaoExaltacao(**dados)
            db.session.add(notificacao)
        else:
            for key, value in dados.items():
                setattr(notificacao, key, value)
        
        if acao == 'enviar':
            notificacao.status_envio = 'enviado'
            notificacao.data_envio = datetime.utcnow()
            notificacao.protocolo_gob = f"GOB-EXALT-{datetime.now().year}-{candidato_id}-{int(datetime.now().timestamp())}"
            db.session.commit()
            flash('✅ Notificação de Exaltação enviada com sucesso!', 'success')
            return redirect(url_for('sindicancia', candidato_id=candidato_id))
        else:
            db.session.commit()
            flash('Rascunho salvo!', 'success')
            return redirect(url_for('notificacoes.notificar_exaltacao', candidato_id=candidato_id))
    
    return render_template('notificacao_exaltacao.html', 
                         candidato=candidato, 
                         notificacao=notificacao,
                         now=datetime.now())


@notificacoes_bp.route('/historico')
@login_required
def historico_notificacoes():
    from models.notificacoes_gob import NotificacaoIniciacao, NotificacaoElevacao, NotificacaoExaltacao
    from database import get_db_connection
    
    # Buscar todas as notificações
    with get_db_connection() as cursor:
        cursor.execute("""
            SELECT * FROM notificacoes_iniciacao 
            ORDER BY created_at DESC
        """)
        iniciacoes = cursor.fetchall()
        
        cursor.execute("""
            SELECT * FROM notificacoes_elevacao 
            ORDER BY created_at DESC
        """)
        elevacoes = cursor.fetchall()
        
        cursor.execute("""
            SELECT * FROM notificacoes_exaltacao 
            ORDER BY created_at DESC
        """)
        exaltacoes = cursor.fetchall()
    
    return render_template('historico_notificacoes.html', 
                         iniciacoes=iniciacoes,
                         elevacoes=elevacoes,
                         exaltacoes=exaltacoes)