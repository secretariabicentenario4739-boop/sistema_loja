from flask import Blueprint, render_template, request, jsonify, current_app, redirect, url_for, flash
from flask_login import login_required, current_user
from datetime import datetime
import json
import requests

notificacoes_bp = Blueprint('notificacoes', __name__, url_prefix='/notificacoes')


@notificacoes_bp.route('/candidato/<int:candidato_id>/notificar-iniciacao', methods=['GET', 'POST'])
def notificar_iniciacao(candidato_id):
    from flask import session, flash, redirect, url_for, render_template, request, current_app
    from database import get_db_connection
    from models.notificacoes_gob import NotificacaoIniciacao
    from services.pdf_service import PDFService
    from datetime import datetime
    
    # Verificação usando sessão
    if not session.get('user_id'):
        flash('Você precisa estar logado para acessar esta página.', 'danger')
        return redirect(url_for('login'))
    
    if session.get('tipo') != 'admin':
        flash('Acesso negado! Apenas administradores.', 'danger')
        return redirect(url_for('dashboard'))
    
    # Buscar candidato
    with get_db_connection() as cursor:
        cursor.execute("""
            SELECT id, nome, email, celular, status, fechado, 
                   loja_nome, loja_numero
            FROM candidatos 
            WHERE id = %s
        """, (candidato_id,))
        candidato = cursor.fetchone()
    
    if not candidato:
        flash('Candidato não encontrado!', 'danger')
        return redirect('/candidatos')
    
    # Buscar o Oriente da loja na tabela lojas
    loja_orient = ''
    if candidato.get('loja_nome'):
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT oriente FROM lojas WHERE nome = %s
            """, (candidato['loja_nome'],))
            loja = cursor.fetchone()
            if loja and loja.get('oriente'):
                loja_orient = loja['oriente']
                print(f"✅ Oriente encontrado: {loja_orient} para a loja {candidato['loja_nome']}")
            else:
                print(f"⚠️ Loja '{candidato['loja_nome']}' não encontrada na tabela lojas")
    
    notificacao = NotificacaoIniciacao.buscar_por_candidato(candidato_id)
    
    if request.method == 'POST':
        acao = request.form.get('acao')
        
        # Tratar campos de data (se vazio, enviar None)
        data_sessao = request.form.get('data_sessao') if request.form.get('data_sessao') else None
        data_iniciacao = request.form.get('data_iniciacao') if request.form.get('data_iniciacao') else None
        ata_data = request.form.get('ata_data') if request.form.get('ata_data') else None
        
        dados = {
            'candidato_id': candidato_id,
            'numero_processo': request.form.get('numero_processo') or f"PROC-{candidato_id}-{datetime.now().year}",
            'loja_nome': request.form.get('loja_nome') or candidato.get('loja_nome', ''),
            'loja_numero': request.form.get('loja_numero') or candidato.get('loja_numero', ''),
            'loja_oriente': request.form.get('loja_oriente') or loja_orient,
            'data_sessao': data_sessao,
            'nome_candidato': request.form.get('nome_candidato') or candidato['nome'],
            'data_iniciacao': data_iniciacao,
            'hora_iniciacao': request.form.get('hora_iniciacao'),
            'ritual_utilizado': request.form.get('ritual_utilizado'),
            'numero_obreiros_presentes': request.form.get('numero_obreiros_presentes'),
            'presidente_comissao': request.form.get('presidente_comissao'),
            'membros_comissao': request.form.get('membros_comissao'),
            'ata_numero': request.form.get('ata_numero'),
            'ata_data': ata_data,
        }
        
        if notificacao:
            dados['id'] = notificacao['id']
        
        id_salvo = NotificacaoIniciacao.salvar(dados)
        
        if acao == 'rascunho':
            flash('Rascunho salvo com sucesso!', 'success')
            return redirect(url_for('notificacoes.notificar_iniciacao', candidato_id=candidato_id))
        
        elif acao == 'enviar':
            protocolo = f"GOB-{datetime.now().year}-{candidato_id}-{int(datetime.now().timestamp())}"
            
            try:
                pdf_service = PDFService()
                pdf_service.init_app(current_app._get_current_object())
                
                dados_pdf = {
                    'candidato_id': candidato_id,
                    'loja_nome': dados['loja_nome'],
                    'loja_numero': dados['loja_numero'],
                    'loja_oriente': dados['loja_oriente'],
                    'data_sessao': dados['data_sessao'],
                    'nome_candidato': dados['nome_candidato'],
                    'data_iniciacao': dados['data_iniciacao'],
                    'hora_iniciacao': dados['hora_iniciacao'],
                    'ritual_utilizado': dados['ritual_utilizado'],
                    'numero_obreiros': dados['numero_obreiros_presentes'],
                    'presidente_comissao': dados['presidente_comissao'],
                    'membros_comissao': dados['membros_comissao'],
                    'ata_numero': dados['ata_numero'],
                    'protocolo': protocolo
                }
                
                resultado_pdf = pdf_service.gerar_comunicado_iniciacao(dados_pdf)
                pdf_url = resultado_pdf['url']
                
                NotificacaoIniciacao.atualizar_status(id_salvo, 'enviado', protocolo=protocolo, data_envio=datetime.now(), pdf_url=pdf_url)
                
                flash(f'✅ Notificação enviada com sucesso! Protocolo: {protocolo}', 'success')
                flash(f'📄 PDF gerado: {pdf_url}', 'info')
                
            except Exception as e:
                flash(f'❌ Erro ao gerar PDF: {str(e)}', 'danger')
                NotificacaoIniciacao.atualizar_status(id_salvo, 'enviado', protocolo=protocolo, data_envio=datetime.now())
            
            return redirect('/candidatos')
    
    return render_template('notificacao_iniciacao.html', 
                         candidato=candidato, 
                         notificacao=notificacao,
                         loja_orient=loja_orient,
                         now=datetime.now())

@notificacoes_bp.route('/obreiro/<int:obreiro_id>/notificar-elevacao', methods=['GET', 'POST'])
def notificar_elevacao(obreiro_id):
    from database import get_db_connection
    from models.notificacoes_gob import NotificacaoElevacao
    from datetime import datetime
    from flask import flash, redirect, render_template, request
    
    with get_db_connection() as cursor:
        cursor.execute("""
            SELECT id, nome_completo as nome, email, telefone, 
                   loja_nome, loja_numero, loja_orient,
                   data_iniciacao, cim_numero, grau_atual
            FROM usuarios 
            WHERE id = %s AND tipo = 'obreiro'
        """, (obreiro_id,))
        obreiro = cursor.fetchone()
    
    if not obreiro:
        flash('Obreiro não encontrado!', 'danger')
        return redirect('/obreiros')
    
    # VERIFICAR SE JÁ FOI ELEVADO
    if obreiro.get('data_elevacao'):
        flash('Este obreiro já foi elevado a Companheiro! Não é possível enviar nova notificação.', 'warning')
        return redirect(f'/obreiros/{obreiro_id}')
    
    notificacao = NotificacaoElevacao.buscar_por_obreiro(obreiro_id)
    
    if request.method == 'POST':
        acao = request.form.get('acao')
        
        data_iniciacao = obreiro.get('data_iniciacao')
        tempo_aprendiz = None
        if data_iniciacao:
            dias = (datetime.now().date() - data_iniciacao).days
            meses = dias // 30
            tempo_aprendiz = f"{meses} meses"
        
        dados = {
            'obreiro_id': obreiro_id,
            'numero_processo': request.form.get('numero_processo') or f"ELEV-{obreiro_id}-{datetime.now().year}",
            'loja_nome': request.form.get('loja_nome') or obreiro.get('loja_nome', ''),
            'loja_numero': request.form.get('loja_numero') or obreiro.get('loja_numero', ''),
            'loja_oriente': request.form.get('loja_oriente') or obreiro.get('loja_orient', ''),
            'data_sessao': request.form.get('data_sessao'),
            'nome_aprendiz': obreiro['nome'],
            'cim_numero': obreiro.get('cim_numero', ''),
            'data_iniciacao': data_iniciacao.strftime('%Y-%m-%d') if data_iniciacao else None,
            'data_elevacao': request.form.get('data_elevacao'),
            'tempo_aprendiz': tempo_aprendiz,
            'frequencia_sessoes': request.form.get('frequencia_sessoes'),
            'trabalhos_apresentados': request.form.get('trabalhos_apresentados'),
            'nota_exame': request.form.get('nota_exame'),
            'conceito_final': request.form.get('conceito_final'),
            'ata_numero': request.form.get('ata_numero'),
            'ata_data': request.form.get('ata_data'),
        }
        
        if notificacao:
            dados['id'] = notificacao['id']
        
        id_salvo = NotificacaoElevacao.salvar(dados)
        
        if acao == 'rascunho':
            flash('Rascunho salvo com sucesso!', 'success')
            return redirect(f'/notificacoes/obreiro/{obreiro_id}/notificar-elevacao')
        
        elif acao == 'enviar':
            protocolo = f"GOB-ELEV-{datetime.now().year}-{obreiro_id}-{int(datetime.now().timestamp())}"
            NotificacaoElevacao.atualizar_status(id_salvo, 'enviado', protocolo=protocolo, data_envio=datetime.now())
            
            # ATUALIZAR O PERFIL DO OBREIRO COM A DATA DE ELEVAÇÃO
            data_elevacao = request.form.get('data_elevacao')
            if data_elevacao:
                with get_db_connection() as cursor:
                    cursor.execute("""
                        UPDATE usuarios 
                        SET data_elevacao = %s,
                            grau_atual = 2
                        WHERE id = %s
                    """, (data_elevacao, obreiro_id))
            
            flash(f'✅ Notificação de Elevação enviada com sucesso! Protocolo: {protocolo}', 'success')
            if data_elevacao:
                flash(f'📅 Data de Elevação atualizada para {data_elevacao}', 'success')
            return redirect('/obreiros')
    
    return render_template('notificacao_elevacao.html', 
                         obreiro=obreiro, 
                         notificacao=notificacao,
                         now=datetime.now())


@notificacoes_bp.route('/obreiro/<int:obreiro_id>/notificar-exaltacao', methods=['GET', 'POST'])
def notificar_exaltacao(obreiro_id):
    from database import get_db_connection
    from models.notificacoes_gob import NotificacaoExaltacao
    from datetime import datetime
    from flask import flash, redirect, render_template, request
    
    with get_db_connection() as cursor:
        cursor.execute("""
            SELECT id, nome_completo as nome, email, telefone, 
                   loja_nome, loja_numero, loja_orient,
                   data_iniciacao, data_elevacao, cim_numero, grau_atual
            FROM usuarios 
            WHERE id = %s AND tipo = 'obreiro'
        """, (obreiro_id,))
        obreiro = cursor.fetchone()
    
    if not obreiro:
        flash('Obreiro não encontrado!', 'danger')
        return redirect('/obreiros')
    
    # VERIFICAR SE JÁ FOI ELEVADO (necessário para exaltação)
    if not obreiro.get('data_elevacao'):
        flash('O obreiro ainda não foi elevado a Companheiro. Primeiro registre a Elevação.', 'warning')
        return redirect(f'/obreiros/{obreiro_id}')
    
    # VERIFICAR SE JÁ FOI EXALTADO
    if obreiro.get('data_exaltacao'):
        flash('Este obreiro já foi exaltado a Mestre! Não é possível enviar nova notificação.', 'warning')
        return redirect(f'/obreiros/{obreiro_id}')
    
    notificacao = NotificacaoExaltacao.buscar_por_obreiro(obreiro_id)
    
    if request.method == 'POST':
        acao = request.form.get('acao')
        
        dados = {
            'obreiro_id': obreiro_id,
            'numero_processo': request.form.get('numero_processo') or f"EXALT-{obreiro_id}-{datetime.now().year}",
            'loja_nome': request.form.get('loja_nome') or obreiro.get('loja_nome', ''),
            'loja_numero': request.form.get('loja_numero') or obreiro.get('loja_numero', ''),
            'loja_oriente': request.form.get('loja_oriente') or obreiro.get('loja_orient', ''),
            'data_sessao': request.form.get('data_sessao'),
            'nome_companheiro': obreiro['nome'],
            'cim_numero': obreiro.get('cim_numero', ''),
            'data_iniciacao': obreiro.get('data_iniciacao').strftime('%Y-%m-%d') if obreiro.get('data_iniciacao') else None,
            'data_elevacao': obreiro.get('data_elevacao').strftime('%Y-%m-%d') if obreiro.get('data_elevacao') else None,
            'data_exaltacao': request.form.get('data_exaltacao'),
            'trabalhos_apresentados': request.form.get('trabalhos_apresentados'),
            'terca_camara': request.form.get('terca_camara'),
            'prova_camara_meio': request.form.get('prova_camara_meio'),
            'prova_camara_justica': request.form.get('prova_camara_justica'),
            'ata_numero': request.form.get('ata_numero'),
            'ata_data': request.form.get('ata_data'),
        }
        
        if notificacao:
            dados['id'] = notificacao['id']
        
        id_salvo = NotificacaoExaltacao.salvar(dados)
        
        if acao == 'rascunho':
            flash('Rascunho salvo com sucesso!', 'success')
            return redirect(f'/notificacoes/obreiro/{obreiro_id}/notificar-exaltacao')
        
        elif acao == 'enviar':
            protocolo = f"GOB-EXALT-{datetime.now().year}-{obreiro_id}-{int(datetime.now().timestamp())}"
            NotificacaoExaltacao.atualizar_status(id_salvo, 'enviado', protocolo=protocolo, data_envio=datetime.now())
            
            # ATUALIZAR O PERFIL DO OBREIRO COM A DATA DE EXALTAÇÃO
            data_exaltacao = request.form.get('data_exaltacao')
            if data_exaltacao:
                with get_db_connection() as cursor:
                    cursor.execute("""
                        UPDATE usuarios 
                        SET data_exaltacao = %s,
                            grau_atual = 3
                        WHERE id = %s
                    """, (data_exaltacao, obreiro_id))
            
            flash(f'✅ Notificação de Exaltação enviada com sucesso! Protocolo: {protocolo}', 'success')
            if data_exaltacao:
                flash(f'📅 Data de Exaltação atualizada para {data_exaltacao}', 'success')
            return redirect('/obreiros')
    
    return render_template('notificacao_exaltacao.html', 
                         obreiro=obreiro, 
                         notificacao=notificacao,
                         now=datetime.now())


@notificacoes_bp.route('/historico')
def historico_notificacoes():
    from models.notificacoes_gob import NotificacaoIniciacao, NotificacaoElevacao, NotificacaoExaltacao
    from database import get_db_connection
    
    with get_db_connection() as cursor:
        cursor.execute("SELECT * FROM notificacoes_iniciacao ORDER BY created_at DESC")
        iniciacoes = cursor.fetchall()
        
        cursor.execute("SELECT * FROM notificacoes_elevacao ORDER BY created_at DESC")
        elevacoes = cursor.fetchall()
        
        cursor.execute("SELECT * FROM notificacoes_exaltacao ORDER BY created_at DESC")
        exaltacoes = cursor.fetchall()
    
    return render_template('historico_notificacoes.html', 
                         iniciacoes=iniciacoes,
                         elevacoes=elevacoes,
                         exaltacoes=exaltacoes)