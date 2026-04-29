from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for, session
from database import get_db_connection
from datetime import datetime, timedelta
import json

calendario_bp = Blueprint('calendario', __name__, url_prefix='/calendario')

@calendario_bp.route('/')
def calendario():
    """Página principal do calendário"""
    if not session.get('user_id'):
        flash('Você precisa estar logado para acessar esta página.', 'danger')
        return redirect(url_for('login'))
    
    return render_template('calendario.html', now=datetime.now())


@calendario_bp.route('/api/eventos')
def api_eventos():
    """Retorna todos os eventos para o calendário"""
    if not session.get('user_id'):
        return jsonify([])
    
    try:
        with get_db_connection() as cursor:
            eventos_calendario = []
            hoje = datetime.now().date()
            
            # 1. Buscar reuniões/sessões
            cursor.execute("""
                SELECT id, titulo, data, hora_inicio, local, observacoes
                FROM reunioes 
                WHERE data >= %s
                ORDER BY data ASC
            """, (hoje,))
            reunioes = cursor.fetchall()
            
            for reuniao in reunioes:
                # Usar APENAS a data como string, SEM conversão de timezone
                data_str = reuniao['data'].strftime('%Y-%m-%d')
                
                evento = {
                    'id': f"reuniao_{reuniao['id']}",
                    'title': reuniao['titulo'],
                    'start': data_str,  # ← String simples, sem T e sem timezone
                    'color': '#3b82f6',
                    'className': 'evento-sessao',
                    'allDay': True,  # ← Forçar como dia inteiro
                    'extendedProps': {
                        'tipo': 'sessao',
                        'descricao': reuniao.get('observacoes', 'Sessão da Loja'),
                        'local': reuniao.get('local', 'Templo Maçônico')
                    }
                }
                
                eventos_calendario.append(evento)
            
            # 2. Buscar aniversários de obreiros
            cursor.execute("""
                SELECT id, nome_completo as nome, data_nascimento
                FROM usuarios 
                WHERE data_nascimento IS NOT NULL AND ativo = 1
            """)
            aniversarios = cursor.fetchall()
            
            for aniv in aniversarios:
                data_nasc = aniv['data_nascimento']
                data_aniversario = datetime(hoje.year, data_nasc.month, data_nasc.day).date()
                if data_aniversario < hoje:
                    data_aniversario = datetime(hoje.year + 1, data_nasc.month, data_nasc.day).date()
                
                eventos_calendario.append({
                    'id': f"aniv_obreiro_{aniv['id']}",
                    'title': f"🎂 {aniv['nome']}",
                    'start': data_aniversario.strftime('%Y-%m-%d'),  # ← String simples
                    'color': '#10b981',
                    'className': 'evento-aniversario',
                    'allDay': True,
                    'extendedProps': {
                        'tipo': 'aniversario',
                        'descricao': f"Aniversário do obreiro {aniv['nome']}"
                    }
                })
            
            return jsonify(eventos_calendario)
            
    except Exception as e:
        print(f"Erro ao carregar eventos: {e}")
        return jsonify([])


@calendario_bp.route('/api/proximos')
def api_proximos_eventos():
    """Retorna os próximos 10 eventos"""
    if not session.get('user_id'):
        return jsonify({'eventos': []})
    
    try:
        with get_db_connection() as cursor:
            hoje = datetime.now().date()
            result = []
            
            # 1. Próximas reuniões
            cursor.execute("""
                SELECT id, titulo, 'sessao' as tipo, data as data_inicio, hora_inicio as horario, 
                       local, '#3b82f6' as cor
                FROM reunioes 
                WHERE data >= %s
                ORDER BY data ASC
                LIMIT 5
            """, (hoje,))
            reunioes = cursor.fetchall()
            
            for r in reunioes:
                result.append({
                    'id': r['id'],
                    'titulo': r['titulo'],
                    'tipo': 'sessao',
                    'data_inicio': r['data_inicio'].isoformat(),
                    'horario': r['horario'].strftime('%H:%M') if r['horario'] else None,
                    'local_evento': r.get('local'),
                    'cor': '#3b82f6'
                })
            
            # 2. Próximos aniversários de obreiros
            cursor.execute("""
                SELECT id, nome_completo as nome, data_nascimento
                FROM usuarios 
                WHERE data_nascimento IS NOT NULL AND ativo = 1
                ORDER BY 
                    CASE 
                        WHEN date_part('month', data_nascimento) > date_part('month', CURRENT_DATE) 
                        OR (date_part('month', data_nascimento) = date_part('month', CURRENT_DATE) 
                            AND date_part('day', data_nascimento) >= date_part('day', CURRENT_DATE))
                        THEN date_part('month', data_nascimento) * 100 + date_part('day', data_nascimento)
                        ELSE (date_part('month', data_nascimento) + 12) * 100 + date_part('day', data_nascimento)
                    END
                LIMIT 5
            """)
            aniversarios = cursor.fetchall()
            
            for aniv in aniversarios:
                data_nasc = aniv['data_nascimento']
                data_aniversario = datetime(hoje.year, data_nasc.month, data_nasc.day).date()
                if data_aniversario < hoje:
                    data_aniversario = datetime(hoje.year + 1, data_nasc.month, data_nasc.day).date()
                
                result.append({
                    'id': f"aniv_{aniv['id']}",
                    'titulo': f"🎂 {aniv['nome']}",
                    'tipo': 'aniversario',
                    'data_inicio': data_aniversario.isoformat(),
                    'cor': '#10b981'
                })
            
            # Ordenar por data
            result.sort(key=lambda x: x['data_inicio'])
            
            return jsonify({'eventos': result[:10]})
            
    except Exception as e:
        print(f"Erro: {e}")
        return jsonify({'eventos': []})


@calendario_bp.route('/api/aniversariantes')
def api_aniversariantes():
    """Retorna aniversariantes do mês atual - Busca da tabela usuarios"""
    if not session.get('user_id'):
        return jsonify({'aniversariantes': []})
    
    try:
        with get_db_connection() as cursor:
            hoje = datetime.now().date()
            mes_atual = hoje.month
            
            # Buscar obreiros aniversariantes do mês atual
            cursor.execute("""
                SELECT id, nome_completo as nome, data_nascimento,
                       CASE 
                           WHEN EXTRACT(DAY FROM data_nascimento) = EXTRACT(DAY FROM CURRENT_DATE)
                           AND EXTRACT(MONTH FROM data_nascimento) = EXTRACT(MONTH FROM CURRENT_DATE)
                           THEN 'hoje'
                           ELSE 'proximo'
                       END as status
                FROM usuarios 
                WHERE EXTRACT(MONTH FROM data_nascimento) = %s 
                AND data_nascimento IS NOT NULL 
                AND ativo = 1
                ORDER BY EXTRACT(DAY FROM data_nascimento)
            """, (mes_atual,))
            aniversariantes = cursor.fetchall()
            
            result = []
            for aniv in aniversariantes:
                # Calcular idade
                idade = hoje.year - aniv['data_nascimento'].year
                if hoje < datetime(hoje.year, aniv['data_nascimento'].month, aniv['data_nascimento'].day).date():
                    idade -= 1
                
                result.append({
                    'id': aniv['id'],
                    'nome': aniv['nome'],
                    'data_nascimento': aniv['data_nascimento'].strftime('%d/%m/%Y'),
                    'data_nascimento_formatted': aniv['data_nascimento'].strftime('%d/%m'),
                    'idade': idade,
                    'tipo': 'obreiro',
                    'status': aniv.get('status', 'proximo'),
                    'dia': aniv['data_nascimento'].day
                })
            
            result.sort(key=lambda x: x['dia'])
            
            return jsonify({'aniversariantes': result})
            
    except Exception as e:
        print(f"Erro em aniversariantes: {e}")
        return jsonify({'aniversariantes': []})


@calendario_bp.route('/api/estatisticas')
def api_estatisticas():
    """Retorna estatísticas do calendário para o mês selecionado"""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Não autenticado'})
    
    try:
        ano = request.args.get('ano', type=int)
        mes = request.args.get('mes', type=int)
        
        if not ano or not mes:
            hoje = datetime.now()
            ano = hoje.year
            mes = hoje.month
        
        with get_db_connection() as cursor:
            # 1. Sessões/Reuniões do mês (da tabela reunioes)
            cursor.execute("""
                SELECT COUNT(*) as total
                FROM reunioes 
                WHERE EXTRACT(YEAR FROM data) = %s 
                AND EXTRACT(MONTH FROM data) = %s
            """, (ano, mes))
            sessoes = cursor.fetchone()['total']
            
            # 2. Aniversários de obreiros do mês (da tabela usuarios)
            cursor.execute("""
                SELECT COUNT(*) as total
                FROM usuarios 
                WHERE EXTRACT(MONTH FROM data_nascimento) = %s 
                AND data_nascimento IS NOT NULL
                AND ativo = 1
            """, (mes,))
            aniversarios_obreiros = cursor.fetchone()['total']
            
            # 3. Aniversários de familiares (se tiver tabela familiares)
            aniversarios_familiares = 0
            try:
                cursor.execute("""
                    SELECT COUNT(*) as total
                    FROM familiares 
                    WHERE EXTRACT(MONTH FROM data_nascimento) = %s 
                    AND data_nascimento IS NOT NULL
                """, (mes,))
                aniversarios_familiares = cursor.fetchone()['total']
            except:
                pass
            
            total_aniversarios = aniversarios_obreiros + aniversarios_familiares
            
            # 4. Eventos e prazos (da tabela calendario_eventos)
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN tipo = 'evento' THEN 1 END) as eventos,
                    COUNT(CASE WHEN tipo = 'prazo' THEN 1 END) as prazos
                FROM calendario_eventos 
                WHERE EXTRACT(YEAR FROM data_inicio) = %s 
                AND EXTRACT(MONTH FROM data_inicio) = %s
                AND ativo = TRUE
            """, (ano, mes))
            resultado = cursor.fetchone()
            eventos = resultado['eventos'] if resultado else 0
            prazos = resultado['prazos'] if resultado else 0
            
            return jsonify({
                'success': True,
                'sessoes': sessoes,
                'eventos': eventos,
                'prazos': prazos,
                'aniversarios': total_aniversarios
            })
            
    except Exception as e:
        print(f"Erro em estatisticas: {e}")
        return jsonify({'success': False, 'error': str(e)})


@calendario_bp.route('/api/eventos', methods=['POST'])
def api_criar_evento():
    """Cria um novo evento no calendário"""
    if not session.get('user_id') or session.get('tipo') != 'admin':
        return jsonify({'success': False, 'error': 'Acesso negado'}), 403
    
    try:
        data = request.json
        
        with get_db_connection() as cursor:
            cursor.execute("""
                INSERT INTO calendario_eventos 
                (titulo, descricao, tipo, data_inicio, data_fim, horario, 
                 local_evento, cor, icone, recorrente, recorrencia_tipo, criado_por)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data.get('titulo'), data.get('descricao'), data.get('tipo'),
                data.get('data_inicio'), data.get('data_fim'), data.get('horario'),
                data.get('local_evento'), data.get('cor'), data.get('icone'),
                data.get('recorrente') == '1', data.get('recorrencia_tipo'),
                session.get('user_id')
            ))
            
            evento_id = cursor.fetchone()['id']
            
        return jsonify({'success': True, 'evento_id': evento_id})
        
    except Exception as e:
        print(f"Erro ao criar evento: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@calendario_bp.route('/api/eventos/<int:evento_id>', methods=['DELETE'])
def api_excluir_evento(evento_id):
    """Exclui um evento do calendário"""
    if not session.get('user_id') or session.get('tipo') != 'admin':
        return jsonify({'success': False, 'error': 'Acesso negado'}), 403
    
    try:
        with get_db_connection() as cursor:
            cursor.execute("UPDATE calendario_eventos SET ativo = 0 WHERE id = %s", (evento_id,))
        
        return jsonify({'success': True})
        
    except Exception as e:
        print(f"Erro ao excluir evento: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500