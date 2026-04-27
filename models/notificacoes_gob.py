# models/notificacoes_gob.py
from database import get_db_connection
from datetime import datetime

class NotificacaoIniciacao:
    @staticmethod
    def salvar(dados):
        """Salva uma notificação de iniciação"""
        with get_db_connection() as cursor:
            if dados.get('id'):
                # Atualizar existente
                cursor.execute("""
                    UPDATE notificacoes_iniciacao SET
                        numero_processo = %s,
                        loja_nome = %s,
                        loja_numero = %s,
                        loja_oriente = %s,
                        data_sessao = %s,
                        nome_candidato = %s,
                        data_iniciacao = %s,
                        hora_iniciacao = %s,
                        ritual_utilizado = %s,
                        numero_obreiros_presentes = %s,
                        presidente_comissao = %s,
                        membros_comissao = %s,
                        ata_numero = %s,
                        ata_data = %s,
                        status_envio = %s,
                        data_envio = %s,
                        protocolo_gob = %s
                    WHERE id = %s
                """, (
                    dados['numero_processo'], dados['loja_nome'], dados['loja_numero'],
                    dados['loja_oriente'], dados['data_sessao'], dados['nome_candidato'],
                    dados['data_iniciacao'], dados['hora_iniciacao'], dados['ritual_utilizado'],
                    dados.get('numero_obreiros_presentes'), dados.get('presidente_comissao'),
                    dados.get('membros_comissao'), dados.get('ata_numero'), dados.get('ata_data'),
                    dados.get('status_envio', 'pendente'), dados.get('data_envio'),
                    dados.get('protocolo_gob'), dados['id']
                ))
                return dados['id']
            else:
                # Inserir novo
                cursor.execute("""
                    INSERT INTO notificacoes_iniciacao (
                        candidato_id, numero_processo, loja_nome, loja_numero, loja_oriente,
                        data_sessao, nome_candidato, data_iniciacao, hora_iniciacao,
                        ritual_utilizado, numero_obreiros_presentes,
                        presidente_comissao, membros_comissao, ata_numero, ata_data,
                        status_envio, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    dados['candidato_id'], dados['numero_processo'], dados['loja_nome'],
                    dados['loja_numero'], dados['loja_oriente'], dados['data_sessao'],
                    dados['nome_candidato'], dados['data_iniciacao'], dados['hora_iniciacao'],
                    dados['ritual_utilizado'], dados.get('numero_obreiros_presentes'),
                    dados.get('presidente_comissao'), dados.get('membros_comissao'),
                    dados.get('ata_numero'), dados.get('ata_data'),
                    dados.get('status_envio', 'pendente'), datetime.now()
                ))
                return cursor.fetchone()['id']
    
    @staticmethod
    def buscar_por_candidato(candidato_id):
        """Busca notificação por candidato_id"""
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_iniciacao 
                WHERE candidato_id = %s
                ORDER BY id DESC
                LIMIT 1
            """, (candidato_id,))
            return cursor.fetchone()
    
    @staticmethod
    def buscar_por_obreiro(obreiro_id):
        """Busca notificação de iniciação por ID do obreiro (candidato_id)"""
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_iniciacao 
                WHERE candidato_id = %s
                ORDER BY id DESC
                LIMIT 1
            """, (obreiro_id,))
            return cursor.fetchone()
    
    @staticmethod
    def atualizar_status(id, status, protocolo=None, data_envio=None):
        """Atualiza o status da notificação"""
        with get_db_connection() as cursor:
            if protocolo:
                cursor.execute("""
                    UPDATE notificacoes_iniciacao SET
                        status_envio = %s,
                        protocolo_gob = %s,
                        data_envio = %s
                    WHERE id = %s
                """, (status, protocolo, data_envio or datetime.now(), id))
            else:
                cursor.execute("""
                    UPDATE notificacoes_iniciacao SET
                        status_envio = %s,
                        data_envio = %s
                    WHERE id = %s
                """, (status, data_envio or datetime.now(), id))
    
    @staticmethod
    def buscar_por_id(id):
        """Busca notificação por ID"""
        with get_db_connection() as cursor:
            cursor.execute("SELECT * FROM notificacoes_iniciacao WHERE id = %s", (id,))
            return cursor.fetchone()


class NotificacaoElevacao:
    @staticmethod
    def salvar(dados):
        """Salva uma notificação de elevação"""
        with get_db_connection() as cursor:
            if dados.get('id'):
                cursor.execute("""
                    UPDATE notificacoes_elevacao SET
                        numero_processo = %s,
                        loja_nome = %s,
                        loja_numero = %s,
                        loja_oriente = %s,
                        data_sessao = %s,
                        nome_aprendiz = %s,
                        cim_aprendiz = %s,
                        data_iniciacao = %s,
                        data_elevacao = %s,
                        tempo_aprendiz = %s,
                        frequencia_sessoes = %s,
                        trabalhos_apresentados = %s,
                        nota_exame = %s,
                        conceito_final = %s,
                        ata_numero = %s,
                        ata_data = %s,
                        status_envio = %s,
                        data_envio = %s,
                        protocolo_gob = %s
                    WHERE id = %s
                """, (
                    dados['numero_processo'], dados['loja_nome'], dados['loja_numero'],
                    dados['loja_oriente'], dados['data_sessao'], dados['nome_aprendiz'],
                    dados.get('cim_numero'), dados['data_iniciacao'], dados['data_elevacao'],
                    dados['tempo_aprendiz'], dados['frequencia_sessoes'],
                    dados['trabalhos_apresentados'], dados['nota_exame'],
                    dados['conceito_final'], dados['ata_numero'], dados['ata_data'],
                    dados.get('status_envio', 'pendente'), dados.get('data_envio'),
                    dados.get('protocolo_gob'), dados['id']
                ))
                return dados['id']
            else:
                cursor.execute("""
                    INSERT INTO notificacoes_elevacao (
                        candidato_id, numero_processo, loja_nome, loja_numero, loja_oriente,
                        data_sessao, nome_aprendiz, cim_aprendiz, data_iniciacao, data_elevacao,
                        tempo_aprendiz, frequencia_sessoes, trabalhos_apresentados,
                        nota_exame, conceito_final, ata_numero, ata_data,
                        status_envio, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    dados['obreiro_id'] or dados['candidato_id'], 
                    dados['numero_processo'], 
                    dados['loja_nome'],
                    dados['loja_numero'], 
                    dados['loja_oriente'], 
                    dados['data_sessao'],
                    dados['nome_aprendiz'], 
                    dados.get('cim_numero'), 
                    dados['data_iniciacao'], 
                    dados['data_elevacao'],
                    dados['tempo_aprendiz'], 
                    dados['frequencia_sessoes'],
                    dados['trabalhos_apresentados'], 
                    dados['nota_exame'],
                    dados['conceito_final'], 
                    dados['ata_numero'], 
                    dados['ata_data'],
                    dados.get('status_envio', 'pendente'), 
                    datetime.now()
                ))
                return cursor.fetchone()['id']
    
    @staticmethod
    def buscar_por_candidato(candidato_id):
        """Busca notificação de elevação por ID do candidato"""
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_elevacao 
                WHERE candidato_id = %s 
                ORDER BY id DESC LIMIT 1
            """, (candidato_id,))
            return cursor.fetchone()
    
    @staticmethod
    def buscar_por_obreiro(obreiro_id):
        """Busca notificação de elevação por ID do obreiro"""
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_elevacao 
                WHERE candidato_id = %s 
                ORDER BY id DESC LIMIT 1
            """, (obreiro_id,))
            return cursor.fetchone()
    
    @staticmethod
    def atualizar_status(id, status, protocolo=None, data_envio=None):
        """Atualiza o status da notificação de elevação"""
        with get_db_connection() as cursor:
            if protocolo:
                cursor.execute("""
                    UPDATE notificacoes_elevacao SET
                        status_envio = %s,
                        protocolo_gob = %s,
                        data_envio = %s
                    WHERE id = %s
                """, (status, protocolo, data_envio or datetime.now(), id))
            else:
                cursor.execute("""
                    UPDATE notificacoes_elevacao SET
                        status_envio = %s,
                        data_envio = %s
                    WHERE id = %s
                """, (status, data_envio or datetime.now(), id))
    
    @staticmethod
    def buscar_por_id(id):
        """Busca notificação de elevação por ID"""
        with get_db_connection() as cursor:
            cursor.execute("SELECT * FROM notificacoes_elevacao WHERE id = %s", (id,))
            return cursor.fetchone()


class NotificacaoExaltacao:
    @staticmethod
    def salvar(dados):
        """Salva uma notificação de exaltação"""
        with get_db_connection() as cursor:
            if dados.get('id'):
                cursor.execute("""
                    UPDATE notificacoes_exaltacao SET
                        numero_processo = %s,
                        loja_nome = %s,
                        loja_numero = %s,
                        loja_oriente = %s,
                        data_sessao = %s,
                        nome_companheiro = %s,
                        cim_companheiro = %s,
                        data_iniciacao = %s,
                        data_elevacao = %s,
                        data_exaltacao = %s,
                        trabalhos_apresentados = %s,
                        terca_camara = %s,
                        prova_camara_meio = %s,
                        prova_camara_justica = %s,
                        ata_numero = %s,
                        ata_data = %s,
                        status_envio = %s,
                        data_envio = %s,
                        protocolo_gob = %s
                    WHERE id = %s
                """, (
                    dados['numero_processo'], dados['loja_nome'], dados['loja_numero'],
                    dados['loja_oriente'], dados['data_sessao'], dados['nome_companheiro'],
                    dados.get('cim_numero'), dados['data_iniciacao'], dados['data_elevacao'],
                    dados['data_exaltacao'], dados['trabalhos_apresentados'],
                    dados['terca_camara'], dados['prova_camara_meio'],
                    dados['prova_camara_justica'], dados['ata_numero'], dados['ata_data'],
                    dados.get('status_envio', 'pendente'), dados.get('data_envio'),
                    dados.get('protocolo_gob'), dados['id']
                ))
                return dados['id']
            else:
                cursor.execute("""
                    INSERT INTO notificacoes_exaltacao (
                        candidato_id, numero_processo, loja_nome, loja_numero, loja_oriente,
                        data_sessao, nome_companheiro, cim_companheiro, data_iniciacao,
                        data_elevacao, data_exaltacao, trabalhos_apresentados,
                        terca_camara, prova_camara_meio, prova_camara_justica,
                        ata_numero, ata_data, status_envio, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    dados['obreiro_id'] or dados['candidato_id'],
                    dados['numero_processo'],
                    dados['loja_nome'],
                    dados['loja_numero'],
                    dados['loja_oriente'],
                    dados['data_sessao'],
                    dados['nome_companheiro'],
                    dados.get('cim_numero'),
                    dados['data_iniciacao'],
                    dados['data_elevacao'],
                    dados['data_exaltacao'],
                    dados['trabalhos_apresentados'],
                    dados['terca_camara'],
                    dados['prova_camara_meio'],
                    dados['prova_camara_justica'],
                    dados['ata_numero'],
                    dados['ata_data'],
                    dados.get('status_envio', 'pendente'),
                    datetime.now()
                ))
                return cursor.fetchone()['id']
    
    @staticmethod
    def buscar_por_candidato(candidato_id):
        """Busca notificação de exaltação por ID do candidato"""
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_exaltacao 
                WHERE candidato_id = %s 
                ORDER BY id DESC LIMIT 1
            """, (candidato_id,))
            return cursor.fetchone()
    
    @staticmethod
    def buscar_por_obreiro(obreiro_id):
        """Busca notificação de exaltação por ID do obreiro"""
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_exaltacao 
                WHERE candidato_id = %s 
                ORDER BY id DESC LIMIT 1
            """, (obreiro_id,))
            return cursor.fetchone()
    
    @staticmethod
    def atualizar_status(id, status, protocolo=None, data_envio=None):
        """Atualiza o status da notificação de exaltação"""
        with get_db_connection() as cursor:
            if protocolo:
                cursor.execute("""
                    UPDATE notificacoes_exaltacao SET
                        status_envio = %s,
                        protocolo_gob = %s,
                        data_envio = %s
                    WHERE id = %s
                """, (status, protocolo, data_envio or datetime.now(), id))
            else:
                cursor.execute("""
                    UPDATE notificacoes_exaltacao SET
                        status_envio = %s,
                        data_envio = %s
                    WHERE id = %s
                """, (status, data_envio or datetime.now(), id))
    
    @staticmethod
    def buscar_por_id(id):
        """Busca notificação de exaltação por ID"""
        with get_db_connection() as cursor:
            cursor.execute("SELECT * FROM notificacoes_exaltacao WHERE id = %s", (id,))
            return cursor.fetchone()