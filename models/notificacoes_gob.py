# models/notificacoes_gob.py
from database import get_db_connection
from datetime import datetime

class NotificacaoIniciacao:
    @staticmethod
    def salvar(dados):
        """Salva uma notificação de iniciação"""
        with get_db_connection() as cursor:
            # TRATAR CAMPOS DE DATA (converter string vazia para None)
            data_sessao = dados.get('data_sessao') if dados.get('data_sessao') else None
            data_iniciacao = dados.get('data_iniciacao') if dados.get('data_iniciacao') else None
            ata_data = dados.get('ata_data') if dados.get('ata_data') else None
            
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
                        protocolo_gob = %s,
                        pdf_url = %s
                    WHERE id = %s
                """, (
                    dados.get('numero_processo'),
                    dados.get('loja_nome'),
                    dados.get('loja_numero'),
                    dados.get('loja_oriente'),
                    data_sessao,
                    dados.get('nome_candidato'),
                    data_iniciacao,
                    dados.get('hora_iniciacao'),
                    dados.get('ritual_utilizado'),
                    dados.get('numero_obreiros_presentes', 0) if dados.get('numero_obreiros_presentes') else 0,
                    dados.get('presidente_comissao'),
                    dados.get('membros_comissao'),
                    dados.get('ata_numero'),
                    ata_data,
                    dados.get('status_envio', 'pendente'),
                    dados.get('data_envio'),
                    dados.get('protocolo_gob'),
                    dados.get('pdf_url'),
                    dados['id']
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
                        status_envio, created_at, pdf_url
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    dados.get('candidato_id'),
                    dados.get('numero_processo'),
                    dados.get('loja_nome'),
                    dados.get('loja_numero'),
                    dados.get('loja_oriente'),
                    data_sessao,
                    dados.get('nome_candidato'),
                    data_iniciacao,
                    dados.get('hora_iniciacao'),
                    dados.get('ritual_utilizado'),
                    dados.get('numero_obreiros_presentes', 0) if dados.get('numero_obreiros_presentes') else 0,
                    dados.get('presidente_comissao'),
                    dados.get('membros_comissao'),
                    dados.get('ata_numero'),
                    ata_data,
                    dados.get('status_envio', 'pendente'),
                    datetime.now(),
                    dados.get('pdf_url')
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
        """Busca notificação de iniciação por ID do obreiro"""
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_iniciacao 
                WHERE candidato_id = %s
                ORDER BY id DESC
                LIMIT 1
            """, (obreiro_id,))
            return cursor.fetchone()
    
    @staticmethod
    def atualizar_status(id, status, protocolo=None, data_envio=None, pdf_url=None):
        """Atualiza o status da notificação"""
        with get_db_connection() as cursor:
            if protocolo and pdf_url:
                cursor.execute("""
                    UPDATE notificacoes_iniciacao SET
                        status_envio = %s,
                        protocolo_gob = %s,
                        data_envio = %s,
                        pdf_url = %s
                    WHERE id = %s
                """, (status, protocolo, data_envio or datetime.now(), pdf_url, id))
            elif protocolo:
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


# Classes NotificacaoElevacao e NotificacaoExaltacao (similar, com os mesmos tratamentos de data)
class NotificacaoElevacao:
    @staticmethod
    def salvar(dados):
        with get_db_connection() as cursor:
            data_sessao = dados.get('data_sessao') if dados.get('data_sessao') else None
            data_iniciacao = dados.get('data_iniciacao') if dados.get('data_iniciacao') else None
            data_elevacao = dados.get('data_elevacao') if dados.get('data_elevacao') else None
            ata_data = dados.get('ata_data') if dados.get('ata_data') else None
            
            if dados.get('id'):
                cursor.execute("""
                    UPDATE notificacoes_elevacao SET
                        numero_processo = %s, loja_nome = %s, loja_numero = %s,
                        loja_oriente = %s, data_sessao = %s, nome_aprendiz = %s,
                        cim_aprendiz = %s, data_iniciacao = %s, data_elevacao = %s,
                        tempo_aprendiz = %s, frequencia_sessoes = %s,
                        trabalhos_apresentados = %s, nota_exame = %s,
                        conceito_final = %s, ata_numero = %s, ata_data = %s,
                        status_envio = %s, data_envio = %s, protocolo_gob = %s,
                        pdf_url = %s
                    WHERE id = %s
                """, (
                    dados.get('numero_processo'), dados.get('loja_nome'), dados.get('loja_numero'),
                    dados.get('loja_oriente'), data_sessao, dados.get('nome_aprendiz'),
                    dados.get('cim_numero'), data_iniciacao, data_elevacao,
                    dados.get('tempo_aprendiz'), dados.get('frequencia_sessoes'),
                    dados.get('trabalhos_apresentados'), dados.get('nota_exame'),
                    dados.get('conceito_final'), dados.get('ata_numero'), ata_data,
                    dados.get('status_envio', 'pendente'), dados.get('data_envio'),
                    dados.get('protocolo_gob'), dados.get('pdf_url'), dados['id']
                ))
                return dados['id']
            else:
                cursor.execute("""
                    INSERT INTO notificacoes_elevacao (
                        candidato_id, numero_processo, loja_nome, loja_numero, loja_oriente,
                        data_sessao, nome_aprendiz, cim_aprendiz, data_iniciacao, data_elevacao,
                        tempo_aprendiz, frequencia_sessoes, trabalhos_apresentados,
                        nota_exame, conceito_final, ata_numero, ata_data,
                        status_envio, created_at, pdf_url
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    dados.get('obreiro_id') or dados.get('candidato_id'),
                    dados.get('numero_processo'), dados.get('loja_nome'),
                    dados.get('loja_numero'), dados.get('loja_oriente'), data_sessao,
                    dados.get('nome_aprendiz'), dados.get('cim_numero'), data_iniciacao,
                    data_elevacao, dados.get('tempo_aprendiz'), dados.get('frequencia_sessoes'),
                    dados.get('trabalhos_apresentados'), dados.get('nota_exame'),
                    dados.get('conceito_final'), dados.get('ata_numero'), ata_data,
                    dados.get('status_envio', 'pendente'), datetime.now(), dados.get('pdf_url')
                ))
                return cursor.fetchone()['id']
    
    @staticmethod
    def buscar_por_candidato(candidato_id):
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_elevacao 
                WHERE candidato_id = %s ORDER BY id DESC LIMIT 1
            """, (candidato_id,))
            return cursor.fetchone()
    
    @staticmethod
    def buscar_por_obreiro(obreiro_id):
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_elevacao 
                WHERE candidato_id = %s ORDER BY id DESC LIMIT 1
            """, (obreiro_id,))
            return cursor.fetchone()
    
    @staticmethod
    def atualizar_status(id, status, protocolo=None, data_envio=None, pdf_url=None):
        with get_db_connection() as cursor:
            if protocolo and pdf_url:
                cursor.execute("""
                    UPDATE notificacoes_elevacao SET
                        status_envio = %s, protocolo_gob = %s, data_envio = %s, pdf_url = %s
                    WHERE id = %s
                """, (status, protocolo, data_envio or datetime.now(), pdf_url, id))
            elif protocolo:
                cursor.execute("""
                    UPDATE notificacoes_elevacao SET
                        status_envio = %s, protocolo_gob = %s, data_envio = %s
                    WHERE id = %s
                """, (status, protocolo, data_envio or datetime.now(), id))
            else:
                cursor.execute("""
                    UPDATE notificacoes_elevacao SET
                        status_envio = %s, data_envio = %s
                    WHERE id = %s
                """, (status, data_envio or datetime.now(), id))
    
    @staticmethod
    def buscar_por_id(id):
        with get_db_connection() as cursor:
            cursor.execute("SELECT * FROM notificacoes_elevacao WHERE id = %s", (id,))
            return cursor.fetchone()


class NotificacaoExaltacao:
    @staticmethod
    def salvar(dados):
        with get_db_connection() as cursor:
            data_sessao = dados.get('data_sessao') if dados.get('data_sessao') else None
            data_iniciacao = dados.get('data_iniciacao') if dados.get('data_iniciacao') else None
            data_elevacao = dados.get('data_elevacao') if dados.get('data_elevacao') else None
            data_exaltacao = dados.get('data_exaltacao') if dados.get('data_exaltacao') else None
            ata_data = dados.get('ata_data') if dados.get('ata_data') else None
            
            if dados.get('id'):
                cursor.execute("""
                    UPDATE notificacoes_exaltacao SET
                        numero_processo = %s, loja_nome = %s, loja_numero = %s,
                        loja_oriente = %s, data_sessao = %s, nome_companheiro = %s,
                        cim_companheiro = %s, data_iniciacao = %s, data_elevacao = %s,
                        data_exaltacao = %s, trabalhos_apresentados = %s,
                        terca_camara = %s, prova_camara_meio = %s, prova_camara_justica = %s,
                        ata_numero = %s, ata_data = %s, status_envio = %s,
                        data_envio = %s, protocolo_gob = %s, pdf_url = %s
                    WHERE id = %s
                """, (
                    dados.get('numero_processo'), dados.get('loja_nome'), dados.get('loja_numero'),
                    dados.get('loja_oriente'), data_sessao, dados.get('nome_companheiro'),
                    dados.get('cim_numero'), data_iniciacao, data_elevacao, data_exaltacao,
                    dados.get('trabalhos_apresentados'), dados.get('terca_camara'),
                    dados.get('prova_camara_meio'), dados.get('prova_camara_justica'),
                    dados.get('ata_numero'), ata_data, dados.get('status_envio', 'pendente'),
                    dados.get('data_envio'), dados.get('protocolo_gob'), dados.get('pdf_url'), dados['id']
                ))
                return dados['id']
            else:
                cursor.execute("""
                    INSERT INTO notificacoes_exaltacao (
                        candidato_id, numero_processo, loja_nome, loja_numero, loja_oriente,
                        data_sessao, nome_companheiro, cim_companheiro, data_iniciacao,
                        data_elevacao, data_exaltacao, trabalhos_apresentados,
                        terca_camara, prova_camara_meio, prova_camara_justica,
                        ata_numero, ata_data, status_envio, created_at, pdf_url
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    dados.get('obreiro_id') or dados.get('candidato_id'),
                    dados.get('numero_processo'), dados.get('loja_nome'),
                    dados.get('loja_numero'), dados.get('loja_oriente'), data_sessao,
                    dados.get('nome_companheiro'), dados.get('cim_numero'),
                    data_iniciacao, data_elevacao, data_exaltacao,
                    dados.get('trabalhos_apresentados'), dados.get('terca_camara'),
                    dados.get('prova_camara_meio'), dados.get('prova_camara_justica'),
                    dados.get('ata_numero'), ata_data,
                    dados.get('status_envio', 'pendente'), datetime.now(), dados.get('pdf_url')
                ))
                return cursor.fetchone()['id']
    
    @staticmethod
    def buscar_por_candidato(candidato_id):
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_exaltacao 
                WHERE candidato_id = %s ORDER BY id DESC LIMIT 1
            """, (candidato_id,))
            return cursor.fetchone()
    
    @staticmethod
    def buscar_por_obreiro(obreiro_id):
        with get_db_connection() as cursor:
            cursor.execute("""
                SELECT * FROM notificacoes_exaltacao 
                WHERE candidato_id = %s ORDER BY id DESC LIMIT 1
            """, (obreiro_id,))
            return cursor.fetchone()
    
    @staticmethod
    def atualizar_status(id, status, protocolo=None, data_envio=None, pdf_url=None):
        with get_db_connection() as cursor:
            if protocolo and pdf_url:
                cursor.execute("""
                    UPDATE notificacoes_exaltacao SET
                        status_envio = %s, protocolo_gob = %s, data_envio = %s, pdf_url = %s
                    WHERE id = %s
                """, (status, protocolo, data_envio or datetime.now(), pdf_url, id))
            elif protocolo:
                cursor.execute("""
                    UPDATE notificacoes_exaltacao SET
                        status_envio = %s, protocolo_gob = %s, data_envio = %s
                    WHERE id = %s
                """, (status, protocolo, data_envio or datetime.now(), id))
            else:
                cursor.execute("""
                    UPDATE notificacoes_exaltacao SET
                        status_envio = %s, data_envio = %s
                    WHERE id = %s
                """, (status, data_envio or datetime.now(), id))
    
    @staticmethod
    def buscar_por_id(id):
        with get_db_connection() as cursor:
            cursor.execute("SELECT * FROM notificacoes_exaltacao WHERE id = %s", (id,))
            return cursor.fetchone()