# services/pdf_service.py
import pdfkit
from jinja2 import Environment, FileSystemLoader
import os
from datetime import datetime
import cloudinary
import cloudinary.uploader
from flask import current_app
import platform
import tempfile

class PDFService:
    def __init__(self, app=None):
        self.template_dir = os.path.join(os.path.dirname(__file__), '..', 'templates')
        self.env = Environment(loader=FileSystemLoader(self.template_dir))
        
        # Configurar o caminho do wkhtmltopdf para Windows
        if platform.system() == 'Windows':
            wkhtmltopdf_paths = [
                r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe',
                r'C:\Program Files (x86)\wkhtmltopdf\bin\wkhtmltopdf.exe',
            ]
            
            wkhtmltopdf_path = None
            for path in wkhtmltopdf_paths:
                if os.path.exists(path):
                    wkhtmltopdf_path = path
                    break
            
            if wkhtmltopdf_path:
                self.pdf_config = pdfkit.configuration(wkhtmltopdf=wkhtmltopdf_path)
                print(f"✅ wkhtmltopdf encontrado em: {wkhtmltopdf_path}")
            else:
                print("⚠️ wkhtmltopdf não encontrado. PDF não será gerado.")
                self.pdf_config = None
        else:
            self.pdf_config = None
        
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        """Inicializa o serviço com o app Flask"""
        cloudinary.config(
            cloud_name=app.config.get('CLOUDINARY_CLOUD_NAME'),
            api_key=app.config.get('CLOUDINARY_API_KEY'),
            api_secret=app.config.get('CLOUDINARY_API_SECRET')
        )
    
    def _get_temp_dir(self):
        """Retorna o diretório temporário correto para o sistema operacional"""
        if platform.system() == 'Windows':
            # Usar o diretório TEMP do Windows
            temp_dir = os.environ.get('TEMP', 'C:\\temp')
        else:
            # Linux/Mac: usar /tmp
            temp_dir = '/tmp'
        
        # Criar diretório se não existir
        os.makedirs(temp_dir, exist_ok=True)
        
        # Criar subdiretório para PDFs
        pdf_dir = os.path.join(temp_dir, 'pdfs')
        os.makedirs(pdf_dir, exist_ok=True)
        
        return pdf_dir
    
    def gerar_comunicado_iniciacao(self, dados):
        """Gera o PDF do comunicado de INICIAÇÃO para o GOB"""
        
        # Converter data para extenso
        if dados.get('data_sessao'):
            data_obj = datetime.strptime(dados['data_sessao'], '%Y-%m-%d')
            dados['data_extenso'] = self._data_por_extenso(data_obj)
        
        dados['data_emissao'] = datetime.now().strftime('%d/%m/%Y às %H:%M')
        
        template = self.env.get_template('gob_comunicado_iniciacao.html')
        html = template.render(dados=dados)
        
        # Usar diretório temporário correto
        temp_dir = self._get_temp_dir()
        
        pdf_filename = f"comunicado_iniciacao_{dados['candidato_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        pdf_path = os.path.join(temp_dir, pdf_filename)
        
        # Configurar opções do PDF
        options = {
            'page-size': 'A4',
            'margin-top': '15mm',
            'margin-right': '15mm',
            'margin-bottom': '15mm',
            'margin-left': '15mm',
            'encoding': 'UTF-8',
            'no-outline': None,
            'enable-local-file-access': None,
        }
        
        # Gerar PDF
        try:
            if self.pdf_config:
                pdfkit.from_string(html, pdf_path, options=options, configuration=self.pdf_config)
            else:
                # Tentar sem configuração (se estiver no PATH)
                pdfkit.from_string(html, pdf_path, options=options)
            
            print(f"✅ PDF gerado com sucesso: {pdf_path}")
            
        except Exception as e:
            print(f"❌ Erro ao gerar PDF: {e}")
            raise Exception(f"Erro ao gerar PDF: {e}. Verifique se o wkhtmltopdf está instalado.")
        
        # Upload para Cloudinary
        try:
            resultado = cloudinary.uploader.upload(
                pdf_path,
                resource_type='raw',
                folder='comunicados_gob/iniciacoes',
                public_id=f"iniciacao_{dados['candidato_id']}_{int(datetime.now().timestamp())}"
            )
            
            # Remover arquivo temporário
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
            
            return {
                'url': resultado['secure_url'],
                'public_id': resultado['public_id']
            }
        except Exception as e:
            print(f"❌ Erro no upload para Cloudinary: {e}")
            # Se falhar no Cloudinary, ainda retorna o caminho local
            return {
                'url': f"file://{pdf_path}",
                'public_id': None,
                'local_path': pdf_path
            }
    
    def _data_por_extenso(self, data):
        """Converte data para formato extenso (ex: 27 de ABRIL de 2026)"""
        meses = {
            1: 'JANEIRO', 2: 'FEVEREIRO', 3: 'MARÇO', 4: 'ABRIL',
            5: 'MAIO', 6: 'JUNHO', 7: 'JULHO', 8: 'AGOSTO',
            9: 'SETEMBRO', 10: 'OUTUBRO', 11: 'NOVEMBRO', 12: 'DEZEMBRO'
        }
        return f"{data.day} de {meses[data.month]} de {data.year}"
        
def gerar_comunicado_elevacao(self, dados):
    """Gera o PDF do comunicado de ELEVAÇÃO para o GOB"""
    
    if dados.get('data_sessao'):
        data_obj = datetime.strptime(dados['data_sessao'], '%Y-%m-%d')
        dados['data_extenso'] = self._data_por_extenso(data_obj)
    
    dados['data_emissao'] = datetime.now().strftime('%d/%m/%Y às %H:%M')
    
    template = self.env.get_template('gob_comunicado_elevacao.html')
    html = template.render(dados=dados)
    
    temp_dir = self._get_temp_dir()
    pdf_filename = f"comunicado_elevacao_{dados['candidato_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    pdf_path = os.path.join(temp_dir, pdf_filename)
    
    options = {
        'page-size': 'A4',
        'margin-top': '15mm',
        'margin-right': '15mm',
        'margin-bottom': '15mm',
        'margin-left': '15mm',
        'encoding': 'UTF-8',
    }
    
    if self.pdf_config:
        pdfkit.from_string(html, pdf_path, options=options, configuration=self.pdf_config)
    else:
        pdfkit.from_string(html, pdf_path, options=options)
    
    resultado = cloudinary.uploader.upload(
        pdf_path,
        resource_type='raw',
        folder='comunicados_gob/elevacoes',
        public_id=f"elevacao_{dados['candidato_id']}_{int(datetime.now().timestamp())}"
    )
    
    os.remove(pdf_path)
    
    return {
        'url': resultado['secure_url'],
        'public_id': resultado['public_id']
    }

def gerar_comunicado_exaltacao(self, dados):
    """Gera o PDF do comunicado de EXALTAÇÃO para o GOB"""
    
    if dados.get('data_sessao'):
        data_obj = datetime.strptime(dados['data_sessao'], '%Y-%m-%d')
        dados['data_extenso'] = self._data_por_extenso(data_obj)
    
    dados['data_emissao'] = datetime.now().strftime('%d/%m/%Y às %H:%M')
    
    template = self.env.get_template('gob_comunicado_exaltacao.html')
    html = template.render(dados=dados)
    
    temp_dir = self._get_temp_dir()
    pdf_filename = f"comunicado_exaltacao_{dados['candidato_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    pdf_path = os.path.join(temp_dir, pdf_filename)
    
    options = {
        'page-size': 'A4',
        'margin-top': '15mm',
        'margin-right': '15mm',
        'margin-bottom': '15mm',
        'margin-left': '15mm',
        'encoding': 'UTF-8',
    }
    
    if self.pdf_config:
        pdfkit.from_string(html, pdf_path, options=options, configuration=self.pdf_config)
    else:
        pdfkit.from_string(html, pdf_path, options=options)
    
    resultado = cloudinary.uploader.upload(
        pdf_path,
        resource_type='raw',
        folder='comunicados_gob/exaltacoes',
        public_id=f"exaltacao_{dados['candidato_id']}_{int(datetime.now().timestamp())}"
    )
    
    os.remove(pdf_path)
    
    return {
        'url': resultado['secure_url'],
        'public_id': resultado['public_id']
    }
        
def gerar_comunicado_gob(self, dados, tipo):
    """Gera o PDF do comunicado GOB (Iniciação, Elevação ou Exaltação)"""
    
    # Converter data para extenso
    if dados.get('data_sessao'):
        data_obj = datetime.strptime(dados['data_sessao'], '%Y-%m-%d')
        dados['data_extenso'] = self._data_por_extenso(data_obj)
    
    dados['data_emissao'] = datetime.now().strftime('%d/%m/%Y às %H:%M')
    dados['tipo'] = tipo
    
    # Adicionar detalhes específicos por tipo
    if tipo == 'INICIAÇÃO':
        dados['detalhes_html'] = f'''
        <table class="main-table">
            <tr>
                <td>Data da Iniciação:</td>
                <td><strong>{dados.get("data_iniciacao", "")}</strong></td>
                <td>Horário:</td>
                <td><strong>{dados.get("hora_iniciacao", "")}</strong></td>
            </tr>
            <tr>
                <td>Ritual Utilizado:</td>
                <td colspan="3"><strong>{dados.get("ritual_utilizado", "")}</strong></td>
            </tr>
            <tr>
                <td>Nº de Obreiros:</td>
                <td><strong>{dados.get("numero_obreiros", "")}</strong></td>
                <td>Ata Nº:</td>
                <td><strong>{dados.get("ata_numero", "")}</strong></td>
            </tr>
        </table>
        '''
    elif tipo == 'ELEVAÇÃO':
        dados['detalhes_html'] = f'''
        <table class="main-table">
            <tr>
                <td>Data da Iniciação:</td>
                <td><strong>{dados.get("data_iniciacao", "")}</strong></td>
                <td>Data da Elevação:</td>
                <td><strong>{dados.get("data_elevacao", "")}</strong></td>
            </tr>
            <tr>
                <td>Tempo no Grau:</td>
                <td><strong>{dados.get("tempo_aprendiz", "")}</strong></td>
                <td>Ata Nº:</td>
                <td><strong>{dados.get("ata_numero", "")}</strong></td>
            </tr>
            <tr>
                <td>Conceito Final:</td>
                <td colspan="3"><strong>{dados.get("conceito_final", "")}</strong></td>
            </tr>
        </table>
        '''
    elif tipo == 'EXALTAÇÃO':
        dados['detalhes_html'] = f'''
        <table class="main-table">
            <tr>
                <td>Data da Iniciação:</td>
                <td><strong>{dados.get("data_iniciacao", "")}</strong></td>
                <td>Data da Elevação:</td>
                <td><strong>{dados.get("data_elevacao", "")}</strong></td>
            </tr>
            <tr>
                <td>Data da Exaltação:</td>
                <td colspan="3"><strong>{dados.get("data_exaltacao", "")}</strong></td>
            </tr>
            <tr>
                <td>Provas Realizadas:</td>
                <td colspan="3"><strong>Todas as Câmaras Aprovadas</strong></td>
            </tr>
        </table>
        '''
    
    template = self.env.get_template('gob_comunicado_padrao.html')
    html = template.render(dados=dados)
    
    temp_dir = self._get_temp_dir()
    pdf_filename = f"comunicado_{tipo.lower()}_{dados['candidato_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    pdf_path = os.path.join(temp_dir, pdf_filename)
    
    options = {
        'page-size': 'A4',
        'margin-top': '20mm',
        'margin-right': '20mm',
        'margin-bottom': '20mm',
        'margin-left': '20mm',
        'encoding': 'UTF-8',
        'no-outline': None,
    }
    
    if self.pdf_config:
        pdfkit.from_string(html, pdf_path, options=options, configuration=self.pdf_config)
    else:
        pdfkit.from_string(html, pdf_path, options=options)
    
    # Upload para Cloudinary
    folder_map = {
        'INICIAÇÃO': 'comunicados_gob/iniciacoes',
        'ELEVAÇÃO': 'comunicados_gob/elevacoes',
        'EXALTAÇÃO': 'comunicados_gob/exaltacoes'
    }
    
    resultado = cloudinary.uploader.upload(
        pdf_path,
        resource_type='raw',
        folder=folder_map.get(tipo, 'comunicados_gob'),
        public_id=f"{tipo.lower()}_{dados['candidato_id']}_{int(datetime.now().timestamp())}"
    )
    
    os.remove(pdf_path)
    
    return {
        'url': resultado['secure_url'],
        'public_id': resultado['public_id']
    }        