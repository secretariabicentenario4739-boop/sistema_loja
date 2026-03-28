# Usar imagem oficial do Python 3.11
FROM python:3.11-slim

# Definir diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema necessárias para psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar arquivo de dependências primeiro (para aproveitar cache do Docker)
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo o código do projeto
COPY . .

# Criar diretórios necessários
RUN mkdir -p uploads/documentos backups

# Expor a porta que o Render usa
EXPOSE 10000

# Definir variáveis de ambiente
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1

# Comando para iniciar a aplicação
CMD ["gunicorn", "--bind", "0.0.0.0:10000", "app:app"]

# Criar diretório para uploads
RUN mkdir -p /app/uploads/obreiros
RUN chmod 755 /app/uploads/obreiros