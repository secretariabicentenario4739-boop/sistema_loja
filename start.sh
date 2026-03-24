#!/bin/bash
# start.sh

# Instalar dependências
pip install -r requirements.txt

# Criar diretórios necessários
mkdir -p uploads/documentos
mkdir -p backups

# Executar migrations (criar tabelas)
python -c "
from app import init_db
init_db()
"

# Iniciar aplicação
gunicorn app:app