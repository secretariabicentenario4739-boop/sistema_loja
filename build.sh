#!/bin/bash
# build.sh

echo "Instalando dependências do sistema..."
apt-get update
apt-get install -y gcc libpq-dev python3-dev

echo "Instalando dependências Python..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Criando diretórios..."
mkdir -p uploads/documentos backups

echo "Build concluído!"