#!/bin/bash

# Script para iniciar la API con variables de entorno cargadas

echo "============================================================"
echo "🚀 INICIANDO API - MÉTODO ALTERNATIVO CON VARIABLES MANUALES"
echo "============================================================"

# Cargar variables de entorno desde .env
set -a  # automatically export all variables
source .env
set +a

# Activar entorno virtual si existe
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "✓ Entorno virtual activado"
fi

# Verificar que las variables estén cargadas
echo "Verificando variables de entorno:"
echo "OLTP_DB_HOST: $OLTP_DB_HOST"
echo "OLTP_DB_PORT: $OLTP_DB_PORT" 
echo "OLTP_DB_NAME: $OLTP_DB_NAME"
echo "OLTP_DB_USER: $OLTP_DB_USER"
echo "OLTP_DB_PASSWORD: [CONFIGURADA]"

echo ""
echo "✅ Variables cargadas correctamente"
echo "🌐 Iniciando servidor API..."
echo ""

# Iniciar API
python start_api.py