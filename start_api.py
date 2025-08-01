#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para iniciar el servidor API REST del análisis de costos de arroz.
"""

import os
import sys
import uvicorn
import logging
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def verificar_dependencias():
    """Verifica que las dependencias estén instaladas."""
    try:
        import fastapi
        import pydantic
        logger.info("✓ Dependencias verificadas correctamente")
        return True
    except ImportError as e:
        logger.error(f"✗ Dependencia faltante: {e}")
        logger.error("Ejecuta: pip install -r requirements.txt")
        return False

def verificar_estructura():
    """Verifica que la estructura del proyecto esté correcta."""
    archivos_requeridos = [
        "api/__init__.py",
        "api/main.py",
        "api/models.py",
        "api/services.py",
        "config/connections/database.py",
        "src/matriz_costos/costos_arroz.py"
    ]
    
    for archivo in archivos_requeridos:
        if not Path(archivo).exists():
            logger.error(f"✗ Archivo faltante: {archivo}")
            return False
    
    logger.info("✓ Estructura del proyecto verificada")
    return True

def verificar_variables_entorno():
    """Verifica que las variables de entorno estén configuradas."""
    variables_requeridas = [
        "OLTP_DB_HOST",
        "OLTP_DB_PORT", 
        "OLTP_DB_NAME",
        "OLTP_DB_USER",
        "OLTP_DB_PASSWORD"
    ]
    
    faltantes = []
    for var in variables_requeridas:
        if not os.getenv(var):
            faltantes.append(var)
    
    if faltantes:
        logger.error(f"✗ Variables de entorno faltantes: {', '.join(faltantes)}")
        logger.error("Configura las variables de entorno antes de iniciar el servidor")
        return False
    
    logger.info("✓ Variables de entorno configuradas")
    return True

def main():
    """Función principal para iniciar el servidor."""
    
    print("=" * 60)
    print("🚀 INICIANDO API REST - ANÁLISIS DE COSTOS DE ARROZ")
    print("=" * 60)
    print()
    
    # Verificaciones previas
    logger.info("Realizando verificaciones previas...")
    
    if not verificar_dependencias():
        sys.exit(1)
    
    if not verificar_estructura():
        sys.exit(1)
    
    if not verificar_variables_entorno():
        sys.exit(1)
    
    print()
    logger.info("✅ Todas las verificaciones pasaron correctamente")
    print()
    
    # Configuración del servidor
    host = "0.0.0.0"
    port = 8000
    
    print(f"🌐 Servidor iniciando en: http://{host}:{port}")
    print(f"📚 Documentación disponible en: http://{host}:{port}/docs")
    print(f"🔍 Endpoint principal: http://{host}:{port}/prod/analisis-costos-arroz")
    print(f"💚 Health check: http://{host}:{port}/health")
    print()
    print("Para detener el servidor presiona Ctrl+C")
    print("=" * 60)
    
    try:
        # Iniciar servidor
        uvicorn.run(
            "api.main:app",
            host=host,
            port=port,
            reload=True,
            log_level="info",
            access_log=True
        )
    except KeyboardInterrupt:
        logger.info("Servidor detenido por el usuario")
    except Exception as e:
        logger.error(f"Error al iniciar el servidor: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()