#!/usr/bin/env python3
"""
Script para crear las tablas de beneficio_plantas y beneficio_mecanizacion.
"""

import sys
import os
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from config.connections.database import db_connection
from loguru import logger

# Configurar logger simple para pantalla
logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

def main():
    """Función principal."""
    logger.info("=== CREANDO TABLAS BENEFICIO_PLANTAS Y BENEFICIO_MECANIZACION ===")
    
    try:
        # Verificar conexión
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return False
        logger.info("✅ Conexión a base de datos exitosa")
        
        # Crear tablas
        create_beneficio_plantas_table()
        create_beneficio_mecanizacion_table()
        
        logger.info("✅ Tablas creadas exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error durante creación: {e}")
        return False

def create_beneficio_plantas_table():
    """Crea la tabla beneficio_plantas."""
    logger.info("Creando tabla beneficio_plantas...")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS "etl-productivo".beneficio_plantas (
        id INTEGER PRIMARY KEY REFERENCES "etl-productivo".beneficio(id),
        actas VARCHAR(50),
        contratista VARCHAR(200),
        cedula_contratista VARCHAR(20),
        entrega INTEGER,
        hectareas DECIMAL(10, 2),
        precio_unitario DECIMAL(10, 3),
        rubro VARCHAR(100),
        observacion TEXT
    );
    
    COMMENT ON TABLE "etl-productivo".beneficio_plantas IS 'Tabla de beneficios específicos de plantas (hereda de beneficio)';
    COMMENT ON COLUMN "etl-productivo".beneficio_plantas.actas IS 'Código único del acta';
    COMMENT ON COLUMN "etl-productivo".beneficio_plantas.entrega IS 'Cantidad de plantas entregadas';
    COMMENT ON COLUMN "etl-productivo".beneficio_plantas.hectareas IS 'Hectáreas del beneficiario';
    '''
    
    try:
        db_connection.execute_query(sql)
        logger.info("✅ Tabla beneficio_plantas creada")
    except Exception as e:
        if "already exists" in str(e):
            logger.info("ℹ️  Tabla beneficio_plantas ya existe")
        else:
            raise

def create_beneficio_mecanizacion_table():
    """Crea la tabla beneficio_mecanizacion."""
    logger.info("Creando tabla beneficio_mecanizacion...")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS "etl-productivo".beneficio_mecanizacion (
        id INTEGER PRIMARY KEY REFERENCES "etl-productivo".beneficio(id),
        estado VARCHAR(50),
        comentario TEXT,
        cu_ha DECIMAL(10, 2),
        inversion DECIMAL(12, 2),
        agrupacion VARCHAR(300),
        coord_x_str VARCHAR(50),
        coord_y_str VARCHAR(50)
    );
    
    COMMENT ON TABLE "etl-productivo".beneficio_mecanizacion IS 'Tabla de beneficios específicos de mecanización (hereda de beneficio)';
    COMMENT ON COLUMN "etl-productivo".beneficio_mecanizacion.cu_ha IS 'Costo unitario por hectárea';
    COMMENT ON COLUMN "etl-productivo".beneficio_mecanizacion.inversion IS 'Monto de inversión';
    COMMENT ON COLUMN "etl-productivo".beneficio_mecanizacion.agrupacion IS 'Nombre de la agrupación';
    '''
    
    try:
        db_connection.execute_query(sql)
        logger.info("✅ Tabla beneficio_mecanizacion creada")
    except Exception as e:
        if "already exists" in str(e):
            logger.info("ℹ️  Tabla beneficio_mecanizacion ya existe")
        else:
            raise

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)