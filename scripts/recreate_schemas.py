#!/usr/bin/env python
"""Script para eliminar y recrear todos los schemas de la base de datos."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy import text
from config.connections.database import DatabaseConnection
from loguru import logger

def drop_and_recreate_schemas():
    """Elimina y recrea el schema etl-productivo."""
    db = DatabaseConnection()
    db.init_engine()
    
    logger.info("=== ELIMINANDO Y RECREANDO SCHEMA ETL-PRODUCTIVO ===")
    
    with db.engine.connect() as conn:
        try:
            # Eliminar el schema en cascada
            logger.info("Eliminando schema etl-productivo...")
            
            conn.execute(text('DROP SCHEMA IF EXISTS "etl-productivo" CASCADE'))
            conn.commit()
            logger.info("✓ Schema etl-productivo eliminado")
            
            # Recrear schema
            logger.info("\nRecreando schema...")
            
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS "etl-productivo"'))
            conn.commit()
            logger.info("✓ Schema etl-productivo creado")
            
            logger.info("\n✓ Schema eliminado y recreado exitosamente")
            
        except Exception as e:
            logger.error(f"Error al recrear schema: {str(e)}")
            raise


if __name__ == "__main__":
    drop_and_recreate_schemas()