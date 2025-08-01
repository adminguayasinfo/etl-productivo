#!/usr/bin/env python3
"""
Script para corregir los tipos de datos de coordenadas en la tabla direccion.
"""

import sys
import os
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from config.connections.database import db_connection

# Configurar logger
logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

def main():
    """Función principal para corregir tipos de coordenadas."""
    logger.info("=== CORRIGIENDO TIPOS DE DATOS DE COORDENADAS ===")
    
    try:
        # Verificar conexión
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return False
        logger.info("✅ Conexión a base de datos exitosa")
        
        # Verificar estructura actual
        logger.info("\n--- Verificando estructura actual ---")
        check_current_structure()
        
        # Corregir tipos de datos
        logger.info("\n--- Corrigiendo tipos de datos ---")
        fix_coordinate_types()
        
        # Verificar estructura corregida
        logger.info("\n--- Verificando estructura corregida ---")
        check_current_structure()
        
        logger.info("\n✅ Corrección de tipos de datos completada exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error durante corrección: {e}")
        return False

def check_current_structure():
    """Verifica la estructura actual de la tabla direccion."""
    query = """
    SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
    FROM information_schema.columns 
    WHERE table_schema = 'etl-productivo' 
    AND table_name = 'direccion'
    AND column_name IN ('coordenada_x', 'coordenada_y')
    ORDER BY column_name;
    """
    
    try:
        result = db_connection.execute_query(query)
        if result:
            logger.info("  Estructura actual de coordenadas:")
            for row in result:
                col_name, data_type, max_length, precision, scale = row
                type_info = data_type
                if max_length:
                    type_info += f"({max_length})"
                elif precision:
                    type_info += f"({precision},{scale or 0})"
                logger.info(f"    - {col_name}: {type_info}")
        else:
            logger.warning("  No se encontraron columnas de coordenadas")
    except Exception as e:
        logger.error(f"  Error verificando estructura: {e}")

def fix_coordinate_types():
    """Corrige los tipos de datos de las coordenadas."""
    
    # Primero limpiar datos problemáticos
    logger.info("  Limpiando datos de coordenadas problemáticos...")
    cleanup_coordinate_data()
    
    # Lista de comandos SQL a ejecutar
    sql_commands = [
        "-- Cambiar coordenada_x a DECIMAL(15,2)",
        'ALTER TABLE "etl-productivo".direccion ALTER COLUMN coordenada_x TYPE DECIMAL(15,2) USING CASE WHEN coordenada_x ~ \'^[0-9]+\.?[0-9]*$\' THEN coordenada_x::DECIMAL(15,2) ELSE NULL END;',
        
        "-- Cambiar coordenada_y a DECIMAL(15,2)", 
        'ALTER TABLE "etl-productivo".direccion ALTER COLUMN coordenada_y TYPE DECIMAL(15,2) USING CASE WHEN coordenada_y ~ \'^[0-9]+\.?[0-9]*$\' THEN coordenada_y::DECIMAL(15,2) ELSE NULL END;',
        
        "-- Agregar comentarios",
        'COMMENT ON COLUMN "etl-productivo".direccion.coordenada_x IS \'Coordenada X (UTM o decimal)\';',
        'COMMENT ON COLUMN "etl-productivo".direccion.coordenada_y IS \'Coordenada Y (UTM o decimal)\';'
    ]
    
    for command in sql_commands:
        if command.startswith('--'):
            logger.info(f"  {command}")
            continue
            
        try:
            logger.info(f"  Ejecutando: {command[:50]}...")
            db_connection.execute_query(command)
            logger.info("    ✅ Completado")
        except Exception as e:
            if "does not exist" in str(e):
                logger.warning(f"    ⚠️  Columna no existe: {e}")
            else:
                logger.error(f"    ❌ Error: {e}")
                raise

def cleanup_coordinate_data():
    """Limpia los datos de coordenadas problemáticos."""
    
    cleanup_commands = [
        "-- Limpiar coordenadas None",
        'UPDATE "etl-productivo".direccion SET coordenada_x = NULL WHERE coordenada_x = \'None\' OR coordenada_x = \'\';',
        'UPDATE "etl-productivo".direccion SET coordenada_y = NULL WHERE coordenada_y = \'None\' OR coordenada_y = \'\';',
        
        "-- Limpiar coordenadas con espacios (tomar primer número)",
        'UPDATE "etl-productivo".direccion SET coordenada_x = split_part(coordenada_x, \' \', 1) WHERE coordenada_x LIKE \'% %\' AND split_part(coordenada_x, \' \', 1) ~ \'^[0-9]+\.?[0-9]*$\';',
        'UPDATE "etl-productivo".direccion SET coordenada_y = split_part(coordenada_y, \' \', 1) WHERE coordenada_y LIKE \'% %\' AND split_part(coordenada_y, \' \', 1) ~ \'^[0-9]+\.?[0-9]*$\';',
        
        "-- Limpiar coordenadas UTM con sufijos (604238.838E -> 604238.838)",
        'UPDATE "etl-productivo".direccion SET coordenada_x = regexp_replace(coordenada_x, \'[^0-9.]\', \'\', \'g\') WHERE coordenada_x ~ \'[0-9]+\.?[0-9]*[A-Z]\';',
        'UPDATE "etl-productivo".direccion SET coordenada_y = regexp_replace(coordenada_y, \'[^0-9.]\', \'\', \'g\') WHERE coordenada_y ~ \'[0-9]+\.?[0-9]*[A-Z]\';',
        
        "-- Limpiar coordenadas GPS decimales (-2.720260, -79.948107 -> NULL por ser formato diferente)",
        'UPDATE "etl-productivo".direccion SET coordenada_x = NULL WHERE coordenada_x LIKE \'%,%\';',
        'UPDATE "etl-productivo".direccion SET coordenada_y = NULL WHERE coordenada_y LIKE \'%,%\';'
    ]
    
    for command in cleanup_commands:
        if command.startswith('--'):
            logger.info(f"    {command}")
            continue
            
        try:
            result = db_connection.execute_query(command)
            logger.info(f"    ✅ Ejecutado")
        except Exception as e:
            logger.warning(f"    ⚠️  Error en limpieza: {e}")
            continue

def verify_data_integrity():
    """Verifica que los datos no se hayan corrompido."""
    logger.info("\n--- Verificando integridad de datos ---")
    
    queries = [
        ("Total direcciones", 'SELECT COUNT(*) FROM "etl-productivo".direccion'),
        ("Coordenadas no nulas", 'SELECT COUNT(*) FROM "etl-productivo".direccion WHERE coordenada_x IS NOT NULL AND coordenada_y IS NOT NULL'),
        ("Coordenadas válidas", 'SELECT COUNT(*) FROM "etl-productivo".direccion WHERE coordenada_x > 0 AND coordenada_y > 0'),
    ]
    
    for name, query in queries:
        try:
            result = db_connection.execute_query(query)
            count = result[0][0] if result else 0
            logger.info(f"  {name}: {count:,}")
        except Exception as e:
            logger.warning(f"  Error verificando {name}: {e}")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)