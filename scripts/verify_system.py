#!/usr/bin/env python3
"""
Script para verificar que el sistema unificado funcione correctamente.
Revisa la integridad de las tablas y conexiones de la arquitectura unified.
"""

import sys
import os
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.connections.database import db_connection
from loguru import logger

# Configurar logger simple para pantalla
logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

def main():
    """Función principal de verificación."""
    logger.info("=== VERIFICACIÓN DEL SISTEMA UNIFICADO ===")
    
    try:
        # Verificar conexión
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return False
        logger.info("✅ Conexión a base de datos exitosa")
        
        # Verificar tablas principales
        success = verify_tables()
        if not success:
            return False
        
        # Verificar integridad de datos
        verify_data_integrity()
        
        # Mostrar resumen final
        show_system_summary()
        
        logger.info("✅ Sistema unificado verificado correctamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error durante verificación: {e}")
        return False

def verify_tables():
    """Verifica que todas las tablas necesarias existan."""
    logger.info("\n--- Verificando estructura de tablas ---")
    
    required_tables = [
        'direccion',
        'asociacion', 
        'tipo_cultivo',
        'beneficiario',
        'beneficio',
        'beneficio_semillas',
        'beneficio_fertilizantes',
        'stg_semilla',
        'stg_fertilizante',
        'stg_planta',
        'stg_mecanizacion'
    ]
    
    existing_tables = []
    missing_tables = []
    
    for table in required_tables:
        query = f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'etl-productivo' AND table_name = '{table}'
        """
        try:
            result = db_connection.execute_query(query)
            if result and result[0][0] > 0:
                existing_tables.append(table)
                logger.info(f"  ✅ {table}")
            else:
                missing_tables.append(table)
                logger.warning(f"  ⚠️  {table} (faltante)")
        except Exception as e:
            logger.error(f"  ❌ Error verificando {table}: {e}")
            missing_tables.append(table)
    
    logger.info(f"\nTablas encontradas: {len(existing_tables)}/{len(required_tables)}")
    
    if missing_tables:
        logger.warning(f"Tablas faltantes: {', '.join(missing_tables)}")
        return len(existing_tables) >= len(required_tables) * 0.8  # 80% mínimo
    
    return True

def verify_data_integrity():
    """Verifica la integridad de los datos."""
    logger.info("\n--- Verificando integridad de datos ---")
    
    integrity_queries = [
        ("Beneficios sin beneficiario", 
         'SELECT COUNT(*) FROM "etl-productivo".beneficio WHERE beneficiario_id IS NULL'),
        ("Beneficios sin tipo cultivo",
         'SELECT COUNT(*) FROM "etl-productivo".beneficio WHERE tipo_cultivo_id IS NULL'),
        ("Beneficiarios sin dirección",
         'SELECT COUNT(*) FROM "etl-productivo".beneficiario WHERE direccion_id IS NULL'),
        ("Direcciones sin coordenadas",
         'SELECT COUNT(*) FROM "etl-productivo".direccion WHERE coord_x IS NULL OR coord_y IS NULL'),
    ]
    
    for name, query in integrity_queries:
        try:
            result = db_connection.execute_query(query)
            count = result[0][0] if result else 0
            if count == 0:
                logger.info(f"  ✅ {name}: {count}")
            else:
                logger.warning(f"  ⚠️  {name}: {count}")
        except Exception as e:
            logger.error(f"  ❌ Error verificando {name}: {e}")

def show_system_summary():
    """Muestra un resumen del estado del sistema."""
    logger.info("\n--- RESUMEN DEL SISTEMA ---")
    
    summary_queries = [
        ("Total direcciones", 'SELECT COUNT(*) FROM "etl-productivo".direccion'),
        ("Total asociaciones", 'SELECT COUNT(*) FROM "etl-productivo".asociacion'),
        ("Total tipos cultivo", 'SELECT COUNT(*) FROM "etl-productivo".tipo_cultivo'),
        ("Total beneficiarios", 'SELECT COUNT(*) FROM "etl-productivo".beneficiario'),
        ("Total beneficios", 'SELECT COUNT(*) FROM "etl-productivo".beneficio'),
        ("Beneficios semillas", 'SELECT COUNT(*) FROM "etl-productivo".beneficio_semillas'),
        ("Beneficios fertilizantes", 'SELECT COUNT(*) FROM "etl-productivo".beneficio_fertilizantes'),
    ]
    
    for name, query in summary_queries:
        try:
            result = db_connection.execute_query(query)
            count = result[0][0] if result else 0
            logger.info(f"  {name}: {count:,}")
        except Exception as e:
            logger.warning(f"  Error consultando {name}: {e}")
    
    # Mostrar hectáreas por tipo de beneficio
    try:
        result = db_connection.execute_query('''
            SELECT 
                tipo_beneficio,
                COUNT(*) as beneficios,
                SUM(hectareas_beneficiadas) as total_hectareas
            FROM "etl-productivo".beneficio 
            WHERE hectareas_beneficiadas IS NOT NULL
            GROUP BY tipo_beneficio 
            ORDER BY total_hectareas DESC
        ''')
        
        if result:
            logger.info("\n  Hectáreas por tipo de beneficio:")
            total_hectareas = 0
            for tipo, beneficios, hectareas in result:
                hectareas_float = float(hectareas) if hectareas else 0
                total_hectareas += hectareas_float
                logger.info(f"    - {tipo}: {hectareas_float:.2f} ha ({beneficios:,} beneficios)")
            logger.info(f"  TOTAL: {total_hectareas:.2f} hectáreas")
    except Exception as e:
        logger.warning(f"  Error consultando hectáreas: {e}")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)