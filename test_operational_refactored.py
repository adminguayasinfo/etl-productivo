#!/usr/bin/env python3
"""Script de prueba para el pipeline operational refactorizado."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.connections.database import db_connection
from src.pipelines.operational_refactored.semillas_operational_refactored_pipeline import SemillasOperationalRefactoredPipeline
from loguru import logger


def test_operational_refactored():
    """Prueba el pipeline operational refactorizado."""
    
    logger.info("=== PRUEBA PIPELINE OPERATIONAL REFACTORIZADO ===")
    
    try:
        # Verificar conexión
        if not db_connection.test_connection():
            logger.error("Error de conexión a la base de datos")
            return
        
        # Verificar datos en staging
        result = db_connection.execute_query('SELECT COUNT(*) FROM "etl-productivo".stg_semilla')
        total_staging = result[0][0]
        logger.info(f"Registros en staging: {total_staging}")
        
        result = db_connection.execute_query('SELECT COUNT(*) FROM "etl-productivo".stg_semilla WHERE processed = false')
        pending_staging = result[0][0]
        logger.info(f"Registros pendientes: {pending_staging}")
        
        if pending_staging == 0:
            logger.warning("No hay registros pendientes para procesar")
            return
        
        # Ejecutar pipeline
        pipeline = SemillasOperationalRefactoredPipeline(batch_size=100)  # Lotes pequeños para prueba
        result = pipeline.execute()
        
        if result['status'] == 'success':
            logger.info("✓ Pipeline ejecutado exitosamente")
            stats = result['statistics']
            logger.info(f"Estadísticas:")
            logger.info(f"  - Procesados: {stats['processed']}")
            logger.info(f"  - Direcciones creadas: {stats['created_direcciones']}")
            logger.info(f"  - Asociaciones creadas: {stats['created_asociaciones']}")
            logger.info(f"  - Tipos cultivo creados: {stats['created_tipos_cultivo']}")
            logger.info(f"  - Beneficiarios creados: {stats['created_beneficiarios']}")
            logger.info(f"  - Beneficios creados: {stats['created_beneficios']}")
            logger.info(f"  - Errores: {stats['errors']}")
            logger.info(f"  - Tiempo: {result['elapsed_time']:.2f}s")
            
            # Verificar datos creados
            logger.info("\n--- Verificando datos creados ---")
            tables_to_check = [
                ('direccion', 'Direcciones'),
                ('asociacion', 'Asociaciones'),
                ('tipo_cultivo', 'Tipos de cultivo'),
                ('beneficiario', 'Beneficiarios'),
                ('beneficio', 'Beneficios'),
                ('beneficio_semillas', 'Beneficios semillas')
            ]
            
            for table, name in tables_to_check:
                result = db_connection.execute_query(f'SELECT COUNT(*) FROM "etl-productivo".{table}')
                count = result[0][0]
                logger.info(f"{name}: {count} registros")
                
        else:
            logger.error(f"Error en pipeline: {result.get('error')}")
        
    except Exception as e:
        logger.error(f"Error en prueba: {str(e)}")
        logger.exception("Detalle del error:")


if __name__ == "__main__":
    test_operational_refactored()