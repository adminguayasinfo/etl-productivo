#!/usr/bin/env python3
"""
Script para ejecutar el pipeline de semillas.
Carga datos desde stg_semilla a la estructura operational usando BeneficioSemillas.
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import click
from loguru import logger

from config.connections.database import db_connection
from src.pipelines.operational_refactored.semillas_operational_pipeline import SemillasOperationalRefactorizedPipeline


# Configurar logger
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"semillas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configurar loguru
logger.add(
    log_file,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO"
)


@click.command()
@click.option('--batch-size', 
              default=1000,
              type=int,
              help='Tamaño del lote para procesamiento')
@click.option('--dry-run', 
              is_flag=True,
              help='Ejecutar en modo de prueba (no modifica datos)')
def run_pipeline(batch_size: int, dry_run: bool):
    """Ejecutar pipeline de semillas."""
    
    logger.info("=== INICIANDO PIPELINE DE SEMILLAS ===")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Modo de prueba: {dry_run}")
    logger.info(f"Log file: {log_file}")
    
    try:
        # Verificar conexión a BD
        logger.info("Verificando conexión a base de datos...")
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            sys.exit(1)
        logger.info("✅ Conexión exitosa")
        
        # Verificar datos en staging
        verify_staging_data()
        
        if dry_run:
            logger.info("🔍 Modo de prueba activado - no se modificarán datos")
            return
        
        # Ejecutar pipeline
        logger.info("\n🚀 Iniciando procesamiento...")
        pipeline = SemillasOperationalRefactorizedPipeline(batch_size=batch_size)
        result = pipeline.run()
        
        # Mostrar resultados
        print_results(result)
        
        # Verificar datos después del proceso
        verify_operational_data()
        
        logger.info("\n✅ Pipeline completado exitosamente")
        
    except Exception as e:
        logger.error(f"❌ Error crítico en pipeline: {e}")
        logger.exception("Detalle del error:")
        sys.exit(1)
    finally:
        logger.info("=== FIN DEL PROCESO ===\n")


def verify_staging_data():
    """Verifica los datos en staging."""
    logger.info("\n--- Verificación de datos en Staging ---")
    
    queries = [
        ("Total semillas", 'SELECT COUNT(*) FROM "etl-productivo".stg_semilla'),
        ("Procesados", 'SELECT COUNT(*) FROM "etl-productivo".stg_semilla WHERE processed = true'),
        ("Pendientes", 'SELECT COUNT(*) FROM "etl-productivo".stg_semilla WHERE processed = false'),
        ("Con errores", 'SELECT COUNT(*) FROM "etl-productivo".stg_semilla WHERE error_message IS NOT NULL')
    ]
    
    for name, query in queries:
        try:
            result = db_connection.execute_query(query)
            count = result[0][0] if result else 0
            logger.info(f"  {name}: {count:,}")
        except Exception as e:
            logger.warning(f"  Error consultando {name}: {e}")
    
    # Mostrar distribución por cultivo
    try:
        result = db_connection.execute_query(
            'SELECT cultivo, COUNT(*) FROM "etl-productivo".stg_semilla '
            'WHERE processed = false AND cultivo IS NOT NULL '
            'GROUP BY cultivo ORDER BY COUNT(*) DESC LIMIT 10'
        )
        if result:
            logger.info("  Distribución por cultivo (pendientes):")
            for cultivo, count in result:
                logger.info(f"    - {cultivo}: {count:,}")
    except Exception as e:
        logger.warning(f"  Error consultando distribución por cultivo: {e}")


def verify_operational_data():
    """Verifica los datos cargados en operational."""
    logger.info("\n--- Verificación de datos en Operational ---")
    
    queries = [
        ("Direcciones", 'SELECT COUNT(*) FROM "etl-productivo".direccion'),
        ("Asociaciones", 'SELECT COUNT(*) FROM "etl-productivo".asociacion'),
        ("Tipos de cultivo", 'SELECT COUNT(*) FROM "etl-productivo".tipo_cultivo'),
        ("Beneficiarios", 'SELECT COUNT(*) FROM "etl-productivo".beneficiario'),
        ("Beneficios (general)", 'SELECT COUNT(*) FROM "etl-productivo".beneficio'),
        ("Beneficios semillas", 'SELECT COUNT(*) FROM "etl-productivo".beneficio_semillas'),
    ]
    
    for name, query in queries:
        try:
            result = db_connection.execute_query(query)
            count = result[0][0] if result else 0
            logger.info(f"  {name}: {count:,}")
        except Exception as e:
            logger.warning(f"  Error consultando {name}: {e}")
    
    # Mostrar distribución por tipo de cultivo
    try:
        result = db_connection.execute_query(
            'SELECT tc.nombre, COUNT(*) FROM "etl-productivo".beneficio b '
            'JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id '
            'WHERE b.tipo_beneficio = \'SEMILLAS\' '
            'GROUP BY tc.nombre ORDER BY COUNT(*) DESC'
        )
        if result:
            logger.info("  Distribución por cultivo (beneficios semillas):")
            for cultivo, count in result:
                logger.info(f"    - {cultivo}: {count:,}")
    except Exception as e:
        logger.warning(f"  Error consultando distribución por cultivo: {e}")
    
    # Mostrar hectáreas por cultivo
    try:
        result = db_connection.execute_query(
            'SELECT tc.nombre, SUM(b.hectareas_beneficiadas) as total_ha, COUNT(*) as beneficios '
            'FROM "etl-productivo".beneficio b '
            'JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id '
            'WHERE b.tipo_beneficio = \'SEMILLAS\' AND b.hectareas_beneficiadas IS NOT NULL '
            'GROUP BY tc.nombre ORDER BY total_ha DESC'
        )
        if result:
            logger.info("  Hectáreas por cultivo (beneficios semillas):")
            for cultivo, hectareas, beneficios in result:
                logger.info(f"    - {cultivo}: {float(hectareas):.2f} ha ({beneficios:,} beneficios)")
    except Exception as e:
        logger.warning(f"  Error consultando hectáreas por cultivo: {e}")


def print_results(result: dict):
    """Imprime los resultados del pipeline."""
    logger.info("\n" + "="*60)
    logger.info("RESULTADOS DEL PIPELINE")
    logger.info("="*60)
    
    logger.info(f"Estado: {result['status']}")
    logger.info(f"Duración: {result['duration_seconds']:.2f} segundos")
    logger.info(f"Total procesados: {result['total_processed']:,}")
    logger.info(f"Exitosos: {result['total_success']:,}")
    logger.info(f"Errores: {result['total_errors']:,}")
    logger.info(f"Tasa de éxito: {result['success_rate']:.1f}%")
    logger.info(f"Lotes procesados: {result['batches_processed']}")
    
    if result['total_processed'] > 0:
        rate = result['total_processed'] / result['duration_seconds']
        logger.info(f"Velocidad: {rate:.1f} registros/segundo")
    
    logger.info("\nEntidades creadas:")
    for entity, count in result['entities_created'].items():
        logger.info(f"  - {entity}: {count:,}")
    
    logger.info("="*60)


if __name__ == "__main__":
    run_pipeline()