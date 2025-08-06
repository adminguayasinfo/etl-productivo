#!/usr/bin/env python3
"""
Script para cargar datos de fertilizantes desde Excel hacia la tabla staging.
Etapa 1 del pipeline de fertilizantes: Excel → stg_fertilizante
"""

import sys
import os
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import click
from datetime import datetime
from loguru import logger

from src.load.fertilizantes_stg_load import FertilizantesStgLoader
from config.connections.database import DatabaseConnection


# Configurar logger
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"fertilizantes_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logger.add(
    log_file,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO"
)


@click.command()
@click.option('--excel-path', 
              default='data/raw/BASE PROYECTOS DESARROLLO PRODUCTIVO.xlsx',
              type=click.Path(exists=True),
              help='Ruta al archivo Excel')
@click.option('--batch-size', 
              default=1000,
              type=int,
              help='Tamaño del lote para procesamiento')
@click.option('--truncate', 
              is_flag=True,
              default=True,
              help='Truncar tabla staging antes de cargar (siempre activo para fertilizantes)')
@click.option('--dry-run', 
              is_flag=True,
              help='Modo de prueba (no modifica datos)')
def load_staging(excel_path: str, batch_size: int, truncate: bool, dry_run: bool):
    """Cargar datos de fertilizantes desde Excel a tabla staging."""
    
    print("🌿 CARGANDO FERTILIZANTES A STAGING")
    print("=" * 40)
    
    logger.info("=== INICIANDO CARGA STAGING FERTILIZANTES ===")
    logger.info(f"Archivo Excel: {excel_path}")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Truncar tabla: {truncate}")
    logger.info(f"Modo prueba: {dry_run}")
    logger.info(f"Log file: {log_file}")
    
    try:
        # 1. Verificar conexión
        print("🔌 Verificando conexión a base de datos...")
        db = DatabaseConnection()
        if not db.test_connection():
            logger.error("❌ Error de conexión a la base de datos")
            print("❌ Error de conexión a la base de datos")
            sys.exit(1)
        
        logger.info("✅ Conexión exitosa")
        print("✅ Conexión exitosa")
        
        # 2. Verificar archivo Excel
        excel_file = Path(excel_path)
        if not excel_file.exists():
            logger.error(f"❌ Archivo Excel no encontrado: {excel_path}")
            print(f"❌ Archivo Excel no encontrado: {excel_path}")
            sys.exit(1)
        
        print(f"📄 Archivo Excel encontrado: {excel_path}")
        
        if dry_run:
            print("🔍 Modo de prueba activado - no se modificarán datos")
            logger.info("Modo de prueba - finalizando sin cambios")
            return
        
        # 3. Cargar staging
        print("💾 Iniciando carga a tabla staging...")
        print("⚠️ Nota: La tabla staging será truncada automáticamente")
        logger.info("Iniciando carga staging - tabla será truncada")
        
        start_time = datetime.now()
        loader = FertilizantesStgLoader()
        
        # Nota: FertilizantesStgLoader siempre trunca la tabla automáticamente
        result = loader.load_excel_to_staging(
            excel_path=excel_path,
            batch_size=batch_size
        )
        
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        
        # 4. Mostrar resultados
        if 'error' not in result:
            print(f"\n✅ CARGA COMPLETADA EXITOSAMENTE")
            print(f"   📊 Total procesados: {result['total_processed']:,} registros")
            print(f"   ⚡ Tiempo transcurrido: {elapsed_time:.2f} segundos")
            print(f"   🚀 Velocidad: {result['total_processed']/elapsed_time:.1f} registros/segundo")
            print(f"   ❌ Errores: {result['total_errors']:,}")
            print(f"   📊 Tasa de éxito: {result['success_rate']:.1f}%")
            print(f"   🔢 Lotes procesados: {result['batches_processed']}")
            
            logger.info(f"Carga exitosa: {result['total_processed']:,} registros en {elapsed_time:.2f}s")
            
            if result['total_errors'] > 0:
                logger.warning(f"Se registraron {result['total_errors']} errores")
        else:
            print(f"❌ ERROR EN CARGA: {result.get('error', 'Error desconocido')}")
            logger.error(f"Carga falló: {result.get('error', 'Error desconocido')}")
            sys.exit(1)
        
        # 5. Verificar datos cargados
        print("\n📊 Verificando datos en staging...")
        verify_staging_data(db)
        
        logger.info("✅ Proceso completado exitosamente")
        print("\n🎉 PROCESO COMPLETADO EXITOSAMENTE")
        
    except Exception as e:
        logger.error(f"❌ Error crítico: {str(e)}")
        logger.exception("Detalle del error:")
        print(f"❌ Error crítico: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("=== FIN DEL PROCESO ===")


def verify_staging_data(db: DatabaseConnection):
    """Verifica los datos cargados en staging."""
    queries = [
        ("Total registros", 'SELECT COUNT(*) FROM "etl-productivo".stg_fertilizante'),
        ("Procesados", 'SELECT COUNT(*) FROM "etl-productivo".stg_fertilizante WHERE processed = true'),
        ("Pendientes", 'SELECT COUNT(*) FROM "etl-productivo".stg_fertilizante WHERE processed = false'),
        ("Con errores", 'SELECT COUNT(*) FROM "etl-productivo".stg_fertilizante WHERE error_message IS NOT NULL')
    ]
    
    for name, query in queries:
        try:
            result = db.execute_query(query)
            count = result[0][0] if result else 0
            print(f"   {name}: {count:,}")
            logger.info(f"Staging - {name}: {count:,}")
        except Exception as e:
            print(f"   Error consultando {name}: {e}")
            logger.warning(f"Error consultando {name}: {e}")
    
    # Mostrar distribución por cultivo
    try:
        result = db.execute_query(
            'SELECT cultivo, COUNT(*) FROM "etl-productivo".stg_fertilizante '
            'WHERE cultivo IS NOT NULL '
            'GROUP BY cultivo ORDER BY COUNT(*) DESC LIMIT 5'
        )
        if result:
            print("   Distribución por cultivo (Top 5):")
            for cultivo, count in result:
                print(f"     - {cultivo}: {count:,}")
                logger.info(f"Cultivo - {cultivo}: {count:,}")
    except Exception as e:
        logger.warning(f"Error consultando distribución por cultivo: {e}")


if __name__ == "__main__":
    load_staging()