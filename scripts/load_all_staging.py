#!/usr/bin/env python3
"""
Script para cargar datos de staging para los 4 tipos de beneficios desde Excel.
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from config.connections.database import db_connection

# Importar extractores y loaders
from src.extract.semillas_excel_extractor import SemillasExcelExtractor
from src.extract.fertilizantes_excel_extractor import FertilizantesExcelExtractor
from src.extract.plantas_excel_extractor import PlantasExcelExtractor
from src.extract.mecanizacion_excel_extractor import MecanizacionExcelExtractor

from src.load.semillas_stg_load import SemillasStgLoader
from src.load.fertilizantes_stg_load import FertilizantesStgLoader
from src.load.plantas_stg_load import PlantasStagingLoader
from src.load.mecanizacion_stg_load import MecanizacionStgLoader

# Configurar logger
logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

def main():
    """Función principal."""
    logger.info("=== CARGANDO DATOS DE STAGING PARA LOS 4 TIPOS ===")
    
    excel_file = "data/raw/BASE PROYECTOS DESARROLLO PRODUCTIVO.xlsx"
    
    if not Path(excel_file).exists():
        logger.error(f"❌ Archivo no encontrado: {excel_file}")
        return False
    
    try:
        # Verificar conexión
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return False
        logger.info("✅ Conexión a base de datos exitosa")
        
        # Cargar datos de staging
        success = True
        
        # 1. SEMILLAS
        logger.info("\n--- CARGANDO SEMILLAS ---")
        try:
            extractor = SemillasExcelExtractor()
            loader = SemillasStgLoader()
            
            # Extraer datos
            df = extractor.extract(excel_file)
            logger.info(f"Extraidos {len(df):,} registros de semillas")
            
            # Cargar a staging
            result = loader.load_from_dataframe(df)
            logger.info(f"✅ Semillas: {result.get('total_loaded', 0):,} registros cargados")
        except Exception as e:
            logger.error(f"❌ Error en semillas: {e}")
            success = False
        
        # 2. FERTILIZANTES
        logger.info("\n--- CARGANDO FERTILIZANTES ---")
        try:
            extractor = FertilizantesExcelExtractor()
            loader = FertilizantesStgLoader()
            
            # Extraer datos
            df = extractor.extract(excel_file)
            logger.info(f"Extraidos {len(df):,} registros de fertilizantes")
            
            # Cargar a staging
            result = loader.load_from_dataframe(df)
            logger.info(f"✅ Fertilizantes: {result.get('total_loaded', 0):,} registros cargados")
        except Exception as e:
            logger.error(f"❌ Error en fertilizantes: {e}")
            success = False
        
        # 3. PLANTAS
        logger.info("\n--- CARGANDO PLANTAS ---")
        try:
            extractor = PlantasExcelExtractor()
            loader = PlantasStagingLoader()
            
            # Extraer datos
            df = extractor.extract(excel_file)
            logger.info(f"Extraidos {len(df):,} registros de plantas")
            
            # Cargar a staging
            result = loader.load_from_dataframe(df)
            logger.info(f"✅ Plantas: {result.get('total_loaded', 0):,} registros cargados")
        except Exception as e:
            logger.error(f"❌ Error en plantas: {e}")
            success = False
        
        # 4. MECANIZACIÓN
        logger.info("\n--- CARGANDO MECANIZACIÓN ---")
        try:
            extractor = MecanizacionExcelExtractor()
            loader = MecanizacionStgLoader()
            
            # Extraer datos
            df = extractor.extract(excel_file)
            logger.info(f"Extraidos {len(df):,} registros de mecanización")
            
            # Cargar a staging
            result = loader.load_from_dataframe(df)
            logger.info(f"✅ Mecanización: {result.get('total_loaded', 0):,} registros cargados")
        except Exception as e:
            logger.error(f"❌ Error en mecanización: {e}")
            success = False
        
        # Verificar resultados finales
        logger.info("\n--- VERIFICACIÓN FINAL DE STAGING ---")
        verify_staging_data()
        
        if success:
            logger.info("\n✅ Carga de staging completada exitosamente")
        else:
            logger.warning("\n⚠️  Carga de staging completada con algunos errores")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Error crítico: {e}")
        return False

def verify_staging_data():
    """Verifica los datos cargados en staging."""
    staging_tables = [
        ("Semillas", 'stg_semilla'),
        ("Fertilizantes", 'stg_fertilizante'),
        ("Plantas", 'stg_plantas'),
        ("Mecanización", 'stg_mecanizacion')
    ]
    
    total_records = 0
    
    for name, table in staging_tables:
        try:
            query = f'SELECT COUNT(*) FROM "etl-productivo".{table}'
            result = db_connection.execute_query(query)
            count = result[0][0] if result else 0
            total_records += count
            logger.info(f"  {name}: {count:,} registros")
        except Exception as e:
            logger.warning(f"  Error consultando {name}: {e}")
    
    logger.info(f"  TOTAL STAGING: {total_records:,} registros")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)