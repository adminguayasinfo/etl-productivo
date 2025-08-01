#!/usr/bin/env python3
"""Script de prueba para verificar el ETL de semillas con Excel."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.extract.semillas_excel_extractor import SemillasExcelExtractor
from loguru import logger

def test_excel_extraction():
    """Prueba la extracción de datos del Excel."""
    excel_path = "data/raw/BASE PROYECTOS DESARROLLO PRODUCTIVO.xlsx"
    
    logger.info("=== PRUEBA DE EXTRACCIÓN EXCEL ===")
    
    try:
        extractor = SemillasExcelExtractor(batch_size=5)
        
        # Extraer primera muestra
        df = extractor.extract(excel_path)
        logger.info(f"Total de registros en Excel: {len(df)}")
        
        # Mostrar primeras filas
        logger.info("Primeras 3 filas:")
        print(df.head(3).to_string())
        
        # Probar preparación de una fila
        logger.info("\n=== PRUEBA DE PREPARACIÓN DE DATOS ===")
        first_row = df.iloc[0]
        prepared_data = extractor.prepare_row(first_row)
        
        logger.info("Datos preparados para la primera fila:")
        for key, value in prepared_data.items():
            if value is not None:
                logger.info(f"  {key}: {value}")
        
        # Probar extracción por lotes
        logger.info("\n=== PRUEBA DE EXTRACCIÓN POR LOTES ===")
        batch_count = 0
        for batch in extractor.extract_batches(excel_path):
            batch_count += 1
            logger.info(f"Lote {batch_count}: {len(batch)} registros")
            if batch_count >= 3:  # Solo mostrar los primeros 3 lotes
                break
        
        logger.info("✓ Prueba de extracción Excel completada exitosamente")
        
    except Exception as e:
        logger.error(f"Error en prueba: {str(e)}")
        logger.exception("Detalle del error:")

if __name__ == "__main__":
    test_excel_extraction()