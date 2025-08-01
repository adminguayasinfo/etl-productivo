"""Debug script para verificar el flujo de cultivos en el ETL."""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from loguru import logger
from config.connections.database import DatabaseConnection
from src.extract.semillas_staging_reader import SemillasStagingReader
from src.transform.semillas_transformer import SemillasTransformer

def debug_cultivo_flow():
    """Verifica el flujo de cultivos desde staging hasta transformación."""
    logger.info("=== DEBUG CULTIVO FLOW ===")
    
    db = DatabaseConnection()
    db.init_engine()
    
    with db.get_session() as session:
        # 1. Leer un lote pequeño de staging
        reader = SemillasStagingReader(batch_size=10)
        df = reader._read_batch(session, offset=0)
        
        logger.info(f"\n1. STAGING DATA ({len(df)} records):")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        # Verificar columna cultivo
        if 'cultivo' in df.columns:
            logger.info(f"Cultivo values in staging: {df['cultivo'].unique()}")
            logger.info(f"Sample records with cultivo:")
            for idx, row in df[df['cultivo'].notna()].head(3).iterrows():
                logger.info(f"  - {row['nombres_apellidos']}: cultivo='{row['cultivo']}'")
        
        # 2. Transformar
        transformer = SemillasTransformer()
        entities, invalid = transformer.transform(df)
        
        logger.info(f"\n2. AFTER TRANSFORMATION:")
        
        # Verificar en cada etapa
        if hasattr(transformer, 'cleaner') and hasattr(transformer.cleaner, 'cleaned_df'):
            cleaned_df = transformer.cleaner.cleaned_df
            logger.info(f"\n2a. AFTER CLEANER:")
            logger.info(f"Columns: {cleaned_df.columns.tolist()}")
            if 'tipo_cultivo' in cleaned_df.columns:
                logger.info(f"tipo_cultivo values: {cleaned_df['tipo_cultivo'].unique()}")
        
        # Verificar beneficios normalizados
        if 'beneficios' in entities and not entities['beneficios'].empty:
            logger.info(f"\n2b. NORMALIZED BENEFICIOS:")
            logger.info(f"Columns: {entities['beneficios'].columns.tolist()}")
            logger.info(f"Total beneficios: {len(entities['beneficios'])}")
            
            # Verificar campo tipo_cultivo
            if 'tipo_cultivo' in entities['beneficios'].columns:
                logger.info(f"tipo_cultivo values: {entities['beneficios']['tipo_cultivo'].unique()}")
                logger.info(f"Beneficios con tipo_cultivo: {entities['beneficios']['tipo_cultivo'].notna().sum()}")
                
                # Mostrar algunos ejemplos
                sample = entities['beneficios'][entities['beneficios']['tipo_cultivo'].notna()].head(3)
                for idx, row in sample.iterrows():
                    logger.info(f"  - Persona: {row.get('persona_nombres', 'N/A')}, tipo_cultivo: {row['tipo_cultivo']}")
            else:
                logger.warning("No se encontró columna tipo_cultivo en beneficios normalizados!")
                
            # Verificar también si hay columna 'cultivo' por error
            if 'cultivo' in entities['beneficios'].columns:
                logger.warning(f"ENCONTRADA columna 'cultivo' (debería ser tipo_cultivo): {entities['beneficios']['cultivo'].unique()}")

if __name__ == "__main__":
    debug_cultivo_flow()