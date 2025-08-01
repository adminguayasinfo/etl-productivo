"""Debug script to check where personas are being lost in the ETL process."""
import pandas as pd
from sqlalchemy import text
from config.connections.database import db_connection
from src.extract.semillas_staging_reader import SemillasStagingReader
from src.transform.semillas_transformer import SemillasTransformer
from loguru import logger

def analyze_persona_loss():
    """Analyze where personas are being lost in the ETL process."""
    
    logger.info("=== ANALYZING PERSONA LOSS IN ETL PROCESS ===")
    
    with db_connection.get_session() as session:
        # 1. Check total unique personas in staging (by nombres_apellidos)
        result = session.execute(text("""
            SELECT COUNT(DISTINCT nombres_apellidos) as unique_nombres
            FROM staging.semillas_stg
            WHERE nombres_apellidos IS NOT NULL 
            AND nombres_apellidos != 'None'
            AND nombres_apellidos != ''
        """))
        unique_nombres_staging = result.scalar()
        logger.info(f"1. Unique personas in staging (by nombres_apellidos): {unique_nombres_staging}")
        
        # 2. Check unique personas by cedula
        result = session.execute(text("""
            SELECT COUNT(DISTINCT cedula) as unique_cedulas
            FROM staging.semillas_stg
            WHERE cedula IS NOT NULL 
            AND cedula != 'None'
            AND cedula != ''
        """))
        unique_cedulas_staging = result.scalar()
        logger.info(f"2. Unique personas in staging (by cedula): {unique_cedulas_staging}")
        
        # 3. Check personas with both cedula and nombres
        result = session.execute(text("""
            SELECT COUNT(DISTINCT COALESCE(cedula, nombres_apellidos)) as unique_personas
            FROM staging.semillas_stg
            WHERE (nombres_apellidos IS NOT NULL AND nombres_apellidos != 'None' AND nombres_apellidos != '')
               OR (cedula IS NOT NULL AND cedula != 'None' AND cedula != '')
        """))
        unique_personas_staging = result.scalar()
        logger.info(f"3. Total unique personas in staging (cedula OR nombres): {unique_personas_staging}")
        
        # 4. Check how many records have invalid nombres_apellidos
        result = session.execute(text("""
            SELECT COUNT(*) as invalid_nombres
            FROM staging.semillas_stg
            WHERE nombres_apellidos IS NULL 
               OR nombres_apellidos = 'None'
               OR nombres_apellidos = ''
               OR LOWER(nombres_apellidos) IN ('none', 'nan', 'null')
        """))
        invalid_nombres = result.scalar()
        logger.info(f"4. Records with invalid nombres_apellidos: {invalid_nombres}")
        
        # 5. Test transformation pipeline on a sample
        logger.info("\n=== Testing transformation pipeline ===")
        reader = SemillasStagingReader(batch_size=1000)
        transformer = SemillasTransformer()
        
        # Get first batch
        batch_df = None
        for batch in reader.read_unprocessed_batches(session):
            batch_df = batch
            break
            
        if batch_df is not None:
            logger.info(f"Sample batch size: {len(batch_df)}")
            
            # Check unique personas in batch
            unique_in_batch = batch_df['nombres_apellidos'].nunique()
            logger.info(f"Unique nombres_apellidos in batch: {unique_in_batch}")
            
            # Transform the batch
            entities, invalid_df = transformer.transform(batch_df)
            
            if 'personas' in entities:
                logger.info(f"Personas extracted: {len(entities['personas'])}")
                logger.info(f"Invalid records: {len(invalid_df)}")
                
                # Check what was marked as invalid
                if len(invalid_df) > 0:
                    logger.info("\nSample of invalid records:")
                    for idx, row in invalid_df.head(5).iterrows():
                        logger.info(f"  - {row.get('nombres_apellidos', 'NO NAME')}: {row.get('errores_validacion', 'No error msg')}")
        
        # 6. Check current personas in operational
        result = session.execute(text("""
            SELECT COUNT(*) as total_personas
            FROM operational.persona_base
        """))
        personas_operational = result.scalar()
        logger.info(f"\n6. Current personas in operational: {personas_operational}")
        
        # 7. Analyze required fields that might be filtering out personas
        logger.info("\n=== Analyzing required fields impact ===")
        result = session.execute(text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN numero_acta IS NULL OR numero_acta = 'None' OR numero_acta = '' THEN 1 END) as missing_numero_acta,
                COUNT(CASE WHEN nombres_apellidos IS NULL OR nombres_apellidos = 'None' OR nombres_apellidos = '' THEN 1 END) as missing_nombres,
                COUNT(CASE WHEN canton IS NULL OR canton = 'None' OR canton = '' THEN 1 END) as missing_canton,
                COUNT(CASE WHEN (numero_acta IS NULL OR numero_acta = 'None' OR numero_acta = '') 
                           OR (nombres_apellidos IS NULL OR nombres_apellidos = 'None' OR nombres_apellidos = '') 
                           OR (canton IS NULL OR canton = 'None' OR canton = '' ) THEN 1 END) as would_be_invalid
            FROM staging.semillas_stg
        """))
        row = result.fetchone()
        logger.info(f"Total staging records: {row.total_records}")
        logger.info(f"Missing numero_acta: {row.missing_numero_acta}")
        logger.info(f"Missing nombres_apellidos: {row.missing_nombres}")
        logger.info(f"Missing canton: {row.missing_canton}")
        logger.info(f"Would be marked invalid (missing required fields): {row.would_be_invalid}")
        
        # 8. Check unique personas that would be valid
        result = session.execute(text("""
            SELECT COUNT(DISTINCT nombres_apellidos) as valid_unique_personas
            FROM staging.semillas_stg
            WHERE numero_acta IS NOT NULL AND numero_acta != 'None' AND numero_acta != ''
              AND nombres_apellidos IS NOT NULL AND nombres_apellidos != 'None' AND nombres_apellidos != ''
              AND canton IS NOT NULL AND canton != 'None' AND canton != ''
        """))
        valid_unique_personas = result.scalar()
        logger.info(f"\nUnique personas with all required fields: {valid_unique_personas}")
        
        logger.info(f"\n=== SUMMARY ===")
        logger.info(f"Expected unique personas: ~{unique_personas_staging}")
        logger.info(f"Valid unique personas (with required fields): {valid_unique_personas}")
        logger.info(f"Personas being lost due to validation: ~{unique_personas_staging - valid_unique_personas}")

if __name__ == "__main__":
    analyze_persona_loss()