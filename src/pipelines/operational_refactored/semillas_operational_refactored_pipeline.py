"""Pipeline operational refactorizado para semillas."""

import logging
from typing import Dict, Any
from datetime import datetime
from sqlalchemy import text

from config.connections.database import db_connection
from src.extract.semillas_staging_reader_refactored import SemillasStagingReaderRefactored
from src.transform.staging_to_operational import StagingToOperationalTransformer

logger = logging.getLogger(__name__)


class SemillasOperationalRefactoredPipeline:
    """Pipeline para transformar datos de staging a operational refactorizado."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.transformer = StagingToOperationalTransformer()
        self.reader = SemillasStagingReaderRefactored(batch_size=batch_size)
        
    def execute(self) -> Dict[str, Any]:
        """Ejecuta el pipeline operational refactorizado."""
        start_time = datetime.now()
        
        logger.info("=== Iniciando Pipeline Operational Refactorizado ===")
        
        try:
            # Verificar registros pendientes
            with db_connection.get_session() as session:
                count_query = text('SELECT COUNT(*) FROM "etl-productivo".stg_semilla WHERE processed = false')
                result = session.execute(count_query)
                pending_count = result.scalar()
                
                if pending_count == 0:
                    logger.info("No hay registros pendientes de procesar")
                    return {
                        'status': 'success',
                        'message': 'No hay registros pendientes',
                        'statistics': self.transformer.get_statistics(),
                        'elapsed_time': 0
                    }
                
                logger.info(f"Registros pendientes de procesar: {pending_count}")
            
            # Procesar en lotes
            batch_num = 0
            total_processed = 0
            
            with db_connection.get_session() as session:
                for batch in self.reader.read_unprocessed_batches(session):
                    batch_num += 1
                    logger.info(f"\n--- Procesando lote {batch_num} ---")
                    logger.info(f"Registros en lote: {len(batch)}")
                    
                    # Transformar lote
                    batch_stats = self.transformer.transform_batch(session, batch)
                    total_processed += batch_stats['processed']
                    
                    logger.info(f"Lote {batch_num} completado: {batch_stats['processed']} exitosos, "
                              f"{batch_stats['errors']} errores")
            
            elapsed_time = (datetime.now() - start_time).total_seconds()
            final_stats = self.transformer.get_statistics()
            
            logger.info("\n=== Pipeline Operational Refactorizado Completado ===")
            logger.info(f"Total procesados: {final_stats['processed']}")
            logger.info(f"Direcciones creadas: {final_stats['created_direcciones']}")
            logger.info(f"Asociaciones creadas: {final_stats['created_asociaciones']}")
            logger.info(f"Tipos cultivo creados: {final_stats['created_tipos_cultivo']}")
            logger.info(f"Beneficiarios creados: {final_stats['created_beneficiarios']}")
            logger.info(f"Beneficios creados: {final_stats['created_beneficios']}")
            logger.info(f"Errores: {final_stats['errors']}")
            logger.info(f"Tiempo total: {elapsed_time:.2f} segundos")
            
            return {
                'status': 'success',
                'statistics': final_stats,
                'elapsed_time': elapsed_time,
                'total_processed': total_processed
            }
            
        except Exception as e:
            logger.error(f"Error en pipeline operational refactorizado: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'statistics': self.transformer.get_statistics()
            }
    
    def get_pending_count(self) -> int:
        """Retorna el número de registros pendientes de procesar."""
        with db_connection.get_session() as session:
            count_query = text('SELECT COUNT(*) FROM "etl-productivo".stg_semilla WHERE processed = false')
            result = session.execute(count_query)
            return result.scalar()