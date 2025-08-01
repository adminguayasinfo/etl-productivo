"""Pipeline de staging para fertilizantes."""
import time
from datetime import datetime
from typing import Dict, Any
from loguru import logger

from src.load.fertilizantes_stg_load import FertilizantesStgLoader
from src.extract.fertilizantes_staging_reader import FertilizantesStagingReader
from config.connections.database import db_connection


class FertilizantesStagingPipeline:
    """Pipeline para cargar datos de fertilizantes a staging."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.loader = FertilizantesStgLoader()
        self.reader = FertilizantesStagingReader(batch_size=batch_size)
        
    def execute(self, csv_path: str, truncate: bool = False) -> Dict[str, Any]:
        """Ejecuta el pipeline de staging para fertilizantes."""
        logger.info("=== Iniciando Pipeline Staging Fertilizantes ===")
        logger.info(f"Archivo: {csv_path}")
        logger.info(f"Tamaño de lote: {self.batch_size}")
        
        start_time = time.time()
        
        try:
            # Cargar CSV a staging
            result = self.loader.load_csv_to_staging(
                csv_path=csv_path,
                batch_size=self.batch_size,
                truncate=truncate
            )
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            if result['status'] == 'success':
                logger.info("=== Pipeline Staging Fertilizantes Completado ===")
                logger.info(f"Registros procesados: {result['total_records']}")
                logger.info(f"Registros con error: {result['error_count']}")
                logger.info(f"Tiempo: {elapsed_time:.2f} segundos")
                
                return {
                    'status': 'success',
                    'total_records': result['total_records'],
                    'error_count': result['error_count'],
                    'elapsed_time': elapsed_time
                }
            else:
                logger.error(f"Error en pipeline staging fertilizantes: {result.get('error')}")
                return {
                    'status': 'error',
                    'error': result.get('error'),
                    'total_records': result.get('total_records', 0),
                    'error_count': result.get('error_count', 0),
                    'elapsed_time': elapsed_time
                }
                
        except Exception as e:
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.error(f"Error crítico en pipeline staging fertilizantes: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'total_records': 0,
                'error_count': 0,
                'elapsed_time': elapsed_time
            }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Obtiene estadísticas de la tabla staging."""
        try:
            with db_connection.get_session() as session:
                stats = self.reader.get_statistics(session)
                return stats
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas de fertilizantes: {str(e)}")
            return {
                'total_records': 0,
                'processed_records': 0,
                'pending_records': 0
            }