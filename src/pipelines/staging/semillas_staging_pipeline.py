"""
Pipeline para cargar datos del programa de semillas al esquema staging.
"""
import logging
from typing import Dict, Any
from pathlib import Path
from src.extract.semillas_excel_extractor import SemillasExcelExtractor
from src.load.semillas_stg_load import SemillasStgLoader

logger = logging.getLogger(__name__)


class SemillasStagingPipeline:
    """Pipeline para cargar datos Excel del programa de semillas al esquema staging."""
    
    def __init__(self, batch_size: int = 1000):
        """
        Inicializa el pipeline de staging.
        
        Args:
            batch_size: Tamaño de lote para procesamiento
        """
        self.batch_size = batch_size
        self.extractor = SemillasExcelExtractor()
        self.loader = SemillasStgLoader()
    
    def execute(self, excel_path: str, truncate: bool = True) -> Dict[str, Any]:
        """
        Ejecuta el pipeline de staging completo.
        
        Args:
            excel_path: Ruta al archivo Excel
            truncate: Si se debe truncar la tabla antes de cargar
            
        Returns:
            Diccionario con estadísticas del proceso
        """
        try:
            logger.info(f"=== Iniciando Pipeline Staging ===")
            logger.info(f"Archivo: {excel_path}")
            logger.info(f"Tamaño de lote: {self.batch_size}")
            
            # Verificar que el archivo existe
            if not Path(excel_path).exists():
                raise FileNotFoundError(f"Archivo no encontrado: {excel_path}")
            
            # Ejecutar carga
            result = self.loader.load_excel_to_staging(
                excel_path=excel_path,
                batch_size=self.batch_size,
                truncate=truncate
            )
            
            logger.info("=== Pipeline Staging Completado ===")
            logger.info(f"Registros procesados: {result['total_records']}")
            logger.info(f"Registros con error: {result['error_count']}")
            logger.info(f"Tiempo: {result.get('elapsed_time', 0):.2f} segundos")
            
            return {
                'status': 'success',
                'stage': 'staging',
                'total_records': result['total_records'],
                'error_count': result['error_count'],
                'elapsed_time': result.get('elapsed_time', 0),
                'errors_file': result.get('errors_file')
            }
            
        except Exception as e:
            logger.error(f"Error en pipeline staging: {e}")
            return {
                'status': 'error',
                'stage': 'staging',
                'error': str(e)
            }
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas del esquema staging.
        
        Returns:
            Diccionario con estadísticas
        """
        try:
            from config.connections.database import db_connection
            
            with db_connection.get_session() as session:
                from src.models.operational.staging.semillas_stg_model import StgSemilla
                
                total = session.query(StgSemilla).count()
                processed = session.query(StgSemilla).filter(
                    StgSemilla.processed == True
                ).count()
                errors = session.query(StgSemilla).filter(
                    StgSemilla.error_message.isnot(None)
                ).count()
                
                return {
                    'total_records': total,
                    'processed_records': processed,
                    'pending_records': total - processed,
                    'error_records': errors
                }
                
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {}