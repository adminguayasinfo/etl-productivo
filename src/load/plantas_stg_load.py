"""
Loader para datos de plantas de cacao a la tabla staging.
"""
from typing import List
from sqlalchemy.orm import Session
from loguru import logger

from config.connections.database import db_connection
from src.models.operational.staging.plantas_stg_model import StgPlantas


class PlantasStagingLoader:
    """Carga datos de plantas de cacao en la tabla staging."""
    
    def __init__(self, batch_size: int = 1000):
        """
        Inicializa el loader.
        
        Args:
            batch_size: Tamaño de lote para inserción
        """
        self.batch_size = batch_size
    
    def load(self, plantas_records: List[StgPlantas]) -> dict:
        """
        Carga registros de plantas en la tabla staging.
        
        Args:
            plantas_records: Lista de objetos StgPlantas
            
        Returns:
            Diccionario con estadísticas de carga
        """
        logger.info(f"Iniciando carga de {len(plantas_records)} registros de plantas")
        
        stats = {
            'total_records': len(plantas_records),
            'loaded_records': 0,
            'failed_records': 0,
            'batches_processed': 0
        }
        
        try:
            with db_connection.get_session() as session:
                # Limpiar tabla staging antes de cargar
                self._truncate_staging_table(session)
                
                # Procesar en lotes
                for i in range(0, len(plantas_records), self.batch_size):
                    batch = plantas_records[i:i + self.batch_size]
                    
                    try:
                        # Insertar lote
                        session.add_all(batch)
                        session.commit()
                        
                        stats['loaded_records'] += len(batch)
                        stats['batches_processed'] += 1
                        
                        logger.info(f"Lote {stats['batches_processed']} cargado: {len(batch)} registros")
                        
                    except Exception as e:
                        session.rollback()
                        logger.error(f"Error cargando lote {stats['batches_processed'] + 1}: {str(e)}")
                        stats['failed_records'] += len(batch)
                        continue
                
                logger.info(f"Carga completada: {stats['loaded_records']} registros exitosos, "
                           f"{stats['failed_records']} fallidos")
                
        except Exception as e:
            logger.error(f"Error durante la carga: {str(e)}")
            raise
        
        return stats
    
    def _truncate_staging_table(self, session: Session):
        """Limpia la tabla staging antes de cargar nuevos datos."""
        try:
            logger.info("Limpiando tabla staging stg_plantas")
            session.query(StgPlantas).delete()
            session.commit()
            logger.info("Tabla staging limpiada exitosamente")
        except Exception as e:
            session.rollback()
            logger.error(f"Error limpiando tabla staging: {str(e)}")
            raise
    
    def get_staging_count(self) -> int:
        """Obtiene el conteo actual de registros en staging."""
        with db_connection.get_session() as session:
            return session.query(StgPlantas).count()