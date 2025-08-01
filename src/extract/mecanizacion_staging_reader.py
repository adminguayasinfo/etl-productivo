"""
Reader para datos de staging de mecanización refactorizado.
"""
from typing import Iterator, List
from sqlalchemy.orm import Session
from loguru import logger

from config.connections.database import db_connection
from src.models.operational.staging.mecanizacion_stg_model import StgMecanizacion


class MecanizacionStagingReader:
    """Lee datos de staging de mecanización de forma optimizada."""
    
    def __init__(self):
        pass
    
    def read_unprocessed_batches(self, batch_size: int = 100) -> Iterator[List[StgMecanizacion]]:
        """
        Lee registros no procesados en lotes.
        
        Args:
            batch_size: Tamaño de lote
            
        Yields:
            Lista de objetos StgMecanizacion
        """
        with db_connection.get_session() as session:
            offset = 0
            
            while True:
                # Obtener lote de registros no procesados
                batch = (session.query(StgMecanizacion)
                        .filter(StgMecanizacion.processed == False)
                        .offset(offset)
                        .limit(batch_size)
                        .all())
                
                if not batch:
                    break
                
                logger.info(f"Leído lote con {len(batch)} registros (offset: {offset})")
                yield batch
                
                offset += batch_size
                
                # Si el lote es menor que el tamaño esperado, hemos llegado al final
                if len(batch) < batch_size:
                    break
    
    def get_unprocessed_count(self) -> int:
        """Obtiene el conteo de registros no procesados."""
        with db_connection.get_session() as session:
            return (session.query(StgMecanizacion)
                   .filter(StgMecanizacion.processed == False)
                   .count())
    
    def get_total_count(self) -> int:
        """Obtiene el conteo total de registros."""
        with db_connection.get_session() as session:
            return session.query(StgMecanizacion).count()