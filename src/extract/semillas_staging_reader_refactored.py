"""
Extractor refactorizado para leer objetos SQLAlchemy desde staging.
"""
from typing import Iterator, List
from sqlalchemy.orm import Session
from loguru import logger
from src.models.operational.staging.semillas_stg_model import StgSemilla


class SemillasStagingReaderRefactored:
    """Lee objetos SQLAlchemy desde las tablas de staging para procesamiento."""
    
    def __init__(self, batch_size: int = 1000):
        """
        Inicializa el lector de staging.
        
        Args:
            batch_size: Tamaño de lote para lectura
        """
        self.batch_size = batch_size
    
    def read_unprocessed_count(self, session: Session) -> int:
        """
        Cuenta registros no procesados en staging.
        
        Args:
            session: Sesión de base de datos
            
        Returns:
            Número de registros sin procesar
        """
        return session.query(StgSemilla).filter(
            StgSemilla.processed == False
        ).count()
    
    def read_unprocessed_batches(self, session: Session) -> Iterator[List[StgSemilla]]:
        """
        Lee registros no procesados de staging en lotes como objetos SQLAlchemy.
        
        Args:
            session: Sesión de base de datos
            
        Yields:
            Lista de objetos StgSemilla para cada lote
        """
        offset = 0
        
        while True:
            # Leer lote de registros no procesados
            batch_records = session.query(StgSemilla).filter(
                StgSemilla.processed == False
            ).offset(offset).limit(self.batch_size).all()
            
            if not batch_records:
                logger.info("No hay más registros para procesar")
                break
            
            logger.info(f"Leído lote con {len(batch_records)} registros (offset: {offset})")
            yield batch_records
            
            # Si el lote es menor que batch_size, no hay más datos
            if len(batch_records) < self.batch_size:
                break
                
            offset += self.batch_size