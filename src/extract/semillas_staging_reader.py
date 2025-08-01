"""
Extractor para leer datos desde el esquema staging.
"""
import pandas as pd
from typing import Iterator, Optional
from sqlalchemy.orm import Session
from loguru import logger
from src.models.operational.staging.semillas_stg_model import StgSemilla


class SemillasStagingReader:
    """Lee datos desde las tablas de staging para procesamiento."""
    
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
    
    def read_unprocessed_batches(self, session: Session) -> Iterator[pd.DataFrame]:
        """
        Lee registros no procesados de staging en lotes.
        
        Args:
            session: Sesión de base de datos
            
        Yields:
            DataFrame con cada lote de registros
        """
        offset = 0
        
        while True:
            # Leer lote de registros no procesados
            batch_data = self._read_batch(session, offset)
            
            if batch_data.empty:
                logger.info("No hay más registros para procesar")
                break
            
            logger.info(f"Leído lote con {len(batch_data)} registros (offset: {offset})")
            yield batch_data
            
            # Si el lote es menor que batch_size, no hay más datos
            if len(batch_data) < self.batch_size:
                break
                
            offset += self.batch_size
    
    def _read_batch(self, session: Session, offset: int) -> pd.DataFrame:
        """
        Lee un lote de registros no procesados.
        
        Args:
            session: Sesión de base de datos
            offset: Desplazamiento para paginación
            
        Returns:
            DataFrame con los registros
        """
        # IMPORTANTE: No usar offset cuando se filtra por processed=False
        # porque los registros se van marcando como procesados
        query = session.query(StgSemilla).filter(
            StgSemilla.processed == False
        ).order_by(StgSemilla.id).limit(self.batch_size)
        
        records = query.all()
        
        if not records:
            return pd.DataFrame()
        
        # Convertir a DataFrame
        data = []
        for record in records:
            row_dict = {
                'id': record.id,
                'numero_acta': record.numero_acta,
                'documento': record.documento,
                'proceso': record.proceso,
                'organizacion': record.organizacion,
                'nombres_apellidos': record.nombres_apellidos,
                'cedula': record.cedula,
                'telefono': record.telefono,
                'genero': record.genero,
                'edad': record.edad,
                'coordenada_x': record.coordenada_x,
                'coordenada_y': record.coordenada_y,
                'canton': record.canton,
                'parroquia': record.parroquia,
                'localidad': record.localidad,
                'hectarias_totales': record.hectarias_totales,
                'hectarias_beneficiadas': record.hectarias_beneficiadas,
                'cultivo': record.cultivo,
                'precio_unitario': record.precio_unitario,
                'inversion': record.inversion,
                'responsable_agencia': record.responsable_agencia,
                'cedula_jefe_sucursal': record.cedula_jefe_sucursal,
                'sucursal': record.sucursal,
                'fecha_retiro': record.fecha_retiro,
                'anio': record.anio,
                'observacion': record.observacion,
                'actualizacion': record.actualizacion,
                'rubro': record.rubro,
                'quintil': record.quintil,
                'score_quintil': record.score_quintil
            }
            data.append(row_dict)
        
        return pd.DataFrame(data)
    
    def mark_as_processed(self, session: Session, record_ids: list, error_messages: Optional[dict] = None):
        """
        Marca registros como procesados.
        
        Args:
            session: Sesión de base de datos
            record_ids: Lista de IDs a marcar
            error_messages: Diccionario opcional {id: mensaje_error}
            
        Note:
            Esta función NO hace commit de la sesión. El commit debe ser manejado
            por el contexto que llama a esta función.
        """
        if not record_ids:
            return
            
        # Actualizar registros
        for record_id in record_ids:
            record = session.query(StgSemilla).filter(
                StgSemilla.id == record_id
            ).first()
            
            if record:
                record.processed = True
                if error_messages and record_id in error_messages:
                    record.error_message = error_messages[record_id]
        
        # NO hacer commit aquí - dejar que el contexto lo maneje
        session.flush()  # Asegurar que los cambios se escriban al buffer de la BD
        logger.debug(f"Marcados {len(record_ids)} registros como procesados")