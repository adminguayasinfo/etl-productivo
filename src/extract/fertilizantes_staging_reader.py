"""Reader para datos de staging de fertilizantes."""
import pandas as pd
from typing import Iterator, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, update, and_, func, text
from loguru import logger

from src.models.operational.staging.fertilizantes_stg_model import StgFertilizante


class FertilizantesStagingReader:
    """Lee datos no procesados de la tabla staging de fertilizantes."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        
    def read_unprocessed_batches(self, session: Session) -> Iterator[pd.DataFrame]:
        """Lee datos no procesados en lotes."""
        
        # Contar registros pendientes
        count_query = select(func.count()).select_from(StgFertilizante).where(
            StgFertilizante.processed == False
        )
        total_pending = session.execute(count_query).scalar()
        logger.info(f"Registros de fertilizantes pendientes de procesar: {total_pending}")
        
        if total_pending == 0:
            logger.info("No hay registros de fertilizantes para procesar")
            return
            
        # Leer en lotes
        offset = 0
        while True:
            # Query para obtener lote
            query = (
                select(StgFertilizante)
                .where(StgFertilizante.processed == False)
                .order_by(StgFertilizante.id)
                .limit(self.batch_size)
                .offset(offset)
            )
            
            result = session.execute(query).scalars().all()
            
            if not result:
                break
                
            # Convertir a DataFrame
            data = []
            for row in result:
                row_dict = {
                    'id': row.id,
                    'numero_acta': row.numero_acta,
                    'documento': row.documento,
                    'proceso': row.proceso,
                    'organizacion': row.organizacion,
                    'nombres_apellidos': row.nombres_apellidos,
                    'cedula': row.cedula,
                    'telefono': row.telefono,
                    'genero': row.genero,
                    'edad': row.edad,
                    'coordenada_x': row.coordenada_x,
                    'coordenada_y': row.coordenada_y,
                    'canton': row.canton,
                    'parroquia': row.parroquia,
                    'localidad': row.localidad,
                    'hectarias_totales': row.hectarias_totales,
                    'hectarias_beneficiadas': row.hectarias_beneficiadas,
                    'tipo_fertilizante': row.tipo_fertilizante,
                    'marca_fertilizante': row.marca_fertilizante,
                    'cantidad_sacos': row.cantidad_sacos,
                    'peso_por_saco': row.peso_por_saco,
                    'precio_unitario': row.precio_unitario,
                    'costo_total': row.costo_total,
                    'responsable_agencia': row.responsable_agencia,
                    'cedula_jefe_sucursal': row.cedula_jefe_sucursal,
                    'sucursal': row.sucursal,
                    'fecha_entrega': row.fecha_entrega,
                    'anio': row.anio,
                    'observacion': row.observacion,
                    'actualizacion': row.actualizacion,
                    'rubro': row.rubro,
                    'quintil': row.quintil,
                    'score_quintil': row.score_quintil
                }
                data.append(row_dict)
            
            df = pd.DataFrame(data)
            logger.info(f"Leído lote de fertilizantes con {len(df)} registros (offset: {offset})")
            
            yield df
            offset += self.batch_size
            
    def mark_as_processed(self, session: Session, record_ids: list):
        """Marca registros como procesados."""
        if not record_ids:
            return
            
        try:
            update_stmt = (
                update(StgFertilizante)
                .where(StgFertilizante.id.in_(record_ids))
                .values(processed=True, updated_at=func.now())
            )
            session.execute(update_stmt)
            logger.debug(f"Marcados {len(record_ids)} registros de fertilizantes como procesados")
            
        except Exception as e:
            logger.error(f"Error marcando registros de fertilizantes como procesados: {str(e)}")
            raise
            
    def get_statistics(self, session: Session) -> Dict[str, int]:
        """Obtiene estadísticas de la tabla staging."""
        total = session.execute(select(func.count()).select_from(StgFertilizante)).scalar()
        processed = session.execute(
            select(func.count()).select_from(StgFertilizante).where(StgFertilizante.processed == True)
        ).scalar()
        pending = total - processed
        
        return {
            'total_records': total,
            'processed_records': processed,
            'pending_records': pending
        }