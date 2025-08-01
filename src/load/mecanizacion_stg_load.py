"""
Loader para cargar datos de mecanización a staging desde Excel.
"""
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from loguru import logger

from config.connections.database import db_connection
from src.models.operational.staging.mecanizacion_stg_model import StgMecanizacion
from src.extract.mecanizacion_excel_extractor import MecanizacionExcelExtractor


class MecanizacionStgLoader:
    """Carga datos de mecanización desde Excel a staging."""
    
    def __init__(self):
        self.extractor = None
        self.session = None
    
    def truncate_staging_table(self):
        """Trunca la tabla de staging de mecanización."""
        with db_connection.get_session() as session:
            try:
                truncate_query = text('TRUNCATE TABLE "etl-productivo".stg_mecanizacion RESTART IDENTITY CASCADE')
                session.execute(truncate_query)
                session.commit()
                logger.info("Tabla etl-productivo.stg_mecanizacion truncada exitosamente")
            except Exception as e:
                session.rollback()
                logger.error(f"Error truncando tabla: {str(e)}")
                raise
    
    def load_batch_to_staging(self, session: Session, batch_data: List[Dict[str, Any]]) -> int:
        """
        Carga un lote de datos a staging.
        
        Args:
            session: Sesión de base de datos
            batch_data: Lista de diccionarios con los datos
            
        Returns:
            Número de registros insertados
        """
        inserted_count = 0
        
        for row_data in batch_data:
            try:
                # Crear objeto StgMecanizacion
                stg_record = StgMecanizacion(
                    nombres_apellidos=row_data.get('nombres_apellidos'),
                    cedula=row_data.get('cedula'),
                    telefono=row_data.get('telefono'),
                    genero=row_data.get('genero'),
                    edad=row_data.get('edad'),
                    canton=row_data.get('canton'),
                    agrupacion=row_data.get('agrupacion'),
                    recinto=row_data.get('recinto'),
                    coord_x=row_data.get('coord_x'),
                    coord_y=row_data.get('coord_y'),
                    hectareas_beneficiadas=row_data.get('hectareas_beneficiadas'),
                    cultivo=row_data.get('cultivo'),
                    estado=row_data.get('estado'),
                    comentario=row_data.get('comentario'),
                    cu_ha=row_data.get('cu_ha'),
                    inversion=row_data.get('inversion'),
                    anio=row_data.get('anio', 2024)  # Default a 2024 si no viene
                )
                
                session.add(stg_record)
                inserted_count += 1
                
            except Exception as e:
                logger.error(f"Error insertando registro: {str(e)}")
                logger.error(f"Datos del registro: {row_data}")
                # Continuar con el siguiente registro
                continue
        
        return inserted_count
    
    def load_excel_to_staging(self, excel_path: str, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Carga datos desde Excel a staging.
        
        Args:
            excel_path: Ruta al archivo Excel
            batch_size: Tamaño de lote
            
        Returns:
            Diccionario con estadísticas de la carga
        """
        logger.info(f"Iniciando carga a staging desde Excel: {excel_path}")
        
        # Truncar tabla
        self.truncate_staging_table()
        
        # Inicializar extractor
        self.extractor = MecanizacionExcelExtractor(excel_path)
        
        # Estadísticas
        total_processed = 0
        total_errors = 0
        batch_num = 0
        
        try:
            # Procesar en lotes
            with db_connection.get_session() as session:
                for batch_data in self.extractor.extract_batches(batch_size):
                    batch_num += 1
                    
                    try:
                        # Cargar lote
                        inserted_count = self.load_batch_to_staging(session, batch_data)
                        
                        # Commit del lote
                        session.commit()
                        
                        total_processed += inserted_count
                        logger.info(f"Lote {batch_num}: {inserted_count} registros insertados (Total: {total_processed})")
                        
                    except Exception as e:
                        session.rollback()
                        total_errors += len(batch_data)
                        logger.error(f"Error en lote {batch_num}: {str(e)}")
                        continue
            
            logger.info("\n=== RESUMEN DE CARGA ===")
            logger.info(f"Total registros procesados: {total_processed}")
            logger.info(f"Errores: {total_errors}")
            
            return {
                'total_processed': total_processed,
                'total_errors': total_errors,
                'success_rate': (total_processed / (total_processed + total_errors)) * 100 if (total_processed + total_errors) > 0 else 0,
                'batches_processed': batch_num
            }
            
        except Exception as e:
            logger.error(f"Error general en carga: {str(e)}")
            return {
                'total_processed': total_processed,
                'total_errors': total_errors + 1,
                'success_rate': 0,
                'batches_processed': batch_num,
                'error': str(e)
            }
    
    def get_staging_count(self) -> int:
        """Obtiene el conteo actual de registros en staging."""
        with db_connection.get_session() as session:
            count_query = text('SELECT COUNT(*) FROM "etl-productivo".stg_mecanizacion')
            result = session.execute(count_query)
            return result.scalar()