"""Loader para cargar datos a la tabla staging."""
import pandas as pd
from datetime import datetime
from typing import Tuple, List, Dict, Any
from sqlalchemy import text
from sqlalchemy.orm import Session
from loguru import logger

from config.connections.database import db_connection
from src.models.operational.staging.semillas_stg_model import StgSemilla


class SemillasStgLoader:
    """Carga datos a la tabla staging de semillas."""
    
    def __init__(self):
        self.processed_count = 0
        self.error_count = 0
        self.errors = []
    
    def truncate_staging_table(self, session: Session):
        """Truncar la tabla staging antes de la carga completa."""
        try:
            session.execute(text('TRUNCATE TABLE "etl-productivo".stg_semilla RESTART IDENTITY CASCADE'))
            session.commit()
            logger.info("Tabla etl-productivo.stg_semilla truncada exitosamente")
        except Exception as e:
            session.rollback()
            logger.error(f"Error al truncar tabla: {str(e)}")
            raise
    
    def load_batch(self, batch_data: List[Dict[str, Any]], session: Session) -> int:
        """Carga un lote de datos a staging."""
        batch_records = []
        
        for data in batch_data:
            try:
                record = StgSemilla(**data)
                batch_records.append(record)
            except Exception as e:
                self.error_count += 1
                error_msg = f"Error creando registro: {str(e)}"
                self.errors.append(error_msg)
                logger.error(error_msg)
        
        # Insertar lote
        if batch_records:
            try:
                session.bulk_save_objects(batch_records)
                session.commit()
                return len(batch_records)
            except Exception as e:
                session.rollback()
                logger.error(f"Error insertando lote: {str(e)}")
                self.error_count += len(batch_records)
                raise
                
        return 0
    
    def load_csv_to_staging(self, csv_path: str, batch_size: int = 1000, truncate: bool = True) -> Dict[str, Any]:
        """Cargar datos del CSV a la tabla staging."""
        logger.info(f"Iniciando carga a staging desde: {csv_path}")
        start_time = datetime.now()
        
        try:
            # Resetear contadores
            self.processed_count = 0
            self.error_count = 0
            self.errors = []
            
            # Importar extractor aquí para evitar dependencia circular
            from src.extract.semillas_csv_extractor import SemillasCSVExtractor
            extractor = SemillasCSVExtractor(batch_size)
            
            with db_connection.get_session() as session:
                # Truncar tabla si se solicita
                if truncate:
                    self.truncate_staging_table(session)
                
                # Procesar en lotes usando el extractor
                batch_num = 0
                for batch_df in extractor.extract_batches(csv_path):
                    batch_num += 1
                    batch_data = []
                    
                    # Preparar datos del batch
                    for idx, row in batch_df.iterrows():
                        try:
                            data = extractor.prepare_row(row)
                            batch_data.append(data)
                        except Exception as e:
                            self.error_count += 1
                            error_msg = f"Error procesando fila {idx + 1}: {str(e)}"
                            self.errors.append(error_msg)
                            logger.error(error_msg)
                    
                    # Cargar el batch
                    if batch_data:
                        loaded = self.load_batch(batch_data, session)
                        self.processed_count += loaded
                        
                        logger.info(f"Lote {batch_num}: {loaded} registros insertados "
                                  f"(Total: {self.processed_count})")
            
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            # Resumen final
            logger.info(f"\n=== RESUMEN DE CARGA ===")
            logger.info(f"Total registros procesados: {self.processed_count}")
            logger.info(f"Errores: {self.error_count}")
            
            errors_file = None
            if self.errors:
                # Guardar errores en archivo
                errors_file = f"data/failed/semillas_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                with open(errors_file, 'w') as f:
                    f.write("\n".join(self.errors))
                logger.info(f"Errores guardados en: {errors_file}")
            
            return {
                'status': 'success',
                'total_records': self.processed_count,
                'error_count': self.error_count,
                'elapsed_time': elapsed_time,
                'errors_file': errors_file
            }
            
        except Exception as e:
            logger.error(f"Error crítico en carga: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'total_records': self.processed_count,
                'error_count': self.error_count
            }
    
    def load_excel_to_staging(self, excel_path: str, batch_size: int = 1000, truncate: bool = True) -> Dict[str, Any]:
        """Cargar datos del Excel (pestaña SEMILLAS) a la tabla staging."""
        logger.info(f"Iniciando carga a staging desde Excel: {excel_path}")
        start_time = datetime.now()
        
        try:
            # Resetear contadores
            self.processed_count = 0
            self.error_count = 0
            self.errors = []
            
            # Importar extractor aquí para evitar dependencia circular
            from src.extract.semillas_excel_extractor import SemillasExcelExtractor
            extractor = SemillasExcelExtractor(batch_size)
            
            with db_connection.get_session() as session:
                # Truncar tabla si se solicita
                if truncate:
                    self.truncate_staging_table(session)
                
                # Procesar en lotes usando el extractor
                batch_num = 0
                for batch_df in extractor.extract_batches(excel_path):
                    batch_num += 1
                    batch_data = []
                    
                    # Preparar datos del batch
                    for idx, row in batch_df.iterrows():
                        try:
                            data = extractor.prepare_row(row)
                            batch_data.append(data)
                        except Exception as e:
                            self.error_count += 1
                            error_msg = f"Error procesando fila {idx + 1}: {str(e)}"
                            self.errors.append(error_msg)
                            logger.error(error_msg)
                    
                    # Cargar el batch
                    if batch_data:
                        loaded = self.load_batch(batch_data, session)
                        self.processed_count += loaded
                        
                        logger.info(f"Lote {batch_num}: {loaded} registros insertados "
                                  f"(Total: {self.processed_count})")
            
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            # Resumen final
            logger.info(f"\n=== RESUMEN DE CARGA ===")
            logger.info(f"Total registros procesados: {self.processed_count}")
            logger.info(f"Errores: {self.error_count}")
            
            errors_file = None
            if self.errors:
                # Guardar errores en archivo
                errors_file = f"data/failed/semillas_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                with open(errors_file, 'w') as f:
                    f.write("\n".join(self.errors))
                logger.info(f"Errores guardados en: {errors_file}")
            
            return {
                'status': 'success',
                'total_records': self.processed_count,
                'error_count': self.error_count,
                'elapsed_time': elapsed_time,
                'errors_file': errors_file
            }
            
        except Exception as e:
            logger.error(f"Error crítico en carga: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'total_records': self.processed_count,
                'error_count': self.error_count
            }
            
    def reset_stats(self):
        """Resetea las estadísticas del loader."""
        self.processed_count = 0
        self.error_count = 0
        self.errors = []


