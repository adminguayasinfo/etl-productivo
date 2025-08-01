"""Pipeline para cargar datos de fertilizantes al esquema analytical."""
from datetime import datetime
from typing import Dict, Any, Optional
from loguru import logger
from sqlalchemy.orm import Session

from config.connections.database import db_connection
from src.load.fertilizantes_dimensional_loader import FertilizantesDimensionalLoader


class FertilizantesAnalyticalPipeline:
    """Pipeline para transformar y cargar datos de fertilizantes a dimensional."""
    
    def __init__(self):
        self.loader = FertilizantesDimensionalLoader()
        self.stats = {
            'dim_persona': 0,
            'dim_organizacion': 0,
            'dim_ubicacion': 0,
            'dim_tiempo': 0,
            'fact_beneficio': 0,
            'duracion_total': 0,
            'errores': []
        }
        
    def execute(self, batch_size: int = 1000) -> Dict[str, Any]:
        """Ejecuta el pipeline analytical completo."""
        logger.info("=== Iniciando Pipeline Analytical Fertilizantes ===")
        start_time = datetime.now()
        
        try:
            with db_connection.get_session() as session:
                # 1. Sincronizar dimensiones
                logger.info("Sincronizando dimensiones...")
                self._sync_dimensions(session)
                
                # 2. Cargar hechos
                logger.info("Cargando hechos de beneficios...")
                self._load_facts(session, batch_size)
                
                # 3. Actualizar agregados
                logger.info("Actualizando tablas agregadas...")
                self._update_aggregates(session)
                
                session.commit()
                
        except Exception as e:
            logger.error(f"Error en pipeline analytical: {str(e)}")
            self.stats['errores'].append(str(e))
            raise
            
        finally:
            self.stats['duracion_total'] = (datetime.now() - start_time).total_seconds()
            self._log_summary()
            
        return self.stats
    
    def _sync_dimensions(self, session: Session):
        """Sincroniza todas las dimensiones."""
        try:
            # Sincronizar personas
            personas_count = self.loader.sync_dim_persona(session)
            self.stats['dim_persona'] = personas_count
            logger.info(f"Dimensión Persona sincronizada: {personas_count} registros")
            
            # Sincronizar organizaciones
            org_count = self.loader.sync_dim_organizacion(session)
            self.stats['dim_organizacion'] = org_count
            logger.info(f"Dimensión Organización sincronizada: {org_count} registros")
            
            # Sincronizar ubicaciones
            ubic_count = self.loader.sync_dim_ubicacion(session)
            self.stats['dim_ubicacion'] = ubic_count
            logger.info(f"Dimensión Ubicación sincronizada: {ubic_count} registros")
            
            # Sincronizar tiempo
            tiempo_count = self.loader.sync_dim_tiempo(session)
            self.stats['dim_tiempo'] = tiempo_count
            logger.info(f"Dimensión Tiempo sincronizada: {tiempo_count} registros")
            
        except Exception as e:
            logger.error(f"Error sincronizando dimensiones: {str(e)}")
            self.stats['errores'].append(f"Error en dimensiones: {str(e)}")
            raise
    
    def _load_facts(self, session: Session, batch_size: int):
        """Carga los hechos de beneficios."""
        try:
            # Obtener total de beneficios pendientes
            pending_count = self.loader.get_pending_beneficios_count(session)
            logger.info(f"Beneficios pendientes de carga: {pending_count}")
            
            if pending_count == 0:
                logger.info("No hay beneficios pendientes para cargar")
                return
            
            # Procesar en lotes
            total_processed = 0
            batch_num = 0
            
            while True:
                batch_num += 1
                logger.info(f"\n--- Procesando lote {batch_num} ---")
                
                # Cargar lote (siempre toma los primeros N no procesados)
                processed = self.loader.load_fact_beneficios_batch(
                    session=session,
                    batch_size=batch_size,
                    offset=0  # Siempre 0 porque ya filtramos los no procesados
                )
                
                if processed == 0:
                    break
                    
                total_processed += processed
                logger.info(f"Lote {batch_num}: {processed} beneficios procesados")
                
                # Commit parcial para liberar memoria
                session.commit()
            
            self.stats['fact_beneficio'] = total_processed
            logger.info(f"Total hechos cargados: {total_processed}")
            
        except Exception as e:
            logger.error(f"Error cargando hechos: {str(e)}")
            self.stats['errores'].append(f"Error en hechos: {str(e)}")
            raise
    
    def _update_aggregates(self, session: Session):
        """Actualiza tablas agregadas y materializa vistas si es necesario."""
        try:
            # Por ahora solo registramos que se completó
            # En el futuro aquí se pueden actualizar tablas de agregación
            logger.info("Tablas agregadas actualizadas (placeholder)")
            
        except Exception as e:
            logger.error(f"Error actualizando agregados: {str(e)}")
            self.stats['errores'].append(f"Error en agregados: {str(e)}")
    
    def _log_summary(self):
        """Registra resumen del pipeline."""
        logger.info("\n=== Pipeline Analytical Fertilizantes Completado ===")
        logger.info(f"Duración total: {self.stats['duracion_total']:.2f} segundos")
        logger.info(f"Dimensiones sincronizadas:")
        logger.info(f"  - Personas: {self.stats['dim_persona']}")
        logger.info(f"  - Organizaciones: {self.stats['dim_organizacion']}")
        logger.info(f"  - Ubicaciones: {self.stats['dim_ubicacion']}")
        logger.info(f"  - Tiempo: {self.stats['dim_tiempo']}")
        logger.info(f"Hechos cargados:")
        logger.info(f"  - Beneficios: {self.stats['fact_beneficio']}")
        
        if self.stats['errores']:
            logger.error(f"Errores encontrados: {len(self.stats['errores'])}")
            for error in self.stats['errores']:
                logger.error(f"  - {error}")