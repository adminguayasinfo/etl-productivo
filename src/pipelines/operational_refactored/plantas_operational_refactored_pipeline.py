"""
Pipeline operational para plantas.
Carga datos desde stg_plantas a la estructura operational usando BeneficioPlanta.
"""
from typing import Dict, Any, List
from datetime import datetime
from loguru import logger
from sqlalchemy.orm import Session

from config.connections.database import db_connection
from src.models.operational.staging.plantas_stg_model import StgPlantas
from src.models.operational_refactored.direccion import Direccion
from src.models.operational_refactored.asociacion import Asociacion
from src.models.operational_refactored.tipo_cultivo import TipoCultivo
from src.models.operational_refactored.beneficiario import Beneficiario
from src.models.operational_refactored.beneficio_plantas import BeneficioPlanta


class PlantasOperationalRefactorizedPipeline:
    """Pipeline para procesar datos de plantas de staging a operational."""
    
    def __init__(self, batch_size: int = 100):
        """
        Inicializa el pipeline.
        
        Args:
            batch_size: Tamaño de lote para procesamiento
        """
        self.batch_size = batch_size
        
        # Estadísticas del pipeline
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_processed': 0,
            'total_success': 0,
            'total_errors': 0,
            'batches_processed': 0,
            'entities_created': {
                'direcciones': 0,
                'asociaciones': 0,
                'tipos_cultivo': 0,
                'beneficiarios': 0,
                'beneficios_plantas': 0
            }
        }
    
    def run(self) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo.
        
        Returns:
            Diccionario con estadísticas de ejecución
        """
        logger.info("=== Iniciando Pipeline Operational Plantas ===")
        self.stats['start_time'] = datetime.now()
        
        try:
            with db_connection.get_session() as session:
                # Contar registros pendientes
                pending_count = self._get_pending_count(session)
                logger.info(f"Registros pendientes en staging: {pending_count:,}")
                
                if pending_count == 0:
                    logger.info("No hay registros pendientes para procesar")
                    return self._build_final_stats()
                
                # Procesar en lotes
                processed_records = 0
                
                while processed_records < pending_count:
                    batch_num = self.stats['batches_processed'] + 1
                    logger.info(f"\n--- Procesando lote {batch_num} ---")
                    
                    # Leer lote de staging
                    staging_records = self._read_staging_batch(session)
                    if not staging_records:
                        break
                    
                    logger.info(f"Procesando {len(staging_records)} registros...")
                    
                    # Procesar lote
                    batch_stats = self._process_batch(session, staging_records)
                    
                    # Actualizar estadísticas
                    self._update_stats(batch_stats)
                    processed_records += len(staging_records)
                    
                    # Commit del lote
                    session.commit()
                    logger.info(f"Lote {batch_num} completado - Éxitos: {batch_stats['success']}, Errores: {batch_stats['errors']}")
        
        except Exception as e:
            logger.error(f"Error crítico en pipeline: {e}")
            raise
        
        finally:
            self.stats['end_time'] = datetime.now()
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            logger.info("\n=== Pipeline Operational Plantas Completado ===")
            logger.info(f"Duración: {duration:.2f} segundos")
            logger.info(f"Lotes procesados: {self.stats['batches_processed']}")
            logger.info(f"Total registros: {self.stats['total_processed']:,}")
            logger.info(f"Exitosos: {self.stats['total_success']:,}")
            logger.info(f"Errores: {self.stats['total_errors']:,}")
            logger.info(f"\nEntidades creadas:")
            for entity, count in self.stats['entities_created'].items():
                logger.info(f"  - {entity}: {count:,}")
        
        return self._build_final_stats()
    
    def _get_pending_count(self, session: Session) -> int:
        """Obtiene cantidad de registros pendientes."""
        return session.query(StgPlantas).filter(
            StgPlantas.processed == False
        ).count()
    
    def _read_staging_batch(self, session: Session) -> List[StgPlantas]:
        """Lee un lote de registros desde staging."""
        return session.query(StgPlantas).filter(
            StgPlantas.processed == False
        ).order_by(StgPlantas.id).limit(self.batch_size).all()
    
    def _process_batch(self, session: Session, staging_records: List[StgPlantas]) -> Dict[str, int]:
        """Procesa un lote de registros de staging."""
        batch_stats = {'success': 0, 'errors': 0}
        
        for record in staging_records:
            try:
                # Procesar registro individual
                self._process_single_record(session, record)
                
                # Marcar como procesado
                record.processed = True
                record.error_message = None
                batch_stats['success'] += 1
                
            except Exception as e:
                logger.error(f"Error procesando registro {record.id}: {e}")
                record.processed = True
                record.error_message = str(e)[:500]  # Limitar longitud del error
                batch_stats['errors'] += 1
        
        return batch_stats
    
    def _process_single_record(self, session: Session, record: StgPlantas):
        """Procesa un registro individual de staging."""
        
        # 1. Crear/obtener dirección
        direccion = self._get_or_create_direccion(session, record)
        
        # 2. Crear/obtener asociación si existe
        asociacion = None
        if record.asociaciones and record.asociaciones.strip():
            asociacion = self._get_or_create_asociacion(session, record.asociaciones.strip())
        
        # 3. Crear/obtener tipo de cultivo
        tipo_cultivo = self._get_or_create_tipo_cultivo(session, record.cultivo_1)
        
        # 4. Crear/obtener beneficiario
        beneficiario = self._get_or_create_beneficiario(session, record, direccion, asociacion)
        
        # 5. Crear beneficio de plantas
        beneficio = BeneficioPlanta.create_from_staging(
            beneficiario=beneficiario,
            tipo_cultivo=tipo_cultivo,
            staging_record=record
        )
        
        session.add(beneficio)
        self.stats['entities_created']['beneficios_plantas'] += 1
    
    def _get_or_create_direccion(self, session: Session, record: StgPlantas) -> Direccion:
        """Crea o obtiene una dirección."""
        # Usar el método del modelo para obtener/crear
        direccion = Direccion.get_or_create_by_location(
            session=session,
            canton=(record.canton or '').strip(),
            parroquia=(record.parroquia or '').strip(),
            recinto=(record.recinto_comuna_sector or '').strip(),
            coord_x=record.coord_x,
            coord_y=record.coord_y
        )
        
        # Solo contar como nueva si es una creación
        if direccion.id is None:
            self.stats['entities_created']['direcciones'] += 1
        
        return direccion
    
    def _get_or_create_asociacion(self, session: Session, nombre_asociacion: str) -> Asociacion:
        """Crea o obtiene una asociación."""
        asociacion = session.query(Asociacion).filter(
            Asociacion.nombre == nombre_asociacion
        ).first()
        
        if not asociacion:
            asociacion = Asociacion(nombre=nombre_asociacion)
            session.add(asociacion)
            session.flush()
            self.stats['entities_created']['asociaciones'] += 1
        
        return asociacion
    
    def _get_or_create_tipo_cultivo(self, session: Session, cultivo: str) -> TipoCultivo:
        """Crea o obtiene un tipo de cultivo."""
        if not cultivo or not cultivo.strip():
            cultivo = 'CACAO'  # Default para plantas
        
        cultivo_normalizado = cultivo.strip().upper()
        
        tipo_cultivo = session.query(TipoCultivo).filter(
            TipoCultivo.nombre == cultivo_normalizado
        ).first()
        
        if not tipo_cultivo:
            tipo_cultivo = TipoCultivo(nombre=cultivo_normalizado)
            session.add(tipo_cultivo)
            session.flush()
            self.stats['entities_created']['tipos_cultivo'] += 1
        
        return tipo_cultivo
    
    def _get_or_create_beneficiario(self, session: Session, record: StgPlantas, 
                                   direccion: Direccion, asociacion: Asociacion = None) -> Beneficiario:
        """Crea o obtiene un beneficiario."""
        # Construir nombres completos
        nombres_completos = ''
        if record.nombres_completos and record.nombres_completos.strip():
            nombres_completos = record.nombres_completos.strip()
        elif record.nombres and record.apellidos:
            nombres_completos = f"{record.nombres.strip()} {record.apellidos.strip()}"
        else:
            nombres_completos = 'NO_ESPECIFICADO'
        
        cedula = (record.cedula or '').strip() or f'TEMP_{record.id}'  # Generar cédula temporal si no existe
        
        # Calcular fecha de nacimiento si hay edad
        fecha_nacimiento = None
        if record.edad and record.anio:
            fecha_nacimiento = Beneficiario.calcular_fecha_nacimiento(record.edad, record.anio)
        
        beneficiario = Beneficiario.get_or_create_by_cedula(
            session=session,
            cedula=cedula,
            nombres_completos=nombres_completos,
            telefono=(record.telefono or '').strip() or None,
            genero=(record.genero or '').strip() or None,
            edad=record.edad,
            anio_beneficio=record.anio,
            direccion=direccion
        )
        
        # Asociar con asociación si existe
        if asociacion and asociacion not in beneficiario.asociaciones:
            beneficiario.asociaciones.append(asociacion)
        
        # Solo contar como nuevo si es una creación
        if beneficiario.id is None:
            self.stats['entities_created']['beneficiarios'] += 1
        
        return beneficiario
    
    def _update_stats(self, batch_stats: Dict[str, int]):
        """Actualiza las estadísticas del pipeline."""
        self.stats['total_processed'] += batch_stats['success'] + batch_stats['errors']
        self.stats['total_success'] += batch_stats['success']
        self.stats['total_errors'] += batch_stats['errors']
        self.stats['batches_processed'] += 1
    
    def _build_final_stats(self) -> Dict[str, Any]:
        """Construye las estadísticas finales."""
        duration = 0
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        return {
            'status': 'completed',
            'duration_seconds': duration,
            'total_processed': self.stats['total_processed'],
            'total_success': self.stats['total_success'],
            'total_errors': self.stats['total_errors'],
            'batches_processed': self.stats['batches_processed'],
            'entities_created': self.stats['entities_created'].copy(),
            'success_rate': (self.stats['total_success'] / max(1, self.stats['total_processed'])) * 100
        }