"""Transformador de datos de staging a operational para mecanización."""

from typing import List, Dict, Any
from datetime import date
from sqlalchemy.orm import Session
from loguru import logger

from src.models.operational.staging.mecanizacion_stg_model import StgMecanizacion
from src.models.operational_refactored.direccion import Direccion
from src.models.operational_refactored.asociacion import Asociacion
from src.models.operational_refactored.tipo_cultivo import TipoCultivo
from src.models.operational_refactored.beneficiario import Beneficiario
from src.models.operational_refactored.beneficio_mecanizacion import BeneficioMecanizacion


class StagingToOperationalMecanizacionTransformer:
    """Transforma datos de staging de mecanización a la nueva estructura operational."""
    
    def __init__(self):
        self.stats = {
            'processed': 0,
            'created_direcciones': 0,
            'created_asociaciones': 0,
            'created_tipos_cultivo': 0,
            'created_beneficiarios': 0,
            'created_beneficios': 0,
            'errors': 0
        }
    
    def transform_batch(self, session: Session, staging_records: List[StgMecanizacion]) -> Dict[str, Any]:
        """Transforma un lote de registros de staging a operational."""
        logger.info(f"Transformando lote de {len(staging_records)} registros de mecanización")
        
        try:
            for record in staging_records:
                try:
                    self._transform_single_record(session, record)
                    self.stats['processed'] += 1
                    
                    # Marcar como procesado en staging
                    record.processed = True
                    
                except Exception as e:
                    logger.error(f"Error transformando registro {record.id}: {str(e)}")
                    record.error_message = str(e)
                    self.stats['errors'] += 1
            
            # Commit del lote
            session.commit()
            logger.info(f"Lote transformado: {self.stats['processed']} registros exitosos, {self.stats['errors']} errores")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error en transformación de lote: {str(e)}")
            raise
        
        return self.stats.copy()
    
    def _transform_single_record(self, session: Session, record: StgMecanizacion):
        """Transforma un registro individual de staging."""
        
        # 1. Crear/obtener Direccion
        direccion = None
        if any([record.canton, record.recinto, record.coord_x, record.coord_y]):
            direccion = Direccion.get_or_create_by_location(
                session=session,
                canton=record.canton,
                parroquia=None,  # No hay parroquia en mecanización
                recinto=record.recinto,
                coord_x=record.coord_x,
                coord_y=record.coord_y
            )
            if direccion and direccion.id is None:  # Nueva dirección
                self.stats['created_direcciones'] += 1
        
        # 2. Crear/obtener Beneficiario
        beneficiario = Beneficiario.get_or_create_by_cedula(
            session=session,
            cedula=record.cedula,
            nombres_completos=record.nombres_apellidos,
            telefono=record.telefono,
            genero=record.genero,
            edad=record.edad,
            anio_beneficio=record.anio,
            direccion=direccion
        )
        
        # Si no se puede crear beneficiario, saltar este registro
        if not beneficiario:
            logger.warning(f"No se pudo crear beneficiario para registro {record.id} - cédula: {record.cedula}")
            record.error_message = f"No se pudo crear beneficiario - cédula inválida: {record.cedula}"
            self.stats['errors'] += 1
            return
        
        if beneficiario and hasattr(beneficiario, '_sa_instance_state') and beneficiario._sa_instance_state.pending:
            self.stats['created_beneficiarios'] += 1
        
        # 3. Crear/obtener Asociacion y establecer relación (usando el campo agrupacion)
        if record.agrupacion:
            asociacion = Asociacion.get_or_create_by_name(
                session=session,
                nombre=record.agrupacion
            )
            if asociacion:
                if hasattr(asociacion, '_sa_instance_state') and asociacion._sa_instance_state.pending:
                    self.stats['created_asociaciones'] += 1
                
                # Establecer relación many-to-many si no existe
                if asociacion not in beneficiario.asociaciones:
                    beneficiario.asociaciones.append(asociacion)
        
        # 4. Crear/obtener TipoCultivo
        tipo_cultivo = None
        if record.cultivo:
            tipo_cultivo = TipoCultivo.get_or_create_by_name(
                session=session,
                nombre=record.cultivo
            )
            if tipo_cultivo and hasattr(tipo_cultivo, '_sa_instance_state') and tipo_cultivo._sa_instance_state.pending:
                self.stats['created_tipos_cultivo'] += 1
        
        # 5. Crear BeneficioMecanizacion
        beneficio_mecanizacion = BeneficioMecanizacion.create_from_staging(
            beneficiario=beneficiario,
            tipo_cultivo=tipo_cultivo,
            staging_record=record
        )
        session.add(beneficio_mecanizacion)
        self.stats['created_beneficios'] += 1
    
    def get_statistics(self) -> Dict[str, Any]:
        """Retorna estadísticas de la transformación."""
        return self.stats.copy()
    
    def reset_statistics(self):
        """Resetea las estadísticas."""
        for key in self.stats:
            self.stats[key] = 0