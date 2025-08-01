"""Modelo de BeneficioPlanta (subtipo) para la capa operational refactorizada."""

from sqlalchemy import Column, Integer, String, DECIMAL, Text, ForeignKey
from sqlalchemy.orm import relationship
from .beneficio import Beneficio


class BeneficioPlanta(Beneficio):
    """Modelo para beneficios específicos de plantas (subtipo de Beneficio)."""
    __tablename__ = 'beneficio_plantas'
    __table_args__ = {'schema': 'etl-productivo'}
    
    # Clave primaria que es también foreign key al supertipo
    id = Column(Integer, ForeignKey('etl-productivo.beneficio.id'), primary_key=True)
    
    # Campos específicos de plantas
    actas = Column(String(50), nullable=True)
    contratista = Column(String(200), nullable=True)
    cedula_contratista = Column(String(20), nullable=True)
    entrega = Column(Integer, nullable=True)  # Cantidad de plantas entregadas
    hectareas = Column(DECIMAL(10, 2), nullable=True)  # Hectáreas del beneficiario
    precio_unitario = Column(DECIMAL(10, 3), nullable=True)
    rubro = Column(String(100), nullable=True)
    observacion = Column(Text, nullable=True)
    
    # La relación con TipoCultivo se hereda del padre
    
    # Configuración para joined table inheritance
    __mapper_args__ = {
        'polymorphic_identity': 'plantas',
    }
    
    def __repr__(self):
        return (f"<BeneficioPlanta(id={self.id}, actas='{self.actas}', "
                f"entrega={self.entrega}, hectareas={self.hectareas})>")
    
    @classmethod
    def create_from_staging(cls, beneficiario, tipo_cultivo, staging_record):
        """Crea un BeneficioPlanta a partir de un registro de staging."""
        return cls(
            # Campos del supertipo Beneficio
            fecha_entrega=staging_record.fecha_entrega,
            tipo_beneficio='PLANTAS',
            monto=staging_record.precio_unitario,
            beneficiario=beneficiario,
            hectareas_beneficiadas=staging_record.hectareas,
            lugar_entrega=staging_record.lugar_entrega,
            observaciones=staging_record.observacion,
            anio_beneficio=staging_record.anio,
            tipo_cultivo=tipo_cultivo,
            
            # Campos específicos de plantas
            actas=staging_record.actas,
            contratista=staging_record.contratista,
            cedula_contratista=staging_record.cedula_contratista,
            entrega=staging_record.entrega,
            hectareas=staging_record.hectareas,
            precio_unitario=staging_record.precio_unitario,
            rubro=staging_record.rubro,
            observacion=staging_record.observacion
        )