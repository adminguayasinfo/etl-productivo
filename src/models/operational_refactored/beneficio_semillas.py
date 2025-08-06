"""Modelo de BeneficioSemillas (subtipo) para la capa operational refactorizada."""

from sqlalchemy import Column, Integer, String, DECIMAL, Text, ForeignKey
from sqlalchemy.orm import relationship
from .beneficio import Beneficio


class BeneficioSemillas(Beneficio):
    """Modelo para beneficios específicos de semillas (subtipo de Beneficio)."""
    __tablename__ = 'beneficio_semillas'
    __table_args__ = {'schema': 'etl-productivo'}
    
    # Clave primaria que es también foreign key al supertipo
    id = Column(Integer, ForeignKey('etl-productivo.beneficio.id'), primary_key=True)
    
    # Campos específicos de semillas
    responsable_agripac = Column(String(200), nullable=True)
    cedula_responsable = Column(String(20), nullable=True)
    # lugar_entrega se hereda del padre
    variedad = Column(String(100), nullable=True)
    entrega = Column(Integer, nullable=True)
    hectareas = Column(DECIMAL(10, 2), nullable=True)
    numero_acta = Column(String(100), nullable=True)
    observacion = Column(Text, nullable=True)
    
    # La relación con TipoCultivo se hereda del padre
    
    # Configuración para joined table inheritance
    __mapper_args__ = {
        'polymorphic_identity': 'semillas',
    }
    
    def __repr__(self):
        return (f"<BeneficioSemillas(id={self.id}, numero_acta='{self.numero_acta}', "
                f"variedad='{self.variedad}', hectareas={self.hectareas})>")
    
    @classmethod
    def create_from_staging(cls, beneficiario, tipo_cultivo, staging_record):
        """Crea un BeneficioSemillas a partir de un registro de staging."""
        return cls(
            # Campos del supertipo Beneficio
            fecha_entrega=staging_record.fecha_entrega,
            tipo_beneficio='SEMILLAS',
            monto=staging_record.precio_unitario,
            beneficiario=beneficiario,
            hectareas_beneficiadas=staging_record.hectarias_beneficiadas,  # CAMPO FALTANTE
            lugar_entrega=staging_record.lugar_entrega,
            observaciones=staging_record.observacion,
            anio_beneficio=staging_record.anio,
            tipo_cultivo=tipo_cultivo,
            
            # Campos específicos de semillas
            responsable_agripac=staging_record.responsable_agencia,
            cedula_responsable=staging_record.cedula_responsable,
            variedad=staging_record.variedad,
            entrega=staging_record.entrega,
            hectareas=staging_record.hectarias_beneficiadas,
            numero_acta=staging_record.numero_acta,
            observacion=staging_record.observacion
        )