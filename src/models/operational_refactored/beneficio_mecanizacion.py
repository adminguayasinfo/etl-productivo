"""Modelo de BeneficioMecanizacion (subtipo) para la capa operational refactorizada."""

from sqlalchemy import Column, Integer, String, DECIMAL, Text, ForeignKey
from sqlalchemy.orm import relationship
from .beneficio import Beneficio


class BeneficioMecanizacion(Beneficio):
    """Modelo para beneficios específicos de mecanización (subtipo de Beneficio)."""
    __tablename__ = 'beneficio_mecanizacion'
    __table_args__ = {'schema': 'etl-productivo'}
    
    # Clave primaria que es también foreign key al supertipo
    id = Column(Integer, ForeignKey('etl-productivo.beneficio.id'), primary_key=True)
    
    # Campos específicos de mecanización
    estado = Column(String(50), nullable=True)
    comentario = Column(Text, nullable=True)
    cu_ha = Column(DECIMAL(10, 2), nullable=True)  # Costo unitario por hectárea
    inversion = Column(DECIMAL(12, 2), nullable=True)
    agrupacion = Column(String(300), nullable=True)  # Nombre de agrupación
    coord_x_str = Column(String(50), nullable=True)  # Coordenada X como string
    coord_y_str = Column(String(50), nullable=True)  # Coordenada Y como string
    
    # La relación con TipoCultivo se hereda del padre
    
    # Configuración para joined table inheritance
    __mapper_args__ = {
        'polymorphic_identity': 'mecanizacion',
    }
    
    def __repr__(self):
        return (f"<BeneficioMecanizacion(id={self.id}, estado='{self.estado}', "
                f"inversion={self.inversion}, hectareas={self.hectareas_beneficiadas})>")
    
    @classmethod
    def create_from_staging(cls, beneficiario, tipo_cultivo, staging_record):
        """Crea un BeneficioMecanizacion a partir de un registro de staging."""
        return cls(
            # Campos del supertipo Beneficio
            fecha_entrega=None,  # No hay fecha de entrega en mecanización
            tipo_beneficio='MECANIZACION',
            monto=staging_record.inversion,
            beneficiario=beneficiario,
            hectareas_beneficiadas=staging_record.hectareas_beneficiadas,
            lugar_entrega=None,  # No aplica para mecanización
            observaciones=staging_record.comentario,
            anio_beneficio=staging_record.anio,
            tipo_cultivo=tipo_cultivo,
            
            # Campos específicos de mecanización
            estado=staging_record.estado,
            comentario=staging_record.comentario,
            cu_ha=staging_record.cu_ha,
            inversion=staging_record.inversion,
            agrupacion=staging_record.agrupacion,
            coord_x_str=staging_record.coord_x,
            coord_y_str=staging_record.coord_y
        )