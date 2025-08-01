"""
Modelo para beneficios de fertilizantes en la estructura operational refactorizada.
"""
from decimal import Decimal
from sqlalchemy import Column, Integer, String, DECIMAL, ForeignKey, Text
from sqlalchemy.orm import relationship

from src.models.operational_refactored.beneficio import Beneficio


class BeneficioFertilizantes(Beneficio):
    """Modelo específico para beneficios de fertilizantes."""
    
    __tablename__ = 'beneficio_fertilizantes'
    __table_args__ = {'schema': 'etl-productivo'}
    
    # Clave foránea hacia la tabla padre
    id = Column(Integer, ForeignKey('etl-productivo.beneficio.id'), primary_key=True)
    
    # Campos específicos de fertilizantes basados en StgFertilizante
    asociaciones = Column(String(500), nullable=True, comment="Nombre de las asociaciones")
    telefono = Column(String(30), nullable=True, comment="Teléfono del beneficiario")
    genero = Column(String(20), nullable=True, comment="Género del beneficiario")
    edad = Column(Integer, nullable=True, comment="Edad del beneficiario")
    coord_x = Column(String(50), nullable=True, comment="Coordenada X")
    coord_y = Column(String(50), nullable=True, comment="Coordenada Y")
    
    # Tipos de fertilizantes entregados
    fertilizante_nitrogenado = Column(Integer, nullable=True, comment="Cantidad fertilizante nitrogenado")
    npk_elementos_menores = Column(Integer, nullable=True, comment="Cantidad NPK + elementos menores")
    organico_foliar = Column(Integer, nullable=True, comment="Cantidad orgánico foliar")
    
    # Información económica
    precio_kit = Column(DECIMAL(10, 2), nullable=True, comment="Precio del kit de fertilizantes")
    
    # Configuración del mapper para herencia
    __mapper_args__ = {
        'polymorphic_identity': 'fertilizantes'
    }
    
    # Relaciones (heredadas de Beneficio)
    # beneficiario = relationship hacia Beneficiario (heredada)
    # tipo_cultivo = relationship hacia TipoCultivo (heredada)
    
    @classmethod
    def create_from_staging(cls, beneficiario, tipo_cultivo, staging_record):
        """
        Crea una instancia de BeneficioFertilizantes desde un registro de staging.
        
        Args:
            beneficiario: Instancia de Beneficiario
            tipo_cultivo: Instancia de TipoCultivo  
            staging_record: Registro de StgFertilizante
            
        Returns:
            Instancia de BeneficioFertilizantes
        """
        return cls(
            # Campos heredados de Beneficio
            beneficiario=beneficiario,
            tipo_cultivo=tipo_cultivo,
            tipo_beneficio='FERTILIZANTES',
            fecha_entrega=staging_record.fecha_entrega,
            monto=staging_record.precio_kit,
            hectareas_beneficiadas=staging_record.hectareas,
            lugar_entrega=staging_record.lugar_entrega,
            observaciones=staging_record.observacion,
            anio_beneficio=staging_record.anio,
            
            # Campos específicos de fertilizantes
            asociaciones=staging_record.asociaciones,
            telefono=staging_record.telefono,
            genero=staging_record.genero,
            edad=staging_record.edad,
            coord_x=staging_record.coord_x,
            coord_y=staging_record.coord_y,
            fertilizante_nitrogenado=staging_record.fertilizante_nitrogenado,
            npk_elementos_menores=staging_record.npk_elementos_menores,
            organico_foliar=staging_record.organico_foliar,
            precio_kit=staging_record.precio_kit
        )
    
    def calcular_total_fertilizantes(self) -> int:
        """Calcula el total de fertilizantes entregados."""
        total = 0
        if self.fertilizante_nitrogenado:
            total += self.fertilizante_nitrogenado
        if self.npk_elementos_menores:
            total += self.npk_elementos_menores
        if self.organico_foliar:
            total += self.organico_foliar
        return total
    
    def obtener_tipos_fertilizantes(self) -> dict:
        """Retorna un diccionario con los tipos y cantidades de fertilizantes."""
        return {
            'fertilizante_nitrogenado': self.fertilizante_nitrogenado or 0,
            'npk_elementos_menores': self.npk_elementos_menores or 0,
            'organico_foliar': self.organico_foliar or 0,
            'total': self.calcular_total_fertilizantes()
        }
    
    def __repr__(self):
        return (f"<BeneficioFertilizantes(id={self.id}, "
                f"beneficiario='{self.beneficiario.nombres_completos if self.beneficiario else None}', "
                f"cultivo='{self.tipo_cultivo.nombre if self.tipo_cultivo else None}', "
                f"total_fertilizantes={self.calcular_total_fertilizantes()})>")