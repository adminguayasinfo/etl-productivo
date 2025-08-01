"""
Modelo staging para datos de plantas de cacao.
"""
from sqlalchemy import Column, String, Integer, DECIMAL, Boolean, DateTime, Text
from src.models.base import Base
from src.models.operational.staging.base_stg import StagingBase, TimestampMixin


class StgPlantas(StagingBase, TimestampMixin):
    """Modelo para staging de datos de plantas de cacao."""
    
    __tablename__ = 'stg_plantas'
    __table_args__ = {'schema': 'etl-productivo'}
    
    id = Column(Integer, primary_key=True)
    
    # Campos identificadores
    actas = Column(String(50), nullable=True, comment="Código único del acta")
    
    # Información del beneficiario
    asociaciones = Column(String(300), nullable=True, comment="Nombre de la asociación")
    apellidos = Column(String(150), nullable=True, comment="Apellidos del beneficiario")
    nombres = Column(String(150), nullable=True, comment="Nombres del beneficiario")
    nombres_completos = Column(String(300), nullable=True, comment="Nombres completos concatenados")
    cedula = Column(String(20), nullable=True, comment="Cédula de identidad")
    telefono = Column(String(20), nullable=True, comment="Número telefónico")
    genero = Column(String(20), nullable=True, comment="Género del beneficiario")
    edad = Column(Integer, nullable=True, comment="Edad del beneficiario")
    
    # Información geográfica
    canton = Column(String(100), nullable=True, comment="Cantón")
    parroquia = Column(String(100), nullable=True, comment="Parroquia")
    recinto_comuna_sector = Column(String(200), nullable=True, comment="Recinto, comuna o sector")
    coord_x = Column(DECIMAL(15, 2), nullable=True, comment="Coordenada X")
    coord_y = Column(DECIMAL(15, 2), nullable=True, comment="Coordenada Y")
    
    # Información del beneficio
    hectareas = Column(DECIMAL(10, 2), nullable=True, comment="Hectáreas del beneficiario")
    entrega = Column(Integer, nullable=True, comment="Cantidad de plantas entregadas")
    cultivo_1 = Column(String(100), nullable=True, comment="Tipo de cultivo (siempre CACAO)")
    fecha_entrega = Column(DateTime, nullable=True, comment="Fecha de entrega")
    lugar_entrega = Column(String(100), nullable=True, comment="Lugar de entrega")
    
    # Información administrativa
    contratista = Column(String(200), nullable=True, comment="Nombre del contratista")
    cedula_contratista = Column(String(20), nullable=True, comment="Cédula del contratista")
    observacion = Column(Text, nullable=True, comment="Observaciones")
    precio_unitario = Column(DECIMAL(10, 3), nullable=True, comment="Precio unitario de la planta")
    anio = Column(Integer, nullable=True, comment="Año del beneficio")
    rubro = Column(String(100), nullable=True, comment="Rubro del programa")
    
    # Campos de control
    processed = Column(Boolean, default=False)
    error_message = Column(Text)
    
    def __repr__(self):
        return f"<StgPlantas(id={self.id}, actas='{self.actas}', cedula='{self.cedula}', entrega={self.entrega})>"