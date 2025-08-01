"""Modelo de staging para mecanización actualizado para Excel."""
from sqlalchemy import Column, Integer, String, DECIMAL, Date, Text, Boolean
from src.models.operational.staging.base_stg import StagingBase, TimestampMixin


class StgMecanizacion(StagingBase, TimestampMixin):
    """Tabla de staging para datos de mecanización desde Excel."""
    __tablename__ = 'stg_mecanizacion'
    __table_args__ = {'schema': 'etl-productivo'}
    
    id = Column(Integer, primary_key=True)
    
    # Campos del Excel MECANIZACIÓN (en orden)
    nombres_apellidos = Column(String(300), nullable=False)  # APELLIDOS Y NOMBRES
    cedula = Column(String(20), nullable=True)  # CÉDULA DE IDENTIDAD
    telefono = Column(String(30), nullable=True)  # NÚMERO DE TELÉFONO
    genero = Column(String(20), nullable=True)  # Género
    edad = Column(Integer, nullable=True)  # EDAD
    canton = Column(String(50), nullable=True)  # CANTON
    agrupacion = Column(String(300), nullable=True)  # AGRUPACIÓN
    recinto = Column(String(100), nullable=True)  # RECINTO, COMUNA O SECTOR
    coord_x = Column(String(50), nullable=True)  # X
    coord_y = Column(String(50), nullable=True)  # Y
    hectareas_beneficiadas = Column(DECIMAL(10, 2), nullable=True)  # HECTÁREAS BENEFICIADAS
    cultivo = Column(String(100), nullable=True)  # CULTIVO
    estado = Column(String(50), nullable=True)  # ESTADO
    comentario = Column(Text, nullable=True)  # COMENTARIO
    cu_ha = Column(DECIMAL(10, 2), nullable=True)  # CU-HA (costo unitario por hectárea)
    inversion = Column(DECIMAL(12, 2), nullable=True)  # INVERSION
    anio = Column(Integer, nullable=False)  # AÑO
    
    # Processing fields
    processed = Column(Boolean, default=False)
    error_message = Column(Text)
    
    def __repr__(self):
        return f"<StgMecanizacion(id={self.id}, nombres='{self.nombres_apellidos}', cultivo='{self.cultivo}')>"