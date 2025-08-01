from sqlalchemy import Column, Integer, String, Date, DateTime, Boolean
from sqlalchemy.sql import func
from src.models.base import Base


class DimPersona(Base):
    __tablename__ = 'dim_persona'
    __table_args__ = {'schema': 'etl-productivo'}
    
    persona_key = Column(Integer, primary_key=True, autoincrement=True)
    persona_id = Column(Integer, nullable=False, unique=True)
    cedula = Column(String(20))
    nombres_apellidos = Column(String(200), nullable=False)
    genero = Column(String(20))
    fecha_nacimiento = Column(Date)
    edad_actual = Column(Integer)
    grupo_etario = Column(String(50))
    email = Column(String(100))
    telefono = Column(String(20))
    es_beneficiario_semillas = Column(Boolean, default=False)
    tipo_productor = Column(String(50))
    hectarias_totales = Column(Integer)
    rango_hectarias = Column(String(50))
    organizacion_nombre = Column(String(200))
    
    fecha_inicio = Column(Date, nullable=False)
    fecha_fin = Column(Date)
    es_vigente = Column(Boolean, default=True)
    version = Column(Integer, default=1)
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())