from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Boolean
from sqlalchemy.sql import func
from src.models.base import Base


class DimUbicacion(Base):
    __tablename__ = 'dim_ubicacion'
    __table_args__ = {'schema': 'etl-productivo'}
    
    ubicacion_key = Column(Integer, primary_key=True, autoincrement=True)
    ubicacion_id = Column(Integer, nullable=False, unique=True)
    provincia = Column(String(100), nullable=False)
    canton = Column(String(100), nullable=False)
    parroquia = Column(String(100), nullable=False)
    comunidad = Column(String(200))
    zona = Column(String(50))
    region = Column(String(50))
    latitud = Column(Float)
    longitud = Column(Float)
    codigo_provincia = Column(String(10))
    codigo_canton = Column(String(10))
    codigo_parroquia = Column(String(10))
    
    fecha_inicio = Column(Date, nullable=False)
    fecha_fin = Column(Date)
    es_vigente = Column(Boolean, default=True)
    version = Column(Integer, default=1)
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())