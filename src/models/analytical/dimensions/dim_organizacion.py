from sqlalchemy import Column, Integer, String, Date, DateTime, Boolean
from sqlalchemy.sql import func
from src.models.base import Base


class DimOrganizacion(Base):
    __tablename__ = 'dim_organizacion'
    __table_args__ = {'schema': 'etl-productivo'}
    
    organizacion_key = Column(Integer, primary_key=True, autoincrement=True)
    organizacion_id = Column(Integer, nullable=False, unique=True)
    nombre = Column(String(200), nullable=False)
    tipo = Column(String(100))
    categoria = Column(String(100))
    estado = Column(String(50))
    
    fecha_inicio = Column(Date, nullable=False)
    fecha_fin = Column(Date)
    es_vigente = Column(Boolean, default=True)
    version = Column(Integer, default=1)
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())