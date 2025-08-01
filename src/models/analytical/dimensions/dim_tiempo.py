from sqlalchemy import Column, Integer, String, Date, Boolean
from src.models.base import Base


class DimTiempo(Base):
    __tablename__ = 'dim_tiempo'
    __table_args__ = {'schema': 'etl-productivo'}
    
    tiempo_key = Column(Integer, primary_key=True, autoincrement=True)
    fecha = Column(Date, nullable=False, unique=True)
    
    dia = Column(Integer, nullable=False)
    dia_semana = Column(Integer, nullable=False)
    nombre_dia = Column(String(20), nullable=False)
    
    semana = Column(Integer, nullable=False)
    semana_anio = Column(Integer, nullable=False)
    
    mes = Column(Integer, nullable=False)
    nombre_mes = Column(String(20), nullable=False)
    mes_abrev = Column(String(10), nullable=False)
    
    trimestre = Column(Integer, nullable=False)
    nombre_trimestre = Column(String(10), nullable=False)
    
    anio = Column(Integer, nullable=False)
    anio_mes = Column(String(10), nullable=False)
    
    es_fin_semana = Column(Boolean, default=False)
    es_feriado = Column(Boolean, default=False)