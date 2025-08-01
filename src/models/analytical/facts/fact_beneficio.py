from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey, Index
from sqlalchemy.sql import func
from src.models.base import Base


class FactBeneficio(Base):
    __tablename__ = 'fact_beneficio'
    __table_args__ = (
        Index('idx_fact_beneficio_cultivo', 'cultivo_key'),
        Index('idx_fact_beneficio_tipo_cultivo', 'tipo_beneficio', 'cultivo_key'),
        Index('idx_fact_beneficio_tiempo_cultivo', 'tiempo_key', 'cultivo_key'),
        {'schema': 'etl-productivo'}
    )
    
    beneficio_key = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign keys a dimensiones
    persona_key = Column(Integer, ForeignKey('etl-productivo.dim_persona.persona_key'), nullable=False)
    ubicacion_key = Column(Integer, ForeignKey('etl-productivo.dim_ubicacion.ubicacion_key'), nullable=False)
    organizacion_key = Column(Integer, ForeignKey('etl-productivo.dim_organizacion.organizacion_key'))
    tiempo_key = Column(Integer, ForeignKey('etl-productivo.dim_tiempo.tiempo_key'), nullable=False)
    cultivo_key = Column(Integer, ForeignKey('etl-productivo.dim_cultivo.cultivo_key'), nullable=False)  # Nueva dimensión
    
    # Identificación del beneficio original
    beneficio_id = Column(Integer, nullable=False)
    tipo_beneficio = Column(String(100), nullable=False)  # SEMILLAS, FERTILIZANTES
    
    # Categorización (mantener por compatibilidad, eventualmente deprecar)
    categoria_beneficio = Column(String(100))  # Redundante con dim_cultivo, mantener para migración
    
    # Métricas
    hectarias_sembradas = Column(Float)
    valor_monetario = Column(Float)  # Valor en dólares del beneficio
    
    # Tiempo (desnormalizado para performance)
    fecha_beneficio = Column(Date, nullable=False)
    anio = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    trimestre = Column(Integer, nullable=False)
    
    # Metadatos
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    def __repr__(self):
        return f"<FactBeneficio(beneficio_key={self.beneficio_key}, tipo='{self.tipo_beneficio}', cultivo_key={self.cultivo_key})>"