from sqlalchemy import Column, Integer, String, Boolean, Date, DateTime, Index
from sqlalchemy.sql import func
from src.models.base import Base


class DimCultivo(Base):
    """Dimensión de cultivos para análisis agrícola."""
    __tablename__ = 'dim_cultivo'
    __table_args__ = (
        Index('idx_dim_cultivo_codigo', 'codigo_cultivo'),
        Index('idx_dim_cultivo_familia', 'familia_botanica'),
        Index('idx_dim_cultivo_clasificacion', 'clasificacion_economica'),
        {'schema': 'etl-productivo'}
    )
    
    # Clave primaria
    cultivo_key = Column(Integer, primary_key=True, autoincrement=True)
    
    # Identificación básica
    codigo_cultivo = Column(String(10), unique=True, nullable=False)  # ARROZ, MAIZ, CACAO
    nombre_cultivo = Column(String(100), nullable=False)              # Arroz, Maíz, Cacao
    nombre_cientifico = Column(String(150), nullable=True)            # Oryza sativa, Zea mays
    
    # Clasificación botánica
    familia_botanica = Column(String(100), nullable=True)             # Poaceae, Malvaceae
    genero = Column(String(100), nullable=True)                       # Oryza, Zea, Theobroma
    
    # Características agronómicas
    tipo_ciclo = Column(String(50), nullable=True)                    # ANUAL, PERENNE, SEMESTRAL
    duracion_ciclo_dias = Column(Integer, nullable=True)              # 120, 365, 180
    estacionalidad = Column(String(100), nullable=True)               # TODO_EL_AÑO, INVIERNO, VERANO
    
    # Clasificación económica
    clasificacion_economica = Column(String(50), nullable=True)       # ALIMENTARIO, INDUSTRIAL, EXPORTACION
    uso_principal = Column(String(100), nullable=True)                # CONSUMO_HUMANO, FORRAJE, INDUSTRIAL
    
    # Requerimientos básicos
    tipo_clima = Column(String(50), nullable=True)                    # TROPICAL, SUBTROPICAL, TEMPLADO
    requerimiento_agua = Column(String(50), nullable=True)            # ALTO, MEDIO, BAJO
    tipo_suelo_preferido = Column(String(100), nullable=True)         # ARCILLOSO, FRANCO, ARENOSO
    
    # Información comercial
    epoca_siembra_principal = Column(String(50), nullable=True)       # INVIERNO, VERANO, TODO_AÑO
    epoca_cosecha_principal = Column(String(50), nullable=True)       # ENE-MAR, ABR-JUN, etc.
    
    # Metadatos de gestión
    es_vigente = Column(Boolean, default=True, nullable=False)
    fecha_inicio = Column(Date, default=func.current_date(), nullable=False)
    fecha_fin = Column(Date, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<DimCultivo(cultivo_key={self.cultivo_key}, codigo='{self.codigo_cultivo}', nombre='{self.nombre_cultivo}')>"