"""Modelo de staging para fertilizantes actualizado para Excel."""
from sqlalchemy import Column, Integer, String, DECIMAL, Date, Text, Boolean
from src.models.operational.staging.base_stg import StagingBase, TimestampMixin


class StgFertilizante(StagingBase, TimestampMixin):
    """Tabla de staging para datos de fertilizantes desde Excel."""
    __tablename__ = 'stg_fertilizante'
    __table_args__ = {'schema': 'etl-productivo'}
    
    id = Column(Integer, primary_key=True)
    
    # Campos del Excel FERTILIZANTES (en orden)
    fecha_entrega = Column(Date, nullable=True)  # FECHA DE ENTREGA
    asociaciones = Column(String(500), nullable=True)  # ASOCIACIONES
    nombres_apellidos = Column(String(300), nullable=False)  # APELLIDOS Y NOMBRES
    cedula = Column(String(20), nullable=True)  # CEDULA
    telefono = Column(String(30), nullable=True)  # TELEFONO
    genero = Column(String(20), nullable=True)  # GENERO
    edad = Column(Integer, nullable=True)  # EDAD
    canton = Column(String(50), nullable=True)  # CANTON
    parroquia = Column(String(50), nullable=True)  # PARROQUIA
    recinto = Column(String(100), nullable=True)  # RECINTO, COMUNA O SECTOR
    coord_x = Column(String(50), nullable=True)  # X
    coord_y = Column(String(50), nullable=True)  # Y
    hectareas = Column(DECIMAL(10, 2), nullable=True)  # HECTAREAS
    fertilizante_nitrogenado = Column(Integer, nullable=True)  # FERTILIZANTE NITROGENADO
    npk_elementos_menores = Column(Integer, nullable=True)  # (N-P-K) + ELEMENTOS MENORES
    organico_foliar = Column(Integer, nullable=True)  # ORGANICO FOLIAR
    cultivo = Column(String(100), nullable=True)  # CULTIVO
    precio_kit = Column(DECIMAL(10, 2), nullable=True)  # Precio_Kit
    lugar_entrega = Column(String(200), nullable=True)  # LUGAR DE ENTREGA
    observacion = Column(Text, nullable=True)  # OBSERVACION
    anio = Column(Integer, nullable=False)  # AÑO
    
    # Processing fields
    processed = Column(Boolean, default=False)
    error_message = Column(Text)
    
    def __repr__(self):
        return f"<StgFertilizante(id={self.id}, nombres='{self.nombres_apellidos}', cultivo='{self.cultivo}')>"