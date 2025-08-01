from sqlalchemy import Column, Integer, String, DECIMAL, Date, Boolean, Text
from src.models.operational.staging.base_stg import StagingBase, TimestampMixin


class StgSemilla(StagingBase, TimestampMixin):
    """Staging table for Excel SEMILLAS sheet."""
    __tablename__ = 'stg_semilla'
    __table_args__ = {'schema': 'etl-productivo'}
    
    id = Column(Integer, primary_key=True)
    
    # Campos del Excel - SEMILLAS sheet
    numero_acta = Column(String(100), nullable=True)  # ACTAS
    organizacion = Column(String(300), nullable=True)  # ASOCIACIONES
    nombres_apellidos = Column(String(300), nullable=True)  # NOMBRES COMPLETOS
    cedula = Column(String(20), nullable=True)  # CEDULA
    telefono = Column(String(50), nullable=True)  # TELEFONO
    genero = Column(String(20), nullable=True)  # GENERO
    edad = Column(Integer, nullable=True)  # EDAD
    canton = Column(String(100), nullable=True)  # CANTON
    parroquia = Column(String(100), nullable=True)  # PARROQUIA
    localidad = Column(String(200), nullable=True)  # RECINTO, COMUNA O SECTOR
    coordenada_x = Column(String(50), nullable=True)  # X
    coordenada_y = Column(String(50), nullable=True)  # Y
    hectarias_totales = Column(DECIMAL(10, 2), nullable=True)  # HECTAREAS
    hectarias_beneficiadas = Column(DECIMAL(10, 2), nullable=True)  # HECTAREAS (mismo valor)
    entrega = Column(Integer, nullable=True)  # ENTREGA
    variedad = Column(String(100), nullable=True)  # VARIEDAD
    cultivo = Column(String(100), nullable=True)  # CULTIVO 1
    fecha_entrega = Column(Date, nullable=True)  # FECHA DE ENTREGA
    lugar_entrega = Column(String(200), nullable=True)  # LUGAR DE ENTREGA
    responsable_agencia = Column(String(200), nullable=True)  # RESPONSABLE DE AGRIPAC
    cedula_responsable = Column(String(20), nullable=True)  # CEDULA2
    precio_unitario = Column(DECIMAL(10, 2), nullable=True)  # PRECIO UNITARIO
    observacion = Column(Text, nullable=True)  # OBSERVACION
    anio = Column(Integer, nullable=True)  # AÑO
    
    # Campos legacy para compatibilidad (deprecados - se pueden eliminar después)
    documento = Column(String(100), nullable=True)
    proceso = Column(String(200), nullable=True)
    inversion = Column(DECIMAL(12, 2), nullable=True)
    cedula_jefe_sucursal = Column(String(20), nullable=True)
    sucursal = Column(String(200), nullable=True)
    fecha_retiro = Column(Date, nullable=True)
    actualizacion = Column(String(200), nullable=True)
    rubro = Column(String(100), nullable=True)
    quintil = Column(Integer, nullable=True)
    score_quintil = Column(DECIMAL(10, 4), nullable=True)
    
    # Processing fields
    processed = Column(Boolean, default=False)
    error_message = Column(Text)
    
    def __repr__(self):
        return f"<StgSemilla(id={self.id}, numero_acta='{self.numero_acta}', beneficiario='{self.nombres_apellidos}')>"