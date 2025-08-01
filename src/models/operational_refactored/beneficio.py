"""Modelo de Beneficio (supertipo) para la capa operational refactorizada."""

from sqlalchemy import Column, Integer, String, DECIMAL, Date, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from src.models.base import Base


class Beneficio(Base):
    """Modelo base para beneficios (supertipo)."""
    __tablename__ = 'beneficio'
    __table_args__ = {'schema': 'etl-productivo'}
    
    id = Column(Integer, primary_key=True)
    fecha_entrega = Column(Date, nullable=True)
    tipo_beneficio = Column(String(100), nullable=False)  # 'SEMILLAS', 'FERTILIZANTE', etc.
    monto = Column(DECIMAL(12, 2), nullable=True)
    
    # Relación con Beneficiario
    beneficiario_id = Column(Integer, ForeignKey('etl-productivo.beneficiario.id'), nullable=False)
    beneficiario = relationship("Beneficiario", back_populates="beneficios")
    
    # Relación con TipoCultivo
    tipo_cultivo_id = Column(Integer, ForeignKey('etl-productivo.tipo_cultivo.id'), nullable=True)
    tipo_cultivo = relationship("TipoCultivo")
    
    # Campos adicionales para compatibilidad
    acta = Column(String(100), nullable=True)
    hectareas_beneficiadas = Column(DECIMAL(10, 2), nullable=True)
    lugar_entrega = Column(String(200), nullable=True)
    observaciones = Column(String(500), nullable=True)
    anio_beneficio = Column(Integer, nullable=True)
    
    # Campos de auditoría
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Discriminator para herencia
    discriminator = Column(String(50))
    
    # Configuración para joined table inheritance
    __mapper_args__ = {
        'polymorphic_identity': 'base',
        'polymorphic_on': discriminator,
    }
    
    def __repr__(self):
        return (f"<Beneficio(id={self.id}, tipo='{self.tipo_beneficio}', "
                f"beneficiario_id={self.beneficiario_id}, monto={self.monto})>")