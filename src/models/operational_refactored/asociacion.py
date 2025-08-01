"""Modelo de Asociacion para la capa operational refactorizada."""

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from src.models.base import Base
from .beneficiario_asociacion import BeneficiarioAsociacion


class Asociacion(Base):
    """Modelo para asociaciones de agricultores."""
    __tablename__ = 'asociacion'
    __table_args__ = {'schema': 'etl-productivo'}
    
    id = Column(Integer, primary_key=True)
    nombre = Column(String(300), nullable=False, unique=True)
    
    # Relación many-to-many con Beneficiario
    beneficiarios = relationship(
        "Beneficiario",
        secondary=BeneficiarioAsociacion,
        back_populates="asociaciones"
    )
    
    def __repr__(self):
        return f"<Asociacion(id={self.id}, nombre='{self.nombre}')>"
    
    @classmethod
    def get_or_create_by_name(cls, session, nombre):
        """Busca o crea una asociación por nombre."""
        if not nombre or nombre.strip() == '':
            return None
            
        nombre = nombre.strip()
        
        # Buscar asociación existente
        asociacion = session.query(cls).filter_by(nombre=nombre).first()
        
        if not asociacion:
            # Crear nueva asociación
            asociacion = cls(nombre=nombre)
            session.add(asociacion)
            session.flush()  # Para obtener el ID
        
        return asociacion