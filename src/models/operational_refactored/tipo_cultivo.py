"""Modelo de TipoCultivo para la capa operational refactorizada."""

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from src.models.base import Base


class TipoCultivo(Base):
    """Catálogo de tipos de cultivos."""
    __tablename__ = 'tipo_cultivo'
    __table_args__ = {'schema': 'etl-productivo'}
    
    id = Column(Integer, primary_key=True)
    nombre = Column(String(100), nullable=False, unique=True)
    
    # Relaciones con beneficios
    beneficios = relationship("Beneficio", back_populates="tipo_cultivo")
    
    def __repr__(self):
        return f"<TipoCultivo(id={self.id}, nombre='{self.nombre}')>"
    
    @classmethod
    def get_or_create_by_name(cls, session, nombre):
        """Busca o crea un tipo de cultivo por nombre."""
        if not nombre or nombre.strip() == '':
            return None
            
        nombre = nombre.strip().upper()  # Estandarizar a mayúsculas
        
        # Buscar tipo cultivo existente
        tipo_cultivo = session.query(cls).filter_by(nombre=nombre).first()
        
        if not tipo_cultivo:
            # Crear nuevo tipo de cultivo
            tipo_cultivo = cls(nombre=nombre)
            session.add(tipo_cultivo)
            session.flush()  # Para obtener el ID
        
        return tipo_cultivo