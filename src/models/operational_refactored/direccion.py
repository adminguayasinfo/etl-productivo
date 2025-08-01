"""Modelo de Direccion para la capa operational refactorizada."""

from sqlalchemy import Column, Integer, String, DECIMAL
from src.models.base import Base


class Direccion(Base):
    """Modelo para direcciones geográficas."""
    __tablename__ = 'direccion'
    __table_args__ = {'schema': 'etl-productivo'}
    
    id = Column(Integer, primary_key=True)
    provincia = Column(String(50), nullable=False, default='GUAYAS')
    canton = Column(String(100), nullable=True)
    parroquia = Column(String(100), nullable=True)
    recinto_comuna_sector = Column(String(200), nullable=True)
    coordenada_x = Column(String(50), nullable=True)
    coordenada_y = Column(String(50), nullable=True)
    
    def __repr__(self):
        return (f"<Direccion(id={self.id}, provincia='{self.provincia}', "
                f"canton='{self.canton}', parroquia='{self.parroquia}')>")
    
    @classmethod
    def get_or_create_by_location(cls, session, canton, parroquia, recinto, coord_x, coord_y):
        """Busca o crea una dirección basada en la ubicación geográfica."""
        # Buscar dirección existente
        direccion = session.query(cls).filter_by(
            canton=canton,
            parroquia=parroquia,
            recinto_comuna_sector=recinto,
            coordenada_x=coord_x,
            coordenada_y=coord_y
        ).first()
        
        if not direccion:
            # Crear nueva dirección
            direccion = cls(
                provincia='GUAYAS',
                canton=canton,
                parroquia=parroquia,
                recinto_comuna_sector=recinto,
                coordenada_x=coord_x,
                coordenada_y=coord_y
            )
            session.add(direccion)
            session.flush()  # Para obtener el ID
        
        return direccion