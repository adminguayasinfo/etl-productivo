"""Modelo de Beneficiario para la capa operational refactorizada."""

from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship
from datetime import date
from src.models.base import Base
from .beneficiario_asociacion import BeneficiarioAsociacion


class Beneficiario(Base):
    """Modelo para beneficiarios (personas que reciben beneficios)."""
    __tablename__ = 'beneficiario'
    __table_args__ = {'schema': 'etl-productivo'}
    
    id = Column(Integer, primary_key=True)
    cedula = Column(String(20), nullable=False, unique=True)
    nombres_completos = Column(String(300), nullable=False)
    telefono = Column(String(50), nullable=True)
    genero = Column(String(20), nullable=True)
    fecha_nacimiento = Column(Date, nullable=True)
    
    # Relación con Direccion
    direccion_id = Column(Integer, ForeignKey('etl-productivo.direccion.id'), nullable=True)
    direccion = relationship("Direccion", backref="beneficiarios")
    
    # Relación many-to-many con Asociacion
    asociaciones = relationship(
        "Asociacion",
        secondary=BeneficiarioAsociacion,
        back_populates="beneficiarios"
    )
    
    # Relación con Beneficios
    beneficios = relationship("Beneficio", back_populates="beneficiario")
    
    def __repr__(self):
        return (f"<Beneficiario(id={self.id}, cedula='{self.cedula}', "
                f"nombres='{self.nombres_completos}')>")
    
    @classmethod
    def calcular_fecha_nacimiento(cls, edad, anio_beneficio):
        """Calcula la fecha de nacimiento basada en la edad y año del beneficio."""
        if not edad or not anio_beneficio:
            return None
        
        anio_nacimiento = anio_beneficio - edad
        return date(anio_nacimiento, 1, 1)
    
    @classmethod
    def get_or_create_by_cedula(cls, session, cedula, nombres_completos, telefono, genero, edad, anio_beneficio, direccion):
        """Busca o crea un beneficiario por cédula."""
        if not cedula or cedula.strip() == '':
            return None
            
        cedula = cedula.strip()
        
        # Buscar beneficiario existente
        beneficiario = session.query(cls).filter_by(cedula=cedula).first()
        
        if not beneficiario:
            # Calcular fecha de nacimiento
            fecha_nacimiento = cls.calcular_fecha_nacimiento(edad, anio_beneficio)
            
            # Crear nuevo beneficiario
            beneficiario = cls(
                cedula=cedula,
                nombres_completos=nombres_completos,
                telefono=telefono,
                genero=genero,
                fecha_nacimiento=fecha_nacimiento,
                direccion=direccion
            )
            session.add(beneficiario)
            session.flush()  # Para obtener el ID
        
        return beneficiario