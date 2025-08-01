"""Tabla intermedia para la relación many-to-many Beneficiario-Asociacion."""

from sqlalchemy import Column, Integer, ForeignKey, Table
from src.models.base import Base

# Tabla de asociación many-to-many
BeneficiarioAsociacion = Table(
    'beneficiario_asociacion',
    Base.metadata,
    Column('beneficiario_id', Integer, ForeignKey('etl-productivo.beneficiario.id'), primary_key=True),
    Column('asociacion_id', Integer, ForeignKey('etl-productivo.asociacion.id'), primary_key=True),
    schema='etl-productivo'
)