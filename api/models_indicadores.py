#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modelos Pydantic para el endpoint de indicadores productivos.
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


class IndicadorReduccionCostos(BaseModel):
    """Modelo para el indicador de reducción de costos de producción."""
    
    porcentaje_promedio_reduccion: float = Field(
        ..., 
        description="Porcentaje promedio ponderado de reducción de costos de producción",
        example=15.75
    )
    
    total_beneficiarios: int = Field(
        ..., 
        description="Total de beneficiarios incluidos en el cálculo",
        example=2847
    )
    
    hectareas_totales: float = Field(
        ..., 
        description="Total de hectáreas incluidas en el análisis",
        example=12543.50
    )
    
    monto_total_beneficios: float = Field(
        ..., 
        description="Monto total de beneficios considerados",
        example=2456789.75
    )
    
    costo_total_sin_subsidios: float = Field(
        ..., 
        description="Costo total de producción sin considerar subsidios",
        example=15678234.25
    )
    
    # NUEVOS CAMPOS DE RENTABILIDAD
    monto_rentabilidad_sin_subvencion: float = Field(
        ..., 
        description="Utilidad total sin considerar subsidios (Ingresos - Costos)",
        example=8765432.10
    )
    
    monto_rentabilidad_con_subvenciones: float = Field(
        ..., 
        description="Utilidad total considerando subsidios como ahorro adicional",
        example=11222221.85
    )
    
    porcentaje_incremento_rentabilidad: float = Field(
        ..., 
        description="Porcentaje de incremento en rentabilidad debido a subsidios",
        example=28.05
    )
    
    metodologia: str = Field(
        default="Promedio ponderado por hectáreas y monto de beneficios",
        description="Metodología utilizada para el cálculo"
    )


class ProductivoIndicadoresResponse(BaseModel):
    """Modelo de respuesta para el endpoint de indicadores productivos."""
    
    fecha_calculo: datetime = Field(
        default_factory=datetime.now,
        description="Fecha y hora del cálculo"
    )
    
    indicador_reduccion_costos: IndicadorReduccionCostos = Field(
        ...,
        description="Indicador principal: porcentaje promedio de reducción de costos"
    )
    
    observaciones: Optional[str] = Field(
        None,
        description="Observaciones adicionales sobre el cálculo"
    )


class ErrorResponse(BaseModel):
    """Modelo de respuesta para errores."""
    
    error: str = Field(..., description="Tipo de error")
    message: str = Field(..., description="Mensaje de error detallado")
    timestamp: datetime = Field(default_factory=datetime.now)