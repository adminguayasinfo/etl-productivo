#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modelos Pydantic para el API REST del análisis de costos de arroz.
"""

from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from datetime import datetime

class DatosBeneficio(BaseModel):
    """Datos de un tipo de beneficio específico."""
    beneficiarios: int
    hectareas: float
    inversion_gad: float
    precio_promedio_gad: float
    descripcion: str

class PreciosComparacion(BaseModel):
    """Comparación de precios GAD vs mercado."""
    precio_gad: float
    precio_mercado: float
    diferencia: float
    unidad: str

class AhorroDetalle(BaseModel):
    """Detalle de ahorro por tipo de beneficio."""
    monto: float
    porcentaje_del_total: float
    descripcion: str

class IndicadoresFinancieros(BaseModel):
    """Indicadores financieros por hectárea."""
    rendimiento_sacas: int
    precio_saca: float
    ingresos_brutos: float
    costo_produccion: float
    utilidad_sin_programa: float
    utilidad_con_programa: float
    mejora_utilidad_porcentaje: float
    ahorro_promedio_ha: float

class CoberturaPrograma(BaseModel):
    """Cobertura del programa por tipo de beneficio."""
    cobertura_porcentaje: float
    hectareas_beneficiadas: float
    hectareas_totales: float

class EficienciaInversion(BaseModel):
    """Eficiencia de la inversión del programa."""
    inversion_total: float
    ahorro_total: float
    eficiencia: float
    calificacion: str
    descripcion: str

class ResumenEjecutivo(BaseModel):
    """Resumen ejecutivo del análisis."""
    inversion_gad_total: float
    ahorro_productores_total: float
    eficiencia_completa: float
    beneficiarios_directos: int
    hectareas_impactadas: float
    mejora_utilidad_promedio: float
    calificacion: str

class AnalisisComparativo(BaseModel):
    """Comparación con análisis sin mecanización."""
    sin_mecanizacion: Dict[str, float]
    con_mecanizacion: Dict[str, float]
    mejora_absoluta: float
    mejora_relativa: float

class AnalisisCostosResponse(BaseModel):
    """Respuesta completa del análisis de costos de arroz."""
    
    # Metadatos
    fecha_analisis: datetime
    total_hectareas_arroz: float
    
    # Datos por tipo de beneficio
    beneficios: Dict[str, DatosBeneficio]
    
    # Comparación de precios
    precios: Dict[str, PreciosComparacion]
    
    # Ahorros calculados
    ahorros: Dict[str, AhorroDetalle]
    ahorro_total: float
    
    # Eficiencia
    eficiencia: EficienciaInversion
    
    # Cobertura
    cobertura: Dict[str, CoberturaPrograma]
    
    # Indicadores financieros
    indicadores_financieros: IndicadoresFinancieros
    
    # Contribución por beneficio
    contribucion_beneficios: Dict[str, float]
    
    # Resumen ejecutivo
    resumen_ejecutivo: ResumenEjecutivo
    
    # Análisis comparativo
    comparativo: AnalisisComparativo
    
    # Datos para gráficos
    graficos: Dict[str, List[Dict[str, Any]]]
    
    # Notas metodológicas
    metodologia: List[str]

class HealthResponse(BaseModel):
    """Respuesta del endpoint de health check."""
    status: str
    timestamp: datetime
    database_connection: bool
    message: str