# -*- coding: utf-8 -*-
"""
Modelos Pydantic para el API de beneficios-cultivos.
"""
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum


class TipoCultivo(str, Enum):
    """Tipos de cultivos soportados."""
    ARROZ = "ARROZ"
    MAIZ = "MAIZ"


class TipoBeneficio(str, Enum):
    """Tipos de beneficios/subsidios disponibles."""
    SEMILLAS = "SEMILLAS"
    FERTILIZANTES = "FERTILIZANTES"
    MECANIZACION = "MECANIZACION"


class HectareasSubsidiadas(BaseModel):
    """Información de hectáreas subsidiadas por cultivo y beneficio."""
    cultivo: TipoCultivo
    tipo_beneficio: TipoBeneficio
    total_hectareas: float = Field(description="Total de hectáreas beneficiadas")
    num_beneficios: int = Field(description="Número de beneficios otorgados")


class CostoProduccion(BaseModel):
    """Costo de producción por hectárea según matriz de costos."""
    cultivo: TipoCultivo
    costo_total_sin_subsidio: float = Field(description="Costo total por hectárea sin subsidio")
    costos_directos: float = Field(description="Subtotal costos directos")
    costos_indirectos: float = Field(description="Subtotal costos indirectos")
    desglose_por_categoria: Dict[str, float] = Field(description="Desglose de costos por categoría")


class MontoSubsidio(BaseModel):
    """Monto total de subsidios entregados."""
    cultivo: TipoCultivo
    tipo_beneficio: TipoBeneficio
    monto_total_entregado: float = Field(description="Monto total entregado por la base de datos")
    monto_matriz_por_hectarea: float = Field(description="Costo por hectárea según matriz")
    num_beneficios: int = Field(description="Número de beneficios otorgados")


class ReduccionCostos(BaseModel):
    """Reducción en costos de producción por subsidios."""
    cultivo: TipoCultivo
    costo_produccion_sin_subsidio: float
    reduccion_por_subsidios: float = Field(description="Reducción total por subsidios aplicados")
    costo_produccion_con_subsidio: float = Field(description="Costo final después de aplicar subsidios")
    porcentaje_reduccion: float = Field(description="Porcentaje de reducción en costos")
    desglose_reducciones: Dict[str, float] = Field(description="Reducción por tipo de beneficio")


class FiltrosBeneficios(BaseModel):
    """Datos para aplicar filtros en el frontend."""
    cultivos_disponibles: List[TipoCultivo]
    beneficios_disponibles: List[TipoBeneficio]
    combinaciones_disponibles: List[Dict[str, str]] = Field(
        description="Combinaciones válidas de cultivo-beneficio disponibles en los datos"
    )


class ResumenEjecutivo(BaseModel):
    """Resumen ejecutivo de todos los subsidios."""
    total_hectareas_impactadas: float
    total_beneficios_otorgados: int
    inversion_total_gad: float
    ahorro_total_productores: float
    cultivos_mas_subsidiados: List[Dict[str, Any]]
    beneficios_mas_utilizados: List[Dict[str, Any]]


class BeneficiosCultivosResponse(BaseModel):
    """Respuesta completa del endpoint beneficios-cultivos."""
    fecha_consulta: datetime = Field(default_factory=datetime.now)
    
    # 1. Hectáreas subsidiadas
    hectareas_subsidiadas: List[HectareasSubsidiadas]
    
    # 2. Costos de producción sin subsidios
    costos_produccion: List[CostoProduccion]
    
    # 3. Montos totales de subsidios
    montos_subsidios: List[MontoSubsidio]
    
    # 4. Reducción en costos
    reduccion_costos: List[ReduccionCostos]
    
    # 5. Filtros disponibles
    filtros: FiltrosBeneficios
    
    # 6. Resumen ejecutivo
    resumen: ResumenEjecutivo
    
    # Metadatos
    metadata: Dict[str, Any] = Field(
        default_factory=lambda: {
            "version": "1.0.0",
            "fuente_datos": "Base de datos ETL-Productivo",
            "matrices_costo": ["costos_arroz.py", "costos_maiz.py"],
            "notas": [
                "MANO_OBRA no se mapea con ningún beneficio-subvención",
                "Mecanización usa solo el item específico de arado por cultivo",
                "Costos indirectos no se recalculan dinámicamente en esta versión"
            ]
        }
    )


class ErrorResponse(BaseModel):
    """Respuesta de error estándar."""
    error: str
    message: str
    timestamp: datetime = Field(default_factory=datetime.now)
    details: Optional[Dict[str, Any]] = None