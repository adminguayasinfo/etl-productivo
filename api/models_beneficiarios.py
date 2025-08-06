# -*- coding: utf-8 -*-
"""
Modelos Pydantic para el API de beneficiarios.
"""
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum


class TipoBeneficio(str, Enum):
    """Tipos de beneficios/subsidios disponibles."""
    SEMILLAS = "SEMILLAS"
    FERTILIZANTES = "FERTILIZANTES"
    MECANIZACION = "MECANIZACION"


class TipoCultivo(str, Enum):
    """Tipos de cultivos soportados."""
    ARROZ = "ARROZ"
    MAIZ = "MAIZ"


class BeneficiariosPorSubvencion(BaseModel):
    """Información de beneficiarios únicos por tipo de subvención."""
    count: int = Field(description="Número de beneficiarios únicos")
    monto_total: float = Field(description="Monto total entregado para este tipo de subvención")


class DistribucionSubvenciones(BaseModel):
    """Distribución de beneficiarios por número de subvenciones recibidas."""
    beneficiarios_1_subvencion: int = Field(description="Beneficiarios con 1 subvención")
    beneficiarios_2_subvenciones: int = Field(description="Beneficiarios con 2 subvenciones")
    beneficiarios_3_subvenciones: int = Field(description="Beneficiarios con 3 subvenciones")


class BeneficioCultivo(BaseModel):
    """Información de beneficio por cultivo para un beneficiario."""
    cultivo: TipoCultivo
    hectareas: float
    costo_produccion_sin_subsidio: float = Field(description="Costo por hectárea según matriz")
    costo_total_sin_subsidio: float = Field(description="Costo total = hectáreas * costo_por_hectárea")


class DetalleSubsidio(BaseModel):
    """Detalle de subsidio recibido por beneficiario."""
    tipo_beneficio: TipoBeneficio
    cultivo: TipoCultivo
    monto: float
    hectareas: float


class TopBeneficiario(BaseModel):
    """Información completa de un beneficiario top."""
    beneficiario_id: int
    cedula: str = Field(description="Cédula del beneficiario")
    nombres_apellidos: str = Field(description="Nombres y apellidos completos del beneficiario")
    total_subvenciones: int = Field(description="Número total de subvenciones recibidas")
    monto_total_recibido: float = Field(description="Suma de todos los montos recibidos")
    
    # Análisis por cultivo
    cultivos_beneficiados: List[BeneficioCultivo] = Field(description="Análisis por cada cultivo")
    
    # Detalle de subsidios
    subsidios_recibidos: List[DetalleSubsidio] = Field(description="Detalle de cada subsidio")
    
    # Cálculos de reducción de costos
    costo_total_sin_subsidio: float = Field(description="Costo total de producción sin subsidios")
    ahorro_total: float = Field(description="Reducción total en costos (3.3 - 3.4)")
    porcentaje_reduccion: float = Field(description="Porcentaje de reducción en costos")


class BeneficiariosPorCanton(BaseModel):
    """Análisis de beneficiarios por cantón para gráfico de barras."""
    canton: str = Field(description="Nombre del cantón")
    total_beneficios: int = Field(description="Total de beneficios otorgados en el cantón")
    porcentaje: float = Field(description="Porcentaje del total de beneficios")


class ResumenBeneficiarios(BaseModel):
    """Resumen ejecutivo de beneficiarios."""
    total_beneficiarios_unicos: int
    total_subvenciones_otorgadas: int
    monto_total_distribuido: float
    beneficiario_con_mayor_ahorro: Dict[str, Any]
    promedio_subvenciones_por_beneficiario: float
    promedio_ahorro_por_beneficiario: float


class BeneficiariosResponse(BaseModel):
    """Respuesta completa del endpoint beneficiarios."""
    fecha_consulta: datetime = Field(default_factory=datetime.now)
    
    # 1. Beneficiarios por subvención
    beneficiarios_por_subvencion: Dict[str, BeneficiariosPorSubvencion]
    
    # 2. Distribución por número de subvenciones
    distribucion_subvenciones: DistribucionSubvenciones
    
    # 3. Top 5 beneficiarios con mayor porcentaje de ahorro
    top_beneficiarios_por_ahorro: List[TopBeneficiario] = Field(description="Top 5 ordenado por mayor % de ahorro")
    
    # 4. Top 5 beneficiarios por número de subvenciones y luego por % de ahorro
    top_beneficiarios_por_subvenciones: List[TopBeneficiario] = Field(description="Top 5 ordenado por más subvenciones, luego por % de ahorro")
    
    # 5. Análisis por cantones para gráfico de barras
    beneficiarios_por_canton: List[BeneficiariosPorCanton] = Field(description="Distribución de beneficios por cantón ordenada DESC")
    
    # Resumen ejecutivo
    resumen: ResumenBeneficiarios
    
    # Metadatos
    metadata: Dict[str, Any] = Field(
        default_factory=lambda: {
            "version": "1.0.0",
            "fuente_datos": "Base de datos ETL-Productivo",
            "matrices_costo": ["costos_arroz.py", "costos_maiz.py"],
            "notas": [
                "Mecanización para arroz usa 'Arado + Fangueo' ($200.00/ha)",
                "Mecanización para maíz usa 'Arado + Rastra' ($70.00/ha)",
                "Un beneficiario puede tener múltiples cultivos",
                "Hectáreas totales = suma de hectareas_beneficiadas de todos los beneficios",
                "Reducción de costos = Costo sin subsidio - Monto subsidios recibidos"
            ]
        }
    )


class ErrorResponse(BaseModel):
    """Respuesta de error estándar."""
    error: str
    message: str
    timestamp: datetime = Field(default_factory=datetime.now)
    details: Optional[Dict[str, Any]] = None