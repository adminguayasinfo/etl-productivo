#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API REST para análisis de costos de producción de arroz.
FastAPI servidor con endpoints para consumo desde frontend.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
from datetime import datetime
from api.models import AnalisisCostosResponse, HealthResponse
from api.services import AnalisisCostosService, HealthService
from api.models_subsidios import BeneficiosCultivosResponse, ErrorResponse
from api.services_subsidios import BeneficiosCultivosService
from api.models_beneficiarios import BeneficiariosResponse
from api.services_beneficiarios import BeneficiariosService
from api.models_indicadores import ProductivoIndicadoresResponse
from api.services_indicadores import IndicadoresService

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Crear aplicación FastAPI
app = FastAPI(
    title="ETL Productivo - Análisis de Costos de Arroz",
    description="API REST para análisis de costos de producción de arroz con beneficios GAD",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS (deshabilitado según requisitos)
# Si necesitas habilitarlo más adelante, descomenta las siguientes líneas:
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["GET", "POST", "PUT", "DELETE"],
#     allow_headers=["*"],
# )

# Instanciar servicios
analisis_service = AnalisisCostosService()
health_service = HealthService()
beneficios_service = BeneficiosCultivosService()
beneficiarios_service = BeneficiariosService()
indicadores_service = IndicadoresService()

@app.get("/", response_model=dict)
async def root():
    """Endpoint raíz del API."""
    return {
        "message": "ETL Productivo - API de Análisis de Costos de Arroz",
        "version": "1.0.0",
        "timestamp": datetime.now(),
        "endpoints": {
            "analisis_completo": "/analisis-costos-arroz",
            "beneficios_cultivos": "/produ/beneficios-cultivos",
            "beneficiarios": "/produ/beneficiarios",
            "indicadores_productivos": "/productivo-indicadores",
            "resumen": "/resumen",
            "graficos": "/graficos",
            "health_check": "/health",
            "documentacion": "/docs"
        }
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Endpoint de health check para verificar el estado del API."""
    try:
        health_status = health_service.get_health_status()
        
        if health_status.status == "healthy":
            return health_status
        else:
            raise HTTPException(status_code=503, detail="Service Unavailable")
            
    except Exception as e:
        logger.error(f"Error en health check: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error interno del servidor: {str(e)}"
        )

@app.get("/analisis-costos-arroz", response_model=AnalisisCostosResponse)
async def obtener_analisis_costos_arroz():
    """
    Endpoint principal que retorna el análisis completo de costos de producción de arroz.
    
    Este endpoint realiza:
    - Extracción de datos reales desde la base de datos
    - Cálculo de ahorros por tipo de beneficio (semillas, fertilizantes, mecanización)
    - Análisis de eficiencia de la inversión del GAD
    - Indicadores financieros por hectárea
    - Datos estructurados para gráficos del frontend
    
    Returns:
        AnalisisCostosResponse: Análisis completo con todos los datos y métricas
    """
    try:
        logger.info("Iniciando análisis de costos de arroz...")
        
        # Realizar análisis completo
        resultado = analisis_service.realizar_analisis_completo()
        
        logger.info(f"Análisis completado exitosamente. Eficiencia: {resultado.eficiencia.eficiencia:.2f}x")
        
        return resultado
        
    except Exception as e:
        logger.error(f"Error al realizar análisis de costos: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al realizar el análisis: {str(e)}"
        )

@app.get("/resumen", response_model=dict)
async def obtener_resumen_ejecutivo():
    """
    Endpoint que retorna solo el resumen ejecutivo del análisis.
    Útil para dashboards que necesitan métricas clave sin toda la información detallada.
    """
    try:
        logger.info("Obteniendo resumen ejecutivo...")
        
        # Realizar análisis y extraer solo el resumen
        resultado = analisis_service.realizar_analisis_completo()
        
        resumen = {
            "fecha_analisis": resultado.fecha_analisis,
            "resumen_ejecutivo": resultado.resumen_ejecutivo,
            "eficiencia": resultado.eficiencia,
            "ahorro_total": resultado.ahorro_total,
            "indicadores_clave": {
                "inversion_total": resultado.resumen_ejecutivo.inversion_gad_total,
                "beneficiarios_directos": resultado.resumen_ejecutivo.beneficiarios_directos,
                "hectareas_impactadas": resultado.resumen_ejecutivo.hectareas_impactadas,
                "mejora_utilidad": resultado.resumen_ejecutivo.mejora_utilidad_promedio
            }
        }
        
        return resumen
        
    except Exception as e:
        logger.error(f"Error al obtener resumen ejecutivo: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener el resumen: {str(e)}"
        )

@app.get("/graficos", response_model=dict)
async def obtener_datos_graficos():
    """
    Endpoint que retorna solo los datos estructurados para gráficos.
    Optimizado para frontends que necesitan datos específicos para visualizaciones.
    """
    try:
        logger.info("Obteniendo datos para gráficos...")
        
        # Realizar análisis y extraer solo los datos de gráficos
        resultado = analisis_service.realizar_analisis_completo()
        
        return {
            "fecha_analisis": resultado.fecha_analisis,
            "graficos": resultado.graficos,
            "contribucion_beneficios": resultado.contribucion_beneficios,
            "comparativo": resultado.comparativo
        }
        
    except Exception as e:
        logger.error(f"Error al obtener datos de gráficos: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de gráficos: {str(e)}"
        )

@app.get("/produ/beneficios-cultivos", response_model=BeneficiosCultivosResponse)
async def obtener_beneficios_cultivos(
    cultivo: str = None,
    beneficio: str = None
):
    """
    Endpoint principal de beneficios-cultivos que proporciona:
    
    1. Hectáreas de cultivo de ARROZ y MAÍZ beneficiadas por tipo de subvención:
       - Hectáreas con semillas
       - Hectáreas con fertilizantes  
       - Hectáreas con mecanizado
    
    2. Costo de producción SIN CONSIDERAR los beneficios/subvención:
       - Usa matrices de costos de arroz y maíz
       - Calculado por hectárea
    
    3. Monto total de beneficios/subvenciones entregados por tipo:
       - Semillas
       - Fertilizantes
       - Mecanizado (usa valores específicos de matriz)
    
    4. Reducción TOTAL en costos de producción CONSIDERANDO subsidios:
       - Diferencia entre costo sin subsidio vs monto de subsidios entregados
    
    Query Parameters:
        cultivo (str, optional): Filtrar por tipo de cultivo ('ARROZ', 'MAIZ')
        beneficio (str, optional): Filtrar por tipo de subvención ('SEMILLAS', 'FERTILIZANTES', 'MECANIZACION')
    
    Returns:
        BeneficiosCultivosResponse: Análisis completo con filtros aplicables
    """
    try:
        logger.info(f"Iniciando análisis de beneficios-cultivos. Filtros: cultivo={cultivo}, beneficio={beneficio}")
        
        # Validar filtros si se proporcionan
        if cultivo and cultivo not in ['ARROZ', 'MAIZ']:
            raise HTTPException(
                status_code=400,
                detail="Cultivo debe ser 'ARROZ' o 'MAIZ'"
            )
        
        if beneficio and beneficio not in ['SEMILLAS', 'FERTILIZANTES', 'MECANIZACION']:
            raise HTTPException(
                status_code=400,
                detail="Beneficio debe ser 'SEMILLAS', 'FERTILIZANTES' o 'MECANIZACION'"
            )
        
        # Realizar análisis completo
        resultado = beneficios_service.obtener_analisis_completo(
            filtro_cultivo=cultivo,
            filtro_beneficio=beneficio
        )
        
        logger.info(f"Análisis completado. Total hectáreas: {resultado.resumen.total_hectareas_impactadas:.2f}")
        
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener beneficios-cultivos: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error interno al procesar beneficios-cultivos: {str(e)}"
        )

@app.get("/produ/beneficiarios", response_model=BeneficiariosResponse)
async def obtener_beneficiarios():
    """
    Endpoint de análisis de beneficiarios que proporciona:
    
    1. Beneficiarios por subvención:
       - Cuántos beneficiarios únicos han recibido semillas y monto total
       - Cuántos beneficiarios únicos han recibido fertilizantes y monto total  
       - Cuántos beneficiarios únicos han recibido mecanizado y monto total
       
    2. Número de subvenciones por beneficiario:
       - Cantidad de beneficiarios que han recibido 1 subvención
       - Cantidad de beneficiarios que han recibido 2 subvenciones
       - Cantidad de beneficiarios que han recibido 3 subvenciones
       
    3. Top 5 beneficiarios con mayor porcentaje de ahorro:
       - Ordenados por mayor porcentaje de reducción de costos
       - Información personal: cédula y nombres completos
       - Análisis completo con cultivos beneficiados
       - Cálculo de costos de producción sin beneficios
       - Monto total de beneficios recibidos por agricultor
       - Reducción total de costos (costo sin subsidio - monto subsidios)
       
    4. Top 5 beneficiarios por número de subvenciones y ahorro:
       - Ordenados primero por mayor número de subvenciones
       - Desempate por mayor porcentaje de ahorro
       - Información personal completa incluida
       - Análisis detallado de efectividad de subsidios múltiples
       
    5. Distribución de beneficios por cantón:
       - Total de beneficios otorgados por cantón (todos los tipos y cultivos)
       - Porcentaje de participación de cada cantón
       - Ordenado por mayor cantidad de beneficios
       - Formato optimizado para gráfico de barras (Eje X: Cantones, Eje Y: Cantidad)
    
    Notas técnicas:
    - Mecanización para arroz usa "Arado + Fangueo" ($200.00/ha)
    - Mecanización para maíz usa "Arado + Rastra" ($70.00/ha)
    - Un beneficiario puede tener beneficios en múltiples cultivos
    - Hectáreas totales = suma de hectareas_beneficiadas de todos los beneficios
    
    Returns:
        BeneficiariosResponse: Análisis completo de beneficiarios con reducción de costos
    """
    try:
        logger.info("Iniciando análisis de beneficiarios...")
        
        # Realizar análisis completo
        resultado = beneficiarios_service.obtener_analisis_completo()
        
        logger.info(f"Análisis completado. Total beneficiarios únicos: {resultado.resumen.total_beneficiarios_unicos}")
        
        return resultado
        
    except Exception as e:
        logger.error(f"Error al obtener análisis de beneficiarios: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error interno al procesar análisis de beneficiarios: {str(e)}"
        )

@app.get("/productivo-indicadores", response_model=ProductivoIndicadoresResponse)
async def obtener_indicadores_productivos():
    """
    Endpoint de indicadores productivos que proporciona:
    
    **Indicador 1: Porcentaje promedio de reducción de costos de producción**
    
    Metodología:
    - Calcula el promedio ponderado de reducción de costos para TODOS los beneficiarios
    - Ponderación: hectáreas totales × monto total de beneficios por beneficiario
    - Fórmula por beneficiario: (monto_beneficios / costo_sin_subsidios) × 100
    - Promedio ponderado: Σ(% reducción × peso) / Σ(peso)
    
    El cálculo considera:
    - **Hectáreas totales**: Suma de hectáreas beneficiadas por agricultor
    - **Costo sin subsidios**: Hectáreas × costo matriz por cultivo (ARROZ/MAÍZ)
    - **Monto beneficios**: Suma de todos los beneficios recibidos por agricultor
      - SEMILLAS: Monto directo del beneficio
      - FERTILIZANTES: Monto directo del beneficio
      - MECANIZACIÓN: Hectáreas × tarifa específica (ARROZ: $200/ha, MAÍZ: $70/ha)
    
    **Indicadores de Rentabilidad (NUEVOS):**
    
    **Indicador 2: Monto de rentabilidad sin subvención**
    - Utilidad total que obtendrían los beneficiarios SIN considerar subsidios
    - Fórmula: (Ingresos por hectárea - Costos por hectárea) × Hectáreas totales
    - Ingresos ARROZ: $2,130.00/ha (60 sacas × $35.50), utilidad: $539.01/ha
    - Ingresos MAÍZ: $3,440.00/ha (200 sacas × $17.20), utilidad: $1,604.70/ha
    
    **Indicador 3: Monto de rentabilidad con subvenciones**
    - Utilidad total considerando que los subsidios se suman como ahorro adicional
    - Fórmula: Rentabilidad sin subsidio + Monto total de beneficios recibidos
    
    **Indicador 4: Porcentaje de incremento en rentabilidad**
    - Porcentaje de incremento debido a los subsidios del GAD
    - Fórmula: ((Rentabilidad con subsidios - Rentabilidad sin subsidios) / Rentabilidad sin subsidios) × 100
    - Equivale a: (Monto beneficios / Rentabilidad sin subsidios) × 100
    
    Datos técnicos:
    - Matriz ARROZ: $1,590.99/ha (costo total de producción)
    - Matriz MAÍZ: $1,835.30/ha (costo total de producción)
    - Mecanización ARROZ: "Arado + Fangueo" ($200.00/ha)
    - Mecanización MAÍZ: "Arado + Rastra" ($70.00/ha)
    
    Returns:
        ProductivoIndicadoresResponse: Indicadores de reducción de costos y rentabilidad agregados
    """
    try:
        logger.info("Iniciando cálculo de indicadores productivos...")
        
        # Realizar análisis completo
        resultado = indicadores_service.obtener_indicadores_completos()
        
        logger.info(f"Indicadores calculados. Reducción promedio: {resultado.indicador_reduccion_costos.porcentaje_promedio_reduccion:.2f}%")
        
        return resultado
        
    except Exception as e:
        logger.error(f"Error al obtener indicadores productivos: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error interno al procesar indicadores productivos: {str(e)}"
        )

# Manejador de errores global
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Manejador global de excepciones."""
    logger.error(f"Error no manejado: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Error interno del servidor",
            "message": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    logger.info("Iniciando servidor API REST...")
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )