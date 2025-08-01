#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Servicios para el API REST del análisis de costos de arroz.
"""

from datetime import datetime
from typing import Dict, List, Any
from config.connections.database import db_connection
from sqlalchemy import text
from src.matriz_costos.costos_arroz import MatrizCostosArroz
from api.models import (
    AnalisisCostosResponse, DatosBeneficio, PreciosComparacion, 
    AhorroDetalle, IndicadoresFinancieros, CoberturaPrograma,
    EficienciaInversion, ResumenEjecutivo, AnalisisComparativo,
    HealthResponse
)

class AnalisisCostosService:
    """Servicio para realizar el análisis completo de costos de arroz."""
    
    def __init__(self):
        self.matriz = MatrizCostosArroz()
    
    def extraer_datos_reales(self) -> Dict[str, Any]:
        """Extrae todos los datos reales de la base de datos."""
        
        with db_connection.get_session() as session:
            # 1. Datos reales de semillas
            query_semillas = text('''
                SELECT 
                    COUNT(*) as beneficiarios,
                    SUM(COALESCE(hectarias_beneficiadas, 0)) as hectareas_semillas,
                    SUM(COALESCE(precio_unitario, 0) * COALESCE(entrega, 0)) as inversion_gad_semillas,
                    SUM(COALESCE(entrega, 0)) as quintales_entregados,
                    AVG(COALESCE(precio_unitario, 0)) as precio_promedio_gad
                FROM "etl-productivo".stg_semilla
                WHERE processed = true 
                AND UPPER(TRIM(cultivo)) = 'ARROZ'
                AND precio_unitario > 0
            ''')
            
            semillas = session.execute(query_semillas).fetchone()
            
            # 2. Datos reales de fertilizantes
            query_fertilizantes = text('''
                SELECT 
                    COUNT(*) as beneficiarios,
                    SUM(COALESCE(hectareas, 0)) as hectareas_fertilizantes,
                    SUM(COALESCE(precio_kit, 0)) as inversion_gad_fertilizantes,
                    AVG(COALESCE(precio_kit, 0)) as precio_promedio_kit_gad
                FROM "etl-productivo".stg_fertilizante
                WHERE processed = true 
                AND UPPER(TRIM(cultivo)) = 'ARROZ'
                AND precio_kit > 0
            ''')
            
            fertilizantes = session.execute(query_fertilizantes).fetchone()
            
            # 3. Datos reales de mecanización
            query_mecanizacion = text('''
                SELECT 
                    COUNT(*) as beneficiarios,
                    SUM(COALESCE(hectareas_beneficiadas, 0)) as hectareas_mecanizacion
                FROM "etl-productivo".stg_mecanizacion
                WHERE processed = true 
                AND UPPER(TRIM(cultivo)) = 'ARROZ'
            ''')
            
            mecanizacion = session.execute(query_mecanizacion).fetchone()
            
            # 4. Total hectáreas arroz
            query_total_hectareas = text('''
                SELECT SUM(COALESCE(hectarias_beneficiadas, 0)) as total_hectareas
                FROM "etl-productivo".stg_semilla 
                WHERE processed = true AND UPPER(TRIM(cultivo)) = 'ARROZ'
            ''')
            
            total_ha = session.execute(query_total_hectareas).fetchone()
            
            return {
                'semillas': {
                    'beneficiarios': semillas.beneficiarios,
                    'hectareas': float(semillas.hectareas_semillas or 0),
                    'inversion_gad': float(semillas.inversion_gad_semillas or 0),
                    'quintales': semillas.quintales_entregados or 0,
                    'precio_gad_promedio': float(semillas.precio_promedio_gad or 0)
                },
                'fertilizantes': {
                    'beneficiarios': fertilizantes.beneficiarios,
                    'hectareas': float(fertilizantes.hectareas_fertilizantes or 0),
                    'inversion_gad': float(fertilizantes.inversion_gad_fertilizantes or 0),
                    'precio_kit_gad_promedio': float(fertilizantes.precio_promedio_kit_gad or 0)
                },
                'mecanizacion': {
                    'beneficiarios': mecanizacion.beneficiarios,
                    'hectareas': float(mecanizacion.hectareas_mecanizacion or 0),
                    'costo_por_hectarea': 200.00,  # Arado + Fangueo de la matriz
                    'inversion_gad': float(mecanizacion.hectareas_mecanizacion or 0) * 200.00
                },
                'total_hectareas_arroz': float(total_ha.total_hectareas or 0)
            }
    
    def calcular_ahorro_completo(self, datos_reales: Dict[str, Any]) -> Dict[str, Any]:
        """Calcula el ahorro completo incluyendo todos los beneficios."""
        
        # Precios de mercado estimados
        precio_semilla_mercado = datos_reales['semillas']['precio_gad_promedio'] * 1.20  # +20%
        precio_fertilizante_mercado = datos_reales['fertilizantes']['precio_kit_gad_promedio'] * 1.15  # +15%
        precio_mecanizacion_mercado = 200.00  # Arado + Fangueo de matriz
        
        # Ahorros por tipo de beneficio
        ahorro_semillas = (precio_semilla_mercado - datos_reales['semillas']['precio_gad_promedio']) * datos_reales['semillas']['quintales']
        ahorro_fertilizantes = (precio_fertilizante_mercado - datos_reales['fertilizantes']['precio_kit_gad_promedio']) * datos_reales['fertilizantes']['beneficiarios']
        ahorro_mecanizacion = precio_mecanizacion_mercado * datos_reales['mecanizacion']['hectareas']  # 100% gratis
        
        return {
            'precios_mercado': {
                'semilla_quintal': precio_semilla_mercado,
                'fertilizante_kit': precio_fertilizante_mercado,
                'mecanizacion_ha': precio_mecanizacion_mercado
            },
            'precios_gad': {
                'semilla_quintal': datos_reales['semillas']['precio_gad_promedio'],
                'fertilizante_kit': datos_reales['fertilizantes']['precio_kit_gad_promedio'],
                'mecanizacion_ha': 0.00  # GAD lo da gratis
            },
            'ahorro_real': {
                'semillas': ahorro_semillas,
                'fertilizantes': ahorro_fertilizantes,
                'mecanizacion': ahorro_mecanizacion,
                'total': ahorro_semillas + ahorro_fertilizantes + ahorro_mecanizacion
            }
        }
    
    def generar_datos_graficos(self, datos_reales: Dict[str, Any], ahorro_info: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Genera datos estructurados para gráficos del frontend."""
        
        return {
            'inversion_por_beneficio': [
                {'nombre': 'Semillas', 'valor': datos_reales['semillas']['inversion_gad'], 'color': '#2563eb'},
                {'nombre': 'Fertilizantes', 'valor': datos_reales['fertilizantes']['inversion_gad'], 'color': '#16a34a'},
                {'nombre': 'Mecanización', 'valor': datos_reales['mecanizacion']['inversion_gad'], 'color': '#dc2626'}
            ],
            'ahorro_por_beneficio': [
                {'nombre': 'Semillas', 'valor': ahorro_info['ahorro_real']['semillas'], 'color': '#2563eb'},
                {'nombre': 'Fertilizantes', 'valor': ahorro_info['ahorro_real']['fertilizantes'], 'color': '#16a34a'},
                {'nombre': 'Mecanización', 'valor': ahorro_info['ahorro_real']['mecanizacion'], 'color': '#dc2626'}
            ],
            'cobertura_hectareas': [
                {
                    'beneficio': 'Semillas',
                    'hectareas_beneficiadas': datos_reales['semillas']['hectareas'],
                    'hectareas_totales': datos_reales['total_hectareas_arroz'],
                    'porcentaje': (datos_reales['semillas']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100
                },
                {
                    'beneficio': 'Fertilizantes',
                    'hectareas_beneficiadas': datos_reales['fertilizantes']['hectareas'],
                    'hectareas_totales': datos_reales['total_hectareas_arroz'],
                    'porcentaje': (datos_reales['fertilizantes']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100
                },
                {
                    'beneficio': 'Mecanización',
                    'hectareas_beneficiadas': datos_reales['mecanizacion']['hectareas'],
                    'hectareas_totales': datos_reales['total_hectareas_arroz'],
                    'porcentaje': (datos_reales['mecanizacion']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100
                }
            ],
            'comparacion_precios': [
                {
                    'beneficio': 'Semillas',
                    'precio_gad': ahorro_info['precios_gad']['semilla_quintal'],
                    'precio_mercado': ahorro_info['precios_mercado']['semilla_quintal'],
                    'unidad': 'por quintal'
                },
                {
                    'beneficio': 'Fertilizantes',
                    'precio_gad': ahorro_info['precios_gad']['fertilizante_kit'],
                    'precio_mercado': ahorro_info['precios_mercado']['fertilizante_kit'],
                    'unidad': 'por kit'
                },
                {
                    'beneficio': 'Mecanización',
                    'precio_gad': ahorro_info['precios_gad']['mecanizacion_ha'],
                    'precio_mercado': ahorro_info['precios_mercado']['mecanizacion_ha'],
                    'unidad': 'por hectárea'
                }
            ]
        }
    
    def realizar_analisis_completo(self) -> AnalisisCostosResponse:
        """Realiza el análisis completo y retorna la respuesta estructurada."""
        
        # 1. Extraer datos reales
        datos_reales = self.extraer_datos_reales()
        
        # 2. Calcular ahorros
        ahorro_info = self.calcular_ahorro_completo(datos_reales)
        
        # 3. Calcular totales
        inversion_total_gad = (
            datos_reales['semillas']['inversion_gad'] + 
            datos_reales['fertilizantes']['inversion_gad'] + 
            datos_reales['mecanizacion']['inversion_gad']
        )
        
        eficiencia_completa = ahorro_info['ahorro_real']['total'] / inversion_total_gad
        
        # 4. Indicadores financieros
        ingresos_brutos_ha = self.matriz.rendimiento_sacas * self.matriz.precio_saca
        costo_total_matriz = self.matriz.calcular_total_general()
        utilidad_sin_subsidio = ingresos_brutos_ha - costo_total_matriz
        ahorro_promedio_ha = ahorro_info['ahorro_real']['total'] / datos_reales['total_hectareas_arroz']
        utilidad_con_subsidio = utilidad_sin_subsidio + ahorro_promedio_ha
        
        # 5. Calificación
        if eficiencia_completa >= 0.8:
            calificacion = "EXCELENTE"
        elif eficiencia_completa >= 0.5:
            calificacion = "BUENA"
        elif eficiencia_completa >= 0.3:
            calificacion = "REGULAR"
        else:
            calificacion = "DEFICIENTE"
        
        # 6. Construcción de la respuesta
        return AnalisisCostosResponse(
            fecha_analisis=datetime.now(),
            total_hectareas_arroz=datos_reales['total_hectareas_arroz'],
            
            beneficios={
                'semillas': DatosBeneficio(
                    beneficiarios=datos_reales['semillas']['beneficiarios'],
                    hectareas=datos_reales['semillas']['hectareas'],
                    inversion_gad=datos_reales['semillas']['inversion_gad'],
                    precio_promedio_gad=datos_reales['semillas']['precio_gad_promedio'],
                    descripcion="Entrega de semillas certificadas de arroz"
                ),
                'fertilizantes': DatosBeneficio(
                    beneficiarios=datos_reales['fertilizantes']['beneficiarios'],
                    hectareas=datos_reales['fertilizantes']['hectareas'],
                    inversion_gad=datos_reales['fertilizantes']['inversion_gad'],
                    precio_promedio_gad=datos_reales['fertilizantes']['precio_kit_gad_promedio'],
                    descripcion="Kits de fertilizantes (Urea + Abono completo)"
                ),
                'mecanizacion': DatosBeneficio(
                    beneficiarios=datos_reales['mecanizacion']['beneficiarios'],
                    hectareas=datos_reales['mecanizacion']['hectareas'],
                    inversion_gad=datos_reales['mecanizacion']['inversion_gad'],
                    precio_promedio_gad=0.0,
                    descripcion="Servicios de arado y fangueo de tierras"
                )
            },
            
            precios={
                'semillas': PreciosComparacion(
                    precio_gad=ahorro_info['precios_gad']['semilla_quintal'],
                    precio_mercado=ahorro_info['precios_mercado']['semilla_quintal'],
                    diferencia=ahorro_info['precios_mercado']['semilla_quintal'] - ahorro_info['precios_gad']['semilla_quintal'],
                    unidad="por quintal"
                ),
                'fertilizantes': PreciosComparacion(
                    precio_gad=ahorro_info['precios_gad']['fertilizante_kit'],
                    precio_mercado=ahorro_info['precios_mercado']['fertilizante_kit'],
                    diferencia=ahorro_info['precios_mercado']['fertilizante_kit'] - ahorro_info['precios_gad']['fertilizante_kit'],
                    unidad="por kit"
                ),
                'mecanizacion': PreciosComparacion(
                    precio_gad=ahorro_info['precios_gad']['mecanizacion_ha'],
                    precio_mercado=ahorro_info['precios_mercado']['mecanizacion_ha'],
                    diferencia=ahorro_info['precios_mercado']['mecanizacion_ha'] - ahorro_info['precios_gad']['mecanizacion_ha'],
                    unidad="por hectárea"
                )
            },
            
            ahorros={
                'semillas': AhorroDetalle(
                    monto=ahorro_info['ahorro_real']['semillas'],
                    porcentaje_del_total=(ahorro_info['ahorro_real']['semillas'] / ahorro_info['ahorro_real']['total']) * 100,
                    descripcion=f"{datos_reales['semillas']['quintales']} quintales × diferencia de precio"
                ),
                'fertilizantes': AhorroDetalle(
                    monto=ahorro_info['ahorro_real']['fertilizantes'],
                    porcentaje_del_total=(ahorro_info['ahorro_real']['fertilizantes'] / ahorro_info['ahorro_real']['total']) * 100,
                    descripcion=f"{datos_reales['fertilizantes']['beneficiarios']} kits × diferencia de precio"
                ),
                'mecanizacion': AhorroDetalle(
                    monto=ahorro_info['ahorro_real']['mecanizacion'],
                    porcentaje_del_total=(ahorro_info['ahorro_real']['mecanizacion'] / ahorro_info['ahorro_real']['total']) * 100,
                    descripcion=f"{datos_reales['mecanizacion']['hectareas']:.2f} ha × $200.00/ha"
                )
            },
            ahorro_total=ahorro_info['ahorro_real']['total'],
            
            eficiencia=EficienciaInversion(
                inversion_total=inversion_total_gad,
                ahorro_total=ahorro_info['ahorro_real']['total'],
                eficiencia=eficiencia_completa,
                calificacion=calificacion,
                descripcion=f"Por cada $1 invertido se generan ${eficiencia_completa:.2f} en ahorro"
            ),
            
            cobertura={
                'semillas': CoberturaPrograma(
                    cobertura_porcentaje=(datos_reales['semillas']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100,
                    hectareas_beneficiadas=datos_reales['semillas']['hectareas'],
                    hectareas_totales=datos_reales['total_hectareas_arroz']
                ),
                'fertilizantes': CoberturaPrograma(
                    cobertura_porcentaje=(datos_reales['fertilizantes']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100,
                    hectareas_beneficiadas=datos_reales['fertilizantes']['hectareas'],
                    hectareas_totales=datos_reales['total_hectareas_arroz']
                ),
                'mecanizacion': CoberturaPrograma(
                    cobertura_porcentaje=(datos_reales['mecanizacion']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100,
                    hectareas_beneficiadas=datos_reales['mecanizacion']['hectareas'],
                    hectareas_totales=datos_reales['total_hectareas_arroz']
                )
            },
            
            indicadores_financieros=IndicadoresFinancieros(
                rendimiento_sacas=self.matriz.rendimiento_sacas,
                precio_saca=self.matriz.precio_saca,
                ingresos_brutos=ingresos_brutos_ha,
                costo_produccion=costo_total_matriz,
                utilidad_sin_programa=utilidad_sin_subsidio,
                utilidad_con_programa=utilidad_con_subsidio,
                mejora_utilidad_porcentaje=((utilidad_con_subsidio - utilidad_sin_subsidio) / utilidad_sin_subsidio) * 100,
                ahorro_promedio_ha=ahorro_promedio_ha
            ),
            
            contribucion_beneficios={
                'semillas': (ahorro_info['ahorro_real']['semillas'] / ahorro_info['ahorro_real']['total']) * 100,
                'fertilizantes': (ahorro_info['ahorro_real']['fertilizantes'] / ahorro_info['ahorro_real']['total']) * 100,
                'mecanizacion': (ahorro_info['ahorro_real']['mecanizacion'] / ahorro_info['ahorro_real']['total']) * 100
            },
            
            resumen_ejecutivo=ResumenEjecutivo(
                inversion_gad_total=inversion_total_gad,
                ahorro_productores_total=ahorro_info['ahorro_real']['total'],
                eficiencia_completa=eficiencia_completa,
                beneficiarios_directos=datos_reales['semillas']['beneficiarios'] + datos_reales['fertilizantes']['beneficiarios'] + datos_reales['mecanizacion']['beneficiarios'],
                hectareas_impactadas=datos_reales['total_hectareas_arroz'],
                mejora_utilidad_promedio=((utilidad_con_subsidio - utilidad_sin_subsidio) / utilidad_sin_subsidio) * 100,
                calificacion=calificacion
            ),
            
            comparativo=AnalisisComparativo(
                sin_mecanizacion={
                    'eficiencia': 0.17,
                    'ahorro': 740128.0
                },
                con_mecanizacion={
                    'eficiencia': eficiencia_completa,
                    'ahorro': ahorro_info['ahorro_real']['total']
                },
                mejora_absoluta=ahorro_info['ahorro_real']['mecanizacion'],
                mejora_relativa=((eficiencia_completa - 0.17) / 0.17) * 100
            ),
            
            graficos=self.generar_datos_graficos(datos_reales, ahorro_info),
            
            metodologia=[
                "Datos reales de inversión y entregas extraídos de la base de datos",
                "Mecanización valorada según matriz AGRIPAC ($200/ha para Arado + Fangueo)",
                "Precios estimados de mercado: +15-20% sobre precios GAD",
                "Matriz de costos basada en sistema semi-tecnificado AGRIPAC",
                "NO incluye beneficios indirectos no monetizables"
            ]
        )
    
    def verificar_salud_bd(self) -> bool:
        """Verifica si la conexión a la base de datos está funcionando."""
        try:
            with db_connection.get_session() as session:
                result = session.execute(text("SELECT 1")).fetchone()
                return result is not None
        except Exception:
            return False

class HealthService:
    """Servicio para health checks."""
    
    @staticmethod
    def get_health_status() -> HealthResponse:
        """Retorna el estado de salud del API."""
        
        # Verificar conexión a BD
        service = AnalisisCostosService()
        db_status = service.verificar_salud_bd()
        
        status = "healthy" if db_status else "unhealthy"
        message = "API funcionando correctamente" if db_status else "Error de conexión a base de datos"
        
        return HealthResponse(
            status=status,
            timestamp=datetime.now(),
            database_connection=db_status,
            message=message
        )