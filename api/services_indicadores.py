#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Servicios para el API de indicadores productivos.
Contiene lógica de negocio para cálculo de indicadores de eficiencia.
"""

import os
from typing import Dict, Any, List, Tuple
from datetime import datetime
from sqlalchemy import text
import logging

from config.connections.database import DatabaseConnection
from src.matriz_costos.costos_arroz import MatrizCostosArroz
from src.matriz_costos.costos_maiz import MatrizCostosMaiz
from api.models_indicadores import (
    ProductivoIndicadoresResponse, 
    IndicadorReduccionCostos
)

logger = logging.getLogger(__name__)


class IndicadoresService:
    """Servicio principal para cálculo de indicadores productivos."""
    
    def __init__(self):
        """Inicializa el servicio con conexión a BD y matrices de costos."""
        # Usar DatabaseConnection que ya carga las variables del .env
        self.db = DatabaseConnection()
        self.db.init_engine()
        
        # Inicializar matrices de costos
        self.matriz_arroz = MatrizCostosArroz()
        self.matriz_maiz = MatrizCostosMaiz()
        
        # Costos específicos por cultivo y beneficio
        self.costos_mecanizacion = {
            'ARROZ': 200.00,  # "Arado + Fangueo", 5, "Hora", 40.00 = 200.00
            'MAIZ': 70.00     # "Arado + Rastra", 2, "Hora", 35.00 = 70.00
        }
        
        self.costos_produccion_por_hectarea = {
            'ARROZ': self.matriz_arroz.calcular_total_general(),
            'MAIZ': self.matriz_maiz.calcular_total_general()
        }
    
    def calcular_indicador_reduccion_costos(self) -> IndicadorReduccionCostos:
        """
        Calcula el indicador de porcentaje promedio de reducción de costos ponderado.
        
        Fórmula del promedio ponderado:
        - Para cada beneficiario: 
          * % reducción = (monto_beneficios / costo_sin_subsidios) * 100
          * Peso = hectareas_totales * monto_beneficios_total
        - Promedio ponderado = Σ(% reducción * peso) / Σ(peso)
        
        Returns:
            IndicadorReduccionCostos: Indicador calculado con datos agregados
        """
        logger.info("Calculando indicador de reducción de costos ponderado...")
        
        try:
            # Query para obtener todos los beneficiarios con sus datos agregados
            query = f"""
            SELECT 
                ben.id,
                ben.cedula,
                ben.nombres_completos,
                SUM(b.hectareas_beneficiadas) as hectareas_totales,
                SUM(CASE 
                    WHEN b.tipo_beneficio = 'SEMILLAS' THEN b.monto
                    WHEN b.tipo_beneficio = 'FERTILIZANTES' THEN b.monto
                    WHEN b.tipo_beneficio = 'MECANIZACION' THEN 
                        CASE 
                            WHEN tc.nombre = 'ARROZ' THEN b.hectareas_beneficiadas * 200.00
                            WHEN tc.nombre = 'MAIZ' THEN b.hectareas_beneficiadas * 70.00
                            ELSE 0
                        END
                    ELSE 0 
                END) as monto_beneficios_total,
                SUM(CASE 
                    WHEN tc.nombre = 'ARROZ' THEN b.hectareas_beneficiadas * {self.costos_produccion_por_hectarea['ARROZ']}
                    WHEN tc.nombre = 'MAIZ' THEN b.hectareas_beneficiadas * {self.costos_produccion_por_hectarea['MAIZ']}
                    ELSE 0 
                END) as costo_sin_subsidios_total
            FROM "etl-productivo".beneficio b
            INNER JOIN "etl-productivo".beneficiario ben ON b.beneficiario_id = ben.id
            INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
            WHERE b.hectareas_beneficiadas > 0
            AND tc.nombre IN ('ARROZ', 'MAIZ')
            GROUP BY ben.id, ben.cedula, ben.nombres_completos
            HAVING SUM(b.hectareas_beneficiadas) > 0 
               AND SUM(CASE 
                    WHEN b.tipo_beneficio = 'SEMILLAS' THEN b.monto
                    WHEN b.tipo_beneficio = 'FERTILIZANTES' THEN b.monto
                    WHEN b.tipo_beneficio = 'MECANIZACION' THEN 
                        CASE 
                            WHEN tc.nombre = 'ARROZ' THEN b.hectareas_beneficiadas * 200.00
                            WHEN tc.nombre = 'MAIZ' THEN b.hectareas_beneficiadas * 70.00
                            ELSE 0
                        END
                    ELSE 0 
                END) > 0
            ORDER BY ben.id
            """
            
            # Ejecutar query usando el método correcto
            beneficiarios = self.db.execute_query(query)
            
            if not beneficiarios:
                logger.warning("No se encontraron beneficiarios para el cálculo")
                return IndicadorReduccionCostos(
                    porcentaje_promedio_reduccion=0.0,
                    total_beneficiarios=0,
                    hectareas_totales=0.0,
                    monto_total_beneficios=0.0,
                    costo_total_sin_subsidios=0.0
                )
            
            # Calcular promedio ponderado
            suma_ponderada = 0.0
            suma_pesos = 0.0
            hectareas_totales = 0.0
            monto_total_beneficios = 0.0
            costo_total_sin_subsidios = 0.0
            
            for beneficiario in beneficiarios:
                hectareas = float(beneficiario.hectareas_totales or 0)
                monto_beneficios = float(beneficiario.monto_beneficios_total or 0)
                costo_sin_subsidios = float(beneficiario.costo_sin_subsidios_total or 0)
                
                # Evitar divisiones por cero
                if hectareas <= 0 or monto_beneficios <= 0 or costo_sin_subsidios <= 0:
                    continue
                
                # Calcular % de reducción para este beneficiario
                porcentaje_reduccion = (monto_beneficios / costo_sin_subsidios) * 100
                
                # Peso = hectareas * monto_beneficios (doble ponderación)
                peso = hectareas * monto_beneficios
                
                # Acumular para promedio ponderado
                suma_ponderada += porcentaje_reduccion * peso
                suma_pesos += peso
                
                # Acumular totales para estadísticas
                hectareas_totales += hectareas
                monto_total_beneficios += monto_beneficios
                costo_total_sin_subsidios += costo_sin_subsidios
            
            # Calcular promedio ponderado final
            if suma_pesos > 0:
                promedio_ponderado = suma_ponderada / suma_pesos
            else:
                promedio_ponderado = 0.0
            
            logger.info(f"Indicador calculado: {promedio_ponderado:.2f}% para {len(beneficiarios)} beneficiarios")
            
            return IndicadorReduccionCostos(
                porcentaje_promedio_reduccion=round(promedio_ponderado, 2),
                total_beneficiarios=len(beneficiarios),
                hectareas_totales=round(hectareas_totales, 2),
                monto_total_beneficios=round(monto_total_beneficios, 2),
                costo_total_sin_subsidios=round(costo_total_sin_subsidios, 2)
            )
            
        except Exception as e:
            logger.error(f"Error calculando indicador de reducción de costos: {str(e)}")
            raise
    
    def obtener_indicadores_completos(self) -> ProductivoIndicadoresResponse:
        """
        Obtiene el análisis completo de indicadores productivos.
        
        Returns:
            ProductivoIndicadoresResponse: Respuesta completa con todos los indicadores
        """
        try:
            logger.info("Iniciando cálculo de indicadores productivos completos...")
            
            # Calcular indicador principal
            indicador_reduccion = self.calcular_indicador_reduccion_costos()
            
            # Generar observaciones
            observaciones = self._generar_observaciones(indicador_reduccion)
            
            response = ProductivoIndicadoresResponse(
                indicador_reduccion_costos=indicador_reduccion,
                observaciones=observaciones
            )
            
            logger.info("Indicadores productivos calculados exitosamente")
            return response
            
        except Exception as e:
            logger.error(f"Error obteniendo indicadores completos: {str(e)}")
            raise
    
    def _generar_observaciones(self, indicador: IndicadorReduccionCostos) -> str:
        """Genera observaciones sobre el indicador calculado."""
        observaciones = []
        
        if indicador.porcentaje_promedio_reduccion > 20:
            observaciones.append("Excelente impacto en reducción de costos")
        elif indicador.porcentaje_promedio_reduccion > 10:
            observaciones.append("Buen impacto en reducción de costos")
        else:
            observaciones.append("Impacto moderado en reducción de costos")
            
        observaciones.append(f"Análisis basado en {indicador.total_beneficiarios:,} beneficiarios")
        observaciones.append(f"Cobertura de {indicador.hectareas_totales:,.2f} hectáreas")
        
        return ". ".join(observaciones)