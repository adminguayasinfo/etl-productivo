# -*- coding: utf-8 -*-
"""
Servicios para el API de beneficios-cultivos.
Contiene toda la lógica de negocio para calcular subsidios y reducciones de costos.
"""
import os
from typing import List, Dict, Any, Tuple
from datetime import datetime
from sqlalchemy import text
import logging

from config.connections.database import DatabaseConnection
from src.matriz_costos.costos_arroz import MatrizCostosArroz, CategoriaInsumo as CategoriaArrozEnum
from src.matriz_costos.costos_maiz import MatrizCostosMaiz, CategoriaInsumo as CategoriaMaizEnum
from api.models_subsidios import (
    BeneficiosCultivosResponse, HectareasSubsidiadas, CostoProduccion,
    MontoSubsidio, ReduccionCostos, FiltrosBeneficios, ResumenEjecutivo,
    TipoCultivo, TipoBeneficio
)

logger = logging.getLogger(__name__)


class BeneficiosCultivosService:
    """Servicio principal para análisis de beneficios en cultivos."""
    
    def __init__(self):
        """Inicializa el servicio con conexión a BD y matrices de costos."""
        # Usar DatabaseConnection que ya carga las variables del .env
        self.db = DatabaseConnection()
        self.db.init_engine()
        
        # Inicializar matrices de costos
        self.matriz_arroz = MatrizCostosArroz()
        self.matriz_maiz = MatrizCostosMaiz()
        
        # Mapeo de beneficios BD a categorías de matriz
        self.mapeo_beneficios = {
            'SEMILLAS': {
                'arroz': CategoriaArrozEnum.SEMILLA,
                'maiz': CategoriaMaizEnum.SEMILLA
            },
            'FERTILIZANTES': {
                'arroz': CategoriaArrozEnum.FERTILIZANTE,  
                'maiz': CategoriaMaizEnum.FERTILIZANTE
            },
            'MECANIZACION': {
                'arroz': CategoriaArrozEnum.MAQUINARIA,
                'maiz': CategoriaMaizEnum.MAQUINARIA
            }
        }
    
    def obtener_hectareas_subsidiadas(self) -> List[HectareasSubsidiadas]:
        """Obtiene hectáreas subsidiadas por cultivo y tipo de beneficio."""
        query = """
        SELECT 
            tc.nombre as cultivo,
            b.tipo_beneficio,
            COUNT(*) as num_beneficios,
            COALESCE(SUM(b.hectareas_beneficiadas), 0) as total_hectareas
        FROM "etl-productivo".beneficio b
        INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
        WHERE tc.nombre IN ('ARROZ', 'MAIZ')
        GROUP BY tc.nombre, b.tipo_beneficio
        ORDER BY tc.nombre, b.tipo_beneficio;
        """
        
        result = []
        with self.db.engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
            for row in rows:
                result.append(HectareasSubsidiadas(
                    cultivo=TipoCultivo(row[0]),
                    tipo_beneficio=TipoBeneficio(row[1]),
                    total_hectareas=float(row[3]),
                    num_beneficios=int(row[2])
                ))
        
        return result
    
    def obtener_costos_produccion(self) -> List[CostoProduccion]:
        """Obtiene costos de producción por hectárea según matrices."""
        result = []
        
        # Arroz
        resumen_arroz = self.matriz_arroz.obtener_resumen_por_categoria()
        result.append(CostoProduccion(
            cultivo=TipoCultivo.ARROZ,
            costo_total_sin_subsidio=self.matriz_arroz.calcular_total_general(),
            costos_directos=self.matriz_arroz.calcular_total_costos_directos(),
            costos_indirectos=self.matriz_arroz.calcular_total_costos_indirectos(),
            desglose_por_categoria={k.value: v for k, v in resumen_arroz.items()}
        ))
        
        # Maíz
        resumen_maiz = self.matriz_maiz.obtener_resumen_por_categoria()
        result.append(CostoProduccion(
            cultivo=TipoCultivo.MAIZ,
            costo_total_sin_subsidio=self.matriz_maiz.calcular_total_general(),
            costos_directos=self.matriz_maiz.calcular_total_costos_directos(),
            costos_indirectos=self.matriz_maiz.calcular_total_costos_indirectos(),
            desglose_por_categoria={k.value: v for k, v in resumen_maiz.items()}
        ))
        
        return result
    
    def obtener_montos_subsidios(self) -> List[MontoSubsidio]:
        """Obtiene montos totales de subsidios entregados y costos por hectárea."""
        query = """
        SELECT 
            tc.nombre as cultivo,
            b.tipo_beneficio,
            COUNT(*) as num_beneficios,
            COALESCE(SUM(b.monto), 0) as total_monto
        FROM "etl-productivo".beneficio b
        INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
        WHERE tc.nombre IN ('ARROZ', 'MAIZ')
        GROUP BY tc.nombre, b.tipo_beneficio
        ORDER BY tc.nombre, b.tipo_beneficio;
        """
        
        result = []
        with self.db.engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
            for row in rows:
                cultivo = row[0].lower()
                beneficio = row[1]
                
                # Obtener costo por hectárea según matriz
                costo_matriz = self._obtener_costo_matriz_por_beneficio(cultivo, beneficio)
                
                result.append(MontoSubsidio(
                    cultivo=TipoCultivo(row[0]),
                    tipo_beneficio=TipoBeneficio(row[1]),
                    monto_total_entregado=float(row[3]) if row[3] else 0.0,
                    monto_matriz_por_hectarea=costo_matriz,
                    num_beneficios=int(row[2])
                ))
        
        return result
    
    def _obtener_costo_matriz_por_beneficio(self, cultivo: str, beneficio: str) -> float:
        """Obtiene el costo por hectárea de un beneficio específico según la matriz."""
        if cultivo == 'arroz':
            matriz = self.matriz_arroz
            categoria_enum = CategoriaArrozEnum
        else:
            matriz = self.matriz_maiz
            categoria_enum = CategoriaMaizEnum
        
        # Mapear beneficio a categoría
        if beneficio == 'SEMILLAS':
            categoria = categoria_enum.SEMILLA
        elif beneficio == 'FERTILIZANTES':
            categoria = categoria_enum.FERTILIZANTE
        elif beneficio == 'MECANIZACION':
            categoria = categoria_enum.MAQUINARIA
            # Para mecanización, solo el item específico
            return self._obtener_costo_mecanizacion_especifico(cultivo)
        else:
            return 0.0
        
        # Sumar todos los items de la categoría
        total = sum(item.costo_total for item in matriz.items_costo 
                   if item.categoria == categoria)
        return total
    
    def _obtener_costo_mecanizacion_especifico(self, cultivo: str) -> float:
        """Obtiene el costo específico de mecanización según las especificaciones."""
        if cultivo == 'arroz':
            # Buscar "Arado + Fangueo" en arroz
            for item in self.matriz_arroz.items_costo:
                if 'Arado + Fangueo' in item.concepto:
                    return item.costo_total
        else:
            # Buscar "Arado + Rastra" en maíz  
            for item in self.matriz_maiz.items_costo:
                if 'Arado + Rastra' in item.concepto:
                    return item.costo_total
        
        return 0.0
    
    def calcular_reduccion_costos(self) -> List[ReduccionCostos]:
        """Calcula la reducción total en costos de producción por subsidios."""
        hectareas_data = self.obtener_hectareas_subsidiadas()
        montos_data = self.obtener_montos_subsidios()
        
        # Agrupar por cultivo
        data_por_cultivo = {}
        for hectarea in hectareas_data:
            cultivo = hectarea.cultivo.value
            if cultivo not in data_por_cultivo:
                data_por_cultivo[cultivo] = {'hectareas': {}, 'montos': {}}
            data_por_cultivo[cultivo]['hectareas'][hectarea.tipo_beneficio.value] = hectarea
        
        for monto in montos_data:
            cultivo = monto.cultivo.value
            if cultivo not in data_por_cultivo:
                data_por_cultivo[cultivo] = {'hectareas': {}, 'montos': {}}
            data_por_cultivo[cultivo]['montos'][monto.tipo_beneficio.value] = monto
        
        result = []
        for cultivo, data in data_por_cultivo.items():
            # Obtener costo base sin subsidio
            if cultivo == 'ARROZ':
                costo_base = self.matriz_arroz.calcular_total_general()
            else:
                costo_base = self.matriz_maiz.calcular_total_general()
            
            # Calcular reducción total y desglose
            reduccion_total = 0.0
            desglose_reducciones = {}
            
            for beneficio, hectarea_info in data['hectareas'].items():
                if beneficio in data['montos']:
                    monto_info = data['montos'][beneficio]
                    # Reducción = monto total entregado por la BD
                    reduccion_beneficio = monto_info.monto_total_entregado
                    reduccion_total += reduccion_beneficio
                    desglose_reducciones[beneficio] = reduccion_beneficio
            
            # Calcular porcentaje de reducción
            # Nota: Esto es conceptual - la reducción real sería por hectárea total impactada
            total_hectareas = sum(h.total_hectareas for h in data['hectareas'].values())
            costo_total_sin_subsidio = costo_base * total_hectareas
            porcentaje_reduccion = (reduccion_total / costo_total_sin_subsidio * 100) if costo_total_sin_subsidio > 0 else 0
            
            result.append(ReduccionCostos(
                cultivo=TipoCultivo(cultivo),
                costo_produccion_sin_subsidio=costo_total_sin_subsidio,
                reduccion_por_subsidios=reduccion_total,
                costo_produccion_con_subsidio=costo_total_sin_subsidio - reduccion_total,
                porcentaje_reduccion=porcentaje_reduccion,
                desglose_reducciones=desglose_reducciones
            ))
        
        return result
    
    def obtener_filtros_disponibles(self) -> FiltrosBeneficios:
        """Obtiene los filtros disponibles para el frontend."""
        query = """
        SELECT DISTINCT 
            tc.nombre as cultivo,
            b.tipo_beneficio
        FROM "etl-productivo".beneficio b
        INNER JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
        WHERE tc.nombre IN ('ARROZ', 'MAIZ')
        ORDER BY tc.nombre, b.tipo_beneficio;
        """
        
        combinaciones = []
        cultivos = set()
        beneficios = set()
        
        with self.db.engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
            for row in rows:
                cultivo = row[0]
                beneficio = row[1]
                cultivos.add(cultivo)
                beneficios.add(beneficio)
                combinaciones.append({
                    "cultivo": cultivo,
                    "beneficio": beneficio
                })
        
        return FiltrosBeneficios(
            cultivos_disponibles=[TipoCultivo(c) for c in sorted(cultivos)],
            beneficios_disponibles=[TipoBeneficio(b) for b in sorted(beneficios)],
            combinaciones_disponibles=combinaciones
        )
    
    def generar_resumen_ejecutivo(self, hectareas_data: List[HectareasSubsidiadas], 
                                 montos_data: List[MontoSubsidio]) -> ResumenEjecutivo:
        """Genera el resumen ejecutivo con métricas clave."""
        total_hectareas = sum(h.total_hectareas for h in hectareas_data)
        total_beneficios = sum(h.num_beneficios for h in hectareas_data)
        inversion_total = sum(m.monto_total_entregado for m in montos_data)
        
        # Cultivos más subsidiados
        cultivos_stats = {}
        for h in hectareas_data:
            cultivo = h.cultivo.value
            if cultivo not in cultivos_stats:
                cultivos_stats[cultivo] = {'hectareas': 0, 'beneficios': 0}
            cultivos_stats[cultivo]['hectareas'] += h.total_hectareas
            cultivos_stats[cultivo]['beneficios'] += h.num_beneficios
        
        cultivos_mas_subsidiados = [
            {
                'cultivo': cultivo,
                'hectareas': stats['hectareas'],
                'beneficios': stats['beneficios']
            }
            for cultivo, stats in sorted(cultivos_stats.items(), 
                                       key=lambda x: x[1]['hectareas'], reverse=True)
        ]
        
        # Beneficios más utilizados
        beneficios_stats = {}
        for h in hectareas_data:
            beneficio = h.tipo_beneficio.value
            if beneficio not in beneficios_stats:
                beneficios_stats[beneficio] = {'hectareas': 0, 'beneficios': 0}
            beneficios_stats[beneficio]['hectareas'] += h.total_hectareas
            beneficios_stats[beneficio]['beneficios'] += h.num_beneficios
        
        beneficios_mas_utilizados = [
            {
                'beneficio': beneficio,
                'hectareas': stats['hectareas'],
                'beneficios': stats['beneficios']
            }
            for beneficio, stats in sorted(beneficios_stats.items(),
                                         key=lambda x: x[1]['hectareas'], reverse=True)
        ]
        
        return ResumenEjecutivo(
            total_hectareas_impactadas=total_hectareas,
            total_beneficios_otorgados=total_beneficios,
            inversion_total_gad=inversion_total,
            ahorro_total_productores=inversion_total,  # Asumiendo que el ahorro = inversión
            cultivos_mas_subsidiados=cultivos_mas_subsidiados,
            beneficios_mas_utilizados=beneficios_mas_utilizados
        )
    
    def obtener_analisis_completo(self, filtro_cultivo: str = None, 
                                 filtro_beneficio: str = None) -> BeneficiosCultivosResponse:
        """Obtiene el análisis completo de beneficios-cultivos."""
        logger.info("Iniciando análisis completo de beneficios-cultivos...")
        
        try:
            # 1. Obtener hectáreas subsidiadas
            hectareas_data = self.obtener_hectareas_subsidiadas()
            
            # 2. Obtener costos de producción
            costos_data = self.obtener_costos_produccion()
            
            # 3. Obtener montos de subsidios
            montos_data = self.obtener_montos_subsidios()
            
            # 4. Calcular reducción de costos
            reduccion_data = self.calcular_reduccion_costos()
            
            # 5. Obtener filtros disponibles
            filtros = self.obtener_filtros_disponibles()
            
            # 6. Generar resumen ejecutivo
            resumen = self.generar_resumen_ejecutivo(hectareas_data, montos_data)
            
            # Aplicar filtros si se proporcionan
            if filtro_cultivo:
                hectareas_data = [h for h in hectareas_data if h.cultivo.value == filtro_cultivo]
                costos_data = [c for c in costos_data if c.cultivo.value == filtro_cultivo]
                montos_data = [m for m in montos_data if m.cultivo.value == filtro_cultivo]
                reduccion_data = [r for r in reduccion_data if r.cultivo.value == filtro_cultivo]
            
            if filtro_beneficio:
                hectareas_data = [h for h in hectareas_data if h.tipo_beneficio.value == filtro_beneficio]
                montos_data = [m for m in montos_data if m.tipo_beneficio.value == filtro_beneficio]
            
            return BeneficiosCultivosResponse(
                hectareas_subsidiadas=hectareas_data,
                costos_produccion=costos_data,
                montos_subsidios=montos_data,
                reduccion_costos=reduccion_data,
                filtros=filtros,
                resumen=resumen
            )
            
        except Exception as e:
            logger.error(f"Error en análisis completo: {str(e)}")
            raise e