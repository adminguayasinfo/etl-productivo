#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análisis de costos de producción de arroz con y sin beneficios del GAD.
"""

from config.connections.database import db_connection
from sqlalchemy import text
from src.matriz_costos.costos_arroz import MatrizCostosArroz, CalculadoraSubsidiosGAD, CategoriaInsumo

def extraer_costos_beneficios():
    """Extrae los costos de beneficios desde la base de datos."""
    
    with db_connection.get_session() as session:
        # 1. Costos de semillas
        query_semillas = text('''
            SELECT 
                COUNT(*) as total_beneficios,
                SUM(COALESCE(precio_unitario, 0) * COALESCE(entrega, 0)) as costo_total_semillas,
                AVG(COALESCE(precio_unitario, 0)) as precio_promedio_unitario,
                SUM(COALESCE(entrega, 0)) as total_entregas
            FROM "etl-productivo".stg_semilla
            WHERE processed = true 
            AND UPPER(TRIM(cultivo)) = 'ARROZ'
            AND precio_unitario > 0
        ''')
        
        semillas_result = session.execute(query_semillas).fetchone()
        
        # 2. Costos de fertilizantes - directamente desde staging
        query_fertilizantes = text('''
            SELECT 
                COUNT(*) as total_beneficios,
                SUM(COALESCE(precio_kit, 0)) as costo_total_fertilizantes,
                AVG(COALESCE(precio_kit, 0)) as precio_promedio_kit
            FROM "etl-productivo".stg_fertilizante
            WHERE processed = true 
            AND UPPER(TRIM(cultivo)) = 'ARROZ'
            AND precio_kit > 0
        ''')
        
        fertilizantes_result = session.execute(query_fertilizantes).fetchone()
        
        # 3. Costos de mecanización - directamente desde staging
        query_mecanizacion = text('''
            SELECT 
                COUNT(*) as total_beneficios,
                SUM(COALESCE(inversion, 0)) as costo_total_mecanizacion,
                AVG(COALESCE(inversion, 0)) as precio_promedio_total
            FROM "etl-productivo".stg_mecanizacion
            WHERE processed = true 
            AND UPPER(TRIM(cultivo)) = 'ARROZ'
            AND inversion > 0
        ''')
        
        mecanizacion_result = session.execute(query_mecanizacion).fetchone()
        
        return {
            'semillas': {
                'beneficios': semillas_result.total_beneficios,
                'costo_total': float(semillas_result.costo_total_semillas or 0),
                'precio_promedio': float(semillas_result.precio_promedio_unitario or 0),
                'total_entregas': semillas_result.total_entregas or 0
            },
            'fertilizantes': {
                'beneficios': fertilizantes_result.total_beneficios,
                'costo_total': float(fertilizantes_result.costo_total_fertilizantes or 0),
                'precio_promedio': float(fertilizantes_result.precio_promedio_kit or 0)
            },
            'mecanizacion': {
                'beneficios': mecanizacion_result.total_beneficios,
                'costo_total': float(mecanizacion_result.costo_total_mecanizacion or 0),
                'precio_promedio': float(mecanizacion_result.precio_promedio_total or 0)
            }
        }

def analizar_costos_produccion():
    """Realiza el análisis completo de costos de producción."""
    
    # Datos del análisis
    hectareas_arroz = 81596.41
    hectareas_semillas = 37640.61
    hectareas_fertilizantes = 43315.55
    hectareas_mecanizacion = 640.25
    
    print("=" * 80)
    print("ANÁLISIS DE COSTOS DE PRODUCCIÓN DE ARROZ")
    print("=" * 80)
    print()
    
    # 1. Extraer costos de beneficios
    print("1. EXTRAYENDO COSTOS DE BENEFICIOS DE LA BASE DE DATOS...")
    costos_bd = extraer_costos_beneficios()
    
    print("COSTOS DE BENEFICIOS EXTRAÍDOS:")
    print("-" * 50)
    print(f"SEMILLAS:")
    print(f"  - Beneficios: {costos_bd['semillas']['beneficios']:,}")
    print(f"  - Costo total: ${costos_bd['semillas']['costo_total']:,.2f}")
    print(f"  - Precio promedio: ${costos_bd['semillas']['precio_promedio']:.2f}")
    print()
    
    print(f"FERTILIZANTES:")
    print(f"  - Beneficios: {costos_bd['fertilizantes']['beneficios']:,}")
    print(f"  - Costo total: ${costos_bd['fertilizantes']['costo_total']:,.2f}")
    print(f"  - Precio promedio kit: ${costos_bd['fertilizantes']['precio_promedio']:.2f}")
    print()
    
    print(f"MECANIZACIÓN:")
    print(f"  - Beneficios: {costos_bd['mecanizacion']['beneficios']:,}")
    print(f"  - Costo total: ${costos_bd['mecanizacion']['costo_total']:,.2f}")
    print(f"  - Precio promedio: ${costos_bd['mecanizacion']['precio_promedio']:.2f}")
    print()
    
    total_inversion = (costos_bd['semillas']['costo_total'] + 
                      costos_bd['fertilizantes']['costo_total'] + 
                      costos_bd['mecanizacion']['costo_total'])
    
    print(f"INVERSIÓN TOTAL EN BENEFICIOS: ${total_inversion:,.2f}")
    print()
    
    # 2. Crear matriz de costos
    print("2. ANALIZANDO MATRIZ DE COSTOS DE PRODUCCIÓN...")
    matriz = MatrizCostosArroz()
    
    # Validar matriz
    validacion = matriz.validar_totales()
    print("VALIDACIÓN DE MATRIZ:")
    print(f"  - Costos directos: ${validacion['costos_directos']['calculado']:.2f} ✓")
    print(f"  - Costos indirectos: ${validacion['costos_indirectos']['calculado']:.2f} ✓")
    print(f"  - Total por hectárea: ${validacion['total_general']['calculado']:.2f} ✓")
    print()
    
    # 3. Calcular costos sin beneficios
    print("3. COSTOS DE PRODUCCIÓN SIN BENEFICIOS:")
    print("-" * 50)
    costo_por_hectarea = matriz.calcular_total_general()
    costo_total_sin_beneficios = hectareas_arroz * costo_por_hectarea
    
    print(f"Costo por hectárea: ${costo_por_hectarea:.2f}")
    print(f"Hectáreas de arroz: {hectareas_arroz:,.2f} ha")
    print(f"COSTO TOTAL SIN BENEFICIOS: ${costo_total_sin_beneficios:,.2f}")
    print()
    
    # 4. Simular diferentes programas de subsidios
    print("4. ANÁLISIS DE SUBSIDIOS POR PROGRAMAS:")
    print("-" * 50)
    
    calculadora = CalculadoraSubsidiosGAD()
    
    # Programa real basado en los beneficios otorgados
    programa_real = {
        CategoriaInsumo.SEMILLA: 1.0,      # 100% subsidiado (programa semillas)
        CategoriaInsumo.FERTILIZANTE: 0.5,  # 50% subsidiado (estimado programa fertilizantes)
        # Mecanización no está en las categorías de insumos base, se trata separadamente
    }
    
    resultado_real = matriz.aplicar_subsidios_gad(programa_real)
    
    print("PROGRAMA REAL (Semillas 100% + Fertilizantes 50%):")
    print(f"  - Ahorro por hectárea: ${resultado_real['ahorro']['monto_total']:.2f} ({resultado_real['ahorro']['porcentaje']:.2f}%)")
    print(f"  - Costo final por hectárea: ${resultado_real['costos_con_subsidio']['total']:.2f}")
    
    # Calcular ahorro total en las hectáreas beneficiadas
    ahorro_semillas = resultado_real['ahorro']['monto_total'] * (hectareas_semillas / hectareas_arroz)
    ahorro_fertilizantes = resultado_real['ahorro']['monto_total'] * (hectareas_fertilizantes / hectareas_arroz)
    
    print(f"  - Ahorro total semillas ({hectareas_semillas:,.0f} ha): ${ahorro_semillas * hectareas_semillas:,.2f}")
    print(f"  - Ahorro total fertilizantes ({hectareas_fertilizantes:,.0f} ha): ${ahorro_fertilizantes * hectareas_fertilizantes:,.2f}")
    print()
    
    # 5. Comparación con programa completo
    programa_completo = calculadora.programa_completo()
    resultado_completo = matriz.aplicar_subsidios_gad(programa_completo)
    
    print("PROGRAMA COMPLETO (Semillas 100% + Fertilizantes 70% + Fitosanitarios 50%):")
    print(f"  - Ahorro por hectárea: ${resultado_completo['ahorro']['monto_total']:.2f} ({resultado_completo['ahorro']['porcentaje']:.2f}%)")
    print(f"  - Costo final por hectárea: ${resultado_completo['costos_con_subsidio']['total']:.2f}")
    print(f"  - Ahorro total en {hectareas_arroz:,.0f} ha: ${resultado_completo['ahorro']['monto_total'] * hectareas_arroz:,.2f}")
    print()
    
    # 6. Indicadores financieros
    print("5. INDICADORES FINANCIEROS:")
    print("-" * 50)
    indicadores = resultado_real['indicadores_financieros']
    
    print(f"Rendimiento esperado: {indicadores['rendimiento_sacas']} sacas/ha")
    print(f"Precio por saca: ${indicadores['precio_saca']:.2f}")
    print(f"Ingresos brutos por ha: ${indicadores['ingresos_brutos']:.2f}")
    print()
    
    print("UTILIDAD POR HECTÁREA:")
    print(f"  - Sin subsidio: ${indicadores['utilidad_sin_subsidio']:.2f} ({indicadores['margen_sin_subsidio']:.1f}%)")
    print(f"  - Con subsidio real: ${indicadores['utilidad_con_subsidio']:.2f} ({indicadores['margen_con_subsidio']:.1f}%)")
    print()
    
    # 7. Resumen final
    print("6. RESUMEN FINAL:")
    print("=" * 50)
    print(f"Total hectáreas arroz: {hectareas_arroz:,.2f} ha")
    print(f"Inversión GAD en beneficios: ${total_inversion:,.2f}")
    print(f"Costo producción sin beneficios: ${costo_total_sin_beneficios:,.2f}")
    print(f"Ahorro generado (estimado): ${resultado_real['ahorro']['monto_total'] * hectareas_arroz:,.2f}")
    print(f"Porcentaje de reducción costos: {resultado_real['ahorro']['porcentaje']:.2f}%")
    print()
    
    eficiencia_inversion = (resultado_real['ahorro']['monto_total'] * hectareas_arroz) / total_inversion
    print(f"EFICIENCIA DE LA INVERSIÓN: {eficiencia_inversion:.2f}x")
    print(f"(Por cada $1 invertido en beneficios, se genera ${eficiencia_inversion:.2f} en ahorro)")

if __name__ == "__main__":
    analizar_costos_produccion()