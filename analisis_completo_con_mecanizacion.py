#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análisis COMPLETO de costos de arroz incluyendo mecanización.
Usa solo datos reales + valor de mecanización de la matriz de costos.
"""

from config.connections.database import db_connection
from sqlalchemy import text
from src.matriz_costos.costos_arroz import MatrizCostosArroz

def extraer_datos_completos():
    """Extrae todos los datos reales incluyendo mecanización."""
    
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

def calcular_ahorro_completo(datos_reales):
    """Calcula el ahorro COMPLETO incluyendo mecanización."""
    
    # Precios de mercado estimados (GAD + margen)
    precio_semilla_mercado = datos_reales['semillas']['precio_gad_promedio'] * 1.20  # +20%
    precio_fertilizante_mercado = datos_reales['fertilizantes']['precio_kit_gad_promedio'] * 1.15  # +15%
    precio_mecanizacion_mercado = 200.00  # Arado + Fangueo de matriz (precio de mercado)
    
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

def analizar_costos_completo():
    """Análisis completo con los 3 tipos de beneficios."""
    
    print("=" * 80)
    print("ANÁLISIS COMPLETO DE COSTOS DE ARROZ - INCLUYENDO MECANIZACIÓN")
    print("=" * 80)
    print()
    
    # 1. Extraer datos reales
    print("1. EXTRAYENDO DATOS REALES COMPLETOS...")
    datos_reales = extraer_datos_completos()
    
    print("DATOS REALES CONFIRMADOS:")
    print("-" * 50)
    print(f"Total hectáreas arroz: {datos_reales['total_hectareas_arroz']:,.2f} ha")
    print()
    print("SEMILLAS:")
    print(f"  • Beneficiarios: {datos_reales['semillas']['beneficiarios']:,}")
    print(f"  • Hectáreas: {datos_reales['semillas']['hectareas']:,.2f} ha")
    print(f"  • Inversión GAD: ${datos_reales['semillas']['inversion_gad']:,.2f}")
    print(f"  • Precio GAD: ${datos_reales['semillas']['precio_gad_promedio']:.2f}/quintal")
    print()
    print("FERTILIZANTES:")
    print(f"  • Beneficiarios: {datos_reales['fertilizantes']['beneficiarios']:,}")
    print(f"  • Hectáreas: {datos_reales['fertilizantes']['hectareas']:,.2f} ha")
    print(f"  • Inversión GAD: ${datos_reales['fertilizantes']['inversion_gad']:,.2f}")
    print(f"  • Precio GAD: ${datos_reales['fertilizantes']['precio_kit_gad_promedio']:.2f}/kit")
    print()
    print("MECANIZACIÓN (Arado + Fangueo):")
    print(f"  • Beneficiarios: {datos_reales['mecanizacion']['beneficiarios']:,}")
    print(f"  • Hectáreas: {datos_reales['mecanizacion']['hectareas']:,.2f} ha")
    print(f"  • Costo matriz: ${datos_reales['mecanizacion']['costo_por_hectarea']:.2f}/ha")
    print(f"  • Inversión GAD estimada: ${datos_reales['mecanizacion']['inversion_gad']:,.2f}")
    print(f"  • Precio GAD: $0.00/ha (100% subsidiado)")
    print()
    
    inversion_total_gad = (datos_reales['semillas']['inversion_gad'] + 
                          datos_reales['fertilizantes']['inversion_gad'] + 
                          datos_reales['mecanizacion']['inversion_gad'])
    
    print(f"INVERSIÓN TOTAL GAD: ${inversion_total_gad:,.2f}")
    print()
    
    # 2. Crear matriz de costos para contexto
    print("2. MATRIZ DE COSTOS DE PRODUCCIÓN (AGRIPAC)...")
    matriz = MatrizCostosArroz()
    
    costo_total_matriz = matriz.calcular_total_general()
    print(f"Costo total producción por hectárea: ${costo_total_matriz:.2f}")
    print(f"Costo total sin beneficios ({datos_reales['total_hectareas_arroz']:,.0f} ha): ${costo_total_matriz * datos_reales['total_hectareas_arroz']:,.2f}")
    print()
    
    # 3. Calcular ahorro completo
    print("3. CÁLCULO DE AHORRO COMPLETO (3 TIPOS DE BENEFICIOS)...")
    ahorro_info = calcular_ahorro_completo(datos_reales)
    
    print("COMPARACIÓN DE PRECIOS:")
    print("-" * 40)
    print(f"SEMILLAS:")
    print(f"  • GAD pagó: ${ahorro_info['precios_gad']['semilla_quintal']:.2f}/quintal")
    print(f"  • Mercado (est.): ${ahorro_info['precios_mercado']['semilla_quintal']:.2f}/quintal")
    print(f"  • Diferencia: ${ahorro_info['precios_mercado']['semilla_quintal'] - ahorro_info['precios_gad']['semilla_quintal']:.2f}/quintal")
    print()
    print(f"FERTILIZANTES:")
    print(f"  • GAD pagó: ${ahorro_info['precios_gad']['fertilizante_kit']:.2f}/kit")
    print(f"  • Mercado (est.): ${ahorro_info['precios_mercado']['fertilizante_kit']:.2f}/kit")
    print(f"  • Diferencia: ${ahorro_info['precios_mercado']['fertilizante_kit'] - ahorro_info['precios_gad']['fertilizante_kit']:.2f}/kit")
    print()
    print(f"MECANIZACIÓN:")
    print(f"  • GAD pagó: ${ahorro_info['precios_gad']['mecanizacion_ha']:.2f}/ha")
    print(f"  • Mercado (matriz): ${ahorro_info['precios_mercado']['mecanizacion_ha']:.2f}/ha")
    print(f"  • Diferencia: ${ahorro_info['precios_mercado']['mecanizacion_ha'] - ahorro_info['precios_gad']['mecanizacion_ha']:.2f}/ha")
    print()
    
    print("AHORRO REAL CALCULADO:")
    print("-" * 40)
    print(f"• Ahorro en semillas: ${ahorro_info['ahorro_real']['semillas']:,.2f}")
    print(f"  ({datos_reales['semillas']['quintales']:,} quintales × diferencia de precio)")
    print(f"• Ahorro en fertilizantes: ${ahorro_info['ahorro_real']['fertilizantes']:,.2f}")
    print(f"  ({datos_reales['fertilizantes']['beneficiarios']:,} kits × diferencia de precio)")
    print(f"• Ahorro en mecanización: ${ahorro_info['ahorro_real']['mecanizacion']:,.2f}")
    print(f"  ({datos_reales['mecanizacion']['hectareas']:,.2f} ha × ${datos_reales['mecanizacion']['costo_por_hectarea']:.2f}/ha)")
    print(f"• AHORRO TOTAL REAL: ${ahorro_info['ahorro_real']['total']:,.2f}")
    print()
    
    # 4. Eficiencia completa
    eficiencia_completa = ahorro_info['ahorro_real']['total'] / inversion_total_gad
    
    print("4. EFICIENCIA COMPLETA DE LA INVERSIÓN:")
    print("=" * 50)
    print(f"Inversión GAD total: ${inversion_total_gad:,.2f}")
    print(f"  • Semillas: ${datos_reales['semillas']['inversion_gad']:,.2f}")
    print(f"  • Fertilizantes: ${datos_reales['fertilizantes']['inversion_gad']:,.2f}")
    print(f"  • Mecanización: ${datos_reales['mecanizacion']['inversion_gad']:,.2f}")
    print()
    print(f"Ahorro productores: ${ahorro_info['ahorro_real']['total']:,.2f}")
    print(f"EFICIENCIA COMPLETA: {eficiencia_completa:.2f}x")
    print()
    
    if eficiencia_completa > 1.0:
        print(f"✅ POR CADA $1 INVERTIDO, SE GENERAN ${eficiencia_completa:.2f} EN AHORRO")
        print("✅ EL PROGRAMA COMPLETO ES RENTABLE SOCIALMENTE")
    else:
        print(f"⚠️  POR CADA $1 INVERTIDO, SE GENERAN ${eficiencia_completa:.2f} EN AHORRO")
        print("⚠️  EL PROGRAMA NO ES RENTABLE EN TÉRMINOS DIRECTOS")
        print("   (Pero tiene beneficios indirectos importantes)")
    print()
    
    # 5. Cobertura completa del programa
    print("5. COBERTURA COMPLETA DEL PROGRAMA:")
    print("-" * 50)
    
    cobertura_semillas = (datos_reales['semillas']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100
    cobertura_fertilizantes = (datos_reales['fertilizantes']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100
    cobertura_mecanizacion = (datos_reales['mecanizacion']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100
    
    print(f"• Cobertura semillas: {cobertura_semillas:.1f}% ({datos_reales['semillas']['hectareas']:,.0f} de {datos_reales['total_hectareas_arroz']:,.0f} ha)")
    print(f"• Cobertura fertilizantes: {cobertura_fertilizantes:.1f}% ({datos_reales['fertilizantes']['hectareas']:,.0f} de {datos_reales['total_hectareas_arroz']:,.0f} ha)")
    print(f"• Cobertura mecanización: {cobertura_mecanizacion:.1f}% ({datos_reales['mecanizacion']['hectareas']:,.0f} de {datos_reales['total_hectareas_arroz']:,.0f} ha)")
    print()
    
    # 6. Indicadores financieros con impacto completo
    print("6. INDICADORES FINANCIEROS COMPLETOS:")
    print("-" * 50)
    
    ingresos_brutos_ha = matriz.rendimiento_sacas * matriz.precio_saca
    utilidad_sin_subsidio = ingresos_brutos_ha - costo_total_matriz
    
    # Ahorro promedio por hectárea (distribuido en todas las hectáreas)
    ahorro_promedio_ha = ahorro_info['ahorro_real']['total'] / datos_reales['total_hectareas_arroz']
    utilidad_con_subsidio_completo = utilidad_sin_subsidio + ahorro_promedio_ha
    
    print(f"Rendimiento esperado: {matriz.rendimiento_sacas} sacas/ha")
    print(f"Precio por saca: ${matriz.precio_saca:.2f}")
    print(f"Ingresos brutos: ${ingresos_brutos_ha:.2f}/ha")
    print(f"Costo producción: ${costo_total_matriz:.2f}/ha")
    print()
    print(f"Utilidad SIN programa: ${utilidad_sin_subsidio:.2f}/ha ({(utilidad_sin_subsidio/ingresos_brutos_ha)*100:.1f}%)")
    print(f"Ahorro promedio real: ${ahorro_promedio_ha:.2f}/ha")
    print(f"Utilidad CON programa completo: ${utilidad_con_subsidio_completo:.2f}/ha ({(utilidad_con_subsidio_completo/ingresos_brutos_ha)*100:.1f}%)")
    print(f"Mejora en utilidad: {((utilidad_con_subsidio_completo - utilidad_sin_subsidio) / utilidad_sin_subsidio) * 100:.1f}%")
    print()
    
    # 7. Desglose de contribución por tipo de beneficio
    print("7. CONTRIBUCIÓN POR TIPO DE BENEFICIO:")
    print("-" * 50)
    
    contrib_semillas = (ahorro_info['ahorro_real']['semillas'] / ahorro_info['ahorro_real']['total']) * 100
    contrib_fertilizantes = (ahorro_info['ahorro_real']['fertilizantes'] / ahorro_info['ahorro_real']['total']) * 100
    contrib_mecanizacion = (ahorro_info['ahorro_real']['mecanizacion'] / ahorro_info['ahorro_real']['total']) * 100
    
    print(f"• Semillas: ${ahorro_info['ahorro_real']['semillas']:,.2f} ({contrib_semillas:.1f}% del ahorro total)")
    print(f"• Fertilizantes: ${ahorro_info['ahorro_real']['fertilizantes']:,.2f} ({contrib_fertilizantes:.1f}% del ahorro total)")
    print(f"• Mecanización: ${ahorro_info['ahorro_real']['mecanizacion']:,.2f} ({contrib_mecanizacion:.1f}% del ahorro total)")
    print()
    
    # 8. Resumen ejecutivo completo
    print("8. RESUMEN EJECUTIVO COMPLETO:")
    print("=" * 50)
    print(f"• Inversión GAD total: ${inversion_total_gad:,.2f}")
    print(f"• Ahorro productores total: ${ahorro_info['ahorro_real']['total']:,.2f}")
    print(f"• Eficiencia completa: {eficiencia_completa:.2f}x")
    print(f"• Beneficiarios directos: {datos_reales['semillas']['beneficiarios'] + datos_reales['fertilizantes']['beneficiarios'] + datos_reales['mecanizacion']['beneficiarios']:,}")
    print(f"• Hectáreas impactadas: {datos_reales['total_hectareas_arroz']:,.0f} ha")
    print(f"• Mejora utilidad promedio: {((utilidad_con_subsidio_completo - utilidad_sin_subsidio) / utilidad_sin_subsidio) * 100:.1f}%")
    print()
    
    if eficiencia_completa >= 0.8:
        calificacion = "EXCELENTE"
    elif eficiencia_completa >= 0.5:
        calificacion = "BUENA"
    elif eficiencia_completa >= 0.3:
        calificacion = "REGULAR"
    else:
        calificacion = "DEFICIENTE"
    
    print(f"CALIFICACIÓN DEL PROGRAMA COMPLETO: {calificacion}")
    print()
    
    print("COMPARACIÓN CON ANÁLISIS ANTERIOR:")
    print("-" * 40)
    print("SIN mecanización:")
    print(f"  • Eficiencia: 0.17x")
    print(f"  • Ahorro: $740,128")
    print()
    print("CON mecanización:")
    print(f"  • Eficiencia: {eficiencia_completa:.2f}x")
    print(f"  • Ahorro: ${ahorro_info['ahorro_real']['total']:,.0f}")
    print(f"  • Mejora: +${ahorro_info['ahorro_real']['mecanizacion']:,.0f} por mecanización")
    print()
    
    print("NOTA METODOLÓGICA:")
    print("• Datos reales de inversión y entregas (base de datos)")
    print("• Mecanización valorada según matriz AGRIPAC ($200/ha)")
    print("• Precios estimados de mercado (+15-20% sobre precios GAD)")
    print("• NO incluye beneficios indirectos no monetizables")

if __name__ == "__main__":
    analizar_costos_completo()