#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análisis REALISTA de costos de producción de arroz usando SOLO datos reales.
"""

from config.connections.database import db_connection
from sqlalchemy import text
from src.matriz_costos.costos_arroz import MatrizCostosArroz, CategoriaInsumo

def extraer_datos_reales():
    """Extrae SOLO los datos reales de la base de datos."""
    
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
        
        # 3. Total hectáreas arroz (de consulta anterior)
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
            'total_hectareas_arroz': float(total_ha.total_hectareas or 0)
        }

def calcular_ahorro_realista(datos_reales, matriz):
    """Calcula el ahorro REALISTA basado en datos reales de entregas."""
    
    # Costos de la matriz (precios de mercado teóricos)
    costo_semilla_matriz = 0
    costo_fertilizante_matriz = 0
    
    for item in matriz.items_costo:
        if item.categoria == CategoriaInsumo.SEMILLA:
            costo_semilla_matriz = item.costo_total  # $138/ha (2 quintales × $69)
        elif item.categoria == CategoriaInsumo.FERTILIZANTE:
            costo_fertilizante_matriz += item.costo_total  # $201/ha total
    
    # Suponer que los precios de mercado son 15-25% más altos que lo que pagó el GAD
    precio_semilla_mercado = datos_reales['semillas']['precio_gad_promedio'] * 1.20  # +20%
    precio_fertilizante_mercado = datos_reales['fertilizantes']['precio_kit_gad_promedio'] * 1.15  # +15%
    
    # Ahorro real = diferencia entre precio de mercado y precio GAD × cantidades reales entregadas
    ahorro_semillas = (precio_semilla_mercado - datos_reales['semillas']['precio_gad_promedio']) * datos_reales['semillas']['quintales']
    ahorro_fertilizantes = (precio_fertilizante_mercado - datos_reales['fertilizantes']['precio_kit_gad_promedio']) * datos_reales['fertilizantes']['beneficiarios']
    
    return {
        'precios_mercado': {
            'semilla_quintal': precio_semilla_mercado,
            'fertilizante_kit': precio_fertilizante_mercado
        },
        'precios_gad': {
            'semilla_quintal': datos_reales['semillas']['precio_gad_promedio'],
            'fertilizante_kit': datos_reales['fertilizantes']['precio_kit_gad_promedio']
        },
        'ahorro_real': {
            'semillas': ahorro_semillas,
            'fertilizantes': ahorro_fertilizantes,
            'total': ahorro_semillas + ahorro_fertilizantes
        },
        'costos_matriz': {
            'semilla_por_ha': costo_semilla_matriz,
            'fertilizante_por_ha': costo_fertilizante_matriz
        }
    }

def analizar_costos_realista():
    """Análisis completo con SOLO datos reales."""
    
    print("=" * 80)
    print("ANÁLISIS REALISTA DE COSTOS DE ARROZ - SOLO DATOS REALES")
    print("=" * 80)
    print()
    
    # 1. Extraer datos reales
    print("1. EXTRAYENDO DATOS REALES DE LA BASE DE DATOS...")
    datos_reales = extraer_datos_reales()
    
    print("DATOS REALES CONFIRMADOS:")
    print("-" * 50)
    print(f"Total hectáreas arroz: {datos_reales['total_hectareas_arroz']:,.2f} ha")
    print()
    print("SEMILLAS:")
    print(f"  • Beneficiarios: {datos_reales['semillas']['beneficiarios']:,}")
    print(f"  • Hectáreas: {datos_reales['semillas']['hectareas']:,.2f} ha")
    print(f"  • Quintales entregados: {datos_reales['semillas']['quintales']:,}")
    print(f"  • Inversión GAD: ${datos_reales['semillas']['inversion_gad']:,.2f}")
    print(f"  • Precio GAD: ${datos_reales['semillas']['precio_gad_promedio']:.2f}/quintal")
    print()
    print("FERTILIZANTES:")
    print(f"  • Beneficiarios: {datos_reales['fertilizantes']['beneficiarios']:,}")
    print(f"  • Hectáreas: {datos_reales['fertilizantes']['hectareas']:,.2f} ha")
    print(f"  • Inversión GAD: ${datos_reales['fertilizantes']['inversion_gad']:,.2f}")
    print(f"  • Precio GAD: ${datos_reales['fertilizantes']['precio_kit_gad_promedio']:.2f}/kit")
    print()
    
    inversion_total_gad = datos_reales['semillas']['inversion_gad'] + datos_reales['fertilizantes']['inversion_gad']
    print(f"INVERSIÓN TOTAL GAD: ${inversion_total_gad:,.2f}")
    print()
    
    # 2. Crear matriz de costos
    print("2. MATRIZ DE COSTOS DE PRODUCCIÓN (AGRIPAC)...")
    matriz = MatrizCostosArroz()
    
    costo_total_matriz = matriz.calcular_total_general()
    print(f"Costo total producción por hectárea: ${costo_total_matriz:.2f}")
    print(f"Costo total sin beneficios ({datos_reales['total_hectareas_arroz']:,.0f} ha): ${costo_total_matriz * datos_reales['total_hectareas_arroz']:,.2f}")
    print()
    
    # 3. Calcular ahorro realista
    print("3. CÁLCULO DE AHORRO REALISTA...")
    ahorro_info = calcular_ahorro_realista(datos_reales, matriz)
    
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
    
    print("AHORRO REAL CALCULADO:")
    print("-" * 40)
    print(f"• Ahorro en semillas: ${ahorro_info['ahorro_real']['semillas']:,.2f}")
    print(f"  ({datos_reales['semillas']['quintales']:,} quintales × diferencia de precio)")
    print(f"• Ahorro en fertilizantes: ${ahorro_info['ahorro_real']['fertilizantes']:,.2f}")
    print(f"  ({datos_reales['fertilizantes']['beneficiarios']:,} kits × diferencia de precio)")
    print(f"• AHORRO TOTAL REAL: ${ahorro_info['ahorro_real']['total']:,.2f}")
    print()
    
    # 4. Eficiencia real
    eficiencia_real = ahorro_info['ahorro_real']['total'] / inversion_total_gad
    
    print("4. EFICIENCIA REAL DE LA INVERSIÓN:")
    print("=" * 50)
    print(f"Inversión GAD: ${inversion_total_gad:,.2f}")
    print(f"Ahorro productores: ${ahorro_info['ahorro_real']['total']:,.2f}")
    print(f"EFICIENCIA REAL: {eficiencia_real:.2f}x")
    print()
    
    if eficiencia_real > 1.0:
        print(f"✅ POR CADA $1 INVERTIDO, SE GENERAN ${eficiencia_real:.2f} EN AHORRO")
        print("✅ EL PROGRAMA ES RENTABLE SOCIALMENTE")
    else:
        print(f"⚠️  POR CADA $1 INVERTIDO, SE GENERAN ${eficiencia_real:.2f} EN AHORRO")
        print("⚠️  EL PROGRAMA NO ES RENTABLE EN TÉRMINOS DIRECTOS")
        print("   (Pero puede tener beneficios indirectos no cuantificados)")
    print()
    
    # 5. Cobertura real del programa
    print("5. COBERTURA REAL DEL PROGRAMA:")
    print("-" * 50)
    
    cobertura_semillas = (datos_reales['semillas']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100
    cobertura_fertilizantes = (datos_reales['fertilizantes']['hectareas'] / datos_reales['total_hectareas_arroz']) * 100
    
    print(f"• Cobertura semillas: {cobertura_semillas:.1f}% ({datos_reales['semillas']['hectareas']:,.0f} de {datos_reales['total_hectareas_arroz']:,.0f} ha)")
    print(f"• Cobertura fertilizantes: {cobertura_fertilizantes:.1f}% ({datos_reales['fertilizantes']['hectareas']:,.0f} de {datos_reales['total_hectareas_arroz']:,.0f} ha)")
    print()
    
    # 6. Indicadores financieros realistas
    print("6. INDICADORES FINANCIEROS (usando matriz AGRIPAC):")
    print("-" * 50)
    
    ingresos_brutos_ha = matriz.rendimiento_sacas * matriz.precio_saca
    utilidad_sin_subsidio = ingresos_brutos_ha - costo_total_matriz
    
    # Ahorro promedio por hectárea (basado en cobertura real)
    ahorro_promedio_ha = ahorro_info['ahorro_real']['total'] / datos_reales['total_hectareas_arroz']
    utilidad_con_subsidio_real = utilidad_sin_subsidio + ahorro_promedio_ha
    
    print(f"Rendimiento esperado: {matriz.rendimiento_sacas} sacas/ha")
    print(f"Precio por saca: ${matriz.precio_saca:.2f}")
    print(f"Ingresos brutos: ${ingresos_brutos_ha:.2f}/ha")
    print(f"Costo producción: ${costo_total_matriz:.2f}/ha")
    print()
    print(f"Utilidad SIN programa: ${utilidad_sin_subsidio:.2f}/ha ({(utilidad_sin_subsidio/ingresos_brutos_ha)*100:.1f}%)")
    print(f"Ahorro promedio real: ${ahorro_promedio_ha:.2f}/ha")
    print(f"Utilidad CON programa: ${utilidad_con_subsidio_real:.2f}/ha ({(utilidad_con_subsidio_real/ingresos_brutos_ha)*100:.1f}%)")
    print(f"Mejora en utilidad: {((utilidad_con_subsidio_real - utilidad_sin_subsidio) / utilidad_sin_subsidio) * 100:.1f}%")
    print()
    
    # 7. Resumen ejecutivo
    print("7. RESUMEN EJECUTIVO - DATOS REALES:")
    print("=" * 50)
    print(f"• Inversión GAD: ${inversion_total_gad:,.2f}")
    print(f"• Ahorro productores: ${ahorro_info['ahorro_real']['total']:,.2f}")
    print(f"• Eficiencia: {eficiencia_real:.2f}x")
    print(f"• Beneficiarios directos: {datos_reales['semillas']['beneficiarios'] + datos_reales['fertilizantes']['beneficiarios']:,}")
    print(f"• Hectáreas impactadas: {datos_reales['total_hectareas_arroz']:,.0f} ha")
    print(f"• Mejora utilidad promedio: {((utilidad_con_subsidio_real - utilidad_sin_subsidio) / utilidad_sin_subsidio) * 100:.1f}%")
    print()
    
    if eficiencia_real >= 0.8:
        calificacion = "EXCELENTE"
    elif eficiencia_real >= 0.5:
        calificacion = "BUENA"
    elif eficiencia_real >= 0.3:
        calificacion = "REGULAR"
    else:
        calificacion = "DEFICIENTE"
    
    print(f"CALIFICACIÓN DEL PROGRAMA: {calificacion}")
    print()
    
    print("NOTA METODOLÓGICA:")
    print("Este análisis usa únicamente:")
    print("• Datos reales de inversión y entregas (base de datos)")
    print("• Precios estimados de mercado (+15-20% sobre precios GAD)")
    print("• Matriz de costos AGRIPAC para contexto")
    print("• NO incluye beneficios indirectos no monetizables")

if __name__ == "__main__":
    analizar_costos_realista()