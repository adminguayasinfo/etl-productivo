#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Explica de dónde salen los $19.5 millones de "ahorro" de los productores
"""

from config.connections.database import db_connection
from sqlalchemy import text
from src.matriz_costos.costos_arroz import MatrizCostosArroz, CalculadoraSubsidiosGAD, CategoriaInsumo

def explicar_origen_ahorro():
    """Explica de dónde sale el cálculo de $19.5 millones de ahorro."""
    
    print("=" * 80)
    print("ORIGEN DE LOS $19.5 MILLONES DE 'AHORRO' - EXPLICACIÓN DETALLADA")
    print("=" * 80)
    print()
    
    print("⚠️  IMPORTANTE: Los $19.5 millones NO son datos reales de la base de datos")
    print("   Son una ESTIMACIÓN TEÓRICA basada en precios de mercado")
    print()
    
    # Datos reales de hectáreas
    hectareas_total = 81596.41
    hectareas_semillas = 37640.61  
    hectareas_fertilizantes = 43315.55
    
    print("1. DATOS REALES (de la base de datos):")
    print("-" * 50)
    print(f"• Total hectáreas arroz: {hectareas_total:,.2f} ha")
    print(f"• Hectáreas con semillas: {hectareas_semillas:,.2f} ha")
    print(f"• Hectáreas con fertilizantes: {hectareas_fertilizantes:,.2f} ha")
    print(f"• Inversión real GAD: $4,255,901.69")
    print()
    
    print("2. PRECIOS TEÓRICOS (de la matriz de costos):")
    print("-" * 50)
    
    # Crear matriz de costos
    matriz = MatrizCostosArroz()
    
    # Obtener costos por categoría
    costos_semillas = 0
    costos_fertilizantes = 0
    
    for item in matriz.items_costo:
        if item.categoria == CategoriaInsumo.SEMILLA:
            costos_semillas += item.costo_total
            print(f"• Semillas (matriz): ${item.costo_total:.2f}/ha ({item.concepto})")
        elif item.categoria == CategoriaInsumo.FERTILIZANTE:
            costos_fertilizantes += item.costo_total
            print(f"• Fertilizantes (matriz): ${item.costo_total:.2f}/ha ({item.concepto})")
    
    print(f"• TOTAL semillas matriz: ${costos_semillas:.2f}/ha")
    print(f"• TOTAL fertilizantes matriz: ${costos_fertilizantes:.2f}/ha")
    print()
    
    print("3. CÁLCULO TEÓRICO DEL 'AHORRO':")
    print("-" * 50)
    
    # Aplicar subsidios teóricos
    programa_subsidios = {
        CategoriaInsumo.SEMILLA: 1.0,      # 100% subsidiado
        CategoriaInsumo.FERTILIZANTE: 0.5  # 50% subsidiado
    }
    
    resultado = matriz.aplicar_subsidios_gad(programa_subsidios)
    ahorro_por_hectarea = resultado['ahorro']['monto_total']
    
    print(f"Según la matriz de costos:")
    print(f"• Ahorro por hectárea: ${ahorro_por_hectarea:.2f}")
    print(f"• Aplicado a {hectareas_total:,.0f} ha = ${ahorro_por_hectarea * hectareas_total:,.2f}")
    print()
    
    print("4. PROBLEMA CON ESTE CÁLCULO:")
    print("-" * 50)
    print("❌ SUPONE que TODOS los productores habrían comprado insumos al precio de matriz")
    print("❌ SUPONE que TODAS las hectáreas recibieron todos los beneficios")
    print("❌ USA precios teóricos, no precios reales del mercado local")
    print("❌ NO considera que algunos productores no habrían comprado insumos")
    print()
    
    print("5. CÁLCULO MÁS REALISTA:")
    print("-" * 50)
    
    with db_connection.get_session() as session:
        # Precios reales que pagó el GAD
        query_precios_gad = text('''
            SELECT 
                AVG(precio_unitario) as precio_semilla_gad,
                (SELECT AVG(precio_kit) FROM "etl-productivo".stg_fertilizante 
                 WHERE processed = true AND UPPER(TRIM(cultivo)) = 'ARROZ' AND precio_kit > 0) as precio_fertilizante_gad
            FROM "etl-productivo".stg_semilla
            WHERE processed = true AND UPPER(TRIM(cultivo)) = 'ARROZ' AND precio_unitario > 0
        ''')
        
        precios_gad = session.execute(query_precios_gad).fetchone()
        
        # Suponer precios de mercado (20-30% más altos que lo que pagó GAD)
        precio_semilla_mercado = float(precios_gad.precio_semilla_gad) * 1.25  # 25% más caro
        precio_fertilizante_mercado = float(precios_gad.precio_fertilizante_gad) * 1.20  # 20% más caro
        
        print("PRECIOS REALES:")
        print(f"• GAD pagó por semillas: ${precios_gad.precio_semilla_gad:.2f}/quintal")
        print(f"• Mercado (estimado): ${precio_semilla_mercado:.2f}/quintal")
        print(f"• GAD pagó por fertilizantes: ${precios_gad.precio_fertilizante_gad:.2f}/kit")
        print(f"• Mercado (estimado): ${precio_fertilizante_mercado:.2f}/kit")
        print()
        
        # Cálculo realista por hectáreas que realmente recibieron beneficios
        query_entregas = text('''
            SELECT 
                COUNT(*) as beneficiarios_semillas,
                SUM(entrega) as total_quintales,
                (SELECT COUNT(*) FROM "etl-productivo".stg_fertilizante 
                 WHERE processed = true AND UPPER(TRIM(cultivo)) = 'ARROZ' AND precio_kit > 0) as beneficiarios_fertilizantes
            FROM "etl-productivo".stg_semilla
            WHERE processed = true AND UPPER(TRIM(cultivo)) = 'ARROZ' AND precio_unitario > 0
        ''')
        
        entregas = session.execute(query_entregas).fetchone()
        
        # Ahorro real basado en entregas reales
        ahorro_semillas = (precio_semilla_mercado - float(precios_gad.precio_semilla_gad)) * entregas.total_quintales
        ahorro_fertilizantes = (precio_fertilizante_mercado - float(precios_gad.precio_fertilizante_gad)) * entregas.beneficiarios_fertilizantes
        
        ahorro_total_realista = ahorro_semillas + ahorro_fertilizantes
        
        print("AHORRO REALISTA (basado en entregas reales):")
        print(f"• Ahorro en semillas: ${ahorro_semillas:,.2f}")
        print(f"  ({entregas.total_quintales:,} quintales × ${precio_semilla_mercado - float(precios_gad.precio_semilla_gad):.2f} diferencia)")
        print(f"• Ahorro en fertilizantes: ${ahorro_fertilizantes:,.2f}")
        print(f"  ({entregas.beneficiarios_fertilizantes:,} kits × ${precio_fertilizante_mercado - float(precios_gad.precio_fertilizante_gad):.2f} diferencia)")
        print(f"• TOTAL REALISTA: ${ahorro_total_realista:,.2f}")
        print()
        
        eficiencia_realista = ahorro_total_realista / 4255901.69
        
        print("6. COMPARACIÓN DE EFICIENCIAS:")
        print("-" * 50)
        print(f"• Eficiencia TEÓRICA (matriz): 4.57x (${19460743.79:,.2f} ÷ $4,255,901.69)")
        print(f"• Eficiencia REALISTA: {eficiencia_realista:.2f}x (${ahorro_total_realista:,.2f} ÷ $4,255,901.69)")
        print()
        
        print("7. CONCLUSIÓN:")
        print("=" * 50)
        print("Los $19.5 millones vienen de:")
        print("1. Matriz de costos teórica ($238.50 ahorro/ha)")
        print("2. Multiplicado por 81,596 hectáreas totales")  
        print("3. Asumiendo que todos habrían comprado al precio de matriz")
        print()
        print("PERO esto es una SOBREESTIMACIÓN porque:")
        print("• No todas las hectáreas recibieron todos los beneficios")
        print("• Los precios de matriz pueden no reflejar el mercado local")
        print("• Algunos productores no habrían comprado insumos sin el programa")
        print()
        print(f"Una estimación más conservadora sería: {eficiencia_realista:.2f}x")
        print("(Que sigue siendo muy buena eficiencia)")

if __name__ == "__main__":
    explicar_origen_ahorro()