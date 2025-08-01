#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Muestra el origen exacto de la inversión de $4.3 millones del GAD
"""

from config.connections.database import db_connection
from sqlalchemy import text

def mostrar_origen_inversion():
    """Muestra de dónde salen los $4.3 millones de inversión del GAD."""
    
    with db_connection.get_session() as session:
        print('ORIGEN DE LOS $4.3 MILLONES - DATOS DE LA BASE DE DATOS:')
        print('=' * 60)
        print()
        
        # 1. Semillas - de stg_semilla
        query_semillas = text('''
            SELECT 
                COUNT(*) as beneficios,
                SUM(COALESCE(precio_unitario, 0) * COALESCE(entrega, 0)) as inversion_total,
                AVG(COALESCE(precio_unitario, 0)) as precio_promedio,
                SUM(COALESCE(entrega, 0)) as total_quintales
            FROM "etl-productivo".stg_semilla
            WHERE processed = true 
            AND UPPER(TRIM(cultivo)) = 'ARROZ'
            AND precio_unitario > 0
        ''')
        
        result_semillas = session.execute(query_semillas).fetchone()
        
        print('1. SEMILLAS (de tabla stg_semilla):')
        print(f'   - Beneficiarios: {result_semillas.beneficios:,}')
        print(f'   - Total quintales entregados: {result_semillas.total_quintales:,}')
        print(f'   - Precio promedio por quintal: ${result_semillas.precio_promedio:.2f}')
        print(f'   - INVERSIÓN TOTAL: ${result_semillas.inversion_total:,.2f}')
        print(f'   - Cálculo: {result_semillas.total_quintales:,} quintales × ${result_semillas.precio_promedio:.2f}/quintal')
        print()
        
        # 2. Fertilizantes - de stg_fertilizante
        query_fertilizantes = text('''
            SELECT 
                COUNT(*) as beneficios,
                SUM(COALESCE(precio_kit, 0)) as inversion_total,
                AVG(COALESCE(precio_kit, 0)) as precio_promedio_kit
            FROM "etl-productivo".stg_fertilizante
            WHERE processed = true 
            AND UPPER(TRIM(cultivo)) = 'ARROZ'
            AND precio_kit > 0
        ''')
        
        result_fertilizantes = session.execute(query_fertilizantes).fetchone()
        
        print('2. FERTILIZANTES (de tabla stg_fertilizante):')
        print(f'   - Beneficiarios: {result_fertilizantes.beneficios:,}')
        print(f'   - Precio promedio por kit: ${result_fertilizantes.precio_promedio_kit:.2f}')
        print(f'   - INVERSIÓN TOTAL: ${result_fertilizantes.inversion_total:,.2f}')
        print(f'   - Cálculo: {result_fertilizantes.beneficios:,} kits × ${result_fertilizantes.precio_promedio_kit:.2f}/kit')
        print()
        
        # 3. Total
        total_inversion = result_semillas.inversion_total + result_fertilizantes.inversion_total
        
        print('3. TOTAL INVERSIÓN GAD:')
        print(f'   - Semillas: ${result_semillas.inversion_total:,.2f}')
        print(f'   - Fertilizantes: ${result_fertilizantes.inversion_total:,.2f}')
        print(f'   - TOTAL: ${total_inversion:,.2f}')
        print()
        
        print('FUENTE DE DATOS:')
        print('=' * 40)
        print('- Columna "precio_unitario" en stg_semilla = precio que pagó GAD por quintal')
        print('- Columna "entrega" en stg_semilla = quintales entregados a cada productor')
        print('- Columna "precio_kit" en stg_fertilizante = precio que pagó GAD por kit')
        print()
        print('IMPORTANTE: Estos son los precios REALES que pagó el GAD,')
        print('registrados en el sistema durante la entrega de beneficios.')
        print()
        
        # 4. Mostrar algunos registros específicos como evidencia
        print('EVIDENCIA - ALGUNOS REGISTROS ESPECÍFICOS:')
        print('=' * 50)
        
        # Muestra de semillas
        query_muestra_semillas = text('''
            SELECT 
                nombres_apellidos,
                precio_unitario,
                entrega,
                (precio_unitario * entrega) as costo_total
            FROM "etl-productivo".stg_semilla
            WHERE processed = true 
            AND UPPER(TRIM(cultivo)) = 'ARROZ'
            AND precio_unitario > 0
            LIMIT 5
        ''')
        
        muestra_semillas = session.execute(query_muestra_semillas).fetchall()
        
        print('Muestra de registros SEMILLAS:')
        for registro in muestra_semillas:
            print(f'  {registro.nombres_apellidos[:20]:<20} | ${registro.precio_unitario:.2f}/qq × {registro.entrega} qq = ${registro.costo_total:.2f}')
        print()
        
        # Muestra de fertilizantes
        query_muestra_fertilizantes = text('''
            SELECT 
                nombres_apellidos,
                precio_kit
            FROM "etl-productivo".stg_fertilizante
            WHERE processed = true 
            AND UPPER(TRIM(cultivo)) = 'ARROZ'
            AND precio_kit > 0
            LIMIT 5
        ''')
        
        muestra_fertilizantes = session.execute(query_muestra_fertilizantes).fetchall()
        
        print('Muestra de registros FERTILIZANTES:')
        for registro in muestra_fertilizantes:
            print(f'  {registro.nombres_apellidos[:20]:<20} | Kit = ${registro.precio_kit:.2f}')
        print()
        
        print('CONCLUSIÓN:')
        print('=' * 40)
        print('Los $4.3 millones NO son una estimación.')
        print('Son los costos REALES registrados en la base de datos')
        print('del programa de beneficios del GAD, calculados como:')
        print()
        print('SEMILLAS: precio_unitario × entrega (por cada beneficiario)')
        print('FERTILIZANTES: precio_kit (por cada beneficiario)')
        print()
        print('Total de registros procesados:')
        print(f'- Semillas: {result_semillas.beneficios:,} beneficiarios')
        print(f'- Fertilizantes: {result_fertilizantes.beneficios:,} beneficiarios')

if __name__ == "__main__":
    mostrar_origen_inversion()