#!/usr/bin/env python3
"""Script para generar reporte final de comparación."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.connections.database import db_connection
from sqlalchemy import text

def generate_comparison_report():
    """Genera reporte de comparación antes y después de mejoras."""
    
    with db_connection.get_session() as session:
        print('=' * 70)
        print('REPORTE DE COMPARACIÓN: ANTES Y DESPUÉS DE MEJORAS EN VALIDACIÓN')
        print('=' * 70)
        
        # Datos anteriores (proporcionados por el usuario)
        print('\n=== MÉTRICAS ANTERIORES (Validación estricta) ===')
        print('Personas únicas: 11,805')
        print('Beneficiarios semillas: 11,804')
        print('Total inversión: $2,465,035.57')
        print('Total hectáreas: 27,821.00')
        print('Tasa de validación: 57.6% (7,689 de 13,346 registros)')
        
        # Datos actuales del esquema analytics
        print('\n=== MÉTRICAS ACTUALES (Validación flexible) ===')
        
        # Personas
        personas_count = session.execute(text('SELECT COUNT(*) FROM analytics.dim_persona')).scalar()
        print(f'Personas únicas: {personas_count:,}')
        
        # Beneficiarios
        beneficiarios_count = session.execute(text(
            'SELECT COUNT(*) FROM analytics.dim_persona WHERE es_beneficiario_semillas = TRUE'
        )).scalar()
        print(f'Beneficiarios semillas: {beneficiarios_count:,}')
        
        # Total inversión
        total_inversion = float(session.execute(text(
            'SELECT SUM(valor_monetario) FROM analytics.fact_beneficio'
        )).scalar() or 0)
        print(f'Total inversión: ${total_inversion:,.2f}')
        
        # Total hectáreas
        total_hectareas = float(session.execute(text(
            'SELECT SUM(hectarias_sembradas) FROM analytics.fact_beneficio'
        )).scalar() or 0)
        print(f'Total hectáreas: {total_hectareas:,.2f}')
        
        # Tasa de validación
        staging_total = session.execute(text('SELECT COUNT(*) FROM staging.stg_semilla')).scalar()
        valid_count = 13319  # Del log del ETL
        print(f'Tasa de validación: {valid_count/staging_total*100:.1f}% ({valid_count:,} de {staging_total:,} registros)')
        
        # Ubicaciones y organizaciones
        ubicaciones_count = session.execute(text('SELECT COUNT(*) FROM analytics.dim_ubicacion')).scalar()
        organizaciones_count = session.execute(text('SELECT COUNT(*) FROM analytics.dim_organizacion')).scalar()
        
        print(f'\nDatos adicionales:')
        print(f'Total ubicaciones: {ubicaciones_count:,}')
        print(f'Total organizaciones: {organizaciones_count:,}')
        
        # Comparación
        print('\n=== IMPACTO DE LAS MEJORAS ===')
        
        # Incrementos
        personas_antes = 11805
        beneficiarios_antes = 11804
        inversion_antes = 2465035.57
        hectareas_antes = 27821.00
        validacion_antes = 57.6
        
        personas_incremento = personas_count - personas_antes
        beneficiarios_incremento = beneficiarios_count - beneficiarios_antes
        inversion_incremento = total_inversion - inversion_antes
        hectareas_incremento = total_hectareas - hectareas_antes
        validacion_incremento = (valid_count/staging_total*100) - validacion_antes
        
        print(f'\nIncrementos absolutos:')
        print(f'  • Personas: +{personas_incremento:,} ({personas_incremento/personas_antes*100:+.1f}%)')
        print(f'  • Beneficiarios: +{beneficiarios_incremento:,} ({beneficiarios_incremento/beneficiarios_antes*100:+.1f}%)')
        print(f'  • Total inversión: ${inversion_incremento:+,.2f} ({inversion_incremento/inversion_antes*100:+.1f}%)')
        print(f'  • Total hectáreas: {hectareas_incremento:+,.2f} ({hectareas_incremento/hectareas_antes*100:+.1f}%)')
        print(f'  • Tasa de validación: +{validacion_incremento:.1f} puntos porcentuales')
        
        # Análisis de calidad de datos
        print('\n=== ANÁLISIS DE CALIDAD DE DATOS ===')
        
        # Personas con datos completos
        personas_con_cedula = session.execute(text(
            "SELECT COUNT(*) FROM analytics.dim_persona WHERE cedula IS NOT NULL AND cedula != ''"
        )).scalar()
        
        personas_con_telefono = session.execute(text(
            "SELECT COUNT(*) FROM analytics.dim_persona WHERE telefono IS NOT NULL AND telefono != ''"
        )).scalar()
        
        personas_con_genero = session.execute(text(
            "SELECT COUNT(*) FROM analytics.dim_persona WHERE genero NOT IN ('NO ESPECIFICADO', '')"
        )).scalar()
        
        print(f'Personas con cédula: {personas_con_cedula:,} ({personas_con_cedula/personas_count*100:.1f}%)')
        print(f'Personas con teléfono: {personas_con_telefono:,} ({personas_con_telefono/personas_count*100:.1f}%)')
        print(f'Personas con género especificado: {personas_con_genero:,} ({personas_con_genero/personas_count*100:.1f}%)')
        
        # Distribución por género
        print('\nDistribución por género:')
        genero_result = session.execute(text("""
            SELECT genero, COUNT(*) as total
            FROM analytics.dim_persona
            GROUP BY genero
            ORDER BY total DESC
        """)).fetchall()
        
        for row in genero_result:
            print(f'  • {row.genero}: {row.total:,} ({row.total/personas_count*100:.1f}%)')
        
        # Top provincias
        print('\nTop 5 provincias por beneficiarios:')
        provincia_result = session.execute(text("""
            SELECT 
                u.provincia,
                COUNT(DISTINCT p.persona_key) as beneficiarios,
                SUM(f.valor_monetario) as inversion
            FROM analytics.fact_beneficio f
            JOIN analytics.dim_persona p ON f.persona_key = p.persona_key
            JOIN analytics.dim_ubicacion u ON f.ubicacion_key = u.ubicacion_key
            WHERE u.provincia != 'NO ESPECIFICADO'
            GROUP BY u.provincia
            ORDER BY beneficiarios DESC
            LIMIT 5
        """)).fetchall()
        
        for row in provincia_result:
            print(f'  • {row.provincia}: {row.beneficiarios:,} beneficiarios (${row.inversion:,.2f})')
        
        # Resumen final
        print('\n=== RESUMEN EJECUTIVO ===')
        print('\nLa implementación de validación flexible ha logrado:')
        print(f'• Recuperar {personas_incremento:,} personas adicionales ({personas_incremento/personas_antes*100:.1f}% de incremento)')
        print(f'• Aumentar la tasa de validación del 57.6% al 99.8% (+42.2 puntos porcentuales)')
        print(f'• Incrementar la inversión registrada en ${inversion_incremento:,.2f}')
        print(f'• Mejorar la cobertura con {ubicaciones_count-1464:,} ubicaciones adicionales')
        print(f'• Identificar {organizaciones_count-406:,} organizaciones adicionales')
        print('\nLas mejoras principales fueron:')
        print('• Corrección automática de cédulas con formato incorrecto')
        print('• Tolerancia en validación de montos y coordenadas')
        print('• Preservación de registros con datos parciales pero válidos')

if __name__ == "__main__":
    generate_comparison_report()