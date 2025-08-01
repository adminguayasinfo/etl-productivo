#!/usr/bin/env python3
"""Script para comparar las métricas analíticas antes y después de las mejoras."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.connections.database import db_connection
from sqlalchemy import text
import pandas as pd

def compare_analytics():
    """Compara las métricas analíticas actuales con las anteriores."""
    
    with db_connection.get_session() as session:
        print('=== RESUMEN DEL PROCESO ETL - DATOS ACTUALIZADOS ===\n')
        
        # Datos procesados
        staging_count = session.execute(text('SELECT COUNT(*) FROM staging.stg_semilla')).scalar()
        print(f'Datos procesados:')
        print(f'  - CSV original: {staging_count:,} registros')
        
        # Registros válidos (aproximación basada en el último run)
        print(f'  - Registros válidos: 13,319 (99.8%)')
        print(f'  - Registros procesados: {staging_count:,}')
        
        print(f'\nConteos finales:')
        
        # Personas
        personas_count = session.execute(text('SELECT COUNT(*) FROM analytics.dim_persona')).scalar()
        print(f'  - Personas: {personas_count:,}')
        
        # Beneficiarios semillas
        beneficiarios_count = session.execute(text('SELECT COUNT(*) FROM analytics.dim_persona WHERE es_beneficiario_semillas = TRUE')).scalar()
        print(f'  - Beneficiarios semillas: {beneficiarios_count:,}')
        
        # Ubicaciones
        ubicaciones_count = session.execute(text('SELECT COUNT(*) FROM analytics.dim_ubicacion')).scalar()
        print(f'  - Ubicaciones: {ubicaciones_count:,}')
        
        # Organizaciones
        organizaciones_count = session.execute(text('SELECT COUNT(*) FROM analytics.dim_organizacion')).scalar()
        print(f'  - Organizaciones: {organizaciones_count:,}')
        
        # Beneficios
        beneficios_count = session.execute(text('SELECT COUNT(*) FROM analytics.fact_beneficio')).scalar()
        print(f'  - Beneficios: {beneficios_count:,}')
        
        print(f'\nAnálisis financiero:')
        
        # Total inversión
        total_inversion = session.execute(text('SELECT SUM(valor_monetario) FROM analytical.fact_beneficio')).scalar() or 0
        print(f'  - Total inversión: ${total_inversion:,.2f}')
        
        # Promedio por beneficio
        promedio_beneficio = session.execute(text('SELECT AVG(valor_monetario) FROM analytical.fact_beneficio WHERE valor_monetario > 0')).scalar() or 0
        print(f'  - Promedio por beneficio: ${promedio_beneficio:.2f}')
        
        # Promedio por beneficiario
        if beneficiarios_count > 0:
            promedio_beneficiario = total_inversion / beneficiarios_count
            print(f'  - Promedio por beneficiario: ${promedio_beneficiario:.2f}')
        
        # Total hectáreas
        total_hectareas = session.execute(text('SELECT SUM(hectarias_totales) FROM analytical.dim_persona WHERE hectarias_totales IS NOT NULL')).scalar() or 0
        print(f'  - Total hectáreas: {total_hectareas:,.2f}')
        
        # Inversión por hectárea
        if total_hectareas > 0:
            inversion_hectarea = total_inversion / total_hectareas
            print(f'  - Inversión por hectárea: ${inversion_hectarea:.2f}')
        
        # Comparación con datos anteriores
        print(f'\n=== COMPARACIÓN CON DATOS ANTERIORES ===\n')
        
        # Datos anteriores
        personas_antes = 11805
        beneficiarios_antes = 11804
        ubicaciones_antes = 1464
        organizaciones_antes = 406
        beneficios_antes = 12477
        inversion_antes = 2465035.57
        hectareas_antes = 27821
        
        print('Incrementos absolutos:')
        print(f'  - Personas: {personas_count - personas_antes:+,} ({(personas_count - personas_antes)/personas_antes*100:+.1f}%)')
        print(f'  - Beneficiarios: {beneficiarios_count - beneficiarios_antes:+,} ({(beneficiarios_count - beneficiarios_antes)/beneficiarios_antes*100:+.1f}%)')
        print(f'  - Ubicaciones: {ubicaciones_count - ubicaciones_antes:+,} ({(ubicaciones_count - ubicaciones_antes)/ubicaciones_antes*100:+.1f}%)')
        print(f'  - Organizaciones: {organizaciones_count - organizaciones_antes:+,} ({(organizaciones_count - organizaciones_antes)/organizaciones_antes*100:+.1f}%)')
        print(f'  - Beneficios: {beneficios_count - beneficios_antes:+,} ({(beneficios_count - beneficios_antes)/beneficios_antes*100:+.1f}%)')
        print(f'  - Total inversión: ${total_inversion - inversion_antes:+,.2f} ({(total_inversion - inversion_antes)/inversion_antes*100:+.1f}%)')
        print(f'  - Total hectáreas: {total_hectareas - hectareas_antes:+,.2f} ({(total_hectareas - hectareas_antes)/hectareas_antes*100:+.1f}%)')
        
        # Análisis adicional
        print(f'\n=== ANÁLISIS ADICIONAL ===\n')
        
        # Distribución por género
        genero_query = '''
        SELECT 
            genero,
            COUNT(*) as total,
            COUNT(CASE WHEN es_beneficiario_semillas THEN 1 END) as beneficiarios
        FROM analytical.dim_persona
        GROUP BY genero
        ORDER BY total DESC
        '''
        genero_result = session.execute(text(genero_query)).fetchall()
        
        print('Distribución por género:')
        for row in genero_result:
            print(f'  - {row.genero}: {row.total:,} personas ({row.beneficiarios:,} beneficiarios)')
        
        # Top provincias
        provincia_query = '''
        SELECT 
            u.provincia,
            COUNT(DISTINCT p.persona_key) as beneficiarios,
            SUM(f.valor_monetario) as inversion_total
        FROM analytical.fact_beneficio f
        JOIN analytical.dim_persona p ON f.persona_key = p.persona_key
        JOIN analytical.dim_ubicacion u ON f.ubicacion_key = u.ubicacion_key
        WHERE u.provincia != 'NO ESPECIFICADO'
        GROUP BY u.provincia
        ORDER BY beneficiarios DESC
        LIMIT 5
        '''
        provincia_result = session.execute(text(provincia_query)).fetchall()
        
        print('\nTop 5 provincias por beneficiarios:')
        for row in provincia_result:
            print(f'  - {row.provincia}: {row.beneficiarios:,} beneficiarios (${row.inversion_total:,.2f})')
        
        # Calidad de datos
        calidad_query = '''
        SELECT 
            COUNT(*) as total,
            COUNT(NULLIF(cedula, '')) as con_cedula,
            COUNT(telefono) as con_telefono,
            COUNT(CASE WHEN genero NOT IN ('NO ESPECIFICADO', '') THEN 1 END) as con_genero,
            COUNT(edad_actual) as con_edad
        FROM analytical.dim_persona
        '''
        calidad = session.execute(text(calidad_query)).first()
        
        print('\nCalidad de datos de personas:')
        print(f'  - Con cédula: {calidad.con_cedula:,} ({calidad.con_cedula/calidad.total*100:.1f}%)')
        print(f'  - Con teléfono: {calidad.con_telefono:,} ({calidad.con_telefono/calidad.total*100:.1f}%)')
        print(f'  - Con género especificado: {calidad.con_genero:,} ({calidad.con_genero/calidad.total*100:.1f}%)')
        print(f'  - Con edad: {calidad.con_edad:,} ({calidad.con_edad/calidad.total*100:.1f}%)')
        
        # Resumen de mejoras
        print('\n=== RESUMEN DE MEJORAS ===\n')
        print('La validación flexible ha permitido:')
        print(f'  - Recuperar {personas_count - personas_antes:,} personas adicionales')
        print(f'  - Aumentar la tasa de validación del 57.6% al 99.8%')
        print(f'  - Incrementar los beneficios registrados en {beneficios_count - beneficios_antes:,}')
        print(f'  - Aumentar la inversión registrada en ${total_inversion - inversion_antes:,.2f}')
        print(f'  - Mejorar la cobertura geográfica con {ubicaciones_count - ubicaciones_antes:,} ubicaciones adicionales')

if __name__ == "__main__":
    compare_analytics()