#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consulta los tipos de cultivo beneficiados por mecanización
"""

from config.connections.database import db_connection
from sqlalchemy import text

def consultar_cultivos_mecanizacion():
    """Consulta los cultivos beneficiados por mecanización."""
    
    with db_connection.get_session() as session:
        # Tipos de cultivo beneficiados por mecanización
        query = text('''
            SELECT 
                UPPER(TRIM(cultivo)) as cultivo,
                COUNT(*) as beneficiarios,
                SUM(COALESCE(hectareas_beneficiadas, 0)) as total_hectareas,
                AVG(COALESCE(hectareas_beneficiadas, 0)) as promedio_hectareas,
                SUM(COALESCE(inversion, 0)) as total_inversion,
                AVG(COALESCE(inversion, 0)) as promedio_inversion
            FROM "etl-productivo".stg_mecanizacion
            WHERE processed = true 
            AND cultivo IS NOT NULL
            AND TRIM(cultivo) != ''
            GROUP BY UPPER(TRIM(cultivo))
            ORDER BY total_hectareas DESC
        ''')
        
        result = session.execute(query).fetchall()
        
        print('CULTIVOS BENEFICIADOS POR MECANIZACIÓN:')
        print('=' * 60)
        print()
        
        total_beneficiarios = 0
        total_hectareas = 0
        total_inversion = 0
        
        for row in result:
            print(f'{row.cultivo}:')
            print(f'  • Beneficiarios: {row.beneficiarios:,}')
            print(f'  • Hectáreas: {row.total_hectareas:,.2f} ha')
            print(f'  • Promedio: {row.promedio_hectareas:.2f} ha/beneficiario')
            print(f'  • Inversión total: ${row.total_inversion:,.2f}')
            print(f'  • Promedio inversión: ${row.promedio_inversion:.2f}/beneficiario')
            print()
            
            total_beneficiarios += row.beneficiarios
            total_hectareas += row.total_hectareas
            total_inversion += row.total_inversion
        
        print('RESUMEN TOTAL MECANIZACIÓN:')
        print('-' * 40)
        print(f'Total beneficiarios: {total_beneficiarios:,}')
        print(f'Total hectáreas: {total_hectareas:,.2f} ha')
        print(f'Total inversión: ${total_inversion:,.2f}')
        print()
        
        # Verificar si hay registros sin cultivo especificado
        query_sin_cultivo = text('''
            SELECT 
                COUNT(*) as registros_sin_cultivo,
                SUM(COALESCE(hectareas_beneficiadas, 0)) as hectareas_sin_cultivo,
                SUM(COALESCE(inversion, 0)) as inversion_sin_cultivo
            FROM "etl-productivo".stg_mecanizacion
            WHERE processed = true 
            AND (cultivo IS NULL OR TRIM(cultivo) = '')
        ''')
        
        sin_cultivo = session.execute(query_sin_cultivo).fetchone()
        
        if sin_cultivo.registros_sin_cultivo > 0:
            print('REGISTROS SIN CULTIVO ESPECIFICADO:')
            print('-' * 40)
            print(f'Registros: {sin_cultivo.registros_sin_cultivo:,}')
            print(f'Hectáreas: {sin_cultivo.hectareas_sin_cultivo:,.2f} ha')
            print(f'Inversión: ${sin_cultivo.inversion_sin_cultivo:,.2f}')
        
        # Mostrar algunos registros específicos como ejemplo
        print()
        print('EJEMPLOS DE REGISTROS:')
        print('-' * 30)
        
        query_ejemplos = text('''
            SELECT 
                nombres_apellidos,
                cultivo,
                hectareas_beneficiadas,
                inversion
            FROM "etl-productivo".stg_mecanizacion
            WHERE processed = true 
            AND cultivo IS NOT NULL
            AND TRIM(cultivo) != ''
            LIMIT 5
        ''')
        
        ejemplos = session.execute(query_ejemplos).fetchall()
        
        for ejemplo in ejemplos:
            inversion_str = f"${ejemplo.inversion:.2f}" if ejemplo.inversion is not None else "$0.00"
            hectareas_str = f"{ejemplo.hectareas_beneficiadas:.2f}" if ejemplo.hectareas_beneficiadas is not None else "0.00"
            print(f'{ejemplo.nombres_apellidos[:25]:<25} | {ejemplo.cultivo:<10} | {hectareas_str:>6} ha | {inversion_str:>10}')

if __name__ == "__main__":
    consultar_cultivos_mecanizacion()