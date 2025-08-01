#!/usr/bin/env python3
"""Script para obtener las métricas actuales del esquema operational."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.connections.database import db_connection
from sqlalchemy import text

def get_current_metrics():
    """Obtiene las métricas actuales del esquema operational."""
    
    with db_connection.get_session() as session:
        print('=== MÉTRICAS ACTUALES DEL SISTEMA ===\n')
        
        # Personas
        personas_count = session.execute(text('SELECT COUNT(*) FROM operational.persona_base')).scalar()
        print(f'Personas únicas: {personas_count:,}')
        
        # Beneficiarios semillas
        beneficiarios_count = session.execute(text('SELECT COUNT(DISTINCT persona_id) FROM operational.beneficiario_semillas')).scalar()
        print(f'Beneficiarios semillas: {beneficiarios_count:,}')
        
        # Total inversión
        total_inversion = float(session.execute(text('SELECT SUM(inversion) FROM operational.beneficio_semillas')).scalar() or 0)
        print(f'Total inversión: ${total_inversion:,.2f}')
        
        # Total hectáreas
        total_hectareas = float(session.execute(text('SELECT SUM(hectarias_beneficiadas) FROM operational.beneficio_semillas')).scalar() or 0)
        print(f'Total hectáreas: {total_hectareas:,.2f}')
        
        # Ubicaciones
        ubicaciones_count = session.execute(text('SELECT COUNT(*) FROM operational.ubicacion')).scalar()
        print(f'Total ubicaciones: {ubicaciones_count:,}')
        
        # Organizaciones
        organizaciones_count = session.execute(text('SELECT COUNT(*) FROM operational.organizacion')).scalar()
        print(f'Total organizaciones: {organizaciones_count:,}')
        
        # Beneficios
        beneficios_count = session.execute(text('SELECT COUNT(*) FROM operational.beneficio_semillas')).scalar()
        print(f'Total beneficios: {beneficios_count:,}')
        
        # Comparación con datos anteriores
        print(f'\n=== COMPARACIÓN CON DATOS ANTERIORES ===\n')
        
        # Datos anteriores (proporcionados por el usuario)
        personas_antes = 11805
        beneficiarios_antes = 11804
        inversion_antes = 2465035.57
        hectareas_antes = 27821
        
        print('Incrementos:')
        print(f'  - Personas: {personas_count:,} → {personas_count - personas_antes:+,} ({(personas_count - personas_antes)/personas_antes*100:+.1f}%)')
        print(f'  - Beneficiarios: {beneficiarios_count:,} → {beneficiarios_count - beneficiarios_antes:+,} ({(beneficiarios_count - beneficiarios_antes)/beneficiarios_antes*100:+.1f}%)')
        print(f'  - Total inversión: ${total_inversion:,.2f} → ${total_inversion - inversion_antes:+,.2f} ({(total_inversion - inversion_antes)/inversion_antes*100:+.1f}%)')
        print(f'  - Total hectáreas: {total_hectareas:,.2f} → {total_hectareas - hectareas_antes:+,.2f} ({(total_hectareas - hectareas_antes)/hectareas_antes*100:+.1f}%)')
        
        # Calidad de datos
        print(f'\n=== CALIDAD DE DATOS ===\n')
        
        # Personas con datos completos
        personas_completas = session.execute(text('''
            SELECT COUNT(*) FROM operational.persona_base 
            WHERE cedula IS NOT NULL AND cedula != ''
        ''')).scalar()
        
        print(f'Personas con cédula: {personas_completas:,} ({personas_completas/personas_count*100:.1f}%)')
        
        # Validación exitosa
        staging_total = session.execute(text('SELECT COUNT(*) FROM staging.stg_semilla')).scalar()
        print(f'\nRegistros en staging: {staging_total:,}')
        print(f'Tasa de carga exitosa: {beneficios_count/staging_total*100:.1f}%')

if __name__ == "__main__":
    get_current_metrics()