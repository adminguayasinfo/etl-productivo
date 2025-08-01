#!/usr/bin/env python3
"""Script para limpiar completamente la base de datos."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.connections.database import db_connection
from sqlalchemy import text
import time

def clean_database():
    """Limpia todas las tablas de la base de datos."""
    
    with db_connection.get_session() as session:
        print("=== LIMPIANDO BASE DE DATOS ===\n")
        
        try:
            # Limpiar tablas analytics
            print("Limpiando tablas analytics...")
            session.execute(text('TRUNCATE TABLE "etl-productivo".fact_beneficio CASCADE'))
            session.execute(text('TRUNCATE TABLE "etl-productivo".dim_persona CASCADE'))
            session.execute(text('TRUNCATE TABLE "etl-productivo".dim_ubicacion CASCADE'))
            session.execute(text('TRUNCATE TABLE "etl-productivo".dim_organizacion CASCADE'))
            session.execute(text('TRUNCATE TABLE "etl-productivo".dim_tiempo CASCADE'))
            session.commit()
            print("✓ Tablas analytics limpiadas")
            
            # Limpiar tablas operational
            print("\nLimpiando tablas operational...")
            session.execute(text('TRUNCATE TABLE "etl-productivo".beneficio_semillas CASCADE'))
            session.execute(text('TRUNCATE TABLE "etl-productivo".beneficiario_semillas CASCADE'))
            session.execute(text('TRUNCATE TABLE "etl-productivo".beneficio_base CASCADE'))
            session.execute(text('TRUNCATE TABLE "etl-productivo".persona_base CASCADE'))
            session.execute(text('TRUNCATE TABLE "etl-productivo".ubicacion CASCADE'))
            session.execute(text('TRUNCATE TABLE "etl-productivo".organizacion CASCADE'))
            session.commit()
            print("✓ Tablas operational limpiadas")
            
            # Limpiar tablas staging
            print("\nLimpiando tablas staging...")
            session.execute(text('TRUNCATE TABLE "etl-productivo".stg_semilla CASCADE'))
            session.execute(text('TRUNCATE TABLE "etl-productivo".stg_fertilizante CASCADE'))
            session.commit()
            print("✓ Tablas staging limpiadas")
            
            # Verificar que todo esté limpio
            print("\n=== VERIFICACIÓN DE LIMPIEZA ===")
            
            # Verificar staging
            count = session.execute(text('SELECT COUNT(*) FROM "etl-productivo".stg_semilla')).scalar()
            print(f"Registros en stg_semilla: {count}")
            count = session.execute(text('SELECT COUNT(*) FROM "etl-productivo".stg_fertilizante')).scalar()
            print(f"Registros en stg_fertilizante: {count}")
            
            # Verificar operational
            tables_operational = [
                'persona_base', 'ubicacion', 'organizacion', 
                'beneficio_base', 'beneficio_semillas', 'beneficiario_semillas'
            ]
            for table in tables_operational:
                count = session.execute(text(f'SELECT COUNT(*) FROM "etl-productivo".{table}')).scalar()
                print(f"Registros en {table}: {count}")
            
            # Verificar analytics
            tables_analytics = [
                'dim_persona', 'dim_ubicacion', 'dim_organizacion', 
                'dim_tiempo', 'fact_beneficio'
            ]
            for table in tables_analytics:
                count = session.execute(text(f'SELECT COUNT(*) FROM "etl-productivo".{table}')).scalar()
                print(f"Registros en {table}: {count}")
            
            print("\n✓ Base de datos limpiada exitosamente")
            
        except Exception as e:
            print(f"\n✗ Error al limpiar la base de datos: {str(e)}")
            session.rollback()
            raise

if __name__ == "__main__":
    clean_database()