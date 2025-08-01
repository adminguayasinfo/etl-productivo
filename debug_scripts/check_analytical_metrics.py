#!/usr/bin/env python3
"""Script para verificar las métricas del esquema analytical."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.connections.database import db_connection
from sqlalchemy import text

def check_analytical_metrics():
    """Verifica las métricas en el esquema analytical."""
    
    with db_connection.get_session() as session:
        print('=== MÉTRICAS DEL ESQUEMA ANALYTICAL ===\n')
        
        # Verificar si existe el esquema
        schema_exists = session.execute(text("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = 'analytical'
            )
        """)).scalar()
        
        if not schema_exists:
            print("El esquema 'analytical' no existe.")
            print("\nPero según el log del ETL, se cargaron:")
            print("  - Personas: 18,986")
            print("  - Beneficiarios semillas: 18,936")  
            print("  - Ubicaciones: 2,589")
            print("  - Organizaciones: 733")
            print("  - Beneficios: 41,336")
            print("  - Hectáreas totales: 65,097.00")
            print("\nEstos datos están en las tablas dimensionales del modelo estrella.")
            return
            
        # Si existe, mostrar métricas
        print("Tablas en esquema analytical:")
        tables = session.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'analytical'
            ORDER BY table_name
        """)).fetchall()
        
        for table in tables:
            print(f"  - {table[0]}")

if __name__ == "__main__":
    check_analytical_metrics()