#!/usr/bin/env python3
"""Debug script para analizar tipo_cultivo en fertilizantes."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.connections.database import DatabaseConnection
from sqlalchemy import text

def main():
    db = DatabaseConnection()
    db.init_engine()
    
    with db.get_session() as session:
        print("=== Análisis de tipo_cultivo en fertilizantes ===\n")
        
        # 1. Distribución en staging
        print("1. Distribución en staging:")
        result = session.execute(text("""
            SELECT tipo_cultivo, COUNT(*) as count
            FROM staging.stg_fertilizante 
            GROUP BY tipo_cultivo 
            ORDER BY COUNT(*) DESC
        """))
        for row in result:
            print(f"   {row.tipo_cultivo}: {row.count}")
        
        # 2. Distribución en operational beneficios
        print("\n2. Distribución en operational beneficios:")
        result = session.execute(text("""
            SELECT bf.tipo_cultivo, COUNT(*) as count
            FROM operational.beneficio_fertilizantes bf
            GROUP BY bf.tipo_cultivo 
            ORDER BY COUNT(*) DESC
        """))
        for row in result:
            print(f"   {row.tipo_cultivo}: {row.count}")
        
        # 3. Verificar dim_cultivo
        print("\n3. Cultivos en dim_cultivo:")
        result = session.execute(text("""
            SELECT codigo_cultivo, nombre_cultivo, tipo_ciclo
            FROM analytics.dim_cultivo 
            ORDER BY codigo_cultivo
        """))
        for row in result:
            print(f"   {row.codigo_cultivo}: {row.nombre_cultivo} (ciclo: {row.tipo_ciclo})")
        
        # 4. Verificar algunos registros específicos
        print("\n4. Muestra de registros con tipo_cultivo NULL/vacío en staging:")
        result = session.execute(text("""
            SELECT id, nombres_apellidos, tipo_cultivo
            FROM staging.stg_fertilizante 
            WHERE tipo_cultivo IS NULL OR tipo_cultivo = '' 
            LIMIT 5
        """))
        for row in result:
            print(f"   ID {row.id}: {row.nombres_apellidos} -> tipo_cultivo: '{row.tipo_cultivo}'")

if __name__ == "__main__":
    main()