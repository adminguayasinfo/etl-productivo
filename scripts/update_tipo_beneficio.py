#!/usr/bin/env python3
"""Script para actualizar tipo_beneficio a SEMILLAS."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.connections.database import db_connection
from sqlalchemy import text

def update_tipo_beneficio():
    """Actualiza todos los registros para que tipo_beneficio sea SEMILLAS."""
    
    with db_connection.get_session() as session:
        try:
            # Primero veamos el estado actual
            print("=== ESTADO ACTUAL ===")
            current = session.execute(text("""
                SELECT tipo_beneficio, COUNT(*) as total
                FROM operational.beneficio_base 
                GROUP BY tipo_beneficio
                ORDER BY tipo_beneficio
            """)).fetchall()
            
            for row in current:
                print(f"  - {row.tipo_beneficio}: {row.total} registros")
            
            # Actualizar todos los registros
            print("\n=== ACTUALIZANDO REGISTROS ===")
            result = session.execute(text("""
                UPDATE operational.beneficio_base 
                SET tipo_beneficio = 'SEMILLAS' 
                WHERE tipo_beneficio <> 'SEMILLAS' OR tipo_beneficio IS NULL
            """))
            
            session.commit()
            print(f"✓ Actualizados {result.rowcount} registros")
            
            # Verificar el resultado
            print("\n=== ESTADO FINAL ===")
            final = session.execute(text("""
                SELECT tipo_beneficio, COUNT(*) as total
                FROM operational.beneficio_base 
                GROUP BY tipo_beneficio
            """)).fetchall()
            
            for row in final:
                print(f"  - {row.tipo_beneficio}: {row.total} registros")
                
        except Exception as e:
            print(f"✗ Error: {str(e)}")
            session.rollback()
            raise

if __name__ == "__main__":
    update_tipo_beneficio()