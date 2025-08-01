#!/usr/bin/env python3
from database import db_connection

if __name__ == "__main__":
    print("Probando conexión a la base de datos...")
    if db_connection.test_connection():
        print("\n✓ Conexión establecida correctamente")
        
        # Prueba adicional: ejecutar una query simple
        try:
            result = db_connection.execute_query("SELECT version()")
            for row in result:
                print(f"PostgreSQL version: {row[0]}")
        except Exception as e:
            print(f"Error al ejecutar query: {e}")
    else:
        print("\n✗ No se pudo establecer la conexión")