#!/usr/bin/env python3
"""
Script para crear la tabla beneficio_fertilizantes en la base de datos.
"""

import sys
import os

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.connections.database import db_connection
from src.models.base import Base

# Importar los modelos necesarios para la herencia
from src.models.operational_refactored.direccion import Direccion
from src.models.operational_refactored.asociacion import Asociacion
from src.models.operational_refactored.tipo_cultivo import TipoCultivo
from src.models.operational_refactored.beneficiario import Beneficiario
from src.models.operational_refactored.beneficio import Beneficio
from src.models.operational_refactored.beneficio_fertilizantes import BeneficioFertilizantes


def create_beneficio_fertilizantes_table():
    """Crear la tabla beneficio_fertilizantes y sus dependencias."""
    print("=== CREANDO TABLA BENEFICIO_FERTILIZANTES ===\n")
    
    # Probar conexión
    if not db_connection.test_connection():
        print("❌ No se pudo conectar a la base de datos. Verifica la configuración.")
        return False
    
    print("✅ Conexión exitosa a la base de datos")
    
    try:
        # Crear schemas si no existen
        print("\n--- Creando Schemas ---")
        db_connection.create_schemas(['etl-productivo'])
        
        # Crear todas las tablas de la metadata
        print("\n--- Creando Tablas ---")
        db_connection.create_all_tables(Base)
        
        # Verificar que la tabla se creó correctamente
        print("\n--- Verificando Tabla Creada ---")
        result = db_connection.execute_query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'etl-productivo' AND table_name = 'beneficio_fertilizantes';"
        )
        
        if result:
            print("✅ Tabla 'beneficio_fertilizantes' creada exitosamente")
            
            # Mostrar estructura de la tabla
            print("\n--- Estructura de la Tabla ---")
            columns = db_connection.execute_query(
                "SELECT column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = 'etl-productivo' AND table_name = 'beneficio_fertilizantes' "
                "ORDER BY ordinal_position;"
            )
            
            print("Columnas de beneficio_fertilizantes:")
            for col_name, data_type, is_nullable in columns:
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                print(f"  - {col_name}: {data_type} {nullable}")
                
        else:
            print("❌ No se pudo crear la tabla 'beneficio_fertilizantes'")
            return False
            
        # Verificar la tabla padre también
        print("\n--- Verificando Tabla Padre ---")
        result = db_connection.execute_query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'etl-productivo' AND table_name = 'beneficio';"
        )
        
        if result:
            print("✅ Tabla padre 'beneficio' existe")
        else:
            print("⚠️  Tabla padre 'beneficio' no encontrada")
        
        print("\n=== CREACIÓN COMPLETADA ===")
        return True
        
    except Exception as e:
        print(f"❌ Error durante la creación: {e}")
        return False


def test_model_creation():
    """Prueba básica de creación del modelo."""
    print("\n=== PRUEBA DEL MODELO ===")
    
    try:
        # Crear una instancia para probar la clase
        fertilizante = BeneficioFertilizantes(
            tipo_beneficio='FERTILIZANTES',
            fertilizante_nitrogenado=10,
            npk_elementos_menores=5,
            organico_foliar=3
        )
        
        print(f"✅ Modelo creado: {fertilizante}")
        print(f"✅ Total fertilizantes: {fertilizante.calcular_total_fertilizantes()}")
        print(f"✅ Tipos fertilizantes: {fertilizante.obtener_tipos_fertilizantes()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en prueba del modelo: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Iniciando creación de tabla beneficio_fertilizantes...")
    
    # Crear tabla
    table_created = create_beneficio_fertilizantes_table()
    
    # Probar modelo
    model_works = test_model_creation()
    
    if table_created and model_works:
        print("\n🎉 ¡Tabla y modelo creados exitosamente!")
        print("\nEl modelo BeneficioFertilizantes está listo para usar.")
        print("\nPróximos pasos:")
        print("1. Crear pipeline ETL para cargar datos desde staging")
        print("2. Implementar transformaciones específicas de fertilizantes")
        print("3. Agregar el modelo a los scripts de consulta de hectáreas")
    else:
        print("\n❌ Hubo problemas durante la creación. Revisar errores arriba.")
        sys.exit(1)