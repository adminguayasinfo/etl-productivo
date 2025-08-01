#!/usr/bin/env python3
"""Script para inicializar la base de datos con los modelos refactorizados."""

from config.connections.database import db_connection
from src.models.base import Base

# Importar modelos de staging
from src.models.operational.staging.semillas_stg_model import StgSemilla
from src.models.operational.staging.fertilizantes_stg_model import StgFertilizante
from src.models.operational.staging.mecanizacion_stg_model import StgMecanizacion
from src.models.operational.staging.plantas_stg_model import StgPlantas

# Importar modelos operacionales refactorizados
from src.models.operational_refactored.direccion import Direccion
from src.models.operational_refactored.asociacion import Asociacion
from src.models.operational_refactored.tipo_cultivo import TipoCultivo
from src.models.operational_refactored.beneficiario import Beneficiario
from src.models.operational_refactored.beneficio import Beneficio
from src.models.operational_refactored.beneficio_semillas import BeneficioSemillas
from src.models.operational_refactored.beneficio_mecanizacion import BeneficioMecanizacion
from src.models.operational_refactored.beneficio_plantas import BeneficioPlanta
from src.models.operational_refactored.beneficiario_asociacion import BeneficiarioAsociacion

# Importar modelos analíticos (existentes)
from src.models.analytical.dimensions import DimPersona, DimUbicacion, DimOrganizacion, DimTiempo, DimCultivo
from src.models.analytical.facts import FactBeneficio


def init_database_refactored():
    """Inicializa la base de datos con los modelos refactorizados."""
    print("=== Inicializando Base de Datos Refactorizada ===\n")
    
    # Probar conexión
    if not db_connection.test_connection():
        print("No se pudo conectar a la base de datos. Verifica la configuración.")
        return
    
    print("\n--- Creando Schema ---")
    db_connection.create_schemas(['etl-productivo'])
    
    print("\n--- Creando Tablas ---")
    db_connection.create_all_tables(Base)
    
    print("\n--- Verificando Tablas Creadas ---")
    tables = db_connection.get_table_info()
    
    if tables:
        print("\nTablas encontradas:")
        current_schema = None
        for schema, table, table_type in tables:
            if schema != current_schema:
                current_schema = schema
                print(f"\nSchema: {schema}")
            print(f"  - {table} ({table_type})")
        
        # Mostrar estadísticas por tipo de tabla
        print("\n--- Estadísticas por Tipo ---")
        staging_tables = [t for s, t, _ in tables if t.startswith('stg_')]
        operational_tables = [t for s, t, _ in tables if not t.startswith('stg_') and not t.startswith('dim_') and not t.startswith('fact_')]
        analytical_tables = [t for s, t, _ in tables if t.startswith('dim_') or t.startswith('fact_')]
        
        print(f"Staging: {len(staging_tables)} tablas")
        print(f"Operational: {len(operational_tables)} tablas")
        print(f"Analytical: {len(analytical_tables)} tablas")
        print(f"Total: {len(tables)} tablas")
        
    else:
        print("No se encontraron tablas.")
    
    print("\n✓ Inicialización completada")


if __name__ == "__main__":
    init_database_refactored()