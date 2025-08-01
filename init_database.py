#!/usr/bin/env python3
"""Script para inicializar la base de datos con schemas y tablas."""

from config.connections.database import db_connection
from src.models.base import Base

# Importar todos los modelos para que SQLAlchemy los registre
from src.models.operational.staging.semillas_stg_model import StgSemilla
from src.models.operational.staging.fertilizantes_stg_model import StgFertilizante
from src.models.operational.operational.persona_base_ops import PersonaBase
from src.models.operational.operational.beneficiario_semillas_ops import BeneficiarioSemillas
from src.models.operational.operational.beneficiario_fertilizantes_ops import BeneficiarioFertilizantes
from src.models.operational.operational.ubicaciones_ops import Ubicacion
from src.models.operational.operational.organizaciones_ops import Organizacion
from src.models.operational.operational.beneficio_base_ops import BeneficioBase
from src.models.operational.operational.beneficio_semillas_ops import BeneficioSemillas
from src.models.operational.operational.beneficio_fertilizantes_ops import BeneficioFertilizantes

# Importar modelos analíticos
from src.models.analytical.dimensions import DimPersona, DimUbicacion, DimOrganizacion, DimTiempo
from src.models.analytical.facts import FactBeneficio


def init_database():
    """Inicializar la base de datos con schemas y tablas."""
    print("=== Inicializando Base de Datos ===\n")
    
    # Probar conexión
    if not db_connection.test_connection():
        print("No se pudo conectar a la base de datos. Verifica la configuración.")
        return
    
    print("\n--- Creando Schemas ---")
    db_connection.create_schemas()
    
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
    else:
        print("No se encontraron tablas.")
    
    print("\n✓ Inicialización completada")


if __name__ == "__main__":
    init_database()