#!/usr/bin/env python3
"""Script para inicializar la base de datos con schemas y tablas."""

from config.connections.database import DatabaseConnection
from src.models.base import Base
from sqlalchemy import text

# Importar todos los modelos para que SQLAlchemy los registre
# Modelos staging
from src.models.operational.staging.semillas_stg_model import StgSemilla
from src.models.operational.staging.fertilizantes_stg_model import StgFertilizante
from src.models.operational.staging.mecanizacion_stg_model import StgMecanizacion
from src.models.operational.staging.plantas_stg_model import StgPlantas

# Modelos operacionales refactorizados
from src.models.operational_refactored.beneficiario import Beneficiario
from src.models.operational_refactored.direccion import Direccion
from src.models.operational_refactored.asociacion import Asociacion
from src.models.operational_refactored.beneficiario_asociacion import BeneficiarioAsociacion
from src.models.operational_refactored.tipo_cultivo import TipoCultivo
from src.models.operational_refactored.beneficio import Beneficio
from src.models.operational_refactored.beneficio_semillas import BeneficioSemillas
from src.models.operational_refactored.beneficio_fertilizantes import BeneficioFertilizantes
from src.models.operational_refactored.beneficio_mecanizacion import BeneficioMecanizacion
from src.models.operational_refactored.beneficio_plantas import BeneficioPlanta


def init_database():
    """Inicializar la base de datos con schemas y tablas."""
    print("🗂️ INICIALIZANDO BASE DE DATOS ETL PRODUCTIVO")
    print("=" * 50)
    
    try:
        # Usar DatabaseConnection
        db = DatabaseConnection()
        engine = db.init_engine()
        
        print("✅ Conexión establecida")
        
        print("📊 Creando todas las tablas...")
        Base.metadata.create_all(engine)
        
        print("✅ Tablas creadas exitosamente")
        
        # Verificar tablas creadas
        with engine.connect() as conn:
            tables_query = '''
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'etl-productivo'
            ORDER BY table_name;
            '''
            result = conn.execute(text(tables_query))
            tables = result.fetchall()
            
            print(f"📋 Tablas creadas ({len(tables)}):")
            for table in tables:
                print(f"   ✅ {table.table_name}")
        
        # Insertar tipos de cultivo básicos
        print("\n🌱 Insertando tipos de cultivo básicos...")
        with engine.connect() as conn:
            # Verificar si ya existen tipos de cultivo
            check_query = '''SELECT COUNT(*) as count FROM "etl-productivo".tipo_cultivo;'''
            result = conn.execute(text(check_query))
            count = result.fetchone().count
            
            if count == 0:
                insert_cultivos = '''
                INSERT INTO "etl-productivo".tipo_cultivo (nombre) VALUES 
                ('ARROZ'),
                ('MAIZ'),
                ('CACAO'),
                ('OTROS');
                '''
                conn.execute(text(insert_cultivos))
                conn.commit()
                print("   ✅ Tipos de cultivo insertados")
            else:
                print("   ✅ Tipos de cultivo ya existen")
        
        print(f"\n🎉 INICIALIZACIÓN COMPLETADA EXITOSAMENTE")
        print("✅ Base de datos lista para ETL")
        
    except Exception as e:
        print(f"❌ ERROR en inicialización: {str(e)}")
        raise


if __name__ == "__main__":
    init_database()