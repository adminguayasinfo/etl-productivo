#!/usr/bin/env python3
"""Script para probar el mapeo de IDs en el ETL."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from src.transform.normalizers.semillas_normalizer import SemillasNormalizer
from config.connections.database import db_connection
from src.load.semillas_ops_load_core import SemillasOperationalLoaderCore
from src.models.operational.operational.persona_base_ops import PersonaBase
from src.models.operational.operational.ubicaciones_ops import Ubicacion
from src.models.operational.operational.organizaciones_ops import Organizacion
from src.models.operational.operational.beneficio_semillas_ops import BeneficioSemillas
from src.models.operational.operational.beneficiario_semillas_ops import BeneficiarioSemillas
from sqlalchemy import select, func
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_data():
    """Crea datos de prueba para el ETL."""
    return pd.DataFrame([
        {
            'nombres_apellidos': 'JUAN PEREZ',
            'cedula': '1234567890',
            'canton': 'QUITO',
            'parroquia': 'CENTRO',
            'localidad': 'BARRIO A',
            'organizacion': 'ASOCIACION AGRICOLA XYZ',
            'fecha_retiro': '2024-01-15',
            'cultivo': 'MAIZ',
            'hectarias_beneficiadas': 3.0,
            'inversion': 200.50,
            'precio_unitario': 50.0,
            'es_valido': True,
            'tiene_campos_requeridos': True
        },
        {
            'nombres_apellidos': 'MARIA GOMEZ',
            'cedula': '0987654321',
            'canton': 'GUAYAQUIL',
            'parroquia': 'TARQUI',
            'localidad': 'BARRIO B',
            'organizacion': 'COOPERATIVA ABC',
            'fecha_retiro': '2024-01-20',
            'cultivo': 'ARROZ',
            'hectarias_beneficiadas': 5.0,
            'inversion': 350.75,
            'precio_unitario': 70.0,
            'es_valido': True,
            'tiene_campos_requeridos': True
        }
    ])

def test_id_mapping():
    """Prueba el mapeo de IDs temporales a reales."""
    
    # 1. Crear datos de prueba
    logger.info("=== CREANDO DATOS DE PRUEBA ===")
    test_df = create_test_data()
    logger.info(f"Datos de prueba creados: {len(test_df)} registros")
    
    # 2. Normalizar datos
    logger.info("\n=== NORMALIZANDO DATOS ===")
    normalizer = SemillasNormalizer()
    normalized_entities = normalizer.normalize(test_df)
    
    logger.info("Entidades normalizadas:")
    for entity_type, df in normalized_entities.items():
        logger.info(f"  {entity_type}: {len(df)} registros")
        if entity_type == 'beneficios' and len(df) > 0:
            logger.info(f"  Ejemplo de beneficio normalizado:")
            logger.info(f"    Columnas: {df.columns.tolist()}")
            logger.info(f"    Primer registro: {df.iloc[0].to_dict()}")
    
    # 3. Cargar datos con el loader
    logger.info("\n=== CARGANDO DATOS A LA BASE DE DATOS ===")
    loader = SemillasOperationalLoaderCore()
    
    with db_connection.get_session() as session:
        # Cargar en orden correcto
        loader.load_batch(normalized_entities, session)
        session.commit()
        
        # 4. Verificar resultados
        logger.info("\n=== VERIFICANDO RESULTADOS ===")
        
        # Contar registros en BD
        personas_count = session.execute(select(func.count()).select_from(PersonaBase)).scalar()
        ubicaciones_count = session.execute(select(func.count()).select_from(Ubicacion)).scalar()
        organizaciones_count = session.execute(select(func.count()).select_from(Organizacion)).scalar()
        beneficios_count = session.execute(select(func.count()).select_from(BeneficioSemillas)).scalar()
        beneficiarios_count = session.execute(select(func.count()).select_from(BeneficiarioSemillas)).scalar()
            
        logger.info(f"Registros en BD:")
        logger.info(f"  Personas: {personas_count}")
        logger.info(f"  Ubicaciones: {ubicaciones_count}")
        logger.info(f"  Organizaciones: {organizaciones_count}")
        logger.info(f"  Beneficios: {beneficios_count}")
        logger.info(f"  Beneficiarios: {beneficiarios_count}")
            
        # Verificar relaciones
        beneficios = session.execute(select(BeneficioSemillas)).scalars().all()
        for beneficio in beneficios:
            logger.info(f"\nBeneficio ID {beneficio.id}:")
            logger.info(f"  Persona ID: {beneficio.persona_id}")
            logger.info(f"  Ubicacion ID: {beneficio.ubicacion_id}")
            logger.info(f"  Cultivo: {beneficio.cultivo}")
            logger.info(f"  Valor monetario: ${beneficio.valor_monetario}")
                
            # Verificar que las relaciones existen
            persona = session.get(PersonaBase, beneficio.persona_id)
            ubicacion = session.get(Ubicacion, beneficio.ubicacion_id)
            
            logger.info(f"  Persona: {persona.nombres_apellidos if persona else 'NO ENCONTRADA'}")
            logger.info(f"  Ubicacion: {ubicacion.canton if ubicacion else 'NO ENCONTRADA'}")
            
        logger.info("\n=== PRUEBA COMPLETADA EXITOSAMENTE ===")

if __name__ == "__main__":
    test_id_mapping()