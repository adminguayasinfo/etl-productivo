#!/usr/bin/env python3
"""Script para analizar por qué el loader no está insertando todas las personas."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from src.transform.normalizers.semillas_normalizer import SemillasNormalizer
from src.load.semillas_ops_load_core import SemillasOperationalLoaderCore
from config.connections.database import db_connection
from src.models.operational.staging.semillas_stg_model import StgSemilla
from src.models.operational.operational.persona_base_ops import PersonaBase
from sqlalchemy import select, func
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_loader_issue():
    """Analiza por qué el loader no está insertando todas las personas."""
    
    with db_connection.get_session() as session:
        # 1. Contar personas en BD antes
        personas_en_bd_antes = session.execute(select(func.count()).select_from(PersonaBase)).scalar()
        logger.info(f'Personas en BD antes: {personas_en_bd_antes}')
        
        # 2. Obtener muestra de datos de staging
        stmt = select(StgSemilla).limit(1000)  # Muestra más pequeña para debug
        result = session.execute(stmt).scalars().all()
        logger.info(f'Registros de staging obtenidos: {len(result)}')
        
        # Convertir a DataFrame
        data_dicts = []
        for row in result:
            data_dicts.append({
                'id': row.id,
                'cedula': row.cedula,
                'nombres_apellidos': row.nombres_apellidos,
                'telefono': row.telefono,
                'genero': row.genero,
                'edad': row.edad,
                'canton': row.canton,
                'parroquia': row.parroquia,
                'localidad': row.localidad,
                'organizacion': row.organizacion,
                'cultivo': row.cultivo,
                'hectarias_beneficiadas': row.hectarias_beneficiadas,
                'precio_unitario': row.precio_unitario,
                'inversion': row.inversion,
                'fecha_retiro': row.fecha_retiro,
                'anio': row.anio,
                'sucursal': row.sucursal,
                'responsable_agencia': row.responsable_agencia,
                'cedula_jefe_sucursal': row.cedula_jefe_sucursal,
                'quintil': row.quintil,
                'score_quintil': row.score_quintil,
                'numero_acta': row.numero_acta,
                'documento': row.documento,
                'proceso': row.proceso,
                'observacion': row.observacion
            })
        
        batch_df = pd.DataFrame(data_dicts)
        
        # 3. Normalizar datos
        logger.info('=== NORMALIZANDO DATOS ===')
        normalizer = SemillasNormalizer()
        normalized_entities = normalizer.normalize(batch_df)
        
        personas_normalizadas = len(normalized_entities['personas'])
        logger.info(f'Personas normalizadas: {personas_normalizadas}')
        
        # 4. Intentar cargar con el loader y ver qué pasa
        logger.info('=== CARGANDO DATOS ===')
        loader = SemillasOperationalLoaderCore()
        
        # Analizar el DataFrame de personas antes de cargar
        personas_df = normalized_entities['personas']
        logger.info(f'Columnas en personas_df: {personas_df.columns.tolist()}')
        
        # Ver algunos ejemplos
        logger.info('Ejemplos de personas a cargar:')
        for i in range(min(5, len(personas_df))):
            row = personas_df.iloc[i]
            logger.info(f'  {i+1}: cedula={row.get("cedula")}, nombres={row.get("nombres_apellidos")}')
        
        # Cargar datos
        try:
            load_result = loader.load_batch(normalized_entities, session)
            session.commit()
            logger.info(f'Resultado de carga: {load_result}')
        except Exception as e:
            logger.error(f'Error durante la carga: {str(e)}')
            session.rollback()
            
        # 5. Contar personas en BD después
        personas_en_bd_despues = session.execute(select(func.count()).select_from(PersonaBase)).scalar()
        logger.info(f'Personas en BD después: {personas_en_bd_despues}')
        logger.info(f'Incremento: {personas_en_bd_despues - personas_en_bd_antes}')
        
        # 6. Verificar algunas de las personas que deberían haberse insertado
        logger.info('=== VERIFICANDO INSERCIÓN ===')
        for i in range(min(3, len(personas_df))):
            row = personas_df.iloc[i]
            cedula = row.get("cedula")
            nombres = row.get("nombres_apellidos")
            
            if cedula:
                persona_en_bd = session.execute(
                    select(PersonaBase).where(PersonaBase.cedula == cedula)
                ).scalar()
            else:
                persona_en_bd = session.execute(
                    select(PersonaBase).where(PersonaBase.nombres_apellidos == nombres)
                ).scalar()
                
            logger.info(f'Persona {i+1} (cedula={cedula}, nombres={nombres}): {"ENCONTRADA" if persona_en_bd else "NO ENCONTRADA"}')

if __name__ == "__main__":
    analyze_loader_issue()