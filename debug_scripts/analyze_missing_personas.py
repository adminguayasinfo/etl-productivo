#!/usr/bin/env python3
"""Script para analizar por qué se están perdiendo registros de personas."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from src.extract.semillas_staging_reader import SemillasStagingReader
from config.connections.database import db_connection
from src.transform.normalizers.semillas_normalizer import SemillasNormalizer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_missing_personas():
    """Analiza por qué se están perdiendo registros de personas."""
    
    with db_connection.get_session() as session:
        reader = SemillasStagingReader(batch_size=50000)
        
        # Obtener una muestra de datos sin procesar
        total_count = reader.read_unprocessed_count(session)
        logger.info(f'Total registros sin procesar: {total_count}')
        
        if total_count == 0:
            logger.info("No hay registros sin procesar. Analizando registros procesados...")
            # Usar SQLAlchemy para obtener datos
            from src.models.operational.staging.semillas_stg_model import StgSemilla
            from sqlalchemy import select
            
            stmt = select(StgSemilla).limit(20000)
            result = session.execute(stmt).scalars().all()
            
            if not result:
                logger.error("No se encontraron datos en staging")
                return
            
            # Convertir a diccionarios para crear DataFrame
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
        else:
            # Leer primer batch completo
            batch_df = next(reader.read_unprocessed_batches(session))
        
        logger.info(f'Registros en batch: {len(batch_df)}')
        
        # Analizar nombres_apellidos
        nombres_totales = len(batch_df)
        nombres_not_null = batch_df['nombres_apellidos'].notna().sum()
        nombres_not_empty = (batch_df['nombres_apellidos'].astype(str).str.strip() != '').sum()
        nombres_not_invalid = (~batch_df['nombres_apellidos'].astype(str).str.strip().str.lower().isin(['none', 'nan', 'null'])).sum()
        
        logger.info('=== ANÁLISIS DE NOMBRES_APELLIDOS ===')
        logger.info(f'  Total registros: {nombres_totales}')
        logger.info(f'  Not null: {nombres_not_null}')
        logger.info(f'  Not empty: {nombres_not_empty}')
        logger.info(f'  Not invalid values: {nombres_not_invalid}')
        
        # Ver algunos ejemplos de valores excluidos
        excluded = batch_df[
            batch_df['nombres_apellidos'].isna() | 
            (batch_df['nombres_apellidos'].astype(str).str.strip() == '') |
            (batch_df['nombres_apellidos'].astype(str).str.strip().str.lower().isin(['none', 'nan', 'null']))
        ]
        logger.info(f'Registros excluidos por nombres inválidos: {len(excluded)}')
        
        if len(excluded) > 0:
            logger.info('Ejemplos de valores excluidos:')
            excluded_counts = excluded['nombres_apellidos'].value_counts().head(10)
            for value, count in excluded_counts.items():
                logger.info(f'  "{value}": {count} registros')
        
        # Analizar registros válidos para deduplicación
        valid_names = batch_df[
            batch_df['nombres_apellidos'].notna() & 
            (batch_df['nombres_apellidos'].astype(str).str.strip() != '') &
            (~batch_df['nombres_apellidos'].astype(str).str.strip().str.lower().isin(['none', 'nan', 'null']))
        ]
        
        logger.info(f'\\n=== ANÁLISIS DE DEDUPLICACIÓN ===')
        logger.info(f'Registros con nombres válidos: {len(valid_names)}')
        
        # Analizar cédulas
        cedulas_validas = valid_names[valid_names['cedula'].notna() & (valid_names['cedula'] != 'None')]
        logger.info(f'  Con cédulas válidas: {len(cedulas_validas)}')
        logger.info(f'  Cédulas únicas: {cedulas_validas["cedula"].nunique()}')
        
        # Sin cédulas (solo nombres)
        sin_cedula = valid_names[valid_names['cedula'].isna() | (valid_names['cedula'] == 'None')]
        logger.info(f'  Sin cédula válida: {len(sin_cedula)}')
        logger.info(f'  Nombres únicos (sin cédula): {sin_cedula["nombres_apellidos"].nunique()}')
        
        # Simular la lógica actual de persona_key
        persona_keys = []
        for _, row in valid_names.iterrows():
            cedula = row.get('cedula') if pd.notna(row.get('cedula')) and row.get('cedula') != 'None' else None
            nombres = row.get('nombres_apellidos')
            persona_key = cedula if cedula else nombres
            persona_keys.append(persona_key)
        
        unique_keys = len(set(persona_keys))
        logger.info(f'  Persona keys únicas (lógica actual): {unique_keys}')
        
        # Probar el normalizer actual
        logger.info(f'\\n=== PRUEBA CON NORMALIZER ACTUAL ===')
        normalizer = SemillasNormalizer()
        
        # Tomar una muestra más pequeña para no saturar
        sample_df = valid_names.head(5000)
        logger.info(f'Probando con muestra de {len(sample_df)} registros')
        
        normalized_entities = normalizer.normalize(sample_df)
        personas_normalizadas = len(normalized_entities['personas'])
        
        logger.info(f'  Personas extraídas por normalizer: {personas_normalizadas}')
        logger.info(f'  Eficiencia: {personas_normalizadas/len(sample_df)*100:.1f}%')
        
        # Comparar con lógica anterior (sin filtros estrictos)
        logger.info(f'\\n=== COMPARACIÓN CON LÓGICA MÁS PERMISIVA ===')
        
        # Contar únicos sin filtros estrictos de nombres
        todos_con_nombres = batch_df[batch_df['nombres_apellidos'].notna()]
        logger.info(f'Todos los registros con nombres not null: {len(todos_con_nombres)}')
        
        # Usar solo cédula como deduplicador cuando existe
        persona_keys_permisivo = []
        for _, row in todos_con_nombres.iterrows():
            cedula = row.get('cedula') if pd.notna(row.get('cedula')) and str(row.get('cedula')).strip() not in ['None', '', 'nan'] else None
            nombres = str(row.get('nombres_apellidos')).strip() if pd.notna(row.get('nombres_apellidos')) else None
            
            if cedula:
                persona_key = cedula
            elif nombres and nombres.lower() not in ['none', 'nan', 'null', '']:
                persona_key = nombres
            else:
                continue
                
            persona_keys_permisivo.append(persona_key)
        
        unique_keys_permisivo = len(set(persona_keys_permisivo))
        logger.info(f'  Persona keys únicas (lógica permisiva): {unique_keys_permisivo}')
        logger.info(f'  Diferencia: {unique_keys_permisivo - unique_keys} registros')

if __name__ == "__main__":
    analyze_missing_personas()