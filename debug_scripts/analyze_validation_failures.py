#!/usr/bin/env python3
"""Script para analizar por qué fallan las validaciones."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from src.transform.validators.data_validator import DataValidator
from src.transform.cleaners.semillas_cleaner import SemillasCleaner
from src.transform.cleaners.data_standardizer import DataStandardizer
from src.models.operational.staging.semillas_stg_model import StgSemilla
from config.connections.database import db_connection
from sqlalchemy import select
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_validation_failures():
    """Analiza por qué fallan las validaciones."""
    
    with db_connection.get_session() as session:
        # Obtener muestra de datos
        stmt = select(StgSemilla).limit(5000)
        result = session.execute(stmt).scalars().all()
        
        logger.info(f'Analizando {len(result)} registros de staging')
        
        # Convertir a DataFrame
        data_dicts = []
        for row in result:
            data_dicts.append({
                'numero_acta': row.numero_acta,
                'documento': row.documento,
                'proceso': row.proceso,
                'organizacion': row.organizacion,
                'nombres_apellidos': row.nombres_apellidos,
                'cedula': row.cedula,
                'telefono': row.telefono,
                'genero': row.genero,
                'edad': row.edad,
                'coordenada_x': row.coordenada_x,
                'coordenada_y': row.coordenada_y,
                'canton': row.canton,
                'parroquia': row.parroquia,
                'localidad': row.localidad,
                'cultivo': row.cultivo,
                'hectarias_totales': row.hectarias_totales,
                'hectarias_beneficiadas': row.hectarias_beneficiadas,
                'precio_unitario': row.precio_unitario,
                'inversion': row.inversion,
                'quintil': row.quintil,
                'score_quintil': row.score_quintil,
                'responsable_agencia': row.responsable_agencia,
                'cedula_jefe_sucursal': row.cedula_jefe_sucursal,
                'sucursal': row.sucursal,
                'fecha_retiro': row.fecha_retiro,
                'anio': row.anio,
                'observacion': row.observacion
            })
        
        df = pd.DataFrame(data_dicts)
        logger.info(f'DataFrame creado con {len(df)} registros')
        
        # Aplicar limpieza y estandarización
        cleaner = SemillasCleaner()
        df_cleaned = cleaner.clean(df)
        
        standardizer = DataStandardizer()
        df_standardized = standardizer.standardize(df_cleaned)
        
        # Aplicar validación
        validator = DataValidator()
        df_validated = validator.validate(df_standardized)
        
        # Analizar resultados
        logger.info(f'\n=== RESULTADOS DE VALIDACIÓN ===')
        logger.info(f'Total registros: {len(df_validated)}')
        logger.info(f'Registros válidos: {df_validated["es_valido"].sum()}')
        logger.info(f'Registros inválidos: {(~df_validated["es_valido"]).sum()}')
        logger.info(f'Tasa de validez: {df_validated["es_valido"].sum() / len(df_validated) * 100:.1f}%')
        
        # Analizar tipos de errores
        logger.info(f'\n=== ANÁLISIS DE ERRORES ===')
        invalid_df = df_validated[~df_validated['es_valido']]
        
        if len(invalid_df) > 0:
            # Contar errores por tipo
            error_counts = {}
            for _, row in invalid_df.iterrows():
                errors = row['errores_validacion'].split('; ')
                for error in errors:
                    if error:
                        if error not in error_counts:
                            error_counts[error] = 0
                        error_counts[error] += 1
            
            # Ordenar por frecuencia
            sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
            
            logger.info('Errores más comunes:')
            for error, count in sorted_errors:
                percentage = count / len(invalid_df) * 100
                logger.info(f'  {error}: {count} ({percentage:.1f}% de los inválidos)')
        
        # Analizar específicamente cada validación
        logger.info(f'\n=== ANÁLISIS DETALLADO POR VALIDACIÓN ===')
        
        # 1. Cédulas
        cedulas_con_valor = df_validated[df_validated['cedula'].notna() & (df_validated['cedula'] != 'None')]
        logger.info(f'\n1. CÉDULAS:')
        logger.info(f'   Total con cédula: {len(cedulas_con_valor)}')
        
        cedula_errors = invalid_df[invalid_df['errores_validacion'].str.contains('Cédula inválida', na=False)]
        logger.info(f'   Con cédula inválida: {len(cedula_errors)}')
        if len(cedula_errors) > 0:
            logger.info('   Ejemplos de cédulas inválidas:')
            for i, (idx, row) in enumerate(cedula_errors.head(5).iterrows()):
                logger.info(f'     - {row["cedula"]} ({row["nombres_apellidos"]})')
        
        # 2. Fechas
        fecha_errors = invalid_df[invalid_df['errores_validacion'].str.contains('Año inconsistente', na=False)]
        logger.info(f'\n2. FECHAS:')
        logger.info(f'   Con año inconsistente: {len(fecha_errors)}')
        if len(fecha_errors) > 0:
            logger.info('   Ejemplos:')
            for i, (idx, row) in enumerate(fecha_errors.head(5).iterrows()):
                logger.info(f'     - Fecha: {row["fecha_retiro"]}, Año: {row["anio"]}')
        
        # 3. Montos
        monto_errors = invalid_df[invalid_df['errores_validacion'].str.contains('Inversión incorrecta', na=False)]
        logger.info(f'\n3. MONTOS:')
        logger.info(f'   Con inversión incorrecta: {len(monto_errors)}')
        if len(monto_errors) > 0:
            logger.info('   Ejemplos:')
            for i, (idx, row) in enumerate(monto_errors.head(5).iterrows()):
                calc = row['hectarias_beneficiadas'] * row['precio_unitario'] if pd.notna(row['hectarias_beneficiadas']) and pd.notna(row['precio_unitario']) else 0
                logger.info(f'     - Hectáreas: {row["hectarias_beneficiadas"]}, Precio: {row["precio_unitario"]}, Inversión: {row["inversion"]} (calc: {calc:.2f})')
        
        hectarea_errors = invalid_df[invalid_df['errores_validacion'].str.contains('Hectáreas beneficiadas > totales', na=False)]
        logger.info(f'   Con hectáreas beneficiadas > totales: {len(hectarea_errors)}')
        
        # 4. Coordenadas
        coord_x_errors = invalid_df[invalid_df['errores_validacion'].str.contains('Coordenada X fuera de rango', na=False)]
        coord_y_errors = invalid_df[invalid_df['errores_validacion'].str.contains('Coordenada Y fuera de rango', na=False)]
        logger.info(f'\n4. COORDENADAS:')
        logger.info(f'   Con coordenada X fuera de rango: {len(coord_x_errors)}')
        logger.info(f'   Con coordenada Y fuera de rango: {len(coord_y_errors)}')
        
        # 5. Relaciones
        org_errors = invalid_df[invalid_df['errores_validacion'].str.contains('Organización sin beneficiario', na=False)]
        cultivo_errors = invalid_df[invalid_df['errores_validacion'].str.contains('Hectáreas sin cultivo', na=False)]
        logger.info(f'\n5. RELACIONES:')
        logger.info(f'   Organización sin beneficiario: {len(org_errors)}')
        logger.info(f'   Hectáreas sin cultivo: {len(cultivo_errors)}')
        
        # Analizar registros con múltiples errores
        logger.info(f'\n=== REGISTROS CON MÚLTIPLES ERRORES ===')
        invalid_df['num_errores'] = invalid_df['errores_validacion'].str.count(';')
        multi_error_counts = invalid_df['num_errores'].value_counts().sort_index()
        for num_errors, count in multi_error_counts.items():
            if num_errors > 0:
                logger.info(f'   {num_errors} errores: {count} registros')

if __name__ == "__main__":
    analyze_validation_failures()