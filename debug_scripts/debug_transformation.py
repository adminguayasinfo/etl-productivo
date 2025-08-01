#!/usr/bin/env python3
"""
Script para debuggear problemas en la transformación de fertilizantes.
Identifica exactamente dónde fallan los registros pendientes.
"""

import sys
import os
from datetime import datetime
import pandas as pd
import traceback

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.connections.database import db_connection
from src.transform.fertilizantes_transformer import FertilizantesTransformer
from src.transform.cleaners.fertilizantes_cleaner import FertilizantesCleaner
from src.transform.validators.data_validator_flexible import DataValidatorFlexible
from src.transform.normalizers.fertilizantes_normalizer import FertilizantesNormalizer

def debug_transformation():
    """Debuggea la transformación paso a paso."""
    
    print("=== DEBUG TRANSFORMACIÓN FERTILIZANTES ===")
    
    # 1. Obtener muestra de registros pendientes
    print("\n1. Obteniendo muestra de registros pendientes...")
    query = """
    SELECT * FROM staging.stg_fertilizante 
    WHERE processed = false 
    LIMIT 50
    """
    
    try:
        result = db_connection.execute_query(query)
        columns = [
            'id', 'numero_acta', 'documento', 'proceso', 'organizacion',
            'nombres_apellidos', 'cedula', 'telefono', 'genero', 'edad',
            'coordenada_x', 'coordenada_y', 'canton', 'parroquia', 'localidad',
            'hectarias_totales', 'hectarias_beneficiadas', 'tipo_fertilizante',
            'marca_fertilizante', 'cantidad_sacos', 'peso_por_saco', 'precio_unitario',
            'costo_total', 'quintil', 'score_quintil', 'responsable_agencia',
            'cedula_jefe_sucursal', 'sucursal', 'fecha_entrega', 'anio',
            'observacion', 'actualizacion', 'rubro', 'processed', 'error_message',
            'created_at', 'updated_at'
        ]
        
        df = pd.DataFrame(result, columns=columns)
        print(f"   Obtenidos {len(df)} registros pendientes")
        
        if len(df) == 0:
            print("   No hay registros pendientes")
            return
            
        # 2. Test de limpieza
        print("\n2. Probando limpieza de datos...")
        cleaner = FertilizantesCleaner()
        try:
            df_cleaned = cleaner.clean(df.copy())
            print(f"   ✓ Limpieza exitosa: {len(df_cleaned)} registros")
        except Exception as e:
            print(f"   ✗ Error en limpieza: {str(e)}")
            traceback.print_exc()
            return
            
        # 3. Test de validación
        print("\n3. Probando validación...")
        validator = DataValidatorFlexible()
        try:
            df_validated = validator.validate(df_cleaned.copy())
            valid_count = df_validated['es_valido'].sum()
            invalid_count = len(df_validated) - valid_count
            print(f"   ✓ Validación exitosa: {valid_count} válidos, {invalid_count} inválidos")
            
            if invalid_count > 0:
                print("   Errores de validación encontrados:")
                invalid_df = df_validated[~df_validated['es_valido']]
                for idx, row in invalid_df.iterrows():
                    if row['errores_validacion']:
                        print(f"     - Registro {idx}: {row['errores_validacion']}")
                        print(f"       Nombre: {row['nombres_apellidos']}")
                        print(f"       Cédula: {row['cedula']}")
                        print(f"       Coords: ({row['coordenada_x']}, {row['coordenada_y']})")
                        
        except Exception as e:
            print(f"   ✗ Error en validación: {str(e)}")
            traceback.print_exc()
            return
            
        # 4. Test de normalización
        print("\n4. Probando normalización...")
        normalizer = FertilizantesNormalizer()
        try:
            # Solo usar registros válidos para normalización
            valid_df = df_validated[df_validated['es_valido']].copy()
            if len(valid_df) > 0:
                entities = normalizer.normalize(valid_df)
                print(f"   ✓ Normalización exitosa")
                print(f"   Entidades generadas:")
                for entity_type, entity_list in entities.items():
                    print(f"     - {entity_type}: {len(entity_list)} registros")
            else:
                print("   ⚠ No hay registros válidos para normalizar")
                
        except Exception as e:
            print(f"   ✗ Error en normalización: {str(e)}")
            traceback.print_exc()
            return
            
        # 5. Análisis de problemas específicos
        print("\n5. Análisis de problemas específicos...")
        
        # Analizar coordenadas
        print("   Coordenadas:")
        coord_issues = 0
        for idx, row in df.iterrows():
            try:
                if pd.notna(row['coordenada_x']):
                    x = float(row['coordenada_x'])
                    if not ((-82 <= x <= -75) or (500000 <= x <= 800000)):
                        coord_issues += 1
                if pd.notna(row['coordenada_y']):
                    y = float(row['coordenada_y'])
                    if not ((-5 <= y <= 2) or (9700000 <= y <= 10100000)):
                        coord_issues += 1
            except:
                coord_issues += 1
        print(f"     - Registros con coordenadas problemáticas: {coord_issues}")
        
        # Analizar cédulas
        print("   Cédulas:")
        cedula_issues = 0
        for idx, row in df.iterrows():
            cedula = str(row['cedula']).strip() if pd.notna(row['cedula']) else ''
            if not cedula or len(cedula) < 8:
                cedula_issues += 1
        print(f"     - Registros con cédulas problemáticas: {cedula_issues}")
        
        # Analizar nombres
        print("   Nombres:")
        nombre_issues = 0
        for idx, row in df.iterrows():
            nombre = str(row['nombres_apellidos']).strip() if pd.notna(row['nombres_apellidos']) else ''
            if not nombre or nombre.lower() in ['none', 'null', '']:
                nombre_issues += 1
        print(f"     - Registros sin nombres válidos: {nombre_issues}")
        
        print("\n=== RESUMEN ===")
        print(f"Total registros analizados: {len(df)}")
        print(f"Registros que pasarían validación: {valid_count if 'valid_count' in locals() else 'N/A'}")
        print(f"Principales problemas identificados:")
        print(f"  - Coordenadas fuera de rango: {coord_issues}")
        print(f"  - Cédulas inválidas: {cedula_issues}")
        print(f"  - Nombres faltantes: {nombre_issues}")
        
    except Exception as e:
        print(f"Error general: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    # Verificar conexión primero
    if not db_connection.test_connection():
        print("Error: No se puede conectar a la base de datos")
        sys.exit(1)
        
    debug_transformation()