"""Limpieza de datos para semillas."""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from loguru import logger


class SemillasCleaner:
    """Limpia y prepara datos de semillas para procesamiento."""
    
    def __init__(self):
        self.cleaned_count = 0
        self.issues_found = []
        
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ejecuta todas las operaciones de limpieza."""
        logger.info(f"Iniciando limpieza de {len(df)} registros")
        
        # Hacer copia para no modificar el original
        df_clean = df.copy()
        
        # Aplicar limpieza
        df_clean = self._clean_text_fields(df_clean)
        df_clean = self._clean_numeric_fields(df_clean)
        df_clean = self._clean_date_fields(df_clean)
        df_clean = self._handle_missing_values(df_clean)
        df_clean = self._remove_duplicates(df_clean)
        df_clean = self._rename_columns(df_clean)
        
        self.cleaned_count = len(df_clean)
        logger.info(f"Limpieza completada: {self.cleaned_count} registros limpios")
        
        return df_clean
    
    def _clean_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia campos de texto."""
        text_columns = [
            'numero_acta', 'documento', 'proceso', 'organizacion',
            'nombres_apellidos', 'cedula', 'telefono', 'genero',
            'canton', 'parroquia', 'localidad',
            'responsable_agencia', 'cedula_jefe_sucursal', 'sucursal',
            'observacion', 'actualizacion', 'rubro'
        ]
        
        for col in text_columns:
            if col in df.columns:
                # Convertir a string y limpiar
                df[col] = df[col].astype(str)
                df[col] = df[col].str.strip()
                df[col] = df[col].str.upper()
                
                # Reemplazar 'NAN' string con None
                df[col] = df[col].replace(['NAN', 'NONE', ''], None)
                
        logger.debug(f"Campos de texto limpiados: {len(text_columns)}")
        return df
    
    def _clean_numeric_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia campos numéricos."""
        # Edad
        if 'edad' in df.columns:
            df['edad'] = pd.to_numeric(df['edad'], errors='coerce')
            # Validar rango razonable
            df.loc[(df['edad'] < 18) | (df['edad'] > 100), 'edad'] = None
            
        # Campos decimales
        decimal_fields = [
            'hectarias_totales', 'hectarias_beneficiadas',
            'precio_unitario', 'inversion', 'score_quintil'
        ]
        
        for col in decimal_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Valores negativos a None
                df.loc[df[col] < 0, col] = None
        
        # Quintil
        if 'quintil' in df.columns:
            df['quintil'] = pd.to_numeric(df['quintil'], errors='coerce')
            # Validar rango 1-5
            df.loc[(df['quintil'] < 1) | (df['quintil'] > 5), 'quintil'] = None
            
        # A�o
        if 'anio' in df.columns:
            df['anio'] = pd.to_numeric(df['anio'], errors='coerce')
            # Validar rango razonable
            df.loc[(df['anio'] < 2000) | (df['anio'] > 2030), 'anio'] = None
            
        logger.debug("Campos num�ricos limpiados")
        return df
    
    def _clean_date_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia campos de fecha."""
        if 'fecha_retiro' in df.columns:
            # Intentar parsear diferentes formatos
            df['fecha_retiro'] = pd.to_datetime(df['fecha_retiro'], errors='coerce')
            
            # Validar fechas razonables (no futuras, no muy antiguas)
            fecha_max = pd.Timestamp.now() + pd.Timedelta(days=365)  # Max 1 a�o futuro
            fecha_min = pd.Timestamp('2000-01-01')
            
            mask = (df['fecha_retiro'] > fecha_max) | (df['fecha_retiro'] < fecha_min)
            df.loc[mask, 'fecha_retiro'] = None
            
        logger.debug("Campos de fecha limpiados")
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Maneja valores faltantes seg�n reglas de negocio."""
        # Campos que NO pueden ser nulos (marcar registro para revisi�n)
        required_fields = ['numero_acta', 'nombres_apellidos', 'canton']
        
        df['tiene_campos_requeridos'] = True
        for field in required_fields:
            if field in df.columns:
                mask = df[field].isna() | (df[field] == 'None')
                df.loc[mask, 'tiene_campos_requeridos'] = False
                if mask.sum() > 0:
                    self.issues_found.append(
                        f"Registros sin {field}: {mask.sum()}"
                    )
        
        # Valores por defecto para algunos campos
        defaults = {
            'hectarias_beneficiadas': 0,
            'inversion': 0,
            'observacion': 'SIN OBSERVACION'
        }
        
        for field, default in defaults.items():
            if field in df.columns:
                df[field] = df[field].fillna(default)
                
        logger.debug(f"Valores faltantes manejados. Issues: {len(self.issues_found)}")
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identifica y marca duplicados."""
        # Marcar duplicados por numero_acta
        if 'numero_acta' in df.columns:
            df['es_duplicado'] = df.duplicated(subset=['numero_acta'], keep='first')
            duplicados = df['es_duplicado'].sum()
            if duplicados > 0:
                self.issues_found.append(f"Actas duplicadas encontradas: {duplicados}")
                logger.warning(f"Se encontraron {duplicados} registros duplicados")
        
        return df
    
    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renombra columnas para consistencia."""
        column_mapping = {
            'cultivo': 'tipo_cultivo'  # Renombrar cultivo a tipo_cultivo para consistencia
        }
        
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name})
                logger.debug(f"Columna renombrada: {old_name} -> {new_name}")
                
        return df
    
    def get_summary(self) -> Dict:
        """Retorna resumen de la limpieza."""
        return {
            'registros_limpios': self.cleaned_count,
            'issues_encontrados': len(self.issues_found),
            'detalle_issues': self.issues_found
        }