"""Cleaner específico para datos de fertilizantes."""
import pandas as pd
import numpy as np
from typing import Dict, Any
from loguru import logger


class FertilizantesCleaner:
    """Limpia y estandariza datos de fertilizantes."""
    
    def __init__(self):
        self.stats = {
            'total_registros': 0,
            'registros_limpios': 0,
            'valores_nulos_corregidos': 0,
            'valores_invalidos_corregidos': 0,
            'fechas_corregidas': 0,
            'numeros_corregidos': 0
        }
        
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia el DataFrame completo."""
        logger.info(f"Iniciando limpieza de {len(df)} registros de fertilizantes")
        self.stats['total_registros'] = len(df)
        
        # Copiar para no modificar original
        df_clean = df.copy()
        
        # Limpiar cada tipo de dato
        df_clean = self._clean_text_fields(df_clean)
        df_clean = self._clean_numeric_fields(df_clean)
        df_clean = self._clean_date_fields(df_clean)
        df_clean = self._clean_specific_fields(df_clean)
        
        # Validar registros limpios
        self.stats['registros_limpios'] = len(df_clean[df_clean['nombres_apellidos'].notna()])
        
        logger.info(f"Limpieza completada. Registros válidos: {self.stats['registros_limpios']}")
        return df_clean
        
    def _clean_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia campos de texto."""
        text_fields = [
            'numero_acta', 'organizacion', 'nombres_apellidos', 
            'telefono', 'genero', 'canton', 'parroquia', 'localidad',
            'tipo_fertilizante', 'marca_fertilizante', 'responsable_agencia',
            'sucursal', 'observacion'
        ]
        
        for field in text_fields:
            if field in df.columns:
                # Convertir a string y limpiar espacios
                df[field] = df[field].astype(str).str.strip()
                
                # Reemplazar 'nan' por NaN
                df.loc[df[field] == 'nan', field] = np.nan
                df.loc[df[field] == '', field] = np.nan
                
                # Estandarizar mayúsculas para ciertos campos
                if field in ['nombres_apellidos', 'organizacion', 'canton', 'parroquia', 'localidad', 'genero']:
                    df[field] = df[field].str.upper()
                    
        # Correcciones específicas de género
        if 'genero' in df.columns:
            df['genero'] = df['genero'].replace({
                'M': 'MASCULINO',
                'F': 'FEMENINO',
                'MASC': 'MASCULINO',
                'FEM': 'FEMENINO',
                'HOMBRE': 'MASCULINO',
                'MUJER': 'FEMENINO'
            })
            self.stats['valores_invalidos_corregidos'] += df['genero'].isin(['MASCULINO', 'FEMENINO']).sum()
            
        return df
        
    def _clean_numeric_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia campos numéricos."""
        numeric_fields = {
            'edad': 'int',
            'hectarias_totales': 'float',
            'hectarias_beneficiadas': 'float',
            'cantidad_sacos': 'int',
            'peso_por_saco': 'float',
            'precio_unitario': 'float',
            'costo_total': 'float',
            'quintil': 'int',
            'score_quintil': 'float'
        }
        
        for field, dtype in numeric_fields.items():
            if field in df.columns:
                # Convertir a numérico
                if dtype == 'int':
                    df[field] = pd.to_numeric(df[field], errors='coerce')
                    # Convertir a int donde no sea NaN
                    mask = df[field].notna()
                    df.loc[mask, field] = df.loc[mask, field].astype(int)
                else:
                    df[field] = pd.to_numeric(df[field], errors='coerce')
                    
                # Contar correcciones
                self.stats['numeros_corregidos'] += df[field].isna().sum()
                
        # Validaciones específicas
        if 'edad' in df.columns:
            # Edad debe estar entre 0 y 120
            invalid_age = (df['edad'] < 0) | (df['edad'] > 120)
            df.loc[invalid_age, 'edad'] = np.nan
            self.stats['valores_invalidos_corregidos'] += invalid_age.sum()
            
        # Hectáreas beneficiadas no puede ser mayor que hectáreas totales
        if 'hectarias_totales' in df.columns and 'hectarias_beneficiadas' in df.columns:
            invalid_hectarias = df['hectarias_beneficiadas'] > df['hectarias_totales']
            df.loc[invalid_hectarias, 'hectarias_beneficiadas'] = df.loc[invalid_hectarias, 'hectarias_totales']
            self.stats['valores_invalidos_corregidos'] += invalid_hectarias.sum()
            
        return df
        
    def _clean_date_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia campos de fecha."""
        if 'fecha_entrega' in df.columns:
            # Convertir a datetime
            df['fecha_entrega'] = pd.to_datetime(df['fecha_entrega'], errors='coerce')
            
            # Validar fechas razonables (no futuras, no muy antiguas)
            today = pd.Timestamp.now()
            min_date = pd.Timestamp('2020-01-01')
            
            invalid_dates = (df['fecha_entrega'] > today) | (df['fecha_entrega'] < min_date)
            self.stats['fechas_corregidas'] = invalid_dates.sum()
            
            # Corregir fechas inválidas a None
            df.loc[invalid_dates, 'fecha_entrega'] = pd.NaT
            
        return df
        
    def _clean_specific_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia campos específicos de fertilizantes."""
        
        # Limpiar cédula
        if 'cedula' in df.columns:
            # Remover caracteres no numéricos
            df['cedula'] = df['cedula'].astype(str).str.replace(r'\D', '', regex=True)
            # Reemplazar strings vacíos con NaN
            df.loc[df['cedula'] == '', 'cedula'] = np.nan
            # Validar longitud de cédula (10 o 13 dígitos para Ecuador)
            invalid_cedula = ~df['cedula'].isna() & ~df['cedula'].str.len().isin([10, 13])
            df.loc[invalid_cedula, 'cedula'] = np.nan
            self.stats['valores_invalidos_corregidos'] += invalid_cedula.sum()
            
        # Limpiar teléfono
        if 'telefono' in df.columns:
            # Mantener solo números y algunos caracteres especiales
            df['telefono'] = df['telefono'].astype(str).str.replace(r'[^\d\-\s/]', '', regex=True)
            df.loc[df['telefono'].str.strip() == '', 'telefono'] = np.nan
            
        # Estandarizar tipo de fertilizante
        if 'tipo_fertilizante' in df.columns:
            df['tipo_fertilizante'] = df['tipo_fertilizante'].str.upper()
            # Mapear valores comunes
            fertilizante_map = {
                'MAIZ': 'MAÍZ',
                'CAFE': 'CAFÉ',
                'PLATANO': 'PLÁTANO',
                'LIMON': 'LIMÓN'
            }
            df['tipo_fertilizante'] = df['tipo_fertilizante'].replace(fertilizante_map)
            
        # Estandarizar tipo de cultivo (para consistencia con dim_cultivo)
        if 'tipo_cultivo' in df.columns:
            df['tipo_cultivo'] = df['tipo_cultivo'].str.upper().str.strip()
            # Mapear cultivos para consistencia con el catálogo
            cultivo_map = {
                'MAÍZ': 'MAIZ',      # Remover acento para consistencia
                'PLÁTANO': 'PLATANO', # Remover acento para consistencia  
                'CAFE': 'CACAO',     # Posible error común
                'PLATANO': 'PLATANO',
                'BANANO': 'BANANO',
                'ARROZ': 'ARROZ',
                'CACAO': 'CACAO'
            }
            df['tipo_cultivo'] = df['tipo_cultivo'].replace(cultivo_map)
            
        # Limpiar coordenadas
        for coord in ['coordenada_x', 'coordenada_y']:
            if coord in df.columns:
                # Convertir a string y limpiar
                df[coord] = df[coord].astype(str).str.strip()
                # Remover valores no numéricos
                df[coord] = df[coord].str.replace(r'[^\d\.\-]', '', regex=True)
                df.loc[df[coord] == '', coord] = np.nan
                
        return df
        
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estadísticas de limpieza."""
        return self.stats.copy()