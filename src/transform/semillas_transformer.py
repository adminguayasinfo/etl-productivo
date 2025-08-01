"""
Transformador para datos del programa de semillas.
Solo se encarga de limpiar, estandarizar, validar, normalizar y enriquecer datos.
"""
import pandas as pd
from typing import Dict, Tuple
from loguru import logger

from src.transform.cleaners.semillas_cleaner import SemillasCleaner
from src.transform.cleaners.data_standardizer import DataStandardizer
from src.transform.validators.data_validator_flexible import DataValidatorFlexible
from src.transform.normalizers.semillas_normalizer import SemillasNormalizer
from src.transform.enrichers.cultivo_enricher import CultivoEnricher


class SemillasTransformer:
    """Transformador que solo se encarga de las transformaciones de datos."""
    
    def __init__(self):
        """Inicializa los componentes de transformación."""
        self.cleaner = SemillasCleaner()
        self.standardizer = DataStandardizer()
        self.validator = DataValidatorFlexible()
        self.normalizer = SemillasNormalizer()
        self.cultivo_enricher = CultivoEnricher()
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia los datos.
        
        Args:
            df: DataFrame con datos crudos
            
        Returns:
            DataFrame con datos limpios
        """
        return self.cleaner.clean(df)
    
    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Estandariza los datos.
        
        Args:
            df: DataFrame con datos limpios
            
        Returns:
            DataFrame con datos estandarizados
        """
        return self.standardizer.standardize(df)
    
    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Valida los datos y separa válidos de inválidos.
        
        Args:
            df: DataFrame con datos estandarizados
            
        Returns:
            Tupla (df_validos, df_invalidos)
        """
        df_validated = self.validator.validate(df)
        
        # Separar válidos e inválidos
        df_valid = df_validated[df_validated['es_valido'] == True].copy()
        df_invalid = df_validated[df_validated['es_valido'] == False].copy()
        
        logger.info(f"Registros válidos: {len(df_valid)}, inválidos: {len(df_invalid)}")
        
        return df_valid, df_invalid
    
    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enriquece los datos con información adicional.
        
        Args:
            df: DataFrame con datos validados
            
        Returns:
            DataFrame con datos enriquecidos
        """
        if 'tipo_cultivo' not in df.columns:
            logger.warning("No se encontró columna 'tipo_cultivo' para enriquecer")
            return df
            
        df_enriched = df.copy()
        
        # Obtener información enriquecida para cada cultivo único
        cultivos_unicos = df_enriched['tipo_cultivo'].dropna().unique()
        cultivo_info = {}
        
        for cultivo in cultivos_unicos:
            enriched_data = self.cultivo_enricher.enrich(cultivo)
            cultivo_info[cultivo] = enriched_data
            
        # Agregar columnas enriquecidas al DataFrame
        for col in ['nombre_cientifico', 'familia_botanica', 'tipo_ciclo', 
                    'clasificacion_economica', 'uso_principal']:
            df_enriched[f'cultivo_{col}'] = df_enriched['tipo_cultivo'].map(
                lambda x: cultivo_info.get(x, {}).get(col) if pd.notna(x) else None
            )
        
        logger.info(f"Enriquecidos {len(cultivos_unicos)} cultivos únicos")
        return df_enriched
    
    def normalize(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Normaliza los datos en entidades separadas.
        
        Args:
            df: DataFrame con datos válidos
            
        Returns:
            Diccionario con DataFrames normalizados por entidad
        """
        # Crear un nuevo normalizer para cada lote
        # para evitar acumulación de datos entre lotes
        batch_normalizer = SemillasNormalizer()
        return batch_normalizer.normalize(df)
    
    def transform(self, df: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
        """
        Ejecuta el pipeline completo de transformación.
        
        Args:
            df: DataFrame con datos crudos de staging
            
        Returns:
            Tupla (entidades_normalizadas, df_invalidos)
        """
        # Pipeline de transformación
        df_clean = self.clean(df)
        df_std = self.standardize(df_clean)
        df_valid, df_invalid = self.validate(df_std)
        
        # Solo procesar registros válidos
        entities = {}
        if len(df_valid) > 0:
            # Enriquecer antes de normalizar
            df_enriched = self.enrich(df_valid)
            entities = self.normalize(df_enriched)
        
        return entities, df_invalid