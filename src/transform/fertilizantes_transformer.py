"""Transformer específico para datos de fertilizantes."""
import pandas as pd
from typing import Dict, Tuple, Optional
from loguru import logger

from src.transform.cleaners.fertilizantes_cleaner import FertilizantesCleaner
from src.transform.normalizers.fertilizantes_normalizer import FertilizantesNormalizer
from src.transform.validators.data_validator_flexible import DataValidatorFlexible
from src.transform.enrichers.cultivo_enricher import CultivoEnricher


class FertilizantesTransformer:
    """Pipeline de transformación para datos de fertilizantes."""
    
    def __init__(self):
        self.cleaner = FertilizantesCleaner()
        self.normalizer = FertilizantesNormalizer()
        self.validator = DataValidatorFlexible()
        self.cultivo_enricher = CultivoEnricher()
        self.stats = {
            'total_registros': 0,
            'registros_validos': 0,
            'registros_invalidos': 0,
            'registros_transformados': 0
        }
    
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
        
    def transform(self, df: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
        """
        Ejecuta el pipeline completo de transformación.
        
        Args:
            df: DataFrame con datos crudos de fertilizantes
            
        Returns:
            Tuple con:
            - Dict de DataFrames normalizados por entidad
            - DataFrame con registros inválidos
        """
        logger.info(f"=== Iniciando transformación de {len(df)} registros de fertilizantes ===")
        self.stats['total_registros'] = len(df)
        
        # 1. Limpieza
        logger.info("Fase 1: Limpieza de datos")
        df_clean = self.cleaner.clean(df)
        
        # 2. Validación
        logger.info("Fase 2: Validación de datos")
        df_validated = self.validator.validate(df_clean)
        
        # Separar válidos e inválidos
        df_valid = df_validated[df_validated['es_valido'] == True].copy()
        df_invalid = df_validated[df_validated['es_valido'] == False].copy()
        
        self.stats['registros_validos'] = len(df_valid)
        self.stats['registros_invalidos'] = len(df_invalid)
        
        if len(df_valid) == 0:
            logger.warning("No hay registros válidos para procesar")
            return {}, df_invalid
            
        # 3. Enriquecimiento
        logger.info("Fase 3: Enriquecimiento de datos")
        df_enriched = self.enrich(df_valid)
        
        # 4. Normalización
        logger.info("Fase 4: Normalización de datos")
        entities = self.normalizer.normalize(df_enriched)
        
        self.stats['registros_transformados'] = len(df_valid)
        
        # 4. Resumen
        self._log_summary(entities)
        
        return entities, df_invalid
        
    def _log_summary(self, entities: Dict[str, pd.DataFrame]):
        """Registra resumen de la transformación."""
        logger.info("=== RESUMEN DE TRANSFORMACIÓN FERTILIZANTES ===")
        logger.info(f"Total registros procesados: {self.stats['total_registros']}")
        logger.info(f"Registros válidos: {self.stats['registros_validos']}")
        logger.info(f"Registros inválidos: {self.stats['registros_invalidos']}")
        logger.info(f"Registros transformados: {self.stats['registros_transformados']}")
        
        logger.info("\nEntidades generadas:")
        for entity_name, df in entities.items():
            logger.info(f"  - {entity_name}: {len(df)} registros")
            
        # Estadísticas del cleaner
        cleaner_stats = self.cleaner.get_stats()
        logger.info("\nEstadísticas de limpieza:")
        for key, value in cleaner_stats.items():
            logger.info(f"  - {key}: {value}")
            
        # Estadísticas del normalizer
        normalizer_stats = self.normalizer.get_stats()
        logger.info("\nEstadísticas de normalización:")
        for key, value in normalizer_stats.items():
            logger.info(f"  - {key}: {value}")
            
    def get_stats(self) -> Dict:
        """Retorna estadísticas de la transformación."""
        return {
            **self.stats,
            'cleaner_stats': self.cleaner.get_stats(),
            'normalizer_stats': self.normalizer.get_stats(),
            'validator_stats': self.validator.get_stats()
        }