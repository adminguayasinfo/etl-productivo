"""
Servicio para cargar cultivos enriquecidos desde el proceso de transformación
hacia el esquema dimensional.
"""
import pandas as pd
from typing import Dict, Any
from sqlalchemy.orm import Session
from loguru import logger
from src.load.semillas_dimensional_loader import SemillasDimensionalLoader
from src.load.fertilizantes_dimensional_loader import FertilizantesDimensionalLoader


class CultivoLoaderService:
    """
    Servicio centralizado para cargar cultivos enriquecidos.
    
    Este servicio se encarga de:
    1. Recibir cultivos enriquecidos desde los transformers
    2. Consolidar cultivos únicos de múltiples fuentes
    3. Cargar cultivos a la dimensión dim_cultivo
    """
    
    def __init__(self):
        self.cultivos_cache = {}
        self.semillas_loader = SemillasDimensionalLoader()
        # Nota: FertilizantesDimensionalLoader puede usar el mismo método
        
    def add_cultivos_from_batch(self, cultivos_df: pd.DataFrame, source: str = "unknown"):
        """
        Agrega cultivos de un batch al cache para carga posterior.
        
        Args:
            cultivos_df: DataFrame con cultivos enriquecidos
            source: Fuente de los cultivos (semillas, fertilizantes, etc.)
        """
        if cultivos_df.empty:
            return
            
        logger.debug(f"Agregando {len(cultivos_df)} cultivos de {source} al cache")
        
        for _, cultivo in cultivos_df.iterrows():
            codigo = cultivo.get('codigo_cultivo')
            if codigo and pd.notna(codigo):
                # Si ya existe, actualizar con datos más completos
                if codigo not in self.cultivos_cache:
                    self.cultivos_cache[codigo] = cultivo.to_dict()
                else:
                    # Actualizar solo campos vacíos
                    existing = self.cultivos_cache[codigo]
                    for key, value in cultivo.to_dict().items():
                        if pd.notna(value) and (key not in existing or pd.isna(existing[key])):
                            existing[key] = value
    
    def load_all_cultivos(self, session: Session) -> Dict[str, Any]:
        """
        Carga todos los cultivos acumulados en cache a dim_cultivo.
        
        Args:
            session: Sesión de base de datos
            
        Returns:
            Diccionario con estadísticas de carga
        """
        if not self.cultivos_cache:
            logger.warning("No hay cultivos en cache para cargar")
            return {'cultivos_loaded': 0, 'status': 'no_data'}
        
        try:
            # Convertir cache a DataFrame
            cultivos_df = pd.DataFrame(list(self.cultivos_cache.values()))
            logger.info(f"Cargando {len(cultivos_df)} cultivos únicos a dim_cultivo")
            
            # Usar el loader para cargar a dim_cultivo
            cultivos_loaded = self.semillas_loader.load_dim_cultivo_from_enriched(cultivos_df, session)
            
            # Limpiar cache después de carga exitosa
            self.cultivos_cache.clear()
            
            return {
                'cultivos_loaded': cultivos_loaded,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error cargando cultivos: {e}")
            return {
                'cultivos_loaded': 0,
                'status': 'error',
                'error': str(e)
            }
    
    def get_cache_status(self) -> Dict[str, Any]:
        """Obtiene el estado actual del cache de cultivos."""
        return {
            'cultivos_en_cache': len(self.cultivos_cache),
            'codigos': list(self.cultivos_cache.keys())
        }