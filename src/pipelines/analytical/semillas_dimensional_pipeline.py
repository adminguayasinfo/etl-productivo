import logging
from typing import Dict, Any
from sqlalchemy.orm import Session
from src.load.semillas_dimensional_loader import SemillasDimensionalLoader

logger = logging.getLogger(__name__)


class SemillasDimensionalPipeline:
    """Pipeline para cargar datos del programa de semillas del esquema operational al analytical."""
    
    def __init__(self):
        self.loader = SemillasDimensionalLoader()
    
    def execute(self, session: Session) -> Dict[str, Any]:
        """Ejecuta el pipeline dimensional completo."""
        try:
            logger.info("=== Iniciando Pipeline Dimensional ===")
            
            self.loader.load_all(session)
            
            stats = self.loader.get_statistics(session)
            
            logger.info("=== Pipeline Dimensional Completado ===")
            logger.info(f"Personas: {stats['total_personas']}")
            logger.info(f"Beneficiarios semillas: {stats['total_beneficiarios_semillas']}")
            logger.info(f"Ubicaciones: {stats['total_ubicaciones']}")
            logger.info(f"Organizaciones: {stats['total_organizaciones']}")
            logger.info(f"Beneficios: {stats['total_beneficios']}")
            logger.info(f"Hectáreas totales: {stats['total_hectarias']:.2f}")
            logger.info(f"Promedio hectáreas: {stats['promedio_hectarias']:.2f}")
            
            return {
                'status': 'success',
                'stage': 'analytical',
                'statistics': stats
            }
            
        except Exception as e:
            logger.error(f"Error en pipeline dimensional: {e}")
            return {
                'status': 'error',
                'stage': 'analytical',
                'error': str(e)
            }