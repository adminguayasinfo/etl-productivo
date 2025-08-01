"""
Enricher para datos de cultivos.
Enriquece tipos de cultivo con información botánica, agronómica y económica.
"""
import json
import os
from typing import Dict, Any, Optional
from pathlib import Path
from loguru import logger


class CultivoEnricher:
    """
    Enriquece datos de cultivos con información adicional.
    
    Este enricher se encarga de agregar información botánica, agronómica
    y económica a los tipos de cultivo encontrados en los datos.
    """
    
    def __init__(self, catalog_path: Optional[str] = None):
        """
        Inicializa el enricher con el catálogo de cultivos.
        
        Args:
            catalog_path: Ruta al archivo de catálogo. Si no se especifica,
                         usa el catálogo por defecto.
        """
        self.catalog_path = catalog_path or self._get_default_catalog_path()
        self.cultivo_catalog = self._load_catalog()
        
    def _get_default_catalog_path(self) -> str:
        """Obtiene la ruta por defecto del catálogo de cultivos."""
        # Buscar en config/catalogs/cultivos.json
        project_root = Path(__file__).parent.parent.parent.parent
        return str(project_root / "config" / "catalogs" / "cultivos.json")
    
    def _load_catalog(self) -> Dict[str, Dict[str, Any]]:
        """
        Carga el catálogo de cultivos desde archivo o fuente de datos.
        
        Returns:
            Diccionario con información de cultivos
        """
        try:
            # Intentar cargar desde archivo JSON
            if os.path.exists(self.catalog_path):
                with open(self.catalog_path, 'r', encoding='utf-8') as f:
                    catalog = json.load(f)
                logger.info(f"Catálogo cargado desde {self.catalog_path}")
                return catalog
            else:
                logger.warning(f"Archivo de catálogo no encontrado: {self.catalog_path}")
        except Exception as e:
            logger.error(f"Error cargando catálogo: {e}")
        
        # Fallback a datos embebidos
        logger.info("Usando catálogo embebido como fallback")
        catalog = {
            'ARROZ': {
                'nombre_cultivo': 'Arroz',
                'nombre_cientifico': 'Oryza sativa',
                'familia_botanica': 'Poaceae',
                'genero': 'Oryza',
                'tipo_ciclo': 'ANUAL',
                'duracion_ciclo_dias': 120,
                'estacionalidad': 'INVIERNO_VERANO',
                'clasificacion_economica': 'ALIMENTARIO',
                'uso_principal': 'CONSUMO_HUMANO',
                'tipo_clima': 'TROPICAL',
                'requerimiento_agua': 'ALTO',
                'tipo_suelo_preferido': 'ARCILLOSO_INUNDABLE',
                'epoca_siembra_principal': 'TODO_AÑO',
                'epoca_cosecha_principal': 'TODO_AÑO'
            },
            'MAIZ': {
                'nombre_cultivo': 'Maíz',
                'nombre_cientifico': 'Zea mays',
                'familia_botanica': 'Poaceae',
                'genero': 'Zea',
                'tipo_ciclo': 'ANUAL',
                'duracion_ciclo_dias': 90,
                'estacionalidad': 'INVIERNO_VERANO',
                'clasificacion_economica': 'ALIMENTARIO',
                'uso_principal': 'CONSUMO_HUMANO_ANIMAL',
                'tipo_clima': 'TROPICAL',
                'requerimiento_agua': 'MEDIO',
                'tipo_suelo_preferido': 'FRANCO',
                'epoca_siembra_principal': 'INVIERNO_VERANO',
                'epoca_cosecha_principal': 'MAY_JUL_NOV_ENE'
            },
            'MAÍZ': {  # Variante ortográfica
                'nombre_cultivo': 'Maíz',
                'nombre_cientifico': 'Zea mays',
                'familia_botanica': 'Poaceae',
                'genero': 'Zea',
                'tipo_ciclo': 'ANUAL',
                'duracion_ciclo_dias': 90,
                'estacionalidad': 'INVIERNO_VERANO',
                'clasificacion_economica': 'ALIMENTARIO',
                'uso_principal': 'CONSUMO_HUMANO_ANIMAL',
                'tipo_clima': 'TROPICAL',
                'requerimiento_agua': 'MEDIO',
                'tipo_suelo_preferido': 'FRANCO',
                'epoca_siembra_principal': 'INVIERNO_VERANO',
                'epoca_cosecha_principal': 'MAY_JUL_NOV_ENE'
            },
            'CACAO': {
                'nombre_cultivo': 'Cacao',
                'nombre_cientifico': 'Theobroma cacao',
                'familia_botanica': 'Malvaceae',
                'genero': 'Theobroma',
                'tipo_ciclo': 'PERENNE',
                'duracion_ciclo_dias': 365,
                'estacionalidad': 'TODO_EL_AÑO',
                'clasificacion_economica': 'EXPORTACION',
                'uso_principal': 'INDUSTRIAL',
                'tipo_clima': 'TROPICAL',
                'requerimiento_agua': 'ALTO',
                'tipo_suelo_preferido': 'FRANCO_DRENADO',
                'epoca_siembra_principal': 'INVIERNO',
                'epoca_cosecha_principal': 'TODO_AÑO'
            },
            'BANANO': {
                'nombre_cultivo': 'Banano',
                'nombre_cientifico': 'Musa × paradisiaca',
                'familia_botanica': 'Musaceae',
                'genero': 'Musa',
                'tipo_ciclo': 'PERENNE',
                'duracion_ciclo_dias': 365,
                'estacionalidad': 'TODO_EL_AÑO',
                'clasificacion_economica': 'EXPORTACION',
                'uso_principal': 'CONSUMO_HUMANO',
                'tipo_clima': 'TROPICAL',
                'requerimiento_agua': 'ALTO',
                'tipo_suelo_preferido': 'FRANCO_HUMIFERO',
                'epoca_siembra_principal': 'TODO_AÑO',
                'epoca_cosecha_principal': 'TODO_AÑO'
            },
            'PLATANO': {
                'nombre_cultivo': 'Plátano',
                'nombre_cientifico': 'Musa × paradisiaca',
                'familia_botanica': 'Musaceae',
                'genero': 'Musa',
                'tipo_ciclo': 'PERENNE',
                'duracion_ciclo_dias': 365,
                'estacionalidad': 'TODO_EL_AÑO',
                'clasificacion_economica': 'ALIMENTARIO',
                'uso_principal': 'CONSUMO_HUMANO',
                'tipo_clima': 'TROPICAL',
                'requerimiento_agua': 'ALTO',
                'tipo_suelo_preferido': 'FRANCO_HUMIFERO',
                'epoca_siembra_principal': 'TODO_AÑO',
                'epoca_cosecha_principal': 'TODO_AÑO'
            }
        }
        
        logger.info(f"Catálogo de cultivos cargado con {len(catalog)} entradas")
        return catalog
    
    def enrich(self, tipo_cultivo: str) -> Dict[str, Any]:
        """
        Enriquece un tipo de cultivo con información adicional.
        
        Args:
            tipo_cultivo: Código o nombre del cultivo
            
        Returns:
            Diccionario con información enriquecida del cultivo
        """
        if not tipo_cultivo:
            return {}
            
        cultivo_upper = tipo_cultivo.upper().strip()
        enriched_data = self.cultivo_catalog.get(cultivo_upper, {})
        
        if enriched_data:
            # Agregar el código original
            enriched_data['codigo_cultivo'] = cultivo_upper
            logger.debug(f"Cultivo '{cultivo_upper}' enriquecido con {len(enriched_data)} atributos")
        else:
            logger.warning(f"Cultivo '{cultivo_upper}' no encontrado en catálogo")
            # Retornar datos mínimos
            enriched_data = {
                'codigo_cultivo': cultivo_upper,
                'nombre_cultivo': tipo_cultivo,
                'clasificacion_economica': 'NO_CLASIFICADO'
            }
        
        return enriched_data
    
    def enrich_batch(self, cultivos: list[str]) -> Dict[str, Dict[str, Any]]:
        """
        Enriquece un lote de cultivos.
        
        Args:
            cultivos: Lista de códigos de cultivo
            
        Returns:
            Diccionario con información enriquecida por cultivo
        """
        enriched_batch = {}
        unique_cultivos = set(c.upper().strip() for c in cultivos if c)
        
        for cultivo in unique_cultivos:
            enriched_batch[cultivo] = self.enrich(cultivo)
        
        logger.info(f"Enriquecidos {len(enriched_batch)} cultivos únicos")
        return enriched_batch
    
    def get_cultivo_dimension_data(self, tipo_cultivo: str) -> Dict[str, Any]:
        """
        Obtiene datos formateados para la dimensión cultivo.
        
        Args:
            tipo_cultivo: Código del cultivo
            
        Returns:
            Diccionario con datos listos para insertar en dim_cultivo
        """
        enriched = self.enrich(tipo_cultivo)
        
        # Asegurar que todos los campos requeridos estén presentes
        dimension_data = {
            'codigo_cultivo': enriched.get('codigo_cultivo', tipo_cultivo.upper()),
            'nombre_cultivo': enriched.get('nombre_cultivo', tipo_cultivo),
            'nombre_cientifico': enriched.get('nombre_cientifico'),
            'familia_botanica': enriched.get('familia_botanica'),
            'genero': enriched.get('genero'),
            'tipo_ciclo': enriched.get('tipo_ciclo'),
            'duracion_ciclo_dias': enriched.get('duracion_ciclo_dias'),
            'estacionalidad': enriched.get('estacionalidad'),
            'clasificacion_economica': enriched.get('clasificacion_economica'),
            'uso_principal': enriched.get('uso_principal'),
            'tipo_clima': enriched.get('tipo_clima'),
            'requerimiento_agua': enriched.get('requerimiento_agua'),
            'tipo_suelo_preferido': enriched.get('tipo_suelo_preferido'),
            'epoca_siembra_principal': enriched.get('epoca_siembra_principal'),
            'epoca_cosecha_principal': enriched.get('epoca_cosecha_principal'),
            'es_vigente': True
        }
        
        return dimension_data
    
    def get_all_cultivos(self) -> Dict[str, Dict[str, Any]]:
        """Retorna todo el catálogo de cultivos."""
        return self.cultivo_catalog.copy()