"""
Script para inicializar la dimensión dim_cultivo con datos base.
Incluye los cultivos identificados en el sistema con sus características agronómicas.
"""

from config.connections.database import DatabaseConnection
from src.models.analytical.dimensions.dim_cultivo import DimCultivo
from sqlalchemy.dialects.postgresql import insert as pg_insert
from loguru import logger


def get_cultivos_data():
    """Retorna los datos base de cultivos con sus características."""
    return [
        {
            'codigo_cultivo': 'ARROZ',
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
        {
            'codigo_cultivo': 'MAIZ',
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
        {
            'codigo_cultivo': 'MAÍZ',  # Variante ortográfica, mapear al mismo
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
        {
            'codigo_cultivo': 'CACAO',
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
        {
            'codigo_cultivo': 'BANANO',
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
        {
            'codigo_cultivo': 'PLATANO',
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
    ]


def init_dim_cultivo():
    """Inicializa la dimensión dim_cultivo con datos base."""
    logger.info("=== INICIALIZANDO DIM_CULTIVO ===")
    
    db = DatabaseConnection()
    
    with db.get_session() as session:
        try:
            # Obtener datos de cultivos
            cultivos_data = get_cultivos_data()
            logger.info(f"Preparando {len(cultivos_data)} cultivos para inserción")
            
            # Usar UPSERT para manejar duplicados
            stmt = pg_insert(DimCultivo).values(cultivos_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['codigo_cultivo'],
                set_={
                    'nombre_cultivo': stmt.excluded.nombre_cultivo,
                    'nombre_cientifico': stmt.excluded.nombre_cientifico,
                    'familia_botanica': stmt.excluded.familia_botanica,
                    'genero': stmt.excluded.genero,
                    'tipo_ciclo': stmt.excluded.tipo_ciclo,
                    'duracion_ciclo_dias': stmt.excluded.duracion_ciclo_dias,
                    'estacionalidad': stmt.excluded.estacionalidad,
                    'clasificacion_economica': stmt.excluded.clasificacion_economica,
                    'uso_principal': stmt.excluded.uso_principal,
                    'tipo_clima': stmt.excluded.tipo_clima,
                    'requerimiento_agua': stmt.excluded.requerimiento_agua,
                    'tipo_suelo_preferido': stmt.excluded.tipo_suelo_preferido,
                    'epoca_siembra_principal': stmt.excluded.epoca_siembra_principal,
                    'epoca_cosecha_principal': stmt.excluded.epoca_cosecha_principal,
                    'updated_at': 'NOW()'
                }
            )
            
            result = session.execute(stmt)
            logger.info(f"Cultivos procesados: {len(cultivos_data)}")
            
            # Verificar resultados
            from sqlalchemy import text
            count_result = session.execute(text("SELECT COUNT(*) FROM analytics.dim_cultivo")).scalar()
            logger.info(f"Total cultivos en dim_cultivo: {count_result}")
            
            # Mostrar cultivos cargados
            cultivos_result = session.execute(text("""
                SELECT cultivo_key, codigo_cultivo, nombre_cultivo, clasificacion_economica 
                FROM analytics.dim_cultivo 
                ORDER BY codigo_cultivo
            """))
            
            logger.info("Cultivos cargados:")
            for row in cultivos_result:
                logger.info(f"  {row[0]}: {row[1]} - {row[2]} ({row[3]})")
            
            logger.info("✓ dim_cultivo inicializada exitosamente")
            
        except Exception as e:
            logger.error(f"Error inicializando dim_cultivo: {e}")
            raise


def get_cultivo_mapping():
    """Retorna mapeo de códigos de cultivo a cultivo_key para uso en ETL."""
    db = DatabaseConnection()
    
    with db.get_session() as session:
        from sqlalchemy import text
        result = session.execute(text("""
            SELECT codigo_cultivo, cultivo_key 
            FROM analytics.dim_cultivo 
            WHERE es_vigente = TRUE
        """))
        
        mapping = {}
        for row in result:
            mapping[row[0]] = row[1]
        
        logger.info(f"Mapeo de cultivos obtenido: {mapping}")
        return mapping


if __name__ == "__main__":
    init_dim_cultivo()
    print("\n--- Mapeo para ETL ---")
    mapping = get_cultivo_mapping()
    for codigo, key in mapping.items():
        print(f"{codigo} -> {key}")