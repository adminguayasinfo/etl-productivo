import logging
from datetime import datetime, date
from typing import Dict, Any, Optional
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from src.models.analytical import DimPersona, DimUbicacion, DimOrganizacion, DimTiempo, DimCultivo, FactBeneficio

logger = logging.getLogger(__name__)


class SemillasDimensionalLoader:
    """Carga datos desde el esquema operational al esquema analytical para el programa de semillas."""
    
    def __init__(self):
        self.tiempo_cache: Dict[date, int] = {}
    
    def load_all(self, session: Session) -> None:
        """Ejecuta el proceso completo de carga dimensional."""
        try:
            logger.info("Iniciando carga dimensional...")
            
            self._load_dim_tiempo(session)
            self._load_dim_ubicacion(session)
            self._load_dim_organizacion(session)
            self._load_dim_persona(session)
            # Los cultivos se cargan desde el transformer con datos enriquecidos
            self._load_fact_beneficio(session)
            
            session.commit()
            logger.info("Carga dimensional completada exitosamente")
            
        except Exception as e:
            logger.error(f"Error en carga dimensional: {e}")
            session.rollback()
            raise
    
    def _load_dim_tiempo(self, session: Session) -> None:
        """Carga la dimensión tiempo basada en las fechas de beneficios."""
        logger.info("Cargando dimensión tiempo...")
        
        sql = text("""
            WITH fechas_unicas AS (
                SELECT DISTINCT fecha FROM operational.beneficio_base
                WHERE fecha IS NOT NULL
            )
            INSERT INTO analytics.dim_tiempo (
                fecha, dia, dia_semana, nombre_dia,
                semana, semana_anio,
                mes, nombre_mes, mes_abrev,
                trimestre, nombre_trimestre,
                anio, anio_mes, es_fin_semana
            )
            SELECT DISTINCT
                fecha,
                EXTRACT(DAY FROM fecha)::INTEGER,
                EXTRACT(DOW FROM fecha)::INTEGER,
                TO_CHAR(fecha, 'Day'),
                EXTRACT(WEEK FROM fecha)::INTEGER,
                EXTRACT(WEEK FROM fecha)::INTEGER,
                EXTRACT(MONTH FROM fecha)::INTEGER,
                TO_CHAR(fecha, 'Month'),
                TO_CHAR(fecha, 'Mon'),
                EXTRACT(QUARTER FROM fecha)::INTEGER,
                'Q' || EXTRACT(QUARTER FROM fecha),
                EXTRACT(YEAR FROM fecha)::INTEGER,
                TO_CHAR(fecha, 'YYYY-MM'),
                EXTRACT(DOW FROM fecha) IN (0, 6)
            FROM fechas_unicas
            ON CONFLICT (fecha) DO NOTHING
        """)
        
        result = session.execute(sql)
        logger.info(f"Dimensión tiempo: {result.rowcount} fechas cargadas")
        
        tiempo_records = session.query(DimTiempo).all()
        self.tiempo_cache = {t.fecha: t.tiempo_key for t in tiempo_records}
    
    def _load_dim_ubicacion(self, session: Session) -> None:
        """Carga la dimensión ubicación desde la tabla operational."""
        logger.info("Cargando dimensión ubicación...")
        
        sql = text("""
            INSERT INTO analytics.dim_ubicacion (
                ubicacion_id, provincia, canton, parroquia,
                comunidad, zona, region,
                latitud, longitud,
                fecha_inicio, es_vigente
            )
            SELECT
                id,
                CASE 
                    WHEN canton IN ('GUAYAQUIL', 'DAULE', 'SAMBORONDON', 'NOBOL', 'COLIMES', 'SANTA LUCIA', 'LOMAS DE SARGENTILLO') THEN 'GUAYAS'
                    WHEN canton IN ('QUITO', 'MEJIA', 'RUMIÑAHUI', 'CAYAMBE', 'PEDRO MONCAYO') THEN 'PICHINCHA'
                    WHEN canton IN ('CUENCA', 'AZOGUES', 'GUALACEO', 'PAUTE') THEN 'AZUAY'
                    WHEN canton IN ('MACHALA', 'PASAJE', 'HUAQUILLAS', 'ARENILLAS') THEN 'EL ORO'
                    WHEN canton IN ('PORTOVIEJO', 'MANTA', 'CHONE', 'JIPIJAPA') THEN 'MANABI'
                    WHEN canton IN ('AMBATO', 'BAÑOS', 'PELILEO', 'CEVALLOS') THEN 'TUNGURAHUA'
                    WHEN canton IN ('RIOBAMBA', 'GUARANDA', 'ALAUSI', 'CHAMBO') THEN 'CHIMBORAZO'
                    WHEN canton IN ('LOJA', 'CATAMAYO', 'GONZANAMA', 'PALTAS') THEN 'LOJA'
                    WHEN canton IN ('IBARRA', 'OTAVALO', 'COTACACHI', 'ANTONIO ANTE') THEN 'IMBABURA'
                    WHEN canton IN ('TULCAN', 'MONTUFAR', 'BOLIVAR', 'ESPEJO') THEN 'CARCHI'
                    ELSE 'OTROS'
                END as provincia,
                canton,
                COALESCE(parroquia, canton) as parroquia,
                localidad as comunidad,
                CASE 
                    WHEN canton IN ('GUAYAQUIL', 'QUITO', 'CUENCA', 'AMBATO', 'MACHALA') THEN 'URBANA'
                    ELSE 'RURAL'
                END as zona,
                CASE
                    WHEN canton IN ('QUITO', 'MEJIA', 'RUMIÑAHUI', 'CAYAMBE', 'PEDRO MONCAYO', 'IBARRA', 'OTAVALO', 'COTACACHI', 'ANTONIO ANTE', 'TULCAN', 'MONTUFAR', 'BOLIVAR', 'ESPEJO', 'CUENCA', 'AZOGUES', 'GUALACEO', 'PAUTE', 'AMBATO', 'BAÑOS', 'PELILEO', 'CEVALLOS', 'RIOBAMBA', 'GUARANDA', 'ALAUSI', 'CHAMBO', 'LOJA', 'CATAMAYO', 'GONZANAMA', 'PALTAS') THEN 'SIERRA'
                    WHEN canton IN ('GUAYAQUIL', 'DAULE', 'SAMBORONDON', 'NOBOL', 'COLIMES', 'SANTA LUCIA', 'LOMAS DE SARGENTILLO', 'MACHALA', 'PASAJE', 'HUAQUILLAS', 'ARENILLAS', 'PORTOVIEJO', 'MANTA', 'CHONE', 'JIPIJAPA') THEN 'COSTA'
                    ELSE 'OTROS'
                END as region,
                CASE WHEN coordenada_x IS NULL OR coordenada_x = 'NaN' THEN NULL ELSE coordenada_x END as latitud,
                CASE WHEN coordenada_y IS NULL OR coordenada_y = 'NaN' THEN NULL ELSE coordenada_y END as longitud,
                CURRENT_DATE,
                TRUE
            FROM operational.ubicacion
            ON CONFLICT (ubicacion_id) DO UPDATE SET
                updated_at = CURRENT_TIMESTAMP
        """)
        
        result = session.execute(sql)
        logger.info(f"Dimensión ubicación: {result.rowcount} registros procesados")
    
    def _load_dim_organizacion(self, session: Session) -> None:
        """Carga la dimensión organización."""
        logger.info("Cargando dimensión organización...")
        
        # Primero insertar registro especial para "SIN ORGANIZACIÓN"
        sql_default = text("""
            INSERT INTO analytics.dim_organizacion (
                organizacion_key, organizacion_id, nombre, tipo, categoria, estado,
                fecha_inicio, es_vigente
            )
            VALUES (-1, -1, 'SIN ORGANIZACIÓN', 'NO_APLICA', 'NO_APLICA', 'ACTIVA', CURRENT_DATE, TRUE)
            ON CONFLICT (organizacion_id) DO NOTHING
        """)
        session.execute(sql_default)
        
        # Luego cargar organizaciones reales
        sql = text("""
            INSERT INTO analytics.dim_organizacion (
                organizacion_id, nombre, tipo, categoria, estado,
                fecha_inicio, es_vigente
            )
            SELECT
                id,
                nombre,
                'ASOCIACION' as tipo,
                CASE
                    WHEN nombre ILIKE '%COOP%' THEN 'COOPERATIVA'
                    WHEN nombre ILIKE '%ASOC%' THEN 'ASOCIACION'
                    WHEN nombre ILIKE '%JUNTA%' THEN 'JUNTA'
                    ELSE 'OTROS'
                END as categoria,
                'ACTIVA' as estado,
                CURRENT_DATE,
                TRUE
            FROM operational.organizacion
            ON CONFLICT (organizacion_id) DO UPDATE SET
                updated_at = CURRENT_TIMESTAMP
        """)
        
        result = session.execute(sql)
        logger.info(f"Dimensión organización: {result.rowcount} registros procesados")
    
    def _load_dim_persona(self, session: Session) -> None:
        """Carga la dimensión persona con información completa."""
        logger.info("Cargando dimensión persona...")
        
        sql = text("""
            INSERT INTO analytics.dim_persona (
                persona_id, cedula, nombres_apellidos, genero,
                fecha_nacimiento, edad_actual, grupo_etario,
                email, telefono,
                es_beneficiario_semillas, tipo_productor, hectarias_totales,
                rango_hectarias, organizacion_nombre,
                fecha_inicio, es_vigente
            )
            SELECT
                p.id,
                p.cedula,
                p.nombres_apellidos,
                p.genero,
                NULL as fecha_nacimiento,
                p.edad as edad_actual,
                CASE 
                    WHEN p.edad IS NULL THEN 'DESCONOCIDO'
                    WHEN p.edad < 18 THEN 'MENOR'
                    WHEN p.edad BETWEEN 18 AND 30 THEN 'JOVEN'
                    WHEN p.edad BETWEEN 31 AND 50 THEN 'ADULTO'
                    WHEN p.edad BETWEEN 51 AND 65 THEN 'ADULTO MAYOR'
                    ELSE 'TERCERA EDAD'
                END as grupo_etario,
                NULL as email,
                p.telefono,
                CASE WHEN pa.persona_id IS NOT NULL THEN TRUE ELSE FALSE END as es_beneficiario_semillas,
                pa.tipo_productor,
                pa.hectarias_totales,
                CASE 
                    WHEN pa.hectarias_totales IS NULL THEN NULL
                    WHEN pa.hectarias_totales < 1 THEN 'MICRO (< 1 ha)'
                    WHEN pa.hectarias_totales BETWEEN 1 AND 5 THEN 'PEQUEÑO (1-5 ha)'
                    WHEN pa.hectarias_totales BETWEEN 5 AND 10 THEN 'MEDIANO (5-10 ha)'
                    ELSE 'GRANDE (> 10 ha)'
                END as rango_hectarias,
                o.nombre as organizacion_nombre,
                CURRENT_DATE,
                TRUE
            FROM operational.persona_base p
            LEFT JOIN operational.beneficiario_semillas pa ON p.id = pa.persona_id
            LEFT JOIN operational.organizacion o ON pa.organizacion_id = o.id
            ON CONFLICT (persona_id) DO UPDATE SET
                es_beneficiario_semillas = EXCLUDED.es_beneficiario_semillas,
                tipo_productor = EXCLUDED.tipo_productor,
                hectarias_totales = EXCLUDED.hectarias_totales,
                rango_hectarias = EXCLUDED.rango_hectarias,
                organizacion_nombre = EXCLUDED.organizacion_nombre,
                updated_at = CURRENT_TIMESTAMP
        """)
        
        result = session.execute(sql)
        logger.info(f"Dimensión persona: {result.rowcount} registros procesados")
    
    def _load_fact_beneficio(self, session: Session) -> None:
        """Carga la tabla de hechos de beneficios."""
        logger.info("Cargando fact beneficio...")
        
        sql = text("""
            INSERT INTO analytics.fact_beneficio (
                beneficio_id,
                persona_key,
                ubicacion_key,
                organizacion_key,
                tiempo_key,
                cultivo_key,
                tipo_beneficio,
                categoria_beneficio,
                hectarias_sembradas,
                valor_monetario,
                fecha_beneficio,
                anio,
                mes,
                trimestre,
                created_at,
                updated_at
            )
            SELECT
                bb.id,
                dp.persona_key,
                du.ubicacion_key,
                COALESCE(org.organizacion_key, -1),
                dt.tiempo_key,
                dc.cultivo_key,
                bb.tipo_beneficio,
                UPPER(COALESCE(bs.tipo_cultivo, 'NO_ESPECIFICADO')) as categoria_beneficio,
                bb.hectarias_beneficiadas,
                bb.valor_monetario,
                bb.fecha,
                EXTRACT(YEAR FROM bb.fecha)::INTEGER,
                EXTRACT(MONTH FROM bb.fecha)::INTEGER,
                EXTRACT(QUARTER FROM bb.fecha)::INTEGER,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            FROM operational.beneficio_base bb
            INNER JOIN operational.beneficio_semillas bs ON bb.id = bs.id
            INNER JOIN analytics.dim_persona dp ON bb.persona_id = dp.persona_id
            INNER JOIN analytics.dim_ubicacion du ON bb.ubicacion_id = du.ubicacion_id
            LEFT JOIN operational.beneficiario_semillas pa ON bb.persona_id = pa.persona_id
            LEFT JOIN analytics.dim_organizacion org ON pa.organizacion_id = org.organizacion_id
            INNER JOIN analytics.dim_tiempo dt ON bb.fecha = dt.fecha
            LEFT JOIN analytics.dim_cultivo dc ON UPPER(bs.tipo_cultivo) = dc.codigo_cultivo
            WHERE bb.fecha IS NOT NULL
            AND dc.cultivo_key IS NOT NULL  -- Solo registros con cultivo válido
            ON CONFLICT DO NOTHING
        """)
        
        result = session.execute(sql)
        logger.info(f"Fact beneficio: {result.rowcount} registros cargados")
        
        count_sql = text("SELECT COUNT(*) FROM analytics.fact_beneficio")
        count = session.execute(count_sql).scalar()
        logger.info(f"Total registros en fact_beneficio: {count}")
    
    def get_statistics(self, session: Session) -> Dict[str, Any]:
        """Obtiene estadísticas del esquema analytical."""
        stats = {}
        
        queries = {
            'total_personas': "SELECT COUNT(*) FROM analytics.dim_persona",
            'total_beneficiarios_semillas': "SELECT COUNT(*) FROM analytics.dim_persona WHERE es_beneficiario_semillas = TRUE",
            'total_ubicaciones': "SELECT COUNT(*) FROM analytics.dim_ubicacion",
            'total_organizaciones': "SELECT COUNT(*) FROM analytics.dim_organizacion",
            'total_beneficios': "SELECT COUNT(*) FROM analytics.fact_beneficio",
            'total_hectarias': "SELECT SUM(hectarias_totales) FROM analytics.dim_persona WHERE hectarias_totales IS NOT NULL",
            'promedio_hectarias': "SELECT AVG(hectarias_totales) FROM analytics.dim_persona WHERE hectarias_totales IS NOT NULL"
        }
        
        for key, query in queries.items():
            result = session.execute(text(query)).scalar()
            stats[key] = result if result is not None else 0
        
        return stats
    
    def load_dim_cultivo_from_enriched(self, cultivos_df: pd.DataFrame, session: Session) -> int:
        """
        Carga la dimensión cultivo desde datos enriquecidos del transformer.
        
        Args:
            cultivos_df: DataFrame con cultivos enriquecidos
            session: Sesión de base de datos
            
        Returns:
            Número de cultivos cargados
        """
        if cultivos_df.empty:
            logger.warning("No hay cultivos enriquecidos para cargar")
            return 0
            
        logger.info(f"Cargando {len(cultivos_df)} cultivos enriquecidos a dim_cultivo")
        
        cultivos_loaded = 0
        for _, cultivo in cultivos_df.iterrows():
            # Preparar datos para upsert
            cultivo_data = {
                'codigo_cultivo': cultivo.get('codigo_cultivo'),
                'nombre_cultivo': cultivo.get('nombre_cultivo'),
                'nombre_cientifico': cultivo.get('nombre_cientifico'),
                'familia_botanica': cultivo.get('familia_botanica'),
                'tipo_ciclo': cultivo.get('tipo_ciclo'),
                'clasificacion_economica': cultivo.get('clasificacion_economica'),
                'uso_principal': cultivo.get('uso_principal'),
                'es_vigente': cultivo.get('es_vigente', True)
            }
            
            # Usar upsert para evitar duplicados
            stmt = pg_insert(DimCultivo).values(**cultivo_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['codigo_cultivo'],
                set_={
                    'nombre_cultivo': stmt.excluded.nombre_cultivo,
                    'nombre_cientifico': stmt.excluded.nombre_cientifico,
                    'familia_botanica': stmt.excluded.familia_botanica,
                    'tipo_ciclo': stmt.excluded.tipo_ciclo,
                    'clasificacion_economica': stmt.excluded.clasificacion_economica,
                    'uso_principal': stmt.excluded.uso_principal,
                    'updated_at': datetime.now()
                }
            )
            
            session.execute(stmt)
            cultivos_loaded += 1
        
        # NO hacer commit aquí - dejar que el contexto lo maneje
        session.flush()  # Asegurar que los cambios se escriban al buffer de la BD
        logger.info(f"Cultivos cargados exitosamente: {cultivos_loaded}")
        return cultivos_loaded