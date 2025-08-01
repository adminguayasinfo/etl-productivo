import logging
from datetime import datetime, date
from typing import Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from src.models.analytical import DimPersona, DimUbicacion, DimOrganizacion, DimTiempo, FactBeneficio

logger = logging.getLogger(__name__)


class SemillasDimensionalLoaderEnhanced:
    """Carga datos desde el esquema operational al esquema analytical con mejoras para aprovechar los datos adicionales."""
    
    def __init__(self):
        self.tiempo_cache: Dict[date, int] = {}
    
    def load_all(self, session: Session) -> None:
        """Ejecuta el proceso completo de carga dimensional."""
        try:
            logger.info("Iniciando carga dimensional mejorada...")
            
            self._load_dim_tiempo(session)
            self._load_dim_ubicacion(session)
            self._load_dim_organizacion(session)
            self._load_dim_persona(session)
            self._load_fact_beneficio(session)
            
            # Nuevas cargas para análisis adicionales
            self._create_aggregated_tables(session)
            self._calculate_quality_metrics(session)
            
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
                UNION
                -- Incluir fechas de años completos para análisis temporal
                SELECT generate_series(
                    date_trunc('year', MIN(fecha)),
                    date_trunc('year', MAX(fecha)) + interval '1 year - 1 day',
                    '1 day'::interval
                )::date
                FROM operational.beneficio_base
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
                TRIM(TO_CHAR(fecha, 'Day')),
                EXTRACT(WEEK FROM fecha)::INTEGER,
                EXTRACT(WEEK FROM fecha)::INTEGER,
                EXTRACT(MONTH FROM fecha)::INTEGER,
                TRIM(TO_CHAR(fecha, 'Month')),
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
        """Carga la dimensión ubicación con mejoras en la clasificación."""
        logger.info("Cargando dimensión ubicación mejorada...")
        
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
                    -- Mapeo mejorado de provincias basado en cantones
                    WHEN UPPER(canton) IN ('GUAYAQUIL', 'DAULE', 'SAMBORONDON', 'NOBOL', 'COLIMES', 'SANTA LUCIA', 
                                          'LOMAS DE SARGENTILLO', 'DURAN', 'MILAGRO', 'YAGUACHI', 'SIMON BOLIVAR',
                                          'ALFREDO BAQUERIZO MORENO', 'BALAO', 'BALZAR', 'EL EMPALME', 'EL TRIUNFO',
                                          'GENERAL ANTONIO ELIZALDE', 'ISIDRO AYORA', 'MARCELINO MARIDUEÑA', 'NARANJAL',
                                          'NARANJITO', 'PALESTINA', 'PEDRO CARBO', 'PLAYAS', 'SALITRE') THEN 'GUAYAS'
                    WHEN UPPER(canton) IN ('PORTOVIEJO', 'MANTA', 'CHONE', 'JIPIJAPA', 'MONTECRISTI', 'ROCAFUERTE',
                                          'SANTA ANA', 'SUCRE', '24 DE MAYO', 'BOLIVAR', 'FLAVIO ALFARO', 'JAMA',
                                          'JARAMIJO', 'JUNIN', 'OLMEDO', 'PAJAN', 'PICHINCHA', 'PUERTO LOPEZ',
                                          'SAN VICENTE', 'TOSAGUA') THEN 'MANABI'
                    WHEN UPPER(canton) IN ('MACHALA', 'PASAJE', 'HUAQUILLAS', 'ARENILLAS', 'ATAHUALPA', 'BALSAS',
                                          'CHILLA', 'EL GUABO', 'LAS LAJAS', 'MARCABELI', 'PIÑAS', 'PORTOVELO',
                                          'SANTA ROSA', 'ZARUMA') THEN 'EL ORO'
                    WHEN UPPER(canton) IN ('SANTA ELENA', 'LA LIBERTAD', 'SALINAS') THEN 'SANTA ELENA'
                    WHEN UPPER(canton) IN ('BABAHOYO', 'BABA', 'MOCACHE', 'MONTALVO', 'PALENQUE', 'PUEBLOVIEJO',
                                          'QUEVEDO', 'QUINSALOMA', 'URDANETA', 'VALENCIA', 'VENTANAS', 'VINCES') THEN 'LOS RIOS'
                    WHEN UPPER(canton) IN ('ESMERALDAS', 'ATACAMES', 'ELOY ALFARO', 'MUISNE', 'QUININDE',
                                          'RIO VERDE', 'SAN LORENZO') THEN 'ESMERALDAS'
                    WHEN UPPER(canton) IN ('SANTO DOMINGO', 'LA CONCORDIA') THEN 'SANTO DOMINGO DE LOS TSACHILAS'
                    WHEN UPPER(canton) IN ('QUITO', 'MEJIA', 'RUMIÑAHUI', 'CAYAMBE', 'PEDRO MONCAYO', 
                                          'SAN MIGUEL DE LOS BANCOS', 'PEDRO VICENTE MALDONADO', 'PUERTO QUITO') THEN 'PICHINCHA'
                    WHEN UPPER(canton) IN ('CUENCA', 'GUALACEO', 'PAUTE', 'SIGSIG', 'CHORDELEG', 'EL PAN',
                                          'GIRON', 'GUACHAPALA', 'NABON', 'OÑA', 'PUCARA', 'SAN FERNANDO',
                                          'SANTA ISABEL', 'SEVILLA DE ORO') THEN 'AZUAY'
                    WHEN UPPER(canton) IN ('AMBATO', 'BAÑOS', 'PELILEO', 'CEVALLOS', 'MOCHA', 'PATATE',
                                          'QUERO', 'SAN PEDRO DE PELILEO', 'SANTIAGO DE PILLARO', 'TISALEO') THEN 'TUNGURAHUA'
                    WHEN UPPER(canton) IN ('RIOBAMBA', 'ALAUSI', 'CHAMBO', 'CHUNCHI', 'COLTA', 'CUMANDA',
                                          'GUAMOTE', 'GUANO', 'PALLATANGA', 'PENIPE') THEN 'CHIMBORAZO'
                    WHEN UPPER(canton) IN ('LOJA', 'CATAMAYO', 'GONZANAMA', 'PALTAS', 'CALVAS', 'CELICA',
                                          'CHAGUARPAMBA', 'ESPINDOLA', 'MACARA', 'OLMEDO', 'PINDAL', 'PUYANGO',
                                          'QUILANGA', 'SARAGURO', 'SOZORANGA', 'ZAPOTILLO') THEN 'LOJA'
                    WHEN UPPER(canton) IN ('IBARRA', 'OTAVALO', 'COTACACHI', 'ANTONIO ANTE', 'PIMAMPIRO',
                                          'SAN MIGUEL DE URCUQUI') THEN 'IMBABURA'
                    WHEN UPPER(canton) IN ('TULCAN', 'MONTUFAR', 'BOLIVAR', 'ESPEJO', 'MIRA', 
                                          'SAN PEDRO DE HUACA') THEN 'CARCHI'
                    WHEN UPPER(canton) IN ('AZOGUES', 'BIBLIAN', 'CAÑAR', 'DELEG', 'EL TAMBO', 
                                          'LA TRONCAL', 'SUSCAL') THEN 'CAÑAR'
                    WHEN UPPER(canton) IN ('GUARANDA', 'CALUMA', 'CHILLANES', 'CHIMBO', 'ECHEANDIA',
                                          'LAS NAVES', 'SAN MIGUEL') THEN 'BOLIVAR'
                    WHEN UPPER(canton) IN ('LATACUNGA', 'LA MANA', 'PANGUA', 'PUJILI', 'SALCEDO',
                                          'SAQUISILI', 'SIGCHOS') THEN 'COTOPAXI'
                    ELSE 'NO ESPECIFICADO'
                END as provincia,
                canton,
                COALESCE(parroquia, canton) as parroquia,
                localidad as comunidad,
                CASE 
                    WHEN UPPER(canton) IN ('GUAYAQUIL', 'QUITO', 'CUENCA', 'SANTO DOMINGO', 'AMBATO', 
                                          'MANTA', 'PORTOVIEJO', 'MACHALA', 'RIOBAMBA', 'IBARRA', 
                                          'LOJA', 'ESMERALDAS', 'MILAGRO', 'BABAHOYO', 'QUEVEDO') THEN 'URBANA'
                    ELSE 'RURAL'
                END as zona,
                CASE
                    -- Clasificación mejorada por región
                    WHEN UPPER(canton) IN (SELECT UNNEST(ARRAY[
                        'QUITO', 'MEJIA', 'RUMIÑAHUI', 'CAYAMBE', 'PEDRO MONCAYO', 'IBARRA', 'OTAVALO', 
                        'COTACACHI', 'ANTONIO ANTE', 'TULCAN', 'MONTUFAR', 'BOLIVAR', 'ESPEJO', 'CUENCA', 
                        'AZOGUES', 'GUALACEO', 'PAUTE', 'AMBATO', 'BAÑOS', 'PELILEO', 'CEVALLOS', 'RIOBAMBA', 
                        'GUARANDA', 'ALAUSI', 'CHAMBO', 'LOJA', 'CATAMAYO', 'GONZANAMA', 'PALTAS', 'LATACUNGA',
                        'PUJILI', 'SALCEDO', 'SIGCHOS'
                    ])) THEN 'SIERRA'
                    WHEN UPPER(canton) IN (SELECT UNNEST(ARRAY[
                        'GUAYAQUIL', 'MANTA', 'PORTOVIEJO', 'MACHALA', 'ESMERALDAS', 'SANTO DOMINGO',
                        'BABAHOYO', 'QUEVEDO', 'MILAGRO', 'SANTA ELENA', 'LA LIBERTAD', 'SALINAS',
                        'DAULE', 'DURAN', 'SAMBORONDON', 'PASAJE', 'HUAQUILLAS', 'CHONE', 'JIPIJAPA'
                    ])) THEN 'COSTA'
                    WHEN UPPER(canton) IN (SELECT UNNEST(ARRAY[
                        'TENA', 'PUYO', 'MACAS', 'ZAMORA', 'NUEVA LOJA', 'FRANCISCO DE ORELLANA'
                    ])) THEN 'ORIENTE'
                    ELSE 'NO ESPECIFICADO'
                END as region,
                -- Mejorar manejo de coordenadas
                CASE 
                    WHEN coordenada_x IS NULL OR coordenada_x = 'NaN' OR ABS(coordenada_x) > 180 THEN NULL 
                    ELSE coordenada_x 
                END as latitud,
                CASE 
                    WHEN coordenada_y IS NULL OR coordenada_y = 'NaN' OR ABS(coordenada_y) > 90 THEN NULL 
                    ELSE coordenada_y 
                END as longitud,
                CURRENT_DATE,
                TRUE
            FROM operational.ubicacion
            ON CONFLICT (ubicacion_id) DO UPDATE SET
                provincia = EXCLUDED.provincia,
                zona = EXCLUDED.zona,
                region = EXCLUDED.region,
                updated_at = CURRENT_TIMESTAMP
        """)
        
        result = session.execute(sql)
        logger.info(f"Dimensión ubicación: {result.rowcount} registros procesados")
    
    def _load_dim_organizacion(self, session: Session) -> None:
        """Carga la dimensión organización con clasificación mejorada."""
        logger.info("Cargando dimensión organización mejorada...")
        
        sql = text("""
            INSERT INTO analytics.dim_organizacion (
                organizacion_id, nombre, tipo, categoria, estado,
                fecha_inicio, es_vigente
            )
            SELECT
                id,
                nombre,
                -- Clasificación mejorada de tipo
                CASE
                    WHEN UPPER(nombre) LIKE '%COOPERATIVA%' OR UPPER(nombre) LIKE '%COOP%' THEN 'COOPERATIVA'
                    WHEN UPPER(nombre) LIKE '%ASOCIACION%' OR UPPER(nombre) LIKE '%ASOC%' THEN 'ASOCIACION'
                    WHEN UPPER(nombre) LIKE '%JUNTA%' THEN 'JUNTA'
                    WHEN UPPER(nombre) LIKE '%CENTRO%' THEN 'CENTRO'
                    WHEN UPPER(nombre) LIKE '%COMITE%' THEN 'COMITE'
                    WHEN UPPER(nombre) LIKE '%UNION%' THEN 'UNION'
                    WHEN UPPER(nombre) LIKE '%FEDERACION%' THEN 'FEDERACION'
                    WHEN UPPER(nombre) LIKE '%FUNDACION%' THEN 'FUNDACION'
                    WHEN UPPER(nombre) LIKE '%EMPRESA%' THEN 'EMPRESA'
                    ELSE 'OTROS'
                END as tipo,
                -- Categoría por sector
                CASE
                    WHEN UPPER(nombre) LIKE '%AGRICOLA%' OR UPPER(nombre) LIKE '%AGRICULTORES%' THEN 'AGRICOLA'
                    WHEN UPPER(nombre) LIKE '%GANADERO%' OR UPPER(nombre) LIKE '%GANADERIA%' THEN 'GANADERA'
                    WHEN UPPER(nombre) LIKE '%PECUARIO%' OR UPPER(nombre) LIKE '%PECUARIA%' THEN 'PECUARIA'
                    WHEN UPPER(nombre) LIKE '%AGROINDUSTRIAL%' THEN 'AGROINDUSTRIAL'
                    WHEN UPPER(nombre) LIKE '%CAMPESINO%' OR UPPER(nombre) LIKE '%CAMPESINA%' THEN 'CAMPESINA'
                    WHEN UPPER(nombre) LIKE '%PRODUCTOR%' THEN 'PRODUCTORES'
                    WHEN UPPER(nombre) LIKE '%MUJERES%' OR UPPER(nombre) LIKE '%FEMENINO%' THEN 'MUJERES'
                    ELSE 'GENERAL'
                END as categoria,
                'ACTIVA' as estado,
                CURRENT_DATE,
                TRUE
            FROM operational.organizacion
            ON CONFLICT (organizacion_id) DO UPDATE SET
                tipo = EXCLUDED.tipo,
                categoria = EXCLUDED.categoria,
                updated_at = CURRENT_TIMESTAMP
        """)
        
        result = session.execute(sql)
        logger.info(f"Dimensión organización: {result.rowcount} registros procesados")
    
    def _load_dim_persona(self, session: Session) -> None:
        """Carga la dimensión persona con información enriquecida."""
        logger.info("Cargando dimensión persona mejorada...")
        
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
                COALESCE(p.genero, 'NO ESPECIFICADO'),
                NULL as fecha_nacimiento,
                p.edad as edad_actual,
                CASE 
                    WHEN p.edad IS NULL THEN 'NO ESPECIFICADO'
                    WHEN p.edad < 18 THEN 'MENOR'
                    WHEN p.edad BETWEEN 18 AND 25 THEN 'JOVEN (18-25)'
                    WHEN p.edad BETWEEN 26 AND 35 THEN 'ADULTO JOVEN (26-35)'
                    WHEN p.edad BETWEEN 36 AND 45 THEN 'ADULTO (36-45)'
                    WHEN p.edad BETWEEN 46 AND 55 THEN 'ADULTO MADURO (46-55)'
                    WHEN p.edad BETWEEN 56 AND 65 THEN 'ADULTO MAYOR (56-65)'
                    WHEN p.edad BETWEEN 66 AND 75 THEN 'TERCERA EDAD (66-75)'
                    ELSE 'TERCERA EDAD AVANZADA (>75)'
                END as grupo_etario,
                NULL as email,
                p.telefono,
                CASE WHEN pa.persona_id IS NOT NULL THEN TRUE ELSE FALSE END as es_beneficiario_semillas,
                COALESCE(pa.tipo_productor, 'NO CLASIFICADO'),
                pa.hectarias_totales,
                CASE 
                    WHEN pa.hectarias_totales IS NULL THEN 'NO ESPECIFICADO'
                    WHEN pa.hectarias_totales = 0 THEN 'SIN TIERRA'
                    WHEN pa.hectarias_totales > 0 AND pa.hectarias_totales <= 0.5 THEN 'MICRO (<= 0.5 ha)'
                    WHEN pa.hectarias_totales > 0.5 AND pa.hectarias_totales <= 1 THEN 'MINIFUNDIO (0.5-1 ha)'
                    WHEN pa.hectarias_totales > 1 AND pa.hectarias_totales <= 2 THEN 'PEQUEÑO (1-2 ha)'
                    WHEN pa.hectarias_totales > 2 AND pa.hectarias_totales <= 5 THEN 'PEQUEÑO-MEDIANO (2-5 ha)'
                    WHEN pa.hectarias_totales > 5 AND pa.hectarias_totales <= 10 THEN 'MEDIANO (5-10 ha)'
                    WHEN pa.hectarias_totales > 10 AND pa.hectarias_totales <= 20 THEN 'MEDIANO-GRANDE (10-20 ha)'
                    WHEN pa.hectarias_totales > 20 AND pa.hectarias_totales <= 50 THEN 'GRANDE (20-50 ha)'
                    ELSE 'MUY GRANDE (>50 ha)'
                END as rango_hectarias,
                o.nombre as organizacion_nombre,
                CURRENT_DATE,
                TRUE
            FROM operational.persona_base p
            LEFT JOIN operational.beneficiario_semillas pa ON p.id = pa.persona_id
            LEFT JOIN operational.organizacion o ON pa.organizacion_id = o.id
            ON CONFLICT (persona_id) DO UPDATE SET
                genero = EXCLUDED.genero,
                edad_actual = EXCLUDED.edad_actual,
                grupo_etario = EXCLUDED.grupo_etario,
                telefono = EXCLUDED.telefono,
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
        """Carga la tabla de hechos de beneficios con información enriquecida."""
        logger.info("Cargando fact beneficio mejorada...")
        
        sql = text("""
            INSERT INTO analytics.fact_beneficio (
                beneficio_id,
                persona_key,
                ubicacion_key,
                organizacion_key,
                tiempo_key,
                tipo_beneficio,
                categoria_beneficio,
                hectarias_sembradas,
                valor_monetario,
                fecha_beneficio,
                anio,
                mes,
                trimestre
            )
            SELECT
                bb.id,
                dp.persona_key,
                du.ubicacion_key,
                COALESCE(org.organizacion_key, -1), -- -1 para sin organización
                dt.tiempo_key,
                UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo, 'NO ESPECIFICADO')),
                CASE 
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%MAIZ%' THEN 'MAIZ'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%ARROZ%' THEN 'ARROZ'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%SOYA%' THEN 'SOYA'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%FREJOL%' OR 
                         UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%FRIJOL%' THEN 'FREJOL'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%CACAO%' THEN 'CACAO'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%CAFE%' THEN 'CAFE'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%BANANO%' OR
                         UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%PLATANO%' THEN 'BANANO/PLATANO'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%PAPA%' THEN 'PAPA'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%YUCA%' THEN 'YUCA'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%TOMATE%' THEN 'TOMATE'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%CEBOLLA%' THEN 'CEBOLLA'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%HORTALIZA%' OR
                         UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%VERDURA%' THEN 'HORTALIZAS'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%FRUTA%' THEN 'FRUTAS'
                    WHEN UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%PASTO%' OR
                         UPPER(COALESCE(bb.tipo_beneficio, bs.tipo_cultivo)) LIKE '%FORRAJE%' THEN 'PASTOS/FORRAJES'
                    ELSE 'OTROS CULTIVOS'
                END as categoria_beneficio,
                COALESCE(bb.hectarias_beneficiadas, bs.hectarias_beneficiadas),
                COALESCE(bb.valor_monetario, bs.inversion),
                bb.fecha,
                EXTRACT(YEAR FROM bb.fecha)::INTEGER,
                EXTRACT(MONTH FROM bb.fecha)::INTEGER,
                EXTRACT(QUARTER FROM bb.fecha)::INTEGER
            FROM operational.beneficio_base bb
            INNER JOIN operational.beneficio_semillas bs ON bb.id = bs.beneficio_id
            INNER JOIN analytics.dim_persona dp ON bb.persona_id = dp.persona_id
            INNER JOIN analytics.dim_ubicacion du ON bb.ubicacion_id = du.ubicacion_id
            LEFT JOIN operational.beneficiario_semillas pa ON bb.persona_id = pa.persona_id
            LEFT JOIN analytics.dim_organizacion org ON pa.organizacion_id = org.organizacion_id
            INNER JOIN analytics.dim_tiempo dt ON bb.fecha = dt.fecha
            WHERE bb.fecha IS NOT NULL
            ON CONFLICT DO NOTHING
        """)
        
        result = session.execute(sql)
        logger.info(f"Fact beneficio: {result.rowcount} registros cargados")
        
        count_sql = text("SELECT COUNT(*) FROM analytics.fact_beneficio")
        count = session.execute(count_sql).scalar()
        logger.info(f"Total registros en fact_beneficio: {count}")
    
    def _create_aggregated_tables(self, session: Session) -> None:
        """Crea tablas agregadas para análisis rápido."""
        logger.info("Creando tablas agregadas...")
        
        # Crear vista materializada de resumen por persona
        sql_persona_summary = text("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_persona_resumen AS
            SELECT 
                p.persona_key,
                p.cedula,
                p.nombres_apellidos,
                p.genero,
                p.grupo_etario,
                p.rango_hectarias,
                p.organizacion_nombre,
                u.provincia,
                u.canton,
                u.zona,
                u.region,
                COUNT(DISTINCT f.beneficio_id) as total_beneficios,
                COUNT(DISTINCT f.anio) as años_beneficiado,
                SUM(f.valor_monetario) as valor_total_recibido,
                AVG(f.valor_monetario) as valor_promedio_beneficio,
                MAX(f.fecha_beneficio) as ultima_fecha_beneficio,
                STRING_AGG(DISTINCT f.categoria_beneficio, ', ') as tipos_cultivos_beneficiados
            FROM analytics.dim_persona p
            LEFT JOIN analytics.fact_beneficio f ON p.persona_key = f.persona_key
            LEFT JOIN analytics.dim_ubicacion u ON f.ubicacion_key = u.ubicacion_key
            WHERE p.es_beneficiario_semillas = TRUE
            GROUP BY 
                p.persona_key, p.cedula, p.nombres_apellidos, p.genero, 
                p.grupo_etario, p.rango_hectarias, p.organizacion_nombre,
                u.provincia, u.canton, u.zona, u.region
        """)
        
        try:
            session.execute(sql_persona_summary)
            logger.info("Vista materializada de resumen por persona creada")
        except Exception as e:
            logger.warning(f"Vista ya existe o error al crear: {e}")
        
        # Crear índices para mejorar performance
        indices = [
            "CREATE INDEX IF NOT EXISTS idx_mv_persona_provincia ON analytics.mv_persona_resumen(provincia)",
            "CREATE INDEX IF NOT EXISTS idx_mv_persona_canton ON analytics.mv_persona_resumen(canton)",
            "CREATE INDEX IF NOT EXISTS idx_mv_persona_genero ON analytics.mv_persona_resumen(genero)",
            "CREATE INDEX IF NOT EXISTS idx_mv_persona_rango_hect ON analytics.mv_persona_resumen(rango_hectarias)"
        ]
        
        for idx_sql in indices:
            try:
                session.execute(text(idx_sql))
            except Exception as e:
                logger.debug(f"Índice ya existe: {e}")
    
    def _calculate_quality_metrics(self, session: Session) -> None:
        """Calcula métricas de calidad de datos."""
        logger.info("Calculando métricas de calidad...")
        
        metrics_sql = text("""
            WITH metricas AS (
                SELECT
                    -- Completitud de datos
                    COUNT(*) as total_personas,
                    COUNT(NULLIF(cedula, '')) as personas_con_cedula,
                    COUNT(telefono) as personas_con_telefono,
                    COUNT(CASE WHEN genero != 'NO ESPECIFICADO' THEN 1 END) as personas_con_genero,
                    COUNT(edad_actual) as personas_con_edad,
                    
                    -- Calidad de beneficiarios
                    COUNT(CASE WHEN es_beneficiario_semillas THEN 1 END) as total_beneficiarios,
                    COUNT(CASE WHEN es_beneficiario_semillas AND hectarias_totales IS NOT NULL THEN 1 END) as beneficiarios_con_hectareas,
                    COUNT(CASE WHEN es_beneficiario_semillas AND organizacion_nombre IS NOT NULL THEN 1 END) as beneficiarios_con_organizacion
                FROM analytics.dim_persona
            )
            SELECT
                total_personas,
                ROUND(100.0 * personas_con_cedula / NULLIF(total_personas, 0), 2) as pct_con_cedula,
                ROUND(100.0 * personas_con_telefono / NULLIF(total_personas, 0), 2) as pct_con_telefono,
                ROUND(100.0 * personas_con_genero / NULLIF(total_personas, 0), 2) as pct_con_genero,
                ROUND(100.0 * personas_con_edad / NULLIF(total_personas, 0), 2) as pct_con_edad,
                total_beneficiarios,
                ROUND(100.0 * beneficiarios_con_hectareas / NULLIF(total_beneficiarios, 0), 2) as pct_beneficiarios_con_hectareas,
                ROUND(100.0 * beneficiarios_con_organizacion / NULLIF(total_beneficiarios, 0), 2) as pct_beneficiarios_con_organizacion
            FROM metricas
        """)
        
        result = session.execute(metrics_sql).first()
        if result:
            logger.info("=== MÉTRICAS DE CALIDAD DE DATOS ===")
            logger.info(f"Total personas: {result.total_personas}")
            logger.info(f"% con cédula: {result.pct_con_cedula}%")
            logger.info(f"% con teléfono: {result.pct_con_telefono}%") 
            logger.info(f"% con género especificado: {result.pct_con_genero}%")
            logger.info(f"% con edad: {result.pct_con_edad}%")
            logger.info(f"Total beneficiarios: {result.total_beneficiarios}")
            logger.info(f"% beneficiarios con hectáreas: {result.pct_beneficiarios_con_hectareas}%")
            logger.info(f"% beneficiarios con organización: {result.pct_beneficiarios_con_organizacion}%")
    
    def get_statistics(self, session: Session) -> Dict[str, Any]:
        """Obtiene estadísticas mejoradas del esquema analytical."""
        stats = {}
        
        queries = {
            'total_personas': "SELECT COUNT(*) FROM analytics.dim_persona",
            'total_beneficiarios_semillas': "SELECT COUNT(*) FROM analytics.dim_persona WHERE es_beneficiario_semillas = TRUE",
            'total_ubicaciones': "SELECT COUNT(*) FROM analytics.dim_ubicacion",
            'total_organizaciones': "SELECT COUNT(*) FROM analytics.dim_organizacion",
            'total_beneficios': "SELECT COUNT(*) FROM analytics.fact_beneficio",
            'total_hectarias': "SELECT SUM(hectarias_totales) FROM analytics.dim_persona WHERE hectarias_totales IS NOT NULL",
            'promedio_hectarias': "SELECT AVG(hectarias_totales) FROM analytics.dim_persona WHERE hectarias_totales IS NOT NULL",
            'valor_total_beneficios': "SELECT SUM(valor_monetario) FROM analytics.fact_beneficio",
            'promedio_valor_beneficio': "SELECT AVG(valor_monetario) FROM analytics.fact_beneficio WHERE valor_monetario > 0",
            'cultivos_distintos': "SELECT COUNT(DISTINCT categoria_beneficio) FROM analytics.fact_beneficio",
            'provincias_cubiertas': "SELECT COUNT(DISTINCT provincia) FROM analytics.dim_ubicacion WHERE provincia != 'NO ESPECIFICADO'",
            'personas_con_multiples_beneficios': """
                SELECT COUNT(*) FROM (
                    SELECT persona_key, COUNT(*) as num_beneficios 
                    FROM analytics.fact_beneficio 
                    GROUP BY persona_key 
                    HAVING COUNT(*) > 1
                ) t
            """
        }
        
        for key, query in queries.items():
            result = session.execute(text(query)).scalar()
            stats[key] = result if result is not None else 0
        
        # Agregar estadísticas adicionales
        if stats['total_beneficiarios_semillas'] > 0:
            stats['cobertura_beneficiarios'] = round(
                stats['total_beneficiarios_semillas'] / stats['total_personas'] * 100, 2
            )
        else:
            stats['cobertura_beneficiarios'] = 0
            
        return stats