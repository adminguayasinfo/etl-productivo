-- Financial Statistics Analysis for Semillas and Fertilizantes
-- Analytics Schema Queries

-- ============================================
-- 1. TOTAL MONETARY VALUES BY TYPE
-- ============================================

-- Total monetary values (valor_total) by type
WITH tipo_beneficio_totals AS (
    SELECT 
        tipo_beneficio,
        SUM(valor_total) as total_valor,
        COUNT(DISTINCT beneficiario_id) as total_beneficiarios,
        COUNT(*) as total_beneficios
    FROM analytics.fact_beneficio
    GROUP BY tipo_beneficio
)
SELECT 
    tipo_beneficio,
    total_valor,
    total_beneficiarios,
    total_beneficios,
    ROUND(total_valor::numeric / NULLIF(total_beneficiarios, 0), 2) as avg_valor_por_beneficiario
FROM tipo_beneficio_totals
ORDER BY total_valor DESC;

-- ============================================
-- 2. AVERAGE VALUES PER BENEFICIARY BY TYPE
-- ============================================

-- Average values per beneficiary by type
SELECT 
    tipo_beneficio,
    beneficiario_id,
    p.nombre || ' ' || p.apellido_paterno || ' ' || COALESCE(p.apellido_materno, '') as beneficiario_nombre,
    COUNT(*) as num_beneficios,
    SUM(valor_total) as valor_total_beneficiario,
    ROUND(AVG(valor_total)::numeric, 2) as valor_promedio_por_beneficio
FROM analytics.fact_beneficio f
JOIN analytics.dim_persona p ON f.beneficiario_id = p.persona_id
GROUP BY tipo_beneficio, beneficiario_id, p.nombre, p.apellido_paterno, p.apellido_materno
ORDER BY tipo_beneficio, valor_total_beneficiario DESC;

-- ============================================
-- 3. DISTRIBUTION BY CROP TYPE (TIPO_CULTIVO)
-- ============================================

-- Distribution by crop type
SELECT 
    tipo_beneficio,
    tipo_cultivo,
    COUNT(*) as num_beneficios,
    SUM(valor_total) as valor_total,
    ROUND(AVG(valor_total)::numeric, 2) as valor_promedio,
    ROUND(100.0 * SUM(valor_total) / SUM(SUM(valor_total)) OVER (PARTITION BY tipo_beneficio), 2) as porcentaje_del_tipo
FROM analytics.fact_beneficio
GROUP BY tipo_beneficio, tipo_cultivo
ORDER BY tipo_beneficio, valor_total DESC;

-- ============================================
-- 4. TOP BENEFICIARIES BY VALUE
-- ============================================

-- Top 10 beneficiaries by value for each type
WITH ranked_beneficiaries AS (
    SELECT 
        tipo_beneficio,
        beneficiario_id,
        p.nombre || ' ' || p.apellido_paterno || ' ' || COALESCE(p.apellido_materno, '') as beneficiario_nombre,
        p.telefono,
        SUM(valor_total) as valor_total_beneficiario,
        COUNT(*) as num_beneficios,
        ROW_NUMBER() OVER (PARTITION BY tipo_beneficio ORDER BY SUM(valor_total) DESC) as ranking
    FROM analytics.fact_beneficio f
    JOIN analytics.dim_persona p ON f.beneficiario_id = p.persona_id
    GROUP BY tipo_beneficio, beneficiario_id, p.nombre, p.apellido_paterno, p.apellido_materno, p.telefono
)
SELECT 
    tipo_beneficio,
    ranking,
    beneficiario_nombre,
    telefono,
    valor_total_beneficiario,
    num_beneficios,
    ROUND(valor_total_beneficiario::numeric / num_beneficios, 2) as valor_promedio_por_beneficio
FROM ranked_beneficiaries
WHERE ranking <= 10
ORDER BY tipo_beneficio, ranking;

-- ============================================
-- 5. SUMMARY STATISTICS (MIN, MAX, AVG, SUM)
-- ============================================

-- Summary statistics by type
SELECT 
    tipo_beneficio,
    COUNT(*) as total_registros,
    COUNT(DISTINCT beneficiario_id) as beneficiarios_unicos,
    MIN(valor_total) as valor_minimo,
    MAX(valor_total) as valor_maximo,
    ROUND(AVG(valor_total)::numeric, 2) as valor_promedio,
    ROUND(STDDEV(valor_total)::numeric, 2) as desviacion_estandar,
    SUM(valor_total) as valor_total,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valor_total) as mediana_valor
FROM analytics.fact_beneficio
GROUP BY tipo_beneficio;

-- ============================================
-- 6. COMPARISON BETWEEN SEMILLAS AND FERTILIZANTES
-- ============================================

-- Direct comparison between semillas and fertilizantes
WITH comparison_base AS (
    SELECT 
        tipo_beneficio,
        COUNT(*) as num_beneficios,
        COUNT(DISTINCT beneficiario_id) as num_beneficiarios,
        SUM(valor_total) as valor_total,
        AVG(valor_total) as valor_promedio,
        MIN(fecha_entrega) as fecha_inicio,
        MAX(fecha_entrega) as fecha_fin
    FROM analytics.fact_beneficio
    WHERE tipo_beneficio IN ('semillas', 'fertilizantes')
    GROUP BY tipo_beneficio
)
SELECT 
    s.num_beneficios as semillas_beneficios,
    f.num_beneficios as fertilizantes_beneficios,
    s.num_beneficiarios as semillas_beneficiarios,
    f.num_beneficiarios as fertilizantes_beneficiarios,
    s.valor_total as semillas_valor_total,
    f.valor_total as fertilizantes_valor_total,
    ROUND(s.valor_promedio::numeric, 2) as semillas_valor_promedio,
    ROUND(f.valor_promedio::numeric, 2) as fertilizantes_valor_promedio,
    ROUND(100.0 * s.valor_total / (s.valor_total + f.valor_total), 2) as semillas_porcentaje_valor,
    ROUND(100.0 * f.valor_total / (s.valor_total + f.valor_total), 2) as fertilizantes_porcentaje_valor
FROM 
    (SELECT * FROM comparison_base WHERE tipo_beneficio = 'semillas') s,
    (SELECT * FROM comparison_base WHERE tipo_beneficio = 'fertilizantes') f;

-- ============================================
-- ADDITIONAL ANALYSIS
-- ============================================

-- Monthly trends for semillas and fertilizantes
SELECT 
    tipo_beneficio,
    TO_CHAR(fecha_entrega, 'YYYY-MM') as mes,
    COUNT(*) as num_beneficios,
    SUM(valor_total) as valor_total_mes,
    ROUND(AVG(valor_total)::numeric, 2) as valor_promedio_mes,
    COUNT(DISTINCT beneficiario_id) as beneficiarios_unicos_mes
FROM analytics.fact_beneficio
WHERE tipo_beneficio IN ('semillas', 'fertilizantes')
GROUP BY tipo_beneficio, TO_CHAR(fecha_entrega, 'YYYY-MM')
ORDER BY tipo_beneficio, mes;

-- Geographic distribution by state
SELECT 
    tipo_beneficio,
    u.estado,
    COUNT(*) as num_beneficios,
    SUM(f.valor_total) as valor_total_estado,
    ROUND(AVG(f.valor_total)::numeric, 2) as valor_promedio_estado,
    COUNT(DISTINCT f.beneficiario_id) as beneficiarios_unicos_estado
FROM analytics.fact_beneficio f
JOIN analytics.dim_ubicacion u ON f.ubicacion_id = u.ubicacion_id
GROUP BY tipo_beneficio, u.estado
ORDER BY tipo_beneficio, valor_total_estado DESC;

-- Organization distribution
SELECT 
    tipo_beneficio,
    o.nombre as organizacion,
    COUNT(*) as num_beneficios,
    SUM(f.valor_total) as valor_total_org,
    ROUND(AVG(f.valor_total)::numeric, 2) as valor_promedio_org,
    COUNT(DISTINCT f.beneficiario_id) as beneficiarios_unicos_org
FROM analytics.fact_beneficio f
JOIN analytics.dim_organizacion o ON f.organizacion_id = o.organizacion_id
GROUP BY tipo_beneficio, o.nombre
ORDER BY tipo_beneficio, valor_total_org DESC;

-- Beneficiaries receiving both semillas and fertilizantes
WITH beneficiarios_ambos AS (
    SELECT 
        beneficiario_id,
        p.nombre || ' ' || p.apellido_paterno || ' ' || COALESCE(p.apellido_materno, '') as beneficiario_nombre,
        SUM(CASE WHEN tipo_beneficio = 'semillas' THEN valor_total ELSE 0 END) as valor_semillas,
        SUM(CASE WHEN tipo_beneficio = 'fertilizantes' THEN valor_total ELSE 0 END) as valor_fertilizantes,
        COUNT(CASE WHEN tipo_beneficio = 'semillas' THEN 1 END) as num_beneficios_semillas,
        COUNT(CASE WHEN tipo_beneficio = 'fertilizantes' THEN 1 END) as num_beneficios_fertilizantes
    FROM analytics.fact_beneficio f
    JOIN analytics.dim_persona p ON f.beneficiario_id = p.persona_id
    WHERE tipo_beneficio IN ('semillas', 'fertilizantes')
    GROUP BY beneficiario_id, p.nombre, p.apellido_paterno, p.apellido_materno
    HAVING COUNT(DISTINCT tipo_beneficio) = 2
)
SELECT 
    beneficiario_nombre,
    valor_semillas,
    valor_fertilizantes,
    valor_semillas + valor_fertilizantes as valor_total_combinado,
    num_beneficios_semillas,
    num_beneficios_fertilizantes,
    ROUND((valor_semillas + valor_fertilizantes)::numeric / (num_beneficios_semillas + num_beneficios_fertilizantes), 2) as valor_promedio_por_beneficio
FROM beneficiarios_ambos
ORDER BY valor_total_combinado DESC
LIMIT 20;

-- Value distribution percentiles
SELECT 
    tipo_beneficio,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY valor_total) as percentil_10,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY valor_total) as percentil_25,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valor_total) as mediana,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY valor_total) as percentil_75,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY valor_total) as percentil_90,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY valor_total) as percentil_95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY valor_total) as percentil_99
FROM analytics.fact_beneficio
WHERE tipo_beneficio IN ('semillas', 'fertilizantes')
GROUP BY tipo_beneficio;

-- Year-over-year growth analysis
WITH yearly_stats AS (
    SELECT 
        tipo_beneficio,
        EXTRACT(YEAR FROM fecha_entrega) as año,
        COUNT(*) as num_beneficios,
        COUNT(DISTINCT beneficiario_id) as num_beneficiarios,
        SUM(valor_total) as valor_total_año
    FROM analytics.fact_beneficio
    WHERE tipo_beneficio IN ('semillas', 'fertilizantes')
    GROUP BY tipo_beneficio, EXTRACT(YEAR FROM fecha_entrega)
)
SELECT 
    tipo_beneficio,
    año,
    num_beneficios,
    num_beneficiarios,
    valor_total_año,
    LAG(valor_total_año) OVER (PARTITION BY tipo_beneficio ORDER BY año) as valor_año_anterior,
    ROUND(100.0 * (valor_total_año - LAG(valor_total_año) OVER (PARTITION BY tipo_beneficio ORDER BY año)) / 
          NULLIF(LAG(valor_total_año) OVER (PARTITION BY tipo_beneficio ORDER BY año), 0), 2) as crecimiento_porcentual
FROM yearly_stats
ORDER BY tipo_beneficio, año;