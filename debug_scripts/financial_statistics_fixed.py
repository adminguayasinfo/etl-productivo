#!/usr/bin/env python3
"""Analyze financial statistics for semillas and fertilizantes."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config.connections.database import db_connection
import pandas as pd

def get_financial_statistics():
    """Get financial statistics from operational and analytical data."""
    if not db_connection.engine:
        db_connection.init_engine()
    engine = db_connection.engine
    
    with engine.connect() as conn:
        print("\n" + "="*80)
        print("ESTADÍSTICAS FINANCIERAS - SEMILLAS Y FERTILIZANTES")
        print("="*80)
        
        # 1. Resumen general por tipo de beneficio
        print("\n1. RESUMEN GENERAL POR TIPO DE BENEFICIO")
        print("-" * 60)
        query = text("""
            SELECT 
                tipo_beneficio,
                COUNT(*) as total_beneficios,
                COUNT(DISTINCT persona_id) as total_beneficiarios,
                SUM(valor_monetario) as valor_total,
                AVG(valor_monetario) as valor_promedio,
                MIN(valor_monetario) as valor_minimo,
                MAX(valor_monetario) as valor_maximo
            FROM operational.beneficio_base
            WHERE tipo_beneficio IN ('SEMILLAS', 'fertilizantes')
            GROUP BY tipo_beneficio
            ORDER BY tipo_beneficio;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))
        
        # 2. Distribución por tipo de cultivo - Semillas
        print("\n2. DISTRIBUCIÓN POR TIPO DE CULTIVO - SEMILLAS")
        print("-" * 60)
        query = text("""
            SELECT 
                bs.tipo_cultivo,
                COUNT(*) as total_beneficios,
                COUNT(DISTINCT bb.persona_id) as total_beneficiarios,
                SUM(bb.valor_monetario) as valor_total,
                AVG(bb.valor_monetario) as valor_promedio
            FROM operational.beneficio_base bb
            JOIN operational.beneficio_semillas bs ON bb.id = bs.id
            WHERE bb.tipo_beneficio = 'SEMILLAS'
            AND bs.tipo_cultivo IS NOT NULL
            GROUP BY bs.tipo_cultivo
            ORDER BY valor_total DESC;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))
        
        # 3. Distribución por tipo de cultivo - Fertilizantes
        print("\n3. DISTRIBUCIÓN POR TIPO DE CULTIVO - FERTILIZANTES")
        print("-" * 60)
        query = text("""
            SELECT 
                bf.tipo_cultivo,
                COUNT(*) as total_beneficios,
                COUNT(DISTINCT bb.persona_id) as total_beneficiarios,
                SUM(bb.valor_monetario) as valor_total,
                AVG(bb.valor_monetario) as valor_promedio
            FROM operational.beneficio_base bb
            JOIN operational.beneficio_fertilizantes bf ON bb.id = bf.id
            WHERE bb.tipo_beneficio = 'fertilizantes'
            AND bf.tipo_cultivo IS NOT NULL
            GROUP BY bf.tipo_cultivo
            ORDER BY valor_total DESC;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))
        
        # 4. Top 10 beneficiarios por valor - Semillas
        print("\n4. TOP 10 BENEFICIARIOS POR VALOR - SEMILLAS")
        print("-" * 60)
        query = text("""
            SELECT 
                p.cedula,
                p.nombres_apellidos as nombre_completo,
                COUNT(*) as total_beneficios,
                SUM(bb.valor_monetario) as valor_total_recibido,
                AVG(bb.valor_monetario) as valor_promedio
            FROM operational.beneficio_base bb
            JOIN operational.persona_base p ON bb.persona_id = p.id
            WHERE bb.tipo_beneficio = 'SEMILLAS'
            GROUP BY p.cedula, p.nombres_apellidos
            ORDER BY valor_total_recibido DESC
            LIMIT 10;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))
        
        # 5. Top 10 beneficiarios por valor - Fertilizantes
        print("\n5. TOP 10 BENEFICIARIOS POR VALOR - FERTILIZANTES")
        print("-" * 60)
        query = text("""
            SELECT 
                p.cedula,
                p.nombres_apellidos as nombre_completo,
                COUNT(*) as total_beneficios,
                SUM(bb.valor_monetario) as valor_total_recibido,
                AVG(bb.valor_monetario) as valor_promedio
            FROM operational.beneficio_base bb
            JOIN operational.persona_base p ON bb.persona_id = p.id
            WHERE bb.tipo_beneficio = 'fertilizantes'
            GROUP BY p.cedula, p.nombres_apellidos
            ORDER BY valor_total_recibido DESC
            LIMIT 10;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))
        
        # 6. Distribución por ubicación (Canton)
        print("\n6. DISTRIBUCIÓN POR CANTON")
        print("-" * 60)
        query = text("""
            SELECT 
                u.canton,
                bb.tipo_beneficio,
                COUNT(*) as total_beneficios,
                SUM(bb.valor_monetario) as valor_total,
                AVG(bb.valor_monetario) as valor_promedio
            FROM operational.beneficio_base bb
            JOIN operational.ubicacion u ON bb.ubicacion_id = u.id
            WHERE bb.tipo_beneficio IN ('SEMILLAS', 'fertilizantes')
            GROUP BY u.canton, bb.tipo_beneficio
            ORDER BY u.canton, bb.tipo_beneficio;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))
        
        # 7. Estadísticas desde el esquema analítico
        print("\n7. ESTADÍSTICAS DESDE ESQUEMA ANALÍTICO (FACT_BENEFICIO)")
        print("-" * 60)
        query = text("""
            SELECT COUNT(*) FROM analytics.fact_beneficio
        """)
        result = conn.execute(query)
        count = result.scalar()
        if count > 0:
            query = text("""
                SELECT 
                    fb.tipo_beneficio,
                    dc.nombre_cultivo,
                    COUNT(*) as total_registros,
                    SUM(fb.valor_total) as valor_total,
                    AVG(fb.valor_total) as valor_promedio,
                    MIN(fb.valor_total) as valor_minimo,
                    MAX(fb.valor_total) as valor_maximo
                FROM analytics.fact_beneficio fb
                JOIN analytics.dim_cultivo dc ON fb.cultivo_key = dc.cultivo_key
                WHERE fb.tipo_beneficio IN ('SEMILLAS', 'fertilizantes')
                GROUP BY fb.tipo_beneficio, dc.nombre_cultivo
                ORDER BY fb.tipo_beneficio, valor_total DESC;
            """)
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            print(df.to_string(index=False))
        else:
            print("No hay datos en el esquema analítico aún")
        
        # 8. Resumen financiero total
        print("\n8. RESUMEN FINANCIERO TOTAL")
        print("-" * 60)
        query = text("""
            WITH totales AS (
                SELECT 
                    tipo_beneficio,
                    SUM(valor_monetario) as valor_total,
                    COUNT(*) as total_beneficios,
                    COUNT(DISTINCT persona_id) as beneficiarios_unicos
                FROM operational.beneficio_base
                WHERE tipo_beneficio IN ('SEMILLAS', 'fertilizantes')
                GROUP BY tipo_beneficio
            )
            SELECT 
                tipo_beneficio,
                valor_total,
                total_beneficios,
                beneficiarios_unicos,
                valor_total / NULLIF(total_beneficios, 0) as valor_promedio_por_beneficio,
                valor_total / NULLIF(beneficiarios_unicos, 0) as valor_promedio_por_beneficiario
            FROM totales
            UNION ALL
            SELECT 
                'TOTAL' as tipo_beneficio,
                SUM(valor_total) as valor_total,
                SUM(total_beneficios) as total_beneficios,
                SUM(beneficiarios_unicos) as beneficiarios_unicos,
                SUM(valor_total) / NULLIF(SUM(total_beneficios), 0) as valor_promedio_por_beneficio,
                SUM(valor_total) / NULLIF(SUM(beneficiarios_unicos), 0) as valor_promedio_por_beneficiario
            FROM totales;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))
        
        # 9. Detalles adicionales - Semillas
        print("\n9. DETALLES ADICIONALES - SEMILLAS (por KG)")
        print("-" * 60)
        query = text("""
            SELECT 
                bs.kg_semilla,
                COUNT(*) as cantidad,
                SUM(bb.valor_monetario) as valor_total,
                AVG(bb.valor_monetario) as valor_promedio,
                SUM(bb.valor_monetario) / NULLIF(SUM(bs.kg_semilla), 0) as valor_por_kg
            FROM operational.beneficio_base bb
            JOIN operational.beneficio_semillas bs ON bb.id = bs.id
            WHERE bb.tipo_beneficio = 'SEMILLAS' 
            AND bs.kg_semilla IS NOT NULL
            GROUP BY bs.kg_semilla
            ORDER BY cantidad DESC
            LIMIT 10;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No hay datos de kg_semilla disponibles")
        
        # 10. Organizaciones con mayor inversión
        print("\n10. ORGANIZACIONES CON MAYOR INVERSIÓN")
        print("-" * 60)
        query = text("""
            WITH org_stats AS (
                SELECT 
                    o.id,
                    o.nombre as organizacion,
                    bb.tipo_beneficio,
                    COUNT(*) as total_beneficios,
                    SUM(bb.valor_monetario) as valor_total
                FROM operational.beneficio_base bb
                JOIN operational.beneficiario_semillas bs ON bb.persona_id = bs.persona_id
                JOIN operational.organizacion o ON bs.organizacion_id = o.id
                WHERE bb.tipo_beneficio = 'SEMILLAS'
                GROUP BY o.id, o.nombre, bb.tipo_beneficio
                
                UNION ALL
                
                SELECT 
                    o.id,
                    o.nombre as organizacion,
                    bb.tipo_beneficio,
                    COUNT(*) as total_beneficios,
                    SUM(bb.valor_monetario) as valor_total
                FROM operational.beneficio_base bb
                JOIN operational.beneficiario_fertilizantes bf ON bb.persona_id = bf.persona_id
                JOIN operational.organizacion o ON bf.organizacion_id = o.id
                WHERE bb.tipo_beneficio = 'fertilizantes'
                GROUP BY o.id, o.nombre, bb.tipo_beneficio
            )
            SELECT 
                organizacion,
                tipo_beneficio,
                total_beneficios,
                valor_total
            FROM org_stats
            WHERE organizacion IS NOT NULL
            ORDER BY valor_total DESC
            LIMIT 10;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No hay datos de organizaciones disponibles")
            
        # 11. Distribución por hectáreas beneficiadas
        print("\n11. DISTRIBUCIÓN POR HECTÁREAS BENEFICIADAS")
        print("-" * 60)
        query = text("""
            SELECT 
                tipo_beneficio,
                COUNT(*) as total_beneficios,
                SUM(hectarias_beneficiadas) as total_hectarias,
                AVG(hectarias_beneficiadas) as promedio_hectarias,
                MIN(hectarias_beneficiadas) as min_hectarias,
                MAX(hectarias_beneficiadas) as max_hectarias
            FROM operational.beneficio_base
            WHERE tipo_beneficio IN ('SEMILLAS', 'fertilizantes')
            AND hectarias_beneficiadas IS NOT NULL
            GROUP BY tipo_beneficio
            ORDER BY tipo_beneficio;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.to_string(index=False))

if __name__ == "__main__":
    get_financial_statistics()