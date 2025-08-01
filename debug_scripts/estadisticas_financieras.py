#!/usr/bin/env python3
"""Analyze financial statistics for semillas and fertilizantes."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config.connections.database import db_connection
import pandas as pd

def format_currency(value):
    """Format currency values with proper formatting."""
    if pd.isna(value):
        return "N/A"
    return f"${value:,.2f}"

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
        print("-" * 80)
        query = text("""
            SELECT 
                UPPER(tipo_beneficio) as tipo_beneficio,
                COUNT(*) as total_beneficios,
                COUNT(DISTINCT persona_id) as total_beneficiarios,
                SUM(valor_monetario) as valor_total,
                AVG(valor_monetario) as valor_promedio,
                MIN(valor_monetario) as valor_minimo,
                MAX(valor_monetario) as valor_maximo
            FROM operational.beneficio_base
            WHERE tipo_beneficio IN ('SEMILLAS', 'fertilizantes')
            GROUP BY UPPER(tipo_beneficio)
            ORDER BY tipo_beneficio;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        # Format currency columns
        currency_cols = ['valor_total', 'valor_promedio', 'valor_minimo', 'valor_maximo']
        for col in currency_cols:
            df[col] = df[col].apply(format_currency)
        
        print(df.to_string(index=False))
        
        # 2. Distribución por tipo de cultivo
        print("\n2. DISTRIBUCIÓN POR TIPO DE CULTIVO")
        print("-" * 80)
        query = text("""
            WITH cultivo_stats AS (
                SELECT 
                    'SEMILLAS' as tipo_beneficio,
                    bs.tipo_cultivo,
                    COUNT(*) as total_beneficios,
                    COUNT(DISTINCT bb.persona_id) as total_beneficiarios,
                    SUM(bb.valor_monetario) as valor_total,
                    AVG(bb.valor_monetario) as valor_promedio,
                    SUM(bb.hectarias_beneficiadas) as total_hectarias
                FROM operational.beneficio_base bb
                JOIN operational.beneficio_semillas bs ON bb.id = bs.id
                WHERE bb.tipo_beneficio = 'SEMILLAS'
                AND bs.tipo_cultivo IS NOT NULL
                GROUP BY bs.tipo_cultivo
                
                UNION ALL
                
                SELECT 
                    'FERTILIZANTES' as tipo_beneficio,
                    bf.tipo_cultivo,
                    COUNT(*) as total_beneficios,
                    COUNT(DISTINCT bb.persona_id) as total_beneficiarios,
                    SUM(bb.valor_monetario) as valor_total,
                    AVG(bb.valor_monetario) as valor_promedio,
                    SUM(bb.hectarias_beneficiadas) as total_hectarias
                FROM operational.beneficio_base bb
                JOIN operational.beneficio_fertilizantes bf ON bb.id = bf.id
                WHERE bb.tipo_beneficio = 'fertilizantes'
                AND bf.tipo_cultivo IS NOT NULL
                GROUP BY bf.tipo_cultivo
            )
            SELECT * FROM cultivo_stats
            ORDER BY tipo_beneficio, valor_total DESC;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        # Format currency and numeric columns
        df['valor_total'] = df['valor_total'].apply(format_currency)
        df['valor_promedio'] = df['valor_promedio'].apply(format_currency)
        df['total_hectarias'] = df['total_hectarias'].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "N/A")
        
        print(df.to_string(index=False))
        
        # 3. Top beneficiarios por valor total recibido
        print("\n3. TOP 10 BENEFICIARIOS CON MAYOR INVERSIÓN TOTAL")
        print("-" * 80)
        query = text("""
            SELECT 
                p.cedula,
                p.nombres_apellidos,
                COUNT(*) as total_beneficios,
                SUM(bb.valor_monetario) as valor_total_recibido,
                AVG(bb.valor_monetario) as valor_promedio,
                STRING_AGG(DISTINCT bb.tipo_beneficio, ', ') as tipos_beneficio
            FROM operational.beneficio_base bb
            JOIN operational.persona_base p ON bb.persona_id = p.id
            WHERE bb.tipo_beneficio IN ('SEMILLAS', 'fertilizantes')
            GROUP BY p.id, p.cedula, p.nombres_apellidos
            ORDER BY valor_total_recibido DESC
            LIMIT 10;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        df['valor_total_recibido'] = df['valor_total_recibido'].apply(format_currency)
        df['valor_promedio'] = df['valor_promedio'].apply(format_currency)
        
        print(df.to_string(index=False))
        
        # 4. Distribución geográfica
        print("\n4. DISTRIBUCIÓN GEOGRÁFICA POR CANTÓN (TOP 15)")
        print("-" * 80)
        query = text("""
            SELECT 
                u.canton,
                COUNT(*) as total_beneficios,
                COUNT(DISTINCT bb.persona_id) as total_beneficiarios,
                SUM(bb.valor_monetario) as valor_total,
                AVG(bb.valor_monetario) as valor_promedio,
                SUM(bb.hectarias_beneficiadas) as total_hectarias
            FROM operational.beneficio_base bb
            JOIN operational.ubicacion u ON bb.ubicacion_id = u.id
            WHERE bb.tipo_beneficio IN ('SEMILLAS', 'fertilizantes')
            GROUP BY u.canton
            ORDER BY valor_total DESC
            LIMIT 15;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        df['valor_total'] = df['valor_total'].apply(format_currency)
        df['valor_promedio'] = df['valor_promedio'].apply(format_currency)
        df['total_hectarias'] = df['total_hectarias'].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "N/A")
        
        print(df.to_string(index=False))
        
        # 5. Estadísticas por hectáreas
        print("\n5. ANÁLISIS POR HECTÁREAS BENEFICIADAS")
        print("-" * 80)
        query = text("""
            SELECT 
                UPPER(tipo_beneficio) as tipo_beneficio,
                COUNT(*) as total_registros,
                COUNT(hectarias_beneficiadas) as registros_con_hectarias,
                SUM(hectarias_beneficiadas) as total_hectarias,
                AVG(hectarias_beneficiadas) as promedio_hectarias,
                MIN(hectarias_beneficiadas) as min_hectarias,
                MAX(hectarias_beneficiadas) as max_hectarias,
                SUM(valor_monetario) / NULLIF(SUM(hectarias_beneficiadas), 0) as valor_por_hectarea
            FROM operational.beneficio_base
            WHERE tipo_beneficio IN ('SEMILLAS', 'fertilizantes')
            GROUP BY UPPER(tipo_beneficio)
            ORDER BY tipo_beneficio;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        # Format numeric columns
        numeric_cols = ['total_hectarias', 'promedio_hectarias', 'min_hectarias', 'max_hectarias']
        for col in numeric_cols:
            df[col] = df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "N/A")
        df['valor_por_hectarea'] = df['valor_por_hectarea'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        
        print(df.to_string(index=False))
        
        # 6. Organizaciones con mayor inversión
        print("\n6. ORGANIZACIONES CON MAYOR INVERSIÓN")
        print("-" * 80)
        query = text("""
            WITH org_stats AS (
                SELECT 
                    o.nombre as organizacion,
                    'SEMILLAS' as tipo_beneficio,
                    COUNT(DISTINCT bb.persona_id) as total_beneficiarios,
                    COUNT(*) as total_beneficios,
                    SUM(bb.valor_monetario) as valor_total
                FROM operational.beneficio_base bb
                JOIN operational.beneficiario_semillas bs ON bb.persona_id = bs.persona_id
                JOIN operational.organizacion o ON bs.organizacion_id = o.id
                WHERE bb.tipo_beneficio = 'SEMILLAS'
                GROUP BY o.id, o.nombre
                
                UNION ALL
                
                SELECT 
                    o.nombre as organizacion,
                    'FERTILIZANTES' as tipo_beneficio,
                    COUNT(DISTINCT bb.persona_id) as total_beneficiarios,
                    COUNT(*) as total_beneficios,
                    SUM(bb.valor_monetario) as valor_total
                FROM operational.beneficio_base bb
                JOIN operational.beneficiario_fertilizantes bf ON bb.persona_id = bf.persona_id
                JOIN operational.organizacion o ON bf.organizacion_id = o.id
                WHERE bb.tipo_beneficio = 'fertilizantes'
                GROUP BY o.id, o.nombre
            )
            SELECT 
                organizacion,
                SUM(total_beneficiarios) as total_beneficiarios,
                SUM(total_beneficios) as total_beneficios,
                SUM(valor_total) as valor_total_invertido,
                SUM(valor_total) / NULLIF(SUM(total_beneficiarios), 0) as promedio_por_beneficiario
            FROM org_stats
            WHERE organizacion IS NOT NULL
            GROUP BY organizacion
            ORDER BY valor_total_invertido DESC
            LIMIT 10;
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        if not df.empty:
            df['valor_total_invertido'] = df['valor_total_invertido'].apply(format_currency)
            df['promedio_por_beneficiario'] = df['promedio_por_beneficiario'].apply(format_currency)
            print(df.to_string(index=False))
        else:
            print("No hay datos de organizaciones disponibles")
        
        # 7. Resumen ejecutivo
        print("\n7. RESUMEN EJECUTIVO")
        print("-" * 80)
        query = text("""
            SELECT 
                COUNT(DISTINCT bb.persona_id) as beneficiarios_totales,
                COUNT(*) as beneficios_totales,
                SUM(bb.valor_monetario) as inversion_total,
                AVG(bb.valor_monetario) as promedio_por_beneficio,
                COUNT(DISTINCT bb.ubicacion_id) as ubicaciones_atendidas,
                COUNT(DISTINCT CASE WHEN bs.organizacion_id IS NOT NULL THEN bs.organizacion_id 
                                   WHEN bf.organizacion_id IS NOT NULL THEN bf.organizacion_id 
                              END) as organizaciones_participantes
            FROM operational.beneficio_base bb
            LEFT JOIN operational.beneficiario_semillas bs ON bb.persona_id = bs.persona_id
            LEFT JOIN operational.beneficiario_fertilizantes bf ON bb.persona_id = bf.persona_id
            WHERE bb.tipo_beneficio IN ('SEMILLAS', 'fertilizantes');
        """)
        result = conn.execute(query)
        row = result.fetchone()
        
        print(f"Beneficiarios totales: {row[0]:,}")
        print(f"Beneficios entregados: {row[1]:,}")
        print(f"Inversión total: {format_currency(row[2])}")
        print(f"Promedio por beneficio: {format_currency(row[3])}")
        print(f"Ubicaciones atendidas: {row[4]:,}")
        print(f"Organizaciones participantes: {row[5]:,}")

if __name__ == "__main__":
    get_financial_statistics()