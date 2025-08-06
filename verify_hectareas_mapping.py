#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar que los datos de HECTAREAS del Excel 
coinciden con hectarias_beneficiadas en stg_semilla de la BD.
"""

import pandas as pd
import sys
sys.path.append('.')

from config.connections.database import DatabaseConnection


def verify_hectareas_mapping():
    """Verifica el mapeo correcto entre Excel y BD."""
    
    print("🔍 VERIFICACIÓN: Excel HECTAREAS vs BD hectarias_beneficiadas")
    print("=" * 70)
    
    try:
        # 1. Leer datos del Excel
        excel_path = "data/raw/BASE PROYECTOS DESARROLLO PRODUCTIVO.xlsx"
        print(f"📄 Leyendo Excel: {excel_path}")
        
        df_excel = pd.read_excel(excel_path, sheet_name='SEMILLAS')
        print(f"   Total registros Excel: {len(df_excel):,}")
        
        # Verificar que existe columna HECTAREAS
        if 'HECTAREAS' not in df_excel.columns:
            print("❌ ERROR: Columna HECTAREAS no encontrada en Excel")
            return False
        
        # 2. Leer datos de la BD
        print(f"\n💾 Conectando a BD...")
        db = DatabaseConnection()
        db.init_engine()
        
        # Query para obtener datos de staging
        staging_query = '''
        SELECT 
            id,
            hectarias_beneficiadas,
            cedula,
            nombres_apellidos,
            cultivo
        FROM "etl-productivo".stg_semilla
        ORDER BY id;
        '''
        
        staging_data = db.execute_query(staging_query)
        print(f"   Total registros BD: {len(staging_data):,}")
        
        # 3. Convertir datos BD a DataFrame para comparación
        bd_df = pd.DataFrame([{
            'id': row.id,
            'hectarias_beneficiadas': float(row.hectarias_beneficiadas) if row.hectarias_beneficiadas else None,
            'cedula': row.cedula,
            'nombres_apellidos': row.nombres_apellidos,
            'cultivo': row.cultivo
        } for row in staging_data])
        
        # 4. Preparar Excel para comparación (mismo orden)
        excel_df = df_excel.copy()
        excel_df['hectareas_excel'] = excel_df['HECTAREAS'].astype(float)
        
        print(f"\n📊 ESTADÍSTICAS COMPARATIVAS:")
        
        # Estadísticas básicas Excel
        excel_hectareas = excel_df['hectareas_excel'].dropna()
        print(f"   Excel HECTAREAS:")
        print(f"     - Total registros: {len(excel_hectareas):,}")
        print(f"     - Suma total: {excel_hectareas.sum():,.2f}")
        print(f"     - Promedio: {excel_hectareas.mean():.2f}")
        print(f"     - Min: {excel_hectareas.min()}, Max: {excel_hectareas.max()}")
        
        # Estadísticas básicas BD
        bd_hectareas = bd_df['hectarias_beneficiadas'].dropna()
        print(f"   BD hectarias_beneficiadas:")
        print(f"     - Total registros: {len(bd_hectareas):,}")
        print(f"     - Suma total: {bd_hectareas.sum():,.2f}")
        print(f"     - Promedio: {bd_hectareas.mean():.2f}")
        print(f"     - Min: {bd_hectareas.min()}, Max: {bd_hectareas.max()}")
        
        # 5. Verificación detallada por muestra
        print(f"\n🔍 VERIFICACIÓN DETALLADA (primeros 10 registros):")
        print(f"{'Index':<5} {'Excel':<8} {'BD':<8} {'Coincide':<8} {'Cédula':<12} {'Cultivo':<10}")
        print("-" * 60)
        
        coincidencias = 0
        total_comparados = 0
        
        # Comparar los primeros registros (asumiendo mismo orden)
        max_compare = min(len(excel_df), len(bd_df), 10)
        
        for i in range(max_compare):
            excel_val = excel_df.iloc[i]['hectareas_excel']
            bd_val = bd_df.iloc[i]['hectarias_beneficiadas']
            cedula_bd = bd_df.iloc[i]['cedula'] or 'N/A'
            cultivo_bd = bd_df.iloc[i]['cultivo'] or 'N/A'
            
            coincide = "✅ SÍ" if excel_val == bd_val else "❌ NO"
            if excel_val == bd_val:
                coincidencias += 1
            total_comparados += 1
            
            print(f"{i+1:<5} {excel_val:<8.1f} {bd_val or 0:<8.1f} {coincide:<8} {cedula_bd[:10]:<12} {cultivo_bd[:8]:<10}")
        
        # 6. Verificación estadística completa
        print(f"\n📈 VERIFICACIÓN ESTADÍSTICA:")
        
        # Comparar sumas totales
        diferencia_suma = abs(excel_hectareas.sum() - bd_hectareas.sum())
        print(f"   Diferencia en suma total: {diferencia_suma:.2f}")
        
        if diferencia_suma < 0.01:
            print("   ✅ CORRECTO: Sumas totales coinciden")
            suma_coincide = True
        else:
            print("   ❌ ERROR: Sumas totales no coinciden")
            suma_coincide = False
        
        # Comparar promedios
        diferencia_promedio = abs(excel_hectareas.mean() - bd_hectareas.mean())
        print(f"   Diferencia en promedio: {diferencia_promedio:.4f}")
        
        if diferencia_promedio < 0.01:
            print("   ✅ CORRECTO: Promedios coinciden")
            promedio_coincide = True
        else:
            print("   ❌ ERROR: Promedios no coinciden")
            promedio_coincide = False
        
        # Comparar cantidad de registros
        if len(excel_hectareas) == len(bd_hectareas):
            print("   ✅ CORRECTO: Mismo número de registros")
            count_coincide = True
        else:
            print(f"   ⚠️  DIFERENCIA: Excel {len(excel_hectareas):,} vs BD {len(bd_hectareas):,} registros")
            count_coincide = False
        
        # 7. Resultado final
        print(f"\n🎯 RESULTADO FINAL:")
        print(f"   Coincidencias en muestra: {coincidencias}/{total_comparados}")
        print(f"   Porcentaje de coincidencia: {(coincidencias/total_comparados)*100:.1f}%")
        
        # Determinar si el mapeo es correcto
        mapeo_correcto = suma_coincide and promedio_coincide and count_coincide and (coincidencias == total_comparados)
        
        if mapeo_correcto:
            print("   ✅ MAPEO CORRECTO: Los datos del Excel se mapearon correctamente a la BD")
            print("   ✅ Columna M 'HECTAREAS' → hectarias_beneficiadas ✓")
        else:
            print("   ❌ MAPEO INCORRECTO: Hay diferencias entre Excel y BD")
            if not count_coincide:
                print("   🔍 Posible causa: Registros filtrados o faltantes en ETL")
            if not suma_coincide or not promedio_coincide:
                print("   🔍 Posible causa: Transformación de datos incorrecta")
        
        return mapeo_correcto
        
    except FileNotFoundError:
        print("❌ ERROR: Archivo Excel no encontrado")
        return False
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return False


if __name__ == "__main__":
    success = verify_hectareas_mapping()
    print(f"\n{'='*70}")
    
    if success:
        print("🎉 VERIFICACIÓN EXITOSA: El mapeo Excel → BD es correcto")
    else:
        print("⚠️  VERIFICACIÓN FALLÓ: Revisar el proceso ETL")
    
    sys.exit(0 if success else 1)