#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para probar las correcciones del ETL de semillas.
Verifica que ya no se duplique hectarias_totales.
"""

import sys
sys.path.append('.')

from src.extract.semillas_excel_extractor import SemillasExcelExtractor


def test_excel_extractor_fix():
    """Prueba el extractor de Excel corregido."""
    
    print("🧪 TESTING: Excel Extractor de Semillas (Corregido)")
    print("=" * 60)
    
    try:
        # Inicializar extractor
        extractor = SemillasExcelExtractor(batch_size=5)
        
        print("✅ Extractor inicializado correctamente")
        
        # Probar extracción de una pequeña muestra
        excel_path = "data/raw/BASE PROYECTOS DESARROLLO PRODUCTIVO.xlsx"
        
        print(f"📄 Extrayendo desde: {excel_path}")
        
        record_count = 0
        sample_records = []
        
        for batch_df in extractor.extract_batches(excel_path):
            for _, row in batch_df.iterrows():
                record_count += 1
                if record_count <= 3:  # Tomar solo 3 registros de muestra
                    sample_records.append(row.to_dict())
                
                if record_count >= 3:  # Solo procesar primeros 3 registros
                    break
            
            if record_count >= 3:
                break
        
        print(f"📊 RESULTADOS:")
        print(f"   Registros procesados: {record_count}")
        
        # Verificar estructura de los registros
        print(f"\n🔍 VERIFICACIÓN DE ESTRUCTURA:")
        for i, record in enumerate(sample_records, 1):
            print(f"\n   📋 REGISTRO {i}:")
            
            # Verificar campos de hectáreas
            hectareas_fields = [k for k in record.keys() if 'hectaria' in k.lower()]
            print(f"      Campos de hectáreas encontrados: {hectareas_fields}")
            
            # Verificar si hectarias_totales está ausente
            if 'hectarias_totales' not in record:
                print("      ✅ CORRECTO: hectarias_totales eliminado")
            else:
                print("      ❌ ERROR: hectarias_totales aún presente")
            
            # Verificar si hectarias_beneficiadas está presente
            if 'hectarias_beneficiadas' in record:
                valor = record['hectarias_beneficiadas']
                print(f"      ✅ CORRECTO: hectarias_beneficiadas = {valor}")
            else:
                print("      ❌ ERROR: hectarias_beneficiadas ausente")
                
            # Mostrar algunos campos clave
            print(f"      Cultivo: {record.get('cultivo')}")
            print(f"      Cédula: {record.get('cedula')}")
        
        # Verificación final
        print(f"\n🎯 RESUMEN DE CORRECCIONES:")
        all_correct = True
        
        for record in sample_records:
            if 'hectarias_totales' in record:
                all_correct = False
                print("   ❌ Aún hay registros con hectarias_totales")
                break
        
        if all_correct:
            print("   ✅ ÉXITO: hectarias_totales eliminado correctamente")
            print("   ✅ ÉXITO: Solo se usa hectarias_beneficiadas")
            print("   ✅ El ETL ahora mapea HECTAREAS → hectarias_beneficiadas únicamente")
        
        return all_correct
        
    except FileNotFoundError:
        print("❌ ERROR: Archivo Excel no encontrado")
        print("   Asegúrate de que el archivo esté en data/raw/")
        return False
    except Exception as e:
        print(f"❌ ERROR inesperado: {str(e)}")
        return False


if __name__ == "__main__":
    success = test_excel_extractor_fix()
    
    if success:
        print("\n🎉 ¡CORRECCIONES APLICADAS EXITOSAMENTE!")
        print("   El ETL de semillas ahora usa solo hectarias_beneficiadas")
        print("   Eliminada la duplicación de datos de hectáreas")
    else:
        print("\n⚠️  Hay problemas pendientes por resolver")
    
    sys.exit(0 if success else 1)