"""
Script de prueba para el pipeline staging de plantas de cacao.
"""
import logging
from datetime import datetime
from src.extract.plantas_excel_extractor import PlantasExcelExtractor
from src.load.plantas_stg_load import PlantasStagingLoader
from config.connections.database import db_connection
from sqlalchemy import text

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Ejecuta el pipeline staging de plantas de cacao."""
    print("=== PRUEBA PIPELINE STAGING PLANTAS DE CACAO ===")
    
    # Configuración
    excel_file = "data/raw/BASE PROYECTOS DESARROLLO PRODUCTIVO.xlsx"
    sheet_name = "PLANTAS DE CACAO"
    
    # Verificar conexión
    try:
        with db_connection.get_session() as session:
            session.execute(text("SELECT 1"))
        print("✓ Conexión exitosa a la base de datos")
    except Exception as e:
        print(f"❌ Error de conexión a base de datos: {str(e)}")
        return
    
    start_time = datetime.now()
    
    try:
        # 1. Extraer datos del Excel
        print("\n--- Extracción ---")
        extractor = PlantasExcelExtractor(excel_file, sheet_name)
        plantas_records = extractor.extract()
        
        print(f"✓ Extraídos: {len(plantas_records)} registros")
        
        # Mostrar algunos ejemplos
        if plantas_records:
            print("\nEjemplos de registros extraídos:")
            for i, record in enumerate(plantas_records[:3]):
                print(f"  {i+1}. {record.actas} - {record.nombres_completos} - {record.entrega} plantas")
        
        # 2. Cargar en staging
        print("\n--- Carga Staging ---")
        loader = PlantasStagingLoader(batch_size=100)
        load_stats = loader.load(plantas_records)
        
        # Mostrar estadísticas
        print(f"✓ Registros cargados: {load_stats['loaded_records']}")
        print(f"  - Total procesados: {load_stats['total_records']}")
        print(f"  - Exitosos: {load_stats['loaded_records']}")
        print(f"  - Fallidos: {load_stats['failed_records']}")
        print(f"  - Lotes procesados: {load_stats['batches_processed']}")
        
        # 3. Verificar datos en base de datos
        print("\n--- Verificación ---")
        final_count = loader.get_staging_count()
        print(f"Registros en staging: {final_count}")
        
        # Mostrar distribución de datos
        with db_connection.get_session() as session:
            # Top asociaciones
            assoc_query = text('''
                SELECT asociaciones, COUNT(*) as count 
                FROM "etl-productivo".stg_plantas 
                WHERE asociaciones IS NOT NULL
                GROUP BY asociaciones 
                ORDER BY count DESC 
                LIMIT 5
            ''')
            assoc_result = session.execute(assoc_query).fetchall()
            
            print("\nTop 5 Asociaciones:")
            for assoc, count in assoc_result:
                print(f"  - {assoc}: {count} registros")
            
            # Distribución por cantón
            canton_query = text('''
                SELECT canton, COUNT(*) as count 
                FROM "etl-productivo".stg_plantas 
                WHERE canton IS NOT NULL
                GROUP BY canton 
                ORDER BY count DESC
            ''')
            canton_result = session.execute(canton_query).fetchall()
            
            print("\nDistribución por Cantón:")
            for canton, count in canton_result:
                print(f"  - {canton}: {count} registros")
            
            # Estadísticas de entrega
            entrega_query = text('''
                SELECT 
                    MIN(entrega) as min_entrega,
                    MAX(entrega) as max_entrega,
                    AVG(entrega) as avg_entrega,
                    SUM(entrega) as total_plantas
                FROM "etl-productivo".stg_plantas 
                WHERE entrega IS NOT NULL
            ''')
            entrega_result = session.execute(entrega_query).fetchone()
            
            print(f"\nEstadísticas de Entrega:")
            print(f"  - Mínimo: {entrega_result.min_entrega} plantas")
            print(f"  - Máximo: {entrega_result.max_entrega} plantas")
            print(f"  - Promedio: {entrega_result.avg_entrega:.1f} plantas")
            print(f"  - Total: {entrega_result.total_plantas:,} plantas")
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        print(f"\n=== RESUMEN ===")
        print(f"✓ Pipeline staging ejecutado exitosamente")
        print(f"  - Records extraídos: {len(plantas_records)}")
        print(f"  - Records en staging: {final_count}")
        print(f"  - Tiempo total: {elapsed_time:.2f}s")
        print(f"  - Velocidad: {len(plantas_records)/elapsed_time:.2f} registros/segundo")
        
    except Exception as e:
        print(f"❌ Error en pipeline: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()