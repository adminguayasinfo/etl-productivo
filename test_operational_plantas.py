"""
Script de prueba para el pipeline operational de plantas de cacao.
"""
import logging
from datetime import datetime
from src.pipelines.operational_refactored.plantas_operational_pipeline import PlantasOperationalPipeline
from config.connections.database import db_connection
from sqlalchemy import text

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Ejecuta el pipeline operational de plantas de cacao."""
    print("=== PRUEBA PIPELINE OPERATIONAL PLANTAS DE CACAO ===")
    
    # Verificar conexión
    try:
        with db_connection.get_session() as session:
            session.execute(text("SELECT 1"))
        print("✓ Conexión exitosa a la base de datos")
    except Exception as e:
        print(f"❌ Error de conexión a base de datos: {str(e)}")
        return
    
    # Verificar datos en staging
    with db_connection.get_session() as session:
        count_query = text('SELECT COUNT(*) FROM "etl-productivo".stg_plantas')
        result = session.execute(count_query)
        total_staging = result.scalar()
        
        pending_query = text('SELECT COUNT(*) FROM "etl-productivo".stg_plantas WHERE processed = false')
        result = session.execute(pending_query)
        pending_staging = result.scalar()
    
    print(f"Registros en staging: {total_staging}")
    print(f"Registros pendientes: {pending_staging}")
    
    if pending_staging == 0:
        print("❌ No hay registros pendientes para procesar")
        return
    
    # Inicializar y ejecutar pipeline
    pipeline = PlantasOperationalPipeline(batch_size=100)
    
    start_time = datetime.now()
    
    try:
        # Ejecutar pipeline
        pipeline_result = pipeline.run()
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        # Mostrar resultados
        print("\n=== RESULTADOS ===")
        print(f"✓ Pipeline ejecutado exitosamente")
        
        print(f"\nEstadísticas:")
        print(f"  - Procesados: {pipeline_result['total_processed']}")
        print(f"  - Errores: {pipeline_result['total_errors']}")
        print(f"  - Lotes procesados: {pipeline_result['batches_processed']}")
        print(f"  - Tasa de éxito: {pipeline_result['success_rate']:.2f}%")
        print(f"  - Tiempo: {pipeline_result['duration_seconds']:.2f}s")
        print(f"  - Velocidad: {pipeline_result['records_per_second']:.2f} registros/segundo")
        
        print(f"\nEntidades creadas:")
        for entity, count in pipeline_result['entities_created'].items():
            print(f"  - {entity.capitalize()}: {count}")
        
        # Verificar datos creados en operational
        print("\n--- Verificando datos creados ---")
        with db_connection.get_session() as session:
            tables_to_check = [
                ('direccion', 'Direcciones'),
                ('asociacion', 'Asociaciones'),
                ('tipo_cultivo', 'Tipos de cultivo'),
                ('beneficiario', 'Beneficiarios'),
                ('beneficio', 'Beneficios'),
                ('beneficio_plantas', 'Beneficios plantas')
            ]
            
            for table, name in tables_to_check:
                count_query = text(f'SELECT COUNT(*) FROM "etl-productivo".{table}')
                result = session.execute(count_query)
                count = result.scalar()
                print(f"{name}: {count} registros")
        
        # Estadísticas específicas de plantas
        print("\n--- Estadísticas de Plantas de Cacao ---")
        with db_connection.get_session() as session:
            # Distribución por cultivo
            cultivo_query = text('''
                SELECT tc.nombre, COUNT(*) as count
                FROM "etl-productivo".beneficio_plantas bp
                JOIN "etl-productivo".beneficio b ON bp.id = b.id
                JOIN "etl-productivo".tipo_cultivo tc ON b.tipo_cultivo_id = tc.id
                GROUP BY tc.nombre
                ORDER BY count DESC
            ''')
            cultivo_result = session.execute(cultivo_query).fetchall()
            
            print("Distribución por tipo de cultivo:")
            if cultivo_result:
                for cultivo, count in cultivo_result:
                    percentage = (count / pipeline_result['total_processed']) * 100 if pipeline_result['total_processed'] > 0 else 0
                    print(f"  - {cultivo}: {count} registros ({percentage:.1f}%)")
            else:
                print("  - No hay datos disponibles")
            
            # Top asociaciones
            asociacion_query = text('''
                SELECT bp.asociaciones, COUNT(*) as count
                FROM "etl-productivo".beneficio_plantas bp
                WHERE bp.asociaciones IS NOT NULL
                GROUP BY bp.asociaciones
                ORDER BY count DESC
                LIMIT 5
            ''')
            asociacion_result = session.execute(asociacion_query).fetchall()
            
            print("\nTop 5 Asociaciones:")
            if asociacion_result:
                for asociacion, count in asociacion_result:
                    percentage = (count / pipeline_result['total_processed']) * 100 if pipeline_result['total_processed'] > 0 else 0
                    print(f"  - {asociacion}: {count} registros ({percentage:.1f}%)")
            else:
                print("  - No hay datos disponibles")
            
            # Estadísticas de entrega de plantas
            entrega_query = text('''
                SELECT 
                    MIN(entrega) as min_entrega,
                    MAX(entrega) as max_entrega,
                    AVG(entrega) as avg_entrega,
                    SUM(entrega) as total_plantas
                FROM "etl-productivo".beneficio_plantas
                WHERE entrega IS NOT NULL
            ''')
            entrega_result = session.execute(entrega_query).fetchone()
            
            if entrega_result:
                print(f"\nEstadísticas de entrega de plantas:")
                print(f"  - Mínimo: {entrega_result.min_entrega} plantas")
                print(f"  - Máximo: {entrega_result.max_entrega} plantas")
                print(f"  - Promedio: {entrega_result.avg_entrega:.1f} plantas")
                print(f"  - Total plantas distribuidas: {entrega_result.total_plantas:,} plantas")
        
    except Exception as e:
        print(f"❌ Error en pipeline: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()