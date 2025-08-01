"""
Script de prueba para el pipeline operational de mecanización.
"""
import logging
from datetime import datetime
from src.pipelines.operational_refactored.mecanizacion_operational_pipeline import MecanizacionOperationalPipeline
from config.connections.database import db_connection
from sqlalchemy import text

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Ejecuta el pipeline operational de mecanización."""
    print("=== PRUEBA PIPELINE OPERATIONAL MECANIZACIÓN ===")
    
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
        count_query = text('SELECT COUNT(*) FROM "etl-productivo".stg_mecanizacion')
        result = session.execute(count_query)
        total_staging = result.scalar()
        
        pending_query = text('SELECT COUNT(*) FROM "etl-productivo".stg_mecanizacion WHERE processed = false')
        result = session.execute(pending_query)
        pending_staging = result.scalar()
    
    print(f"Registros en staging: {total_staging}")
    print(f"Registros pendientes: {pending_staging}")
    
    if pending_staging == 0:
        print("❌ No hay registros pendientes para procesar")
        return
    
    # Inicializar y ejecutar pipeline
    pipeline = MecanizacionOperationalPipeline(batch_size=100)
    
    start_time = datetime.now()
    
    try:
        # Ejecutar pipeline
        result = pipeline.run()
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        # Mostrar resultados
        print("\n=== RESULTADOS ===")
        print(f"✓ Pipeline ejecutado exitosamente")
        
        print(f"\nEstadísticas:")
        print(f"  - Procesados: {result['total_processed']}")
        print(f"  - Errores: {result['total_errors']}")
        print(f"  - Lotes procesados: {result['batches_processed']}")
        print(f"  - Tasa de éxito: {result['success_rate']:.2f}%")
        print(f"  - Tiempo: {result['duration_seconds']:.2f}s")
        print(f"  - Velocidad: {result['records_per_second']:.2f} registros/segundo")
        
        print(f"\nEntidades creadas:")
        for entity, count in result['entities_created'].items():
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
                ('beneficio_mecanizacion', 'Beneficios mecanización')
            ]
            
            for table, name in tables_to_check:
                count_query = text(f'SELECT COUNT(*) FROM "etl-productivo".{table}')
                result = session.execute(count_query)
                count = result.scalar()
                print(f"{name}: {count} registros")
        
    except Exception as e:
        print(f"❌ Error en pipeline: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()