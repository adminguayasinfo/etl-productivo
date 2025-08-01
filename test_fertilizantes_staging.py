"""
Script de prueba para el pipeline staging de fertilizantes.
"""
import logging
from datetime import datetime
from src.load.fertilizantes_stg_load import FertilizantesStgLoader
from config.connections.database import db_connection

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Ejecuta el pipeline staging de fertilizantes."""
    print("=== PRUEBA PIPELINE STAGING FERTILIZANTES ===")
    
    # Verificar conexión
    try:
        from sqlalchemy import text
        with db_connection.get_session() as session:
            session.execute(text("SELECT 1"))
        print("✓ Conexión exitosa a la base de datos")
    except Exception as e:
        print(f"❌ Error de conexión a base de datos: {str(e)}")
        return
    
    # Inicializar loader
    loader = FertilizantesStgLoader()
    
    # Ejecutar carga
    excel_path = "data/raw/BASE PROYECTOS DESARROLLO PRODUCTIVO.xlsx"
    batch_size = 1000
    
    print(f"Archivo Excel: {excel_path}")
    print(f"Tamaño de lote: {batch_size}")
    
    start_time = datetime.now()
    
    try:
        # Ejecutar carga
        result = loader.load_excel_to_staging(excel_path, batch_size)
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        # Mostrar resultados
        print("\n=== RESULTADOS ===")
        print(f"✓ Pipeline ejecutado exitosamente")
        print(f"Registros procesados: {result['total_processed']}")
        print(f"Errores: {result['total_errors']}")
        print(f"Tasa de éxito: {result['success_rate']:.2f}%")
        print(f"Lotes procesados: {result['batches_processed']}")
        print(f"Tiempo total: {elapsed_time:.2f} segundos")
        
        if result['total_processed'] > 0:
            print(f"Velocidad: {result['total_processed'] / elapsed_time:.2f} registros/segundo")
        
        # Verificar conteo final
        final_count = loader.get_staging_count()
        print(f"Registros en staging: {final_count}")
        
    except Exception as e:
        print(f"❌ Error en pipeline: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()