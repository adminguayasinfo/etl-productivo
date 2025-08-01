"""
Script de prueba para el pipeline staging de mecanización.
"""
import logging
from datetime import datetime
from src.load.mecanizacion_stg_load import MecanizacionStgLoader
from config.connections.database import db_connection
from sqlalchemy import text

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Ejecuta el pipeline staging de mecanización."""
    print("=== PRUEBA PIPELINE STAGING MECANIZACIÓN ===")
    
    # Verificar conexión
    try:
        with db_connection.get_session() as session:
            session.execute(text("SELECT 1"))
        print("✓ Conexión exitosa a la base de datos")
    except Exception as e:
        print(f"❌ Error de conexión a base de datos: {str(e)}")
        return
    
    # Inicializar loader
    loader = MecanizacionStgLoader()
    
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
        
        # Análisis básico de los datos cargados
        print("\n=== ANÁLISIS DE DATOS CARGADOS ===")
        with db_connection.get_session() as session:
            # Distribución por cultivo
            cultivo_query = text("""
                SELECT cultivo, COUNT(*) as cantidad
                FROM "etl-productivo".stg_mecanizacion 
                GROUP BY cultivo
                ORDER BY cantidad DESC
            """)
            result_cultivos = session.execute(cultivo_query)
            cultivos = result_cultivos.fetchall()
            
            print("Distribución por cultivo:")
            for cultivo, cantidad in cultivos:
                cultivo_name = cultivo if cultivo else 'Sin especificar'
                print(f"  {cultivo_name}: {cantidad} registros")
            
            # Distribución por estado
            estado_query = text("""
                SELECT estado, COUNT(*) as cantidad
                FROM "etl-productivo".stg_mecanizacion 
                GROUP BY estado
                ORDER BY cantidad DESC
            """)
            result_estados = session.execute(estado_query)
            estados = result_estados.fetchall()
            
            print("\nDistribución por estado:")
            for estado, cantidad in estados:
                estado_name = estado if estado else 'Sin especificar'
                print(f"  {estado_name}: {cantidad} registros")
            
            # Top cantones
            canton_query = text("""
                SELECT canton, COUNT(*) as cantidad
                FROM "etl-productivo".stg_mecanizacion 
                WHERE canton IS NOT NULL
                GROUP BY canton
                ORDER BY cantidad DESC
                LIMIT 5
            """)
            result_cantones = session.execute(canton_query)
            cantones = result_cantones.fetchall()
            
            print("\nTop 5 cantones:")
            for canton, cantidad in cantones:
                print(f"  {canton}: {cantidad} registros")
        
    except Exception as e:
        print(f"❌ Error en pipeline: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()