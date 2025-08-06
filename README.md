# etl-productivo


# Eliminar schema
  python scripts/recreate_schemas.py

  2. INICIALIZAR BASE DE DATOS

  # Crear todas las tablas y estructuras
  python init_database.py

  3. CREAR DIRECTORIO DE LOGS

  # Crear directorio para logs del ETL
  mkdir -p logs


  # Semillas (staging)
  python scripts/load_semillas_staging.py

  # Semillas (operational)
  python scripts/run_semillas.py --batch-size 1000

  # Fertilizantes (staging)
  python scripts/load_fertilizantes_staging.py

  # Fertilizantes (operational)
  python scripts/run_fertilizantes.py --batch-size 1000
  
  # Mecanizacion (staging)
  

  # Mecanizacion (operational)

