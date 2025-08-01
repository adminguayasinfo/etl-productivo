# API REST - Análisis de Costos de Arroz

API REST desarrollado con FastAPI para proporcionar análisis de costos de producción de arroz con beneficios del GAD.

## 🚀 Inicio Rápido

### 1. Instalar Dependencias
```bash
pip install -r requirements.txt
```

### 2. Configurar Variables de Entorno
```bash
export OLTP_DB_HOST=localhost
export OLTP_DB_PORT=5432
export OLTP_DB_NAME=your_db_name
export OLTP_DB_USER=your_user
export OLTP_DB_PASSWORD=your_password
```

### 3. Iniciar el Servidor
```bash
python start_api.py
```

El servidor estará disponible en: http://localhost:8000

## 📚 Endpoints Disponibles

### Endpoint Principal
**GET** `/prod/analisis-costos-arroz`

Retorna el análisis completo de costos de producción de arroz incluyendo:
- Datos reales de beneficios (semillas, fertilizantes, mecanización)
- Cálculo de ahorros por tipo de beneficio
- Eficiencia de la inversión del GAD
- Indicadores financieros por hectárea
- Datos estructurados para gráficos

### Endpoints Adicionales
- **GET** `/` - Información general del API
- **GET** `/health` - Health check del servidor
- **GET** `/prod/resumen` - Solo resumen ejecutivo
- **GET** `/prod/graficos` - Solo datos para gráficos

### Documentación Interactiva
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## 📊 Estructura de Respuesta

### Respuesta Principal (`/prod/analisis-costos-arroz`)

```json
{
  "fecha_analisis": "2024-01-15T10:30:00",
  "total_hectareas_arroz": 37640.61,
  "beneficios": {
    "semillas": {
      "beneficiarios": 10772,
      "hectareas": 37640.61,
      "inversion_gad": 2034851.69,
      "precio_promedio_gad": 69.67,
      "descripcion": "Entrega de semillas certificadas de arroz"
    },
    "fertilizantes": { ... },
    "mecanizacion": { ... }
  },
  "precios": {
    "semillas": {
      "precio_gad": 69.67,
      "precio_mercado": 83.60,
      "diferencia": 13.93,
      "unidad": "por quintal"
    },
    ...
  },
  "ahorros": {
    "semillas": {
      "monto": 406970.34,
      "porcentaje_del_total": 46.9,
      "descripcion": "29,207 quintales × diferencia de precio"
    },
    ...
  },
  "ahorro_total": 868177.84,
  "eficiencia": {
    "inversion_total": 4383951.69,
    "ahorro_total": 868177.84,
    "eficiencia": 0.20,
    "calificacion": "DEFICIENTE",
    "descripcion": "Por cada $1 invertido se generan $0.20 en ahorro"
  },
  "indicadores_financieros": {
    "rendimiento_sacas": 60,
    "precio_saca": 35.50,
    "ingresos_brutos": 2130.00,
    "costo_produccion": 1590.99,
    "utilidad_sin_programa": 539.01,
    "utilidad_con_programa": 562.08,
    "mejora_utilidad_porcentaje": 4.3,
    "ahorro_promedio_ha": 23.06
  },
  "resumen_ejecutivo": {
    "inversion_gad_total": 4383951.69,
    "ahorro_productores_total": 868177.84,
    "eficiencia_completa": 0.20,
    "beneficiarios_directos": 22049,
    "hectareas_impactadas": 37640.61,
    "mejora_utilidad_promedio": 4.3,
    "calificacion": "DEFICIENTE"
  },
  "graficos": {
    "inversion_por_beneficio": [
      {
        "nombre": "Semillas",
        "valor": 2034851.69,
        "color": "#2563eb"
      },
      ...
    ],
    "ahorro_por_beneficio": [...],
    "cobertura_hectareas": [...],
    "comparacion_precios": [...]
  },
  "metodologia": [
    "Datos reales de inversión y entregas extraídos de la base de datos",
    "Mecanización valorada según matriz AGRIPAC ($200/ha para Arado + Fangueo)",
    ...
  ]
}
```

## 🔧 Configuración

### Variables de Entorno Requeridas
- `OLTP_DB_HOST`: Host de la base de datos
- `OLTP_DB_PORT`: Puerto de la base de datos  
- `OLTP_DB_NAME`: Nombre de la base de datos
- `OLTP_DB_USER`: Usuario de la base de datos
- `OLTP_DB_PASSWORD`: Contraseña de la base de datos

### Configuración del Servidor
- **Puerto**: 8000 (configurable en `start_api.py`)
- **Host**: 0.0.0.0 (accesible desde cualquier IP)
- **Recarga automática**: Habilitada en desarrollo
- **CORS**: Deshabilitado (configurable en `api/main.py`)

## 🏗️ Arquitectura

```
api/
├── __init__.py          # Inicialización del módulo
├── main.py              # Aplicación FastAPI principal
├── models.py            # Modelos Pydantic para request/response
└── services.py          # Lógica de negocio y servicios
```

### Servicios
- **AnalisisCostosService**: Servicio principal que realiza el análisis completo
- **HealthService**: Servicio para health checks

### Modelos
- **AnalisisCostosResponse**: Modelo completo de respuesta
- **DatosBeneficio**: Datos de un tipo de beneficio específico
- **PreciosComparacion**: Comparación GAD vs mercado
- **IndicadoresFinancieros**: Métricas financieras por hectárea
- Y más...

## 🔍 Monitoreo y Logs

El API incluye logging detallado que registra:
- Inicio y parada del servidor
- Errores de conexión a base de datos
- Tiempo de respuesta de endpoints
- Errores de procesamiento

Los logs se muestran en consola durante desarrollo.

## 🚦 Health Check

**GET** `/health`

Retorna:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00",
  "database_connection": true,
  "message": "API funcionando correctamente"
}
```

## 🔄 Actualización de Datos

El API consulta la base de datos en **tiempo real** en cada request. No hay cache, por lo que siempre retorna los datos más actuales.

## 🛠️ Desarrollo

### Ejecutar en Modo Desarrollo
```bash
python start_api.py
```

### Ejecutar con Uvicorn Directamente
```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

### Testing
```bash
# Verificar que el servidor esté funcionando
curl http://localhost:8000/health

# Obtener análisis completo
curl http://localhost:8000/prod/analisis-costos-arroz
```

## 📝 Notas

- El análisis se basa en datos reales extraídos de la base de datos
- La mecanización se valora según la matriz AGRIPAC ($200/ha)
- Los precios de mercado son estimaciones (+15-20% sobre precios GAD)
- No incluye beneficios indirectos no monetizables