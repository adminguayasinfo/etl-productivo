# API Beneficios-Cultivos - Guía de Uso

## Resumen de la Implementación

Se ha implementado exitosamente el endpoint `/produ/beneficios-cultivos` que proporciona toda la información solicitada sobre subsidios agrícolas en Ecuador.

## Endpoint Principal

**URL:** `GET /produ/beneficios-cultivos`

**Parámetros de consulta opcionales:**
- `cultivo`: Filtrar por tipo de cultivo (`ARROZ`, `MAIZ`)
- `beneficio`: Filtrar por tipo de subvención (`SEMILLAS`, `FERTILIZANTES`, `MECANIZACION`)

## Información Proporcionada

### 1. Hectáreas Subsidiadas por Cultivo y Beneficio
```json
{
  "hectareas_subsidiadas": [
    {
      "cultivo": "ARROZ",
      "tipo_beneficio": "FERTILIZANTES", 
      "total_hectareas": 43315.55,
      "num_beneficios": 11000
    },
    {
      "cultivo": "ARROZ",
      "tipo_beneficio": "SEMILLAS",
      "total_hectareas": 37640.61, 
      "num_beneficios": 10772
    },
    {
      "cultivo": "MAIZ",
      "tipo_beneficio": "SEMILLAS",
      "total_hectareas": 11309.00,
      "num_beneficios": 4228
    }
  ]
}
```

### 2. Costos de Producción (Sin Subsidios)
```json
{
  "costos_produccion": [
    {
      "cultivo": "ARROZ",
      "costo_total_sin_subsidio": 1590.99,
      "costos_directos": 1320.32,
      "costos_indirectos": 270.67,
      "desglose_por_categoria": {
        "MANO_OBRA": 210.00,
        "SEMILLA": 138.00,
        "FERTILIZANTE": 201.00,
        "FITOSANITARIO": 438.32,
        "MAQUINARIA": 333.00,
        "ADMINISTRATIVO": 270.67
      }
    },
    {
      "cultivo": "MAIZ", 
      "costo_total_sin_subsidio": 1835.30,
      "costos_directos": 1523.07,
      "costos_indirectos": 312.23
    }
  ]
}
```

### 3. Montos Totales de Subsidios Entregados
```json
{
  "montos_subsidios": [
    {
      "cultivo": "ARROZ",
      "tipo_beneficio": "FERTILIZANTES",
      "monto_total_entregado": 2221050.00,
      "monto_matriz_por_hectarea": 201.00,
      "num_beneficios": 11000
    },
    {
      "cultivo": "ARROZ", 
      "tipo_beneficio": "SEMILLAS",
      "monto_total_entregado": 750485.24,
      "monto_matriz_por_hectarea": 138.00,
      "num_beneficios": 10772
    },
    {
      "cultivo": "MAIZ",
      "tipo_beneficio": "SEMILLAS", 
      "monto_total_entregado": 961093.28,
      "monto_matriz_por_hectarea": 235.00,
      "num_beneficios": 4228
    }
  ]
}
```

### 4. Reducción Total en Costos de Producción
```json
{
  "reduccion_costos": [
    {
      "cultivo": "ARROZ",
      "costo_produccion_sin_subsidio": 129818868.35,
      "reduccion_por_subsidios": 2971535.24,
      "costo_produccion_con_subsidio": 126847333.11,
      "porcentaje_reduccion": 2.29,
      "desglose_reducciones": {
        "FERTILIZANTES": 2221050.00,
        "SEMILLAS": 750485.24
      }
    },
    {
      "cultivo": "MAIZ",
      "costo_produccion_sin_subsidio": 20755407.70,
      "reduccion_por_subsidios": 961093.28,
      "costo_produccion_con_subsidio": 19794314.42,
      "porcentaje_reduccion": 4.63,
      "desglose_reducciones": {
        "SEMILLAS": 961093.28
      }
    }
  ]
}
```

### 5. Filtros Disponibles
```json
{
  "filtros": {
    "cultivos_disponibles": ["ARROZ", "MAIZ"],
    "beneficios_disponibles": ["FERTILIZANTES", "MECANIZACION", "SEMILLAS"],
    "combinaciones_disponibles": [
      {"cultivo": "ARROZ", "beneficio": "FERTILIZANTES"},
      {"cultivo": "ARROZ", "beneficio": "MECANIZACION"},
      {"cultivo": "ARROZ", "beneficio": "SEMILLAS"},
      {"cultivo": "MAIZ", "beneficio": "SEMILLAS"}
    ]
  }
}
```

### 6. Resumen Ejecutivo
```json
{
  "resumen": {
    "total_hectareas_impactadas": 92905.41,
    "total_beneficios_otorgados": 26277,
    "inversion_total_gad": 3932628.52,
    "ahorro_total_productores": 3932628.52,
    "cultivos_mas_subsidiados": [
      {
        "cultivo": "ARROZ",
        "hectareas": 81596.41,
        "beneficios": 22049
      },
      {
        "cultivo": "MAIZ", 
        "hectareas": 11309.00,
        "beneficios": 4228
      }
    ],
    "beneficios_mas_utilizados": [
      {
        "beneficio": "FERTILIZANTES",
        "hectareas": 43315.55,
        "beneficios": 11000
      },
      {
        "beneficio": "SEMILLAS",
        "hectareas": 48949.61, 
        "beneficios": 15000
      }
    ]
  }
}
```

## Ejemplos de Uso

### 1. Obtener todos los datos
```bash
GET /produ/beneficios-cultivos
```

### 2. Filtrar solo datos de ARROZ
```bash
GET /produ/beneficios-cultivos?cultivo=ARROZ
```

### 3. Filtrar solo subsidios de SEMILLAS
```bash
GET /produ/beneficios-cultivos?beneficio=SEMILLAS
```

### 4. Filtrar ARROZ con FERTILIZANTES
```bash
GET /produ/beneficios-cultivos?cultivo=ARROZ&beneficio=FERTILIZANTES
```

## Iniciar el Servidor

### 🐳 **Entorno Docker + Kong (Producción)**
Esta aplicación está configurada para ejecutarse en un entorno Docker con Kong como API Gateway:

```bash
# 1. Verificar configuración .env
# OLTP_DB_HOST=host.docker.internal (para Docker)
# Otros parámetros de BD según el entorno

# 2. Activar entorno virtual
source venv/bin/activate

# 3. Iniciar servidor API
python start_api.py

# 4. El API estará disponible a través de Kong Gateway
# - Rutas configuradas en Kong para proxy del tráfico
# - Base de datos accesible via host.docker.internal
```

### 💻 **Desarrollo Local (Opcional)**
Para desarrollo local sin Docker:

```bash
# 1. Modificar .env temporalmente:
# OLTP_DB_HOST=localhost

# 2. Activar entorno virtual  
source venv/bin/activate

# 3. Iniciar servidor API
python start_api.py

# 4. Acceder directamente:
# - API: http://localhost:8000/produ/beneficios-cultivos
# - Documentación: http://localhost:8000/docs
```

## Datos Clave Obtenidos

- **Total hectáreas impactadas:** 92,905.41 ha
- **Total beneficios otorgados:** 26,277
- **Inversión total GAD:** $3,932,628.52
- **Cultivos:** ARROZ (81,596 ha) y MAÍZ (11,309 ha)
- **Beneficios:** Semillas, Fertilizantes, Mecanización
- **Reducción promedio costos:** 2.29% (Arroz), 4.63% (Maíz)

## Notas Técnicas

1. **MANO_OBRA** no se mapea con ningún beneficio-subvención (según especificación)
2. **Mecanización** usa solo el item específico de arado por cultivo:
   - ARROZ: "Arado + Fangueo" ($200.00/ha)
   - MAÍZ: "Arado + Rastra" ($70.00/ha)
3. **Reducción de costos** se calcula restando directamente los montos de subsidios (no recálculo dinámico de indirectos)
4. Los datos provienen de la base de datos real del sistema ETL-Productivo
5. Las matrices de costos están validadas contra los archivos Excel oficiales del Ministerio de Agricultura

## Arquitectura de Despliegue

### 🐳 **Entorno Docker + Kong**
La aplicación está diseñada para ejecutarse en un entorno containerizado:

- **API Gateway:** Kong maneja el enrutamiento y proxy de requests
- **Base de Datos:** PostgreSQL accesible via `host.docker.internal`
- **Configuración:** Variables de entorno desde `.env` adaptadas para Docker
- **Networking:** Comunicación entre contenedores a través de red Docker

### 🔧 **Configuración de Red**
```bash
# Para Docker (Producción)
OLTP_DB_HOST=host.docker.internal

# Para desarrollo local
OLTP_DB_HOST=localhost
```

### 📡 **Acceso a través de Kong**
El endpoint `/produ/beneficios-cultivos` es accesible a través de las rutas configuradas en Kong Gateway, que maneja:
- Load balancing
- Rate limiting  
- Authentication/Authorization
- Request/Response transformation
- Logging y monitoreo