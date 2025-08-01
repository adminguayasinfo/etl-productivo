# 🐳 Configuración Docker + Kong para API Beneficios-Cultivos

## Resumen de Arquitectura

Esta aplicación está configurada para ejecutarse en un entorno Docker containerizado con Kong como API Gateway.

## 🏗️ Arquitectura del Sistema

```
[Cliente/Frontend] 
       ↓
[Kong API Gateway] 
       ↓
[FastAPI Container - Puerto 8000]
       ↓
[PostgreSQL DB - host.docker.internal]
```

## 📋 Configuración Actual

### Variables de Entorno (.env)
```bash
# Database Configuration para Docker
OLTP_DB_HOST=host.docker.internal  # ✅ Configurado para Docker
OLTP_DB_PORT=5432
OLTP_DB_NAME=etl
OLTP_DB_USER=postgres
OLTP_DB_PASSWORD=paul010.
```

### 🔧 Explicación de `host.docker.internal`

- **Propósito:** Permite que contenedores Docker accedan a servicios ejecutándose en el host
- **Uso:** La aplicación FastAPI (en contenedor) se conecta a PostgreSQL (en host o contenedor separado)
- **Alternativa:** En desarrollo local, cambiar a `localhost`

## 🚀 Despliegue en Docker

### 1. Preparación del Entorno
```bash
# Verificar que Docker esté ejecutándose
docker --version
docker-compose --version

# Verificar configuración de red Docker
docker network ls
```

### 2. Configuración de Kong (Ejemplo)
```yaml
# docker-compose.yml (ejemplo)
version: '3.8'
services:
  kong:
    image: kong:latest
    ports:
      - "8000:8000"  # Proxy
      - "8001:8001"  # Admin API
    environment:
      - KONG_DATABASE=off
      - KONG_DECLARATIVE_CONFIG=/kong/declarative/kong.yml
    volumes:
      - ./kong.yml:/kong/declarative/kong.yml
  
  etl-api:
    build: .
    ports:
      - "8080:8000"  # Puerto interno del FastAPI
    environment:
      - OLTP_DB_HOST=host.docker.internal
    depends_on:
      - kong
```

### 3. Configuración de Rutas en Kong
```yaml
# kong.yml (ejemplo)
_format_version: "2.1"
services:
  - name: etl-productivo-api
    url: http://etl-api:8000
    routes:
      - name: beneficios-cultivos
        paths:
          - /produ/beneficios-cultivos
        methods:
          - GET
```

## 🔗 Endpoints a través de Kong

### Rutas Disponibles
```bash
# A través de Kong Gateway
GET http://kong-gateway:8000/produ/beneficios-cultivos
GET http://kong-gateway:8000/produ/beneficios-cultivos?cultivo=ARROZ
GET http://kong-gateway:8000/produ/beneficios-cultivos?beneficio=SEMILLAS

# Acceso directo al contenedor (desarrollo)
GET http://localhost:8080/produ/beneficios-cultivos
```

## 🛠️ Comandos de Desarrollo

### Iniciar en Docker
```bash
# 1. Construir imagen
docker build -t etl-productivo-api .

# 2. Ejecutar contenedor
docker run -p 8080:8000 \
  -e OLTP_DB_HOST=host.docker.internal \
  -e OLTP_DB_PORT=5432 \
  -e OLTP_DB_NAME=etl \
  -e OLTP_DB_USER=postgres \
  -e OLTP_DB_PASSWORD=paul010. \
  etl-productivo-api

# 3. Verificar funcionamiento
curl http://localhost:8080/produ/beneficios-cultivos
```

### Iniciar con Docker Compose
```bash
# Iniciar todos los servicios
docker-compose up -d

# Ver logs
docker-compose logs -f etl-api

# Parar servicios
docker-compose down
```

## 🔍 Debugging y Troubleshooting

### Verificar Conectividad de BD
```bash
# Desde dentro del contenedor
docker exec -it <container-id> python -c "
from config.connections.database import DatabaseConnection
db = DatabaseConnection()
print('Host:', db.host)
print('Connected:', db.test_connection())
"
```

### Logs de Kong
```bash
# Ver logs de Kong
docker logs kong-container

# Verificar configuración
curl http://localhost:8001/services
curl http://localhost:8001/routes
```

### Health Check
```bash
# Verificar API directamente
curl http://localhost:8080/health

# Verificar a través de Kong
curl http://localhost:8000/health
```

## 📊 Monitoreo y Métricas

Kong puede configurarse para:
- **Logging:** Registrar todas las requests
- **Metrics:** Prometheus/Grafana integration
- **Rate Limiting:** Control de tráfico
- **Authentication:** JWT, OAuth, etc.

## 🚨 Consideraciones de Seguridad

1. **Variables de Entorno:** No hardcodear credenciales
2. **Network Security:** Usar redes Docker internas
3. **Kong Plugins:** Aplicar autenticación y rate limiting
4. **SSL/TLS:** Configurar certificates para HTTPS

## 📝 Notas de Producción

- **Scaling:** Kong maneja load balancing automático
- **High Availability:** Configurar múltiples instancias
- **Database:** PostgreSQL debe estar altamente disponible
- **Backups:** Implementar estrategia de respaldo de BD
- **Logging:** Centralizar logs con ELK stack o similar

## 🔄 Workflow de Desarrollo

1. **Desarrollo Local:** `OLTP_DB_HOST=localhost`
2. **Testing:** Docker con `host.docker.internal`
3. **Staging/Production:** Kong + Docker con configuración específica

---

**✅ Estado Actual:** API configurado y funcionando en entorno Docker + Kong con acceso a BD via `host.docker.internal`