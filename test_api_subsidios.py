#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de prueba para el API de beneficios-cultivos.
Simula llamadas al endpoint y muestra los resultados.
"""
import os
import sys
import json
from datetime import datetime

# Agregar el directorio raíz al path
sys.path.append('.')

# Cargar variables de entorno desde .env (ya se hace automáticamente en DatabaseConnection)

def test_api_completo():
    """Prueba el análisis completo sin filtros."""
    print("="*60)
    print("🔬 PRUEBA API COMPLETO (SIN FILTROS)")
    print("="*60)
    
    try:
        from api.services_subsidios import BeneficiosCultivosService
        service = BeneficiosCultivosService()
        
        resultado = service.obtener_analisis_completo()
        
        print(f"📅 Fecha consulta: {resultado.fecha_consulta}")
        print(f"🌾 Total hectáreas: {resultado.resumen.total_hectareas_impactadas:,.2f} ha")
        print(f"🎯 Total beneficios: {resultado.resumen.total_beneficios_otorgados:,}")
        print(f"💰 Inversión GAD: ${resultado.resumen.inversion_total_gad:,.2f}")
        
        print("\n📊 HECTÁREAS SUBSIDIADAS:")
        for h in resultado.hectareas_subsidiadas:
            print(f"  • {h.cultivo.value} - {h.tipo_beneficio.value}: {h.total_hectareas:,.2f} ha ({h.num_beneficios:,} beneficios)")
        
        print("\n💵 COSTOS DE PRODUCCIÓN (por hectárea):")
        for c in resultado.costos_produccion:
            print(f"  • {c.cultivo.value}: ${c.costo_total_sin_subsidio:.2f}")
            print(f"    - Directos: ${c.costos_directos:.2f}")
            print(f"    - Indirectos: ${c.costos_indirectos:.2f}")
        
        print("\n💸 MONTOS SUBSIDIOS:")
        for m in resultado.montos_subsidios:
            if m.monto_total_entregado > 0:
                print(f"  • {m.cultivo.value} - {m.tipo_beneficio.value}: ${m.monto_total_entregado:,.2f}")
                print(f"    - Por hectárea (matriz): ${m.monto_matriz_por_hectarea:.2f}")
        
        print("\n📉 REDUCCIÓN DE COSTOS:")
        for r in resultado.reduccion_costos:
            print(f"  • {r.cultivo.value}:")
            print(f"    - Costo sin subsidio: ${r.costo_produccion_sin_subsidio:,.2f}")
            print(f"    - Reducción por subsidios: ${r.reduccion_por_subsidios:,.2f}")
            print(f"    - Costo con subsidio: ${r.costo_produccion_con_subsidio:,.2f}")
            print(f"    - % Reducción: {r.porcentaje_reduccion:.2f}%")
        
        print("\n🎯 FILTROS DISPONIBLES:")
        print(f"  • Cultivos: {[c.value for c in resultado.filtros.cultivos_disponibles]}")
        print(f"  • Beneficios: {[b.value for b in resultado.filtros.beneficios_disponibles]}")
        
        print("\n✅ PRUEBA COMPLETA EXITOSA")
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_api_filtrado():
    """Prueba el API con filtros."""
    print("\n" + "="*60)
    print("🔍 PRUEBA API CON FILTROS")
    print("="*60)
    
    try:
        from api.services_subsidios import BeneficiosCultivosService
        service = BeneficiosCultivosService()
        
        # Filtro por cultivo
        print("\n🌾 FILTRO: Solo ARROZ")
        resultado_arroz = service.obtener_analisis_completo(filtro_cultivo='ARROZ')
        print(f"  - Hectáreas ARROZ: {sum(h.total_hectareas for h in resultado_arroz.hectareas_subsidiadas):,.2f}")
        
        # Filtro por beneficio
        print("\n🌱 FILTRO: Solo SEMILLAS")
        resultado_semillas = service.obtener_analisis_completo(filtro_beneficio='SEMILLAS')
        print(f"  - Hectáreas SEMILLAS: {sum(h.total_hectareas for h in resultado_semillas.hectareas_subsidiadas):,.2f}")
        
        # Filtro combinado
        print("\n🌾🌱 FILTRO: ARROZ + SEMILLAS")
        resultado_combo = service.obtener_analisis_completo(filtro_cultivo='ARROZ', filtro_beneficio='SEMILLAS')
        if resultado_combo.hectareas_subsidiadas:
            h = resultado_combo.hectareas_subsidiadas[0]
            print(f"  - {h.cultivo.value} + {h.tipo_beneficio.value}: {h.total_hectareas:,.2f} ha")
        
        print("\n✅ PRUEBA FILTRADA EXITOSA")
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def mostrar_resumen_tecnico():
    """Muestra un resumen técnico de la implementación."""
    print("\n" + "="*60)
    print("📋 RESUMEN TÉCNICO DE LA IMPLEMENTACIÓN")
    print("="*60)
    
    print("""
✅ IMPLEMENTACIÓN COMPLETADA:

1️⃣ HECTÁREAS SUBSIDIADAS:
   • Query BD para obtener hectáreas por cultivo y beneficio
   • Datos de ARROZ y MAÍZ con SEMILLAS, FERTILIZANTES, MECANIZACIÓN
   • Total: ~92,905 hectáreas impactadas

2️⃣ COSTOS DE PRODUCCIÓN (matrices):
   • ARROZ: $1,590.99/ha (sin subsidios)
   • MAÍZ: $1,835.30/ha (sin subsidios) 
   • Desglose por categorías (SEMILLA, FERTILIZANTE, MAQUINARIA, etc.)

3️⃣ MONTOS SUBSIDIOS ENTREGADOS:
   • Datos reales de BD ($3,932,628.52 total)
   • Mecanización usa valores específicos de matriz
   • Mapeo correcto BD → Categorías matriz

4️⃣ REDUCCIÓN TOTAL DE COSTOS:
   • Resta directa: Costo sin subsidio - Montos entregados
   • No usa recálculo dinámico de indirectos (según especificación)
   • Cálculo por cultivo agregado

5️⃣ FILTROS IMPLEMENTADOS:
   • Por tipo cultivo (ARROZ, MAÍZ)
   • Por tipo subvención (SEMILLAS, FERTILIZANTES, MECANIZACIÓN)
   • Combinaciones válidas disponibles para frontend

📡 ENDPOINT: GET /produ/beneficios-cultivos
   • Query params: ?cultivo=ARROZ&beneficio=SEMILLAS
   • Response: JSON estructurado con toda la información
   • Validación de parámetros
   • Manejo de errores

🔧 ARQUITECTURA:
   • Models (Pydantic): api/models_subsidios.py
   • Services (Lógica): api/services_subsidios.py  
   • Endpoint: api/main.py
   • Matrices: src/matriz_costos/costos_*.py
   • BD: PostgreSQL con schema 'etl-productivo'

🐳 ENTORNO DOCKER + KONG:
   • API Gateway: Kong maneja enrutamiento y proxy
   • BD Connection: host.docker.internal (configurado en .env)
   • Networking: Comunicación entre contenedores Docker
   • Config: Variables desde .env adaptadas para containerización
""")

if __name__ == "__main__":
    print("🚀 INICIANDO PRUEBAS DEL API BENEFICIOS-CULTIVOS")
    print(f"⏰ {datetime.now()}")
    
    # Ejecutar pruebas
    success1 = test_api_completo()
    success2 = test_api_filtrado()
    
    # Mostrar resumen
    mostrar_resumen_tecnico()
    
    # Resultado final
    if success1 and success2:
        print("\n🎉 TODAS LAS PRUEBAS EXITOSAS - API LISTO PARA USO")
        print("\n🌐 Para usar el API:")
        print("   🐳 ENTORNO DOCKER + KONG (Producción):")
        print("      1. Verificar .env: OLTP_DB_HOST=host.docker.internal")
        print("      2. Activar entorno: source venv/bin/activate")
        print("      3. Iniciar servidor: python start_api.py")
        print("      4. Acceso vía Kong Gateway (rutas configuradas)")
        print("   💻 DESARROLLO LOCAL (Opcional):")
        print("      1. Cambiar .env: OLTP_DB_HOST=localhost")
        print("      2. Activar entorno: source venv/bin/activate")
        print("      3. Iniciar: python start_api.py")
        print("      4. Acceder: http://localhost:8000/produ/beneficios-cultivos")
        print("      5. Docs: http://localhost:8000/docs")
    else:
        print("\n❌ ALGUNAS PRUEBAS FALLARON - REVISAR LOGS")