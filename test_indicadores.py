#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de prueba para el endpoint /productivo-indicadores.
Verifica el cálculo del porcentaje promedio ponderado de reducción de costos.
"""

import requests
import json
from datetime import datetime

def test_indicadores_endpoint():
    """Prueba el endpoint de indicadores productivos."""
    
    print("🧪 TESTING: Endpoint /productivo-indicadores")
    print("=" * 60)
    
    try:
        # Realizar request al endpoint
        print("📡 Realizando request a http://localhost:8000/productivo-indicadores...")
        response = requests.get("http://localhost:8000/productivo-indicadores")
        
        if response.status_code == 200:
            print("✅ Request exitoso!")
            
            # Parsear respuesta JSON
            data = response.json()
            
            # Mostrar información general
            print(f"\n📊 RESULTADOS - {data['fecha_calculo']}")
            print("=" * 60)
            
            # Indicador principal
            indicador = data['indicador_reduccion_costos']
            print(f"🎯 INDICADOR PRINCIPAL")
            print(f"   Porcentaje promedio reducción: {indicador['porcentaje_promedio_reduccion']:.2f}%")
            print(f"   Metodología: {indicador['metodologia']}")
            
            # Estadísticas de base
            print(f"\n📈 ESTADÍSTICAS BASE")
            print(f"   Total beneficiarios: {indicador['total_beneficiarios']:,}")
            print(f"   Hectáreas totales: {indicador['hectareas_totales']:,.2f}")
            print(f"   Monto total beneficios: ${indicador['monto_total_beneficios']:,.2f}")
            print(f"   Costo total sin subsidios: ${indicador['costo_total_sin_subsidios']:,.2f}")
            
            # Cálculos derivados
            if indicador['costo_total_sin_subsidios'] > 0:
                porcentaje_simple = (indicador['monto_total_beneficios'] / indicador['costo_total_sin_subsidios']) * 100
                print(f"\n🧮 VERIFICACIONES")
                print(f"   % Reducción simple (sin ponderación): {porcentaje_simple:.2f}%")
                print(f"   % Reducción ponderado (calculado): {indicador['porcentaje_promedio_reduccion']:.2f}%")
                print(f"   Diferencia ponderación: {indicador['porcentaje_promedio_reduccion'] - porcentaje_simple:.2f} puntos")
            
            # Observaciones
            if 'observaciones' in data and data['observaciones']:
                print(f"\n💡 OBSERVACIONES")
                print(f"   {data['observaciones']}")
            
            # Análisis de la efectividad
            print(f"\n🎯 ANÁLISIS DE EFECTIVIDAD")
            if indicador['porcentaje_promedio_reduccion'] > 20:
                print("   🟢 EXCELENTE impacto - Reducción > 20%")
            elif indicador['porcentaje_promedio_reduccion'] > 10:
                print("   🟡 BUENO impacto - Reducción entre 10-20%")
            elif indicador['porcentaje_promedio_reduccion'] > 5:
                print("   🟠 MODERADO impacto - Reducción entre 5-10%")
            else:
                print("   🔴 BAJO impacto - Reducción < 5%")
            
            print(f"\n✅ Endpoint funcionando correctamente!")
            
        else:
            print(f"❌ Error en request: {response.status_code}")
            print(f"   Respuesta: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Error: No se pudo conectar al servidor.")
        print("   Asegúrate de que el API esté ejecutándose en http://localhost:8000")
    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")


if __name__ == "__main__":
    test_indicadores_endpoint()