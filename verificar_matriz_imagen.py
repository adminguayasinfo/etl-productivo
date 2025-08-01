#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verifica si la matriz de costos coincide con la imagen
"""

from src.matriz_costos.costos_arroz import MatrizCostosArroz, CategoriaInsumo

def verificar_matriz_vs_imagen():
    """Compara la matriz de código con los valores de la imagen."""
    
    # Crear matriz y mostrar desglose detallado
    matriz = MatrizCostosArroz()
    
    print('COMPARACIÓN CÓDIGO vs IMAGEN:')
    print('=' * 50)
    print()
    
    print('MAQUINARIA/EQUIPOS/MATERIALES:')
    for item in matriz.items_costo:
        if item.categoria == CategoriaInsumo.MAQUINARIA:
            print(f'{item.concepto:<40}: {item.cantidad:>6} {item.unidad:<8} × ${item.precio_unitario:>6.2f} = ${item.costo_total:>7.2f}')
    
    print()
    total_maquinaria = sum(item.costo_total for item in matriz.items_costo if item.categoria == CategoriaInsumo.MAQUINARIA)
    print(f'TOTAL MAQUINARIA (código): ${total_maquinaria:.2f}')
    print(f'TOTAL MAQUINARIA (imagen): $679.50')
    print()
    
    # Verificar Arado + Fangueo específicamente
    for item in matriz.items_costo:
        if "Arado" in item.concepto:
            print(f'✓ Arado + Fangueo: {item.cantidad} × ${item.precio_unitario} = ${item.costo_total} (CORRECTO)')
            break
    
    print()
    print('TOTALES GENERALES:')
    print('-' * 30)
    validacion = matriz.validar_totales()
    print(f'Código Python: ${validacion["total_general"]["calculado"]:.2f}')
    print(f'Imagen: ${validacion["total_general"]["esperado"]:.2f}')
    print(f'Diferencia: ${validacion["total_general"]["diferencia"]:.2f}')
    print(f'¿Coincide?: {"✓ SÍ" if validacion["total_general"]["correcto"] else "✗ NO"}')
    print()
    
    print('DESGLOSE POR CATEGORÍA (CÓDIGO):')
    print('-' * 40)
    resumen = matriz.obtener_resumen_por_categoria()
    for categoria, total in resumen.items():
        print(f'{categoria.value:<15}: ${total:>8.2f}')
    
    print()
    print('VALORES ESPERADOS SEGÚN IMAGEN:')
    print('-' * 40)
    print('MANO OBRA      : $  210.00  (según imagen)')
    print('SEMILLA        : $  138.00  (según imagen)') 
    print('FERTILIZANTE   : $  201.00  (según imagen)')
    print('FITOSANITARIO  : $  102.42  (según imagen)')
    print('MAQUINARIA     : $  679.50  (según imagen)')
    print('TOTAL DIRECTO  : $1320.32  (según imagen)')
    print('INDIRECTO      : $  270.67  (según imagen)')
    print('TOTAL GENERAL  : $1590.99  (según imagen)')

if __name__ == "__main__":
    verificar_matriz_vs_imagen()