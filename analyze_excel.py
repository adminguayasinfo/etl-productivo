#!/usr/bin/env python3
"""Script para analizar la estructura del archivo Excel."""

import pandas as pd
import sys
import os

# Agregar el directorio raíz al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def analyze_excel_sheet(file_path: str, sheet_name: str):
    """Analiza una pestaña del archivo Excel."""
    print(f"=== ANALIZANDO PESTAÑA: {sheet_name} ===\n")
    
    try:
        # Leer solo los headers (primera fila)
        df_headers = pd.read_excel(file_path, sheet_name=sheet_name, nrows=0)
        print("HEADERS ENCONTRADOS:")
        for i, col in enumerate(df_headers.columns):
            print(f"{i+1:2d}. {col}")
        
        # Leer las primeras 5 filas de datos para entender la estructura
        df_sample = pd.read_excel(file_path, sheet_name=sheet_name, nrows=5)
        print(f"\nTOTAL DE COLUMNAS: {len(df_sample.columns)}")
        print(f"PRIMERAS 5 FILAS DE MUESTRA:")
        print(df_sample.head())
        
        # Información de tipos de datos
        print(f"\nTIPOS DE DATOS:")
        for col in df_sample.columns:
            dtype = df_sample[col].dtype
            non_null = df_sample[col].notna().sum()
            print(f"{col:30s} | {str(dtype):15s} | Non-null: {non_null}/5")
        
        # Contar total de filas
        df_count = pd.read_excel(file_path, sheet_name=sheet_name, usecols=[0])
        total_rows = len(df_count)
        print(f"\nTOTAL DE FILAS (incluye header): {total_rows}")
        print(f"TOTAL DE FILAS DE DATOS: {total_rows - 1}")
        
    except Exception as e:
        print(f"Error analizando la pestaña {sheet_name}: {str(e)}")

def main():
    file_path = "data/raw/BASE PROYECTOS DESARROLLO PRODUCTIVO.xlsx"
    
    # Verificar que el archivo existe
    if not os.path.exists(file_path):
        print(f"Error: No se encontró el archivo {file_path}")
        return
    
    # Listar todas las pestañas disponibles
    try:
        excel_file = pd.ExcelFile(file_path)
        print(f"PESTAÑAS DISPONIBLES EN EL ARCHIVO:")
        for i, sheet in enumerate(excel_file.sheet_names):
            print(f"{i+1}. {sheet}")
        print()
        
        # Analizar pestaña SEMILLAS
        if "SEMILLAS" in excel_file.sheet_names:
            analyze_excel_sheet(file_path, "SEMILLAS")
        else:
            print("Error: No se encontró la pestaña SEMILLAS")
            
    except Exception as e:
        print(f"Error leyendo el archivo Excel: {str(e)}")

if __name__ == "__main__":
    main()