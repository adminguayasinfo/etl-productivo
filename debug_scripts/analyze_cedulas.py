#!/usr/bin/env python3
"""Script para analizar las cédulas que fallan la validación."""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from src.models.operational.staging.semillas_stg_model import StgSemilla
from config.connections.database import db_connection
from sqlalchemy import select
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validar_cedula_ecuador_estricto(cedula):
    """Algoritmo estricto de validación de cédula ecuatoriana."""
    if pd.isna(cedula) or cedula == 'None':
        return True
        
    cedula = str(cedula).strip()
    
    # Debe tener 10 dígitos
    if not cedula.isdigit() or len(cedula) != 10:
        return False, f"Longitud incorrecta: {len(cedula)} dígitos"
        
    # Validar provincia (primeros 2 dígitos)
    provincia = int(cedula[:2])
    if provincia < 1 or provincia > 24:
        return False, f"Provincia inválida: {provincia}"
        
    # Validar tercer dígito
    tercer_digito = int(cedula[2])
    if tercer_digito > 6:
        return False, f"Tercer dígito inválido: {tercer_digito}"
        
    # Algoritmo de validación
    coeficientes = [2, 1, 2, 1, 2, 1, 2, 1, 2]
    suma = 0
    
    for i in range(9):
        valor = int(cedula[i]) * coeficientes[i]
        if valor >= 10:
            valor = valor - 9
        suma += valor
        
    digito_verificador = 0 if suma % 10 == 0 else 10 - (suma % 10)
    
    if digito_verificador == int(cedula[9]):
        return True, "OK"
    else:
        return False, f"Dígito verificador incorrecto: esperado {digito_verificador}, encontrado {cedula[9]}"

def analyze_cedulas():
    """Analiza las cédulas y por qué fallan."""
    
    with db_connection.get_session() as session:
        # Obtener todas las cédulas únicas
        stmt = select(StgSemilla.cedula, StgSemilla.nombres_apellidos).distinct()
        result = session.execute(stmt).all()
        
        logger.info(f'Total de registros únicos: {len(result)}')
        
        # Analizar cada cédula
        cedulas_validas = 0
        cedulas_invalidas = 0
        errores_por_tipo = {}
        
        ejemplos_por_tipo = {}
        
        for cedula, nombres in result:
            if pd.notna(cedula) and cedula != 'None':
                resultado = validar_cedula_ecuador_estricto(cedula)
                if isinstance(resultado, tuple):
                    valida, razon = resultado
                    if not valida:
                        cedulas_invalidas += 1
                        if razon not in errores_por_tipo:
                            errores_por_tipo[razon] = 0
                            ejemplos_por_tipo[razon] = []
                        errores_por_tipo[razon] += 1
                        if len(ejemplos_por_tipo[razon]) < 5:
                            ejemplos_por_tipo[razon].append((cedula, nombres))
                    else:
                        cedulas_validas += 1
                else:
                    if resultado:
                        cedulas_validas += 1
        
        # Mostrar resultados
        logger.info(f'\n=== ANÁLISIS DE CÉDULAS ===')
        logger.info(f'Cédulas válidas: {cedulas_validas}')
        logger.info(f'Cédulas inválidas: {cedulas_invalidas}')
        logger.info(f'Tasa de validez: {cedulas_validas/(cedulas_validas+cedulas_invalidas)*100:.1f}%')
        
        logger.info(f'\n=== TIPOS DE ERRORES ===')
        for error, count in sorted(errores_por_tipo.items(), key=lambda x: x[1], reverse=True):
            logger.info(f'{error}: {count} casos')
            logger.info('  Ejemplos:')
            for cedula, nombres in ejemplos_por_tipo[error]:
                logger.info(f'    - {cedula} ({nombres})')
        
        # Analizar patrones específicos
        logger.info(f'\n=== ANÁLISIS DE PATRONES ===')
        
        # Contar cédulas por provincia
        provincias = {}
        for cedula, _ in result:
            if pd.notna(cedula) and cedula != 'None' and len(str(cedula)) >= 2:
                prov = str(cedula)[:2]
                if prov not in provincias:
                    provincias[prov] = 0
                provincias[prov] += 1
        
        logger.info('\nDistribución por provincia (primeros 2 dígitos):')
        for prov, count in sorted(provincias.items()):
            try:
                prov_num = int(prov)
                if prov_num > 24:
                    logger.info(f'  {prov}: {count} casos (FUERA DE RANGO)')
                else:
                    logger.info(f'  {prov}: {count} casos')
            except:
                logger.info(f'  {prov}: {count} casos (NO NUMÉRICO)')

if __name__ == "__main__":
    analyze_cedulas()