"""Validador flexible de datos para ETL - versión más permisiva."""
import pandas as pd
import re
from typing import Dict, List, Tuple
from loguru import logger


class DataValidatorFlexible:
    """Valida datos con reglas más flexibles para recuperar más registros."""
    
    def __init__(self):
        self.validation_errors = []
        self.valid_count = 0
        self.invalid_count = 0
        self.cedulas_corregidas = 0
        
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ejecuta todas las validaciones y marca registros."""
        logger.info(f"Iniciando validación flexible de {len(df)} registros")
        
        df_val = df.copy()
        
        # Agregar columnas de validación
        df_val['es_valido'] = True
        df_val['errores_validacion'] = ''
        df_val['cedula_corregida'] = df_val['cedula']  # Nueva columna para cédulas corregidas
        
        # Ejecutar validaciones (más flexibles)
        df_val = self._validate_cedulas_flexible(df_val)
        df_val = self._validate_fechas(df_val)
        df_val = self._validate_montos_flexible(df_val)
        df_val = self._validate_coordenadas(df_val)
        df_val = self._validate_relaciones_flexible(df_val)
        
        # Contar válidos e inválidos
        self.valid_count = df_val['es_valido'].sum()
        self.invalid_count = len(df_val) - self.valid_count
        
        logger.info(f"Validación completada: {self.valid_count} válidos, {self.invalid_count} inválidos")
        logger.info(f"Cédulas corregidas automáticamente: {self.cedulas_corregidas}")
        
        return df_val
    
    def _limpiar_cedula(self, cedula):
        """Limpia y corrige formato de cédula."""
        if pd.isna(cedula) or cedula == 'None':
            return None
            
        cedula_str = str(cedula).strip().upper()
        
        # Remover .0 del final (formato float)
        if cedula_str.endswith('.0'):
            cedula_str = cedula_str[:-2]
            
        # Reemplazar O por 0
        cedula_str = cedula_str.replace('O', '0')
        
        # Remover guiones y espacios
        cedula_str = cedula_str.replace('-', '').replace(' ', '')
        
        # Solo mantener dígitos
        cedula_str = ''.join(c for c in cedula_str if c.isdigit())
        
        # Si tiene 9 dígitos y no empieza con 0, agregar 0 al inicio
        if len(cedula_str) == 9 and not cedula_str.startswith('0'):
            cedula_str = '0' + cedula_str
            self.cedulas_corregidas += 1
            
        return cedula_str if cedula_str else None
    
    def _validate_cedulas_flexible(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida cédulas con corrección automática y validación flexible."""
        def validar_cedula_ecuador_flexible(cedula_original):
            """Validación flexible de cédula ecuatoriana."""
            # Limpiar cédula
            cedula = self._limpiar_cedula(cedula_original)
            
            if not cedula:
                return True, None  # No validar si no hay cédula
            
            # Verificar longitud
            if len(cedula) != 10:
                # Si tiene 11+ dígitos, podría ser un RUC, marcarlo como válido
                if len(cedula) >= 11:
                    return True, cedula[:10]  # Tomar solo los primeros 10 dígitos
                return False, cedula
                
            # Verificar que sean solo dígitos
            if not cedula.isdigit():
                return False, cedula
                
            # Validar provincia (primeros 2 dígitos) - ser más permisivo
            provincia = int(cedula[:2])
            if provincia < 1 or provincia > 30:  # Extender rango para incluir provincias nuevas
                # Si es 90-99, podría ser cédula antigua o especial
                if 90 <= provincia <= 99:
                    return True, cedula  # Aceptar como válida
                return False, cedula
                
            # Validar tercer dígito - ser más permisivo
            tercer_digito = int(cedula[2])
            if tercer_digito > 9:  # Solo verificar que sea un dígito
                return False, cedula
                
            # Para tercer dígito 7, 8, 9 (RUC o cédulas especiales), aceptar
            if tercer_digito in [7, 8, 9]:
                return True, cedula
                
            # Validación del dígito verificador - opcional
            # Si falla, aún marcar como válida pero con advertencia
            try:
                coeficientes = [2, 1, 2, 1, 2, 1, 2, 1, 2]
                suma = 0
                
                for i in range(9):
                    valor = int(cedula[i]) * coeficientes[i]
                    if valor >= 10:
                        valor = valor - 9
                    suma += valor
                    
                digito_verificador = 0 if suma % 10 == 0 else 10 - (suma % 10)
                
                if digito_verificador != int(cedula[9]):
                    # Solo advertencia, no invalidar
                    logger.debug(f"Cédula {cedula} con dígito verificador incorrecto, pero se acepta")
                    
            except Exception as e:
                logger.debug(f"Error validando dígito verificador de {cedula}: {e}")
                
            return True, cedula
        
        # Validar y corregir cédulas
        for idx, row in df.iterrows():
            if pd.notna(row.get('cedula')) and row['cedula'] != 'None':
                es_valida, cedula_corregida = validar_cedula_ecuador_flexible(row['cedula'])
                
                if not es_valida:
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += 'Cédula inválida (no recuperable); '
                elif cedula_corregida and cedula_corregida != str(row['cedula']):
                    # Actualizar con cédula corregida
                    df.at[idx, 'cedula_corregida'] = cedula_corregida
                    df.at[idx, 'cedula'] = cedula_corregida  # Actualizar también la original
                    
        logger.debug("Cédulas validadas con enfoque flexible")
        return df
    
    def _validate_fechas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida consistencia de fechas - mantener igual."""
        if 'fecha_retiro' not in df.columns or 'anio' not in df.columns:
            return df
            
        for idx, row in df.iterrows():
            if pd.notna(row['fecha_retiro']) and pd.notna(row['anio']):
                try:
                    fecha_anio = pd.to_datetime(row['fecha_retiro']).year
                    if fecha_anio != row['anio']:
                        # Solo advertencia, no invalidar
                        logger.debug(f"Año inconsistente en fila {idx}: {fecha_anio} != {row['anio']}")
                except:
                    pass
                    
        logger.debug("Fechas validadas")
        return df
    
    def _validate_montos_flexible(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida montos con mayor tolerancia."""
        for idx, row in df.iterrows():
            # Validar inversión con mayor tolerancia (10% de diferencia) - solo si el campo existe
            if 'inversion' in df.columns and all(pd.notna(row.get(col)) for col in ['hectarias_beneficiadas', 'precio_unitario', 'inversion']):
                inversion_calculada = row['hectarias_beneficiadas'] * row['precio_unitario']
                diferencia_porcentual = abs(inversion_calculada - row['inversion']) / row['inversion'] if row['inversion'] > 0 else 0
                
                # Permitir hasta 10% de diferencia
                if diferencia_porcentual > 0.1:
                    # Solo advertencia
                    logger.debug(f"Inversión con diferencia >10% en fila {idx}")
                    
            # Validar hectáreas - solo si la diferencia es muy grande
            if all(pd.notna(row.get(col)) for col in ['hectarias_totales', 'hectarias_beneficiadas']):
                if row['hectarias_beneficiadas'] > row['hectarias_totales'] * 1.5:  # 50% de tolerancia
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += 'Hectáreas beneficiadas excesivas; '
                    
        logger.debug("Montos validados con tolerancia")
        return df
    
    def _validate_coordenadas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida coordenadas - mantener flexible."""
        # No invalidar por coordenadas, solo advertir
        for idx, row in df.iterrows():
            if pd.notna(row.get('coordenada_x')):
                try:
                    x = float(row['coordenada_x'])
                    if not ((-82 <= x <= -75) or (500000 <= x <= 800000)):
                        logger.debug(f"Coordenada X fuera de rango en fila {idx}: {x}")
                except (ValueError, TypeError):
                    logger.debug(f"Coordenada X no numérica en fila {idx}: {row['coordenada_x']}")
                    
            if pd.notna(row.get('coordenada_y')):
                try:
                    y = float(row['coordenada_y'])
                    if not ((-5 <= y <= 2) or (9700000 <= y <= 10100000)):
                        logger.debug(f"Coordenada Y fuera de rango en fila {idx}: {y}")
                except (ValueError, TypeError):
                    logger.debug(f"Coordenada Y no numérica en fila {idx}: {row['coordenada_y']}")
                    
        logger.debug("Coordenadas validadas")
        return df
    
    def _validate_relaciones_flexible(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida relaciones con mayor flexibilidad."""
        for idx, row in df.iterrows():
            # Solo validar casos extremos
            
            # Si no hay nombres en absoluto, sí invalidar
            if pd.isna(row.get('nombres_apellidos')) or str(row.get('nombres_apellidos')).strip() == '':
                if pd.notna(row.get('cedula')) and row['cedula'] != 'None':
                    # Si hay cédula pero no nombres, es un problema serio
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += 'Sin nombres de beneficiario; '
                    
        logger.debug("Relaciones validadas con flexibilidad")
        return df
    
    def get_summary(self) -> Dict:
        """Retorna resumen de validación."""
        return {
            'registros_validos': self.valid_count,
            'registros_invalidos': self.invalid_count,
            'cedulas_corregidas': self.cedulas_corregidas,
            'tasa_validez': f"{(self.valid_count / (self.valid_count + self.invalid_count) * 100):.2f}%" if (self.valid_count + self.invalid_count) > 0 else "0%"
        }
    
    def get_validation_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera reporte de errores de validación."""
        invalid_df = df[~df['es_valido']].copy()
        
        if len(invalid_df) > 0:
            error_types = []
            for errors in invalid_df['errores_validacion']:
                error_types.extend(errors.split('; '))
                
            error_counts = pd.Series(error_types).value_counts()
            
            logger.info("Resumen de errores de validación (versión flexible):")
            for error, count in error_counts.items():
                if error:
                    logger.info(f"  - {error}: {count} ocurrencias")
                    
        return invalid_df[['numero_acta', 'nombres_apellidos', 'cedula', 'cedula_corregida', 'errores_validacion']]