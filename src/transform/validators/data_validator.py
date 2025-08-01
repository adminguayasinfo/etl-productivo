"""Validador genérico de datos para ETL."""
import pandas as pd
import re
from typing import Dict, List, Tuple
from loguru import logger


class DataValidator:
    """Valida datos según reglas de negocio genéricas."""
    
    def __init__(self):
        self.validation_errors = []
        self.valid_count = 0
        self.invalid_count = 0
        
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ejecuta todas las validaciones y marca registros."""
        logger.info(f"Iniciando validación de {len(df)} registros")
        
        df_val = df.copy()
        
        # Agregar columnas de validación
        df_val['es_valido'] = True
        df_val['errores_validacion'] = ''
        
        # Ejecutar validaciones
        df_val = self._validate_cedulas(df_val)
        df_val = self._validate_fechas(df_val)
        df_val = self._validate_montos(df_val)
        df_val = self._validate_coordenadas(df_val)
        df_val = self._validate_relaciones(df_val)
        
        # Contar válidos e inválidos
        self.valid_count = df_val['es_valido'].sum()
        self.invalid_count = len(df_val) - self.valid_count
        
        logger.info(f"Validación completada: {self.valid_count} válidos, {self.invalid_count} inválidos")
        
        return df_val
    
    def _validate_cedulas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida cédulas ecuatorianas."""
        def validar_cedula_ecuador(cedula):
            """Algoritmo de validación de cédula ecuatoriana."""
            if pd.isna(cedula) or cedula == 'None':
                return True  # No validar si no hay cédula
                
            cedula = str(cedula).strip()
            
            # Debe tener 10 dígitos
            if not cedula.isdigit() or len(cedula) != 10:
                return False
                
            # Validar provincia (primeros 2 dígitos)
            provincia = int(cedula[:2])
            if provincia < 1 or provincia > 24:
                return False
                
            # Validar tercer dígito
            tercer_digito = int(cedula[2])
            if tercer_digito > 6:
                return False
                
            # Algoritmo de validación
            coeficientes = [2, 1, 2, 1, 2, 1, 2, 1, 2]
            suma = 0
            
            for i in range(9):
                valor = int(cedula[i]) * coeficientes[i]
                if valor >= 10:
                    valor = valor - 9
                suma += valor
                
            digito_verificador = 0 if suma % 10 == 0 else 10 - (suma % 10)
            
            return digito_verificador == int(cedula[9])
        
        # Validar cédula del beneficiario
        for idx, row in df.iterrows():
            if pd.notna(row.get('cedula')) and row['cedula'] != 'None':
                if not validar_cedula_ecuador(row['cedula']):
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += 'Cédula inválida; '
                    
        logger.debug("Cédulas validadas")
        return df
    
    def _validate_fechas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida consistencia de fechas."""
        if 'fecha_retiro' not in df.columns or 'anio' not in df.columns:
            return df
            
        for idx, row in df.iterrows():
            if pd.notna(row['fecha_retiro']) and pd.notna(row['anio']):
                # Verificar que el año de la fecha coincida con el campo año
                fecha_anio = pd.to_datetime(row['fecha_retiro']).year
                if fecha_anio != row['anio']:
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += f'Año inconsistente ({fecha_anio} != {row["anio"]}); '
                    
        logger.debug("Fechas validadas")
        return df
    
    def _validate_montos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida montos y cálculos."""
        for idx, row in df.iterrows():
            # Validar inversión = hectáreas * precio_unitario
            if all(pd.notna(row[col]) for col in ['hectarias_beneficiadas', 'precio_unitario', 'inversion']):
                inversion_calculada = row['hectarias_beneficiadas'] * row['precio_unitario']
                diferencia = abs(inversion_calculada - row['inversion'])
                
                # Permitir pequeña diferencia por redondeo
                if diferencia > 0.01:
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += f'Inversión incorrecta (esperado: {inversion_calculada:.2f}); '
                    
            # Validar hectáreas beneficiadas <= hectáreas totales
            if all(pd.notna(row[col]) for col in ['hectarias_totales', 'hectarias_beneficiadas']):
                if row['hectarias_beneficiadas'] > row['hectarias_totales']:
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += 'Hectáreas beneficiadas > totales; '
                    
        logger.debug("Montos validados")
        return df
    
    def _validate_coordenadas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida que las coordenadas estén en rangos válidos."""
        for idx, row in df.iterrows():
            # Validar coordenada X (longitud)
            if pd.notna(row.get('coordenada_x')):
                x = row['coordenada_x']
                # Ecuador está aproximadamente entre -75 y -82 (o en UTM)
                if not ((-82 <= x <= -75) or (500000 <= x <= 800000)):
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += 'Coordenada X fuera de rango; '
                    
            # Validar coordenada Y (latitud)
            if pd.notna(row.get('coordenada_y')):
                y = row['coordenada_y']
                # Ecuador está aproximadamente entre -5 y 2 (o en UTM)
                if not ((-5 <= y <= 2) or (9700000 <= y <= 10100000)):
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += 'Coordenada Y fuera de rango; '
                    
        logger.debug("Coordenadas validadas")
        return df
    
    def _validate_relaciones(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida relaciones lógicas entre campos."""
        for idx, row in df.iterrows():
            # Si hay organización, debe haber al menos un nombre
            if pd.notna(row.get('organizacion')) and row['organizacion'] != 'None':
                if pd.isna(row.get('nombres_apellidos')) or row['nombres_apellidos'] == 'None':
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += 'Organización sin beneficiario; '
                    
            # Si hay hectáreas beneficiadas, debe haber cultivo
            if pd.notna(row.get('hectarias_beneficiadas')) and row['hectarias_beneficiadas'] > 0:
                if pd.isna(row.get('cultivo')) or row['cultivo'] == 'None':
                    df.at[idx, 'es_valido'] = False
                    df.at[idx, 'errores_validacion'] += 'Hectáreas sin cultivo especificado; '
                    
        logger.debug("Relaciones validadas")
        return df
    
    def get_summary(self) -> Dict:
        """Retorna resumen de validación."""
        return {
            'registros_validos': self.valid_count,
            'registros_invalidos': self.invalid_count,
            'tasa_validez': f"{(self.valid_count / (self.valid_count + self.invalid_count) * 100):.2f}%" if (self.valid_count + self.invalid_count) > 0 else "0%"
        }
    
    def get_validation_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera reporte de errores de validación."""
        invalid_df = df[~df['es_valido']].copy()
        
        if len(invalid_df) > 0:
            # Agrupar por tipo de error
            error_types = []
            for errors in invalid_df['errores_validacion']:
                error_types.extend(errors.split('; '))
                
            error_counts = pd.Series(error_types).value_counts()
            
            logger.info("Resumen de errores de validación:")
            for error, count in error_counts.items():
                if error:  # Ignorar strings vacíos
                    logger.info(f"  - {error}: {count} ocurrencias")
                    
        return invalid_df[['numero_acta', 'nombres_apellidos', 'cedula', 'errores_validacion']]