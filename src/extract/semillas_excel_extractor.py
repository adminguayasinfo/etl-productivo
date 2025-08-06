"""Extractor para leer datos del Excel de semillas - pestaña SEMILLAS."""
import pandas as pd
from typing import Iterator, Dict, Any
from datetime import datetime
from loguru import logger


class SemillasExcelExtractor:
    """Extrae datos de la pestaña SEMILLAS del archivo Excel."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.total_rows = 0
    
    def safe_int(self, value):
        """Convierte de forma segura a entero, maneja valores nulos y espacios."""
        if value is None:
            return None
        str_val = str(value).strip()
        if str_val in ['', ' ', 'nan', 'NaN', 'None']:
            return None
        try:
            return int(float(str_val))  # Usar float primero por si hay decimales
        except (ValueError, TypeError):
            return None
    
    def safe_float(self, value):
        """Convierte de forma segura a float, maneja valores nulos y espacios."""
        if value is None:
            return None
        str_val = str(value).strip()
        if str_val in ['', ' ', 'nan', 'NaN', 'None']:
            return None
        try:
            return float(str_val)
        except (ValueError, TypeError):
            return None
        
    def extract(self, excel_path: str) -> pd.DataFrame:
        """Lee la pestaña SEMILLAS completa y retorna un DataFrame."""
        logger.info(f"Extrayendo datos de Excel: {excel_path}, pestaña: SEMILLAS")
        
        try:
            df = pd.read_excel(excel_path, sheet_name='SEMILLAS')
            self.total_rows = len(df)
            logger.info(f"Total de registros extraidos: {self.total_rows}")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo Excel: {str(e)}")
            raise
            
    def extract_batches(self, excel_path: str) -> Iterator[pd.DataFrame]:
        """Lee el Excel en lotes para procesamiento eficiente."""
        logger.info(f"Extrayendo datos en lotes de {self.batch_size} registros")
        
        try:
            # Leer el DataFrame completo
            df = pd.read_excel(excel_path, sheet_name='SEMILLAS')
            self.total_rows = len(df)
            
            # Dividir en chunks
            for i in range(0, len(df), self.batch_size):
                chunk = df.iloc[i:i+self.batch_size]
                yield chunk
                
        except Exception as e:
            logger.error(f"Error extrayendo Excel en lotes: {str(e)}")
            raise
            
    def prepare_row(self, row: pd.Series) -> Dict[str, Any]:
        """Prepara una fila para ser cargada a staging."""
        # Convertir NaN a None
        row = row.where(pd.notna(row), None)
        
        # Mapear campos del Excel a campos del modelo staging
        data = {
            # Campos principales del Excel
            'numero_acta': str(row.get('ACTAS')) if row.get('ACTAS') is not None else None,
            'organizacion': row.get('ASOCIACIONES'),
            'nombres_apellidos': row.get('NOMBRES COMPLETOS'),
            'cedula': str(row.get('CEDULA')) if row.get('CEDULA') is not None else None,
            'telefono': str(row.get('TELEFONO')) if row.get('TELEFONO') is not None else None,
            'genero': row.get('GENERO'),
            'edad': self.safe_int(row.get('EDAD')),
            'canton': row.get('CANTON'),
            'parroquia': row.get('PARROQUIA'),
            'localidad': row.get('RECINTO, COMUNA O SECTOR'),
            'coordenada_x': str(row.get('X')) if row.get('X') is not None else None,
            'coordenada_y': str(row.get('Y')) if row.get('Y') is not None else None,
            'hectarias_beneficiadas': self.safe_float(row.get('HECTAREAS')),
            'entrega': self.safe_int(row.get('ENTREGA')),
            'variedad': row.get('VARIEDAD'),
            'cultivo': row.get('CULTIVO 1'),
            'fecha_entrega': pd.to_datetime(row.get('FECHA DE ENTREGA')).date() if row.get('FECHA DE ENTREGA') is not None else None,
            'lugar_entrega': row.get('LUGAR DE ENTREGA'),
            'responsable_agencia': row.get('RESPONSABLE DE AGRIPAC'),
            'cedula_responsable': str(row.get('CEDULA2')) if row.get('CEDULA2') is not None else None,
            'precio_unitario': self.safe_float(row.get('PRECIO UNITARIO')),
            'observacion': row.get('OBSERVACION'),
            'anio': self.safe_int(row.get('AÑO')),
            
            # Campos legacy para mantener compatibilidad (se llenan con None)
            'documento': None,
            'proceso': None,
            'inversion': None,
            'cedula_jefe_sucursal': None,
            'sucursal': None,
            'fecha_retiro': None,
            'actualizacion': None,
            'rubro': None,
            'quintil': None,
            'score_quintil': None,
            
            # Campo de procesamiento
            'processed': False
        }
        
        return data
        
    def get_total_rows(self) -> int:
        """Retorna el total de filas extraidas."""
        return self.total_rows
        
    def get_column_mapping(self) -> Dict[str, str]:
        """Retorna el mapeo de columnas Excel -> Staging."""
        return {
            'ACTAS': 'numero_acta',
            'ASOCIACIONES': 'organizacion', 
            'NOMBRES COMPLETOS': 'nombres_apellidos',
            'CEDULA': 'cedula',
            'TELEFONO': 'telefono',
            'GENERO': 'genero',
            'EDAD': 'edad',
            'CANTON': 'canton',
            'PARROQUIA': 'parroquia',
            'RECINTO, COMUNA O SECTOR': 'localidad',
            'X': 'coordenada_x',
            'Y': 'coordenada_y',
            'HECTAREAS': 'hectarias_beneficiadas',
            'ENTREGA': 'entrega',
            'VARIEDAD': 'variedad',
            'CULTIVO 1': 'cultivo',
            'FECHA DE ENTREGA': 'fecha_entrega',
            'LUGAR DE ENTREGA': 'lugar_entrega',
            'RESPONSABLE DE AGRIPAC': 'responsable_agencia',
            'CEDULA2': 'cedula_responsable',
            'PRECIO UNITARIO': 'precio_unitario',
            'OBSERVACION': 'observacion',
            'AÑO': 'anio'
        }